/**
 * KBO Open Data Portal Client
 * Handles authentication and file downloads from the Belgian KBO portal
 * Uses session-based authentication (login form + cookies)
 */

import { KboDatasetFile } from '../types/kbo-portal'

const KBO_BASE_URL = 'https://kbopub.economie.fgov.be/kbo-open-data'
const LOGIN_URL = `${KBO_BASE_URL}/static/j_spring_security_check`
const FILES_XML_URL = `${KBO_BASE_URL}/affiliation/xml/?files`
const FILES_DOWNLOAD_BASE = `${KBO_BASE_URL}/affiliation/xml/files`

// Cache for session cookies (reuse across requests in the same process)
let cachedSessionCookies: string | null = null
let sessionExpiry: number = 0

/**
 * Error thrown when KBO portal authentication fails
 */
export class KboAuthenticationError extends Error {
  constructor(message: string, public statusCode?: number) {
    super(message)
    this.name = 'KboAuthenticationError'
  }
}

/**
 * Error thrown when KBO portal file download fails
 */
export class KboDownloadError extends Error {
  constructor(message: string, public statusCode?: number) {
    super(message)
    this.name = 'KboDownloadError'
  }
}

/**
 * Get KBO credentials from environment variables
 */
function getCredentials(): { username: string; password: string } {
  const username = process.env.KBO_USERNAME
  const password = process.env.KBO_PASSWORD

  if (!username || !password) {
    throw new KboAuthenticationError(
      'KBO credentials not configured. Set KBO_USERNAME and KBO_PASSWORD environment variables.'
    )
  }

  return { username, password }
}

/**
 * Authenticate with KBO portal and get session cookies
 * Uses Spring Security form-based login
 */
async function authenticate(): Promise<string> {
  const { username, password } = getCredentials()

  try {
    // Step 1: POST credentials to Spring Security endpoint
    const formData = new URLSearchParams()
    formData.append('j_username', username)
    formData.append('j_password', password)
    formData.append('submit', 'Login')

    const loginResponse = await fetch(LOGIN_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: formData.toString(),
      redirect: 'manual', // Don't follow redirects automatically
    })

    // Collect all Set-Cookie headers
    const setCookieHeaders = loginResponse.headers.getSetCookie?.() || []

    if (setCookieHeaders.length === 0) {
      // Fallback for older Node.js versions
      const rawSetCookie = loginResponse.headers.get('set-cookie')
      if (rawSetCookie) {
        setCookieHeaders.push(rawSetCookie)
      }
    }

    if (setCookieHeaders.length === 0) {
      throw new KboAuthenticationError(
        'No session cookies received from login. Authentication may have failed.'
      )
    }

    // Extract cookie values (before the first semicolon)
    const cookies = setCookieHeaders.map(cookie => cookie.split(';')[0]).join('; ')

    // Check if login was successful (Spring Security redirects on success)
    if (loginResponse.status !== 302 && loginResponse.status !== 200) {
      throw new KboAuthenticationError(
        `Login failed with status ${loginResponse.status}. Check credentials.`,
        loginResponse.status
      )
    }

    // Step 2: Visit signup page to set language preference and get additional cookies
    // This is required for the portal to work correctly
    const signupUrl = `${KBO_BASE_URL}/signup?form=&lang=en`
    const signupResponse = await fetch(signupUrl, {
      headers: {
        Cookie: cookies,
      },
    })

    // Collect any additional cookies from signup
    const additionalCookies = signupResponse.headers.getSetCookie?.() || []
    let allCookies = cookies
    if (additionalCookies.length > 0) {
      allCookies = cookies + '; ' + additionalCookies.map(c => c.split(';')[0]).join('; ')
    }

    console.log('✓ KBO authentication successful')
    return allCookies
  } catch (error) {
    if (error instanceof KboAuthenticationError) {
      throw error
    }
    throw new KboAuthenticationError(
      `Authentication error: ${error instanceof Error ? error.message : 'Unknown error'}`
    )
  }
}

/**
 * Get session cookies (cached or fresh)
 */
async function getSessionCookies(): Promise<string> {
  const now = Date.now()

  // Return cached cookies if still valid (within 30 minutes)
  if (cachedSessionCookies && now < sessionExpiry) {
    return cachedSessionCookies
  }

  // Authenticate and cache the cookies
  cachedSessionCookies = await authenticate()
  sessionExpiry = now + 30 * 60 * 1000 // 30 minutes

  return cachedSessionCookies
}

/**
 * Fetch and parse the file listing page to find available downloads
 * @returns List of available dataset files with metadata
 */
export async function listAvailableFiles(): Promise<KboDatasetFile[]> {
  const cookies = await getSessionCookies()

  try {
    // Fetch the XML files listing page
    const response = await fetch(FILES_XML_URL, {
      headers: {
        Cookie: cookies,
      },
    })

    if (!response.ok) {
      throw new KboDownloadError(
        `Failed to fetch file listing: ${response.status} ${response.statusText}`,
        response.status
      )
    }

    const html = await response.text()

    // Parse HTML to find download links
    // The page should contain links to ZIP files
    const files: KboDatasetFile[] = []
    const linkRegex = /href="([^"]*KboOpenData[^"]*\.zip)"/gi
    let match

    while ((match = linkRegex.exec(html)) !== null) {
      const linkPath = match[1]
      // Convert relative URLs to absolute
      // Links are in format "files/KboOpenData_..." relative to /affiliation/xml/
      let url: string
      if (linkPath.startsWith('http')) {
        url = linkPath
      } else if (linkPath.startsWith('/')) {
        url = `${KBO_BASE_URL}${linkPath}`
      } else {
        // Relative path like "files/KboOpenData_..."
        url = `${KBO_BASE_URL}/affiliation/xml/${linkPath}`
      }

      const filename = url.split('/').pop() || ''
      const metadata = parseFilename(filename)

      if (metadata) {
        files.push({
          filename,
          url,
          extract_number: metadata.extract_number,
          snapshot_date: metadata.snapshot_date,
          file_type: metadata.file_type,
          imported: false, // This will be updated by the API endpoint
        })
      }
    }

    return files.sort((a, b) => b.extract_number - a.extract_number)
  } catch (error) {
    if (error instanceof KboDownloadError) {
      throw error
    }
    throw new KboDownloadError(
      `Error fetching file list: ${error instanceof Error ? error.message : 'Unknown error'}`
    )
  }
}

/**
 * Download a file from the KBO portal using session authentication
 * @param url - Full URL to the file OR just the filename (will be resolved from file listing)
 * @returns Buffer containing the file data
 */
export async function downloadFile(url: string): Promise<Buffer> {
  const cookies = await getSessionCookies()

  // If only a filename was provided, construct the full URL
  let downloadUrl = url
  if (!url.startsWith('http')) {
    // It's just a filename, construct the URL
    downloadUrl = `${FILES_DOWNLOAD_BASE}/${url}`
  }

  try {
    const response = await fetch(downloadUrl, {
      headers: {
        Cookie: cookies,
      },
    })

    if (!response.ok) {
      // If we get 401, try refreshing the session
      if (response.status === 401) {
        console.log('Session expired, re-authenticating...')
        cachedSessionCookies = null // Clear cache
        const newCookies = await getSessionCookies()

        // Retry with new cookies
        const retryResponse = await fetch(downloadUrl, {
          headers: {
            Cookie: newCookies,
          },
        })

        if (!retryResponse.ok) {
          throw new KboDownloadError(
            `Failed to download file: ${retryResponse.status} ${retryResponse.statusText}`,
            retryResponse.status
          )
        }

        const arrayBuffer = await retryResponse.arrayBuffer()
        return Buffer.from(arrayBuffer)
      }

      throw new KboDownloadError(
        `Failed to download file: ${response.status} ${response.statusText}`,
        response.status
      )
    }

    const arrayBuffer = await response.arrayBuffer()
    return Buffer.from(arrayBuffer)
  } catch (error) {
    if (error instanceof KboDownloadError) {
      throw error
    }

    // Provide more detailed error information
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    const errorCause = error instanceof Error && 'cause' in error ? String(error.cause) : ''

    throw new KboDownloadError(
      `Network error while downloading file: ${errorMessage}${errorCause ? ` (${errorCause})` : ''}`
    )
  }
}

/**
 * Parse filename to extract metadata
 * Format: KboOpenData_0141_2025_10_06_Update.zip or KboOpenData_0140_2024_01_Full.zip
 */
function parseFilename(filename: string): {
  extract_number: number
  snapshot_date: string
  file_type: 'full' | 'update'
} | null {
  const match = filename.match(
    /KboOpenData_(\d+)_(\d{4})_(\d{2})_(\d{2})_(Update|Full)\.zip/i
  )

  if (!match) {
    return null
  }

  const [, extractNum, year, month, day, type] = match
  const extract_number = parseInt(extractNum, 10)
  const snapshot_date = `${year}-${month}-${day}`
  const file_type = type.toLowerCase() === 'update' ? 'update' : 'full'

  return { extract_number, snapshot_date, file_type }
}

/**
 * List available daily update files from the KBO portal
 * Filters to only show update files (not full dumps)
 */
export async function listDailyUpdates(): Promise<KboDatasetFile[]> {
  const allFiles = await listAvailableFiles()
  return allFiles.filter(file => file.file_type === 'update')
}

/**
 * Construct download URL for a known filename
 */
export function getDownloadUrl(filename: string): string {
  return `${FILES_DOWNLOAD_BASE}/${filename}`
}

/**
 * Extract metadata from a KBO filename
 */
export function extractFileMetadata(filename: string) {
  return parseFilename(filename)
}
