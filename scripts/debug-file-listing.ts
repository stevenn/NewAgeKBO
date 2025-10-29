#!/usr/bin/env tsx

/**
 * Debug KBO File Listing Page
 * Shows the raw HTML to help understand the page structure
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { KboAuthenticationError } from '../lib/kbo-client'

const KBO_BASE_URL = 'https://kbopub.economie.fgov.be/kbo-open-data'
const LOGIN_URL = `${KBO_BASE_URL}/static/j_spring_security_check`
const DOWNLOAD_PAGE_URL = `${KBO_BASE_URL}/affiliation/xml?form=`

async function authenticate(): Promise<string> {
  const username = process.env.KBO_USERNAME!
  const password = process.env.KBO_PASSWORD!

  const formData = new URLSearchParams()
  formData.append('j_username', username)
  formData.append('j_password', password)
  formData.append('submit', 'Login')

  console.log('Posting login credentials to Spring Security endpoint...')
  console.log(`URL: ${LOGIN_URL}`)
  console.log(`Username: ${username}`)
  console.log(`Form data: ${formData.toString()}`)

  const loginResponse = await fetch(LOGIN_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded',
    },
    body: formData.toString(),
    redirect: 'manual',
  })

  console.log(`\nLogin response status: ${loginResponse.status}`)
  console.log(`Location: ${loginResponse.headers.get('location')}`)

  const setCookieHeaders = loginResponse.headers.getSetCookie?.() || []
  console.log(`Cookies received: ${setCookieHeaders.length}`)

  if (setCookieHeaders.length > 0) {
    console.log('\nCookies:')
    setCookieHeaders.forEach((cookie, i) => {
      console.log(`  ${i + 1}. ${cookie.substring(0, 150)}`)
    })
  }

  const cookies = setCookieHeaders.map(cookie => cookie.split(';')[0]).join('; ')
  console.log(`\nCookie string for future requests: ${cookies}`)

  // Follow the redirect if there is one
  const location = loginResponse.headers.get('location')
  if (location) {
    console.log(`\nFollowing redirect to: ${location}`)
    let redirectUrl = location
    if (!location.startsWith('http')) {
      // It's a relative URL
      redirectUrl = location.startsWith('/') ? `https://kbopub.economie.fgov.be${location}` : `${KBO_BASE_URL}/${location}`
    }
    console.log(`Full redirect URL: ${redirectUrl}`)

    const redirectResponse = await fetch(redirectUrl, {
      headers: {
        Cookie: cookies,
      },
    })

    console.log(`Redirect response status: ${redirectResponse.status}`)

    // Collect any additional cookies from the redirect
    const additionalCookies = redirectResponse.headers.getSetCookie?.() || []
    if (additionalCookies.length > 0) {
      console.log(`Additional cookies from redirect: ${additionalCookies.length}`)
      const allCookies = cookies + '; ' + additionalCookies.map(c => c.split(';')[0]).join('; ')
      return allCookies
    }
  }

  return cookies
}

async function main() {
  console.log('🔍 Debugging KBO Download Page\n')

  const cookies = await authenticate()
  console.log('✓ Authenticated\n')

  // Step 1: Navigate to signup with language (to set language preference)
  console.log('Step 1: Setting language preference to English...')
  const signupUrl = `${KBO_BASE_URL}/signup?form=&lang=en`
  const signupResponse = await fetch(signupUrl, {
    headers: {
      Cookie: cookies,
    },
  })
  console.log(`Signup response status: ${signupResponse.status}`)

  // Collect any new cookies
  const newCookies = signupResponse.headers.getSetCookie?.() || []
  let allCookies = cookies
  if (newCookies.length > 0) {
    console.log(`Got ${newCookies.length} additional cookies`)
    allCookies = cookies + '; ' + newCookies.map(c => c.split(';')[0]).join('; ')
  }

  // Step 2: Try the files listing page directly
  console.log('\nStep 2: Fetching files listing page...')
  const filesListUrl = `${KBO_BASE_URL}/affiliation/xml/?files`
  console.log(`URL: ${filesListUrl}`)

  const response = await fetch(filesListUrl, {
    headers: {
      Cookie: allCookies,
    },
  })

  console.log(`Status: ${response.status}`)
  console.log(`Content-Type: ${response.headers.get('content-type')}`)
  console.log('')

  const content = await response.text()
  console.log(`Content length: ${content.length} characters\n`)

  // Look for ZIP files
  const zipFiles = content.match(/KboOpenData[^"<>]*\.zip/gi)
  console.log(`ZIP filename pattern matches: ${zipFiles?.length || 0}`)

  if (zipFiles) {
    console.log('\n✅ Found ZIP files:')
    // Deduplicate
    const uniqueFiles = [...new Set(zipFiles)]
    uniqueFiles.slice(0, 20).forEach(f => console.log(`  ${f}`))
  }

  // Look for download links with href
  const downloadLinks = content.match(/href="([^"]*\.zip)"/gi)
  if (downloadLinks) {
    console.log(`\nFound ${downloadLinks.length} download links with href:`)
    downloadLinks.slice(0, 10).forEach(link => console.log(`  ${link}`))
  }

  // Pretty print the HTML for inspection
  console.log('\n\n='.repeat(80))
  console.log('FULL PAGE CONTENT (formatted for readability):')
  console.log('='.repeat(80))

  // Basic pretty printing - add newlines after common tags
  const prettyHtml = content
    .replace(/></g, '>\n<')
    .replace(/(<\/[^>]+>)/g, '$1\n')
    .split('\n')
    .filter(line => line.trim().length > 0)
    .join('\n')

  console.log(prettyHtml)
  console.log('\n' + '='.repeat(80))
  console.log('END OF PAGE CONTENT')
  console.log('='.repeat(80))

  // Save to file for easier inspection
  const fs = require('fs')
  const outputPath = '/tmp/kbo-debug.html'
  fs.writeFileSync(outputPath, content)
  console.log(`\n✓ Full HTML saved to: ${outputPath}`)

  const prettyPath = '/tmp/kbo-debug-pretty.html'
  fs.writeFileSync(prettyPath, prettyHtml)
  console.log(`✓ Pretty HTML saved to: ${prettyPath}`)
}

main().catch(console.error)
