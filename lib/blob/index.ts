/**
 * Vercel Blob Storage Utilities
 *
 * Provides helpers for uploading, downloading, and deleting files from Vercel Blob storage.
 * Used to store KBO import ZIP files temporarily, avoiding Restate payload limits.
 */

import { put, del } from '@vercel/blob'

/**
 * Result from uploading a file to blob storage
 */
export interface BlobUploadResult {
  url: string
  pathname: string
  size: number
}

/**
 * Upload a buffer to Vercel Blob storage
 *
 * @param buffer - File contents to upload
 * @param filename - Original filename (used for path construction)
 * @param workflowId - Restate workflow ID for grouping
 * @returns Blob URL and metadata
 */
export async function uploadToBlob(
  buffer: Buffer,
  filename: string,
  workflowId: string
): Promise<BlobUploadResult> {
  const pathname = `kbo-imports/${workflowId}-${filename}`

  const blob = await put(pathname, buffer, {
    access: 'public',
    token: process.env.NEWAGEKBOBLOB_READ_WRITE_TOKEN,
  })

  return {
    url: blob.url,
    pathname: blob.pathname,
    size: buffer.length,
  }
}

/**
 * Download a file from Vercel Blob storage
 *
 * @param url - Blob URL to download from
 * @returns File contents as Buffer
 */
export async function downloadFromBlob(url: string): Promise<Buffer> {
  const response = await fetch(url)

  if (!response.ok) {
    throw new Error(`Failed to download from blob: ${response.status} ${response.statusText}`)
  }

  const arrayBuffer = await response.arrayBuffer()
  return Buffer.from(arrayBuffer)
}

/**
 * Delete a file from Vercel Blob storage
 *
 * @param url - Blob URL to delete
 */
export async function deleteFromBlob(url: string): Promise<void> {
  await del(url, {
    token: process.env.NEWAGEKBOBLOB_READ_WRITE_TOKEN,
  })
}
