import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { prepareImport } from '@/lib/import/batched-update'
import { downloadFile, extractFileMetadata } from '@/lib/kbo-client'
import { WorkerType } from '@/lib/types/import-job'

/**
 * POST /api/admin/imports/prepare
 *
 * Prepares a KBO update ZIP file for batched import by downloading from URL.
 * Expects JSON body with 'url' or 'filename' field.
 *
 * Parses the ZIP, populates staging tables, and creates batch tracking records.
 *
 * Response: PrepareImportResult with job_id and batch counts
 */
export async function POST(request: NextRequest) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Get worker type from query params (default to 'vercel')
    const workerType = request.nextUrl.searchParams.get('workerType') || 'vercel'
    if (!['local', 'vercel', 'backfill', 'web_manual'].includes(workerType)) {
      return NextResponse.json(
        { error: 'Invalid workerType. Must be: local, vercel, backfill, or web_manual' },
        { status: 400 }
      )
    }

    // Parse JSON body with URL or filename
    const body = await request.json()
    const { url, filename: filenameParam } = body

    if (!url && !filenameParam) {
      return NextResponse.json(
        { error: 'Either url or filename is required' },
        { status: 400 }
      )
    }

    // Construct full URL if only filename provided
    const downloadUrl = url || `https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/${filenameParam}`
    const filename = downloadUrl.split('/').pop() || ''

    // Validate filename format
    const metadata = extractFileMetadata(filename)
    if (!metadata) {
      return NextResponse.json(
        { error: 'Invalid filename format. Expected: KboOpenData_NNNN_YYYY_MM_DD_Update.zip' },
        { status: 400 }
      )
    }

    console.log(`[API] Downloading file: ${filename}`)
    console.log(`   Extract: ${metadata.extract_number}, Date: ${metadata.snapshot_date}`)

    // Download the file from KBO portal
    let buffer: Buffer
    try {
      buffer = await downloadFile(downloadUrl)
      console.log(`   ✓ Downloaded ${buffer.length} bytes`)
    } catch (error) {
      console.error('[API] Download failed:', error)
      return NextResponse.json(
        {
          error: 'Failed to download file from KBO portal',
          details: error instanceof Error ? error.message : 'Unknown error'
        },
        { status: 500 }
      )
    }

    // Validate file size (max 100MB)
    if (buffer.length > 100 * 1024 * 1024) {
      return NextResponse.json(
        { error: 'File too large (max 100MB)' },
        { status: 400 }
      )
    }

    // Prepare the import
    console.log(`[API] Preparing batched import: ${filename} (${Math.round(buffer.length / 1024)}KB)`)
    const result = await prepareImport(buffer, workerType as WorkerType)

    return NextResponse.json(result)

  } catch (error) {
    console.error('[API] Failed to prepare import:', error)

    if (error instanceof Error) {
      return NextResponse.json(
        { error: error.message },
        { status: 500 }
      )
    }

    return NextResponse.json(
      { error: 'Failed to prepare import' },
      { status: 500 }
    )
  }
}
