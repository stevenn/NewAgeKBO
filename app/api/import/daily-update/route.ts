import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { downloadFile, extractFileMetadata } from '@/lib/kbo-client'
import { processDailyUpdate } from '@/lib/import/daily-update'

// Vercel serverless function configuration
export const maxDuration = 300 // 5 minutes for large imports (requires Pro plan)
export const dynamic = 'force-dynamic' // Disable caching for import operations

/**
 * POST /api/import/daily-update
 * Trigger a daily update import from the KBO portal
 *
 * Request body:
 * {
 *   "url": "https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/KboOpenData_0141_2025_10_06_Update.zip"
 * }
 *
 * OR
 *
 * {
 *   "filename": "KboOpenData_0141_2025_10_06_Update.zip"
 * }
 *
 * Response:
 * {
 *   "success": true,
 *   "job_id": "uuid",
 *   "extract_number": 141,
 *   "snapshot_date": "2025-10-06",
 *   "records_processed": 156,
 *   "tables_processed": ["enterprises", "addresses"]
 * }
 */
export async function POST(request: NextRequest) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Parse request body
    const body = await request.json()
    const { url, filename } = body

    if (!url && !filename) {
      return NextResponse.json(
        { error: 'Either url or filename is required' },
        { status: 400 }
      )
    }

    // Construct full URL if only filename provided
    const downloadUrl = url || `https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/${filename}`

    // Extract metadata from filename for validation
    const urlFilename = downloadUrl.split('/').pop() || ''
    const metadata = extractFileMetadata(urlFilename)

    if (!metadata) {
      return NextResponse.json(
        { error: 'Invalid filename format. Expected: KboOpenData_NNNN_YYYY_MM_DD_Update.zip' },
        { status: 400 }
      )
    }

    if (metadata.file_type !== 'update') {
      return NextResponse.json(
        { error: 'This endpoint only accepts daily update files (not full dumps)' },
        { status: 400 }
      )
    }

    console.log(`📥 Starting download: ${urlFilename}`)
    console.log(`   Extract: ${metadata.extract_number}, Date: ${metadata.snapshot_date}`)

    // Download the file from KBO portal
    let zipBuffer: Buffer
    try {
      zipBuffer = await downloadFile(downloadUrl)
      console.log(`   ✓ Downloaded ${zipBuffer.length} bytes`)
    } catch (error: any) {
      console.error('Download failed:', error)
      return NextResponse.json(
        {
          error: 'Failed to download file from KBO portal',
          details: error.message
        },
        { status: 502 } // Bad Gateway - external service error
      )
    }

    // Process the import
    console.log(`🔄 Processing import...`)
    try {
      const stats = await processDailyUpdate(zipBuffer, 'web_manual')

      console.log(`   ✓ Import completed successfully`)
      console.log(`   Tables: ${stats.tablesProcessed.join(', ')}`)
      console.log(`   Records: ${stats.deletesApplied + stats.insertsApplied}`)

      // Return success response
      return NextResponse.json({
        success: true,
        extract_number: stats.metadata.extractNumber,
        snapshot_date: stats.metadata.snapshotDate,
        records_processed: stats.deletesApplied + stats.insertsApplied,
        records_inserted: stats.insertsApplied,
        records_deleted: stats.deletesApplied,
        tables_processed: stats.tablesProcessed,
        errors: stats.errors.length > 0 ? stats.errors : undefined
      })
    } catch (error: any) {
      console.error('Import processing failed:', error)

      // Check if it's a duplicate extract error
      if (error.message?.includes('UNIQUE constraint') || error.message?.includes('duplicate key')) {
        return NextResponse.json(
          {
            error: 'This extract has already been imported',
            extract_number: metadata.extract_number,
            details: 'Check the import jobs list to see existing import'
          },
          { status: 409 } // Conflict
        )
      }

      return NextResponse.json(
        {
          error: 'Failed to process import',
          details: error.message
        },
        { status: 500 }
      )
    }
  } catch (error: any) {
    console.error('Unexpected error in daily update import:', error)
    return NextResponse.json(
      {
        error: 'Internal server error',
        details: error.message
      },
      { status: 500 }
    )
  }
}
