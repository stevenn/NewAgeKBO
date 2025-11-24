import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import type { ExportJob } from '@/lib/export/types'

/**
 * GET /api/admin/exports/[jobId]/download
 * Download exported CSV file from MotherDuck table
 */
export async function GET(
  request: Request,
  { params }: { params: Promise<{ jobId: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const { jobId } = await params

    // Connect to Motherduck
    const connection = await connectMotherduck()

    try {
      // Get export job details
      const jobs = await executeQuery<ExportJob>(
        connection,
        `
          SELECT id, status, table_name, export_type, expires_at, records_exported
          FROM export_jobs
          WHERE id = '${jobId}'
        `
      )

      if (jobs.length === 0) {
        return NextResponse.json(
          { error: 'Export job not found' },
          { status: 404 }
        )
      }

      const job = jobs[0]

      // Check if export is completed
      if (job.status !== 'completed') {
        return NextResponse.json(
          { error: 'Export not completed yet', status: job.status },
          { status: 400 }
        )
      }

      // Check if table exists
      if (!job.table_name) {
        return NextResponse.json(
          { error: 'No export table associated with this job' },
          { status: 400 }
        )
      }

      // Check if export has expired
      if (job.expires_at && new Date(job.expires_at) < new Date()) {
        return NextResponse.json(
          { error: 'Export has expired and been deleted' },
          { status: 410 } // 410 Gone
        )
      }

      console.log(`📥 Downloading export from table: ${job.table_name}`)

      // Query data from MotherDuck table
      const data = await executeQuery<Record<string, unknown>>(
        connection,
        `SELECT * FROM ${job.table_name} ORDER BY entity_number`
      )

      // Convert to CSV
      if (data.length === 0) {
        return NextResponse.json(
          { error: 'Export table is empty' },
          { status: 404 }
        )
      }

      // Build CSV content
      const columns = Object.keys(data[0])
      const header = columns.map(col => `"${col}"`).join(',') + '\n'

      const rows = data.map(row => {
        return columns
          .map(col => {
            const value = row[col]
            if (value === null || value === undefined) return '""'
            const str = String(value)
            // Escape quotes by doubling them
            const escaped = str.replace(/"/g, '""')
            return `"${escaped}"`
          })
          .join(',')
      }).join('\n')

      const csvContent = header + rows

      // Generate filename with date
      const date = new Date().toISOString().split('T')[0]
      const filename = `kbo-vat-entities-${date}.csv`

      // Return as downloadable file
      return new NextResponse(csvContent, {
        status: 200,
        headers: {
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': `attachment; filename="${filename}"`,
          'Content-Length': Buffer.byteLength(csvContent, 'utf8').toString(),
        },
      })
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error: unknown) {
    console.error('❌ Download failed:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Download failed',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
