import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import type { ExportJob } from '@/lib/export/types'

interface StatusResponse extends ExportJob {
  cli_command?: string
  download_url?: string
}

/**
 * GET /api/admin/exports/[jobId]/status
 * Get export job status and metadata
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
          SELECT
            id,
            export_type,
            filter_config,
            status,
            started_at,
            completed_at,
            error_message,
            records_exported,
            table_name,
            expires_at,
            worker_type,
            created_by,
            created_at
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
      const response: StatusResponse = { ...job }

      // Add CLI command if completed
      if (job.status === 'completed' && job.table_name) {
        const database = process.env.MOTHERDUCK_DATABASE || 'newagekbo'
        response.cli_command = `duckdb -c "COPY (SELECT * FROM md:${database}.${job.table_name}) TO 'vat-entities.csv' (FORMAT CSV, HEADER)"`

        // Add download URL
        const baseUrl = request.url.split('/api/')[0]
        response.download_url = `${baseUrl}/api/admin/exports/${jobId}/download`
      }

      return NextResponse.json(response)
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error: unknown) {
    console.error('Failed to get export status:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Failed to get export status',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
