import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import type { ExportJob, ExportJobsListResponse } from '@/lib/export/types'

/**
 * GET /api/export-jobs
 * List all export jobs with pagination
 */
export async function GET(request: Request) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Parse pagination parameters
    const { searchParams } = new URL(request.url)
    const page = parseInt(searchParams.get('page') || '1')
    const limit = parseInt(searchParams.get('limit') || '25')
    const offset = (page - 1) * limit

    // Connect to Motherduck
    const connection = await connectMotherduck()

    try {
      // Get total count
      const countResult = await executeQuery<{ total: bigint }>(
        connection,
        'SELECT COUNT(*) as total FROM export_jobs'
      )
      const total = Number(countResult[0]?.total || 0)

      // Get paginated export jobs
      const jobs = await executeQuery<ExportJob>(
        connection,
        `
          SELECT
            id,
            export_type,
            filter_config,
            status,
            strftime(started_at, '%Y-%m-%dT%H:%M:%S.000Z') as started_at,
            strftime(completed_at, '%Y-%m-%dT%H:%M:%S.000Z') as completed_at,
            error_message,
            records_exported,
            table_name,
            strftime(expires_at, '%Y-%m-%dT%H:%M:%S.000Z') as expires_at,
            worker_type,
            created_by,
            strftime(created_at, '%Y-%m-%dT%H:%M:%S.000Z') as created_at
          FROM export_jobs
          ORDER BY created_at DESC
          LIMIT ${limit} OFFSET ${offset}
        `
      )

      const totalPages = Math.ceil(total / limit)

      // Convert BigInt for JSON serialization (timestamps already formatted by SQL)
      const serializedJobs = jobs.map(job => ({
        id: job.id,
        export_type: job.export_type,
        filter_config: job.filter_config,
        status: job.status,
        started_at: job.started_at,
        completed_at: job.completed_at,
        error_message: job.error_message,
        records_exported: typeof job.records_exported === 'bigint' ? Number(job.records_exported) : job.records_exported,
        table_name: job.table_name,
        expires_at: job.expires_at,
        worker_type: job.worker_type,
        created_by: job.created_by,
        created_at: job.created_at,
      }))

      const response: ExportJobsListResponse = {
        jobs: serializedJobs,
        total,
        page,
        totalPages,
      }

      // Use JSON.stringify replacer to catch any remaining BigInt values
      return NextResponse.json(
        JSON.parse(
          JSON.stringify(response, (_, value) =>
            typeof value === 'bigint' ? Number(value) : value
          )
        )
      )
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error: unknown) {
    console.error('Failed to fetch export jobs:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Failed to fetch export jobs',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
