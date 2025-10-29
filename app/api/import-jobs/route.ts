import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'

export interface ImportJobRecord {
  id: string
  type: 'daily' | 'monthly'
  status: 'pending' | 'running' | 'completed' | 'failed'
  startedAt: string | null
  completedAt: string | null
  extractNumber: number
  snapshotDate: string
  recordsProcessed: number
  recordsInserted: number
  recordsUpdated: number
  recordsDeleted: number
  errorMessage: string | null
  workerType: string
}

export async function GET(request: NextRequest) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Get pagination parameters
    const searchParams = request.nextUrl.searchParams
    const page = parseInt(searchParams.get('page') || '1', 10)
    const limit = parseInt(searchParams.get('limit') || '25', 10)
    const offset = (page - 1) * limit

    // Validate pagination parameters
    if (page < 1 || limit < 1 || limit > 200) {
      return NextResponse.json(
        { error: 'Invalid pagination parameters' },
        { status: 400 }
      )
    }

    // Connect to Motherduck
    const connection = await connectMotherduck()

    try {
      // Get total count
      const countResults = await executeQuery<{ total: number }>(
        connection,
        `SELECT COUNT(*) as total FROM import_jobs`
      )
      const total = Number(countResults[0]?.total || 0)
      const totalPages = Math.ceil(total / limit)

      // Fetch import jobs ordered by most recent first
      const results = await executeQuery<{
        id: string
        extract_number: number
        extract_type: string
        snapshot_date: string
        status: string
        started_at: string | null
        completed_at: string | null
        records_processed: number
        records_inserted: number
        records_updated: number
        records_deleted: number
        error_message: string | null
        worker_type: string
      }>(
        connection,
        `SELECT
          id,
          extract_number,
          extract_type,
          snapshot_date::VARCHAR as snapshot_date,
          status,
          started_at::VARCHAR as started_at,
          completed_at::VARCHAR as completed_at,
          records_processed,
          records_inserted,
          records_updated,
          records_deleted,
          error_message,
          worker_type
        FROM import_jobs
        ORDER BY extract_number DESC
        OFFSET ${offset} LIMIT ${limit}`
      )

      const jobs: ImportJobRecord[] = results.map((row) => ({
        id: row.id,
        type: row.extract_type === 'full' ? 'monthly' : 'daily',
        status: row.status as 'pending' | 'running' | 'completed' | 'failed',
        startedAt: row.started_at,
        completedAt: row.completed_at,
        extractNumber: row.extract_number,
        snapshotDate: row.snapshot_date,
        recordsProcessed: Number(row.records_processed),
        recordsInserted: Number(row.records_inserted),
        recordsUpdated: Number(row.records_updated),
        recordsDeleted: Number(row.records_deleted),
        errorMessage: row.error_message,
        workerType: row.worker_type,
      }))

      return NextResponse.json({
        jobs,
        total,
        page,
        totalPages,
      })
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error) {
    console.error('Failed to fetch import jobs:', error)
    return NextResponse.json({ error: 'Failed to fetch import jobs' }, { status: 500 })
  }
}
