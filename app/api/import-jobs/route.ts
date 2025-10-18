import { NextResponse } from 'next/server'
import { auth } from '@clerk/nextjs/server'
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

export async function GET() {
  try {
    // Check authentication
    const { userId } = await auth()
    if (!userId) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    // Connect to Motherduck
    const db = await connectMotherduck()

    try {
      const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
      await executeQuery(db, `USE ${dbName}`)

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
        db,
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
        LIMIT 100`
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

      await closeMotherduck(db)

      return NextResponse.json({ jobs })
    } catch (error) {
      await closeMotherduck(db)
      throw error
    }
  } catch (error) {
    console.error('Failed to fetch import jobs:', error)
    return NextResponse.json({ error: 'Failed to fetch import jobs' }, { status: 500 })
  }
}
