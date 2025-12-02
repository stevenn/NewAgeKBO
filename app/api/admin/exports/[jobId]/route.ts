import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import type { ExportJob } from '@/lib/export/types'

/**
 * DELETE /api/admin/exports/[jobId]
 * Delete an export job and drop its associated MotherDuck table
 */
export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ jobId: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const { jobId } = await params

    const connection = await connectMotherduck()

    try {
      // Get export job details
      const jobs = await executeQuery<ExportJob>(
        connection,
        `
          SELECT id, status, table_name, export_type
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
      let tableDropped = false

      // Drop the MotherDuck table if it exists
      if (job.table_name) {
        try {
          // Check if table exists
          const tableExists = await executeQuery<{ count: number }>(
            connection,
            `
              SELECT COUNT(*) as count
              FROM information_schema.tables
              WHERE table_name = '${job.table_name}'
            `
          )

          if (tableExists[0] && tableExists[0].count > 0) {
            await connection.run(`DROP TABLE IF EXISTS ${job.table_name}`)
            tableDropped = true
            console.log(`🗑️ Dropped table ${job.table_name}`)
          }
        } catch (dropError) {
          console.error(`Failed to drop table ${job.table_name}:`, dropError)
          // Continue to delete the job record even if table drop fails
        }
      }

      // Delete the job record
      await connection.run(`DELETE FROM export_jobs WHERE id = '${jobId}'`)
      console.log(`🗑️ Deleted export job ${jobId}`)

      return NextResponse.json({
        success: true,
        job_deleted: jobId,
        table_dropped: tableDropped,
        table_name: job.table_name,
      })
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error: unknown) {
    console.error('❌ Delete failed:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Delete failed',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
