/**
 * Cleanup expired export tables
 *
 * Drops MotherDuck tables for exports that have expired (older than 24 hours)
 * and optionally deletes the job records.
 */

import { connectMotherduck, closeMotherduck, executeQuery } from '../motherduck'
import type { ExportJob } from './types'

export interface CleanupResult {
  tables_dropped: number
  jobs_deleted: number
  tables_dropped_names: string[]
  errors: Array<{ table_name: string; error: string }>
}

/**
 * Clean up expired export tables
 *
 * @param deleteJobRecords - If true, also delete job records from export_jobs table
 * @returns Cleanup result with counts and errors
 */
export async function cleanupExpiredExports(
  deleteJobRecords: boolean = false
): Promise<CleanupResult> {
  console.log('🧹 Starting export cleanup...')

  const conn = await connectMotherduck()
  const result: CleanupResult = {
    tables_dropped: 0,
    jobs_deleted: 0,
    tables_dropped_names: [],
    errors: [],
  }

  try {
    // 1. Find expired export jobs
    const expiredJobs = await executeQuery<ExportJob>(
      conn,
      `
        SELECT id, table_name, export_type, expires_at, records_exported
        FROM export_jobs
        WHERE status = 'completed'
          AND expires_at < CURRENT_TIMESTAMP
          AND table_name IS NOT NULL
        ORDER BY expires_at
      `
    )

    if (expiredJobs.length === 0) {
      console.log('✨ No expired exports to clean up')
      return result
    }

    console.log(`📋 Found ${expiredJobs.length} expired export(s) to clean up`)

    // 2. Drop each table
    for (const job of expiredJobs) {
      const tableName = job.table_name as string

      try {
        console.log(`  🗑️  Dropping table ${tableName}...`)

        // Check if table exists before attempting to drop
        const tableExists = await executeQuery<{ count: number }>(
          conn,
          `
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = '${tableName}'
          `
        )

        if (tableExists[0] && tableExists[0].count > 0) {
          await conn.run(`DROP TABLE IF EXISTS ${tableName}`)
          result.tables_dropped++
          result.tables_dropped_names.push(tableName)
          console.log(`  ✅ Dropped ${tableName}`)
        } else {
          console.log(`  ⚠️  Table ${tableName} does not exist (already cleaned?)`)
        }

        // 3. Optionally delete job record
        if (deleteJobRecords) {
          await conn.run(`DELETE FROM export_jobs WHERE id = '${job.id}'`)
          result.jobs_deleted++
        }
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error)
        console.error(`  ❌ Failed to drop ${tableName}: ${errorMessage}`)
        result.errors.push({
          table_name: tableName,
          error: errorMessage,
        })
      }
    }

    console.log(`✅ Cleanup complete: ${result.tables_dropped} table(s) dropped`)

    if (result.errors.length > 0) {
      console.log(`⚠️  ${result.errors.length} error(s) occurred during cleanup`)
    }

    return result
  } finally {
    await closeMotherduck(conn)
  }
}

/**
 * Get count of expired exports waiting for cleanup
 *
 * @returns Number of expired exports
 */
export async function countExpiredExports(): Promise<number> {
  const conn = await connectMotherduck()

  try {
    const result = await executeQuery<{ count: number }>(
      conn,
      `
        SELECT COUNT(*) as count
        FROM export_jobs
        WHERE status = 'completed'
          AND expires_at < CURRENT_TIMESTAMP
          AND table_name IS NOT NULL
      `
    )

    return result[0]?.count || 0
  } finally {
    await closeMotherduck(conn)
  }
}
