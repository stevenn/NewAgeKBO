/**
 * Export VAT-liable entities to MotherDuck table
 *
 * Creates a table in MotherDuck containing entity numbers for VAT-liable entities
 * based on activity groups 001 (VAT), 004 (Government), and 007 (Education).
 *
 * See docs/ACTIVITY_GROUP_ANALYSIS.md for filter rationale.
 */

import { randomUUID } from 'crypto'
import { connectMotherduck, closeMotherduck } from '../motherduck'
import type { ExportVatEntitiesResult, WorkerType, VatEntityRow } from './types'

/**
 * Export VAT-liable entities to a MotherDuck table
 *
 * @param workerType - Worker type for tracking
 * @param userId - User ID from Clerk (optional)
 * @returns Export job result with table name and record count
 */
export async function exportVatEntities(
  workerType: WorkerType = 'web_manual',
  userId?: string
): Promise<ExportVatEntitiesResult> {
  const jobId = randomUUID()
  const timestamp = new Date().toISOString().replace(/[:\-\.]/g, '_').split('T').join('_').substring(0, 19)
  const tableName = `export_vat_entities_${timestamp}`
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000) // 24 hours from now

  const conn = await connectMotherduck()

  try {
    // 1. Create job record
    console.log(`📤 Creating export job ${jobId}...`)
    await conn.run(`
      INSERT INTO export_jobs (
        id, export_type, filter_config, status, started_at,
        table_name, expires_at, worker_type, created_by
      ) VALUES (
        '${jobId}',
        'vat_entities',
        '{"activity_groups": ["001", "004", "007"]}'::JSON,
        'running',
        CURRENT_TIMESTAMP,
        '${tableName}',
        TIMESTAMP '${expiresAt.toISOString()}',
        '${workerType}',
        ${userId ? `'${userId}'` : 'NULL'}
      )
    `)

    // 2. Create MotherDuck table with VAT-liable entities
    console.log(`📊 Creating table ${tableName} with VAT-liable entities...`)

    const createTableQuery = `
      CREATE TABLE ${tableName} AS
      SELECT DISTINCT
        a.entity_number,
        e.juridical_form,
        e.status,
        STRING_AGG(DISTINCT a.activity_group, ',' ORDER BY a.activity_group) as activity_groups
      FROM activities a
      INNER JOIN enterprises e ON a.entity_number = e.enterprise_number
      WHERE a.activity_group IN ('001', '004', '007')
        AND a._is_current = true
        AND e._is_current = true
        AND e.status = 'AC'
      GROUP BY a.entity_number, e.juridical_form, e.status
      ORDER BY a.entity_number
    `

    await conn.run(createTableQuery)

    // 3. Count records
    console.log(`🔢 Counting records...`)
    const countResult = await conn.run(`SELECT COUNT(*) as count FROM ${tableName}`)
    const chunks = await countResult.fetchAllChunks()
    const columnNames = countResult.columnNames()

    let recordCount = 0
    for (const chunk of chunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        const row: Record<string, unknown> = {}
        columnNames.forEach((col, idx) => {
          row[col] = rowArray[idx]
        })
        recordCount = Number(row.count)
      }
    }

    // 4. Update job record with completion
    console.log(`✅ Export completed: ${recordCount.toLocaleString()} records`)
    await conn.run(`
      UPDATE export_jobs
      SET status = 'completed',
          completed_at = CURRENT_TIMESTAMP,
          records_exported = ${recordCount}
      WHERE id = '${jobId}'
    `)

    return {
      job_id: jobId,
      table_name: tableName,
      records_exported: recordCount,
      expires_at: expiresAt.toISOString(),
    }
  } catch (error) {
    // Update job record with failure
    console.error('❌ Export failed:', error)
    const errorMessage = error instanceof Error ? error.message : String(error)

    try {
      await conn.run(`
        UPDATE export_jobs
        SET status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            error_message = '${errorMessage.replace(/'/g, "''")}'
        WHERE id = '${jobId}'
      `)
    } catch (updateError) {
      console.error('Failed to update job status:', updateError)
    }

    throw error
  } finally {
    await closeMotherduck(conn)
  }
}

/**
 * Get table name for an export job
 *
 * @param jobId - Export job ID
 * @returns Table name or null if not found
 */
export async function getExportTableName(jobId: string): Promise<string | null> {
  const conn = await connectMotherduck()

  try {
    const result = await conn.run(`
      SELECT table_name
      FROM export_jobs
      WHERE id = '${jobId}' AND status = 'completed'
    `)

    const chunks = await result.fetchAllChunks()
    const columnNames = result.columnNames()

    for (const chunk of chunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        const row: Record<string, unknown> = {}
        columnNames.forEach((col, idx) => {
          row[col] = rowArray[idx]
        })
        return row.table_name as string
      }
    }

    return null
  } finally {
    await closeMotherduck(conn)
  }
}

/**
 * Check if export table exists
 *
 * @param tableName - Table name to check
 * @returns true if table exists
 */
export async function exportTableExists(tableName: string): Promise<boolean> {
  const conn = await connectMotherduck()

  try {
    const result = await conn.run(`
      SELECT COUNT(*) as count
      FROM information_schema.tables
      WHERE table_name = '${tableName}'
    `)

    const chunks = await result.fetchAllChunks()
    const columnNames = result.columnNames()

    for (const chunk of chunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        const row: Record<string, unknown> = {}
        columnNames.forEach((col, idx) => {
          row[col] = rowArray[idx]
        })
        return Number(row.count) > 0
      }
    }

    return false
  } finally {
    await closeMotherduck(conn)
  }
}
