/**
 * Export entity denominations with activity groups to MotherDuck table
 *
 * Creates a table in MotherDuck containing denominations for all active enterprises
 * with 7 boolean activity group columns (ag_001 through ag_007).
 * Output format: EntityNumber, Language, TypeOfDenomination, Denomination, ag_001, ..., ag_007
 *
 * See docs/ACTIVITY_GROUP_ANALYSIS.md for activity group definitions.
 */

import { randomUUID } from 'crypto'
import { connectMotherduck, closeMotherduck } from '../motherduck'
import type { ExportVatEntitiesResult, WorkerType } from './types'

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
        'all_entities',
        '{"activity_groups": "all", "schema_version": "v2"}'::JSON,
        'running',
        CURRENT_TIMESTAMP,
        '${tableName}',
        TIMESTAMP '${expiresAt.toISOString()}',
        '${workerType}',
        ${userId ? `'${userId}'` : 'NULL'}
      )
    `)

    // 2. Create MotherDuck table with denominations and activity group flags
    console.log(`📊 Creating table ${tableName} with entity denominations and activity groups...`)

    // Query includes ALL active enterprises with ONE denomination per enterprise
    // Denomination priority: 001 (legal name) > 002 (commercial) > others, then NL > FR > DE > EN
    // Activity groups are computed from both direct enterprise activities
    // and activities via establishments (LEFT JOIN so enterprises without activities are included)
    const createTableQuery = `
      CREATE TABLE ${tableName} AS
      SELECT
        ranked.entity_number as "EntityNumber",
        ranked.denomination as "Denomination",
        COALESCE(ag.ag_001, false) as "ag_001",
        COALESCE(ag.ag_002, false) as "ag_002",
        COALESCE(ag.ag_003, false) as "ag_003",
        COALESCE(ag.ag_004, false) as "ag_004",
        COALESCE(ag.ag_005, false) as "ag_005",
        COALESCE(ag.ag_006, false) as "ag_006",
        COALESCE(ag.ag_007, false) as "ag_007"
      FROM (
        -- Pick one denomination per enterprise: prefer legal name (001), then NL language
        SELECT
          d.entity_number,
          d.denomination,
          ROW_NUMBER() OVER (
            PARTITION BY d.entity_number
            ORDER BY
              CASE d.denomination_type WHEN '001' THEN 1 WHEN '002' THEN 2 ELSE 3 END,
              CASE d.language WHEN 'NL' THEN 1 WHEN 'FR' THEN 2 WHEN 'DE' THEN 3 WHEN 'EN' THEN 4 ELSE 5 END
          ) as rn
        FROM denominations d
        INNER JOIN enterprises e ON d.entity_number = e.enterprise_number
        WHERE d._is_current = true
          AND d.entity_type = 'enterprise'
          AND e._is_current = true
          AND e.status = 'AC'
      ) ranked
      LEFT JOIN (
        -- Aggregate activity groups from both enterprise and establishment activities
        SELECT
          enterprise_number,
          MAX(ag_001) as ag_001,
          MAX(ag_002) as ag_002,
          MAX(ag_003) as ag_003,
          MAX(ag_004) as ag_004,
          MAX(ag_005) as ag_005,
          MAX(ag_006) as ag_006,
          MAX(ag_007) as ag_007
        FROM (
          -- Direct enterprise activities
          SELECT
            a.entity_number as enterprise_number,
            MAX(CASE WHEN a.activity_group = '001' THEN true ELSE false END) as ag_001,
            MAX(CASE WHEN a.activity_group = '002' THEN true ELSE false END) as ag_002,
            MAX(CASE WHEN a.activity_group = '003' THEN true ELSE false END) as ag_003,
            MAX(CASE WHEN a.activity_group = '004' THEN true ELSE false END) as ag_004,
            MAX(CASE WHEN a.activity_group = '005' THEN true ELSE false END) as ag_005,
            MAX(CASE WHEN a.activity_group = '006' THEN true ELSE false END) as ag_006,
            MAX(CASE WHEN a.activity_group = '007' THEN true ELSE false END) as ag_007
          FROM activities a
          WHERE a._is_current = true AND a.entity_type = 'enterprise'
          GROUP BY a.entity_number

          UNION ALL

          -- Activities via establishments
          SELECT
            est.enterprise_number,
            MAX(CASE WHEN a.activity_group = '001' THEN true ELSE false END) as ag_001,
            MAX(CASE WHEN a.activity_group = '002' THEN true ELSE false END) as ag_002,
            MAX(CASE WHEN a.activity_group = '003' THEN true ELSE false END) as ag_003,
            MAX(CASE WHEN a.activity_group = '004' THEN true ELSE false END) as ag_004,
            MAX(CASE WHEN a.activity_group = '005' THEN true ELSE false END) as ag_005,
            MAX(CASE WHEN a.activity_group = '006' THEN true ELSE false END) as ag_006,
            MAX(CASE WHEN a.activity_group = '007' THEN true ELSE false END) as ag_007
          FROM activities a
          INNER JOIN establishments est ON a.entity_number = est.establishment_number
          WHERE a._is_current = true AND a.entity_type = 'establishment' AND est._is_current = true
          GROUP BY est.enterprise_number
        ) combined
        GROUP BY enterprise_number
      ) ag ON ranked.entity_number = ag.enterprise_number
      WHERE ranked.rn = 1
      ORDER BY "EntityNumber"
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
