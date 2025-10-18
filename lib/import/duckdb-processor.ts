/**
 * DuckDB Processing Utilities
 *
 * Handles DuckDB operations for KBO data import:
 * - CSV staging in local temp tables
 * - SQL transformations
 * - Streaming to Motherduck
 */

import * as duckdb from 'duckdb'
import { join } from 'path'
import { Metadata } from './metadata'
import { TableTransformation, injectMetadata } from './transformations'

/**
 * Import progress callback
 */
export interface ImportProgress {
  table: string
  phase: 'checking' | 'loading' | 'transforming' | 'uploading' | 'complete'
  rowsProcessed?: number
  totalRows?: number
}

/**
 * Import statistics for a single table
 */
export interface ImportStats {
  table: string
  rowsInserted: number
  durationMs: number
}

/**
 * Initialize local DuckDB instance with Motherduck extension
 *
 * @param motherduckToken Motherduck authentication token
 * @param motherduckDatabase Database name to attach
 * @returns Local DuckDB instance with Motherduck attached
 */
export async function initializeDuckDBWithMotherduck(
  motherduckToken: string,
  motherduckDatabase: string
): Promise<duckdb.Database> {
  // Set Motherduck token in environment (required for DuckDB motherduck extension)
  process.env.MOTHERDUCK_TOKEN = motherduckToken

  // Create local DuckDB that can also access Motherduck
  const localDb = new duckdb.Database(':memory:')

  // Install and load Motherduck extension
  await new Promise<void>((resolve, reject) => {
    localDb.exec(
      `INSTALL motherduck;
       LOAD motherduck;
       PRAGMA enable_progress_bar;`,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })

  // Attach Motherduck to local DuckDB instance
  await new Promise<void>((resolve, reject) => {
    localDb.exec(
      `ATTACH 'md:${motherduckDatabase}' AS motherduck;`,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })

  return localDb
}

/**
 * Stage CSV file into local DuckDB temp table
 *
 * @param db DuckDB instance
 * @param csvPath Path to CSV file
 * @param tableName Name for the staged table (will be prefixed with 'staged_')
 */
export async function stageCsvFile(
  db: duckdb.Database,
  csvPath: string,
  tableName: string
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    db.exec(
      `
      DROP TABLE IF EXISTS staged_${tableName};
      CREATE TEMP TABLE staged_${tableName} AS
      SELECT * FROM read_csv('${csvPath}', AUTO_DETECT=TRUE, HEADER=TRUE);
      `,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })
}

/**
 * Create ranked denominations temp table for primary name selection
 * This is required before processing enterprises and establishments
 *
 * @param db DuckDB instance
 */
export async function createRankedDenominations(db: duckdb.Database): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    db.exec(
      `
      CREATE TEMP TABLE ranked_denominations AS
      SELECT
        EntityNumber,
        Language,
        TypeOfDenomination,
        Denomination,
        ROW_NUMBER() OVER (
          PARTITION BY EntityNumber
          ORDER BY
            CASE TypeOfDenomination
              WHEN '001' THEN 1  -- Legal name (highest priority)
              WHEN '003' THEN 2  -- Commercial name
              WHEN '002' THEN 3  -- Abbreviation
              WHEN '004' THEN 4  -- Branch name
              ELSE 5
            END,
            CASE Language
              WHEN '2' THEN 1  -- Dutch (highest priority)
              WHEN '1' THEN 2  -- French
              WHEN '3' THEN 3  -- German
              WHEN '4' THEN 4  -- English
              WHEN '0' THEN 5  -- Unknown
              ELSE 6
            END
        ) as priority_rank
      FROM staged_denominations
      WHERE EntityNumber IN (SELECT EnterpriseNumber FROM staged_enterprises);
      `,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })
}

/**
 * Process a single table: stage CSV (optional), transform, stream to Motherduck
 *
 * @param localDb Local DuckDB instance with Motherduck attached
 * @param dataPath Path to directory containing CSV files
 * @param transformation Table transformation definition
 * @param metadata Import metadata for temporal columns
 * @param onProgress Optional progress callback
 * @returns Import statistics
 */
export async function processTable(
  localDb: duckdb.Database,
  dataPath: string,
  transformation: TableTransformation,
  metadata: Metadata,
  onProgress?: (progress: ImportProgress) => void
): Promise<ImportStats> {
  const startTime = Date.now()
  const { tableName, csvFile, transformSql } = transformation

  // Load CSV into local DuckDB temp table (if path provided)
  if (csvFile) {
    onProgress?.({ table: tableName, phase: 'loading' })

    const csvPath = join(dataPath, csvFile)
    await stageCsvFile(localDb, csvPath, tableName)
  }

  onProgress?.({ table: tableName, phase: 'transforming' })

  // Replace metadata placeholders in SQL
  const finalSql = injectMetadata(transformSql, metadata)

  // Apply transformations
  await new Promise<void>((resolve, reject) => {
    localDb.exec(
      `
      DROP TABLE IF EXISTS transformed_${tableName};
      CREATE TEMP TABLE transformed_${tableName} AS
      ${finalSql}
      `,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })

  // Get row count for progress tracking
  const rowCount = await new Promise<number>((resolve, reject) => {
    localDb.all(
      `SELECT COUNT(*) as count FROM transformed_${tableName}`,
      (err, rows: any[]) => {
        if (err) reject(err)
        else resolve(Number(rows[0].count))  // Convert BigInt to Number
      }
    )
  })

  onProgress?.({
    table: tableName,
    phase: 'uploading',
    rowsProcessed: 0,
    totalRows: rowCount,
  })

  // Direct INSERT SELECT from local to Motherduck (via ATTACH)
  // This is the most efficient way - DuckDB handles the streaming internally
  await new Promise<void>((resolve, reject) => {
    localDb.exec(
      `INSERT INTO motherduck.${tableName} SELECT * FROM transformed_${tableName}`,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })

  const duration = Date.now() - startTime

  onProgress?.({
    table: tableName,
    phase: 'uploading',
    rowsProcessed: rowCount,
    totalRows: rowCount,
  })

  onProgress?.({ table: tableName, phase: 'complete' })

  return {
    table: tableName,
    rowsInserted: rowCount,
    durationMs: duration,
  }
}

/**
 * Stage all required CSV files for full import
 *
 * @param db DuckDB instance
 * @param dataPath Path to directory containing CSV files
 */
export async function stageAllCsvFiles(
  db: duckdb.Database,
  dataPath: string,
  onProgress?: (message: string) => void
): Promise<void> {
  const csvFiles = [
    { name: 'enterprises', file: 'enterprise.csv' },
    { name: 'establishments', file: 'establishment.csv' },
    { name: 'denominations', file: 'denomination.csv' },
    { name: 'addresses', file: 'address.csv' },
    { name: 'activities', file: 'activity.csv' },
    { name: 'contacts', file: 'contact.csv' },
    { name: 'branches', file: 'branch.csv' },
  ]

  for (const { name, file } of csvFiles) {
    onProgress?.(`Loading ${file}...`)
    await stageCsvFile(db, join(dataPath, file), name)
  }
}

/**
 * Mark all current records as historical in all tables
 *
 * @param motherduckDb Motherduck database connection
 * @param onProgress Optional progress callback
 * @returns Total number of records marked historical
 */
export async function markAllCurrentAsHistorical(
  motherduckDb: any,
  onProgress?: (table: string, count: number) => void
): Promise<number> {
  const tables = [
    'enterprises',
    'establishments',
    'denominations',
    'addresses',
    'activities',
    'contacts',
    'branches'
  ]

  let totalMarked = 0

  for (const table of tables) {
    const result = await new Promise<any[]>((resolve, reject) => {
      motherduckDb.all(
        `UPDATE ${table}
         SET _is_current = false
         WHERE _is_current = true`,
        (err: Error | null, rows: any[]) => {
          if (err) reject(err)
          else resolve(rows)
        }
      )
    })

    // DuckDB returns changes_count in the result
    const count = result.length > 0 ? Number(result[0]?.count || 0) : 0
    totalMarked += count
    onProgress?.(table, count)
  }

  return totalMarked
}

/**
 * Clean up snapshots older than retention period
 *
 * @param motherduckDb Motherduck database connection
 * @param retentionMonths Number of months to retain (default: 24)
 * @param onProgress Optional progress callback
 * @returns Total number of records deleted
 */
export async function cleanupOldSnapshots(
  motherduckDb: any,
  retentionMonths: number = 24,
  onProgress?: (table: string, count: number) => void
): Promise<number> {
  const tables = [
    'enterprises',
    'establishments',
    'denominations',
    'addresses',
    'activities',
    'contacts',
    'branches'
  ]

  let totalCleaned = 0

  for (const table of tables) {
    const result = await new Promise<any[]>((resolve, reject) => {
      motherduckDb.all(
        `DELETE FROM ${table}
         WHERE _snapshot_date < CURRENT_DATE - INTERVAL '${retentionMonths} months'`,
        (err: Error | null, rows: any[]) => {
          if (err) reject(err)
          else resolve(rows)
        }
      )
    })

    const count = result.length > 0 ? Number(result[0]?.count || 0) : 0
    totalCleaned += count

    if (count > 0) {
      onProgress?.(table, count)
    }
  }

  return totalCleaned
}
