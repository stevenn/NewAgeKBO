/**
 * Batched Import System - Core Library
 *
 * Implements micro-batch processing for KBO imports to avoid Vercel timeouts.
 * Uses staging tables and batch tracking for resumable, progress-trackable imports.
 *
 * Key functions:
 * - prepareImport(): Parse ZIP, populate staging tables, create batches
 * - processBatch(): Execute single batch (delete or insert)
 * - getImportProgress(): Query batch status
 * - finalizeImport(): Resolve names, cleanup staging data
 */

import StreamZip from 'node-stream-zip'
import { parse } from 'csv-parse/sync'
import { createHash, randomUUID } from 'crypto'
import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../motherduck'
import {
  csvColumnToDbColumn,
  csvTableToDbTable,
  computeEntityType,
  convertKboDateFormat,
  isKboDateFormat
} from '../utils/column-mapping'
import { WorkerType, ImportJobType } from '../types/import-job'
import { Metadata } from './metadata'
import { tmpdir } from 'os'
import { writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'

// ============================================================================
// TYPE DEFINITIONS
// ============================================================================

/**
 * Batch size configuration per table
 * Conservative sizes for large tables, larger for smaller tables
 */
const BATCH_SIZES: Record<string, number> = {
  activities: 500,      // Large table, conservative batch size
  addresses: 1000,
  contacts: 1000,
  denominations: 1000,
  enterprises: 2000,    // Smaller table, larger batches ok
  establishments: 2000,
  branches: 1000,
}

/**
 * Result from prepareImport() - job metadata and batch counts
 */
export interface PrepareImportResult {
  job_id: string
  extract_number: number
  snapshot_date: string
  total_batches: number
  batches_by_table: Record<string, { delete: number; insert: number }>
}

/**
 * Result from processBatch() - batch completion status and progress
 */
export interface ProcessBatchResult {
  batch_completed: true
  table_name: string
  batch_number: number
  operation: 'delete' | 'insert'
  records_processed: number
  progress: {
    completed_batches: number
    total_batches: number
    percentage: number
  }
  next_batch: {
    table_name: string
    batch_number: number
    operation: 'delete' | 'insert'
  } | null
}

/**
 * Batch status for a single table
 */
export interface TableBatchStatus {
  completed: number
  total: number
  status: 'pending' | 'processing' | 'completed'
}

/**
 * Overall import progress across all tables
 */
export interface ImportProgress {
  job_id: string
  status: 'pending' | 'processing' | 'completed' | 'failed'
  overall_progress: {
    completed_batches: number
    total_batches: number
    percentage: number
  }
  tables: Record<string, TableBatchStatus>
  current_batch: {
    table: string
    batch: number
    operation: string
  } | null
  next_batch: {
    table: string
    batch: number
    operation: string
  } | null
}

/**
 * Result from finalizeImport() - completion status
 */
export interface FinalizeResult {
  success: boolean
  names_resolved: number
  staging_cleaned: boolean
}

/**
 * Raw metadata from meta.csv (PascalCase, string types)
 */
interface RawMetadata {
  SnapshotDate: string
  ExtractTimestamp: string
  ExtractType: string
  ExtractNumber: string
  Version: string
}

interface MetaRecord {
  Variable?: string
  Value?: string
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Generate a short hash for a string (8 characters)
 * Used to create unique IDs for denominations
 */
function shortHash(text: string): string {
  return createHash('sha256').update(text).digest('hex').substring(0, 8)
}

/**
 * Parse metadata from meta.csv in ZIP
 */
async function parseMetadata(zip: StreamZip.StreamZipAsync): Promise<RawMetadata> {
  const metaContent = await zip.entryData('meta.csv')
  const metaRecords = parse(metaContent.toString(), {
    columns: true,
    skip_empty_lines: true
  }) as MetaRecord[]

  return {
    SnapshotDate: metaRecords.find((r) => r.Variable === 'SnapshotDate')?.Value || metaRecords[0]?.Value || '',
    ExtractTimestamp: metaRecords.find((r) => r.Variable === 'ExtractTimestamp')?.Value || metaRecords[1]?.Value || '',
    ExtractType: metaRecords.find((r) => r.Variable === 'ExtractType')?.Value || metaRecords[2]?.Value || '',
    ExtractNumber: metaRecords.find((r) => r.Variable === 'ExtractNumber')?.Value || metaRecords[3]?.Value || '',
    Version: metaRecords.find((r) => r.Variable === 'Version')?.Value || metaRecords[4]?.Value || ''
  }
}

/**
 * Convert raw CSV metadata to standardized Metadata format
 */
function convertMetadata(raw: RawMetadata): Metadata {
  const snapshotDate = convertKboDateFormat(raw.SnapshotDate)

  const timestampParts = raw.ExtractTimestamp.split(' ')
  if (timestampParts.length !== 2) {
    throw new Error(`Invalid ExtractTimestamp format: ${raw.ExtractTimestamp}`)
  }
  const [datePart, timePart] = timestampParts
  const extractTimestamp = `${convertKboDateFormat(datePart)} ${timePart}`

  const extractNumber = parseInt(raw.ExtractNumber, 10)
  if (isNaN(extractNumber)) {
    throw new Error(`Invalid ExtractNumber: ${raw.ExtractNumber}`)
  }

  return {
    snapshotDate,
    extractNumber,
    extractType: raw.ExtractType as ImportJobType,
    version: raw.Version,
    extractTimestamp
  }
}

/**
 * Get batch size for a table (with fallback for small files)
 */
function getBatchSize(tableName: string, totalRecords: number): number {
  const configuredSize = BATCH_SIZES[tableName] || 1000

  // For small files (< 5000 records total), use 1 batch
  if (totalRecords < 5000) {
    return totalRecords
  }

  return configuredSize
}

/**
 * Calculate number of batches needed for a record set
 */
function calculateBatchCount(recordCount: number, batchSize: number): number {
  if (recordCount === 0) return 0
  return Math.ceil(recordCount / batchSize)
}

/**
 * Populate a staging table with CSV records, assigning batch numbers
 */
async function populateStagingTable(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  stagingTableName: string,
  dbTableName: string,
  records: Record<string, string>[],
  operation: 'delete' | 'insert',
  jobId: string,
  batchSize: number,
  metadata: Metadata
): Promise<void> {
  if (records.length === 0) return

  const csvColumns = Object.keys(records[0])
  const dbColumns = csvColumns.map(col => csvColumnToDbColumn(col))

  // Build column list for staging table (without entity_type - computed later)
  const stagingColumns = ['job_id', 'batch_number', 'operation', ...dbColumns]

  // Build VALUES for all records
  const values: string[] = []

  for (let i = 0; i < records.length; i++) {
    const record = records[i]
    const batchNumber = Math.floor(i / batchSize) + 1

    const recordValues = csvColumns.map(col => {
      const val = record[col]
      if (val === '' || val === null) {
        return 'NULL'
      }

      // Check if this looks like a date (DD-MM-YYYY format)
      if (col.toLowerCase().includes('date') && isKboDateFormat(val)) {
        const converted = convertKboDateFormat(val)
        return `'${converted}'`
      }

      // SECURITY: Escape single quotes for SQL string literals
      return `'${val.replace(/'/g, "''")}'`
    })

    values.push(`('${jobId}', ${batchNumber}, '${operation}', ${recordValues.join(',')})`)
  }

  const sql = `
    INSERT INTO ${stagingTableName} (${stagingColumns.join(', ')})
    VALUES ${values.join(',\n      ')}
  `

  await executeStatement(db, sql)
}

/**
 * Create batch tracking records in import_job_batches
 */
async function createBatchRecords(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  jobId: string,
  tableName: string,
  operation: 'delete' | 'insert',
  batchCount: number,
  totalRecords: number
): Promise<void> {
  if (batchCount === 0) return

  const recordsPerBatch = Math.ceil(totalRecords / batchCount)
  const values: string[] = []

  for (let i = 1; i <= batchCount; i++) {
    // Calculate records for this batch (last batch might be smaller)
    const recordsInBatch = (i === batchCount)
      ? totalRecords - (recordsPerBatch * (batchCount - 1))
      : recordsPerBatch

    values.push(
      `('${jobId}', '${tableName}', ${i}, '${operation}', 'pending', ${recordsInBatch}, NULL, NULL, NULL)`
    )
  }

  const sql = `
    INSERT INTO import_job_batches (
      job_id, table_name, batch_number, operation, status, records_count,
      started_at, completed_at, error_message
    )
    VALUES ${values.join(',\n      ')}
  `

  await executeStatement(db, sql)
}

// ============================================================================
// MAIN FUNCTIONS
// ============================================================================

/**
 * Step 1: Prepare Import
 *
 * Extracts and parses the ZIP file, populates staging tables with data,
 * creates batch records for processing, and returns job metadata.
 *
 * @param zipBuffer - Buffer containing the KBO update ZIP file
 * @param workerType - Type of worker (local, vercel, etc.)
 * @returns Job ID and batch information
 */
export async function prepareImport(
  zipBuffer: Buffer,
  workerType: WorkerType = 'local'
): Promise<PrepareImportResult> {
  // Write buffer to temporary file (node-stream-zip requires a file path)
  const tempFilePath = join(tmpdir(), `kbo-update-${randomUUID()}.zip`)
  writeFileSync(tempFilePath, zipBuffer)

  let zip: StreamZip.StreamZipAsync | null = null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let db: any = null
  const jobId = randomUUID()

  try {
    zip = new StreamZip.async({ file: tempFilePath })
    db = await connectMotherduck()

    // Step 1: Parse metadata
    console.log('📋 Parsing metadata...')
    const rawMetadata = await parseMetadata(zip)
    const metadata = convertMetadata(rawMetadata)
    console.log(`   ✓ Snapshot Date: ${metadata.snapshotDate}`)
    console.log(`   ✓ Extract Number: ${metadata.extractNumber}`)
    console.log(`   ✓ Extract Type: ${metadata.extractType}`)

    if (metadata.extractType !== 'update') {
      throw new Error(`Expected 'update' extract type, got '${metadata.extractType}'`)
    }

    // Step 2: Create import job record
    console.log('\n📝 Creating import job record...')
    const jobStartTime = new Date().toISOString()

    await executeStatement(db, `
      INSERT INTO import_jobs (
        id, extract_number, extract_type, snapshot_date, extract_timestamp,
        status, started_at, worker_type
      ) VALUES (
        '${jobId}',
        ${metadata.extractNumber},
        'update',
        '${metadata.snapshotDate}',
        '${metadata.extractTimestamp}',
        'pending',
        '${jobStartTime}',
        '${workerType}'
      )
    `)
    console.log(`   ✓ Job ID: ${jobId}`)

    // Step 3: Get list of tables to process
    const entries = await zip.entries()
    const tables = new Set<string>()

    for (const name of Object.keys(entries)) {
      if (name.endsWith('_delete.csv') || name.endsWith('_insert.csv')) {
        const tableName = name.replace('_delete.csv', '').replace('_insert.csv', '')
        tables.add(tableName)
      }
    }

    const tableList = Array.from(tables).sort()
    console.log(`\n📊 Tables to process: ${tableList.join(', ')}\n`)

    // Step 4: Process each table and populate staging
    const batchesByTable: Record<string, { delete: number; insert: number }> = {}
    let totalBatches = 0

    for (const csvTableName of tableList) {
      const dbTableName = csvTableToDbTable(csvTableName)
      const stagingTableName = `import_staging_${dbTableName}`

      console.log(`🔄 Preparing ${dbTableName}...`)

      batchesByTable[dbTableName] = { delete: 0, insert: 0 }

      // Process DELETE file
      const deleteFileName = `${csvTableName}_delete.csv`
      try {
        const deleteContent = await zip.entryData(deleteFileName)
        const deleteRecords = parse(deleteContent.toString(), {
          columns: true,
          skip_empty_lines: true
        }) as Record<string, string>[]

        if (deleteRecords.length > 0) {
          const batchSize = getBatchSize(dbTableName, deleteRecords.length)
          const batchCount = calculateBatchCount(deleteRecords.length, batchSize)
          batchesByTable[dbTableName].delete = batchCount
          totalBatches += batchCount

          await populateStagingTable(
            db, stagingTableName, dbTableName, deleteRecords,
            'delete', jobId, batchSize, metadata
          )

          await createBatchRecords(db, jobId, dbTableName, 'delete', batchCount, deleteRecords.length)

          console.log(`   ✓ Delete: ${deleteRecords.length} records → ${batchCount} batches`)
        }
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : ''
        if (!errorMessage.includes('Entry not found')) {
          throw error
        }
      }

      // Process INSERT file
      const insertFileName = `${csvTableName}_insert.csv`
      try {
        const insertContent = await zip.entryData(insertFileName)
        const insertRecords = parse(insertContent.toString(), {
          columns: true,
          skip_empty_lines: true
        }) as Record<string, string>[]

        if (insertRecords.length > 0) {
          const batchSize = getBatchSize(dbTableName, insertRecords.length)
          const batchCount = calculateBatchCount(insertRecords.length, batchSize)
          batchesByTable[dbTableName].insert = batchCount
          totalBatches += batchCount

          await populateStagingTable(
            db, stagingTableName, dbTableName, insertRecords,
            'insert', jobId, batchSize, metadata
          )

          await createBatchRecords(db, jobId, dbTableName, 'insert', batchCount, insertRecords.length)

          console.log(`   ✓ Insert: ${insertRecords.length} records → ${batchCount} batches`)
        }
      } catch (error: unknown) {
        const errorMessage = error instanceof Error ? error.message : ''
        if (!errorMessage.includes('Entry not found')) {
          throw error
        }
      }
    }

    console.log(`\n✨ Preparation complete!`)
    console.log(`   • Total batches: ${totalBatches}`)

    return {
      job_id: jobId,
      extract_number: metadata.extractNumber,
      snapshot_date: metadata.snapshotDate,
      total_batches: totalBatches,
      batches_by_table: batchesByTable
    }

  } finally {
    if (zip) {
      await zip.close()
    }
    if (db) {
      await closeMotherduck(db)
    }
    // Clean up temporary file
    try {
      unlinkSync(tempFilePath)
    } catch {
      // Ignore cleanup errors
    }
  }
}

/**
 * Execute batch DELETE operation (mark records as historical)
 */
async function executeBatchDelete(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  tableName: string,
  stagingTableName: string,
  jobId: string,
  batchNumber: number,
  extractNumber: number
): Promise<number> {
  // Get primary key column name for this table
  const pkColumn = tableName === 'enterprises' ? 'enterprise_number' :
                   tableName === 'establishments' ? 'establishment_number' :
                   tableName === 'branches' ? 'id' :
                   'entity_number'

  const sql = `
    UPDATE ${tableName}
    SET _is_current = false,
        _deleted_at_extract = ${extractNumber}
    WHERE ${pkColumn} IN (
      SELECT ${pkColumn}
      FROM ${stagingTableName}
      WHERE job_id = '${jobId}'
        AND operation = 'delete'
        AND batch_number = ${batchNumber}
        AND processed = false
    )
    AND _is_current = true
  `

  await executeStatement(db, sql)

  // Mark staging records as processed
  await executeStatement(db, `
    UPDATE ${stagingTableName}
    SET processed = true
    WHERE job_id = '${jobId}'
      AND operation = 'delete'
      AND batch_number = ${batchNumber}
  `)

  // Count how many were marked as historical
  const result = await executeQuery<{ count: number }>(db, `
    SELECT COUNT(*) as count
    FROM ${stagingTableName}
    WHERE job_id = '${jobId}'
      AND operation = 'delete'
      AND batch_number = ${batchNumber}
      AND processed = true
  `)

  return result[0]?.count || 0
}

/**
 * Execute batch INSERT operation
 */
async function executeBatchInsert(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  tableName: string,
  stagingTableName: string,
  jobId: string,
  batchNumber: number,
  snapshotDate: string,
  extractNumber: number
): Promise<number> {
  const needsEntityType = ['activities', 'addresses', 'contacts', 'denominations'].includes(tableName)
  const needsComputedId = ['activities', 'addresses', 'contacts', 'denominations'].includes(tableName)

  // Build INSERT SQL based on table type
  let sql = ''

  if (tableName === 'enterprises') {
    sql = buildEnterpriseInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  } else if (tableName === 'establishments') {
    sql = buildEstablishmentInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  } else if (tableName === 'branches') {
    sql = buildBranchInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  } else if (tableName === 'activities') {
    sql = buildActivityInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  } else if (tableName === 'addresses') {
    sql = buildAddressInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  } else if (tableName === 'contacts') {
    sql = buildContactInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  } else if (tableName === 'denominations') {
    sql = buildDenominationInsert(stagingTableName, jobId, batchNumber, snapshotDate, extractNumber)
  }

  await executeStatement(db, sql)

  // Mark staging records as processed
  await executeStatement(db, `
    UPDATE ${stagingTableName}
    SET processed = true
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batchNumber}
  `)

  // Count how many were inserted
  const result = await executeQuery<{ count: number }>(db, `
    SELECT COUNT(*) as count
    FROM ${stagingTableName}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batchNumber}
      AND processed = true
  `)

  return result[0]?.count || 0
}

/**
 * Calculate overall progress for a job
 */
async function calculateProgress(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  jobId: string
): Promise<{ completed: number; total: number }> {
  const result = await executeQuery<{ completed: number; total: number }>(db, `
    SELECT
      SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
      COUNT(*) as total
    FROM import_job_batches
    WHERE job_id = '${jobId}'
  `)

  return result[0] || { completed: 0, total: 0 }
}

// Build INSERT SQL for specific table types (reused from daily-update.ts logic)

function buildEnterpriseInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO enterprises (
      enterprise_number, status, juridical_situation, type_of_enterprise,
      juridical_form, juridical_form_cac, start_date,
      primary_name, primary_name_language, primary_name_nl, primary_name_fr, primary_name_de,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      enterprise_number, status, juridical_situation, type_of_enterprise,
      juridical_form, juridical_form_cac, start_date,
      enterprise_number as primary_name, NULL as primary_name_language,
      NULL as primary_name_nl, NULL as primary_name_fr, NULL as primary_name_de,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

function buildEstablishmentInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO establishments (
      establishment_number, enterprise_number, start_date,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      establishment_number, enterprise_number, start_date,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

function buildBranchInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO branches (
      id, enterprise_number, start_date,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      id, enterprise_number, start_date,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

function buildActivityInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO activities (
      id, entity_number, entity_type, activity_group, nace_version, nace_code, classification,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      entity_number || '_' || activity_group || '_' || nace_version || '_' || nace_code || '_' || classification as id,
      entity_number,
      CASE
        WHEN SUBSTRING(entity_number, 2, 1) = '.' THEN 'establishment'
        ELSE 'enterprise'
      END as entity_type,
      activity_group, nace_version, nace_code, classification,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

function buildAddressInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO addresses (
      id, entity_number, entity_type, type_of_address,
      country_nl, country_fr, zipcode, municipality_nl, municipality_fr,
      street_nl, street_fr, house_number, box, extra_address_info, date_striking_off,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      entity_number || '_' || type_of_address as id,
      entity_number,
      CASE
        WHEN SUBSTRING(entity_number, 2, 1) = '.' THEN 'establishment'
        ELSE 'enterprise'
      END as entity_type,
      type_of_address, country_nl, country_fr, zipcode, municipality_nl, municipality_fr,
      street_nl, street_fr, house_number, box, extra_address_info, date_striking_off,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

function buildContactInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO contacts (
      id, entity_number, entity_type, entity_contact, contact_type, contact_value,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      entity_number || '_' || entity_contact || '_' || contact_type || '_' || SUBSTRING(MD5(contact_value), 1, 8) as id,
      entity_number,
      CASE
        WHEN SUBSTRING(entity_number, 2, 1) = '.' THEN 'establishment'
        ELSE 'enterprise'
      END as entity_type,
      entity_contact, contact_type, contact_value,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

function buildDenominationInsert(stagingTable: string, jobId: string, batch: number, snapshotDate: string, extractNumber: number): string {
  return `
    INSERT INTO denominations (
      id, entity_number, entity_type, denomination_type, language, denomination,
      _snapshot_date, _extract_number, _is_current
    )
    SELECT
      entity_number || '_' || denomination_type || '_' || language || '_' || SUBSTRING(MD5(denomination), 1, 8) as id,
      entity_number,
      CASE
        WHEN SUBSTRING(entity_number, 2, 1) = '.' THEN 'establishment'
        ELSE 'enterprise'
      END as entity_type,
      denomination_type, language, denomination,
      '${snapshotDate}'::DATE, ${extractNumber}, true
    FROM ${stagingTable}
    WHERE job_id = '${jobId}'
      AND operation = 'insert'
      AND batch_number = ${batch}
      AND processed = false
  `
}

/**
 * Step 2: Process Batch
 *
 * Executes a single batch (either delete or insert operation) from staging tables
 * into the final tables. Marks batch as completed and returns progress.
 *
 * @param jobId - Import job ID
 * @param tableName - Optional: specific table to process
 * @param batchNumber - Optional: specific batch number to process
 * @returns Batch completion status and progress info
 */
export async function processBatch(
  jobId: string,
  tableName?: string,
  batchNumber?: number
): Promise<ProcessBatchResult> {
  const db = await connectMotherduck()

  try {
    // Step 1: Find the batch to process
    let batch: any
    if (tableName && batchNumber !== undefined) {
      // Process specific batch
      const batches = await executeQuery(db, `
        SELECT * FROM import_job_batches
        WHERE job_id = '${jobId}'
          AND table_name = '${tableName}'
          AND batch_number = ${batchNumber}
        LIMIT 1
      `)
      batch = batches[0]
    } else {
      // Find next pending batch
      const batches = await executeQuery(db, `
        SELECT * FROM import_job_batches
        WHERE job_id = '${jobId}'
          AND status = 'pending'
        ORDER BY table_name, batch_number, operation
        LIMIT 1
      `)
      batch = batches[0]
    }

    if (!batch) {
      throw new Error('No pending batch found')
    }

    // Step 2: Mark batch as processing
    await executeStatement(db, `
      UPDATE import_job_batches
      SET status = 'processing',
          started_at = '${new Date().toISOString()}'
      WHERE job_id = '${jobId}'
        AND table_name = '${batch.table_name}'
        AND batch_number = ${batch.batch_number}
        AND operation = '${batch.operation}'
    `)

    // Step 3: Get job metadata for snapshot_date and extract_number
    const jobs = await executeQuery(db, `
      SELECT snapshot_date, extract_number
      FROM import_jobs
      WHERE id = '${jobId}'
    `)
    const job = jobs[0]

    // Step 4: Execute the batch operation
    const stagingTableName = `import_staging_${batch.table_name}`
    let recordsProcessed = 0

    if (batch.operation === 'delete') {
      recordsProcessed = await executeBatchDelete(
        db, batch.table_name, stagingTableName, jobId,
        batch.batch_number, job.extract_number
      )
    } else {
      recordsProcessed = await executeBatchInsert(
        db, batch.table_name, stagingTableName, jobId,
        batch.batch_number, job.snapshot_date, job.extract_number
      )
    }

    // Step 5: Mark batch as completed
    await executeStatement(db, `
      UPDATE import_job_batches
      SET status = 'completed',
          completed_at = '${new Date().toISOString()}'
      WHERE job_id = '${jobId}'
        AND table_name = '${batch.table_name}'
        AND batch_number = ${batch.batch_number}
        AND operation = '${batch.operation}'
    `)

    // Step 6: Calculate progress
    const progress = await calculateProgress(db, jobId)

    // Step 7: Find next batch
    const nextBatches = await executeQuery(db, `
      SELECT table_name, batch_number, operation
      FROM import_job_batches
      WHERE job_id = '${jobId}'
        AND status = 'pending'
      ORDER BY table_name, batch_number, operation
      LIMIT 1
    `)
    const nextBatch = nextBatches[0] || null

    return {
      batch_completed: true,
      table_name: batch.table_name,
      batch_number: batch.batch_number,
      operation: batch.operation,
      records_processed: recordsProcessed,
      progress: {
        completed_batches: progress.completed,
        total_batches: progress.total,
        percentage: Math.round((progress.completed / progress.total) * 100)
      },
      next_batch: nextBatch ? {
        table_name: nextBatch.table_name,
        batch_number: nextBatch.batch_number,
        operation: nextBatch.operation
      } : null
    }

  } finally {
    await closeMotherduck(db)
  }
}

/**
 * Step 3: Get Import Progress
 *
 * Queries the current status of all batches for an import job,
 * calculates overall progress percentage, and identifies the next batch to process.
 *
 * @param jobId - Import job ID
 * @returns Detailed progress information
 */
export async function getImportProgress(
  jobId: string
): Promise<ImportProgress> {
  const db = await connectMotherduck()

  try {
    // Get job status
    const jobs = await executeQuery(db, `
      SELECT status FROM import_jobs WHERE id = '${jobId}'
    `)
    const jobStatus = jobs[0]?.status || 'pending'

    // Get batch statistics by table
    const batchStats = await executeQuery<{
      table_name: string;
      total: number;
      completed: number;
      processing: number;
      failed: number;
    }>(db, `
      SELECT
        table_name,
        COUNT(*) as total,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
        SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) as processing,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
      FROM import_job_batches
      WHERE job_id = '${jobId}'
      GROUP BY table_name
      ORDER BY table_name
    `)

    // Calculate overall progress
    const totalBatches = batchStats.reduce((sum, s) => sum + s.total, 0)
    const completedBatches = batchStats.reduce((sum, s) => sum + s.completed, 0)
    const percentage = totalBatches > 0 ? Math.round((completedBatches / totalBatches) * 100) : 0

    // Build table status map
    const tables: Record<string, TableBatchStatus> = {}
    for (const stat of batchStats) {
      const tableStatus: 'pending' | 'processing' | 'completed' =
        stat.completed === stat.total ? 'completed' :
        stat.processing > 0 ? 'processing' :
        'pending'

      tables[stat.table_name] = {
        completed: stat.completed,
        total: stat.total,
        status: tableStatus
      }
    }

    // Find current processing batch
    const currentBatches = await executeQuery<{
      table_name: string;
      batch_number: number;
      operation: string;
    }>(db, `
      SELECT table_name, batch_number, operation
      FROM import_job_batches
      WHERE job_id = '${jobId}'
        AND status = 'processing'
      ORDER BY started_at DESC
      LIMIT 1
    `)
    const currentBatch = currentBatches[0] || null

    // Find next pending batch
    const nextBatches = await executeQuery<{
      table_name: string;
      batch_number: number;
      operation: string;
    }>(db, `
      SELECT table_name, batch_number, operation
      FROM import_job_batches
      WHERE job_id = '${jobId}'
        AND status = 'pending'
      ORDER BY table_name, batch_number, operation
      LIMIT 1
    `)
    const nextBatch = nextBatches[0] || null

    return {
      job_id: jobId,
      status: jobStatus,
      overall_progress: {
        completed_batches: completedBatches,
        total_batches: totalBatches,
        percentage
      },
      tables,
      current_batch: currentBatch ? {
        table: currentBatch.table_name,
        batch: currentBatch.batch_number,
        operation: currentBatch.operation
      } : null,
      next_batch: nextBatch ? {
        table: nextBatch.table_name,
        batch: nextBatch.batch_number,
        operation: nextBatch.operation
      } : null
    }

  } finally {
    await closeMotherduck(db)
  }
}

/**
 * Resolve primary names for enterprises
 * Updates enterprises where primary_name is the enterprise number (temporary placeholder)
 * to use actual names from the denominations table
 */
async function resolvePrimaryNames(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  snapshotDate: string,
  extractNumber: number
): Promise<number> {
  const sql = `
    UPDATE enterprises e
    SET
      primary_name = COALESCE(
        d.denomination_nl,
        d.denomination_fr,
        d.denomination_unknown,
        d.denomination_de,
        d.denomination_en,
        e.enterprise_number
      ),
      primary_name_language = COALESCE(
        CASE WHEN d.denomination_nl IS NOT NULL THEN '2' END,
        CASE WHEN d.denomination_fr IS NOT NULL THEN '1' END,
        CASE WHEN d.denomination_unknown IS NOT NULL THEN '0' END,
        CASE WHEN d.denomination_de IS NOT NULL THEN '3' END,
        CASE WHEN d.denomination_en IS NOT NULL THEN '4' END,
        NULL
      ),
      primary_name_nl = d.denomination_nl,
      primary_name_fr = d.denomination_fr,
      primary_name_de = d.denomination_de
    FROM (
      SELECT
        entity_number,
        MAX(CASE WHEN language = '2' AND denomination_type = '001' THEN denomination END) as denomination_nl,
        MAX(CASE WHEN language = '1' AND denomination_type = '001' THEN denomination END) as denomination_fr,
        MAX(CASE WHEN language = '0' AND denomination_type = '001' THEN denomination END) as denomination_unknown,
        MAX(CASE WHEN language = '3' AND denomination_type = '001' THEN denomination END) as denomination_de,
        MAX(CASE WHEN language = '4' AND denomination_type = '001' THEN denomination END) as denomination_en
      FROM denominations
      WHERE _is_current = true
        AND entity_type = 'enterprise'
        AND denomination_type = '001'
      GROUP BY entity_number
    ) d
    WHERE e.enterprise_number = d.entity_number
      AND e._snapshot_date = '${snapshotDate}'
      AND e._extract_number = ${extractNumber}
      AND e._is_current = true
      AND e.primary_name = e.enterprise_number
  `

  await executeStatement(db, sql)

  // Count how many were updated
  const result = await executeQuery<{ count: number }>(db, `
    SELECT COUNT(*) as count
    FROM enterprises
    WHERE _snapshot_date = '${snapshotDate}'
      AND _extract_number = ${extractNumber}
      AND _is_current = true
      AND primary_name != enterprise_number
  `)

  return result[0]?.count || 0
}

/**
 * Clean up all staging tables for a completed job
 */
async function cleanupStagingTables(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  db: any,
  jobId: string
): Promise<void> {
  const stagingTables = [
    'import_staging_enterprises',
    'import_staging_establishments',
    'import_staging_denominations',
    'import_staging_addresses',
    'import_staging_contacts',
    'import_staging_activities',
    'import_staging_branches'
  ]

  for (const table of stagingTables) {
    await executeStatement(db, `
      DELETE FROM ${table} WHERE job_id = '${jobId}'
    `)
  }
}

/**
 * Step 4: Finalize Import
 *
 * Completes the import by resolving primary names for enterprises,
 * updating the job status to completed, and cleaning up staging data.
 *
 * @param jobId - Import job ID
 * @returns Finalization status
 */
export async function finalizeImport(
  jobId: string
): Promise<FinalizeResult> {
  const db = await connectMotherduck()

  try {
    // Step 1: Verify all batches completed
    const pendingBatches = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM import_job_batches
      WHERE job_id = '${jobId}'
        AND status != 'completed'
    `)

    if (pendingBatches[0]?.count > 0) {
      throw new Error(`Cannot finalize: ${pendingBatches[0].count} batches still pending or failed`)
    }

    // Step 2: Get job metadata
    const jobs = await executeQuery<{
      snapshot_date: string;
      extract_number: number;
    }>(db, `
      SELECT snapshot_date, extract_number
      FROM import_jobs
      WHERE id = '${jobId}'
    `)
    const job = jobs[0]

    // Step 3: Resolve primary names for new enterprises
    console.log('\n🔄 Resolving primary names for enterprises...')
    const namesResolved = await resolvePrimaryNames(db, job.snapshot_date, job.extract_number)
    if (namesResolved > 0) {
      console.log(`   ✓ Resolved primary names for ${namesResolved} enterprises`)
    } else {
      console.log(`   ℹ️  No new enterprises requiring name resolution`)
    }

    // Step 4: Update job status to completed
    const totalRecords = await executeQuery<{ total: number }>(db, `
      SELECT SUM(records_count) as total
      FROM import_job_batches
      WHERE job_id = '${jobId}'
    `)

    await executeStatement(db, `
      UPDATE import_jobs
      SET status = 'completed',
          completed_at = '${new Date().toISOString()}',
          records_processed = ${totalRecords[0]?.total || 0}
      WHERE id = '${jobId}'
    `)

    // Step 5: Clean up staging data
    console.log('\n🧹 Cleaning up staging tables...')
    await cleanupStagingTables(db, jobId)
    console.log('   ✓ Staging data cleaned up')

    console.log('\n✨ Import finalized successfully!')

    return {
      success: true,
      names_resolved: namesResolved,
      staging_cleaned: true
    }

  } finally {
    await closeMotherduck(db)
  }
}
