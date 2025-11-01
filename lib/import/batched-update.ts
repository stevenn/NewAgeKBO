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
  // TODO: Implement prepareImport
  // 1. Write buffer to temp file and open ZIP
  // 2. Parse metadata
  // 3. Create import_jobs record
  // 4. Extract CSV files and parse
  // 5. Insert into staging tables with batch_number assigned
  // 6. Create batch records in import_job_batches
  // 7. Return job metadata

  throw new Error('Not implemented')
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
  // TODO: Implement processBatch
  // 1. Get next pending batch (or specified batch)
  // 2. Fetch staging data for batch
  // 3. Execute DELETE or INSERT for that batch
  // 4. Mark batch as completed
  // 5. Return progress info (completed/total, next_batch)

  throw new Error('Not implemented')
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
  // TODO: Implement getImportProgress
  // 1. Query batch statuses by table
  // 2. Calculate overall percentage
  // 3. Identify next pending batch
  // 4. Return structured progress data

  throw new Error('Not implemented')
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
  // TODO: Implement finalizeImport
  // 1. Verify all batches completed
  // 2. Resolve primary names (existing logic from daily-update.ts)
  // 3. Update import_jobs status to 'completed'
  // 4. Clean up staging data

  throw new Error('Not implemented')
}
