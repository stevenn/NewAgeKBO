/**
 * Types for KBO Open Data Portal integration
 */

import { ImportJobType } from './import-job'

/**
 * Represents a dataset file available on the KBO portal
 */
export interface KboDatasetFile {
  /** Filename (e.g., "KboOpenData_0141_2025_10_06_Update.zip") */
  filename: string

  /** Full download URL */
  url: string

  /** Extract number from KBO (e.g., 141) */
  extract_number: number

  /** Snapshot date in ISO format (YYYY-MM-DD) */
  snapshot_date: string

  /** Type of dataset file */
  file_type: ImportJobType

  /** File size in bytes (if available) */
  size_bytes?: number

  /** Whether this extract has already been imported */
  imported: boolean
}

/**
 * Response from KBO portal authentication
 */
export interface KboAuthResult {
  success: boolean
  cookies?: string[]
  error?: string
}

/**
 * Statistics returned from a daily update import
 */
export interface DailyUpdateStats {
  metadata: {
    SnapshotDate: string
    ExtractTimestamp: string
    ExtractType: string
    ExtractNumber: string
    Version: string
  }
  tablesProcessed: string[]
  deletesApplied: number
  insertsApplied: number
  errors: string[]
}
