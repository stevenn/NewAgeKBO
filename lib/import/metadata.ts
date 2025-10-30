/**
 * KBO Metadata Parsing Utilities
 *
 * Handles parsing of meta.csv files from KBO Open Data extracts
 */

// Note: Old duckdb import commented out - not used in production web flow
// Only needed for CLI scripts which should be updated to use @duckdb/node-api
// import * as duckdb from 'duckdb'
// import { join } from 'path'
import { ImportJobType } from '../types/import-job'

/**
 * Metadata from meta.csv
 */
export interface Metadata {
  snapshotDate: string      // YYYY-MM-DD format (converted from DD-MM-YYYY)
  extractNumber: number     // Extract sequence number (e.g., 140, 141)
  extractType: ImportJobType       // 'full' or 'update'
  version: string          // Version string (e.g., 'R018.00')
  extractTimestamp?: string // Optional timestamp
}

/**
 * Raw meta.csv record (Variable/Value pairs)
 * Used in commented-out parseMetadataWithDuckDB function
 */
// interface MetaRecord {
//   Variable?: string
//   Value?: string
// }

/**
 * Parse meta.csv using DuckDB to extract snapshot date and extract number
 *
 * NOTE: This function uses the old 'duckdb' package and is ONLY for CLI scripts.
 * The production web import uses parseMetadataFromContent() instead.
 * This function is commented out to avoid build errors - CLI scripts need updating.
 *
 * @param db DuckDB database instance
 * @param dataPath Path to directory containing meta.csv
 * @returns Parsed metadata with converted date format
 */
/*
export async function parseMetadataWithDuckDB(
  db: any, // duckdb.Database - old package not used in production
  dataPath: string
): Promise<Metadata> {
  const metaPath = join(dataPath, 'meta.csv')

  // Load meta.csv and pivot it to get values by variable name
  const rows = await new Promise<MetaRecord[]>((resolve, reject) => {
    db.all(
      `SELECT Variable, Value
       FROM read_csv('${metaPath}', AUTO_DETECT=TRUE, HEADER=TRUE)`,
      (err, rows) => {
        if (err) reject(err)
        else resolve(rows as MetaRecord[])
      }
    )
  })

  // Convert rows to key-value map
  const metadata: Record<string, string> = {}
  for (const row of rows) {
    if (row.Variable && row.Value) {
      metadata[row.Variable] = row.Value
    }
  }

  // Validate required fields
  if (!metadata.SnapshotDate) {
    throw new Error('Missing SnapshotDate in meta.csv')
  }
  if (!metadata.ExtractNumber) {
    throw new Error('Missing ExtractNumber in meta.csv')
  }
  if (!metadata.ExtractType) {
    throw new Error('Missing ExtractType in meta.csv')
  }

  // Convert DD-MM-YYYY to YYYY-MM-DD
  const snapshotDateParts = metadata.SnapshotDate.split('-')
  if (snapshotDateParts.length !== 3) {
    throw new Error(`Invalid SnapshotDate format: ${metadata.SnapshotDate}`)
  }
  const snapshotDate = `${snapshotDateParts[2]}-${snapshotDateParts[1]}-${snapshotDateParts[0]}`

  return {
    snapshotDate,
    extractNumber: parseInt(metadata.ExtractNumber, 10),
    extractType: metadata.ExtractType as ImportJobType,
    version: metadata.Version || 'unknown',
    extractTimestamp: metadata.ExtractTimestamp
  }
}
*/

/**
 * Parse meta.csv from filesystem (for non-DuckDB contexts like ZIP processing)
 *
 * @param metaContent Raw CSV content from meta.csv
 * @returns Parsed metadata
 */
export function parseMetadataFromContent(metaContent: string): Metadata {
  // Simple CSV parsing for meta.csv (always has 2 columns: Variable, Value)
  const lines = metaContent.trim().split('\n')
  const metadata: Record<string, string> = {}

  // Skip header row
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim()
    if (!line) continue

    // Split on first comma (Value might contain commas in quotes)
    const firstComma = line.indexOf(',')
    if (firstComma === -1) continue

    const variable = line.substring(0, firstComma).trim()
    let value = line.substring(firstComma + 1).trim()

    // Remove quotes if present
    if (value.startsWith('"') && value.endsWith('"')) {
      value = value.substring(1, value.length - 1)
    }

    metadata[variable] = value
  }

  // Validate required fields
  if (!metadata.SnapshotDate) {
    throw new Error('Missing SnapshotDate in meta.csv')
  }
  if (!metadata.ExtractNumber) {
    throw new Error('Missing ExtractNumber in meta.csv')
  }
  if (!metadata.ExtractType) {
    throw new Error('Missing ExtractType in meta.csv')
  }

  // Convert DD-MM-YYYY to YYYY-MM-DD
  const snapshotDateParts = metadata.SnapshotDate.split('-')
  if (snapshotDateParts.length !== 3) {
    throw new Error(`Invalid SnapshotDate format: ${metadata.SnapshotDate}`)
  }
  const snapshotDate = `${snapshotDateParts[2]}-${snapshotDateParts[1]}-${snapshotDateParts[0]}`

  return {
    snapshotDate,
    extractNumber: parseInt(metadata.ExtractNumber, 10),
    extractType: metadata.ExtractType as ImportJobType,
    version: metadata.Version || 'unknown',
    extractTimestamp: metadata.ExtractTimestamp
  }
}

/**
 * Validate extract type (must be 'full' or 'update')
 */
export function validateExtractType(metadata: Metadata, expected: ImportJobType): void {
  if (metadata.extractType !== expected) {
    throw new Error(
      `Expected extract type '${expected}', got '${metadata.extractType}' (Extract #${metadata.extractNumber})`
    )
  }
}
