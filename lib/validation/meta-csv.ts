/**
 * Meta CSV validation and parsing
 */

import { parseKBODate, parseKBOTimestamp } from '@/lib/utils/date'
import { ValidationError } from '@/lib/errors'
import type { MetaData, RawMeta } from '@/lib/types'

/**
 * Parse meta.csv content
 * Expected format: SnapshotDate,ExtractTimestamp,ExtractNumber,ExtractType,Version
 */
export function parseMetaCsv(content: string): MetaData {
  const lines = content.trim().split('\n')
  if (lines.length < 2) {
    throw new ValidationError('meta.csv must have at least header and one data row')
  }

  // Parse header
  const header = lines[0].split(',')
  const expectedHeaders = [
    'SnapshotDate',
    'ExtractTimestamp',
    'ExtractNumber',
    'ExtractType',
    'Version',
  ]

  if (header.length !== expectedHeaders.length) {
    throw new ValidationError(
      `meta.csv header has ${header.length} columns, expected ${expectedHeaders.length}`
    )
  }

  // Parse data row
  const dataRow = lines[1].split(',')
  if (dataRow.length !== expectedHeaders.length) {
    throw new ValidationError(
      `meta.csv data row has ${dataRow.length} columns, expected ${expectedHeaders.length}`
    )
  }

  const rawMeta: RawMeta = {
    SnapshotDate: dataRow[0].trim(),
    ExtractTimestamp: dataRow[1].trim(),
    ExtractNumber: dataRow[2].trim(),
    ExtractType: dataRow[3].trim(),
    Version: dataRow[4].trim(),
  }

  try {
    const snapshotDate = parseKBODate(rawMeta.SnapshotDate)
    if (!snapshotDate) {
      throw new Error('SnapshotDate is null')
    }

    const extractTimestamp = parseKBOTimestamp(rawMeta.ExtractTimestamp)
    const extractNumber = parseInt(rawMeta.ExtractNumber, 10)

    if (isNaN(extractNumber)) {
      throw new Error(`ExtractNumber is not a number: ${rawMeta.ExtractNumber}`)
    }

    const extractType = rawMeta.ExtractType.toLowerCase()
    if (extractType !== 'full' && extractType !== 'update') {
      throw new Error(`ExtractType must be 'Full' or 'Update', got: ${rawMeta.ExtractType}`)
    }

    return {
      snapshot_date: snapshotDate,
      extract_timestamp: extractTimestamp,
      extract_number: extractNumber,
      extract_type: extractType as 'full' | 'update',
      version: rawMeta.Version,
    }
  } catch (error) {
    throw new ValidationError(
      `Failed to parse meta.csv: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { rawMeta }
    )
  }
}

export interface ValidationResult {
  valid: boolean
  errors: string[]
}

/**
 * Validate meta.csv data
 */
export function validateMeta(meta: MetaData): ValidationResult {
  const errors: string[] = []

  // Validate snapshot date is not in future
  const now = new Date()
  if (meta.snapshot_date > now) {
    errors.push(`SnapshotDate is in the future: ${meta.snapshot_date}`)
  }

  // Validate extract number is positive
  if (meta.extract_number <= 0) {
    errors.push(`ExtractNumber must be positive: ${meta.extract_number}`)
  }

  // Validate extract timestamp is after snapshot date
  if (meta.extract_timestamp < meta.snapshot_date) {
    errors.push('ExtractTimestamp must be after SnapshotDate')
  }

  // Validate version format
  if (!meta.version || meta.version.trim() === '') {
    errors.push('Version is empty')
  }

  return {
    valid: errors.length === 0,
    errors,
  }
}

/**
 * Check if new meta is newer than old meta
 */
export function isNewerMeta(newMeta: MetaData, oldMeta: MetaData): boolean {
  return newMeta.extract_number > oldMeta.extract_number
}
