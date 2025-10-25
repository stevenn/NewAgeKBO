/**
 * Temporal query helpers for point-in-time data reconstruction
 *
 * Handles the complexity of querying incremental updates to reconstruct
 * the complete state at any point in time.
 *
 * Strategy:
 * - Extract N=140: Full dump (all records)
 * - Extract N=150: Incremental (only changes)
 * - Extract N=157: Incremental (only changes)
 *
 * To get state at Extract 150:
 * 1. Find all versions of each record where _extract_number <= 150
 * 2. Exclude records deleted before or at Extract 150
 * 3. Take the latest version of each record
 */

export interface TemporalFilter {
  /**
   * For current/latest data: Use _is_current = true
   * For historical point-in-time: Use extract number
   */
  type: 'current' | 'point-in-time'

  /**
   * Extract number for point-in-time queries
   */
  extractNumber?: number

  /**
   * Snapshot date for point-in-time queries
   */
  snapshotDate?: string
}

/**
 * Build WHERE clause for point-in-time reconstruction
 *
 * For current data: Simple filter on _is_current = true
 * For historical data: Complex filter to reconstruct state at extract N
 */
export function buildTemporalFilter(
  filter: TemporalFilter,
  tableAlias: string = ''
): string {
  const prefix = tableAlias ? `${tableAlias}.` : ''

  if (filter.type === 'current') {
    return `${prefix}_is_current = true`
  }

  if (filter.type === 'point-in-time' && filter.extractNumber) {
    const extractNum = filter.extractNumber

    // Records that:
    // 1. Were created/updated on or before extract N
    // 2. Were either never deleted, or deleted after extract N
    return `
      ${prefix}_extract_number <= ${extractNum}
      AND (
        ${prefix}_deleted_at_extract IS NULL
        OR ${prefix}_deleted_at_extract > ${extractNum}
      )
    `.trim()
  }

  throw new Error('Invalid temporal filter configuration')
}

/**
 * Wrap a query to get only the latest version of each record
 * Uses ROW_NUMBER() window function to partition by ID and order by extract number
 *
 * @param selectColumns - The columns to select (without the ID column)
 * @param fromTable - The table name
 * @param whereClause - The WHERE clause (without WHERE keyword)
 * @param idColumn - The primary key column name (default: 'id')
 * @param orderBy - Optional additional ORDER BY clause for the final result
 */
export function buildPointInTimeQuery(
  selectColumns: string,
  fromTable: string,
  whereClause: string,
  idColumn: string = 'id',
  orderBy?: string
): string {
  return `
    SELECT ${selectColumns}
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY ${idColumn}
          ORDER BY _extract_number DESC, _snapshot_date DESC
        ) as rn
      FROM ${fromTable}
      WHERE ${whereClause}
    ) sub
    WHERE sub.rn = 1
    ${orderBy ? `ORDER BY ${orderBy}` : ''}
  `.trim()
}

/**
 * For tables without computed IDs (enterprises, establishments),
 * use the natural primary key for partitioning
 */
export function buildPointInTimeQueryByNaturalKey(
  selectColumns: string,
  fromTable: string,
  whereClause: string,
  partitionByColumn: string,
  orderBy?: string
): string {
  return `
    SELECT ${selectColumns}
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY ${partitionByColumn}
          ORDER BY _extract_number DESC, _snapshot_date DESC
        ) as rn
      FROM ${fromTable}
      WHERE ${whereClause}
    ) sub
    WHERE sub.rn = 1
    ${orderBy ? `ORDER BY ${orderBy}` : ''}
  `.trim()
}

/**
 * Simple helper to build temporal queries for child tables
 * Handles both current and point-in-time queries with proper ROW_NUMBER windowing
 */
export function buildChildTableQuery(
  tableName: string,
  selectColumns: string,
  entityNumber: string,
  filter: TemporalFilter,
  orderBy?: string,
  partitionKey: string = 'id'
): string {
  const temporalWhere = buildTemporalFilter(filter)
  const baseWhere = `entity_number = '${entityNumber}' AND ${temporalWhere}`

  if (filter.type === 'current') {
    // Simple query for current data
    return `
      SELECT ${selectColumns}
      FROM ${tableName}
      WHERE ${baseWhere}
      ${orderBy ? `ORDER BY ${orderBy}` : ''}
    `.trim()
  }

  // Point-in-time query with window function
  return buildPointInTimeQuery(
    selectColumns,
    tableName,
    baseWhere,
    partitionKey,
    orderBy
  )
}
