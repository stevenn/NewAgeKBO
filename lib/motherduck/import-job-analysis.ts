/**
 * Import Job Analysis
 * Analyze which enterprises were affected by a specific import job
 * Computes change types and affected tables breakdown on-demand from temporal data
 */

import { DuckDBConnection } from '@duckdb/node-api'
import { executeQuery } from './index'

export interface AffectedEnterprise {
  enterpriseNumber: string
  primaryName: string
  changeType: 'insert' | 'update' | 'delete'
  affectedTables: Array<{
    tableName: string
    changeCount: number
  }>
}

export interface AffectedEnterprisesResult {
  enterprises: AffectedEnterprise[]
  total: number
  page: number
  totalPages: number
}

/**
 * Get enterprises affected by a specific extract/import job
 * Queries temporal data across all tables to find changes
 */
export async function getAffectedEnterprises(
  connection: DuckDBConnection,
  extractNumber: number,
  page: number = 1,
  limit: number = 50
): Promise<AffectedEnterprisesResult> {
  const offset = (page - 1) * limit

  // Single query that gets both count and paginated results
  // We'll use window functions to avoid running the CTE twice
  const query = `
    WITH affected_records AS (
      -- Enterprises table
      SELECT
        enterprise_number as entity_number,
        'enterprises' as table_name,
        CASE
          WHEN _deleted_at_extract = ${extractNumber} THEN 'delete'
          WHEN _extract_number = ${extractNumber} THEN 'insert_or_update'
        END as change_type,
        1 as change_count
      FROM enterprises
      WHERE _extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber}

      UNION ALL

      -- Establishments table
      SELECT
        enterprise_number as entity_number,
        'establishments' as table_name,
        CASE
          WHEN _deleted_at_extract = ${extractNumber} THEN 'delete'
          WHEN _extract_number = ${extractNumber} THEN 'insert_or_update'
        END as change_type,
        1 as change_count
      FROM establishments
      WHERE _extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber}

      UNION ALL

      -- Denominations table
      SELECT
        entity_number,
        'denominations' as table_name,
        CASE
          WHEN _deleted_at_extract = ${extractNumber} THEN 'delete'
          WHEN _extract_number = ${extractNumber} THEN 'insert_or_update'
        END as change_type,
        1 as change_count
      FROM denominations
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'

      UNION ALL

      -- Addresses table
      SELECT
        entity_number,
        'addresses' as table_name,
        CASE
          WHEN _deleted_at_extract = ${extractNumber} THEN 'delete'
          WHEN _extract_number = ${extractNumber} THEN 'insert_or_update'
        END as change_type,
        1 as change_count
      FROM addresses
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'

      UNION ALL

      -- Activities table
      SELECT
        entity_number,
        'activities' as table_name,
        CASE
          WHEN _deleted_at_extract = ${extractNumber} THEN 'delete'
          WHEN _extract_number = ${extractNumber} THEN 'insert_or_update'
        END as change_type,
        1 as change_count
      FROM activities
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'

      UNION ALL

      -- Contacts table
      SELECT
        entity_number,
        'contacts' as table_name,
        CASE
          WHEN _deleted_at_extract = ${extractNumber} THEN 'delete'
          WHEN _extract_number = ${extractNumber} THEN 'insert_or_update'
        END as change_type,
        1 as change_count
      FROM contacts
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'
    ),

    -- Group by enterprise to compute aggregates
    enterprise_changes AS (
      SELECT
        entity_number,
        table_name,
        SUM(change_count) as change_count,
        -- Determine overall change type for the enterprise:
        -- If any delete, it's a delete. Otherwise if any insert_or_update, check if enterprise existed before.
        MAX(CASE WHEN change_type = 'delete' THEN 1 ELSE 0 END) as has_delete
      FROM affected_records
      GROUP BY entity_number, table_name
    ),

    -- Determine the primary change type for each enterprise
    enterprise_change_type AS (
      SELECT
        ec.entity_number,
        CASE
          -- If enterprise itself was deleted, it's a delete
          WHEN MAX(CASE WHEN ec.table_name = 'enterprises' AND ec.has_delete = 1 THEN 1 ELSE 0 END) = 1
            THEN 'delete'
          -- If enterprise was just inserted (no previous extract), it's an insert
          WHEN NOT EXISTS (
            SELECT 1 FROM enterprises e
            WHERE e.enterprise_number = ec.entity_number
              AND e._extract_number < ${extractNumber}
          ) THEN 'insert'
          -- Otherwise it's an update
          ELSE 'update'
        END as change_type
      FROM enterprise_changes ec
      GROUP BY ec.entity_number
    ),

    -- Combine all data into final result set
    final_results AS (
      SELECT
        ect.entity_number,
        ect.change_type,
        COALESCE(e.primary_name, ect.entity_number) as primary_name,
        -- Build array of affected tables with counts
        LIST({
          'tableName': ec.table_name,
          'changeCount': ec.change_count
        } ORDER BY ec.table_name) as affected_tables_json
      FROM enterprise_change_type ect
      LEFT JOIN enterprise_changes ec ON ec.entity_number = ect.entity_number
      LEFT JOIN enterprises e ON e.enterprise_number = ect.entity_number AND e._is_current = true
      GROUP BY ect.entity_number, ect.change_type, e.primary_name
    ),

    -- Add row numbers and total count
    paginated AS (
      SELECT
        entity_number,
        change_type,
        primary_name,
        affected_tables_json,
        ROW_NUMBER() OVER (ORDER BY entity_number) as row_num,
        COUNT(*) OVER () as total_count
      FROM final_results
    )

  SELECT
    entity_number,
    change_type,
    primary_name,
    affected_tables_json,
    total_count
  FROM paginated
  WHERE row_num > ${offset} AND row_num <= ${offset + limit}
  `

  const results = await executeQuery<{
    entity_number: string
    change_type: 'insert' | 'update' | 'delete'
    primary_name: string
    affected_tables_json: Array<{ tableName: string; changeCount: number }>
    total_count: number
  }>(connection, query)

  const total = results.length > 0 ? Number(results[0].total_count) : 0
  const totalPages = Math.ceil(total / limit)

  // Step 4: Transform results
  const enterprises: AffectedEnterprise[] = results.map((row) => {
    // Ensure affectedTables is always an array
    let affectedTables = row.affected_tables_json || []

    // If it's a string (shouldn't happen but just in case), try to parse it
    if (typeof affectedTables === 'string') {
      try {
        affectedTables = JSON.parse(affectedTables)
      } catch {
        affectedTables = []
      }
    }

    // Ensure it's an array
    if (!Array.isArray(affectedTables)) {
      affectedTables = []
    }

    return {
      enterpriseNumber: row.entity_number,
      primaryName: row.primary_name,
      changeType: row.change_type,
      affectedTables: affectedTables,
    }
  })

  return {
    enterprises,
    total,
    page,
    totalPages,
  }
}

/**
 * Get summary statistics for an import job
 * Returns counts by change type and table
 */
export async function getImportJobSummary(
  connection: DuckDBConnection,
  extractNumber: number
): Promise<{
  totalEnterprises: number
  insertCount: number
  updateCount: number
  deleteCount: number
  tableBreakdown: Record<string, number>
}> {
  const query = `
    WITH affected_records AS (
      SELECT 'enterprises' as table_name, COUNT(*) as count
      FROM enterprises
      WHERE _extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber}

      UNION ALL

      SELECT 'establishments', COUNT(*)
      FROM establishments
      WHERE _extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber}

      UNION ALL

      SELECT 'denominations', COUNT(*)
      FROM denominations
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'

      UNION ALL

      SELECT 'addresses', COUNT(*)
      FROM addresses
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'

      UNION ALL

      SELECT 'activities', COUNT(*)
      FROM activities
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'

      UNION ALL

      SELECT 'contacts', COUNT(*)
      FROM contacts
      WHERE (_extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber})
        AND entity_type = 'enterprise'
    )
    SELECT table_name, count FROM affected_records
  `

  const results = await executeQuery<{ table_name: string; count: number }>(connection, query)

  const tableBreakdown: Record<string, number> = {}
  for (const row of results) {
    tableBreakdown[row.table_name] = Number(row.count)
  }

  // Get change type breakdown
  const changeTypeQuery = `
    WITH all_affected_enterprises AS (
      SELECT DISTINCT enterprise_number
      FROM enterprises
      WHERE _extract_number = ${extractNumber} OR _deleted_at_extract = ${extractNumber}
    ),
    change_types AS (
      SELECT
        CASE
          WHEN EXISTS(SELECT 1 FROM enterprises e
                     WHERE e.enterprise_number = ae.enterprise_number
                       AND e._deleted_at_extract = ${extractNumber})
            THEN 'delete'
          WHEN NOT EXISTS(SELECT 1 FROM enterprises e
                         WHERE e.enterprise_number = ae.enterprise_number
                           AND e._extract_number < ${extractNumber})
            THEN 'insert'
          ELSE 'update'
        END as change_type
      FROM all_affected_enterprises ae
    )
    SELECT
      change_type,
      COUNT(*) as count
    FROM change_types
    GROUP BY change_type
  `

  const changeTypeResults = await executeQuery<{ change_type: string; count: number }>(
    connection,
    changeTypeQuery
  )

  let insertCount = 0
  let updateCount = 0
  let deleteCount = 0

  for (const row of changeTypeResults) {
    const count = Number(row.count)
    if (row.change_type === 'insert') insertCount = count
    else if (row.change_type === 'update') updateCount = count
    else if (row.change_type === 'delete') deleteCount = count
  }

  return {
    totalEnterprises: insertCount + updateCount + deleteCount,
    insertCount,
    updateCount,
    deleteCount,
    tableBreakdown,
  }
}
