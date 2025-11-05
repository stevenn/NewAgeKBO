#!/usr/bin/env tsx
/**
 * Migrate _deleted_at_extract Field
 *
 * Backfills the _deleted_at_extract field for historical records across all temporal tables.
 * This field was added on 2025-10-26 but was not populated by monthly snapshot imports.
 *
 * Logic: For each historical record, _deleted_at_extract is set to the _extract_number
 * of the next version of the same entity (the version that superseded it).
 *
 * Usage:
 *   npx tsx scripts/migrate-deleted-at-extract.ts           # Dry run (no changes)
 *   npx tsx scripts/migrate-deleted-at-extract.ts --execute # Execute migration
 *   npx tsx scripts/migrate-deleted-at-extract.ts --table enterprises --execute # Single table
 *
 * Safety:
 *   - Defaults to dry-run mode (shows what would be updated)
 *   - Validates results on sample before full migration
 *   - Provides detailed statistics and verification
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, executeQuery, closeMotherduck } from '../lib/motherduck'

interface TableMigrationStats {
  table_name: string
  total_historical: number
  missing_deleted_at: number
  would_update: number
  actually_updated?: number
  sample_validations: ValidationSample[]
  issues: string[]
}

interface ValidationSample {
  entity_id: string
  extract_number: number
  snapshot_date: string
  computed_deleted_at: number | null
  next_version_extract: number | null
  is_valid: boolean
  issue?: string
}

interface MigrationReport {
  timestamp: string
  mode: 'dry_run' | 'execute'
  tables_processed: string[]
  total_records_migrated: number
  table_stats: TableMigrationStats[]
  overall_success: boolean
  warnings: string[]
}

// Only tables with temporal tracking (excludes codes and nace_codes which are static)
const TABLES = [
  'enterprises',
  'establishments',
  'denominations',
  'addresses',
  'activities',
  'contacts',
  'branches'
]

// Primary key column name for each table
// (used to identify different versions of the same logical record)
const ENTITY_ID_COLUMNS: Record<string, string> = {
  enterprises: 'enterprise_number',
  establishments: 'establishment_number',
  denominations: 'id',  // Composite hash, not entity_number
  addresses: 'id',      // Composite hash, not entity_number
  activities: 'id',     // Composite hash, not entity_number
  contacts: 'id',       // Composite hash, not entity_number
  branches: 'id'        // Composite hash, not enterprise_number
}

async function validateSamples(
  db: any,
  tableName: string,
  entityIdColumn: string,
  sampleSize: number = 10
): Promise<ValidationSample[]> {
  console.log(`   🔍 Validating ${sampleSize} sample records...`)

  const samples = await executeQuery<{
    entity_id: string
    extract_number: number
    snapshot_date: string
    is_current: boolean
    deleted_at_extract: number | null
  }>(db, `
    SELECT
      ${entityIdColumn} as entity_id,
      _extract_number as extract_number,
      _snapshot_date::VARCHAR as snapshot_date,
      _is_current as is_current,
      _deleted_at_extract as deleted_at_extract
    FROM ${tableName}
    WHERE _is_current = false
      AND _deleted_at_extract IS NULL
    ORDER BY RANDOM()
    LIMIT ${sampleSize}
  `)

  const validations: ValidationSample[] = []

  for (const sample of samples) {
    // Find the next version of this entity
    const nextVersion = await executeQuery<{
      next_extract: number | null
    }>(db, `
      SELECT MIN(_extract_number) as next_extract
      FROM ${tableName}
      WHERE ${entityIdColumn} = '${sample.entity_id}'
        AND _extract_number > ${sample.extract_number}
    `)

    const nextExtract = nextVersion[0]?.next_extract
    const computedDeletedAt = nextExtract

    // Validation: computed value should exist (unless entity was deleted entirely)
    const isValid = computedDeletedAt !== null

    validations.push({
      entity_id: sample.entity_id,
      extract_number: sample.extract_number,
      snapshot_date: sample.snapshot_date,
      computed_deleted_at: computedDeletedAt,
      next_version_extract: nextExtract,
      is_valid: isValid,
      issue: !isValid ? 'No subsequent version found (entity may have been deleted)' : undefined
    })
  }

  const validCount = validations.filter(v => v.is_valid).length
  const invalidCount = validations.filter(v => !v.is_valid).length

  console.log(`      ✓ Valid: ${validCount}/${sampleSize}`)
  if (invalidCount > 0) {
    console.log(`      ⚠ Invalid: ${invalidCount}/${sampleSize} (entities with no subsequent version)`)
  }

  return validations
}

async function migrateTable(
  db: any,
  tableName: string,
  dryRun: boolean
): Promise<TableMigrationStats> {
  const entityIdColumn = ENTITY_ID_COLUMNS[tableName]
  console.log(`\n📋 Processing ${tableName} (entity ID: ${entityIdColumn})...`)

  const stats: TableMigrationStats = {
    table_name: tableName,
    total_historical: 0,
    missing_deleted_at: 0,
    would_update: 0,
    sample_validations: [],
    issues: []
  }

  try {
    // Step 1: Count historical records
    const historicalCount = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM ${tableName}
      WHERE _is_current = false
    `)
    stats.total_historical = Number(historicalCount[0].count)
    console.log(`   Total historical records: ${stats.total_historical.toLocaleString()}`)

    // Step 2: Count missing _deleted_at_extract
    const missingCount = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM ${tableName}
      WHERE _is_current = false
        AND _deleted_at_extract IS NULL
    `)
    stats.missing_deleted_at = Number(missingCount[0].count)
    console.log(`   Missing _deleted_at_extract: ${stats.missing_deleted_at.toLocaleString()}`)

    if (stats.missing_deleted_at === 0) {
      console.log(`   ✓ No records to migrate`)
      return stats
    }

    // Step 3: Validate on samples
    stats.sample_validations = await validateSamples(db, tableName, entityIdColumn, 10)

    const invalidSamples = stats.sample_validations.filter(v => !v.is_valid)
    if (invalidSamples.length > 0) {
      stats.issues.push(
        `${invalidSamples.length}/10 samples have no subsequent version (may be legitimately deleted entities)`
      )
    }

    // Step 4: Calculate what would be updated
    const wouldUpdateCount = await executeQuery<{ count: number }>(db, `
      WITH next_versions AS (
        SELECT
          ${entityIdColumn},
          _extract_number,
          _snapshot_date,
          LEAD(_extract_number) OVER (
            PARTITION BY ${entityIdColumn}
            ORDER BY _extract_number, _snapshot_date
          ) as next_extract
        FROM ${tableName}
      )
      SELECT COUNT(*) as count
      FROM next_versions
      WHERE next_extract IS NOT NULL
        AND ${entityIdColumn} IN (
          SELECT ${entityIdColumn}
          FROM ${tableName}
          WHERE _is_current = false
            AND _deleted_at_extract IS NULL
        )
    `)
    stats.would_update = Number(wouldUpdateCount[0].count)
    console.log(`   Would update: ${stats.would_update.toLocaleString()} records`)

    // Step 5: Execute update (if not dry run)
    if (!dryRun) {
      console.log(`   ⚙️  Executing update...`)

      const updateQuery = `
        UPDATE ${tableName}
        SET _deleted_at_extract = (
          SELECT MIN(_extract_number)
          FROM ${tableName} t2
          WHERE t2.${entityIdColumn} = ${tableName}.${entityIdColumn}
            AND t2._extract_number > ${tableName}._extract_number
        )
        WHERE _is_current = false
          AND _deleted_at_extract IS NULL
          AND EXISTS (
            SELECT 1
            FROM ${tableName} t3
            WHERE t3.${entityIdColumn} = ${tableName}.${entityIdColumn}
              AND t3._extract_number > ${tableName}._extract_number
          )
      `

      await executeQuery(db, updateQuery)

      // Verify update
      const updatedCount = await executeQuery<{ count: number }>(db, `
        SELECT COUNT(*) as count
        FROM ${tableName}
        WHERE _is_current = false
          AND _deleted_at_extract IS NOT NULL
      `)
      stats.actually_updated = Number(updatedCount[0].count)

      console.log(`   ✅ Updated ${stats.actually_updated.toLocaleString()} records`)

      // Check remaining nulls
      const remainingNulls = await executeQuery<{ count: number }>(db, `
        SELECT COUNT(*) as count
        FROM ${tableName}
        WHERE _is_current = false
          AND _deleted_at_extract IS NULL
      `)
      const remaining = Number(remainingNulls[0].count)
      if (remaining > 0) {
        stats.issues.push(
          `${remaining} historical records still have NULL _deleted_at_extract (entities with no subsequent version)`
        )
        console.log(`   ℹ️  ${remaining} records remain NULL (entities with no subsequent version)`)
      }
    } else {
      console.log(`   🔍 DRY RUN - No changes made`)
    }

  } catch (err) {
    const errorMsg = `Error processing ${tableName}: ${err instanceof Error ? err.message : 'Unknown'}`
    stats.issues.push(errorMsg)
    console.error(`   ❌ ${errorMsg}`)
  }

  return stats
}

async function runMigration(
  tablesToProcess: string[],
  dryRun: boolean
): Promise<MigrationReport> {
  const db = await connectMotherduck()
  await executeQuery(db, `USE ${process.env.MOTHERDUCK_DATABASE}`)

  const report: MigrationReport = {
    timestamp: new Date().toISOString(),
    mode: dryRun ? 'dry_run' : 'execute',
    tables_processed: tablesToProcess,
    total_records_migrated: 0,
    table_stats: [],
    overall_success: true,
    warnings: []
  }

  try {
    console.log('🔄 _deleted_at_extract Migration\n')
    console.log(`Mode: ${dryRun ? 'DRY RUN (no changes)' : 'EXECUTE (will update database)'}`)
    console.log(`Tables: ${tablesToProcess.join(', ')}`)
    console.log('='.repeat(80))

    for (const tableName of tablesToProcess) {
      const stats = await migrateTable(db, tableName, dryRun)
      report.table_stats.push(stats)

      if (stats.issues.length > 0) {
        report.warnings.push(...stats.issues.map(i => `${tableName}: ${i}`))
      }

      if (!dryRun && stats.actually_updated !== undefined) {
        report.total_records_migrated += stats.actually_updated
      }
    }

    // Overall statistics
    console.log('\n' + '='.repeat(80))
    console.log('MIGRATION SUMMARY')
    console.log('='.repeat(80))

    const totalHistorical = report.table_stats.reduce((sum, s) => sum + s.total_historical, 0)
    const totalMissing = report.table_stats.reduce((sum, s) => sum + s.missing_deleted_at, 0)
    const totalWouldUpdate = report.table_stats.reduce((sum, s) => sum + s.would_update, 0)

    console.log(`\nTotal historical records: ${totalHistorical.toLocaleString()}`)
    console.log(`Missing _deleted_at_extract: ${totalMissing.toLocaleString()}`)
    console.log(`${dryRun ? 'Would update' : 'Updated'}: ${dryRun ? totalWouldUpdate.toLocaleString() : report.total_records_migrated.toLocaleString()}`)

    if (report.warnings.length > 0) {
      console.log(`\nWarnings (${report.warnings.length}):`)
      report.warnings.forEach(w => console.log(`  ⚠ ${w}`))
    }

    if (dryRun) {
      console.log('\n💡 This was a DRY RUN. No changes were made.')
      console.log('   Run with --execute flag to apply changes.')
    } else {
      console.log('\n✅ Migration completed successfully!')
    }

  } finally {
    await closeMotherduck(db)
  }

  return report
}

async function main() {
  const args = process.argv.slice(2)
  const executeMode = args.includes('--execute')
  const dryRun = !executeMode

  // Allow single table processing
  const tableArg = args.find(arg => arg.startsWith('--table='))
  const tablesToProcess = tableArg
    ? [tableArg.split('=')[1]]
    : TABLES

  // Validate table names
  for (const table of tablesToProcess) {
    if (!TABLES.includes(table)) {
      console.error(`❌ Invalid table name: ${table}`)
      console.error(`   Valid tables: ${TABLES.join(', ')}`)
      process.exit(1)
    }
  }

  try {
    await runMigration(tablesToProcess, dryRun)
  } catch (error) {
    console.error('❌ Migration failed:', error)
    process.exit(1)
  }
}

main()
