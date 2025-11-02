#!/usr/bin/env tsx

/**
 * Validate Database State with Full Dump
 *
 * Imports a KBO full dump into temporary tables and compares against current
 * database state to identify discrepancies. Generates detailed report to help
 * decide whether to keep temporal history or start fresh.
 *
 * Usage: npx tsx scripts/validate-with-full-dump.ts /path/to/extracted/dump [--json]
 *
 * Process:
 * 1. Import full dump CSVs into temporary tables (nov_enterprises, nov_activities, etc.)
 * 2. Compare current database state (_is_current=true) against temp tables
 * 3. Calculate discrepancy metrics (entity counts, missing entities, data mismatches)
 * 4. Generate comprehensive report with recommendation
 * 5. Clean up temporary tables
 *
 * Decision Threshold:
 * - < 1% discrepancy: Keep history, proceed with standard snapshot
 * - 1-5% discrepancy: Review details, likely keep history
 * - > 5% discrepancy: Consider fresh start from full dump
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import * as fs from 'fs'
import {
  connectMotherduck,
  closeMotherduck,
  getMotherduckConfig,
  executeQuery
} from '../lib/motherduck'
import {
  parseMetadataWithDuckDB,
  Metadata
} from '../lib/import/metadata'
import {
  initializeDuckDBWithMotherduck,
  stageCsvFile,
  createRankedDenominations,
} from '../lib/import/duckdb-processor'
import * as path from 'path'
const { join } = path

interface ComparisonStats {
  table_name: string
  current_db_count: number
  full_dump_count: number
  difference: number
  difference_percent: number
  missing_in_db: number
  missing_in_dump: number
  data_mismatches: number
}

interface ValidationReport {
  timestamp: string
  full_dump_metadata: Metadata
  current_db_extracts: number[]
  overall_discrepancy_percent: number
  recommendation: 'keep_history' | 'review_details' | 'start_fresh'
  recommendation_reason: string
  table_comparisons: ComparisonStats[]
  sample_discrepancies: {
    entity_id: string
    issue_type: string
    db_value: string
    dump_value: string
  }[]
  summary: string
}

/**
 * Import full dump into temporary tables
 */
async function importToTempTables(
  localDb: any,
  motherduckDb: any,
  dataDir: string,
  metadata: Metadata
): Promise<void> {
  console.log('\n📥 Importing full dump into temporary tables...\n')

  // Stage CSV files in local DuckDB
  await stageCsvFile(localDb, join(dataDir, 'code.csv'), 'temp_codes')
  console.log('   ✓ code.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'enterprise.csv'), 'temp_enterprises')
  console.log('   ✓ enterprise.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'denomination.csv'), 'temp_denominations')
  console.log('   ✓ denomination.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'establishment.csv'), 'temp_establishments')
  console.log('   ✓ establishment.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'address.csv'), 'temp_addresses')
  console.log('   ✓ address.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'activity.csv'), 'temp_activities')
  console.log('   ✓ activity.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'contact.csv'), 'temp_contacts')
  console.log('   ✓ contact.csv staged')

  await stageCsvFile(localDb, join(dataDir, 'branch.csv'), 'temp_branches')
  console.log('   ✓ branch.csv staged')

  console.log('\n   ✅ All CSV files staged in local DuckDB')

  // Upload to Motherduck as temporary tables
  console.log('\n☁️  Uploading to Motherduck temporary tables...\n')

  const tables = [
    'temp_enterprises',
    'temp_establishments',
    'temp_denominations',
    'temp_addresses',
    'temp_activities',
    'temp_contacts',
    'temp_branches'
  ]

  for (const table of tables) {
    await executeQuery(localDb, `
      CREATE OR REPLACE TABLE ${mdConfig.database}.nov_${table.replace('temp_', '')} AS
      SELECT * FROM ${table}
    `)
    console.log(`   ✓ ${table} uploaded to nov_${table.replace('temp_', '')}`)
  }

  console.log('\n   ✅ All temporary tables created in Motherduck')
}

/**
 * Compare current database state with full dump
 */
async function compareStates(motherduckDb: any): Promise<ComparisonStats[]> {
  console.log('\n🔍 Comparing database state with full dump...\n')

  const comparisons: ComparisonStats[] = []

  // Compare enterprises
  console.log('   📊 Analyzing enterprises...')
  const enterpriseComparison = await executeQuery<{
    current_db_count: number
    full_dump_count: number
    missing_in_db: number
    missing_in_dump: number
  }>(motherduckDb, `
    WITH current_db AS (
      SELECT enterprise_number
      FROM enterprises
      WHERE _is_current = true
    ),
    full_dump AS (
      SELECT "EnterpriseNumber" as enterprise_number
      FROM nov_enterprises
    )
    SELECT
      (SELECT COUNT(*) FROM current_db) as current_db_count,
      (SELECT COUNT(*) FROM full_dump) as full_dump_count,
      (SELECT COUNT(*) FROM full_dump WHERE enterprise_number NOT IN (SELECT enterprise_number FROM current_db)) as missing_in_db,
      (SELECT COUNT(*) FROM current_db WHERE enterprise_number NOT IN (SELECT enterprise_number FROM full_dump)) as missing_in_dump
  `)

  const ec = enterpriseComparison[0]
  const entDiff = Math.abs(Number(ec.current_db_count) - Number(ec.full_dump_count))
  const entPercent = Number(ec.full_dump_count) > 0
    ? (entDiff / Number(ec.full_dump_count)) * 100
    : 0

  comparisons.push({
    table_name: 'enterprises',
    current_db_count: Number(ec.current_db_count),
    full_dump_count: Number(ec.full_dump_count),
    difference: entDiff,
    difference_percent: entPercent,
    missing_in_db: Number(ec.missing_in_db),
    missing_in_dump: Number(ec.missing_in_dump),
    data_mismatches: 0 // Will calculate in next phase if needed
  })

  console.log(`      DB: ${Number(ec.current_db_count).toLocaleString()} | Dump: ${Number(ec.full_dump_count).toLocaleString()} | Diff: ${entDiff.toLocaleString()} (${entPercent.toFixed(2)}%)`)

  // Compare establishments
  console.log('   📊 Analyzing establishments...')
  const establishmentComparison = await executeQuery<{
    current_db_count: number
    full_dump_count: number
    missing_in_db: number
    missing_in_dump: number
  }>(motherduckDb, `
    WITH current_db AS (
      SELECT establishment_number
      FROM establishments
      WHERE _is_current = true
    ),
    full_dump AS (
      SELECT "EstablishmentNumber" as establishment_number
      FROM nov_establishments
    )
    SELECT
      (SELECT COUNT(*) FROM current_db) as current_db_count,
      (SELECT COUNT(*) FROM full_dump) as full_dump_count,
      (SELECT COUNT(*) FROM full_dump WHERE establishment_number NOT IN (SELECT establishment_number FROM current_db)) as missing_in_db,
      (SELECT COUNT(*) FROM current_db WHERE establishment_number NOT IN (SELECT establishment_number FROM full_dump)) as missing_in_dump
  `)

  const estc = establishmentComparison[0]
  const estDiff = Math.abs(Number(estc.current_db_count) - Number(estc.full_dump_count))
  const estPercent = Number(estc.full_dump_count) > 0
    ? (estDiff / Number(estc.full_dump_count)) * 100
    : 0

  comparisons.push({
    table_name: 'establishments',
    current_db_count: Number(estc.current_db_count),
    full_dump_count: Number(estc.full_dump_count),
    difference: estDiff,
    difference_percent: estPercent,
    missing_in_db: Number(estc.missing_in_db),
    missing_in_dump: Number(estc.missing_in_dump),
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(estc.current_db_count).toLocaleString()} | Dump: ${Number(estc.full_dump_count).toLocaleString()} | Diff: ${estDiff.toLocaleString()} (${estPercent.toFixed(2)}%)`)

  // Compare activities (row counts)
  console.log('   📊 Analyzing activities (row count)...')
  const activityComparison = await executeQuery<{
    current_db_count: number
    full_dump_count: number
  }>(motherduckDb, `
    SELECT
      (SELECT COUNT(*) FROM activities WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM nov_activities) as full_dump_count
  `)

  const actc = activityComparison[0]
  const actDiff = Math.abs(Number(actc.current_db_count) - Number(actc.full_dump_count))
  const actPercent = Number(actc.full_dump_count) > 0
    ? (actDiff / Number(actc.full_dump_count)) * 100
    : 0

  comparisons.push({
    table_name: 'activities',
    current_db_count: Number(actc.current_db_count),
    full_dump_count: Number(actc.full_dump_count),
    difference: actDiff,
    difference_percent: actPercent,
    missing_in_db: 0,
    missing_in_dump: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(actc.current_db_count).toLocaleString()} | Dump: ${Number(actc.full_dump_count).toLocaleString()} | Diff: ${actDiff.toLocaleString()} (${actPercent.toFixed(2)}%)`)

  console.log('\n   ✅ Comparison complete')

  return comparisons
}

/**
 * Get sample discrepancies for investigation
 */
async function getSampleDiscrepancies(motherduckDb: any): Promise<ValidationReport['sample_discrepancies']> {
  const samples: ValidationReport['sample_discrepancies'] = []

  // Find 5 enterprises in DB but not in dump
  const missingInDump = await executeQuery<{
    enterprise_number: string
    status: string
    primary_name: string
  }>(motherduckDb, `
    SELECT
      enterprise_number,
      status,
      primary_name
    FROM enterprises
    WHERE _is_current = true
      AND enterprise_number NOT IN (SELECT "EnterpriseNumber" FROM nov_enterprises)
    LIMIT 5
  `)

  for (const row of missingInDump) {
    samples.push({
      entity_id: row.enterprise_number,
      issue_type: 'missing_in_dump',
      db_value: `${row.status} - ${row.primary_name}`,
      dump_value: 'NOT FOUND'
    })
  }

  // Find 5 enterprises in dump but not in DB
  const missingInDb = await executeQuery<{
    enterprise_number: string
    status: string
  }>(motherduckDb, `
    SELECT
      "EnterpriseNumber" as enterprise_number,
      "Status" as status
    FROM nov_enterprises
    WHERE "EnterpriseNumber" NOT IN (SELECT enterprise_number FROM enterprises WHERE _is_current = true)
    LIMIT 5
  `)

  for (const row of missingInDb) {
    samples.push({
      entity_id: row.enterprise_number,
      issue_type: 'missing_in_db',
      db_value: 'NOT FOUND',
      dump_value: row.status
    })
  }

  return samples
}

/**
 * Generate validation report
 */
async function generateReport(
  metadata: Metadata,
  currentExtracts: number[],
  comparisons: ComparisonStats[],
  samples: ValidationReport['sample_discrepancies']
): Promise<ValidationReport> {

  // Calculate overall discrepancy
  const totalCurrentDb = comparisons.reduce((sum, c) => sum + c.current_db_count, 0)
  const totalFullDump = comparisons.reduce((sum, c) => sum + c.full_dump_count, 0)
  const totalDifference = Math.abs(totalCurrentDb - totalFullDump)
  const overallPercent = totalFullDump > 0 ? (totalDifference / totalFullDump) * 100 : 0

  // Make recommendation
  let recommendation: ValidationReport['recommendation']
  let recommendationReason: string

  if (overallPercent < 1) {
    recommendation = 'keep_history'
    recommendationReason = 'Very low discrepancy (<1%). Database state is accurate. Safe to keep temporal history and import November dump as new snapshot.'
  } else if (overallPercent < 5) {
    recommendation = 'review_details'
    recommendationReason = `Moderate discrepancy (${overallPercent.toFixed(2)}%). Review sample discrepancies to understand cause. Likely legitimate changes between last update and November dump. Recommend keeping history unless specific data quality issues found.`
  } else {
    recommendation = 'start_fresh'
    recommendationReason = `High discrepancy (${overallPercent.toFixed(2)}%). Significant divergence suggests data quality or import issues. Recommend starting fresh from November dump as new baseline.`
  }

  // Generate summary
  const summary = `
Validation Summary:
- Current database has ${totalCurrentDb.toLocaleString()} records
- November dump has ${totalFullDump.toLocaleString()} records
- Overall discrepancy: ${overallPercent.toFixed(2)}%

Key Findings:
- Enterprises: ${comparisons[0]?.difference_percent.toFixed(2)}% difference
- Establishments: ${comparisons[1]?.difference_percent.toFixed(2)}% difference
- Activities: ${comparisons[2]?.difference_percent.toFixed(2)}% difference

Recommendation: ${recommendation.toUpperCase().replace(/_/g, ' ')}
${recommendationReason}
  `.trim()

  return {
    timestamp: new Date().toISOString(),
    full_dump_metadata: metadata,
    current_db_extracts: currentExtracts,
    overall_discrepancy_percent: overallPercent,
    recommendation,
    recommendation_reason: recommendationReason,
    table_comparisons: comparisons,
    sample_discrepancies: samples,
    summary
  }
}

/**
 * Clean up temporary tables
 */
async function cleanupTempTables(motherduckDb: any): Promise<void> {
  console.log('\n🧹 Cleaning up temporary tables...\n')

  const tables = [
    'nov_enterprises',
    'nov_establishments',
    'nov_denominations',
    'nov_addresses',
    'nov_activities',
    'nov_contacts',
    'nov_branches'
  ]

  for (const table of tables) {
    try {
      await executeQuery(motherduckDb, `DROP TABLE IF EXISTS ${table}`)
      console.log(`   ✓ ${table} dropped`)
    } catch (err) {
      console.log(`   ⚠ Failed to drop ${table}: ${err instanceof Error ? err.message : 'Unknown'}`)
    }
  }

  console.log('\n   ✅ Cleanup complete')
}

/**
 * Main validation process
 */
async function main() {
  const args = process.argv.slice(2)
  const jsonOutput = args.includes('--json')
  const dataDir = args.find(arg => !arg.startsWith('--'))

  if (!dataDir) {
    console.error('❌ Usage: npx tsx scripts/validate-with-full-dump.ts /path/to/extracted/dump [--json]')
    process.exit(1)
  }

  if (!fs.existsSync(dataDir)) {
    console.error(`❌ Directory not found: ${dataDir}`)
    process.exit(1)
  }

  console.log('🔍 FULL DUMP VALIDATION')
  console.log('=' .repeat(80))
  console.log(`Data directory: ${dataDir}`)
  console.log()

  const mdConfig = getMotherduckConfig()
  let motherduckDb: any
  let localDb: any

  try {
    // Connect to databases
    console.log('1️⃣  Connecting to Motherduck...')
    motherduckDb = await connectMotherduck()
    await executeQuery(motherduckDb, `USE ${mdConfig.database}`)
    console.log(`   ✅ Connected to ${mdConfig.database}`)

    console.log('\n2️⃣  Initializing local DuckDB...')
    localDb = await initializeDuckDBWithMotherduck(mdConfig.token, mdConfig.database || 'kbo')
    console.log('   ✅ Local DuckDB initialized')

    // Parse full dump metadata
    console.log('\n3️⃣  Reading full dump metadata...')
    const metadata = await parseMetadataWithDuckDB(localDb, dataDir)
    console.log(`   ✅ Extract #${metadata.extractNumber} (${metadata.snapshotDate})`)

    // Get current database extracts
    console.log('\n4️⃣  Checking current database state...')
    const extractsResult = await executeQuery<{ _extract_number: number }>(
      motherduckDb,
      'SELECT DISTINCT _extract_number FROM enterprises ORDER BY _extract_number'
    )
    const currentExtracts = extractsResult.map(r => r._extract_number)
    console.log(`   ✅ Current extracts: ${currentExtracts.join(', ')}`)

    // Import to temporary tables
    console.log('\n5️⃣  Importing full dump to temporary tables...')
    await importToTempTables(localDb, motherduckDb, dataDir, metadata)

    // Compare states
    console.log('\n6️⃣  Comparing database state with full dump...')
    const comparisons = await compareStates(motherduckDb)

    // Get sample discrepancies
    console.log('\n7️⃣  Collecting sample discrepancies...')
    const samples = await getSampleDiscrepancies(motherduckDb)
    console.log(`   ✅ Found ${samples.length} sample discrepancies`)

    // Generate report
    console.log('\n8️⃣  Generating validation report...')
    const report = await generateReport(metadata, currentExtracts, comparisons, samples)

    // Clean up (unless --keep-temp flag provided)
    if (!args.includes('--keep-temp')) {
      await cleanupTempTables(motherduckDb)
    } else {
      console.log('\n💾 Temporary tables kept (--keep-temp flag set)')
    }

    // Output report
    if (jsonOutput) {
      console.log(JSON.stringify(report, null, 2))
    } else {
      console.log('\n' + '='.repeat(80))
      console.log('📊 VALIDATION REPORT')
      console.log('='.repeat(80))
      console.log(report.summary)
      console.log('\n' + '='.repeat(80))
      console.log('\n📝 Sample Discrepancies:\n')

      if (report.sample_discrepancies.length === 0) {
        console.log('   No discrepancies found in sample')
      } else {
        for (const sample of report.sample_discrepancies) {
          console.log(`   • ${sample.entity_id} (${sample.issue_type})`)
          console.log(`     DB: ${sample.db_value}`)
          console.log(`     Dump: ${sample.dump_value}`)
        }
      }

      console.log('\n' + '='.repeat(80))
      console.log(`\n✅ Validation complete! Recommendation: ${report.recommendation.toUpperCase().replace(/_/g, ' ')}`)
      console.log(`\n💡 ${report.recommendation_reason}`)
      console.log('\n' + '='.repeat(80))
    }

  } catch (error) {
    console.error('\n❌ Validation failed:', error)
    process.exit(1)
  } finally {
    if (motherduckDb) {
      await closeMotherduck(motherduckDb)
    }
  }
}

const mdConfig = getMotherduckConfig()
main()
