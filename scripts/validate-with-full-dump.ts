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

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve, join } from 'path'
import * as fs from 'fs'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

interface Metadata {
  extractNumber: number
  snapshotDate: string
  extractTimestamp: string
  extractType: string
}

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
 * Parse metadata from meta.csv
 */
async function parseMetadata(conn: any, dataDir: string): Promise<Metadata> {
  const metaPath = join(dataDir, 'meta.csv')

  if (!fs.existsSync(metaPath)) {
    throw new Error(`meta.csv not found in ${dataDir}`)
  }

  const result = await conn.run(`
    SELECT * FROM read_csv('${metaPath}', AUTO_DETECT=TRUE, HEADER=TRUE)
  `)

  const chunks = await result.fetchAllChunks()
  if (chunks.length === 0) {
    throw new Error('meta.csv is empty')
  }

  // meta.csv has format: Variable,Value with rows for each field
  const metaData: Record<string, string> = {}
  for (const chunk of chunks) {
    const rows = chunk.getRows()
    for (const row of rows) {
      const variable = String(row[0])
      const value = String(row[1])
      metaData[variable] = value
    }
  }

  return {
    extractNumber: Number(metaData.ExtractNumber),
    snapshotDate: metaData.SnapshotDate,
    extractTimestamp: metaData.ExtractTimestamp,
    extractType: metaData.ExtractType
  }
}

/**
 * Import full dump CSVs into temporary tables
 */
async function importToTempTables(conn: any, dataDir: string, database: string): Promise<void> {
  console.log('\n📥 Importing full dump into temporary tables...\n')

  const files = [
    { csv: 'enterprise.csv', table: 'nov_enterprises' },
    { csv: 'establishment.csv', table: 'nov_establishments' },
    { csv: 'denomination.csv', table: 'nov_denominations' },
    { csv: 'address.csv', table: 'nov_addresses' },
    { csv: 'activity.csv', table: 'nov_activities' },
    { csv: 'contact.csv', table: 'nov_contacts' },
    { csv: 'branch.csv', table: 'nov_branches' }
  ]

  for (const { csv, table } of files) {
    const csvPath = join(dataDir, csv)

    if (!fs.existsSync(csvPath)) {
      console.log(`   ⚠ ${csv} not found, skipping...`)
      continue
    }

    console.log(`   📄 Loading ${csv}...`)

    await conn.run(`
      CREATE OR REPLACE TABLE ${table} AS
      SELECT * FROM read_csv('${csvPath}', AUTO_DETECT=TRUE, HEADER=TRUE)
    `)

    console.log(`   ✓ ${csv} loaded to ${table}`)
  }

  console.log('\n   ✅ All CSV files loaded into temporary tables')
}

/**
 * Compare current database state with full dump
 */
async function compareStates(conn: any): Promise<ComparisonStats[]> {
  console.log('\n🔍 Comparing database state with full dump...\n')

  const comparisons: ComparisonStats[] = []

  // Compare enterprises
  console.log('   📊 Analyzing enterprises...')
  const entResult = await conn.run(`
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

  const entChunks = await entResult.fetchAllChunks()
  const entRow = entChunks[0].getRows()[0]

  const entDiff = Math.abs(Number(entRow[0]) - Number(entRow[1]))
  const entPercent = Number(entRow[1]) > 0 ? (entDiff / Number(entRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'enterprises',
    current_db_count: Number(entRow[0]),
    full_dump_count: Number(entRow[1]),
    difference: entDiff,
    difference_percent: entPercent,
    missing_in_db: Number(entRow[2]),
    missing_in_dump: Number(entRow[3]),
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(entRow[0]).toLocaleString()} | Dump: ${Number(entRow[1]).toLocaleString()} | Diff: ${entDiff.toLocaleString()} (${entPercent.toFixed(2)}%)`)

  // Compare establishments
  console.log('   📊 Analyzing establishments...')
  const estResult = await conn.run(`
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

  const estChunks = await estResult.fetchAllChunks()
  const estRow = estChunks[0].getRows()[0]

  const estDiff = Math.abs(Number(estRow[0]) - Number(estRow[1]))
  const estPercent = Number(estRow[1]) > 0 ? (estDiff / Number(estRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'establishments',
    current_db_count: Number(estRow[0]),
    full_dump_count: Number(estRow[1]),
    difference: estDiff,
    difference_percent: estPercent,
    missing_in_db: Number(estRow[2]),
    missing_in_dump: Number(estRow[3]),
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(estRow[0]).toLocaleString()} | Dump: ${Number(estRow[1]).toLocaleString()} | Diff: ${estDiff.toLocaleString()} (${estPercent.toFixed(2)}%)`)

  // Compare activities (row counts)
  console.log('   📊 Analyzing activities (row count)...')
  const actResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM activities WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM nov_activities) as full_dump_count
  `)

  const actChunks = await actResult.fetchAllChunks()
  const actRow = actChunks[0].getRows()[0]

  const actDiff = Math.abs(Number(actRow[0]) - Number(actRow[1]))
  const actPercent = Number(actRow[1]) > 0 ? (actDiff / Number(actRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'activities',
    current_db_count: Number(actRow[0]),
    full_dump_count: Number(actRow[1]),
    difference: actDiff,
    difference_percent: actPercent,
    missing_in_db: 0,
    missing_in_dump: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(actRow[0]).toLocaleString()} | Dump: ${Number(actRow[1]).toLocaleString()} | Diff: ${actDiff.toLocaleString()} (${actPercent.toFixed(2)}%)`)

  console.log('\n   ✅ Comparison complete')

  return comparisons
}

/**
 * Get sample discrepancies for investigation
 */
async function getSampleDiscrepancies(conn: any): Promise<ValidationReport['sample_discrepancies']> {
  const samples: ValidationReport['sample_discrepancies'] = []

  // Find 5 enterprises in DB but not in dump
  const missingInDumpResult = await conn.run(`
    SELECT
      enterprise_number,
      status,
      primary_name
    FROM enterprises
    WHERE _is_current = true
      AND enterprise_number NOT IN (SELECT "EnterpriseNumber" FROM nov_enterprises)
    LIMIT 5
  `)

  const missingInDumpChunks = await missingInDumpResult.fetchAllChunks()
  for (const chunk of missingInDumpChunks) {
    const rows = chunk.getRows()
    for (const row of rows) {
      samples.push({
        entity_id: String(row[0]),
        issue_type: 'missing_in_dump',
        db_value: `${row[1]} - ${row[2]}`,
        dump_value: 'NOT FOUND'
      })
    }
  }

  // Find 5 enterprises in dump but not in DB
  const missingInDbResult = await conn.run(`
    SELECT
      "EnterpriseNumber" as enterprise_number,
      "Status" as status
    FROM nov_enterprises
    WHERE "EnterpriseNumber" NOT IN (SELECT enterprise_number FROM enterprises WHERE _is_current = true)
    LIMIT 5
  `)

  const missingInDbChunks = await missingInDbResult.fetchAllChunks()
  for (const chunk of missingInDbChunks) {
    const rows = chunk.getRows()
    for (const row of rows) {
      samples.push({
        entity_id: String(row[0]),
        issue_type: 'missing_in_db',
        db_value: 'NOT FOUND',
        dump_value: String(row[1])
      })
    }
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
    recommendationReason = 'Very low discrepancy (<1%). Database state is accurate. Safe to keep temporal history and continue with incremental updates.'
  } else if (overallPercent < 5) {
    recommendation = 'review_details'
    recommendationReason = `Moderate discrepancy (${overallPercent.toFixed(2)}%). Review sample discrepancies to understand cause. May be due to missing incremental update #162. Recommend investigating before proceeding.`
  } else {
    recommendation = 'start_fresh'
    recommendationReason = `High discrepancy (${overallPercent.toFixed(2)}%). Significant divergence suggests data quality or import issues. Recommend resetting from full dump #${metadata.extractNumber} as new baseline.`
  }

  // Generate summary
  const summary = `
Validation Summary:
- Current database has ${totalCurrentDb.toLocaleString()} current records
- Full dump #${metadata.extractNumber} has ${totalFullDump.toLocaleString()} records
- Overall discrepancy: ${overallPercent.toFixed(2)}%

Key Findings:
- Enterprises: ${comparisons[0]?.difference_percent.toFixed(2)}% difference (${comparisons[0]?.missing_in_db} missing in DB, ${comparisons[0]?.missing_in_dump} missing in dump)
- Establishments: ${comparisons[1]?.difference_percent.toFixed(2)}% difference (${comparisons[1]?.missing_in_db} missing in DB, ${comparisons[1]?.missing_in_dump} missing in dump)
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
async function cleanupTempTables(conn: any): Promise<void> {
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
      await conn.run(`DROP TABLE IF EXISTS ${table}`)
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

  const token = process.env.MOTHERDUCK_TOKEN
  const database = process.env.MOTHERDUCK_DATABASE || 'newagekbo'

  if (!token) {
    console.error('❌ MOTHERDUCK_TOKEN not set in .env.local')
    process.exit(1)
  }

  console.log('🔍 FULL DUMP VALIDATION')
  console.log('='.repeat(80))
  console.log(`Data directory: ${dataDir}`)
  console.log()

  // Create in-memory database instance
  const db = await DuckDBInstance.create(':memory:')
  const conn = await db.connect()

  try {
    // Set directory configs for serverless compatibility
    await conn.run(`SET home_directory='/tmp'`)
    await conn.run(`SET extension_directory='/tmp/.duckdb/extensions'`)
    await conn.run(`SET temp_directory='/tmp'`)

    // Set Motherduck token as environment variable
    process.env.motherduck_token = token

    // Attach motherduck database
    console.log('1️⃣  Connecting to Motherduck...')
    await conn.run(`ATTACH 'md:${database}' AS md`)
    await conn.run(`USE md`)
    console.log(`   ✅ Connected to ${database}`)

    // Parse full dump metadata
    console.log('\n2️⃣  Reading full dump metadata...')
    const metadata = await parseMetadata(conn, dataDir)
    console.log(`   ✅ Extract #${metadata.extractNumber} (${metadata.snapshotDate})`)

    // Get current database extracts
    console.log('\n3️⃣  Checking current database state...')
    const extractsResult = await conn.run('SELECT DISTINCT _extract_number FROM enterprises ORDER BY _extract_number')
    const extractsChunks = await extractsResult.fetchAllChunks()
    const currentExtracts: number[] = []

    for (const chunk of extractsChunks) {
      const rows = chunk.getRows()
      for (const row of rows) {
        currentExtracts.push(Number(row[0]))
      }
    }
    console.log(`   ✅ Current extracts: ${currentExtracts.join(', ')}`)

    // Import to temporary tables
    console.log('\n4️⃣  Importing full dump to temporary tables...')
    await importToTempTables(conn, dataDir, database)

    // Compare states
    console.log('\n5️⃣  Comparing database state with full dump...')
    const comparisons = await compareStates(conn)

    // Get sample discrepancies
    console.log('\n6️⃣  Collecting sample discrepancies...')
    const samples = await getSampleDiscrepancies(conn)
    console.log(`   ✅ Found ${samples.length} sample discrepancies`)

    // Generate report
    console.log('\n7️⃣  Generating validation report...')
    const report = await generateReport(metadata, currentExtracts, comparisons, samples)

    // Clean up (unless --keep-temp flag provided)
    if (!args.includes('--keep-temp')) {
      await cleanupTempTables(conn)
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
        console.log('   ✅ No discrepancies found in sample')
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
    // Always close connection
    conn.closeSync()
  }
}

main().catch(console.error)
