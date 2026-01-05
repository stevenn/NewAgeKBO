#!/usr/bin/env tsx

/**
 * Validate Database State with Full Dump
 *
 * Imports a KBO full dump into temporary tables and compares against current
 * database state to identify discrepancies. Generates detailed report to help
 * decide whether to keep temporal history or start fresh.
 *
 * Usage: npx tsx scripts/validate-with-full-dump.ts /path/to/extracted/dump [--json] [--keep-temp] [--skip-import]
 *
 * Tables Compared:
 * - enterprises (with detailed missing_in_db/orphaned_in_db)
 * - establishments (with detailed missing_in_db/orphaned_in_db)
 * - denominations (row count)
 * - addresses (row count)
 * - activities (row count)
 * - contacts (row count)
 * - branches (row count)
 * - codes (row count + missing/orphaned/data_mismatch detection)
 *
 * Note: The dump is authoritative. Records in DB but not in dump are "orphaned"
 * (should be deleted). Records in dump but not in DB are "missing" (should be added).
 *
 * Process:
 * 1. Import full dump CSVs into temporary tables (dump_*)
 * 2. Compare current database state (_is_current=true) against dump tables
 * 3. Calculate discrepancy metrics (entity counts, missing, orphaned, data mismatches)
 * 4. Collect detailed discrepancies with both MotherDuck and KBO versions
 * 5. Save discrepancies to JSON file in output/ directory for analysis
 * 6. Generate comprehensive report with recommendation
 * 7. Clean up temporary tables
 *
 * Output Files:
 * - output/discrepancies-{extract_number}-{date}.json - Detailed discrepancy report
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
  orphaned_in_db: number  // In DB but not in dump (should be deleted)
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

interface DetailedDiscrepancy {
  table: string
  key: Record<string, string | number>
  issue_type: 'missing_in_db' | 'orphaned_in_db' | 'data_mismatch'
  motherduck: Record<string, unknown> | null
  kbo: Record<string, unknown> | null
  differing_fields?: string[]
}

interface DiscrepancyReport {
  generated_at: string
  full_dump_extract: number
  full_dump_date: string
  discrepancies: DetailedDiscrepancy[]
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
    { csv: 'enterprise.csv', table: 'dump_enterprises' },
    { csv: 'establishment.csv', table: 'dump_establishments' },
    { csv: 'denomination.csv', table: 'dump_denominations' },
    { csv: 'address.csv', table: 'dump_addresses' },
    { csv: 'activity.csv', table: 'dump_activities' },
    { csv: 'contact.csv', table: 'dump_contacts' },
    { csv: 'branch.csv', table: 'dump_branches' },
    { csv: 'code.csv', table: 'dump_codes' }
  ]

  for (const { csv, table } of files) {
    const csvPath = join(dataDir, csv)

    if (!fs.existsSync(csvPath)) {
      console.log(`   ⚠ ${csv} not found, skipping...`)
      continue
    }

    process.stdout.write(`   📄 Loading ${csv}...`)
    const loadStart = Date.now()

    await conn.run(`
      CREATE OR REPLACE TABLE ${table} AS
      SELECT * FROM read_csv('${csvPath}', AUTO_DETECT=TRUE, HEADER=TRUE)
    `)

    // Get row count
    const countResult = await conn.run(`SELECT COUNT(*) FROM ${table}`)
    const countChunks = await countResult.fetchAllChunks()
    const rowCount = Number(countChunks[0].getRows()[0][0])
    const loadTime = ((Date.now() - loadStart) / 1000).toFixed(1)

    console.log(` ${rowCount.toLocaleString()} rows (${loadTime}s)`)
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
  process.stdout.write('   📊 Analyzing enterprises...')
  const entStart = Date.now()
  const entResult = await conn.run(`
    WITH current_db AS (
      SELECT enterprise_number
      FROM enterprises
      WHERE _is_current = true
    ),
    full_dump AS (
      SELECT "EnterpriseNumber" as enterprise_number
      FROM dump_enterprises
    )
    SELECT
      (SELECT COUNT(*) FROM current_db) as current_db_count,
      (SELECT COUNT(*) FROM full_dump) as full_dump_count,
      (SELECT COUNT(*) FROM full_dump WHERE enterprise_number NOT IN (SELECT enterprise_number FROM current_db)) as missing_in_db,
      (SELECT COUNT(*) FROM current_db WHERE enterprise_number NOT IN (SELECT enterprise_number FROM full_dump)) as orphaned_in_db
  `)

  const entChunks = await entResult.fetchAllChunks()
  const entRow = entChunks[0].getRows()[0]
  console.log(` (${((Date.now() - entStart) / 1000).toFixed(1)}s)`)

  const entDiff = Math.abs(Number(entRow[0]) - Number(entRow[1]))
  const entPercent = Number(entRow[1]) > 0 ? (entDiff / Number(entRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'enterprises',
    current_db_count: Number(entRow[0]),
    full_dump_count: Number(entRow[1]),
    difference: entDiff,
    difference_percent: entPercent,
    missing_in_db: Number(entRow[2]),
    orphaned_in_db: Number(entRow[3]),
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(entRow[0]).toLocaleString()} | Dump: ${Number(entRow[1]).toLocaleString()} | Missing: ${Number(entRow[2]).toLocaleString()} | Orphaned: ${Number(entRow[3]).toLocaleString()}`)

  // Compare establishments
  process.stdout.write('   📊 Analyzing establishments...')
  const estStart = Date.now()
  const estResult = await conn.run(`
    WITH current_db AS (
      SELECT establishment_number
      FROM establishments
      WHERE _is_current = true
    ),
    full_dump AS (
      SELECT "EstablishmentNumber" as establishment_number
      FROM dump_establishments
    )
    SELECT
      (SELECT COUNT(*) FROM current_db) as current_db_count,
      (SELECT COUNT(*) FROM full_dump) as full_dump_count,
      (SELECT COUNT(*) FROM full_dump WHERE establishment_number NOT IN (SELECT establishment_number FROM current_db)) as missing_in_db,
      (SELECT COUNT(*) FROM current_db WHERE establishment_number NOT IN (SELECT establishment_number FROM full_dump)) as orphaned_in_db
  `)

  const estChunks = await estResult.fetchAllChunks()
  const estRow = estChunks[0].getRows()[0]
  console.log(` (${((Date.now() - estStart) / 1000).toFixed(1)}s)`)

  const estDiff = Math.abs(Number(estRow[0]) - Number(estRow[1]))
  const estPercent = Number(estRow[1]) > 0 ? (estDiff / Number(estRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'establishments',
    current_db_count: Number(estRow[0]),
    full_dump_count: Number(estRow[1]),
    difference: estDiff,
    difference_percent: estPercent,
    missing_in_db: Number(estRow[2]),
    orphaned_in_db: Number(estRow[3]),
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(estRow[0]).toLocaleString()} | Dump: ${Number(estRow[1]).toLocaleString()} | Missing: ${Number(estRow[2]).toLocaleString()} | Orphaned: ${Number(estRow[3]).toLocaleString()}`)

  // Compare denominations (row count)
  console.log('   📊 Analyzing denominations (row count)...')
  const denomResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM denominations WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM dump_denominations) as full_dump_count
  `)

  const denomChunks = await denomResult.fetchAllChunks()
  const denomRow = denomChunks[0].getRows()[0]

  const denomDiff = Math.abs(Number(denomRow[0]) - Number(denomRow[1]))
  const denomPercent = Number(denomRow[1]) > 0 ? (denomDiff / Number(denomRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'denominations',
    current_db_count: Number(denomRow[0]),
    full_dump_count: Number(denomRow[1]),
    difference: denomDiff,
    difference_percent: denomPercent,
    missing_in_db: 0,
    orphaned_in_db: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(denomRow[0]).toLocaleString()} | Dump: ${Number(denomRow[1]).toLocaleString()} | Diff: ${denomDiff.toLocaleString()} (${denomPercent.toFixed(2)}%)`)

  // Compare addresses (row count)
  console.log('   📊 Analyzing addresses (row count)...')
  const addrResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM addresses WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM dump_addresses) as full_dump_count
  `)

  const addrChunks = await addrResult.fetchAllChunks()
  const addrRow = addrChunks[0].getRows()[0]

  const addrDiff = Math.abs(Number(addrRow[0]) - Number(addrRow[1]))
  const addrPercent = Number(addrRow[1]) > 0 ? (addrDiff / Number(addrRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'addresses',
    current_db_count: Number(addrRow[0]),
    full_dump_count: Number(addrRow[1]),
    difference: addrDiff,
    difference_percent: addrPercent,
    missing_in_db: 0,
    orphaned_in_db: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(addrRow[0]).toLocaleString()} | Dump: ${Number(addrRow[1]).toLocaleString()} | Diff: ${addrDiff.toLocaleString()} (${addrPercent.toFixed(2)}%)`)

  // Compare activities (row count)
  console.log('   📊 Analyzing activities (row count)...')
  const actResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM activities WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM dump_activities) as full_dump_count
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
    orphaned_in_db: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(actRow[0]).toLocaleString()} | Dump: ${Number(actRow[1]).toLocaleString()} | Diff: ${actDiff.toLocaleString()} (${actPercent.toFixed(2)}%)`)

  // Compare contacts (row count)
  console.log('   📊 Analyzing contacts (row count)...')
  const contactResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM contacts WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM dump_contacts) as full_dump_count
  `)

  const contactChunks = await contactResult.fetchAllChunks()
  const contactRow = contactChunks[0].getRows()[0]

  const contactDiff = Math.abs(Number(contactRow[0]) - Number(contactRow[1]))
  const contactPercent = Number(contactRow[1]) > 0 ? (contactDiff / Number(contactRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'contacts',
    current_db_count: Number(contactRow[0]),
    full_dump_count: Number(contactRow[1]),
    difference: contactDiff,
    difference_percent: contactPercent,
    missing_in_db: 0,
    orphaned_in_db: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(contactRow[0]).toLocaleString()} | Dump: ${Number(contactRow[1]).toLocaleString()} | Diff: ${contactDiff.toLocaleString()} (${contactPercent.toFixed(2)}%)`)

  // Compare branches (row count)
  console.log('   📊 Analyzing branches (row count)...')
  const branchResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM branches WHERE _is_current = true) as current_db_count,
      (SELECT COUNT(*) FROM dump_branches) as full_dump_count
  `)

  const branchChunks = await branchResult.fetchAllChunks()
  const branchRow = branchChunks[0].getRows()[0]

  const branchDiff = Math.abs(Number(branchRow[0]) - Number(branchRow[1]))
  const branchPercent = Number(branchRow[1]) > 0 ? (branchDiff / Number(branchRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'branches',
    current_db_count: Number(branchRow[0]),
    full_dump_count: Number(branchRow[1]),
    difference: branchDiff,
    difference_percent: branchPercent,
    missing_in_db: 0,
    orphaned_in_db: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(branchRow[0]).toLocaleString()} | Dump: ${Number(branchRow[1]).toLocaleString()} | Diff: ${branchDiff.toLocaleString()} (${branchPercent.toFixed(2)}%)`)

  // Compare codes (row count)
  console.log('   📊 Analyzing codes (row count)...')
  const codesResult = await conn.run(`
    SELECT
      (SELECT COUNT(*) FROM codes) as current_db_count,
      (SELECT COUNT(*) FROM dump_codes) as full_dump_count
  `)

  const codesChunks = await codesResult.fetchAllChunks()
  const codesRow = codesChunks[0].getRows()[0]

  const codesDiff = Math.abs(Number(codesRow[0]) - Number(codesRow[1]))
  const codesPercent = Number(codesRow[1]) > 0 ? (codesDiff / Number(codesRow[1])) * 100 : 0

  comparisons.push({
    table_name: 'codes',
    current_db_count: Number(codesRow[0]),
    full_dump_count: Number(codesRow[1]),
    difference: codesDiff,
    difference_percent: codesPercent,
    missing_in_db: 0,
    orphaned_in_db: 0,
    data_mismatches: 0
  })

  console.log(`      DB: ${Number(codesRow[0]).toLocaleString()} | Dump: ${Number(codesRow[1]).toLocaleString()} | Diff: ${codesDiff.toLocaleString()} (${codesPercent.toFixed(2)}%)`)

  console.log('\n   ✅ Comparison complete')

  return comparisons
}

/**
 * Collect all detailed discrepancies for local storage
 */
async function collectDetailedDiscrepancies(conn: any): Promise<DetailedDiscrepancy[]> {
  console.log('\n📋 Collecting detailed discrepancies...\n')
  const discrepancies: DetailedDiscrepancy[] = []
  const totalStart = Date.now()

  // Enterprises missing in DB
  process.stdout.write('   📄 Enterprises missing in DB...')
  const entMissingStart = Date.now()
  const entMissingInDb = await conn.run(`
    SELECT *
    FROM dump_enterprises
    WHERE "EnterpriseNumber" NOT IN (SELECT enterprise_number FROM enterprises WHERE _is_current = true)
  `)
  for (const chunk of await entMissingInDb.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      const kboRecord: Record<string, unknown> = {}
      const columns = chunk.columnNames
      columns.forEach((col: string, i: number) => { kboRecord[col] = row[i] })
      discrepancies.push({
        table: 'enterprises',
        key: { enterprise_number: String(kboRecord['EnterpriseNumber']) },
        issue_type: 'missing_in_db',
        motherduck: null,
        kbo: kboRecord
      })
    }
  }
  const entMissingCount = discrepancies.filter(d => d.table === 'enterprises' && d.issue_type === 'missing_in_db').length
  console.log(` ${entMissingCount.toLocaleString()} found (${((Date.now() - entMissingStart) / 1000).toFixed(1)}s)`)

  // Enterprises orphaned in DB (in DB but not in dump - should be deleted)
  process.stdout.write('   📄 Enterprises orphaned in DB...')
  const entOrphanedStart = Date.now()
  const entOrphaned = await conn.run(`
    SELECT *
    FROM enterprises
    WHERE _is_current = true
      AND enterprise_number NOT IN (SELECT "EnterpriseNumber" FROM dump_enterprises)
  `)
  for (const chunk of await entOrphaned.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      const mdRecord: Record<string, unknown> = {}
      const columns = chunk.columnNames
      columns.forEach((col: string, i: number) => { mdRecord[col] = row[i] })
      discrepancies.push({
        table: 'enterprises',
        key: { enterprise_number: String(mdRecord['enterprise_number']) },
        issue_type: 'orphaned_in_db',
        motherduck: mdRecord,
        kbo: null
      })
    }
  }
  const entOrphanedCount = discrepancies.filter(d => d.table === 'enterprises' && d.issue_type === 'orphaned_in_db').length
  console.log(` ${entOrphanedCount.toLocaleString()} found (${((Date.now() - entOrphanedStart) / 1000).toFixed(1)}s)`)

  // Establishments missing in DB
  process.stdout.write('   📄 Establishments missing in DB...')
  const estMissingStart = Date.now()
  const estMissingInDb = await conn.run(`
    SELECT *
    FROM dump_establishments
    WHERE "EstablishmentNumber" NOT IN (SELECT establishment_number FROM establishments WHERE _is_current = true)
  `)
  for (const chunk of await estMissingInDb.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      const kboRecord: Record<string, unknown> = {}
      const columns = chunk.columnNames
      columns.forEach((col: string, i: number) => { kboRecord[col] = row[i] })
      discrepancies.push({
        table: 'establishments',
        key: { establishment_number: String(kboRecord['EstablishmentNumber']) },
        issue_type: 'missing_in_db',
        motherduck: null,
        kbo: kboRecord
      })
    }
  }
  const estMissingCount = discrepancies.filter(d => d.table === 'establishments' && d.issue_type === 'missing_in_db').length
  console.log(` ${estMissingCount.toLocaleString()} found (${((Date.now() - estMissingStart) / 1000).toFixed(1)}s)`)

  // Establishments orphaned in DB (in DB but not in dump - should be deleted)
  process.stdout.write('   📄 Establishments orphaned in DB...')
  const estOrphanedStart = Date.now()
  const estOrphaned = await conn.run(`
    SELECT *
    FROM establishments
    WHERE _is_current = true
      AND establishment_number NOT IN (SELECT "EstablishmentNumber" FROM dump_establishments)
  `)
  for (const chunk of await estOrphaned.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      const mdRecord: Record<string, unknown> = {}
      const columns = chunk.columnNames
      columns.forEach((col: string, i: number) => { mdRecord[col] = row[i] })
      discrepancies.push({
        table: 'establishments',
        key: { establishment_number: String(mdRecord['establishment_number']) },
        issue_type: 'orphaned_in_db',
        motherduck: mdRecord,
        kbo: null
      })
    }
  }
  const estOrphanedCount = discrepancies.filter(d => d.table === 'establishments' && d.issue_type === 'orphaned_in_db').length
  console.log(` ${estOrphanedCount.toLocaleString()} found (${((Date.now() - estOrphanedStart) / 1000).toFixed(1)}s)`)

  // Denominations - compare on business key (entity_number, denomination_type, language, denomination)
  // Using LEFT JOIN instead of NOT IN to handle NULL values correctly
  process.stdout.write('   📄 Denominations orphaned in DB...')
  const denomOrphanedStart = Date.now()
  const denomOrphaned = await conn.run(`
    SELECT d.entity_number, d.denomination_type, d.language, d.denomination
    FROM denominations d
    LEFT JOIN dump_denominations dd
      ON d.entity_number = dd."EntityNumber"::VARCHAR
      AND d.denomination_type = dd."TypeOfDenomination"::VARCHAR
      AND d.language = dd."Language"::VARCHAR
      AND d.denomination = dd."Denomination"::VARCHAR
    WHERE d._is_current = true
      AND dd."EntityNumber" IS NULL
  `)
  for (const chunk of await denomOrphaned.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'denominations',
        key: {
          entity_number: String(row[0]),
          denomination_type: String(row[1]),
          language: String(row[2])
        },
        issue_type: 'orphaned_in_db',
        motherduck: { denomination: row[3] },
        kbo: null
      })
    }
  }
  const denomOrphanedCount = discrepancies.filter(d => d.table === 'denominations' && d.issue_type === 'orphaned_in_db').length
  console.log(` ${denomOrphanedCount.toLocaleString()} found (${((Date.now() - denomOrphanedStart) / 1000).toFixed(1)}s)`)

  process.stdout.write('   📄 Denominations missing in DB...')
  const denomMissingStart = Date.now()
  const denomMissing = await conn.run(`
    SELECT dd."EntityNumber"::VARCHAR, dd."TypeOfDenomination"::VARCHAR, dd."Language"::VARCHAR, dd."Denomination"::VARCHAR
    FROM dump_denominations dd
    LEFT JOIN denominations d
      ON dd."EntityNumber"::VARCHAR = d.entity_number
      AND dd."TypeOfDenomination"::VARCHAR = d.denomination_type
      AND dd."Language"::VARCHAR = d.language
      AND dd."Denomination"::VARCHAR = d.denomination
      AND d._is_current = true
    WHERE d.entity_number IS NULL
  `)
  for (const chunk of await denomMissing.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'denominations',
        key: {
          entity_number: String(row[0]),
          denomination_type: String(row[1]),
          language: String(row[2])
        },
        issue_type: 'missing_in_db',
        motherduck: null,
        kbo: { denomination: row[3] }
      })
    }
  }
  const denomMissingCount = discrepancies.filter(d => d.table === 'denominations' && d.issue_type === 'missing_in_db').length
  console.log(` ${denomMissingCount.toLocaleString()} found (${((Date.now() - denomMissingStart) / 1000).toFixed(1)}s)`)

  // Activities - compare on business key
  process.stdout.write('   📄 Activities orphaned in DB...')
  const actOrphanedStart = Date.now()
  const actOrphaned = await conn.run(`
    SELECT a.entity_number, a.activity_group, a.nace_version, a.nace_code, a.classification
    FROM activities a
    LEFT JOIN dump_activities da
      ON a.entity_number = da."EntityNumber"::VARCHAR
      AND a.activity_group = da."ActivityGroup"::VARCHAR
      AND a.nace_version = da."NaceVersion"::VARCHAR
      AND a.nace_code = da."NaceCode"::VARCHAR
      AND a.classification = da."Classification"::VARCHAR
    WHERE a._is_current = true
      AND da."EntityNumber" IS NULL
  `)
  for (const chunk of await actOrphaned.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'activities',
        key: {
          entity_number: String(row[0]),
          activity_group: String(row[1]),
          nace_version: String(row[2]),
          nace_code: String(row[3]),
          classification: String(row[4])
        },
        issue_type: 'orphaned_in_db',
        motherduck: null,
        kbo: null
      })
    }
  }
  const actOrphanedCount = discrepancies.filter(d => d.table === 'activities' && d.issue_type === 'orphaned_in_db').length
  console.log(` ${actOrphanedCount.toLocaleString()} found (${((Date.now() - actOrphanedStart) / 1000).toFixed(1)}s)`)

  process.stdout.write('   📄 Activities missing in DB...')
  const actMissingStart = Date.now()
  const actMissing = await conn.run(`
    SELECT da."EntityNumber"::VARCHAR, da."ActivityGroup"::VARCHAR, da."NaceVersion"::VARCHAR, da."NaceCode"::VARCHAR, da."Classification"::VARCHAR
    FROM dump_activities da
    LEFT JOIN activities a
      ON da."EntityNumber"::VARCHAR = a.entity_number
      AND da."ActivityGroup"::VARCHAR = a.activity_group
      AND da."NaceVersion"::VARCHAR = a.nace_version
      AND da."NaceCode"::VARCHAR = a.nace_code
      AND da."Classification"::VARCHAR = a.classification
      AND a._is_current = true
    WHERE a.entity_number IS NULL
  `)
  for (const chunk of await actMissing.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'activities',
        key: {
          entity_number: String(row[0]),
          activity_group: String(row[1]),
          nace_version: String(row[2]),
          nace_code: String(row[3]),
          classification: String(row[4])
        },
        issue_type: 'missing_in_db',
        motherduck: null,
        kbo: null
      })
    }
  }
  const actMissingCount = discrepancies.filter(d => d.table === 'activities' && d.issue_type === 'missing_in_db').length
  console.log(` ${actMissingCount.toLocaleString()} found (${((Date.now() - actMissingStart) / 1000).toFixed(1)}s)`)

  // Contacts - compare on business key
  process.stdout.write('   📄 Contacts orphaned in DB...')
  const contactOrphanedStart = Date.now()
  const contactOrphaned = await conn.run(`
    SELECT c.entity_number, c.entity_contact, c.contact_type, c.contact_value
    FROM contacts c
    LEFT JOIN dump_contacts dc
      ON c.entity_number = dc."EntityNumber"::VARCHAR
      AND c.entity_contact = dc."EntityContact"::VARCHAR
      AND c.contact_type = dc."ContactType"::VARCHAR
      AND c.contact_value = dc."Value"::VARCHAR
    WHERE c._is_current = true
      AND dc."EntityNumber" IS NULL
  `)
  for (const chunk of await contactOrphaned.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'contacts',
        key: {
          entity_number: String(row[0]),
          entity_contact: String(row[1]),
          contact_type: String(row[2])
        },
        issue_type: 'orphaned_in_db',
        motherduck: { contact_value: row[3] },
        kbo: null
      })
    }
  }
  const contactOrphanedCount = discrepancies.filter(d => d.table === 'contacts' && d.issue_type === 'orphaned_in_db').length
  console.log(` ${contactOrphanedCount.toLocaleString()} found (${((Date.now() - contactOrphanedStart) / 1000).toFixed(1)}s)`)

  process.stdout.write('   📄 Contacts missing in DB...')
  const contactMissingStart = Date.now()
  const contactMissing = await conn.run(`
    SELECT dc."EntityNumber"::VARCHAR, dc."EntityContact"::VARCHAR, dc."ContactType"::VARCHAR, dc."Value"::VARCHAR
    FROM dump_contacts dc
    LEFT JOIN contacts c
      ON dc."EntityNumber"::VARCHAR = c.entity_number
      AND dc."EntityContact"::VARCHAR = c.entity_contact
      AND dc."ContactType"::VARCHAR = c.contact_type
      AND dc."Value"::VARCHAR = c.contact_value
      AND c._is_current = true
    WHERE c.entity_number IS NULL
  `)
  for (const chunk of await contactMissing.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'contacts',
        key: {
          entity_number: String(row[0]),
          entity_contact: String(row[1]),
          contact_type: String(row[2])
        },
        issue_type: 'missing_in_db',
        motherduck: null,
        kbo: { contact_value: row[3] }
      })
    }
  }
  const contactMissingCount = discrepancies.filter(d => d.table === 'contacts' && d.issue_type === 'missing_in_db').length
  console.log(` ${contactMissingCount.toLocaleString()} found (${((Date.now() - contactMissingStart) / 1000).toFixed(1)}s)`)

  // Codes - check for missing and data mismatches
  // The codes table uses (Category, Code, Language) as composite key
  process.stdout.write('   📄 Codes missing in DB...')
  const codesMissingStart = Date.now()
  const codesMissingInDb = await conn.run(`
    SELECT *
    FROM dump_codes
    WHERE ("Category", "Code", "Language") NOT IN (
      SELECT category, code, language FROM codes
    )
  `)
  for (const chunk of await codesMissingInDb.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      const kboRecord: Record<string, unknown> = {}
      const columns = chunk.columnNames
      columns.forEach((col: string, i: number) => { kboRecord[col] = row[i] })
      discrepancies.push({
        table: 'codes',
        key: {
          category: String(kboRecord['Category']),
          code: String(kboRecord['Code']),
          language: String(kboRecord['Language'])
        },
        issue_type: 'missing_in_db',
        motherduck: null,
        kbo: kboRecord
      })
    }
  }
  const codesMissingCount = discrepancies.filter(d => d.table === 'codes' && d.issue_type === 'missing_in_db').length
  console.log(` ${codesMissingCount.toLocaleString()} found (${((Date.now() - codesMissingStart) / 1000).toFixed(1)}s)`)

  process.stdout.write('   📄 Codes orphaned in DB...')
  const codesOrphanedStart = Date.now()
  const codesOrphaned = await conn.run(`
    SELECT *
    FROM codes
    WHERE (category, code, language) NOT IN (
      SELECT "Category", "Code", "Language" FROM dump_codes
    )
  `)
  for (const chunk of await codesOrphaned.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      const mdRecord: Record<string, unknown> = {}
      const columns = chunk.columnNames
      columns.forEach((col: string, i: number) => { mdRecord[col] = row[i] })
      discrepancies.push({
        table: 'codes',
        key: {
          category: String(mdRecord['category']),
          code: String(mdRecord['code']),
          language: String(mdRecord['language'])
        },
        issue_type: 'orphaned_in_db',
        motherduck: mdRecord,
        kbo: null
      })
    }
  }
  const codesOrphanedCount = discrepancies.filter(d => d.table === 'codes' && d.issue_type === 'orphaned_in_db').length
  console.log(` ${codesOrphanedCount.toLocaleString()} found (${((Date.now() - codesOrphanedStart) / 1000).toFixed(1)}s)`)

  // Codes data mismatches (same key but different description)
  process.stdout.write('   📄 Codes with different descriptions...')
  const codesMismatchStart = Date.now()
  const codesMismatch = await conn.run(`
    SELECT
      c.category, c.code, c.language, c.description as md_description,
      n."Description" as kbo_description
    FROM codes c
    JOIN dump_codes n ON c.category = n."Category" AND c.code = n."Code" AND c.language = n."Language"
    WHERE c.description != n."Description"
  `)
  for (const chunk of await codesMismatch.fetchAllChunks()) {
    for (const row of chunk.getRows()) {
      discrepancies.push({
        table: 'codes',
        key: {
          category: String(row[0]),
          code: String(row[1]),
          language: String(row[2])
        },
        issue_type: 'data_mismatch',
        motherduck: { description: row[3] },
        kbo: { description: row[4] },
        differing_fields: ['description']
      })
    }
  }
  const codesMismatchCount = discrepancies.filter(d => d.table === 'codes' && d.issue_type === 'data_mismatch').length
  console.log(` ${codesMismatchCount.toLocaleString()} found (${((Date.now() - codesMismatchStart) / 1000).toFixed(1)}s)`)

  const totalTime = ((Date.now() - totalStart) / 1000).toFixed(1)
  console.log(`\n   ✅ Total discrepancies collected: ${discrepancies.length.toLocaleString()} (${totalTime}s)`)
  return discrepancies
}

/**
 * Save discrepancies to JSON file
 */
function saveDiscrepancies(
  discrepancies: DetailedDiscrepancy[],
  metadata: Metadata,
  outputDir: string
): string {
  const report: DiscrepancyReport = {
    generated_at: new Date().toISOString(),
    full_dump_extract: metadata.extractNumber,
    full_dump_date: metadata.snapshotDate,
    discrepancies
  }

  const filename = `discrepancies-${metadata.extractNumber}-${new Date().toISOString().split('T')[0]}.json`
  const outputPath = join(outputDir, filename)

  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2))
  return outputPath
}

/**
 * Get sample discrepancies for investigation
 */
async function getSampleDiscrepancies(conn: any): Promise<ValidationReport['sample_discrepancies']> {
  const samples: ValidationReport['sample_discrepancies'] = []

  // Find 5 enterprises in DB but not in dump (orphaned)
  const orphanedResult = await conn.run(`
    SELECT
      enterprise_number,
      status,
      primary_name
    FROM enterprises
    WHERE _is_current = true
      AND enterprise_number NOT IN (SELECT "EnterpriseNumber" FROM dump_enterprises)
    LIMIT 5
  `)

  const orphanedChunks = await orphanedResult.fetchAllChunks()
  for (const chunk of orphanedChunks) {
    const rows = chunk.getRows()
    for (const row of rows) {
      samples.push({
        entity_id: String(row[0]),
        issue_type: 'orphaned_in_db',
        db_value: `${row[1]} - ${row[2]}`,
        dump_value: 'NOT IN DUMP'
      })
    }
  }

  // Find 5 enterprises in dump but not in DB
  const missingInDbResult = await conn.run(`
    SELECT
      "EnterpriseNumber" as enterprise_number,
      "Status" as status
    FROM dump_enterprises
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

  // Find specific table stats
  const entStats = comparisons.find(c => c.table_name === 'enterprises')
  const estStats = comparisons.find(c => c.table_name === 'establishments')
  const actStats = comparisons.find(c => c.table_name === 'activities')
  const codesStats = comparisons.find(c => c.table_name === 'codes')

  // Generate summary
  const summary = `
Validation Summary:
- Current database has ${totalCurrentDb.toLocaleString()} current records
- Full dump #${metadata.extractNumber} has ${totalFullDump.toLocaleString()} records
- Overall discrepancy: ${overallPercent.toFixed(2)}%

Key Findings:
- Enterprises: ${entStats?.difference_percent.toFixed(2)}% difference (${entStats?.missing_in_db} missing in DB, ${entStats?.orphaned_in_db} orphaned)
- Establishments: ${estStats?.difference_percent.toFixed(2)}% difference (${estStats?.missing_in_db} missing in DB, ${estStats?.orphaned_in_db} orphaned)
- Activities: ${actStats?.difference_percent.toFixed(2)}% difference
- Codes: ${codesStats?.difference_percent.toFixed(2)}% difference

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

  const tables = [
    'dump_enterprises',
    'dump_establishments',
    'dump_denominations',
    'dump_addresses',
    'dump_activities',
    'dump_contacts',
    'dump_branches',
    'dump_codes'
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

    // Import to temporary tables (unless --skip-import flag)
    if (args.includes('--skip-import')) {
      console.log('\n4️⃣  Skipping import (--skip-import flag set, reusing existing dump_* tables)')
    } else {
      console.log('\n4️⃣  Importing full dump to temporary tables...')
      await importToTempTables(conn, dataDir, database)
    }

    // Compare states
    console.log('\n5️⃣  Comparing database state with full dump...')
    const comparisons = await compareStates(conn)

    // Get sample discrepancies
    console.log('\n6️⃣  Collecting sample discrepancies...')
    const samples = await getSampleDiscrepancies(conn)
    console.log(`   ✅ Found ${samples.length} sample discrepancies`)

    // Collect detailed discrepancies for local storage
    console.log('\n7️⃣  Collecting detailed discrepancies for local storage...')
    const detailedDiscrepancies = await collectDetailedDiscrepancies(conn)

    // Save discrepancies to JSON file
    const outputDir = resolve(__dirname, '../output')
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true })
    }
    const discrepancyFile = saveDiscrepancies(detailedDiscrepancies, metadata, outputDir)
    console.log(`   ✅ Discrepancies saved to: ${discrepancyFile}`)

    // Generate report
    console.log('\n8️⃣  Generating validation report...')
    const report = await generateReport(metadata, currentExtracts, comparisons, samples)

    // Clean up (unless --keep-temp flag provided)
    if (!args.includes('--keep-temp')) {
      console.log('\n9️⃣  Cleaning up temporary tables...')
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
