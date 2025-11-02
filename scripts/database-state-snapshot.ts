#!/usr/bin/env tsx
/**
 * Database State Snapshot
 *
 * Captures comprehensive metrics about the current database state:
 * - Extract numbers present
 * - Row counts per table (total, current, historical)
 * - Date ranges
 * - Sample data validation
 * - Anomaly detection
 *
 * Usage: npx tsx scripts/database-state-snapshot.ts [--json]
 *
 * Use --json flag to output JSON for programmatic processing
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, executeQuery, closeMotherduck } from '../lib/motherduck'

interface TableStats {
  table_name: string
  total_rows: number
  current_rows: number
  historical_rows: number
  extract_count: number
  min_extract: number
  max_extract: number
  min_snapshot_date: string
  max_snapshot_date: string
}

interface ExtractInfo {
  extract_number: number
  snapshot_date: string
  record_count: number
}

interface SampleEntity {
  enterprise_number: string
  version_count: number
  current_status: string
  current_name: string | null
  first_extract: number
  last_extract: number
}

interface DatabaseSnapshot {
  timestamp: string
  extracts: ExtractInfo[]
  table_stats: TableStats[]
  sample_entities: SampleEntity[]
  anomalies: string[]
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

async function captureSnapshot(): Promise<DatabaseSnapshot> {
  const db = await connectMotherduck()
  await executeQuery(db, `USE ${process.env.MOTHERDUCK_DATABASE}`)

  const snapshot: DatabaseSnapshot = {
    timestamp: new Date().toISOString(),
    extracts: [],
    table_stats: [],
    sample_entities: [],
    anomalies: []
  }

  try {
    console.log('📊 Capturing database state snapshot...\n')

    // 1. Extract numbers
    console.log('🔢 Analyzing extract numbers...')
    const extractData = await executeQuery<{
      _extract_number: number
      _snapshot_date: string
      record_count: number
    }>(db, `
      SELECT
        _extract_number,
        _snapshot_date::VARCHAR as _snapshot_date,
        COUNT(*) as record_count
      FROM enterprises
      GROUP BY _extract_number, _snapshot_date
      ORDER BY _extract_number
    `)

    snapshot.extracts = extractData.map(e => ({
      extract_number: e._extract_number,
      snapshot_date: e._snapshot_date,
      record_count: Number(e.record_count)
    }))

    console.log(`   ✓ Found ${snapshot.extracts.length} extract(s)`)
    console.log(`   Range: ${snapshot.extracts[0]?.extract_number} to ${snapshot.extracts[snapshot.extracts.length - 1]?.extract_number}\n`)

    // 2. Table statistics
    console.log('📋 Analyzing table statistics...')
    for (const tableName of TABLES) {
      try {
        const stats = await executeQuery<{
          total_rows: number
          current_rows: number
          historical_rows: number
          extract_count: number
          min_extract: number
          max_extract: number
          min_snapshot_date: string
          max_snapshot_date: string
        }>(db, `
          SELECT
            COUNT(*) as total_rows,
            SUM(CASE WHEN _is_current THEN 1 ELSE 0 END) as current_rows,
            SUM(CASE WHEN NOT _is_current THEN 1 ELSE 0 END) as historical_rows,
            COUNT(DISTINCT _extract_number) as extract_count,
            MIN(_extract_number) as min_extract,
            MAX(_extract_number) as max_extract,
            MIN(_snapshot_date)::VARCHAR as min_snapshot_date,
            MAX(_snapshot_date)::VARCHAR as max_snapshot_date
          FROM ${tableName}
        `)

        if (stats.length > 0) {
          const s = stats[0]
          snapshot.table_stats.push({
            table_name: tableName,
            total_rows: Number(s.total_rows),
            current_rows: Number(s.current_rows),
            historical_rows: Number(s.historical_rows),
            extract_count: Number(s.extract_count),
            min_extract: Number(s.min_extract),
            max_extract: Number(s.max_extract),
            min_snapshot_date: s.min_snapshot_date,
            max_snapshot_date: s.max_snapshot_date
          })
          console.log(`   ✓ ${tableName}: ${Number(s.total_rows).toLocaleString()} rows (${Number(s.current_rows).toLocaleString()} current)`)
        }
      } catch (err) {
        console.log(`   ⚠ ${tableName}: Error - ${err instanceof Error ? err.message : 'Unknown'}`)
        snapshot.anomalies.push(`Error analyzing ${tableName}: ${err instanceof Error ? err.message : 'Unknown'}`)
      }
    }
    console.log('')

    // 3. Sample entities
    console.log('🔍 Sampling enterprise data...')
    const samples = await executeQuery<{
      enterprise_number: string
      version_count: number
      current_status: string
      current_name: string | null
      first_extract: number
      last_extract: number
    }>(db, `
      WITH enterprise_versions AS (
        SELECT
          enterprise_number,
          COUNT(*) as version_count,
          MIN(_extract_number) as first_extract,
          MAX(_extract_number) as last_extract
        FROM enterprises
        GROUP BY enterprise_number
        HAVING COUNT(*) > 1
        ORDER BY RANDOM()
        LIMIT 5
      )
      SELECT
        ev.enterprise_number,
        ev.version_count,
        e.status as current_status,
        e.primary_name as current_name,
        ev.first_extract,
        ev.last_extract
      FROM enterprise_versions ev
      JOIN enterprises e ON e.enterprise_number = ev.enterprise_number AND e._is_current = true
    `)

    snapshot.sample_entities = samples.map(s => ({
      enterprise_number: s.enterprise_number,
      version_count: Number(s.version_count),
      current_status: s.current_status,
      current_name: s.current_name,
      first_extract: Number(s.first_extract),
      last_extract: Number(s.last_extract)
    }))

    console.log(`   ✓ Sampled ${snapshot.sample_entities.length} entities with version history\n`)

    // 4. Anomaly detection
    console.log('🔍 Checking for anomalies...')

    // Check for missing _is_current flags
    const missingCurrent = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM enterprises
      WHERE _is_current IS NULL
    `)
    if (Number(missingCurrent[0].count) > 0) {
      const msg = `Found ${Number(missingCurrent[0].count)} enterprises with NULL _is_current flag`
      snapshot.anomalies.push(msg)
      console.log(`   ⚠ ${msg}`)
    }

    // Check for multiple current versions of same entity
    const duplicateCurrent = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM (
        SELECT enterprise_number
        FROM enterprises
        WHERE _is_current = true
        GROUP BY enterprise_number
        HAVING COUNT(*) > 1
      )
    `)
    if (Number(duplicateCurrent[0].count) > 0) {
      const msg = `Found ${Number(duplicateCurrent[0].count)} enterprises with multiple current versions`
      snapshot.anomalies.push(msg)
      console.log(`   ⚠ ${msg}`)
    }

    // Check for gaps in extract numbers
    if (snapshot.extracts.length > 1) {
      const extractNumbers = snapshot.extracts.map(e => e.extract_number)
      for (let i = 0; i < extractNumbers.length - 1; i++) {
        const gap = extractNumbers[i + 1] - extractNumbers[i]
        if (gap > 1) {
          const msg = `Gap in extract numbers: ${extractNumbers[i]} → ${extractNumbers[i + 1]} (${gap - 1} missing)`
          snapshot.anomalies.push(msg)
          console.log(`   ⚠ ${msg}`)
        }
      }
    }

    // Check _deleted_at_extract population
    const missingDeletedAt = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM enterprises
      WHERE _is_current = false AND _deleted_at_extract IS NULL
    `)
    if (Number(missingDeletedAt[0].count) > 0) {
      const msg = `Found ${Number(missingDeletedAt[0].count)} historical enterprises with NULL _deleted_at_extract (known limitation)`
      snapshot.anomalies.push(msg)
      console.log(`   ℹ ${msg}`)
    }

    if (snapshot.anomalies.length === 0) {
      console.log('   ✓ No critical anomalies detected')
    }

  } finally {
    await closeMotherduck(db)
  }

  return snapshot
}

async function main() {
  const jsonOutput = process.argv.includes('--json')

  try {
    const snapshot = await captureSnapshot()

    if (jsonOutput) {
      console.log(JSON.stringify(snapshot, null, 2))
    } else {
      // Human-readable summary
      console.log('\n' + '='.repeat(80))
      console.log('DATABASE STATE SNAPSHOT SUMMARY')
      console.log('='.repeat(80))
      console.log(`\nTimestamp: ${snapshot.timestamp}`)

      console.log(`\nExtracts: ${snapshot.extracts.length} total`)
      snapshot.extracts.forEach(e => {
        console.log(`  • Extract ${e.extract_number}: ${e.snapshot_date} (${e.record_count.toLocaleString()} enterprise records)`)
      })

      console.log('\nTable Statistics:')
      const totalCurrent = snapshot.table_stats.reduce((sum, t) => sum + t.current_rows, 0)
      const totalHistorical = snapshot.table_stats.reduce((sum, t) => sum + t.historical_rows, 0)
      console.log(`  Total current records: ${totalCurrent.toLocaleString()}`)
      console.log(`  Total historical records: ${totalHistorical.toLocaleString()}`)
      console.log(`  Total records: ${(totalCurrent + totalHistorical).toLocaleString()}`)

      if (snapshot.sample_entities.length > 0) {
        console.log('\nSample Entities with Version History:')
        snapshot.sample_entities.forEach(e => {
          console.log(`  • ${e.enterprise_number}: ${e.version_count} versions (${e.first_extract}→${e.last_extract})`)
          console.log(`    Current: ${e.current_status} - ${e.current_name || 'N/A'}`)
        })
      }

      if (snapshot.anomalies.length > 0) {
        console.log('\nAnomalies:')
        snapshot.anomalies.forEach(a => {
          console.log(`  ⚠ ${a}`)
        })
      }

      console.log('\n' + '='.repeat(80))
      console.log(`\nSnapshot saved! You can run with --json flag to get machine-readable output.`)
    }
  } catch (error) {
    console.error('❌ Failed to capture database snapshot:', error)
    process.exit(1)
  }
}

main()
