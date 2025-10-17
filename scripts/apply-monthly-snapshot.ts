#!/usr/bin/env tsx

/**
 * Apply monthly KBO snapshot (full dump)
 * Purpose: Import new monthly snapshot while maintaining 24-month history
 *
 * IMPORTANT: Monthly snapshots are MANUAL CLI-ONLY operations
 * - Full dumps are too large for ZIP processing (2+ GB)
 * - Must extract ZIP externally first
 * - Run this script pointing to extracted CSV directory
 * - Direct ETL to Motherduck
 * - NO webapp automation, NO cron
 *
 * Strategy:
 * 1. Mark all current records as historical (_is_current = false)
 * 2. Import new full dump from directory with _is_current = true
 * 3. Clean up snapshots older than 24 months
 *
 * Storage: ~2.4 GB for 24 months (acceptable at ~$0.05/month on Motherduck)
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import * as fs from 'fs'
import * as path from 'path'
import { parse } from 'csv-parse/sync'
import { connectMotherduck, closeMotherduck } from '../lib/motherduck'

interface MetaRecord {
  Variable?: string
  Value?: string
}

interface Metadata {
  SnapshotDate: string
  ExtractTimestamp: string
  ExtractType: string
  ExtractNumber: string
  Version: string
}

interface SnapshotStats {
  metadata: Metadata
  recordsMarkedHistorical: number
  recordsImported: number
  recordsCleaned: number
  tablesProcessed: string[]
}

/**
 * Parse metadata from meta.csv in directory
 */
async function parseMetadata(dataDir: string): Promise<Metadata> {
  const metaPath = path.join(dataDir, 'meta.csv')
  const metaContent = fs.readFileSync(metaPath, 'utf-8')
  const metaRecords = parse(metaContent, {
    columns: true,
    skip_empty_lines: true
  }) as MetaRecord[]

  return {
    SnapshotDate: metaRecords.find((r) => r.Variable === 'SnapshotDate')?.Value || metaRecords[0]?.Value || '',
    ExtractTimestamp: metaRecords.find((r) => r.Variable === 'ExtractTimestamp')?.Value || metaRecords[1]?.Value || '',
    ExtractType: metaRecords.find((r) => r.Variable === 'ExtractType')?.Value || metaRecords[2]?.Value || '',
    ExtractNumber: metaRecords.find((r) => r.Variable === 'ExtractNumber')?.Value || metaRecords[3]?.Value || '',
    Version: metaRecords.find((r) => r.Variable === 'Version')?.Value || metaRecords[4]?.Value || ''
  }
}

/**
 * Convert DD-MM-YYYY to YYYY-MM-DD for SQL
 */
function convertDateFormat(ddmmyyyy: string): string {
  const [day, month, year] = ddmmyyyy.split('-')
  return `${year}-${month}-${day}`
}

/**
 * Mark all current records as historical
 */
async function markCurrentAsHistorical(db: any): Promise<number> {
  console.log('\n📝 Marking current records as historical...')

  const tables = [
    'enterprises',
    'establishments',
    'denominations',
    'addresses',
    'activities',
    'contacts',
    'branches'
  ]

  let totalMarked = 0

  for (const table of tables) {
    const result = await db.all(`
      UPDATE ${table}
      SET _is_current = false
      WHERE _is_current = true
      RETURNING COUNT(*) as count
    `)

    const count = result[0]?.count || 0
    totalMarked += count
    console.log(`   ✓ ${table}: ${count.toLocaleString()} records marked historical`)
  }

  return totalMarked
}

/**
 * Import full snapshot from CSV directory
 */
async function importFullSnapshot(
  db: any,
  dataDir: string,
  metadata: Metadata
): Promise<{ recordsImported: number; tablesProcessed: string[] }> {
  console.log('\n📥 Importing full snapshot from directory...')

  const snapshotDate = convertDateFormat(metadata.SnapshotDate)
  const extractNumber = parseInt(metadata.ExtractNumber)

  const tableMapping = {
    'enterprise.csv': 'enterprises',
    'establishment.csv': 'establishments',
    'denomination.csv': 'denominations',
    'address.csv': 'addresses',
    'activity.csv': 'activities',
    'contact.csv': 'contacts',
    'branch.csv': 'branches'
  }

  let totalImported = 0
  const tablesProcessed: string[] = []

  for (const [csvFile, tableName] of Object.entries(tableMapping)) {
    try {
      const csvPath = path.join(dataDir, csvFile)

      if (!fs.existsSync(csvPath)) {
        console.log(`   ℹ️  ${csvFile}: Not found in directory`)
        continue
      }

      console.log(`   🔄 Processing ${csvFile}...`)

      const content = fs.readFileSync(csvPath, 'utf-8')
      const records = parse(content, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true
      }) as Record<string, string>[]

      if (records.length === 0) {
        console.log(`   ⚠️  ${csvFile}: No records found`)
        continue
      }

      // Build INSERT statement with temporal columns
      const columns = Object.keys(records[0])
      const allColumns = [...columns, '_snapshot_date', '_extract_number', '_is_current']

      // Process in batches of 10,000 to avoid SQL statement size limits
      const batchSize = 10000
      let imported = 0

      for (let i = 0; i < records.length; i += batchSize) {
        const batch = records.slice(i, i + batchSize)

        const values = batch.map(record => {
          const recordValues = columns.map(col => {
            const val = record[col]
            return val === '' || val === null ? 'NULL' : `'${val.replace(/'/g, "''")}'`
          })
          return `(${recordValues.join(',')}, '${snapshotDate}', ${extractNumber}, true)`
        }).join(',\n        ')

        const sql = `
          INSERT INTO ${tableName} (${allColumns.join(', ')})
          VALUES
          ${values}
        `

        await db.exec(sql)
        imported += batch.length

        if (records.length > batchSize) {
          console.log(`      • Imported ${imported.toLocaleString()} / ${records.length.toLocaleString()} records`)
        }
      }

      totalImported += imported
      tablesProcessed.push(tableName)
      console.log(`   ✓ ${csvFile}: ${imported.toLocaleString()} records imported`)

    } catch (error: any) {
      console.error(`   ❌ ${csvFile}: ${error.message}`)
      throw error
    }
  }

  return { recordsImported: totalImported, tablesProcessed }
}

/**
 * Clean up snapshots older than retention period (24 months)
 */
async function cleanupOldSnapshots(db: any, retentionMonths: number = 24): Promise<number> {
  console.log(`\n🗑️  Cleaning up snapshots older than ${retentionMonths} months...`)

  const tables = [
    'enterprises',
    'establishments',
    'denominations',
    'addresses',
    'activities',
    'contacts',
    'branches'
  ]

  let totalCleaned = 0

  for (const table of tables) {
    const result = await db.all(`
      DELETE FROM ${table}
      WHERE _snapshot_date < CURRENT_DATE - INTERVAL '${retentionMonths} months'
      RETURNING COUNT(*) as count
    `)

    const count = result[0]?.count || 0
    totalCleaned += count

    if (count > 0) {
      console.log(`   ✓ ${table}: ${count.toLocaleString()} old records deleted`)
    }
  }

  if (totalCleaned === 0) {
    console.log('   ℹ️  No old records to clean up')
  }

  return totalCleaned
}

/**
 * Process monthly snapshot from CSV directory
 */
async function processMonthlySnapshot(dataDir: string): Promise<SnapshotStats> {
  console.log(`\n📦 Processing monthly snapshot from: ${dataDir}\n`)

  const stats: SnapshotStats = {
    metadata: {} as Metadata,
    recordsMarkedHistorical: 0,
    recordsImported: 0,
    recordsCleaned: 0,
    tablesProcessed: []
  }

  const db = await connectMotherduck()

  try {
    // Step 1: Parse metadata
    console.log('📋 Reading metadata...')
    stats.metadata = await parseMetadata(dataDir)

    console.log(`   ✓ Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`   ✓ Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`   ✓ Extract Type: ${stats.metadata.ExtractType}`)

    if (stats.metadata.ExtractType !== 'full') {
      throw new Error(`Expected 'full' extract type, got '${stats.metadata.ExtractType}'`)
    }

    // Step 2: Mark all current records as historical
    stats.recordsMarkedHistorical = await markCurrentAsHistorical(db)

    // Step 3: Import new full snapshot
    const importResult = await importFullSnapshot(db, dataDir, stats.metadata)
    stats.recordsImported = importResult.recordsImported
    stats.tablesProcessed = importResult.tablesProcessed

    // Step 4: Clean up old snapshots (24-month retention)
    stats.recordsCleaned = await cleanupOldSnapshots(db, 24)

  } finally {
    await closeMotherduck(db)
  }

  return stats
}

/**
 * Main execution
 */
async function main() {
  const args = process.argv.slice(2)

  if (args.length === 0) {
    console.error('Usage: npx tsx scripts/apply-monthly-snapshot.ts <path-to-extracted-csv-directory>')
    console.error('\nIMPORTANT: Extract the ZIP file externally first!')
    console.error('\nExample:')
    console.error('  unzip KboOpenData_0145_2025_11_03_Full.zip -d /tmp/kbo-full')
    console.error('  npx tsx scripts/apply-monthly-snapshot.ts /tmp/kbo-full')
    process.exit(1)
  }

  const dataDir = args[0]

  // Validate directory exists
  if (!fs.existsSync(dataDir)) {
    console.error(`❌ Directory not found: ${dataDir}`)
    process.exit(1)
  }

  // Validate meta.csv exists
  const metaPath = path.join(dataDir, 'meta.csv')
  if (!fs.existsSync(metaPath)) {
    console.error(`❌ meta.csv not found in directory: ${dataDir}`)
    console.error('   Make sure you extracted the KBO ZIP file to this directory')
    process.exit(1)
  }

  try {
    const stats = await processMonthlySnapshot(dataDir)

    // Summary
    console.log('\n' + '='.repeat(60))
    console.log('📊 MONTHLY SNAPSHOT SUMMARY')
    console.log('='.repeat(60))
    console.log(`Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`\nTables Processed: ${stats.tablesProcessed.length}`)
    console.log(`Records Marked Historical: ${stats.recordsMarkedHistorical.toLocaleString()}`)
    console.log(`Records Imported: ${stats.recordsImported.toLocaleString()}`)
    console.log(`Old Records Cleaned: ${stats.recordsCleaned.toLocaleString()}`)

    console.log('\n💾 Storage Impact:')
    const monthlySize = 100 // ~100 MB compressed per snapshot
    const totalSnapshots = 24
    const totalSize = monthlySize * totalSnapshots
    console.log(`   • Per snapshot: ~${monthlySize} MB (Parquet + ZSTD)`)
    console.log(`   • 24-month retention: ~${totalSize / 1000} GB`)
    console.log(`   • Estimated cost: ~$0.05/month on Motherduck`)

    console.log('\n' + '='.repeat(60))
    console.log('✅ Monthly snapshot applied successfully')
    console.log('='.repeat(60) + '\n')

  } catch (error) {
    console.error('\n❌ Snapshot import failed:', error)
    process.exit(1)
  }
}

main()
