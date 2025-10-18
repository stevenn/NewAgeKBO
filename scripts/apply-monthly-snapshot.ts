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
import {
  connectMotherduck,
  closeMotherduck,
  getMotherduckConfig,
  executeQuery
} from '../lib/motherduck'
import {
  parseMetadataWithDuckDB,
  validateExtractType,
  Metadata
} from '../lib/import/metadata'
import {
  getCodesTransformation,
  getNaceCodesTransformation,
  getEnterprisesTransformation,
  getEstablishmentsTransformation,
  getDenominationsTransformation,
  getAddressesTransformation,
  getActivitiesTransformation,
  getContactsTransformation,
  getBranchesTransformation
} from '../lib/import/transformations'
import {
  initializeDuckDBWithMotherduck,
  stageCsvFile,
  createRankedDenominations,
  processTable,
  markAllCurrentAsHistorical,
  cleanupOldSnapshots,
  ImportStats,
  ImportProgress
} from '../lib/import/duckdb-processor'
import * as path from 'path'
const { join } = path

/**
 * Snapshot statistics
 */
interface SnapshotStats {
  metadata: Metadata
  recordsMarkedHistorical: number
  recordsImported: number
  recordsCleaned: number
  tablesProcessed: string[]
}

/**
 * Show progress indicator
 */
function showProgress(progress: ImportProgress): void {
  const phases = {
    checking: '🔍',
    loading: '📥',
    transforming: '⚙️',
    uploading: '☁️',
    complete: '✅',
  }

  const icon = phases[progress.phase]
  let message = `${icon}  ${progress.table.padEnd(15)} - ${progress.phase}`

  if (progress.rowsProcessed && progress.totalRows) {
    const percent = Math.round((progress.rowsProcessed / progress.totalRows) * 100)
    message += ` (${progress.rowsProcessed.toLocaleString()}/${progress.totalRows.toLocaleString()} - ${percent}%)`
  }

  console.log(message)
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

  const importStats: ImportStats[] = []
  const mdConfig = getMotherduckConfig()

  // Step 1: Connect to Motherduck (for marking historical records)
  console.log('1️⃣  Connecting to Motherduck...')
  const motherduckDb = await connectMotherduck()
  await executeQuery(motherduckDb, `USE ${mdConfig.database}`)
  console.log(`   ✅ Connected to database: ${mdConfig.database}\n`)

  // Step 2: Initialize local DuckDB with Motherduck extension
  console.log('2️⃣  Initializing DuckDB with Motherduck connection...')
  const localDb = await initializeDuckDBWithMotherduck(mdConfig.token, mdConfig.database)
  console.log('   ✅ DuckDB initialized and connected to Motherduck\n')

  try {
    // Step 3: Parse metadata
    console.log('3️⃣  Reading metadata from meta.csv...')
    stats.metadata = await parseMetadataWithDuckDB(localDb, dataDir)
    console.log(`   ✅ Extract #${stats.metadata.extractNumber} (${stats.metadata.snapshotDate})`)
    console.log(`   📅 Snapshot date: ${stats.metadata.snapshotDate}`)
    console.log(`   📦 Extract type: ${stats.metadata.extractType}`)
    console.log(`   🔢 Version: ${stats.metadata.version}\n`)

    // Validate extract type is 'full'
    validateExtractType(stats.metadata, 'full')

    // Step 4: Mark all current records as historical
    console.log('4️⃣  Marking current records as historical...\n')
    stats.recordsMarkedHistorical = await markAllCurrentAsHistorical(
      motherduckDb,
      (table: string, count: number) => {
        console.log(`   ✓ ${table.padEnd(15)} ${count.toLocaleString().padStart(10)} records marked historical`)
      }
    )
    console.log(`\n   📊 Total marked historical: ${stats.recordsMarkedHistorical.toLocaleString()}\n`)

    // Step 5: Stage CSV files
    console.log('5️⃣  Loading CSV files into local DuckDB...\n')

    // Load code.csv first (used by codes and nace_codes tables)
    await stageCsvFile(localDb, join(dataDir, 'code.csv'), 'codes')
    console.log('   ✓ code.csv loaded')

    // Load enterprise and denomination files for primary name selection
    await stageCsvFile(localDb, join(dataDir, 'enterprise.csv'), 'enterprises')
    console.log('   ✓ enterprise.csv loaded')

    await stageCsvFile(localDb, join(dataDir, 'denomination.csv'), 'denominations')
    console.log('   ✓ denomination.csv loaded')

    // Create ranked denominations for primary name selection
    console.log('\n   📝 Creating ranked denominations for primary name selection...')
    await createRankedDenominations(localDb)
    console.log('   ✓ Ranked denominations created')

    // Load remaining CSV files
    const remainingFiles = [
      { name: 'establishments', file: 'establishment.csv' },
      { name: 'addresses', file: 'address.csv' },
      { name: 'activities', file: 'activity.csv' },
      { name: 'contacts', file: 'contact.csv' },
      { name: 'branches', file: 'branch.csv' },
    ]

    for (const { name, file } of remainingFiles) {
      await stageCsvFile(localDb, join(dataDir, file), name)
      console.log(`   ✓ ${file} loaded`)
    }

    console.log('\n   ✅ All CSV files loaded\n')

    // Step 6: Process all tables with transformations
    console.log('6️⃣  Processing and uploading data to Motherduck...\n')

    const transformations = [
      getCodesTransformation(),
      getNaceCodesTransformation(),
      getEnterprisesTransformation(),
      getEstablishmentsTransformation(),
      getDenominationsTransformation(),
      getAddressesTransformation(),
      getActivitiesTransformation(),
      getContactsTransformation(),
      getBranchesTransformation()
    ]

    for (const transformation of transformations) {
      const stat = await processTable(
        localDb,
        dataDir,
        transformation,
        stats.metadata,
        showProgress
      )
      importStats.push(stat)
      stats.tablesProcessed.push(transformation.tableName)
      stats.recordsImported += stat.rowsInserted
    }

    console.log()

    // Step 7: Clean up old snapshots (24-month retention)
    console.log('7️⃣  Cleaning up snapshots older than 24 months...\n')
    stats.recordsCleaned = await cleanupOldSnapshots(
      motherduckDb,
      24,
      (table: string, count: number) => {
        console.log(`   ✓ ${table.padEnd(15)} ${count.toLocaleString().padStart(10)} old records deleted`)
      }
    )

    if (stats.recordsCleaned === 0) {
      console.log('   ℹ️  No old records to clean up')
    }

  } finally {
    await closeMotherduck(motherduckDb)
    // Local DuckDB will be garbage collected
  }

  // Show summary
  console.log('\n' + '='.repeat(60))
  console.log('📊 MONTHLY SNAPSHOT SUMMARY')
  console.log('='.repeat(60))
  console.log(`Extract Number: ${stats.metadata.extractNumber}`)
  console.log(`Snapshot Date: ${stats.metadata.snapshotDate}`)
  console.log(`\nTables Processed: ${stats.tablesProcessed.length}`)
  console.log(`Records Marked Historical: ${stats.recordsMarkedHistorical.toLocaleString()}`)
  console.log(`Records Imported: ${stats.recordsImported.toLocaleString()}`)
  console.log(`Old Records Cleaned: ${stats.recordsCleaned.toLocaleString()}`)

  console.log('\n📋 Import Details:\n')
  for (const stat of importStats) {
    console.log(
      `   ${stat.table.padEnd(15)} ${stat.rowsInserted.toLocaleString().padStart(10)} rows  (${(stat.durationMs / 1000).toFixed(2)}s)`
    )
  }

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
  const metaPath = join(dataDir, 'meta.csv')
  if (!fs.existsSync(metaPath)) {
    console.error(`❌ meta.csv not found in directory: ${dataDir}`)
    console.error('   Make sure you extracted the KBO ZIP file to this directory')
    process.exit(1)
  }

  try {
    await processMonthlySnapshot(dataDir)
    process.exit(0)
  } catch (error) {
    console.error('\n❌ Snapshot import failed:', error)
    process.exit(1)
  }
}

main()
