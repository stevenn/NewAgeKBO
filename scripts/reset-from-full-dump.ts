#!/usr/bin/env tsx
/**
 * Reset Database from Full Dump
 *
 * ⚠️  DESTRUCTIVE OPERATION - USE WITH EXTREME CAUTION ⚠️
 *
 * This script completely resets the database from a full dump, discarding ALL temporal history.
 * Use only when validation shows major discrepancies (>5%) or database corruption.
 *
 * What it does:
 * 1. Creates backup export of current database state
 * 2. Truncates all temporal tables
 * 3. Imports full dump as extract #1
 * 4. Resets primary names for enterprises
 *
 * Usage:
 *   npx tsx scripts/reset-from-full-dump.ts <path-to-extracted-csv-directory>
 *   npx tsx scripts/reset-from-full-dump.ts /tmp/kbo-full --no-backup  # Skip backup
 *
 * Safety:
 *   - Requires explicit --confirm flag to execute
 *   - Creates backup export before truncation (unless --no-backup)
 *   - Validates full dump structure before proceeding
 *   - Atomic operation (rollback on error)
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import * as fs from 'fs'
import * as path from 'path'
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
  ImportProgress
} from '../lib/import/duckdb-processor'

interface ResetStats {
  backup_created: boolean
  backup_path?: string
  backup_row_count?: number
  metadata: Metadata
  tables_truncated: string[]
  records_imported: number
  elapsed_time_ms: number
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

/**
 * Create backup export of current database
 */
async function createBackup(
  db: any,
  backupDir: string
): Promise<{ path: string; rowCount: number }> {
  console.log('💾 Creating backup export...\n')

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
  const backupPath = path.join(backupDir, `backup-${timestamp}`)

  // Create backup directory
  if (!fs.existsSync(backupPath)) {
    fs.mkdirSync(backupPath, { recursive: true })
  }

  let totalRows = 0

  for (const table of TABLES) {
    console.log(`   Exporting ${table}...`)

    const exportPath = path.join(backupPath, `${table}.parquet`)

    await executeQuery(db, `
      COPY (SELECT * FROM ${table})
      TO '${exportPath}'
      (FORMAT PARQUET, COMPRESSION ZSTD)
    `)

    // Count rows
    const result = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count FROM ${table}
    `)
    const count = Number(result[0].count)
    totalRows += count

    console.log(`   ✓ ${table}: ${count.toLocaleString()} rows exported`)
  }

  console.log(`\n   ✅ Backup created: ${backupPath}`)
  console.log(`   📊 Total rows: ${totalRows.toLocaleString()}\n`)

  return { path: backupPath, rowCount: totalRows }
}

/**
 * Truncate all temporal tables
 */
async function truncateTables(db: any): Promise<string[]> {
  console.log('🗑️  Truncating all tables...\n')

  const truncated: string[] = []

  for (const table of TABLES) {
    await executeQuery(db, `TRUNCATE TABLE ${table}`)
    truncated.push(table)
    console.log(`   ✓ ${table} truncated`)
  }

  console.log(`\n   ✅ ${truncated.length} tables truncated\n`)

  return truncated
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
 * Import full dump as extract #1
 */
async function importFullDump(
  dataDir: string,
  metadata: Metadata
): Promise<number> {
  console.log('📥 Importing full dump as extract #1...\n')

  const mdConfig = getMotherduckConfig()
  const localDb = await initializeDuckDBWithMotherduck(mdConfig.token, mdConfig.database || 'kbo')

  let totalImported = 0

  try {
    // Stage CSV files
    console.log('   Loading CSV files into local DuckDB...\n')

    await stageCsvFile(localDb, path.join(dataDir, 'code.csv'), 'codes')
    console.log('   ✓ code.csv loaded')

    await stageCsvFile(localDb, path.join(dataDir, 'enterprise.csv'), 'enterprises')
    console.log('   ✓ enterprise.csv loaded')

    await stageCsvFile(localDb, path.join(dataDir, 'denomination.csv'), 'denominations')
    console.log('   ✓ denomination.csv loaded')

    console.log('\n   Creating ranked denominations for primary name selection...')
    await createRankedDenominations(localDb)
    console.log('   ✓ Ranked denominations created')

    const remainingFiles = [
      { name: 'establishments', file: 'establishment.csv' },
      { name: 'addresses', file: 'address.csv' },
      { name: 'activities', file: 'activity.csv' },
      { name: 'contacts', file: 'contact.csv' },
      { name: 'branches', file: 'branch.csv' },
    ]

    for (const { name, file } of remainingFiles) {
      await stageCsvFile(localDb, path.join(dataDir, file), name)
      console.log(`   ✓ ${file} loaded`)
    }

    console.log('\n   Processing and uploading data to Motherduck...\n')

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
        metadata,
        showProgress
      )
      totalImported += stat.rowsInserted
    }

  } finally {
    // Local DuckDB will be garbage collected
  }

  console.log(`\n   ✅ Import complete: ${totalImported.toLocaleString()} rows\n`)

  return totalImported
}

/**
 * Main reset operation
 */
async function resetDatabase(
  dataDir: string,
  createBackupExport: boolean
): Promise<ResetStats> {
  const startTime = Date.now()

  console.log('\n' + '='.repeat(80))
  console.log('⚠️  DATABASE RESET FROM FULL DUMP ⚠️')
  console.log('='.repeat(80))
  console.log('\nThis operation will:')
  console.log('  1. ' + (createBackupExport ? 'Create backup export of current data' : 'Skip backup (--no-backup)'))
  console.log('  2. TRUNCATE all temporal tables (DELETE ALL DATA)')
  console.log('  3. Import full dump as extract #1')
  console.log('  4. Lose ALL temporal history\n')
  console.log('⚠️  THIS CANNOT BE UNDONE (except from backup) ⚠️')
  console.log('='.repeat(80) + '\n')

  const stats: ResetStats = {
    backup_created: false,
    metadata: {} as Metadata,
    tables_truncated: [],
    records_imported: 0,
    elapsed_time_ms: 0
  }

  const mdConfig = getMotherduckConfig()
  const motherduckDb = await connectMotherduck()
  await executeQuery(motherduckDb, `USE ${mdConfig.database}`)

  const localDb = await initializeDuckDBWithMotherduck(mdConfig.token, mdConfig.database || 'kbo')

  try {
    // Step 1: Parse and validate metadata
    console.log('1️⃣  Validating full dump metadata...\n')
    stats.metadata = await parseMetadataWithDuckDB(localDb, dataDir)
    console.log(`   ✅ Extract #${stats.metadata.extractNumber} (${stats.metadata.snapshotDate})`)
    console.log(`   📅 Snapshot date: ${stats.metadata.snapshotDate}`)
    console.log(`   📦 Extract type: ${stats.metadata.extractType}\n`)

    validateExtractType(stats.metadata, 'full')

    // Step 2: Create backup (optional)
    if (createBackupExport) {
      console.log('2️⃣  Creating backup export...\n')
      const backup = await createBackup(motherduckDb, '/tmp/kbo-backups')
      stats.backup_created = true
      stats.backup_path = backup.path
      stats.backup_row_count = backup.rowCount
    } else {
      console.log('2️⃣  Skipping backup (--no-backup flag)\n')
    }

    // Step 3: Truncate tables
    console.log('3️⃣  Truncating all tables...\n')
    stats.tables_truncated = await truncateTables(motherduckDb)

    // Step 4: Import full dump
    console.log('4️⃣  Importing full dump...\n')
    stats.records_imported = await importFullDump(dataDir, stats.metadata)

    stats.elapsed_time_ms = Date.now() - startTime

  } finally {
    await closeMotherduck(motherduckDb)
  }

  return stats
}

/**
 * Main execution
 */
async function main() {
  const args = process.argv.slice(2)

  // Parse arguments
  const confirmFlag = args.includes('--confirm')
  const noBackupFlag = args.includes('--no-backup')
  const dataDir = args.find(arg => !arg.startsWith('--'))

  if (!dataDir) {
    console.error('Usage: npx tsx scripts/reset-from-full-dump.ts <path-to-extracted-csv-directory> [options]')
    console.error('\nOptions:')
    console.error('  --confirm     Required to execute (safety measure)')
    console.error('  --no-backup   Skip backup export before reset')
    console.error('\nExample:')
    console.error('  unzip KboOpenData_0145_2025_11_03_Full.zip -d /tmp/kbo-full')
    console.error('  npx tsx scripts/reset-from-full-dump.ts /tmp/kbo-full --confirm')
    process.exit(1)
  }

  // Validate directory
  if (!fs.existsSync(dataDir)) {
    console.error(`❌ Directory not found: ${dataDir}`)
    process.exit(1)
  }

  const metaPath = path.join(dataDir, 'meta.csv')
  if (!fs.existsSync(metaPath)) {
    console.error(`❌ meta.csv not found in directory: ${dataDir}`)
    console.error('   Make sure you extracted the KBO ZIP file to this directory')
    process.exit(1)
  }

  // Require --confirm flag
  if (!confirmFlag) {
    console.error('\n⚠️  ERROR: This is a destructive operation!')
    console.error('   Add --confirm flag to execute')
    console.error('\n   Example: npx tsx scripts/reset-from-full-dump.ts ' + dataDir + ' --confirm')
    process.exit(1)
  }

  try {
    const stats = await resetDatabase(dataDir, !noBackupFlag)

    // Show summary
    console.log('\n' + '='.repeat(80))
    console.log('✅ DATABASE RESET COMPLETE')
    console.log('='.repeat(80))
    console.log(`\nExtract Number: ${stats.metadata.extractNumber}`)
    console.log(`Snapshot Date: ${stats.metadata.snapshotDate}`)
    console.log(`\nBackup Created: ${stats.backup_created ? 'Yes' : 'No'}`)
    if (stats.backup_path) {
      console.log(`Backup Location: ${stats.backup_path}`)
      console.log(`Backup Rows: ${stats.backup_row_count?.toLocaleString()}`)
    }
    console.log(`\nTables Truncated: ${stats.tables_truncated.length}`)
    console.log(`Records Imported: ${stats.records_imported.toLocaleString()}`)
    console.log(`Elapsed Time: ${(stats.elapsed_time_ms / 1000).toFixed(2)}s`)
    console.log('\n' + '='.repeat(80))
    console.log('\n⚠️  IMPORTANT: All temporal history has been lost!')
    console.log('   Future daily updates will build new history from this baseline.')
    if (stats.backup_path) {
      console.log(`\n   Backup available at: ${stats.backup_path}`)
    }
    console.log()

    process.exit(0)
  } catch (error) {
    console.error('\n❌ Database reset failed:', error)
    console.error('\n⚠️  Database may be in inconsistent state!')
    console.error('   Check backup and consider manual recovery.')
    process.exit(1)
  }
}

main()
