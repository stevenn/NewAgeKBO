#!/usr/bin/env tsx

/**
 * Initial KBO data import
 *
 * Downloads full dataset from KBO portal, processes with local DuckDB,
 * and streams directly to Motherduck
 *
 * Usage:
 *   npx tsx scripts/initial-import.ts <path-to-extracted-kbo-data>
 *
 * Example:
 *   npx tsx scripts/initial-import.ts ./sampledata/KboOpenData_0140_2025_10_05_Full
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { existsSync } from 'fs'
import * as path from 'path'
const { join } = path
import {
  connectMotherduck,
  closeMotherduck,
  getMotherduckConfig,
  executeQuery,
  tableExists,
} from '../lib/motherduck'
import { formatUserError } from '../lib/errors'
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
  ImportStats,
  ImportProgress
} from '../lib/import/duckdb-processor'

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
 * Verify all required CSV files exist
 */
function verifyCsvFiles(dataPath: string): string[] {
  const requiredFiles = [
    'meta.csv',
    'code.csv',
    'enterprise.csv',
    'establishment.csv',
    'denomination.csv',
    'address.csv',
    'activity.csv',
    'contact.csv',
    'branch.csv',
  ]

  const missingFiles: string[] = []

  for (const file of requiredFiles) {
    const filePath = join(dataPath, file)
    if (!existsSync(filePath)) {
      missingFiles.push(file)
    }
  }

  return missingFiles
}

/**
 * Main import function
 */
async function initialImport() {
  console.log('🚀 KBO Initial Data Import\n')

  // Get data path from command line
  const dataPath = process.argv[2]

  if (!dataPath) {
    console.error('❌ Error: Data path not provided\n')
    console.error('Usage: npx tsx scripts/initial-import.ts <path-to-extracted-kbo-data>\n')
    console.error('Example: npx tsx scripts/initial-import.ts ./sampledata/KboOpenData_0140_2025_10_05_Full')
    process.exit(1)
  }

  if (!existsSync(dataPath)) {
    console.error(`❌ Error: Data path does not exist: ${dataPath}`)
    process.exit(1)
  }

  console.log(`📂 Data path: ${dataPath}\n`)

  try {
    // Step 1: Verify CSV files
    console.log('1️⃣  Verifying CSV files...')
    const missingFiles = verifyCsvFiles(dataPath)

    if (missingFiles.length > 0) {
      console.error(`   ❌ Missing required files:`)
      for (const file of missingFiles) {
        console.error(`      • ${file}`)
      }
      console.error()
      process.exit(1)
    }

    console.log('   ✅ All required CSV files found\n')

    // Step 2: Connect to Motherduck
    console.log('2️⃣  Connecting to Motherduck...')
    const mdConfig = getMotherduckConfig()
    const motherduckDb = await connectMotherduck()

    // Use the database
    await executeQuery(motherduckDb, `USE ${mdConfig.database}`)
    console.log(`   ✅ Connected to database: ${mdConfig.database}\n`)

    // Step 3: Verify schema exists
    console.log('3️⃣  Verifying database schema...')
    const requiredTables = [
      'enterprises',
      'establishments',
      'denominations',
      'addresses',
      'activities',
      'nace_codes',
      'contacts',
      'branches',
      'codes',
      'import_jobs',
    ]

    const missingTables: string[] = []
    for (const tableName of requiredTables) {
      const exists = await tableExists(motherduckDb, tableName)
      if (!exists) {
        missingTables.push(tableName)
      }
    }

    if (missingTables.length > 0) {
      console.error('   ❌ Missing required tables:')
      for (const table of missingTables) {
        console.error(`      • ${table}`)
      }
      console.error('\n   💡 Run: npx tsx scripts/create-schema.ts\n')
      process.exit(1)
    }

    console.log('   ✅ All required tables exist\n')

    // Step 4: Check if database already has data
    console.log('4️⃣  Checking for existing data...')

    // Check multiple tables to ensure a clean import
    const tablesToCheck = ['enterprises', 'codes', 'nace_codes']
    let totalRows = 0

    for (const table of tablesToCheck) {
      const result = await executeQuery<{ count: number }>(
        motherduckDb,
        `SELECT COUNT(*) as count FROM ${table}`
      )
      totalRows += Number(result[0].count)  // Convert BigInt to Number
    }

    if (totalRows > 0) {
      console.error(`   ⚠️  Database already contains data (${totalRows} total rows across tables)`)
      console.error('\n   This script is for INITIAL import only.')
      console.error('   Run: npx tsx scripts/cleanup-data.ts (to clean existing data)\n')
      process.exit(1)
    }

    console.log('   ✅ Database is empty and ready for import\n')

    // Step 5: Initialize local DuckDB with Motherduck extension
    console.log('5️⃣  Initializing DuckDB with Motherduck connection...')
    const localDb = await initializeDuckDBWithMotherduck(mdConfig.token, mdConfig.database || 'kbo')
    console.log('   ✅ Connected to Motherduck via local DuckDB\n')

    // Step 6: Parse metadata from meta.csv
    console.log('6️⃣  Reading metadata from meta.csv...')
    const metadata = await parseMetadataWithDuckDB(localDb, dataPath)
    console.log(`   ✅ Extract #${metadata.extractNumber} (${metadata.snapshotDate})`)
    console.log(`   📅 Snapshot date: ${metadata.snapshotDate}`)
    console.log(`   📦 Extract type: ${metadata.extractType}`)
    console.log(`   🔢 Version: ${metadata.version}\n`)

    // Validate extract type is 'full'
    validateExtractType(metadata, 'full')

    // Step 7: Load and process data
    console.log('7️⃣  Processing data...\n')

    const stats: ImportStats[] = []
    const startTime = Date.now()

    // Stage code.csv first (used by codes and nace_codes tables)
    await stageCsvFile(localDb, join(dataPath, 'code.csv'), 'codes')

    // Process codes and nace_codes tables
    let stat = await processTable(
      localDb,
      dataPath,
      getCodesTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    stat = await processTable(
      localDb,
      dataPath,
      getNaceCodesTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    // Load enterprise and denomination files for primary name selection
    console.log('   📝 Loading enterprises and denominations for primary name selection...')

    await stageCsvFile(localDb, join(dataPath, 'enterprise.csv'), 'enterprises')
    await stageCsvFile(localDb, join(dataPath, 'denomination.csv'), 'denominations')

    // Create ranked denominations for primary name selection
    await createRankedDenominations(localDb)

    // Process enterprises and establishments
    stat = await processTable(
      localDb,
      dataPath,
      getEnterprisesTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    // Stage remaining CSV files
    console.log('   📝 Loading remaining CSV files...')
    const csvFiles = [
      { name: 'establishments', file: 'establishment.csv' },
      { name: 'addresses', file: 'address.csv' },
      { name: 'activities', file: 'activity.csv' },
      { name: 'contacts', file: 'contact.csv' },
      { name: 'branches', file: 'branch.csv' },
    ]

    for (const { name, file } of csvFiles) {
      await stageCsvFile(localDb, join(dataPath, file), name)
    }
    console.log('   ✅ All CSV files loaded\n')

    // Process remaining tables
    stat = await processTable(
      localDb,
      dataPath,
      getEstablishmentsTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    stat = await processTable(
      localDb,
      dataPath,
      getDenominationsTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    stat = await processTable(
      localDb,
      dataPath,
      getAddressesTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    stat = await processTable(
      localDb,
      dataPath,
      getActivitiesTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    stat = await processTable(
      localDb,
      dataPath,
      getContactsTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    stat = await processTable(
      localDb,
      dataPath,
      getBranchesTransformation(),
      metadata,
      showProgress
    )
    stats.push(stat)

    // Close connections
    await closeMotherduck(motherduckDb)

    // Summary
    const totalDuration = Date.now() - startTime
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ SUCCESS! Initial import complete')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📊 Import Statistics:\n')
    for (const stat of stats) {
      console.log(
        `   ${stat.table.padEnd(15)} ${stat.rowsInserted.toLocaleString().padStart(10)} rows  (${(stat.durationMs / 1000).toFixed(2)}s)`
      )
    }

    console.log(`\n   Total duration: ${(totalDuration / 1000).toFixed(2)}s`)
    console.log()

  } catch (error) {
    console.error('\n❌ Import failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${formatUserError(error)}\n`)

      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

// Run the import
initialImport()
