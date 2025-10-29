#!/usr/bin/env tsx

/**
 * Apply daily KBO update from ZIP file
 * Purpose: Process incremental updates using delete-then-insert pattern
 *
 * This is now a thin CLI wrapper around the core library function
 * The actual import logic is in lib/import/daily-update.ts
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { readFileSync } from 'fs'
import { processDailyUpdate } from '../lib/import/daily-update'
import * as path from 'path'

/**
 * Main execution
 */
async function main() {
  const args = process.argv.slice(2)

  if (args.length === 0) {
    console.error('Usage: npx tsx scripts/apply-daily-update.ts <path-to-update.zip>')
    console.error('\nExample:')
    console.error('  npx tsx scripts/apply-daily-update.ts sampledata/KboOpenData_0141_2025_10_06_Update.zip')
    process.exit(1)
  }

  const zipPath = args[0]

  console.log(`\n📦 Processing daily update: ${path.basename(zipPath)}\n`)

  try {
    // Read the ZIP file into a buffer
    const zipBuffer = readFileSync(zipPath)

    // Call the library function
    const stats = await processDailyUpdate(zipBuffer, 'local')

    // Summary
    console.log('\n' + '='.repeat(60))
    console.log('📊 DAILY UPDATE SUMMARY')
    console.log('='.repeat(60))
    console.log(`Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`\nTables Processed: ${stats.tablesProcessed.length}`)
    console.log(`Records Marked Historical: ${stats.deletesApplied}`)
    console.log(`Records Inserted: ${stats.insertsApplied}`)
    console.log(`Total Changes: ${stats.deletesApplied + stats.insertsApplied}`)

    if (stats.errors.length > 0) {
      console.log(`\n⚠️  Errors: ${stats.errors.length}`)
      stats.errors.forEach(err => console.log(`   • ${err}`))
    }

    console.log('\n' + '='.repeat(60))
    console.log(stats.errors.length > 0 ? '⚠️  Completed with errors' : '✅ Update applied successfully')
    console.log('='.repeat(60) + '\n')

    process.exit(stats.errors.length > 0 ? 1 : 0)

  } catch (error) {
    console.error('\n❌ Update failed:', error)
    process.exit(1)
  }
}

main()
