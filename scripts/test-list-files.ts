#!/usr/bin/env tsx

/**
 * Test KBO Portal File Listing
 * This script tests the ability to list available files from the KBO portal
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { listAvailableFiles, listDailyUpdates } from '../lib/kbo-client'

async function main() {
  console.log('📋 Testing KBO Portal File Listing\n')

  try {
    console.log('Fetching all available files...')
    const allFiles = await listAvailableFiles()

    console.log(`\n✅ Found ${allFiles.length} total files\n`)

    // Group by type
    const updateFiles = allFiles.filter(f => f.file_type === 'update')
    const fullFiles = allFiles.filter(f => f.file_type === 'full')

    console.log(`📊 File Types:`)
    console.log(`   Daily Updates: ${updateFiles.length}`)
    console.log(`   Full Dumps: ${fullFiles.length}`)

    // Show latest 5 daily updates
    console.log(`\n📅 Latest 5 Daily Updates:`)
    updateFiles.slice(0, 5).forEach(file => {
      console.log(`   ${file.extract_number} - ${file.snapshot_date} - ${file.filename}`)
    })

    // Test the daily updates filter function
    console.log(`\n🔍 Testing listDailyUpdates()...`)
    const dailyUpdates = await listDailyUpdates()
    console.log(`   Found ${dailyUpdates.length} daily updates`)

    console.log('\n🎉 File listing test passed!')

  } catch (error: any) {
    console.error('❌ Test failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  }
}

main()
