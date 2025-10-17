#!/usr/bin/env tsx

/**
 * Batch apply all daily updates from a directory
 * Skips the base extract (140) and applies all subsequent updates
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import * as fs from 'fs'
import * as path from 'path'
import { execSync } from 'child_process'

async function batchApply() {
  const dataDir = process.argv[2] || './sampledata'

  console.log('📦 Batch Apply Daily Updates\n')
  console.log(`📂 Scanning directory: ${dataDir}\n`)

  // Find all update ZIP files (skip 0140 - that's redundant with the full dump)
  const files = fs.readdirSync(dataDir)
    .filter(f => f.match(/KboOpenData_\d+.*Update\.zip$/))
    .filter(f => !f.includes('0140')) // Skip 0140 update
    .sort() // Natural sort by filename

  if (files.length === 0) {
    console.error('❌ No update files found')
    process.exit(1)
  }

  console.log(`Found ${files.length} update files:\n`)
  files.forEach(f => console.log(`   - ${f}`))
  console.log('')

  let successCount = 0
  let errorCount = 0
  const errors: string[] = []

  for (const file of files) {
    const filePath = path.join(dataDir, file)
    const extractNumber = file.match(/KboOpenData_(\d+)/)?.[1]

    console.log(`\n${'='.repeat(60)}`)
    console.log(`📥 Applying: ${file} (Extract ${extractNumber})`)
    console.log('='.repeat(60))

    try {
      // Run the apply-daily-update script
      execSync(`npx tsx scripts/apply-daily-update.ts "${filePath}"`, {
        stdio: 'inherit',
        cwd: process.cwd()
      })

      successCount++
      console.log(`✅ Extract ${extractNumber} applied successfully`)

    } catch (error: any) {
      errorCount++
      const errorMsg = `Extract ${extractNumber} (${file})`
      errors.push(errorMsg)
      console.error(`❌ Extract ${extractNumber} failed`)
      console.error(`   Error: ${error.message}`)

      // Ask if we should continue
      console.log('\n⚠️  An error occurred. Continue with remaining updates? (Ctrl+C to stop)')
      // Wait 2 seconds before continuing
      await new Promise(resolve => setTimeout(resolve, 2000))
    }
  }

  // Summary
  console.log(`\n\n${'='.repeat(60)}`)
  console.log('📊 BATCH UPDATE SUMMARY')
  console.log('='.repeat(60))
  console.log(`Total files processed: ${files.length}`)
  console.log(`✅ Successful: ${successCount}`)
  console.log(`❌ Failed: ${errorCount}`)

  if (errors.length > 0) {
    console.log(`\n⚠️  Failed extracts:`)
    errors.forEach(err => console.log(`   • ${err}`))
  }

  console.log('='.repeat(60) + '\n')

  process.exit(errorCount > 0 ? 1 : 0)
}

batchApply()
