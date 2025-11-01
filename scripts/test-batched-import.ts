#!/usr/bin/env tsx

/**
 * Test script for batched import system
 *
 * Tests all 4 core functions with sample KBO update file:
 * 1. prepareImport() - Parse ZIP and populate staging
 * 2. processBatch() - Process a few batches
 * 3. getImportProgress() - Check progress
 * 4. finalizeImport() - Complete the import
 *
 * Usage: npx tsx scripts/test-batched-import.ts
 */

import { config } from 'dotenv'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import {
  prepareImport,
  processBatch,
  getImportProgress,
  finalizeImport
} from '../lib/import/batched-update'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

async function testBatchedImport() {
  console.log('🧪 Testing Batched Import System\n')
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

  try {
    // Step 1: Load sample data
    console.log('📦 Loading sample update file...')
    const samplePath = resolve(__dirname, '../sampledata/KboOpenData_0167_2025_10_31_Update.zip')
    const zipBuffer = readFileSync(samplePath)
    console.log(`   ✓ Loaded ${Math.round(zipBuffer.length / 1024)}KB\n`)

    // Step 2: Prepare import
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 1: PREPARE IMPORT')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    const prepareResult = await prepareImport(zipBuffer, 'local')

    console.log('\n📊 Preparation Result:')
    console.log(`   • Job ID: ${prepareResult.job_id}`)
    console.log(`   • Extract: ${prepareResult.extract_number}`)
    console.log(`   • Snapshot: ${prepareResult.snapshot_date}`)
    console.log(`   • Total Batches: ${prepareResult.total_batches}\n`)

    console.log('   Batches by Table:')
    for (const [table, counts] of Object.entries(prepareResult.batches_by_table)) {
      const total = counts.delete + counts.insert
      if (total > 0) {
        console.log(`   • ${table}: ${total} batches (${counts.delete} delete, ${counts.insert} insert)`)
      }
    }

    const jobId = prepareResult.job_id

    // Step 3: Check initial progress
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 2: CHECK INITIAL PROGRESS')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    let progress = await getImportProgress(jobId)

    console.log(`   Job Status: ${progress.status}`)
    console.log(`   Overall Progress: ${progress.overall_progress.percentage}% (${progress.overall_progress.completed_batches}/${progress.overall_progress.total_batches})\n`)

    console.log('   Table Status:')
    for (const [table, status] of Object.entries(progress.tables)) {
      console.log(`   • ${table}: ${status.completed}/${status.total} batches (${status.status})`)
    }

    // Step 4: Process first 5 batches
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 3: PROCESS FIRST 5 BATCHES')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    for (let i = 1; i <= 5; i++) {
      console.log(`\n🔄 Processing Batch ${i}...`)
      const batchResult = await processBatch(jobId)

      console.log(`   ✓ Completed: ${batchResult.table_name} batch ${batchResult.batch_number} (${batchResult.operation})`)
      console.log(`   • Records: ${batchResult.records_processed}`)
      console.log(`   • Progress: ${batchResult.progress.percentage}% (${batchResult.progress.completed_batches}/${batchResult.progress.total_batches})`)

      if (batchResult.next_batch) {
        console.log(`   • Next: ${batchResult.next_batch.table_name} batch ${batchResult.next_batch.batch_number} (${batchResult.next_batch.operation})`)
      } else {
        console.log(`   • Next: None (all batches completed!)`)
        break
      }
    }

    // Step 5: Check progress after processing
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 4: CHECK PROGRESS AFTER PROCESSING')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    progress = await getImportProgress(jobId)

    console.log(`   Job Status: ${progress.status}`)
    console.log(`   Overall Progress: ${progress.overall_progress.percentage}% (${progress.overall_progress.completed_batches}/${progress.overall_progress.total_batches})\n`)

    console.log('   Table Status:')
    for (const [table, status] of Object.entries(progress.tables)) {
      const pct = Math.round((status.completed / status.total) * 100)
      console.log(`   • ${table}: ${status.completed}/${status.total} batches (${pct}% - ${status.status})`)
    }

    if (progress.current_batch) {
      console.log(`\n   Current Batch: ${progress.current_batch.table} #${progress.current_batch.batch} (${progress.current_batch.operation})`)
    }

    if (progress.next_batch) {
      console.log(`   Next Batch: ${progress.next_batch.table} #${progress.next_batch.batch} (${progress.next_batch.operation})`)
    }

    // Step 6: Process remaining batches
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 5: PROCESS REMAINING BATCHES')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    let batchCount = 5
    let hasMore = true

    while (hasMore && batchCount < 100) { // Safety limit
      batchCount++

      try {
        const batchResult = await processBatch(jobId)

        if (batchCount % 5 === 0) {
          console.log(`   ✓ Batch ${batchCount}: ${batchResult.table_name} #${batchResult.batch_number} (${batchResult.progress.percentage}% complete)`)
        }

        hasMore = batchResult.next_batch !== null
      } catch (error) {
        if (error instanceof Error && error.message.includes('No pending batch')) {
          console.log(`   ✓ All batches completed!\n`)
          hasMore = false
        } else {
          throw error
        }
      }
    }

    console.log(`   Total batches processed: ${batchCount}`)

    // Step 7: Final progress check
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 6: FINAL PROGRESS CHECK')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    progress = await getImportProgress(jobId)

    console.log(`   Job Status: ${progress.status}`)
    console.log(`   Overall Progress: ${progress.overall_progress.percentage}% (${progress.overall_progress.completed_batches}/${progress.overall_progress.total_batches})\n`)

    console.log('   Final Table Status:')
    for (const [table, status] of Object.entries(progress.tables)) {
      console.log(`   • ${table}: ${status.completed}/${status.total} batches (${status.status})`)
    }

    // Step 8: Finalize import
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('STEP 7: FINALIZE IMPORT')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    const finalizeResult = await finalizeImport(jobId)

    console.log('\n📊 Finalization Result:')
    console.log(`   • Success: ${finalizeResult.success}`)
    console.log(`   • Names Resolved: ${finalizeResult.names_resolved}`)
    console.log(`   • Staging Cleaned: ${finalizeResult.staging_cleaned}`)

    // Final summary
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✅ TEST COMPLETE!')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📊 Summary:')
    console.log(`   • Job ID: ${jobId}`)
    console.log(`   • Extract Number: ${prepareResult.extract_number}`)
    console.log(`   • Total Batches: ${prepareResult.total_batches}`)
    console.log(`   • Batches Processed: ${batchCount}`)
    console.log(`   • Names Resolved: ${finalizeResult.names_resolved}`)
    console.log(`   • Status: COMPLETED ✓\n`)

  } catch (error) {
    console.error('\n❌ TEST FAILED!\n')
    if (error instanceof Error) {
      console.error(`Error: ${error.message}\n`)
      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }
    process.exit(1)
  }
}

// Run the test
testBatchedImport()
