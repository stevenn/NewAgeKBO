#!/usr/bin/env tsx

/**
 * Add missing import_jobs records
 * - Extract 140 UPDATE (KboOpenData_0140_2025_10_05_Update.zip)
 * - Extract 162 UPDATE (KboOpenData_0162_2025_10_26_Update.zip - empty file)
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../lib/motherduck'
import { randomUUID } from 'crypto'

async function addRecord(
  conn: any,
  extractNumber: number,
  snapshotDate: string,
  note: string
) {
  const jobId = randomUUID()

  await executeStatement(conn, `
    INSERT INTO import_jobs (
      id,
      extract_number,
      extract_type,
      snapshot_date,
      extract_timestamp,
      status,
      started_at,
      completed_at,
      error_message,
      records_processed,
      records_inserted,
      records_updated,
      records_deleted,
      worker_type
    ) VALUES (
      '${jobId}',
      ${extractNumber},
      'update',
      '${snapshotDate}',
      '${snapshotDate} 00:00:00',
      'completed',
      CURRENT_TIMESTAMP,
      CURRENT_TIMESTAMP,
      '${note}',
      0,
      0,
      0,
      0,
      'backfill'
    )
  `)
}

async function main() {
  console.log('🔧 Adding missing import_jobs records\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    // Check what's already there
    const allJobs = await executeQuery<{ extract_number: number; extract_type: string }>(
      conn,
      `SELECT extract_number, extract_type FROM import_jobs ORDER BY extract_number`
    )

    const has140Update = allJobs.some(j => j.extract_number === 140 && j.extract_type === 'update')
    const has162 = allJobs.some(j => j.extract_number === 162)

    console.log('Current state:')
    console.log(`  Extract 140 UPDATE: ${has140Update ? '✓ exists' : '✗ missing'}`)
    console.log(`  Extract 162 UPDATE: ${has162 ? '✓ exists' : '✗ missing'}`)
    console.log('')

    let added = 0

    // Add extract 140 UPDATE if missing
    if (!has140Update) {
      console.log('Adding extract 140 UPDATE...')
      await addRecord(conn, 140, '2025-10-05', 'Backfilled - KboOpenData_0140_2025_10_05_Update.zip')
      console.log('✓ Extract 140 UPDATE added\n')
      added++
    }

    // Add extract 162 UPDATE if missing
    if (!has162) {
      console.log('Adding extract 162 UPDATE (empty file)...')
      await addRecord(conn, 162, '2025-10-26', 'Backfilled - KboOpenData_0162_2025_10_26_Update.zip (empty)')
      console.log('✓ Extract 162 UPDATE added\n')
      added++
    }

    if (added === 0) {
      console.log('✓ All records already exist, nothing to add')
    } else {
      console.log(`✅ Added ${added} missing record(s)!`)
    }

  } catch (error: any) {
    console.error('❌ Failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
