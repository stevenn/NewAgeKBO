#!/usr/bin/env tsx

/**
 * Backfill import_jobs table from actual database data
 *
 * Creates retrospective import job records for all extracts found in the database.
 * Uses worker_type='backfill' to honestly label these as reconstructed records.
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../lib/motherduck'
import { randomUUID } from 'crypto'

async function main() {
  console.log('🔧 Backfilling import_jobs table\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    // Step 1: Get all extracts from the database
    console.log('Step 1: Discovering extracts from enterprises table...')
    const extracts = await executeQuery<{
      _extract_number: number
      _snapshot_date: string
    }>(conn, `
      SELECT DISTINCT
        _extract_number,
        _snapshot_date::VARCHAR as _snapshot_date
      FROM enterprises
      ORDER BY _extract_number
    `)

    console.log(`✓ Found ${extracts.length} extracts\n`)

    // Step 2: Check what's already in import_jobs
    console.log('Step 2: Checking existing import_jobs...')
    const existing = await executeQuery<{ extract_number: number }>(
      conn,
      `SELECT extract_number FROM import_jobs`
    )
    const existingSet = new Set(existing.map(e => e.extract_number))
    console.log(`✓ Found ${existing.length} existing records\n`)

    // Step 3: Backfill missing records
    console.log('Step 3: Creating backfill records...\n')

    let created = 0
    let skipped = 0

    for (const extract of extracts) {
      if (existingSet.has(extract._extract_number)) {
        console.log(`   ⏭  #${extract._extract_number} - already exists, skipping`)
        skipped++
        continue
      }

      const extractType = extract._extract_number === 140 ? 'full' : 'update'
      const jobId = randomUUID()

      // Use snapshot_date + 00:00:00 as placeholder for extract_timestamp
      // We don't have the original meta.csv files, so this is honest placeholder
      const extractTimestamp = `${extract._snapshot_date} 00:00:00`

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
          ${extract._extract_number},
          '${extractType}',
          '${extract._snapshot_date}',
          '${extractTimestamp}',
          'completed',
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP,
          NULL,
          0,
          0,
          0,
          0,
          'backfill'
        )
      `)

      console.log(`   ✓ #${extract._extract_number} - ${extractType} - ${extract._snapshot_date}`)
      created++
    }

    // Step 4: Summary
    console.log(`\n📊 Summary:`)
    console.log(`   Created: ${created}`)
    console.log(`   Skipped: ${skipped}`)
    console.log(`   Total in database: ${created + skipped}`)

    // Step 5: Verify
    console.log(`\n✅ Backfill completed successfully!`)
    console.log(`\nNote: These records use:`)
    console.log(`  - worker_type='backfill' (honest labeling)`)
    console.log(`  - extract_timestamp=snapshot_date (placeholder, original unknown)`)
    console.log(`  - started_at/completed_at=CURRENT_TIMESTAMP (honest audit trail)`)
    console.log(`  - statistics=0 (original values unknown)`)

  } catch (error: any) {
    console.error('❌ Backfill failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
