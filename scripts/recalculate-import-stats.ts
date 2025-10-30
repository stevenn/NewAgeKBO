#!/usr/bin/env tsx

/**
 * Recalculate statistics for import jobs
 *
 * Queries the actual data tables to determine:
 * - records_inserted: Records with _extract_number = N
 * - records_deleted: Records with _deleted_at_extract = N
 * - records_processed: records_inserted + records_deleted
 * - records_updated: Always 0 (by design in import logic)
 *
 * Sums across all 7 data tables with temporal tracking:
 * enterprises, establishments, denominations, addresses, activities, contacts, branches
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../lib/motherduck'

const DATA_TABLES = [
  'enterprises',
  'establishments',
  'denominations',
  'addresses',
  'activities',
  'contacts',
  'branches'
]

interface ExtractStats {
  records_inserted: number
  records_deleted: number
  records_processed: number
  records_updated: number
}

async function calculateStatsForExtract(conn: any, extractNumber: number): Promise<ExtractStats> {
  // Build a UNION ALL query that sums inserts and deletes across all tables
  const unionQuery = DATA_TABLES.map(table => `
    SELECT
      COUNT(*) FILTER (WHERE _extract_number = ${extractNumber}) as inserts,
      COUNT(*) FILTER (WHERE _deleted_at_extract = ${extractNumber}) as deletes
    FROM ${table}
  `).join(' UNION ALL ')

  const sql = `
    WITH table_stats AS (
      ${unionQuery}
    )
    SELECT
      SUM(inserts) as records_inserted,
      SUM(deletes) as records_deleted
    FROM table_stats
  `

  const results = await executeQuery<{
    records_inserted: number
    records_deleted: number
  }>(conn, sql)

  const inserted = Number(results[0]?.records_inserted || 0)
  const deleted = Number(results[0]?.records_deleted || 0)

  return {
    records_inserted: inserted,
    records_deleted: deleted,
    records_processed: inserted + deleted,
    records_updated: 0  // Always 0, matching the import logic
  }
}

async function main() {
  console.log('📊 Recalculating import job statistics\n')
  console.log('This will query 7 data tables with ~46M total records')
  console.log('Estimated time: 30-60 seconds\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    // Get all jobs that need statistics recalculated
    console.log('Step 1: Finding jobs to recalculate...')
    const jobs = await executeQuery<{
      extract_number: number
      extract_type: string
      worker_type: string
    }>(conn, `
      SELECT extract_number, extract_type, worker_type
      FROM import_jobs
      WHERE records_processed = 0 OR records_processed IS NULL
      ORDER BY extract_number
    `)

    console.log(`✓ Found ${jobs.length} jobs to recalculate\n`)

    if (jobs.length === 0) {
      console.log('No jobs need recalculation!')
      return
    }

    // Calculate and update statistics for each job
    console.log('Step 2: Recalculating statistics...\n')

    let updated = 0
    const startTime = Date.now()

    for (const job of jobs) {
      const stats = await calculateStatsForExtract(conn, job.extract_number)

      await executeStatement(conn, `
        UPDATE import_jobs
        SET
          records_inserted = ${stats.records_inserted},
          records_deleted = ${stats.records_deleted},
          records_processed = ${stats.records_processed},
          records_updated = ${stats.records_updated}
        WHERE extract_number = ${job.extract_number}
      `)

      console.log(
        `   ✓ #${job.extract_number} (${job.extract_type}): ` +
        `${stats.records_processed.toLocaleString()} processed ` +
        `(${stats.records_inserted.toLocaleString()} inserted, ${stats.records_deleted.toLocaleString()} deleted)`
      )

      updated++
    }

    const duration = ((Date.now() - startTime) / 1000).toFixed(1)

    console.log(`\n📊 Summary:`)
    console.log(`   Jobs updated: ${updated}`)
    console.log(`   Duration: ${duration}s`)
    console.log(`   Average: ${(parseFloat(duration) / updated).toFixed(1)}s per job`)

    // Validation
    console.log(`\n🔍 Validation:`)
    const validation = await executeQuery<{
      extract_number: number
      records_inserted: number
      records_deleted: number
      records_processed: number
      is_valid: boolean
    }>(conn, `
      SELECT
        extract_number,
        records_inserted,
        records_deleted,
        records_processed,
        (records_processed = records_inserted + records_deleted) as is_valid
      FROM import_jobs
      WHERE extract_number IN (${jobs.map(j => j.extract_number).join(',')})
      ORDER BY extract_number
    `)

    const allValid = validation.every(v => v.is_valid)
    if (allValid) {
      console.log(`   ✅ All statistics are mathematically valid`)
      console.log(`   (records_processed = records_inserted + records_deleted for all jobs)`)
    } else {
      console.log(`   ⚠️  Some statistics may be invalid`)
      validation.filter(v => !v.is_valid).forEach(v => {
        console.log(`   ❌ #${v.extract_number}: ${v.records_processed} ≠ ${v.records_inserted} + ${v.records_deleted}`)
      })
    }

    console.log(`\n✅ Recalculation completed successfully!`)

  } catch (error: any) {
    console.error('❌ Recalculation failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
