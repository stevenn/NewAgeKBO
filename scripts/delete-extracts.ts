#!/usr/bin/env tsx

/**
 * Delete specific extract(s) from the database
 * Can delete a single extract or a range of extracts
 * Also reverts records that were marked as historical by deleted extracts
 *
 * Usage:
 *   npx tsx scripts/delete-extracts.ts <extract-number>
 *   npx tsx scripts/delete-extracts.ts <start-extract> <end-extract>
 *
 * Examples:
 *   npx tsx scripts/delete-extracts.ts 143        # Delete Extract 143
 *   npx tsx scripts/delete-extracts.ts 143 152    # Delete Extracts 143-152
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, executeStatement, closeMotherduck, executeQuery } from '../lib/motherduck'

async function deleteExtracts(startExtract: number, endExtract?: number) {
  const db = await connectMotherduck()
  await executeQuery(db, `USE ${process.env.MOTHERDUCK_DATABASE}`)

  const extractRange = endExtract ? `${startExtract}-${endExtract}` : `${startExtract}`
  console.log(`🧹 Deleting Extract(s) ${extractRange}...\n`)

  const tables = ['activities', 'addresses', 'branches', 'contacts', 'denominations', 'enterprises', 'establishments']

  // Build the extract list
  const extracts = endExtract
    ? Array.from({ length: endExtract - startExtract + 1 }, (_, i) => startExtract + i)
    : [startExtract]

  console.log(`Extract numbers to delete: ${extracts.join(', ')}\n`)

  let totalDeleted = 0

  // Step 1: Delete records from target extracts
  for (const table of tables) {
    const result = await executeQuery<{ count: number }>(db, `
      SELECT COUNT(*) as count
      FROM ${table}
      WHERE _extract_number IN (${extracts.join(',')})
    `)
    const count = Number(result[0].count)

    if (count > 0) {
      await executeStatement(db, `
        DELETE FROM ${table}
        WHERE _extract_number IN (${extracts.join(',')})
      `)
      console.log(`   ✓ ${table}: Deleted ${count} records`)
      totalDeleted += count
    } else {
      console.log(`   - ${table}: No records to delete`)
    }
  }

  // Step 2: Find the snapshot date(s) of the deleted extracts to revert historical markings
  const snapshotDates = await executeQuery<{ _snapshot_date: string }>(db, `
    SELECT DISTINCT _snapshot_date
    FROM enterprises
    WHERE _extract_number IN (SELECT MAX(_extract_number) FROM enterprises WHERE _extract_number < ${startExtract})
    UNION
    SELECT DISTINCT _snapshot_date
    FROM enterprises
    WHERE _extract_number = (SELECT MAX(_extract_number) FROM enterprises)
    LIMIT 2
  `)

  if (snapshotDates.length > 0) {
    console.log('\n🔄 Reverting records marked as historical...\n')

    // Get the date just before the first deleted extract
    const beforeExtract = startExtract - 1
    const beforeSnapshotResult = await executeQuery<{ _snapshot_date: string }>(db, `
      SELECT DISTINCT _snapshot_date
      FROM enterprises
      WHERE _extract_number = ${beforeExtract}
      LIMIT 1
    `)

    if (beforeSnapshotResult.length > 0) {
      const beforeDate = new Date(beforeSnapshotResult[0]._snapshot_date)
      const afterDate = new Date(beforeDate)
      afterDate.setDate(afterDate.getDate() + 1) // Day after the before extract

      for (const table of tables) {
        // Find records from earlier extracts that were marked historical by deleted extracts
        const result = await executeQuery<{ count: number }>(db, `
          SELECT COUNT(*) as count
          FROM ${table}
          WHERE _extract_number < ${startExtract}
            AND _is_current = false
            AND _snapshot_date >= '${afterDate.toISOString().split('T')[0]}'
        `)
        const count = Number(result[0].count)

        if (count > 0) {
          await executeStatement(db, `
            UPDATE ${table}
            SET _is_current = true, _snapshot_date = '${beforeDate.toISOString().split('T')[0]}'
            WHERE _extract_number < ${startExtract}
              AND _is_current = false
              AND _snapshot_date >= '${afterDate.toISOString().split('T')[0]}'
          `)
          console.log(`   ✓ ${table}: Reverted ${count} records to current`)
        }
      }
    }
  }

  // Step 3: Verify final state
  console.log('\n📊 Verifying database state...\n')

  const extractCheck = await executeQuery<{ _extract_number: number }>(db, `
    SELECT DISTINCT _extract_number
    FROM enterprises
    ORDER BY _extract_number
  `)

  console.log(`   Extract numbers remaining: ${extractCheck.map((r) => r._extract_number).join(', ')}`)

  const latestExtract = extractCheck.length > 0 ? extractCheck[extractCheck.length - 1]._extract_number : 'none'
  console.log(`   Latest extract: ${latestExtract}`)

  console.log(`\n✅ Deletion complete - Deleted ${totalDeleted.toLocaleString()} records`)
  console.log(`📊 Database is now at Extract ${latestExtract} state`)

  await closeMotherduck(db)
}

// Main
const args = process.argv.slice(2)

if (args.length === 0) {
  console.error('Usage: npx tsx scripts/delete-extracts.ts <extract-number> [end-extract-number]')
  console.error('\nExamples:')
  console.error('  npx tsx scripts/delete-extracts.ts 143        # Delete Extract 143')
  console.error('  npx tsx scripts/delete-extracts.ts 143 152    # Delete Extracts 143-152')
  process.exit(1)
}

const startExtract = parseInt(args[0], 10)
const endExtract = args[1] ? parseInt(args[1], 10) : undefined

if (isNaN(startExtract)) {
  console.error(`❌ Invalid start extract number: ${args[0]}`)
  process.exit(1)
}

if (endExtract !== undefined && isNaN(endExtract)) {
  console.error(`❌ Invalid end extract number: ${args[1]}`)
  process.exit(1)
}

if (endExtract !== undefined && endExtract < startExtract) {
  console.error(`❌ End extract (${endExtract}) must be >= start extract (${startExtract})`)
  process.exit(1)
}

deleteExtracts(startExtract, endExtract)
