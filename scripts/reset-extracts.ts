#!/usr/bin/env tsx

/**
 * Reset database to a specific extract number
 * Deletes all records where _extract_number > N
 * Marks all Extract N records as current
 *
 * Usage: npx tsx scripts/reset-extracts.ts <extract-number>
 * Example: npx tsx scripts/reset-extracts.ts 140
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, executeStatement, closeMotherduck, executeQuery } from '../lib/motherduck'

async function reset(targetExtract: number) {
  const db = await connectMotherduck()
  await executeQuery(db, `USE ${process.env.MOTHERDUCK_DATABASE}`)

  console.log(`🧹 Resetting database to Extract ${targetExtract}...\n`)

  const tables = ['activities', 'addresses', 'branches', 'contacts', 'denominations', 'enterprises', 'establishments']

  let totalDeleted = 0

  for (const table of tables) {
    // Count records from extracts > targetExtract
    const result = await executeQuery(db, `
      SELECT COUNT(*) as count
      FROM ${table}
      WHERE _extract_number > ${targetExtract}
    `)
    const count = Number(result[0].count)

    if (count > 0) {
      // Delete all records from extracts > targetExtract
      await executeStatement(db, `
        DELETE FROM ${table}
        WHERE _extract_number > ${targetExtract}
      `)
      console.log(`   ✓ ${table}: Deleted ${count} records`)
      totalDeleted += count
    } else {
      console.log(`   - ${table}: No records to delete`)
    }

    // Ensure all Extract targetExtract records are marked as current
    await executeStatement(db, `
      UPDATE ${table}
      SET _is_current = true
      WHERE _extract_number = ${targetExtract}
    `)
  }

  // Verify final state
  console.log('\n📊 Verifying database state...\n')

  const extractCheck = await executeQuery(db, `
    SELECT DISTINCT _extract_number
    FROM enterprises
    ORDER BY _extract_number
  `)

  console.log(`   Extract numbers remaining: ${extractCheck.map(r => r._extract_number).join(', ')}`)

  const currentCount = await executeQuery(db, `
    SELECT COUNT(*) as count
    FROM enterprises
    WHERE _is_current = true
  `)

  console.log(`   Current enterprises: ${Number(currentCount[0].count).toLocaleString()}`)

  console.log(`\n✅ Reset complete - Deleted ${totalDeleted.toLocaleString()} records`)
  console.log(`📊 Database is now at Extract ${targetExtract} state`)

  await closeMotherduck(db)
}

// Main
const args = process.argv.slice(2)

if (args.length === 0) {
  console.error('Usage: npx tsx scripts/reset-extracts.ts <extract-number>')
  console.error('\nExample:')
  console.error('  npx tsx scripts/reset-extracts.ts 140  # Reset to Extract 140')
  process.exit(1)
}

const targetExtract = parseInt(args[0], 10)

if (isNaN(targetExtract)) {
  console.error(`❌ Invalid extract number: ${args[0]}`)
  process.exit(1)
}

reset(targetExtract)
