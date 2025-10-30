#!/usr/bin/env tsx

/**
 * Discover what extracts have been imported by querying the actual data
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function main() {
  console.log('🔍 Discovering imported extracts from database\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    // Query all distinct extract numbers from enterprises table
    console.log('Querying enterprises table for extract numbers...')
    const extracts = await executeQuery<{
      _extract_number: number
      _snapshot_date: string
      record_count: number
    }>(conn, `
      SELECT
        _extract_number,
        _snapshot_date::VARCHAR as _snapshot_date,
        COUNT(*) as record_count
      FROM enterprises
      GROUP BY _extract_number, _snapshot_date
      ORDER BY _extract_number
    `)

    console.log(`\n✅ Found ${extracts.length} imported extracts:\n`)

    console.table(extracts.map(e => ({
      'Extract': e._extract_number,
      'Snapshot Date': e._snapshot_date,
      'Records': e.record_count.toLocaleString(),
      'Type': e._extract_number === 140 ? 'FULL' : 'UPDATE'
    })))

    // Summary
    const extractNumbers = extracts.map(e => e._extract_number)
    const min = Math.min(...extractNumbers)
    const max = Math.max(...extractNumbers)
    const full = extracts.filter(e => e._extract_number === 140).length
    const updates = extracts.filter(e => e._extract_number !== 140).length

    console.log(`\n📊 Summary:`)
    console.log(`   Range: ${min} - ${max}`)
    console.log(`   Full dumps: ${full}`)
    console.log(`   Updates: ${updates}`)
    console.log(`   Total: ${extracts.length}`)

    // Check for gaps
    const expected = max - min + 1
    if (extracts.length !== expected) {
      console.log(`\n⚠️  Warning: Found ${extracts.length} extracts but expected ${expected}`)
      console.log(`   There may be gaps in the import sequence`)

      const missing = []
      for (let i = min; i <= max; i++) {
        if (!extractNumbers.includes(i)) {
          missing.push(i)
        }
      }
      if (missing.length > 0) {
        console.log(`   Missing extracts: ${missing.join(', ')}`)
      }
    } else {
      console.log(`\n✓ No gaps detected - extracts ${min}-${max} are sequential`)
    }

  } catch (error: any) {
    console.error('❌ Discovery failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
