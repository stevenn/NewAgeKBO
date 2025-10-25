#!/usr/bin/env tsx

/**
 * Check what snapshots exist for specific enterprises
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function checkSnapshots() {
  const enterprises = ['0801.668.871', '0721.700.388', '0533.711.618']

  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await executeQuery(db, `USE ${dbName}`)

    for (const number of enterprises) {
      console.log(`\n📊 Checking ${number}:\n`)

      // Check enterprises table
      const enterpriseSnapshots = await executeQuery<{
        extract_number: number
        snapshot_date: string
        is_current: boolean
      }>(
        db,
        `SELECT DISTINCT
          _extract_number as extract_number,
          _snapshot_date::VARCHAR as snapshot_date,
          _is_current as is_current
        FROM enterprises
        WHERE enterprise_number = '${number}'
        ORDER BY _snapshot_date DESC, _extract_number DESC`
      )

      console.log(`   Enterprises table (${enterpriseSnapshots.length} snapshots):`)
      enterpriseSnapshots.forEach(s => {
        console.log(`      Extract ${s.extract_number} - ${s.snapshot_date}${s.is_current ? ' (CURRENT)' : ''}`)
      })

      // Check denominations
      const denomSnapshots = await executeQuery<{
        extract_number: number
        count: number
      }>(
        db,
        `SELECT
          _extract_number as extract_number,
          COUNT(*) as count
        FROM denominations
        WHERE entity_number = '${number}'
        GROUP BY _extract_number
        ORDER BY _extract_number`
      )

      console.log(`\n   Denominations by extract:`)
      denomSnapshots.forEach(s => {
        console.log(`      Extract ${s.extract_number}: ${s.count} denominations`)
      })

      // Check addresses
      const addressSnapshots = await executeQuery<{
        extract_number: number
        count: number
      }>(
        db,
        `SELECT
          _extract_number as extract_number,
          COUNT(*) as count
        FROM addresses
        WHERE entity_number = '${number}'
        GROUP BY _extract_number
        ORDER BY _extract_number`
      )

      console.log(`\n   Addresses by extract:`)
      addressSnapshots.forEach(s => {
        console.log(`      Extract ${s.extract_number}: ${s.count} addresses`)
      })
    }

  } catch (error) {
    console.error('❌ Query failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

checkSnapshots().catch(console.error)
