#!/usr/bin/env tsx

/**
 * Check what extracts we have and which one is marked as current
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function checkExtracts() {
  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await executeQuery(db, `USE ${dbName}`)

    // Check all extracts we have
    console.log('\n📊 All extracts in database:\n')
    const allExtracts = await executeQuery<{
      extract_number: number
      snapshot_date: string
      extract_type: string
      has_current: boolean
      record_count: number
    }>(
      db,
      `SELECT
        _extract_number as extract_number,
        _snapshot_date::VARCHAR as snapshot_date,
        MAX(_is_current::INTEGER) as has_current,
        COUNT(*) as record_count
      FROM enterprises
      GROUP BY _extract_number, _snapshot_date
      ORDER BY _extract_number`
    )

    allExtracts.forEach(e => {
      console.log(`   Extract ${e.extract_number} (${e.snapshot_date}) - ${e.record_count} records - has_current=${e.has_current === 1}`)
    })

    // Check metadata table if it exists
    console.log('\n📊 Checking metadata table:\n')
    try {
      const metadata = await executeQuery<{
        extract_number: number
        snapshot_date: string
        extract_type: string
      }>(
        db,
        `SELECT
          extract_number,
          snapshot_date::VARCHAR as snapshot_date,
          extract_type
        FROM metadata
        ORDER BY extract_number`
      )

      metadata.forEach(m => {
        console.log(`   Extract ${m.extract_number} (${m.snapshot_date}) - Type: ${m.extract_type}`)
      })
    } catch (e) {
      console.log('   (No metadata table found)')
    }

    // Check specifically for 0721.700.388
    console.log('\n📊 Extract 157 details for 0721.700.388:\n')
    const extract157Details = await executeQuery<{
      table_name: string
      record_count: number
    }>(
      db,
      `SELECT 'addresses' as table_name, COUNT(*) as record_count
      FROM addresses
      WHERE entity_number = '0721.700.388' AND _extract_number = 157
      UNION ALL
      SELECT 'denominations', COUNT(*)
      FROM denominations
      WHERE entity_number = '0721.700.388' AND _extract_number = 157
      UNION ALL
      SELECT 'activities', COUNT(*)
      FROM activities
      WHERE entity_number = '0721.700.388' AND _extract_number = 157
      UNION ALL
      SELECT 'contacts', COUNT(*)
      FROM contacts
      WHERE entity_number = '0721.700.388' AND _extract_number = 157`
    )

    extract157Details.forEach(d => {
      console.log(`   ${d.table_name}: ${d.record_count} records`)
    })

    // Check if there are any extracts after 157
    console.log('\n📊 Highest extract number:\n')
    const highest = await executeQuery<{
      max_extract: number
      table_name: string
    }>(
      db,
      `SELECT 'enterprises' as table_name, MAX(_extract_number) as max_extract FROM enterprises
      UNION ALL
      SELECT 'addresses', MAX(_extract_number) FROM addresses
      UNION ALL
      SELECT 'denominations', MAX(_extract_number) FROM denominations`
    )

    highest.forEach(h => {
      console.log(`   ${h.table_name}: Extract ${h.max_extract}`)
    })

  } catch (error) {
    console.error('❌ Query failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

checkExtracts().catch(console.error)
