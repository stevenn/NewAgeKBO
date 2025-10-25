#!/usr/bin/env tsx

/**
 * Find enterprises that have actual changes across extracts
 * to test the comparison UI
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function findChangedEnterprises() {
  console.log('🔍 Finding enterprises with changes across extracts\n')

  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await executeQuery(db, `USE ${dbName}`)

    // Find enterprises with denominations in multiple extracts
    console.log('📊 Enterprises with denomination changes:\n')
    const denomChanges = await executeQuery<{
      entity_number: string
      extract_count: number
      extracts: string
    }>(
      db,
      `SELECT
        entity_number,
        COUNT(DISTINCT _extract_number) as extract_count,
        LIST(DISTINCT _extract_number ORDER BY _extract_number) as extracts
      FROM denominations
      WHERE entity_type = 'enterprise'
      GROUP BY entity_number
      HAVING COUNT(DISTINCT _extract_number) > 1
      ORDER BY extract_count DESC
      LIMIT 10`
    )

    denomChanges.forEach(row => {
      console.log(`   ${row.entity_number} - ${row.extract_count} extracts (${JSON.stringify(row.extracts)})`)
    })

    // Find enterprises with address changes
    console.log('\n📊 Enterprises with address changes:\n')
    const addressChanges = await executeQuery<{
      entity_number: string
      extract_count: number
      extracts: string
    }>(
      db,
      `SELECT
        entity_number,
        COUNT(DISTINCT _extract_number) as extract_count,
        LIST(DISTINCT _extract_number ORDER BY _extract_number) as extracts
      FROM addresses
      GROUP BY entity_number
      HAVING COUNT(DISTINCT _extract_number) > 1
      ORDER BY extract_count DESC
      LIMIT 10`
    )

    addressChanges.forEach(row => {
      console.log(`   ${row.entity_number} - ${row.extract_count} extracts (${JSON.stringify(row.extracts)})`)
    })

    // Find enterprises with changes in extract 150 or 157 (incremental updates)
    console.log('\n📊 Enterprises updated in Extract 150 (incremental):\n')
    const extract150 = await executeQuery<{
      enterprise_number: string
      status: string
    }>(
      db,
      `SELECT DISTINCT enterprise_number, status
      FROM enterprises
      WHERE _extract_number IN (150, 157)
      LIMIT 10`
    )

    extract150.forEach(row => {
      console.log(`   ${row.enterprise_number} - Status: ${row.status}`)
    })

    // Check what changed for one of the enterprises
    if (extract150.length > 0) {
      const testNumber = extract150[0].enterprise_number
      console.log(`\n📊 Detailed check for ${testNumber}:\n`)

      const details = await executeQuery<{
        extract_number: number
        snapshot_date: string
        denom_count: number
        address_count: number
        activity_count: number
      }>(
        db,
        `SELECT
          e._extract_number as extract_number,
          e._snapshot_date::VARCHAR as snapshot_date,
          (SELECT COUNT(*) FROM denominations d WHERE d.entity_number = e.enterprise_number AND d._extract_number = e._extract_number) as denom_count,
          (SELECT COUNT(*) FROM addresses a WHERE a.entity_number = e.enterprise_number AND a._extract_number = e._extract_number) as address_count,
          (SELECT COUNT(*) FROM activities ac WHERE ac.entity_number = e.enterprise_number AND ac._extract_number = e._extract_number) as activity_count
        FROM enterprises e
        WHERE e.enterprise_number = '${testNumber}'
        ORDER BY e._extract_number`
      )

      details.forEach(row => {
        console.log(`   Extract ${row.extract_number} (${row.snapshot_date}):`)
        console.log(`      Denominations: ${row.denom_count}`)
        console.log(`      Addresses: ${row.address_count}`)
        console.log(`      Activities: ${row.activity_count}`)
      })
    }

  } catch (error) {
    console.error('❌ Query failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

findChangedEnterprises().catch(console.error)
