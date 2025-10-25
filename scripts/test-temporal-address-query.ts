#!/usr/bin/env tsx

/**
 * Test temporal address query directly
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'
import { buildChildTableQuery } from '../lib/motherduck/temporal-query'

async function testTemporalQuery() {
  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await executeQuery(db, `USE ${dbName}`)

    const enterpriseNumber = '0721.700.388'

    // Test 1: Current query
    console.log('\n1️⃣ Current query (_is_current = true):\n')
    const currentQuery = buildChildTableQuery(
      'addresses',
      `type_of_address,
      country_nl,
      country_fr,
      zipcode,
      municipality_nl,
      municipality_fr,
      street_nl,
      street_fr,
      house_number,
      box,
      extra_address_info,
      date_striking_off::VARCHAR as date_striking_off`,
      enterpriseNumber,
      { type: 'current' },
      'type_of_address',
      'id'
    )

    console.log('Query:', currentQuery)
    console.log('\nResults:')

    const current = await executeQuery<any>(db, currentQuery)
    current.forEach((addr) => {
      console.log(`   ${addr.street_nl} ${addr.house_number}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipality_nl}`)
    })

    // Test 2: Point-in-time at Extract 157
    console.log('\n\n2️⃣ Point-in-time at Extract 157:\n')
    const extract157Query = buildChildTableQuery(
      'addresses',
      `type_of_address,
      country_nl,
      country_fr,
      zipcode,
      municipality_nl,
      municipality_fr,
      street_nl,
      street_fr,
      house_number,
      box,
      extra_address_info,
      date_striking_off::VARCHAR as date_striking_off`,
      enterpriseNumber,
      {
        type: 'point-in-time',
        extractNumber: 157,
        snapshotDate: '2025-10-21'
      },
      'type_of_address',
      'id'
    )

    console.log('Query:', extract157Query)
    console.log('\nResults:')

    const extract157 = await executeQuery<any>(db, extract157Query)
    extract157.forEach((addr) => {
      console.log(`   ${addr.street_nl} ${addr.house_number}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipality_nl}`)
    })

    // Test 3: Point-in-time at Extract 150
    console.log('\n\n3️⃣ Point-in-time at Extract 150:\n')
    const extract150Query = buildChildTableQuery(
      'addresses',
      `type_of_address,
      country_nl,
      country_fr,
      zipcode,
      municipality_nl,
      municipality_fr,
      street_nl,
      street_fr,
      house_number,
      box,
      extra_address_info,
      date_striking_off::VARCHAR as date_striking_off`,
      enterpriseNumber,
      {
        type: 'point-in-time',
        extractNumber: 150,
        snapshotDate: '2025-10-14'
      },
      'type_of_address',
      'id'
    )

    console.log('Query:', extract150Query)
    console.log('\nResults:')

    const extract150 = await executeQuery<any>(db, extract150Query)
    extract150.forEach((addr) => {
      console.log(`   ${addr.street_nl} ${addr.house_number}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipality_nl}`)
    })

    // Test 4: What does _is_current show?
    console.log('\n\n4️⃣ All addresses with _is_current flag:\n')
    const allAddresses = await executeQuery<any>(
      db,
      `SELECT
        _extract_number,
        _snapshot_date::VARCHAR as snapshot_date,
        _is_current,
        street_nl,
        house_number,
        box,
        zipcode,
        municipality_nl
      FROM addresses
      WHERE entity_number = '${enterpriseNumber}'
      ORDER BY _extract_number`
    )

    allAddresses.forEach((addr) => {
      console.log(`   Extract ${addr._extract_number} (${addr.snapshot_date}) [_is_current=${addr._is_current}]:`)
      console.log(`      ${addr.street_nl} ${addr.house_number}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipality_nl}`)
    })

  } catch (error) {
    console.error('❌ Query failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

testTemporalQuery().catch(console.error)
