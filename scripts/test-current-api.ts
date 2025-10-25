#!/usr/bin/env tsx

/**
 * Test what the API returns for current snapshot
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

const enterpriseNumber = '0721.700.388'

async function testAPI() {
  console.log(`\n📊 Testing API for ${enterpriseNumber}\n`)

  // Test current data (no params)
  console.log('1️⃣ Current data (no params):')
  const currentRes = await fetch(`http://localhost:3000/api/enterprises/${enterpriseNumber}`)
  const currentData = await currentRes.json()
  console.log(`   Status: ${currentRes.status}`)
  console.log(`   Response:`, JSON.stringify(currentData, null, 2))

  // Test Extract 157 explicitly
  console.log('\n2️⃣ Extract 157 (explicit):')
  const extract157Res = await fetch(`http://localhost:3000/api/enterprises/${enterpriseNumber}?snapshot_date=2025-10-21&extract_number=157`)
  const extract157Data = await extract157Res.json()
  console.log(`   Extract: ${extract157Data.extractNumber}`)
  console.log(`   Snapshot: ${extract157Data.snapshotDate}`)
  console.log(`   Addresses: ${extract157Data.addresses.length}`)
  extract157Data.addresses.forEach((addr: any) => {
    console.log(`      - ${addr.streetNL} ${addr.houseNumber}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipalityNL}`)
  })

  // Test Extract 150
  console.log('\n3️⃣ Extract 150:')
  const extract150Res = await fetch(`http://localhost:3000/api/enterprises/${enterpriseNumber}?snapshot_date=2025-10-14&extract_number=150`)
  const extract150Data = await extract150Res.json()
  console.log(`   Extract: ${extract150Data.extractNumber}`)
  console.log(`   Snapshot: ${extract150Data.snapshotDate}`)
  console.log(`   Addresses: ${extract150Data.addresses.length}`)
  extract150Data.addresses.forEach((addr: any) => {
    console.log(`      - ${addr.streetNL} ${addr.houseNumber}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipalityNL}`)
  })

  // Test Extract 140
  console.log('\n4️⃣ Extract 140:')
  const extract140Res = await fetch(`http://localhost:3000/api/enterprises/${enterpriseNumber}?snapshot_date=2025-10-04&extract_number=140`)
  const extract140Data = await extract140Res.json()
  console.log(`   Extract: ${extract140Data.extractNumber}`)
  console.log(`   Snapshot: ${extract140Data.snapshotDate}`)
  console.log(`   Addresses: ${extract140Data.addresses.length}`)
  extract140Data.addresses.forEach((addr: any) => {
    console.log(`      - ${addr.streetNL} ${addr.houseNumber}${addr.box ? ` box ${addr.box}` : ''}, ${addr.zipcode} ${addr.municipalityNL}`)
  })
}

testAPI().catch(console.error)
