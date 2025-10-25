#!/usr/bin/env tsx

/**
 * Test temporal query fix - verify that point-in-time queries work correctly
 *
 * Test Case: Enterprise 0878.689.049 at Extract 150
 * - Extract 140 (full dump): Has denomination "A.S.B.L. Villers 2000"
 * - Extract 150 (incremental): No denomination records (not updated)
 * - Expected: Query for Extract 150 should return denomination from Extract 140
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck } from '../lib/motherduck'
import { fetchEnterpriseDetail } from '../lib/motherduck/enterprise-detail'

async function testTemporalQuery() {
  console.log('🧪 Testing Temporal Query Fix\n')

  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await db.all(`USE ${dbName}`)

    const enterpriseNumber = '0878.689.049'

    // Test 1: Current data (Extract 157)
    console.log('📊 Test 1: Current data (Extract 157)')
    const currentDetail = await fetchEnterpriseDetail(db, enterpriseNumber, { type: 'current' })
    console.log(`   Enterprise: ${currentDetail?.enterpriseNumber}`)
    console.log(`   Status: ${currentDetail?.status}`)
    console.log(`   Extract: ${currentDetail?.extractNumber}`)
    console.log(`   Snapshot: ${currentDetail?.snapshotDate}`)
    console.log(`   Denominations: ${currentDetail?.denominations.length || 0}`)
    if (currentDetail?.denominations.length) {
      currentDetail.denominations.forEach(d => {
        console.log(`      - ${d.denomination} (${d.language})`)
      })
    }
    console.log('')

    // Test 2: Point-in-time at Extract 150 (incremental - should show data from Extract 140)
    console.log('📊 Test 2: Point-in-time at Extract 150 (incremental)')
    const extract150Detail = await fetchEnterpriseDetail(db, enterpriseNumber, {
      type: 'point-in-time',
      extractNumber: 150,
      snapshotDate: '2025-10-12'
    })
    console.log(`   Enterprise: ${extract150Detail?.enterpriseNumber}`)
    console.log(`   Status: ${extract150Detail?.status}`)
    console.log(`   Extract: ${extract150Detail?.extractNumber}`)
    console.log(`   Snapshot: ${extract150Detail?.snapshotDate}`)
    console.log(`   Denominations: ${extract150Detail?.denominations.length || 0}`)
    if (extract150Detail?.denominations.length) {
      extract150Detail.denominations.forEach(d => {
        console.log(`      - ${d.denomination} (${d.language})`)
      })
    } else {
      console.log('   ⚠️  WARNING: No denominations found!')
    }
    console.log('')

    // Test 3: Point-in-time at Extract 140 (full dump)
    console.log('📊 Test 3: Point-in-time at Extract 140 (full dump)')
    const extract140Detail = await fetchEnterpriseDetail(db, enterpriseNumber, {
      type: 'point-in-time',
      extractNumber: 140,
      snapshotDate: '2025-10-05'
    })
    console.log(`   Enterprise: ${extract140Detail?.enterpriseNumber}`)
    console.log(`   Status: ${extract140Detail?.status}`)
    console.log(`   Extract: ${extract140Detail?.extractNumber}`)
    console.log(`   Snapshot: ${extract140Detail?.snapshotDate}`)
    console.log(`   Denominations: ${extract140Detail?.denominations.length || 0}`)
    if (extract140Detail?.denominations.length) {
      extract140Detail.denominations.forEach(d => {
        console.log(`      - ${d.denomination} (${d.language})`)
      })
    }
    console.log('')

    // Validation
    console.log('✅ VALIDATION')
    console.log('─'.repeat(60))

    if (extract150Detail?.denominations.length === 0) {
      console.log('❌ FAIL: Extract 150 should return denominations from Extract 140')
      console.log('   The temporal query fix is not working correctly.')
    } else if (extract150Detail?.denominations.length === extract140Detail?.denominations.length) {
      console.log('✅ PASS: Extract 150 correctly returns denominations from Extract 140')
      console.log(`   Found ${extract150Detail.denominations.length} denomination(s)`)
    } else {
      console.log('⚠️  PARTIAL: Extract 150 has denominations but count differs from Extract 140')
      console.log(`   Extract 150: ${extract150Detail?.denominations.length || 0}`)
      console.log(`   Extract 140: ${extract140Detail?.denominations.length || 0}`)
    }

  } catch (error) {
    console.error('❌ Test failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

testTemporalQuery().catch(console.error)
