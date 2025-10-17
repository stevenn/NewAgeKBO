#!/usr/bin/env tsx

/**
 * Check NACE version distribution in Motherduck
 * Purpose: Determine if Extract 0140 Update is needed
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck } from '../lib/motherduck'

interface NaceVersionRow {
  nace_version: string
  count: number
  percentage: number
}

interface ActivityRow {
  entity_number: string
  nace_version: string
  nace_code: string
  classification: string
}

async function checkNaceVersions() {
  console.log('\n🔍 Checking NACE version distribution in Motherduck...\n')

  const db = await connectMotherduck()

  try {
    // Query 1: NACE version distribution in activities
    console.log('📊 NACE Versions in Activities Table:')
    const result: NaceVersionRow[] = await new Promise((resolve, reject) => {
      db.all(`
        SELECT
          nace_version,
          COUNT(*) as count,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM activities
        WHERE _is_current = true
        GROUP BY nace_version
        ORDER BY nace_version DESC
      `, (err, rows) => {
        if (err) reject(err)
        else resolve(rows as NaceVersionRow[])
      })
    })

    console.log('\n   Version | Count      | Percentage')
    console.log('   --------|------------|------------')

    if (result && result.length > 0) {
      for (const row of result) {
        console.log(`   ${row.nace_version}    | ${String(row.count).padStart(10)} | ${String(row.percentage).padStart(6)}%`)
      }
    } else {
      console.log('   ⚠️  No data returned')
    }

    // Query 2: Sample activities with NACE 2025
    console.log('\n\n📝 Sample Activities with NACE 2025:')
    const sample2025: ActivityRow[] = await new Promise((resolve, reject) => {
      db.all(`
        SELECT
          entity_number,
          nace_version,
          nace_code,
          classification
        FROM activities
        WHERE _is_current = true
          AND nace_version = '2025'
        LIMIT 5
      `, (err, rows) => {
        if (err) reject(err)
        else resolve(rows as ActivityRow[])
      })
    })

    if (sample2025.length > 0) {
      for (const row of sample2025) {
        console.log(`   • ${row.entity_number} - NACE ${row.nace_version}: ${row.nace_code} (${row.classification})`)
      }
    } else {
      console.log('   ⚠️  No NACE 2025 activities found!')
    }

    // Query 3: Sample activities with NACE 2008
    console.log('\n📝 Sample Activities with NACE 2008:')
    const sample2008: ActivityRow[] = await new Promise((resolve, reject) => {
      db.all(`
        SELECT
          entity_number,
          nace_version,
          nace_code,
          classification
        FROM activities
        WHERE _is_current = true
          AND nace_version = '2008'
        LIMIT 5
      `, (err, rows) => {
        if (err) reject(err)
        else resolve(rows as ActivityRow[])
      })
    })

    if (sample2008.length > 0) {
      for (const row of sample2008) {
        console.log(`   • ${row.entity_number} - NACE ${row.nace_version}: ${row.nace_code} (${row.classification})`)
      }
    } else {
      console.log('   ℹ️  No NACE 2008 activities found')
    }

    // Analysis
    const has2025 = result.find((r) => r.nace_version === '2025')
    const has2008 = result.find((r) => r.nace_version === '2008')
    const has2003 = result.find((r) => r.nace_version === '2003')

    console.log('\n' + '='.repeat(60))
    console.log('📊 ANALYSIS')
    console.log('='.repeat(60))

    if (has2025 && has2025.percentage > 40) {
      console.log('\n✅ VERDICT: Full dump already includes NACE 2025 migration')
      console.log('\n📋 Recommendation:')
      console.log('   • SKIP Extract 0140 Update (redundant)')
      console.log('   • Start daily updates from Extract 0141')
      console.log('\n💡 Reason: The full dump was prepared AFTER the NACE')
      console.log('   migration, so it already contains updated activities.')
    } else if (has2008 && has2008.percentage > 40) {
      console.log('\n⚠️  VERDICT: Full dump uses OLD NACE 2008 codes')
      console.log('\n📋 Recommendation:')
      console.log('   • APPLY Extract 0140 Update to migrate to NACE 2025')
      console.log('   • Then start daily updates from Extract 0141')
      console.log('\n💡 Reason: The full dump snapshot was taken BEFORE the')
      console.log('   NACE migration. We need the update to be current.')
    } else {
      console.log('\n❓ VERDICT: Mixed or unclear NACE version distribution')
      console.log('\n📋 Recommendation:')
      console.log('   • Review the percentages above carefully')
      console.log('   • Check with KBO documentation')
      console.log('   • Consider applying Extract 0140 Update to be safe')
    }

    console.log('\n' + '='.repeat(60) + '\n')

  } finally {
    await closeMotherduck(db)
  }
}

checkNaceVersions().catch(error => {
  console.error('\n❌ Check failed:', error)
  process.exit(1)
})
