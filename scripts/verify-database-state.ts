#!/usr/bin/env tsx

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, executeQuery, closeMotherduck } from '../lib/motherduck'

async function verify() {
  const db = await connectMotherduck()
  await executeQuery(db, `USE ${process.env.MOTHERDUCK_DATABASE}`)

  console.log('📊 Database State Verification\n')
  console.log('='.repeat(60))

  // Extract numbers present
  const extractCheck = await executeQuery(db, `
    SELECT DISTINCT _extract_number
    FROM enterprises
    ORDER BY _extract_number
  `)
  console.log('Extract numbers present:', extractCheck.map(r => r._extract_number).join(', '))

  // Latest extract
  const latestExtract = extractCheck[extractCheck.length - 1]._extract_number
  console.log(`Latest extract: ${latestExtract}`)

  // Current snapshot date
  const snapshotCheck = await executeQuery(db, `
    SELECT DISTINCT _snapshot_date
    FROM enterprises
    WHERE _extract_number = ${latestExtract}
    LIMIT 1
  `)
  console.log(`Latest snapshot date: ${snapshotCheck[0]._snapshot_date}`)

  console.log('\n' + '='.repeat(60))
  console.log('Current Records by Table (where _is_current = true)')
  console.log('='.repeat(60))

  const tables = ['enterprises', 'establishments', 'activities', 'addresses', 'branches', 'contacts', 'denominations']

  for (const table of tables) {
    const result = await executeQuery(db, `
      SELECT COUNT(*) as count
      FROM ${table}
      WHERE _is_current = true
    `)
    const count = Number(result[0].count).toLocaleString()
    console.log(`${table.padEnd(20)} ${count.padStart(12)}`)
  }

  console.log('\n' + '='.repeat(60))
  console.log('Total Records by Table (all extracts)')
  console.log('='.repeat(60))

  let grandTotal = 0
  for (const table of tables) {
    const result = await executeQuery(db, `
      SELECT COUNT(*) as count
      FROM ${table}
    `)
    const count = Number(result[0].count)
    grandTotal += count
    console.log(`${table.padEnd(20)} ${count.toLocaleString().padStart(12)}`)
  }

  console.log('─'.repeat(60))
  console.log(`${'TOTAL'.padEnd(20)} ${grandTotal.toLocaleString().padStart(12)}`)

  console.log('\n' + '='.repeat(60))
  console.log('Records by Extract Number')
  console.log('='.repeat(60))

  for (const extract of extractCheck) {
    const result = await executeQuery(db, `
      SELECT COUNT(*) as count
      FROM enterprises
      WHERE _extract_number = ${extract._extract_number}
    `)
    const count = Number(result[0].count).toLocaleString()
    console.log(`Extract ${extract._extract_number.toString().padEnd(4)} ${count.padStart(12)} enterprises`)
  }

  console.log('\n✅ Database verification complete\n')

  await closeMotherduck(db)
}

verify()
