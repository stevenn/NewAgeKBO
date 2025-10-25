#!/usr/bin/env tsx

/**
 * Compare addresses in detail for enterprise 0721.700.388
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function compareAddresses() {
  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await executeQuery(db, `USE ${dbName}`)

    const enterpriseNumber = '0721.700.388'

    console.log(`\n📊 Address data for ${enterpriseNumber}:\n`)

    const addresses = await executeQuery<{
      extract_number: number
      snapshot_date: string
      type_of_address: string
      country_nl: string | null
      country_fr: string | null
      zipcode: string | null
      municipality_nl: string | null
      municipality_fr: string | null
      street_nl: string | null
      street_fr: string | null
      house_number: string | null
      box: string | null
      extra_address_info: string | null
      date_striking_off: string | null
    }>(
      db,
      `SELECT
        _extract_number as extract_number,
        _snapshot_date::VARCHAR as snapshot_date,
        type_of_address,
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
        date_striking_off::VARCHAR as date_striking_off
      FROM addresses
      WHERE entity_number = '${enterpriseNumber}'
      ORDER BY _extract_number`
    )

    addresses.forEach((addr, idx) => {
      console.log(`\n─── Address #${idx + 1} - Extract ${addr.extract_number} (${addr.snapshot_date}) ───`)
      console.log(`Type: ${addr.type_of_address}`)
      console.log(`Country NL: "${addr.country_nl}"`)
      console.log(`Country FR: "${addr.country_fr}"`)
      console.log(`Zipcode: "${addr.zipcode}"`)
      console.log(`Municipality NL: "${addr.municipality_nl}"`)
      console.log(`Municipality FR: "${addr.municipality_fr}"`)
      console.log(`Street NL: "${addr.street_nl}"`)
      console.log(`Street FR: "${addr.street_fr}"`)
      console.log(`House Number: "${addr.house_number}"`)
      console.log(`Box: "${addr.box}"`)
      console.log(`Extra Info: "${addr.extra_address_info}"`)
      console.log(`Date Striking Off: "${addr.date_striking_off}"`)
    })

    // Show comparison key
    console.log('\n\n📊 Comparison Keys:\n')
    addresses.forEach((addr, idx) => {
      const key = `${addr.type_of_address}-${addr.street_nl}-${addr.zipcode}`
      console.log(`Address #${idx + 1} (Extract ${addr.extract_number}): "${key}"`)
    })

  } catch (error) {
    console.error('❌ Query failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

compareAddresses().catch(console.error)
