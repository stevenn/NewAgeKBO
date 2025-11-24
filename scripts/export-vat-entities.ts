#!/usr/bin/env tsx

/**
 * Export VAT-liable entities to MotherDuck table
 *
 * Creates a table in MotherDuck containing entity numbers for VAT-liable entities
 * based on activity groups 001 (VAT), 004 (Government), and 007 (Education).
 *
 * Usage:
 *   npx tsx scripts/export-vat-entities.ts
 *
 * The script will create a table in MotherDuck and provide a DuckDB CLI command
 * for downloading the data locally.
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { exportVatEntities } from '../lib/export/vat-entities'
import { formatUserError } from '../lib/errors'

async function main() {
  console.log('═══════════════════════════════════════════════════════════')
  console.log('  VAT-Liable Entities Export')
  console.log('═══════════════════════════════════════════════════════════\n')

  try {
    // Execute export
    const result = await exportVatEntities('cli')

    console.log('\n' + '='.repeat(60))
    console.log('✅ Export Complete!')
    console.log('='.repeat(60))
    console.log(`Job ID: ${result.job_id}`)
    console.log(`Table: ${result.table_name}`)
    console.log(`Records: ${result.records_exported.toLocaleString()}`)
    console.log(`Expires: ${new Date(result.expires_at).toLocaleString()}`)
    console.log('='.repeat(60))

    // Provide DuckDB CLI command
    const database = process.env.MOTHERDUCK_DATABASE || 'newagekbo'
    console.log('\n📥 Download CSV using DuckDB CLI:\n')
    console.log(`duckdb -c "COPY (SELECT * FROM md:${database}.${result.table_name}) TO 'vat-entities.csv' (FORMAT CSV, HEADER)"`)
    console.log('\n' + '='.repeat(60) + '\n')

  } catch (error) {
    console.error('\n❌ Export failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${formatUserError(error)}\n`)

      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

main()
