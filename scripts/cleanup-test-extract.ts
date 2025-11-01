#!/usr/bin/env tsx

/**
 * Cleanup script to remove test extract data
 * Removes all data for extract 167 (test data)
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

async function cleanupTestExtract() {
  const token = process.env.MOTHERDUCK_TOKEN
  const database = process.env.MOTHERDUCK_DATABASE || 'newagekbo'

  if (!token) {
    throw new Error('MOTHERDUCK_TOKEN not set in .env.local')
  }

  const db = await DuckDBInstance.create(':memory:')
  const conn = await db.connect()

  try {
    // Set directory configs
    await conn.run(`SET home_directory='/tmp'`)
    await conn.run(`SET extension_directory='/tmp/.duckdb/extensions'`)
    await conn.run(`SET temp_directory='/tmp'`)

    process.env.motherduck_token = token

    console.log('🧹 Cleaning up test extract data...\n')

    await conn.run(`ATTACH 'md:${database}' AS md`)
    await conn.run(`USE md`)

    const extractNumber = 167
    const snapshotDate = '2025-10-30'

    console.log(`   • Extract: ${extractNumber}`)
    console.log(`   • Snapshot Date: ${snapshotDate}\n`)

    // Delete from all main tables
    const tables = [
      'activities',
      'addresses',
      'contacts',
      'denominations',
      'enterprises',
      'establishments',
      'branches'
    ]

    for (const table of tables) {
      await conn.run(`
        DELETE FROM ${table}
        WHERE _extract_number = ${extractNumber}
          AND _snapshot_date = '${snapshotDate}'
      `)
      console.log(`   ✓ Cleaned ${table}`)
    }

    // Clean up any import jobs for this extract
    await conn.run(`
      DELETE FROM import_jobs
      WHERE extract_number = ${extractNumber}
        AND snapshot_date = '${snapshotDate}'
    `)
    console.log(`   ✓ Cleaned import_jobs\n`)

    console.log('✨ Cleanup complete!\n')

  } catch (error) {
    console.error('\n❌ Cleanup failed!\n')
    if (error instanceof Error) {
      console.error(`Error: ${error.message}\n`)
    }
    throw error
  }
}

cleanupTestExtract()
