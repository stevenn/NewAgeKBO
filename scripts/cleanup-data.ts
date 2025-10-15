#!/usr/bin/env tsx

/**
 * Cleanup script - deletes all data from tables
 * Use before re-running initial import
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import {
  connectMotherduck,
  closeMotherduck,
  executeStatement,
  getMotherduckConfig,
} from '../lib/motherduck'

async function cleanup() {
  console.log('🧹 Cleaning up database...\n')

  const mdConfig = getMotherduckConfig()
  const db = await connectMotherduck()
  await executeStatement(db, `USE ${mdConfig.database}`)

  const tables = [
    'codes',
    'nace_codes',
    'enterprises',
    'establishments',
    'denominations',
    'addresses',
    'activities',
    'contacts',
    'branches',
    'import_jobs',
  ]

  for (const table of tables) {
    console.log(`   🗑️  Deleting from ${table}...`)
    await executeStatement(db, `DELETE FROM ${table}`)
  }

  console.log('\n✅ All tables cleaned\n')

  await closeMotherduck(db)
}

cleanup().catch(console.error)
