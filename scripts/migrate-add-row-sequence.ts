#!/usr/bin/env tsx

/**
 * Migration script: Add row_sequence column to staging tables
 *
 * This column tracks the original CSV row order (1-based) so that when
 * duplicates exist in the CSV, we use the last occurrence (highest row_sequence)
 * as the authoritative record during INSERT operations.
 *
 * Usage:
 *   npx tsx scripts/migrate-add-row-sequence.ts
 *
 * This migration is idempotent - safe to run multiple times.
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

const STAGING_TABLES = [
  'import_staging_enterprises',
  'import_staging_establishments',
  'import_staging_denominations',
  'import_staging_addresses',
  'import_staging_contacts',
  'import_staging_activities',
  'import_staging_branches',
]

async function columnExists(
  conn: Awaited<ReturnType<Awaited<ReturnType<typeof DuckDBInstance.create>>['connect']>>,
  tableName: string,
  columnName: string
): Promise<boolean> {
  const result = await conn.run(`
    SELECT COUNT(*) as cnt
    FROM information_schema.columns
    WHERE table_name = '${tableName}'
      AND column_name = '${columnName}'
  `)

  const chunks = await result.fetchAllChunks()
  for (const chunk of chunks) {
    const rows = chunk.getRows()
    if (rows.length > 0) {
      return Number(rows[0][0]) > 0
    }
  }
  return false
}

async function runMigration() {
  console.log('🔄 Migration: Adding row_sequence column to staging tables\n')

  const token = process.env.MOTHERDUCK_TOKEN
  const database = process.env.MOTHERDUCK_DATABASE || 'newagekbo'

  if (!token) {
    throw new Error('MOTHERDUCK_TOKEN not set in .env.local')
  }

  // Create in-memory database instance
  const db = await DuckDBInstance.create(':memory:')
  const conn = await db.connect()

  try {
    // Set directory configs for serverless compatibility
    await conn.run(`SET home_directory='/tmp'`)
    await conn.run(`SET extension_directory='/tmp/.duckdb/extensions'`)
    await conn.run(`SET temp_directory='/tmp'`)

    // Set Motherduck token as environment variable
    process.env.motherduck_token = token

    // Attach motherduck database
    console.log(`📦 Connecting to Motherduck database: ${database}`)
    await conn.run(`ATTACH 'md:${database}' AS md`)
    await conn.run(`USE md`)
    console.log('   ✅ Connected\n')

    // Check each staging table
    let migratedCount = 0
    let skippedCount = 0
    let missingCount = 0

    for (const tableName of STAGING_TABLES) {
      process.stdout.write(`   Checking ${tableName}... `)

      // First check if table exists
      const tableExistsResult = await conn.run(`
        SELECT COUNT(*) as cnt
        FROM information_schema.tables
        WHERE table_name = '${tableName}'
      `)
      const tableChunks = await tableExistsResult.fetchAllChunks()
      let tableExists = false
      for (const chunk of tableChunks) {
        const rows = chunk.getRows()
        if (rows.length > 0 && Number(rows[0][0]) > 0) {
          tableExists = true
        }
      }

      if (!tableExists) {
        console.log('⏭️  Table does not exist (will be created on next schema run)')
        missingCount++
        continue
      }

      // Check if column already exists
      const hasColumn = await columnExists(conn, tableName, 'row_sequence')

      if (hasColumn) {
        console.log('✅ Column already exists')
        skippedCount++
        continue
      }

      // Add the column with default value 0 for existing rows
      // Note: DuckDB doesn't support NOT NULL constraint when adding columns
      // New imports will always provide row_sequence, existing staging data (if any) gets 0
      console.log('🔧 Adding column...')
      await conn.run(`
        ALTER TABLE ${tableName}
        ADD COLUMN row_sequence INTEGER DEFAULT 0
      `)
      console.log(`   ✅ Added row_sequence to ${tableName}`)
      migratedCount++
    }

    // Summary
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ Migration complete!')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log(`   Tables migrated: ${migratedCount}`)
    console.log(`   Tables skipped (already had column): ${skippedCount}`)
    console.log(`   Tables missing (not yet created): ${missingCount}`)
    console.log()

    if (missingCount > 0) {
      console.log('💡 Tip: Run "npx tsx scripts/create-schema.ts" to create missing tables')
      console.log()
    }

  } finally {
    conn.closeSync()
  }
}

runMigration().catch((error) => {
  console.error('\n❌ Migration failed:', error.message)
  process.exit(1)
})
