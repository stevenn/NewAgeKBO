#!/usr/bin/env tsx

/**
 * Migration script to update staging tables schema
 * Drops and recreates staging tables to allow NULL data columns for DELETE operations
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'
import { readFileSync } from 'fs'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

async function migrateStagingTables() {
  console.log('🔄 Migrating staging tables schema...\n')

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

    console.log('1️⃣  Connecting to Motherduck...')
    // Attach motherduck database
    await conn.run(`ATTACH 'md:${database}' AS md`)
    await conn.run(`USE md`)
    console.log('   ✅ Connected successfully!\n')

    // List of staging tables to recreate
    const stagingTables = [
      'import_staging_enterprises',
      'import_staging_establishments',
      'import_staging_denominations',
      'import_staging_addresses',
      'import_staging_contacts',
      'import_staging_activities',
      'import_staging_branches'
    ]

    // Step 1: Drop existing staging tables
    console.log('2️⃣  Dropping existing staging tables...')
    for (const table of stagingTables) {
      try {
        await conn.run(`DROP TABLE IF EXISTS ${table}`)
        console.log(`   ✅ Dropped: ${table}`)
      } catch (error) {
        console.error(`   ❌ Failed to drop ${table}:`, error)
      }
    }
    console.log()

    // Step 2: Load and execute new schema
    console.log('3️⃣  Creating updated staging tables...')
    const schemaPath = resolve(__dirname, '../lib/sql/schema/11_batched_import.sql')
    const schemaSQL = readFileSync(schemaPath, 'utf-8')

    // Remove comments and execute entire schema
    const cleanedSQL = schemaSQL
      .split('\n')
      .filter(line => !line.trim().startsWith('--'))
      .join('\n')

    try {
      // Execute the entire schema file at once (DuckDB can handle multiple statements)
      await conn.run(cleanedSQL)
      console.log(`   ✅ Created all staging tables and indexes\n`)
    } catch (error) {
      if (error instanceof Error) {
        console.error(`   ❌ Error executing schema: ${error.message}`)
        throw error
      }
    }

    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ SUCCESS! Staging tables migrated')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📋 Next steps:')
    console.log('   1. Run test script: npx tsx scripts/test-batched-import.ts')
    console.log()

  } catch (error) {
    console.error('\n❌ Migration failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${error.message}\n`)
      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

migrateStagingTables()
