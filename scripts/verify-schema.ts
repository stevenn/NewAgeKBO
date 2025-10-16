#!/usr/bin/env tsx

/**
 * Verify KBO database schema in Motherduck
 * Shows detailed information about tables and their structure
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import {
  connectMotherduck,
  closeMotherduck,
  executeQuery,
  getMotherduckConfig,
} from '../lib/motherduck'
import { formatUserError } from '../lib/errors'

interface TableInfo {
  table_name: string
  column_name: string
  data_type: string
  is_nullable: string
}

interface TableStats {
  table_name: string
  row_count: number
  estimated_size: string
}

async function verifySchema() {
  console.log('🔍 Verifying KBO database schema...\n')

  try {
    // Connect
    console.log('Connecting to Motherduck...')
    const mdConfig = getMotherduckConfig()
    const db = await connectMotherduck()
    console.log(`✅ Connected to database: ${mdConfig.database}\n`)

    // Use database
    await executeQuery(db, `USE ${mdConfig.database}`)

    // Get all tables
    const tables = await executeQuery<{ table_name: string }>(
      db,
      `SELECT table_name
       FROM information_schema.tables
       WHERE table_schema = 'main'
       AND table_name NOT LIKE 'duckdb_%'
       ORDER BY table_name`
    )

    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log(`📊 Found ${tables.length} tables in ${mdConfig.database}`)
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    // Show details for each table
    for (const { table_name } of tables) {
      console.log(`\n📋 Table: ${table_name}`)
      console.log('─'.repeat(50))

      // Get column information
      const columns = await executeQuery<TableInfo>(
        db,
        `SELECT
          column_name,
          data_type,
          is_nullable
         FROM information_schema.columns
         WHERE table_name = '${table_name}'
         ORDER BY ordinal_position`
      )

      console.log(`   Columns: ${columns.length}`)
      for (const col of columns) {
        const nullable = col.is_nullable === 'YES' ? 'NULL' : 'NOT NULL'
        console.log(`      • ${col.column_name.padEnd(25)} ${col.data_type.padEnd(15)} ${nullable}`)
      }

      // Get row count
      try {
        const countResult = await executeQuery<{ count: number }>(
          db,
          `SELECT COUNT(*) as count FROM ${table_name}`
        )
        const rowCount = countResult[0].count
        console.log(`   Rows: ${rowCount.toLocaleString()}`)
      } catch (error) {
        console.log('   Rows: (unable to count)')
      }
    }

    console.log('\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✅ Schema Validation Complete')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    // Check for temporal tracking columns
    console.log('🔍 Checking temporal tracking fields...\n')

    const temporalTables = [
      'enterprises',
      'establishments',
      'denominations',
      'addresses',
      'activities',
      'contacts',
      'branches',
    ]

    let allHaveTemporal = true
    for (const tableName of temporalTables) {
      const columns = await executeQuery<{ column_name: string }>(
        db,
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_name = '${tableName}'
         AND column_name IN ('_snapshot_date', '_extract_number', '_is_current')`
      )

      const hasAll = columns.length === 3
      if (hasAll) {
        console.log(`   ✅ ${tableName} has temporal tracking`)
      } else {
        console.log(`   ❌ ${tableName} missing temporal fields`)
        allHaveTemporal = false
      }
    }

    if (allHaveTemporal) {
      console.log('\n   ✅ All data tables have temporal tracking\n')
    } else {
      console.log('\n   ⚠️  Some tables are missing temporal tracking\n')
    }

    // Close connection
    await closeMotherduck(db)

    console.log('📋 Next steps:')
    console.log('   1. Review schema in Motherduck web UI')
    console.log('   2. Run: npx tsx scripts/initial-import.ts (when ready)')
    console.log()

  } catch (error) {
    console.error('\n❌ Schema verification failed!\n')

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

// Run the verification
verifySchema()
