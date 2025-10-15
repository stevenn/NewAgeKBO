#!/usr/bin/env tsx

/**
 * Create KBO database schema in Motherduck
 * Executes all DDL statements from lib/sql/schema/
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import {
  connectMotherduck,
  closeMotherduck,
  executeStatement,
  getMotherduckConfig,
  tableExists,
  ensureDatabase,
} from '../lib/motherduck'
import { loadAllSchemas, extractTableNames } from '../lib/sql'
import { formatUserError } from '../lib/errors'

async function createSchema() {
  console.log('🏗️  Creating KBO database schema...\n')

  try {
    // Step 1: Connect
    console.log('1️⃣  Connecting to Motherduck...')
    const mdConfig = getMotherduckConfig()
    const db = await connectMotherduck()
    console.log('   ✅ Connected successfully!\n')

    // Step 2: Ensure database exists
    console.log('2️⃣  Ensuring database exists...')
    await ensureDatabase(db, mdConfig.database!)
    console.log(`   ✅ Database "${mdConfig.database}" is ready\n`)

    // Step 3: Use database
    console.log('3️⃣  Using database...')
    await executeStatement(db, `USE ${mdConfig.database}`)
    console.log(`   ✅ Using database "${mdConfig.database}"\n`)

    // Step 4: Load schema files
    console.log('4️⃣  Loading schema files...')
    const schemas = await loadAllSchemas()
    console.log(`   ✅ Loaded ${schemas.length} schema files\n`)

    // Step 5: Check existing tables
    console.log('5️⃣  Checking for existing tables...')
    const tableNames = extractTableNames()
    const existingTables: string[] = []

    for (const tableName of tableNames) {
      const exists = await tableExists(db, tableName)
      if (exists) {
        existingTables.push(tableName)
      }
    }

    if (existingTables.length > 0) {
      console.log(`   ⚠️  Found ${existingTables.length} existing tables:`)
      for (const name of existingTables) {
        console.log(`      • ${name}`)
      }
      console.log(
        '\n   💡 Tip: Tables will be created with IF NOT EXISTS, so existing tables are safe\n'
      )
    } else {
      console.log('   ℹ️  No existing tables found\n')
    }

    // Step 6: Execute schema statements
    console.log('6️⃣  Creating tables...')
    let tablesCreated = 0

    for (let i = 0; i < schemas.length; i++) {
      const schemaSQL = schemas[i]
      const tableName = tableNames[i]

      try {
        // Execute the CREATE TABLE statement
        await executeStatement(db, schemaSQL)

        if (!existingTables.includes(tableName)) {
          console.log(`   ✅ Created table: ${tableName}`)
          tablesCreated++
        } else {
          console.log(`   ⏭️  Skipped (already exists): ${tableName}`)
        }
      } catch (error) {
        console.error(`   ❌ Failed to create table: ${tableName}`)
        if (error instanceof Error) {
          console.error(`      Error: ${error.message}`)
        }
        // Continue with other tables
      }
    }

    console.log(`\n   ✅ Created ${tablesCreated} new tables\n`)

    // Step 7: Verify all tables exist
    console.log('7️⃣  Verifying schema...')
    const missingTables: string[] = []

    for (const tableName of tableNames) {
      const exists = await tableExists(db, tableName)
      if (!exists) {
        missingTables.push(tableName)
      }
    }

    if (missingTables.length > 0) {
      console.log(`   ⚠️  Warning: ${missingTables.length} tables are missing:`)
      for (const name of missingTables) {
        console.log(`      • ${name}`)
      }
      console.log()
    } else {
      console.log(`   ✅ All ${tableNames.length} tables exist\n`)
    }

    // Close connection
    await closeMotherduck(db)

    // Success summary
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ SUCCESS! Database schema is ready')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📊 Schema Summary:')
    console.log(`   • Database: ${mdConfig.database}`)
    console.log(`   • Tables: ${tableNames.length}`)
    console.log(`   • New tables created: ${tablesCreated}`)
    console.log(`   • Previously existing: ${existingTables.length}`)
    console.log()

    console.log('📋 Next steps:')
    console.log('   1. Verify schema in Motherduck web UI')
    console.log('   2. Run: npx tsx scripts/initial-import.ts (when ready)')
    console.log()
  } catch (error) {
    console.error('\n❌ Schema creation failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${formatUserError(error)}\n`)

      console.error('💡 Troubleshooting:')
      console.error('   1. Check Motherduck connection')
      console.error('   2. Verify database exists')
      console.error('   3. Check SQL syntax in schema files')
      console.error('   4. See docs/MOTHERDUCK_SETUP.md\n')

      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

// Run the schema creation
createSchema()
