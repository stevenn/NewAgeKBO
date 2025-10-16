#!/usr/bin/env tsx

/**
 * Test Motherduck connection
 * Verifies that your MOTHERDUCK_TOKEN is configured correctly
 * and you can connect to Motherduck
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import {
  connectMotherduck,
  closeMotherduck,
  executeQuery,
  ensureDatabase,
  getMotherduckConfig,
  getDatabaseStats,
} from '../lib/motherduck'
import { formatUserError } from '../lib/errors'

async function testConnection() {
  console.log('🔌 Testing Motherduck connection...\n')

  try {
    // Step 1: Check configuration
    console.log('1️⃣  Checking configuration...')
    const config = getMotherduckConfig()
    console.log(`   ✅ Token found (${config.token.substring(0, 20)}...)`)
    console.log(`   ✅ Database: ${config.database}\n`)

    // Step 2: Connect
    console.log('2️⃣  Connecting to Motherduck...')
    const db = await connectMotherduck()
    console.log('   ✅ Connected successfully!\n')

    // Step 3: Ensure database exists
    console.log('3️⃣  Ensuring database exists...')
    await ensureDatabase(db, config.database!)
    console.log(`   ✅ Database "${config.database}" is ready\n`)

    // Step 4: Use database
    console.log('4️⃣  Switching to database...')
    await executeQuery(db, `USE ${config.database}`)
    console.log(`   ✅ Using database "${config.database}"\n`)

    // Step 5: Run test query
    console.log('5️⃣  Running test query...')
    const result = await executeQuery<{ version: string }>(
      db,
      'SELECT version() as version'
    )
    console.log(`   ✅ DuckDB version: ${result[0].version}\n`)

    // Step 6: Check for existing tables
    console.log('6️⃣  Checking for existing tables...')
    const stats = await getDatabaseStats(db)

    if (stats.length === 0) {
      console.log('   ℹ️  No tables found (expected for new database)')
      console.log(
        '   💡 Run scripts/create-schema.ts to create tables\n'
      )
    } else {
      console.log(`   ✅ Found ${stats.length} tables:\n`)
      for (const { table_name, row_count } of stats) {
        console.log(`      • ${table_name}: ${row_count.toLocaleString()} rows`)
      }
      console.log()
    }

    // Step 7: Test write capability
    console.log('7️⃣  Testing write capability...')
    await executeQuery(db, 'CREATE TABLE IF NOT EXISTS _test (id INTEGER)')
    await executeQuery(db, "INSERT INTO _test VALUES (1), (2), (3)")
    const testResult = await executeQuery<{ count: number }>(
      db,
      'SELECT COUNT(*) as count FROM _test'
    )
    await executeQuery(db, 'DROP TABLE _test')
    console.log(`   ✅ Write test passed (inserted ${testResult[0].count} rows)\n`)

    // Close connection
    await closeMotherduck(db)

    // Success summary
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ SUCCESS! Motherduck connection is working')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📋 Next steps:')
    if (stats.length === 0) {
      console.log('   1. Run: npx tsx scripts/create-schema.ts')
      console.log('   2. Run: npx tsx scripts/initial-import.ts (when ready)')
    } else {
      console.log('   • Schema already exists')
      console.log('   • You can start importing data')
    }
    console.log()

  } catch (error) {
    console.error('\n❌ Connection test failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${formatUserError(error)}\n`)

      if (error.message.includes('MOTHERDUCK_TOKEN')) {
        console.error('💡 Quick fix:')
        console.error('   1. Create .env.local file in project root')
        console.error('   2. Add: MOTHERDUCK_TOKEN=your_token_here')
        console.error('   3. Get token from: https://motherduck.com/settings\n')
      } else if (error.message.includes('Invalid token')) {
        console.error('💡 Token issue:')
        console.error('   1. Check your token in .env.local')
        console.error('   2. Tokens start with "motherduck_"')
        console.error('   3. No extra spaces or newlines\n')
      } else {
        console.error('💡 Troubleshooting:')
        console.error('   1. Check internet connection')
        console.error('   2. Verify motherduck.com is accessible')
        console.error('   3. See docs/MOTHERDUCK_SETUP.md\n')
      }

      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

// Run the test
testConnection()
