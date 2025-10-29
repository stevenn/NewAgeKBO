#!/usr/bin/env tsx

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function testDatabaseSize() {
  const conn = await connectMotherduck()

  try {
    console.log('Testing database size queries...\n')

    // Try 1: pragma_database_size
    console.log('1. Testing pragma_database_size():')
    try {
      const result1 = await executeQuery(conn, 'SELECT * FROM pragma_database_size()')
      console.log('   ✅ Result:', JSON.stringify(result1, null, 2))
    } catch (e: any) {
      console.log('   ❌ Failed:', e.message)
    }

    // Try 2: storage_info table (Motherduck specific)
    console.log('\n2. Testing md_information_schema.storage_info:')
    try {
      const result2 = await executeQuery(conn, `
        SELECT database_name,
               CAST(active_bytes AS VARCHAR) as active_bytes,
               CAST(historical_bytes AS VARCHAR) as historical_bytes
        FROM md_information_schema.storage_info
        WHERE database_name = 'newagekbo'
      `)
      console.log('   ✅ Result:', JSON.stringify(result2, null, 2))
    } catch (e: any) {
      console.log('   ❌ Failed:', e.message)
    }

    // Try 3: database_size() function
    console.log('\n3. Testing database_size() function:')
    try {
      const result3 = await executeQuery(conn, "SELECT database_size('newagekbo') as size")
      console.log('   ✅ Result:', JSON.stringify(result3, null, 2))
    } catch (e: any) {
      console.log('   ❌ Failed:', e.message)
    }

    // Try 4: Show databases with size
    console.log('\n4. Testing SHOW DATABASES:')
    try {
      const result4 = await executeQuery(conn, 'SELECT * FROM duckdb_databases()')
      console.log('   ✅ Result:', JSON.stringify(result4, null, 2))
    } catch (e: any) {
      console.log('   ❌ Failed:', e.message)
    }

  } finally {
    await closeMotherduck(conn)
  }
}

testDatabaseSize()
