/**
 * Template script for investigative database queries
 *
 * Usage:
 * 1. Copy this file to a new name (e.g., scripts/investigate-xyz.ts)
 * 2. Update the query logic in the main function
 * 3. Run with: npx tsx scripts/investigate-xyz.ts [args]
 * 4. Delete the script when investigation is complete
 *
 * This template follows the working pattern for Motherduck connections
 * using @duckdb/node-api in serverless environments.
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

async function investigateData(param: string) {
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
    await conn.run(`ATTACH 'md:${database}' AS md`)
    await conn.run(`USE md`)

    // ========================================
    // YOUR QUERY LOGIC HERE
    // ========================================

    console.log(`\nInvestigating parameter: ${param}\n`)

    // Example: Query enterprises table
    const result = await conn.run(`
      SELECT *
      FROM enterprises
      WHERE enterprise_number = '${param}'
      LIMIT 5
    `)

    // Process results using the correct pattern
    const chunks = await result.fetchAllChunks()
    const columnNames = result.columnNames()

    for (const chunk of chunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        const row: Record<string, unknown> = {}
        columnNames.forEach((col, idx) => {
          row[col] = rowArray[idx]
        })
        console.log(JSON.stringify(row, null, 2))
        console.log('---')
      }
    }

    // Example: Query codes table
    const codesResult = await conn.run(`
      SELECT category, code, language, description
      FROM codes
      WHERE category = 'Status'
      ORDER BY code, language
    `)

    console.log('\nCode descriptions:\n')
    const codesChunks = await codesResult.fetchAllChunks()
    const codesColumnNames = codesResult.columnNames()

    for (const chunk of codesChunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        const code: Record<string, unknown> = {}
        codesColumnNames.forEach((col, idx) => {
          code[col] = rowArray[idx]
        })
        console.log(`${code.code} (${code.language}): ${code.description}`)
      }
    }

    // ========================================
    // END QUERY LOGIC
    // ========================================

  } finally {
    // Always close connection (use closeSync, not close)
    conn.closeSync()
  }
}

// Get parameter from command line or use default
const param = process.argv[2] || 'default-value'
investigateData(param).catch(console.error)
