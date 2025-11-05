#!/usr/bin/env tsx

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

async function list() {
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

    // Query distinct extract numbers
    const result = await conn.run(`
      SELECT DISTINCT _extract_number
      FROM enterprises
      ORDER BY _extract_number
    `)

    console.log('Extract numbers in database:')

    const chunks = await result.fetchAllChunks()
    const extractNumbers: number[] = []

    for (const chunk of chunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        extractNumbers.push(rowArray[0] as number)
      }
    }

    console.log(extractNumbers.join(', '))

  } finally {
    // Always close connection
    conn.closeSync()
  }
}

list().catch(console.error)
