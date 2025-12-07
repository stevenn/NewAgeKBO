/**
 * Import Peppol registration data from parquet file into MotherDuck
 *
 * Usage:
 *   npx tsx scripts/import-peppol-registrations.ts <parquet-path>
 *
 * Example:
 *   npx tsx scripts/import-peppol-registrations.ts /path/to/peppolgrowth/data/smp-data-2025-11-30.parquet
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

// Load environment variables from .env.local
config({ path: resolve(__dirname, '../.env.local') })

async function importPeppolRegistrations(parquetPath: string) {
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

    console.log(`\nImporting Peppol registrations from: ${parquetPath}\n`)

    // Drop existing table first
    console.log('Dropping existing peppol_registrations table if exists...')
    await conn.run(`DROP TABLE IF EXISTS peppol_registrations`)
    console.log('Done.\n')

    console.log('Creating peppol_registrations table from parquet...')
    await conn.run(`
      CREATE TABLE peppol_registrations AS
      SELECT * FROM read_parquet('${parquetPath}')
    `)
    console.log('Table created successfully.\n')

    // Get row count
    const countResult = await conn.run(`SELECT COUNT(*) as count FROM peppol_registrations`)
    const countChunks = await countResult.fetchAllChunks()
    for (const chunk of countChunks) {
      const rows = chunk.getRows()
      for (const row of rows) {
        console.log(`Total rows imported: ${row[0]}`)
      }
    }

    // Show sample data
    console.log('\nSample data (first 3 rows):')
    console.log('---')

    const sampleResult = await conn.run(`
      SELECT company_id, company_name, registration_status, smp_hostname
      FROM peppol_registrations
      LIMIT 3
    `)

    const sampleChunks = await sampleResult.fetchAllChunks()
    const columnNames = sampleResult.columnNames()

    for (const chunk of sampleChunks) {
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

    // Show schema
    console.log('\nTable schema:')
    const schemaResult = await conn.run(`DESCRIBE peppol_registrations`)
    const schemaChunks = await schemaResult.fetchAllChunks()
    const schemaColumns = schemaResult.columnNames()

    for (const chunk of schemaChunks) {
      const rowArrays = chunk.getRows()
      for (const rowArray of rowArrays) {
        console.log(`  ${rowArray[0]}: ${rowArray[1]}`)
      }
    }

  } finally {
    conn.closeSync()
  }
}

// Get parquet path from command line (required)
const parquetPath = process.argv[2]

if (!parquetPath) {
  console.error('Usage: npx tsx scripts/import-peppol-registrations.ts <parquet-path>')
  console.error('Example: npx tsx scripts/import-peppol-registrations.ts /path/to/smp-data-2025-11-30.parquet')
  process.exit(1)
}

importPeppolRegistrations(resolve(parquetPath)).catch(console.error)
