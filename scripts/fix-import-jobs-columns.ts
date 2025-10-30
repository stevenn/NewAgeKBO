#!/usr/bin/env tsx

/**
 * Fix import_jobs table to have correct columns
 * Adds: records_inserted, records_updated, records_deleted, error_message
 * Removes: tables_processed, errors
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeStatement } from '../lib/motherduck'

async function main() {
  console.log('🔧 Fixing import_jobs table structure\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    console.log('Step 1: Backing up current table...')
    await executeStatement(conn, `
      CREATE TABLE import_jobs_backup AS
      SELECT * FROM import_jobs
    `)
    console.log('✓ Backup created\n')

    console.log('Step 2: Dropping current table...')
    await executeStatement(conn, `DROP TABLE import_jobs`)
    console.log('✓ Table dropped\n')

    console.log('Step 3: Creating table with correct structure...')
    await executeStatement(conn, `
      CREATE TABLE import_jobs (
        -- Primary key
        id VARCHAR PRIMARY KEY,

        -- Extract metadata
        extract_number INTEGER NOT NULL,
        extract_type VARCHAR NOT NULL CHECK (extract_type IN ('full', 'update')),
        snapshot_date DATE NOT NULL,
        extract_timestamp TIMESTAMP NOT NULL,

        -- Job status
        status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
        started_at TIMESTAMP,
        completed_at TIMESTAMP,
        error_message VARCHAR,

        -- Statistics
        records_processed BIGINT DEFAULT 0,
        records_inserted BIGINT DEFAULT 0,
        records_updated BIGINT DEFAULT 0,
        records_deleted BIGINT DEFAULT 0,

        -- Worker info
        worker_type VARCHAR NOT NULL CHECK (worker_type IN ('local', 'vercel', 'backfill', 'web_manual')),

        -- Unique constraint
        UNIQUE (extract_number, extract_type)
      )
    `)
    console.log('✓ Table created with correct structure\n')

    console.log('Step 4: Restoring data from backup...')
    await executeStatement(conn, `
      INSERT INTO import_jobs (
        id, extract_number, extract_type, snapshot_date, extract_timestamp,
        status, started_at, completed_at, error_message,
        records_processed, records_inserted, records_updated, records_deleted,
        worker_type
      )
      SELECT
        id, extract_number, extract_type, snapshot_date, extract_timestamp,
        status, started_at, completed_at,
        errors as error_message,  -- Map errors -> error_message
        records_processed,
        0 as records_inserted,    -- Default to 0
        0 as records_updated,     -- Default to 0
        0 as records_deleted,     -- Default to 0
        worker_type
      FROM import_jobs_backup
    `)
    console.log('✓ Data restored\n')

    console.log('Step 5: Dropping backup table...')
    await executeStatement(conn, `DROP TABLE import_jobs_backup`)
    console.log('✓ Backup dropped\n')

    console.log('✅ Migration completed successfully!')
    console.log('   Table now has correct columns matching schema and API expectations')

  } catch (error: any) {
    console.error('❌ Migration failed!')
    console.error(`   Error: ${error.message}`)
    console.error('\nIf backup exists, you can restore with:')
    console.error('   DROP TABLE import_jobs;')
    console.error('   ALTER TABLE import_jobs_backup RENAME TO import_jobs;')
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
