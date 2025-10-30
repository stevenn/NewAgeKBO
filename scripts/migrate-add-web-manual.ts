#!/usr/bin/env tsx

/**
 * Migration: Add 'web_manual' to worker_type CHECK constraint
 *
 * This script updates the import_jobs table to allow 'web_manual' as a valid worker_type.
 * The constraint needs to be dropped and recreated because DuckDB doesn't support
 * ALTER CONSTRAINT directly.
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeStatement } from '../lib/motherduck'

async function main() {
  console.log('🔧 Migrating import_jobs table to add web_manual worker type\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    console.log('📋 Current constraint: worker_type IN (local, vercel, backfill)')
    console.log('🎯 Target constraint: worker_type IN (local, vercel, backfill, web_manual)\n')

    // DuckDB doesn't support ALTER TABLE DROP CONSTRAINT directly
    // We need to recreate the table or use a workaround

    console.log('Step 1: Creating temporary table without constraint...')
    await executeStatement(conn, `
      CREATE TEMP TABLE import_jobs_temp AS
      SELECT * FROM import_jobs
    `)
    console.log('✓ Temporary table created\n')

    console.log('Step 2: Dropping original table...')
    await executeStatement(conn, `DROP TABLE import_jobs`)
    console.log('✓ Original table dropped\n')

    console.log('Step 3: Recreating table with updated constraint...')
    await executeStatement(conn, `
      CREATE TABLE import_jobs (
        id VARCHAR PRIMARY KEY,
        extract_number INTEGER NOT NULL,
        extract_type VARCHAR NOT NULL CHECK (extract_type IN ('update', 'full')),
        snapshot_date DATE NOT NULL,
        extract_timestamp TIMESTAMP NOT NULL,
        status VARCHAR NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
        started_at TIMESTAMP NOT NULL,
        completed_at TIMESTAMP,
        records_processed INTEGER DEFAULT 0,
        tables_processed TEXT,
        errors TEXT,
        worker_type VARCHAR NOT NULL CHECK (worker_type IN ('local', 'vercel', 'backfill', 'web_manual')),
        UNIQUE (extract_number, extract_type)
      )
    `)
    console.log('✓ Table recreated with new constraint\n')

    console.log('Step 4: Restoring data from temporary table...')
    await executeStatement(conn, `
      INSERT INTO import_jobs
      SELECT * FROM import_jobs_temp
    `)
    console.log('✓ Data restored\n')

    console.log('Step 5: Dropping temporary table...')
    await executeStatement(conn, `DROP TABLE import_jobs_temp`)
    console.log('✓ Temporary table dropped\n')

    console.log('✅ Migration completed successfully!')
    console.log('   worker_type now accepts: local, vercel, backfill, web_manual')

  } catch (error: any) {
    console.error('❌ Migration failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
