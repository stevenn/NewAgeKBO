#!/usr/bin/env tsx

/**
 * Restore import_jobs from backup
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../lib/motherduck'

async function main() {
  console.log('🔧 Restoring import_jobs from backup\n')

  let conn: any = null

  try {
    conn = await connectMotherduck()

    // Check what's in the backup
    console.log('Checking backup table structure...')
    const backupSchema = await executeQuery(conn, `DESCRIBE import_jobs_backup`)
    console.log('Backup columns:', backupSchema.map(r => r.column_name).join(', '))

    const backupData = await executeQuery(conn, `SELECT COUNT(*) as count FROM import_jobs_backup`)
    console.log(`Backup contains ${backupData[0].count} rows\n`)

    // Drop current empty table
    console.log('Dropping empty import_jobs table...')
    await executeStatement(conn, `DROP TABLE IF EXISTS import_jobs`)
    console.log('✓ Dropped\n')

    // Rename backup to import_jobs
    console.log('Renaming backup to import_jobs...')
    await executeStatement(conn, `ALTER TABLE import_jobs_backup RENAME TO import_jobs`)
    console.log('✓ Restored\n')

    // Verify
    const currentSchema = await executeQuery(conn, `DESCRIBE import_jobs`)
    console.log('Current table structure:')
    console.table(currentSchema)

    console.log('\n✅ Restoration completed successfully!')

  } catch (error: any) {
    console.error('❌ Restoration failed!')
    console.error(`   Error: ${error.message}`)
    process.exit(1)
  } finally {
    if (conn) {
      await closeMotherduck(conn)
    }
  }
}

main()
