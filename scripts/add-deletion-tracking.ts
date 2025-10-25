#!/usr/bin/env tsx

/**
 * Add deletion tracking to temporal tables
 *
 * Purpose: Track when records were deleted to enable accurate point-in-time queries
 *
 * Adds _deleted_at_extract column to track which extract number marked a record as deleted.
 * This allows us to correctly reconstruct historical snapshots from incremental updates.
 *
 * Example:
 * - Extract 140 (full): Record exists, _is_current = true, _deleted_at_extract = NULL
 * - Extract 150 (incremental): Record deleted, _is_current = false, _deleted_at_extract = 150
 * - Query for Extract 145: Should include record (deleted after 145)
 * - Query for Extract 155: Should exclude record (deleted before 155)
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeStatement, executeQuery } from '../lib/motherduck'

const TEMPORAL_TABLES = [
  'enterprises',
  'establishments',
  'denominations',
  'addresses',
  'contacts',
  'activities',
  'branches'
]

async function addDeletionTracking() {
  console.log('🔧 Adding deletion tracking to temporal tables\n')

  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
  await executeQuery(db, `USE ${dbName}`)

  for (const tableName of TEMPORAL_TABLES) {
    console.log(`📊 Processing ${tableName}...`)

    try {
      // Check if column already exists
      const columns = await executeQuery<{ column_name: string }>(
        db,
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_name = '${tableName}'
           AND column_name = '_deleted_at_extract'`
      )

      if (columns.length > 0) {
        console.log(`   ℹ️  Column already exists, skipping\n`)
        continue
      }

      // Add the column
      await executeStatement(
        db,
        `ALTER TABLE ${tableName} ADD COLUMN _deleted_at_extract INTEGER DEFAULT NULL`
      )

      console.log(`   ✅ Added _deleted_at_extract column\n`)
    } catch (error) {
      console.error(`   ❌ Failed to process ${tableName}:`, error)
      throw error
    }
  }

  await closeMotherduck(db)

  console.log('✅ Migration complete!\n')
  console.log('📝 Notes:')
  console.log('   - Existing deleted records have _deleted_at_extract = NULL')
  console.log('   - Future deletions will track the extract number')
  console.log('   - Next full dump will reset all deletion tracking\n')
}

addDeletionTracking().catch(console.error)
