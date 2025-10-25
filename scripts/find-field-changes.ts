#!/usr/bin/env tsx

/**
 * Find enterprises with changes in header fields (status, juridical form, etc.)
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

async function findFieldChanges() {
  const db = await connectMotherduck()
  const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

  try {
    await executeQuery(db, `USE ${dbName}`)

    console.log('🔍 Finding enterprises with header field changes\n')

    // Find enterprises with status changes
    console.log('📊 Status Changes:\n')
    const statusChanges = await executeQuery<{
      enterprise_number: string
      old_status: string
      new_status: string
      old_extract: number
      new_extract: number
    }>(
      db,
      `WITH status_changes AS (
        SELECT
          enterprise_number,
          status,
          _extract_number,
          LAG(status) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_status,
          LAG(_extract_number) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_extract
        FROM enterprises
      )
      SELECT
        enterprise_number,
        prev_status as old_status,
        status as new_status,
        prev_extract as old_extract,
        _extract_number as new_extract
      FROM status_changes
      WHERE prev_status IS NOT NULL
        AND prev_status != status
      LIMIT 5`
    )

    statusChanges.forEach(row => {
      console.log(`   ${row.enterprise_number}`)
      console.log(`      Extract ${row.old_extract}: ${row.old_status}`)
      console.log(`      Extract ${row.new_extract}: ${row.new_status}\n`)
    })

    // Find enterprises with juridical form changes
    console.log('📊 Juridical Form Changes:\n')
    const juridicalChanges = await executeQuery<{
      enterprise_number: string
      old_form: string
      new_form: string
      old_extract: number
      new_extract: number
    }>(
      db,
      `WITH jf_changes AS (
        SELECT
          enterprise_number,
          juridical_form,
          _extract_number,
          LAG(juridical_form) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_form,
          LAG(_extract_number) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_extract
        FROM enterprises
      )
      SELECT
        enterprise_number,
        prev_form as old_form,
        juridical_form as new_form,
        prev_extract as old_extract,
        _extract_number as new_extract
      FROM jf_changes
      WHERE prev_form IS NOT NULL
        AND prev_form != juridical_form
      LIMIT 5`
    )

    juridicalChanges.forEach(row => {
      console.log(`   ${row.enterprise_number}`)
      console.log(`      Extract ${row.old_extract}: ${row.old_form}`)
      console.log(`      Extract ${row.new_extract}: ${row.new_form}\n`)
    })

    // Find enterprises with type changes
    console.log('📊 Type of Enterprise Changes:\n')
    const typeChanges = await executeQuery<{
      enterprise_number: string
      old_type: string | null
      new_type: string | null
      old_extract: number
      new_extract: number
    }>(
      db,
      `WITH type_changes AS (
        SELECT
          enterprise_number,
          type_of_enterprise,
          _extract_number,
          LAG(type_of_enterprise) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_type,
          LAG(_extract_number) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_extract
        FROM enterprises
      )
      SELECT
        enterprise_number,
        prev_type as old_type,
        type_of_enterprise as new_type,
        prev_extract as old_extract,
        _extract_number as new_extract
      FROM type_changes
      WHERE prev_type IS NOT NULL
        AND (prev_type != type_of_enterprise OR (prev_type IS NULL AND type_of_enterprise IS NOT NULL) OR (prev_type IS NOT NULL AND type_of_enterprise IS NULL))
      LIMIT 5`
    )

    typeChanges.forEach(row => {
      console.log(`   ${row.enterprise_number}`)
      console.log(`      Extract ${row.old_extract}: ${row.old_type || 'NULL'}`)
      console.log(`      Extract ${row.new_extract}: ${row.new_type || 'NULL'}\n`)
    })

    // Find enterprises with juridical situation changes
    console.log('📊 Juridical Situation Changes:\n')
    const situationChanges = await executeQuery<{
      enterprise_number: string
      old_situation: string | null
      new_situation: string | null
      old_extract: number
      new_extract: number
    }>(
      db,
      `WITH js_changes AS (
        SELECT
          enterprise_number,
          juridical_situation,
          _extract_number,
          LAG(juridical_situation) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_situation,
          LAG(_extract_number) OVER (PARTITION BY enterprise_number ORDER BY _extract_number) as prev_extract
        FROM enterprises
      )
      SELECT
        enterprise_number,
        prev_situation as old_situation,
        juridical_situation as new_situation,
        prev_extract as old_extract,
        _extract_number as new_extract
      FROM js_changes
      WHERE prev_situation IS NOT NULL
        AND prev_situation != juridical_situation
      LIMIT 5`
    )

    situationChanges.forEach(row => {
      console.log(`   ${row.enterprise_number}`)
      console.log(`      Extract ${row.old_extract}: ${row.old_situation || 'NULL'}`)
      console.log(`      Extract ${row.new_extract}: ${row.new_situation || 'NULL'}\n`)
    })

  } catch (error) {
    console.error('❌ Query failed:', error)
    throw error
  } finally {
    await closeMotherduck(db)
  }
}

findFieldChanges().catch(console.error)
