/**
 * Daily Update Import Library
 * Core logic for processing KBO daily update ZIP files
 * Can be called from both CLI scripts and API routes
 */

import StreamZip from 'node-stream-zip'
import { parse } from 'csv-parse/sync'
import { createHash, randomUUID } from 'crypto'
import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../motherduck'
import {
  csvColumnToDbColumn,
  csvTableToDbTable,
  computeEntityType,
  convertKboDateFormat,
  isKboDateFormat
} from '../utils/column-mapping'
import { WorkerType } from '../types/import-job'
import { DailyUpdateStats } from '../types/kbo-portal'
import { tmpdir } from 'os'
import { writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'

interface MetaRecord {
  Variable?: string
  Value?: string
}

interface Metadata {
  SnapshotDate: string
  ExtractTimestamp: string
  ExtractType: string
  ExtractNumber: string
  Version: string
}

/**
 * Generate a short hash for a string (8 characters)
 * Used to create unique IDs for denominations
 */
function shortHash(text: string): string {
  return createHash('sha256').update(text).digest('hex').substring(0, 8)
}

/**
 * Parse metadata from meta.csv in ZIP
 */
async function parseMetadata(zip: StreamZip.StreamZipAsync): Promise<Metadata> {
  const metaContent = await zip.entryData('meta.csv')
  const metaRecords = parse(metaContent.toString(), {
    columns: true,
    skip_empty_lines: true
  }) as MetaRecord[]

  return {
    SnapshotDate: metaRecords.find((r) => r.Variable === 'SnapshotDate')?.Value || metaRecords[0]?.Value || '',
    ExtractTimestamp: metaRecords.find((r) => r.Variable === 'ExtractTimestamp')?.Value || metaRecords[1]?.Value || '',
    ExtractType: metaRecords.find((r) => r.Variable === 'ExtractType')?.Value || metaRecords[2]?.Value || '',
    ExtractNumber: metaRecords.find((r) => r.Variable === 'ExtractNumber')?.Value || metaRecords[3]?.Value || '',
    Version: metaRecords.find((r) => r.Variable === 'Version')?.Value || metaRecords[4]?.Value || ''
  }
}

/**
 * Apply delete operations (mark as historical, don't actually delete)
 */
async function applyDeletes(
  db: any,
  zip: StreamZip.StreamZipAsync,
  csvTableName: string,
  dbTableName: string,
  metadata: Metadata
): Promise<number> {
  const fileName = `${csvTableName}_delete.csv`

  try {
    const content = await zip.entryData(fileName)
    const records = parse(content.toString(), {
      columns: true,
      skip_empty_lines: true
    })

    if (records.length === 0) {
      console.log(`   ℹ️  ${dbTableName}: No deletes`)
      return 0
    }

    const csvPkColumn = Object.keys(records[0])[0]
    const dbPkColumn = csvColumnToDbColumn(csvPkColumn)
    const entityNumbers = records.map((r: any) => `'${r[csvPkColumn]}'`).join(',')
    const extractNumber = parseInt(metadata.ExtractNumber)

    const sql = `
      UPDATE ${dbTableName}
      SET _is_current = false,
          _deleted_at_extract = ${extractNumber}
      WHERE ${dbPkColumn} IN (${entityNumbers})
        AND _is_current = true
    `

    await executeStatement(db, sql)
    console.log(`   ✓ ${dbTableName}: Marked ${records.length} records as historical`)
    return records.length
  } catch (error: any) {
    if (error.message?.includes('Entry not found')) {
      console.log(`   ℹ️  ${dbTableName}: No delete file`)
      return 0
    }
    throw error
  }
}

/**
 * Apply insert operations
 */
async function applyInserts(
  db: any,
  zip: StreamZip.StreamZipAsync,
  csvTableName: string,
  dbTableName: string,
  metadata: Metadata
): Promise<number> {
  const fileName = `${csvTableName}_insert.csv`

  try {
    const content = await zip.entryData(fileName)
    const records = parse(content.toString(), {
      columns: true,
      skip_empty_lines: true
    })

    if (records.length === 0) {
      console.log(`   ℹ️  ${dbTableName}: No inserts`)
      return 0
    }

    // Deduplicate records
    const uniqueRecords: any[] = []
    const seen = new Set<string>()

    for (const record of records) {
      const key = JSON.stringify(record)
      if (!seen.has(key)) {
        seen.add(key)
        uniqueRecords.push(record)
      }
    }

    if (uniqueRecords.length < records.length) {
      console.log(`   ⚠️  ${dbTableName}: Removed ${records.length - uniqueRecords.length} duplicate records from insert file`)
    }

    // For enterprises, fetch existing names for re-insertion
    const enterpriseNames = new Map<string, any>()
    if (dbTableName === 'enterprises') {
      const enterpriseNumbers = uniqueRecords.map((r: any) => `'${r['EnterpriseNumber']}'`).join(',')
      const existingRecords = await executeQuery(db, `
        SELECT enterprise_number, primary_name, primary_name_language,
               primary_name_nl, primary_name_fr, primary_name_de
        FROM enterprises
        WHERE enterprise_number IN (${enterpriseNumbers})
          AND _is_current = false
        ORDER BY _snapshot_date DESC, _extract_number DESC
      `)

      for (const rec of existingRecords) {
        if (!enterpriseNames.has(rec.enterprise_number)) {
          enterpriseNames.set(rec.enterprise_number, rec)
        }
      }
    }

    const snapshotDate = convertKboDateFormat(metadata.SnapshotDate)
    const extractNumber = parseInt(metadata.ExtractNumber)
    const csvColumns = Object.keys(uniqueRecords[0])
    const dbColumns = csvColumns.map(col => csvColumnToDbColumn(col))

    const needsEntityType = ['activities', 'addresses', 'contacts', 'denominations'].includes(dbTableName)
    const needsComputedId = ['activities', 'addresses', 'contacts', 'denominations'].includes(dbTableName)

    let allColumns: string[]
    if (needsComputedId) {
      allColumns = ['id', '_snapshot_date', '_extract_number', ...dbColumns, 'entity_type', '_is_current']
    } else if (needsEntityType) {
      allColumns = [...dbColumns, 'entity_type', '_snapshot_date', '_extract_number', '_is_current']
    } else {
      allColumns = [...dbColumns, '_snapshot_date', '_extract_number', '_is_current']
    }

    const values = uniqueRecords.map((record: any) => {
      // Compute ID for tables that need it
      let computedId: string | null = null
      if (needsComputedId) {
        const entityNumber = record['EntityNumber'] || record[csvColumns[0]]

        if (dbTableName === 'activities') {
          computedId = `${entityNumber}_${record['ActivityGroup']}_${record['NaceVersion']}_${record['NaceCode']}_${record['Classification']}`
        } else if (dbTableName === 'addresses') {
          computedId = `${entityNumber}_${record['TypeOfAddress']}`
        } else if (dbTableName === 'contacts') {
          computedId = `${entityNumber}_${record['EntityContact']}_${record['ContactType']}_${record['Value']}`
        } else if (dbTableName === 'denominations') {
          const denominationHash = shortHash(record['Denomination'] || '')
          computedId = `${entityNumber}_${record['TypeOfDenomination']}_${record['Language']}_${denominationHash}`
        }
      }

      const recordValues = csvColumns.map(col => {
        const val = record[col]
        if (val === '' || val === null) {
          return 'NULL'
        }

        // Check if this looks like a date (DD-MM-YYYY format)
        if (col.toLowerCase().includes('date') && isKboDateFormat(val)) {
          const converted = convertKboDateFormat(val)
          return `'${converted}'`
        }

        // Escape single quotes
        return `'${val.replace(/'/g, "''")}'`
      })

      // Build values string
      if (needsComputedId) {
        const entityNumber = record['EntityNumber'] || record[csvColumns[0]]
        const entityType = computeEntityType(entityNumber)
        return `('${computedId}', '${snapshotDate}', ${extractNumber}, ${recordValues.join(',')}, '${entityType}', true)`
      } else if (needsEntityType) {
        const entityNumber = record['EntityNumber'] || record[csvColumns[0]]
        const entityType = computeEntityType(entityNumber)
        return `(${recordValues.join(',')}, '${entityType}', '${snapshotDate}', ${extractNumber}, true)`
      }

      // For enterprises, add primary_name fields from existing record
      if (dbTableName === 'enterprises') {
        const enterpriseNumber = record['EnterpriseNumber']
        const existing = enterpriseNames.get(enterpriseNumber)

        if (existing) {
          const primaryName = existing.primary_name ? `'${existing.primary_name.replace(/'/g, "''")}'` : `'${enterpriseNumber}'`
          const primaryNameLang = existing.primary_name_language ? `'${existing.primary_name_language}'` : 'NULL'
          const primaryNameNl = existing.primary_name_nl ? `'${existing.primary_name_nl.replace(/'/g, "''")}'` : 'NULL'
          const primaryNameFr = existing.primary_name_fr ? `'${existing.primary_name_fr.replace(/'/g, "''")}'` : 'NULL'
          const primaryNameDe = existing.primary_name_de ? `'${existing.primary_name_de.replace(/'/g, "''")}'` : 'NULL'

          return `(${recordValues.join(',')}, ${primaryName}, ${primaryNameLang}, ${primaryNameNl}, ${primaryNameFr}, ${primaryNameDe}, '${snapshotDate}', ${extractNumber}, true)`
        } else {
          // New enterprise - use enterprise number as primary name
          return `(${recordValues.join(',')}, '${enterpriseNumber}', NULL, NULL, NULL, NULL, '${snapshotDate}', ${extractNumber}, true)`
        }
      }

      return `(${recordValues.join(',')}, '${snapshotDate}', ${extractNumber}, true)`
    }).join(',\n      ')

    // Update column list for enterprises to include primary_name fields
    if (dbTableName === 'enterprises') {
      allColumns = [...dbColumns, 'primary_name', 'primary_name_language', 'primary_name_nl', 'primary_name_fr', 'primary_name_de', '_snapshot_date', '_extract_number', '_is_current']
    }

    const sql = `
      INSERT INTO ${dbTableName} (${allColumns.join(', ')})
      VALUES
      ${values}
    `

    await executeStatement(db, sql)
    console.log(`   ✓ ${dbTableName}: Inserted ${uniqueRecords.length} records`)
    return uniqueRecords.length
  } catch (error: any) {
    if (error.message?.includes('Entry not found')) {
      console.log(`   ℹ️  ${dbTableName}: No insert file`)
      return 0
    }
    throw error
  }
}

/**
 * Resolve primary names for enterprises
 * Updates enterprises where primary_name is the enterprise number (temporary placeholder)
 * to use actual names from the denominations table
 */
async function resolvePrimaryNames(db: any, metadata: Metadata): Promise<number> {
  const snapshotDate = convertKboDateFormat(metadata.SnapshotDate)
  const extractNumber = parseInt(metadata.ExtractNumber)

  const sql = `
    UPDATE enterprises e
    SET
      primary_name = COALESCE(
        d.denomination_nl,
        d.denomination_fr,
        d.denomination_unknown,
        d.denomination_de,
        d.denomination_en,
        e.enterprise_number
      ),
      primary_name_language = COALESCE(
        CASE WHEN d.denomination_nl IS NOT NULL THEN '2' END,
        CASE WHEN d.denomination_fr IS NOT NULL THEN '1' END,
        CASE WHEN d.denomination_unknown IS NOT NULL THEN '0' END,
        CASE WHEN d.denomination_de IS NOT NULL THEN '3' END,
        CASE WHEN d.denomination_en IS NOT NULL THEN '4' END,
        NULL
      ),
      primary_name_nl = d.denomination_nl,
      primary_name_fr = d.denomination_fr,
      primary_name_de = d.denomination_de
    FROM (
      SELECT
        entity_number,
        MAX(CASE WHEN language = '2' AND denomination_type = '001' THEN denomination END) as denomination_nl,
        MAX(CASE WHEN language = '1' AND denomination_type = '001' THEN denomination END) as denomination_fr,
        MAX(CASE WHEN language = '0' AND denomination_type = '001' THEN denomination END) as denomination_unknown,
        MAX(CASE WHEN language = '3' AND denomination_type = '001' THEN denomination END) as denomination_de,
        MAX(CASE WHEN language = '4' AND denomination_type = '001' THEN denomination END) as denomination_en
      FROM denominations
      WHERE _is_current = true
        AND entity_type = 'enterprise'
        AND denomination_type = '001'
      GROUP BY entity_number
    ) d
    WHERE e.enterprise_number = d.entity_number
      AND e._snapshot_date = '${snapshotDate}'
      AND e._extract_number = ${extractNumber}
      AND e._is_current = true
      AND e.primary_name = e.enterprise_number
  `

  await executeStatement(db, sql)

  // Count how many were updated
  const result = await executeQuery(db, `
    SELECT COUNT(*) as count
    FROM enterprises
    WHERE _snapshot_date = '${snapshotDate}'
      AND _extract_number = ${extractNumber}
      AND _is_current = true
      AND primary_name != enterprise_number
  `)

  return result[0]?.count || 0
}

/**
 * Process a daily update from a Buffer containing the ZIP file data
 * This is the core function that can be called from both CLI and API routes
 *
 * @param zipBuffer - Buffer containing the ZIP file data
 * @param workerType - Type of worker processing this update
 * @returns Statistics about the import
 */
export async function processDailyUpdate(
  zipBuffer: Buffer,
  workerType: WorkerType = 'local'
): Promise<DailyUpdateStats> {
  // Write buffer to temporary file (node-stream-zip requires a file path)
  const tempFilePath = join(tmpdir(), `kbo-update-${randomUUID()}.zip`)
  writeFileSync(tempFilePath, zipBuffer)

  const stats: DailyUpdateStats = {
    metadata: {
      SnapshotDate: '',
      ExtractTimestamp: '',
      ExtractType: '',
      ExtractNumber: '',
      Version: ''
    },
    tablesProcessed: [],
    deletesApplied: 0,
    insertsApplied: 0,
    errors: []
  }

  let zip: StreamZip.StreamZipAsync | null = null
  let db: any = null

  try {
    zip = new StreamZip.async({ file: tempFilePath })
    db = await connectMotherduck()

    const dbName = process.env.MOTHERDUCK_DATABASE
    if (!dbName) {
      throw new Error('MOTHERDUCK_DATABASE not set in environment')
    }

    await executeQuery(db, `USE ${dbName}`)

    // Step 1: Parse metadata
    console.log('📋 Reading metadata...')
    stats.metadata = await parseMetadata(zip)
    console.log(`   ✓ Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`   ✓ Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`   ✓ Extract Type: ${stats.metadata.ExtractType}`)

    if (stats.metadata.ExtractType !== 'update') {
      throw new Error(`Expected 'update' extract type, got '${stats.metadata.ExtractType}'`)
    }

    // Step 2: Create import job record
    console.log('\n📝 Creating import job record...')
    const jobId = randomUUID()
    const jobStartTime = new Date().toISOString()

    await executeQuery(db, `INSERT INTO import_jobs (
        id, extract_number, extract_type, snapshot_date, extract_timestamp,
        status, started_at, worker_type
      ) VALUES (
        '${jobId}',
        ${stats.metadata.ExtractNumber},
        'update',
        '${stats.metadata.SnapshotDate}',
        '${stats.metadata.ExtractTimestamp}',
        'running',
        '${jobStartTime}',
        '${workerType}'
      )`)

    console.log(`   ✓ Job ID: ${jobId}`)

    // Step 3: Get list of tables to process
    const entries = await zip.entries()
    const tables = new Set<string>()

    for (const name of Object.keys(entries)) {
      if (name.endsWith('_delete.csv')) {
        tables.add(name.replace('_delete.csv', ''))
      } else if (name.endsWith('_insert.csv')) {
        tables.add(name.replace('_insert.csv', ''))
      }
    }

    const tableList = Array.from(tables).sort()
    console.log(`\n📊 Tables to process: ${tableList.join(', ')}\n`)

    // Step 4: Process each table
    for (const csvTableName of tableList) {
      const dbTableName = csvTableToDbTable(csvTableName)

      try {
        console.log(`🔄 Processing ${dbTableName}...`)
        const deletes = await applyDeletes(db, zip, csvTableName, dbTableName, stats.metadata)
        stats.deletesApplied += deletes

        const inserts = await applyInserts(db, zip, csvTableName, dbTableName, stats.metadata)
        stats.insertsApplied += inserts

        stats.tablesProcessed.push(dbTableName)
      } catch (error: any) {
        const errorMsg = `${dbTableName}: ${error.message}`
        stats.errors.push(errorMsg)
        console.error(`   ❌ ${errorMsg}`)
      }
    }

    // Step 5: Resolve primary names if enterprises or denominations were processed
    if (stats.tablesProcessed.includes('enterprises') || stats.tablesProcessed.includes('denominations')) {
      console.log('\n🔄 Resolving primary names for new enterprises...')
      try {
        const resolved = await resolvePrimaryNames(db, stats.metadata)
        if (resolved > 0) {
          console.log(`   ✓ Resolved primary names for ${resolved} enterprises`)
        } else {
          console.log(`   ℹ️  No new enterprises requiring name resolution`)
        }
      } catch (error: any) {
        const errorMsg = `Primary name resolution: ${error.message}`
        stats.errors.push(errorMsg)
        console.error(`   ❌ ${errorMsg}`)
      }
    }

    // Step 6: Update job status
    if (jobId) {
      const totalRecordsProcessed = stats.deletesApplied + stats.insertsApplied
      const jobStatus = stats.errors.length > 0 ? 'failed' : 'completed'
      const errorMessage = stats.errors.length > 0 ? stats.errors.join('; ') : null

      await executeQuery(db, `UPDATE import_jobs SET
          status = '${jobStatus}',
          completed_at = '${new Date().toISOString()}',
          records_processed = ${totalRecordsProcessed},
          records_inserted = ${stats.insertsApplied},
          records_updated = 0,
          records_deleted = ${stats.deletesApplied}
          ${errorMessage ? `, error_message = '${errorMessage.replace(/'/g, "''")}'` : ''}
        WHERE id = '${jobId}'`)

      console.log(`\n   ✓ Job ${jobStatus}: ${jobId}`)
    }
  } catch (error: any) {
    // If there's a job ID in scope, mark it as failed
    if (db) {
      try {
        const errorMessage = error.message || 'Unknown error'
        // This is a best-effort update, we don't have jobId in this scope
        // The calling code should handle this case
        throw error
      } catch (updateError) {
        console.error('Failed to update job status:', updateError)
      }
    }
    throw error
  } finally {
    if (zip) {
      await zip.close()
    }
    if (db) {
      await closeMotherduck(db)
    }
    // Clean up temporary file
    try {
      unlinkSync(tempFilePath)
    } catch (e) {
      // Ignore cleanup errors
    }
  }

  return stats
}
