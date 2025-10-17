#!/usr/bin/env tsx

/**
 * Apply daily KBO update from ZIP file
 * Purpose: Process incremental updates using delete-then-insert pattern
 *
 * Strategy:
 * - Process ZIP files directly (no extraction)
 * - DELETE operations: Mark records as _is_current = false (preserve history)
 * - INSERT operations: Add new records with _is_current = true
 * - Update _extract_number and _snapshot_date from meta.csv
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import StreamZip from 'node-stream-zip'
import { parse } from 'csv-parse/sync'
import * as path from 'path'
import { connectMotherduck, closeMotherduck, executeQuery, executeStatement } from '../lib/motherduck'
import {
  csvColumnToDbColumn,
  csvTableToDbTable,
  computeEntityType,
  convertKboDateFormat,
  isKboDateFormat
} from '../lib/utils/column-mapping'

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

interface UpdateStats {
  metadata: Metadata
  tablesProcessed: string[]
  deletesApplied: number
  insertsApplied: number
  errors: string[]
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

// Removed - now using shared library from lib/utils/column-mapping

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
    }) as Record<string, string>[]

    if (records.length === 0) {
      console.log(`   ℹ️  ${dbTableName}: No deletes`)
      return 0
    }

    // Get primary key column name from first record (CSV uses PascalCase, DB uses snake_case)
    const csvPkColumn = Object.keys(records[0])[0]
    const dbPkColumn = csvColumnToDbColumn(csvPkColumn)
    const entityNumbers = records.map(r => `'${r[csvPkColumn]}'`).join(',')

    // Mark records as historical (don't actually delete)
    const sql = `
      UPDATE ${dbTableName}
      SET _is_current = false
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
    }) as Record<string, string>[]

    if (records.length === 0) {
      console.log(`   ℹ️  ${dbTableName}: No inserts`)
      return 0
    }

    // Deduplicate records based on all fields (KBO sometimes sends duplicates)
    const uniqueRecords: Record<string, string>[] = []
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

    // For enterprises, we need to fetch existing primary_name values
    let enterpriseNames: Map<string, any> = new Map()
    if (dbTableName === 'enterprises') {
      const enterpriseNumbers = uniqueRecords.map(r => `'${r['EnterpriseNumber']}'`).join(',')
      const existingRecords = await executeQuery<any>(db, `
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

    // Build INSERT statement with temporal columns
    // CSV uses PascalCase, DB uses snake_case
    const csvColumns = Object.keys(uniqueRecords[0])
    const dbColumns = csvColumns.map(col => csvColumnToDbColumn(col))

    // Check if we need to add computed columns
    const needsEntityType = ['activities', 'addresses', 'contacts', 'denominations'].includes(dbTableName)
    const needsComputedId = ['activities', 'addresses', 'contacts', 'denominations'].includes(dbTableName)

    // Build column list
    let allColumns: string[]
    if (needsComputedId) {
      // For tables with computed IDs, we need: id, _snapshot_date, _extract_number, then all CSV columns, entity_type, _is_current
      allColumns = ['id', '_snapshot_date', '_extract_number', ...dbColumns, 'entity_type', '_is_current']
    } else if (needsEntityType) {
      allColumns = [...dbColumns, 'entity_type', '_snapshot_date', '_extract_number', '_is_current']
    } else {
      allColumns = [...dbColumns, '_snapshot_date', '_extract_number', '_is_current']
    }

    const values = uniqueRecords.map(record => {
      // Compute ID for tables that need it
      let computedId: string | null = null
      if (needsComputedId) {
        const entityNumber = record['EntityNumber'] || record[csvColumns[0]]

        if (dbTableName === 'activities') {
          // id: entity_number_group_version_code_classification
          computedId = `${entityNumber}_${record['ActivityGroup']}_${record['NaceVersion']}_${record['NaceCode']}_${record['Classification']}`
        } else if (dbTableName === 'addresses') {
          // id: entity_number_type_of_address
          computedId = `${entityNumber}_${record['TypeOfAddress']}`
        } else if (dbTableName === 'contacts') {
          // id: entity_number_entity_contact_contact_type_value
          computedId = `${entityNumber}_${record['EntityContact']}_${record['ContactType']}_${record['Value']}`
        } else if (dbTableName === 'denominations') {
          // id: entity_number_type_language_row_number (for now, use simple concat without row number)
          computedId = `${entityNumber}_${record['TypeOfDenomination']}_${record['Language']}_1`
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
        // Order: id, _snapshot_date, _extract_number, ...all CSV columns..., entity_type, _is_current
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
 * Process daily update ZIP file
 */
async function processDailyUpdate(zipPath: string): Promise<UpdateStats> {
  console.log(`\n📦 Processing daily update: ${path.basename(zipPath)}\n`)

  const stats: UpdateStats = {
    metadata: {} as Metadata,
    tablesProcessed: [],
    deletesApplied: 0,
    insertsApplied: 0,
    errors: []
  }

  const zip = new StreamZip.async({ file: zipPath })
  const db = await connectMotherduck()

  try {
    // Connect to database from environment
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

    // Step 2: Get list of tables from delete/insert files
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

    // Step 3: Process each table (delete then insert)
    for (const csvTableName of tableList) {
      const dbTableName = csvTableToDbTable(csvTableName)
      try {
        console.log(`🔄 Processing ${dbTableName}...`)

        // Delete first
        const deletes = await applyDeletes(db, zip, csvTableName, dbTableName, stats.metadata)
        stats.deletesApplied += deletes

        // Then insert
        const inserts = await applyInserts(db, zip, csvTableName, dbTableName, stats.metadata)
        stats.insertsApplied += inserts

        stats.tablesProcessed.push(dbTableName)
      } catch (error: any) {
        const errorMsg = `${dbTableName}: ${error.message}`
        stats.errors.push(errorMsg)
        console.error(`   ❌ ${errorMsg}`)
      }
    }

  } finally {
    await zip.close()
    await closeMotherduck(db)
  }

  return stats
}

/**
 * Main execution
 */
async function main() {
  const args = process.argv.slice(2)

  if (args.length === 0) {
    console.error('Usage: npx tsx scripts/apply-daily-update.ts <path-to-update.zip>')
    console.error('\nExample:')
    console.error('  npx tsx scripts/apply-daily-update.ts sampledata/KboOpenData_0141_2025_10_06_Update.zip')
    process.exit(1)
  }

  const zipPath = args[0]

  try {
    const stats = await processDailyUpdate(zipPath)

    // Summary
    console.log('\n' + '='.repeat(60))
    console.log('📊 DAILY UPDATE SUMMARY')
    console.log('='.repeat(60))
    console.log(`Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`\nTables Processed: ${stats.tablesProcessed.length}`)
    console.log(`Records Marked Historical: ${stats.deletesApplied}`)
    console.log(`Records Inserted: ${stats.insertsApplied}`)
    console.log(`Total Changes: ${stats.deletesApplied + stats.insertsApplied}`)

    if (stats.errors.length > 0) {
      console.log(`\n⚠️  Errors: ${stats.errors.length}`)
      stats.errors.forEach(err => console.log(`   • ${err}`))
    }

    console.log('\n' + '='.repeat(60))
    console.log(stats.errors.length > 0 ? '⚠️  Completed with errors' : '✅ Update applied successfully')
    console.log('='.repeat(60) + '\n')

    process.exit(stats.errors.length > 0 ? 1 : 0)

  } catch (error) {
    console.error('\n❌ Update failed:', error)
    process.exit(1)
  }
}

main()
