#!/usr/bin/env tsx

/**
 * Analyze KBO update ZIP to understand its contents
 * Purpose: Determine if Extract 0140 Update is redundant with the full dump
 */

import StreamZip from 'node-stream-zip'
import { parse } from 'csv-parse/sync'
import * as path from 'path'

interface Metadata {
  SnapshotDate: string
  ExtractTimestamp: string
  ExtractType: string
  ExtractNumber: string
  Version: string
}

interface MetaRecord {
  Variable?: string
  Value?: string
}

interface UpdateStats {
  metadata: Metadata
  deletes: { [table: string]: number }
  inserts: { [table: string]: number }
  totalDeletes: number
  totalInserts: number
  sampleRecords: { [table: string]: any[] }
}

async function analyzeUpdate(zipPath: string): Promise<UpdateStats> {
  console.log(`\n📦 Analyzing update: ${path.basename(zipPath)}\n`)

  const zip = new StreamZip.async({ file: zipPath })
  const entries = await zip.entries()

  const stats: UpdateStats = {
    metadata: {} as Metadata,
    deletes: {},
    inserts: {},
    totalDeletes: 0,
    totalInserts: 0,
    sampleRecords: {}
  }

  // Step 1: Read meta.csv
  console.log('📋 Reading meta.csv...')
  try {
    const metaContent = await zip.entryData('meta.csv')
    const metaRecords = parse(metaContent.toString(), {
      columns: true,
      skip_empty_lines: true
    }) as MetaRecord[]

    stats.metadata = {
      SnapshotDate: metaRecords[0]?.Value || metaRecords.find((r) => r.Variable === 'SnapshotDate')?.Value || '',
      ExtractTimestamp: metaRecords[1]?.Value || metaRecords.find((r) => r.Variable === 'ExtractTimestamp')?.Value || '',
      ExtractType: metaRecords[2]?.Value || metaRecords.find((r) => r.Variable === 'ExtractType')?.Value || '',
      ExtractNumber: metaRecords[3]?.Value || metaRecords.find((r) => r.Variable === 'ExtractNumber')?.Value || '',
      Version: metaRecords[4]?.Value || metaRecords.find((r) => r.Variable === 'Version')?.Value || ''
    }

    console.log(`   ✓ Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`   ✓ Extract Timestamp: ${stats.metadata.ExtractTimestamp}`)
    console.log(`   ✓ Extract Type: ${stats.metadata.ExtractType}`)
    console.log(`   ✓ Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`   ✓ Version: ${stats.metadata.Version}`)
  } catch (error) {
    console.error('   ✗ Failed to read meta.csv:', error)
  }

  // Step 2: Analyze delete files
  console.log('\n🗑️  Analyzing delete files...')
  for (const [name, entry] of Object.entries(entries)) {
    if (name.endsWith('_delete.csv')) {
      const tableName = name.replace('_delete.csv', '')
      const content = await zip.entryData(name)
      const records = parse(content.toString(), {
        columns: true,
        skip_empty_lines: true
      })

      stats.deletes[tableName] = records.length
      stats.totalDeletes += records.length

      console.log(`   • ${tableName}: ${records.length} deletes`)

      // Sample first 3 records
      if (records.length > 0) {
        stats.sampleRecords[`${tableName}_delete`] = records.slice(0, 3)
      }
    }
  }

  // Step 3: Analyze insert files
  console.log('\n➕ Analyzing insert files...')
  for (const [name, entry] of Object.entries(entries)) {
    if (name.endsWith('_insert.csv')) {
      const tableName = name.replace('_insert.csv', '')
      const content = await zip.entryData(name)
      const records = parse(content.toString(), {
        columns: true,
        skip_empty_lines: true
      })

      stats.inserts[tableName] = records.length
      stats.totalInserts += records.length

      console.log(`   • ${tableName}: ${records.length} inserts`)

      // Sample first 3 records
      if (records.length > 0) {
        stats.sampleRecords[`${tableName}_insert`] = records.slice(0, 3)
      }
    }
  }

  await zip.close()

  return stats
}

async function main() {
  const args = process.argv.slice(2)

  if (args.length === 0) {
    console.error('Usage: npx tsx scripts/analyze-update.ts <path-to-update.zip>')
    console.error('\nExample:')
    console.error('  npx tsx scripts/analyze-update.ts sampledata/KboOpenData_0140_2025_10_05_Update.zip')
    process.exit(1)
  }

  const zipPath = args[0]

  try {
    const stats = await analyzeUpdate(zipPath)

    // Summary
    console.log('\n' + '='.repeat(60))
    console.log('📊 SUMMARY')
    console.log('='.repeat(60))
    console.log(`Extract Number: ${stats.metadata.ExtractNumber}`)
    console.log(`Snapshot Date: ${stats.metadata.SnapshotDate}`)
    console.log(`Extract Timestamp: ${stats.metadata.ExtractTimestamp}`)
    console.log(`Extract Type: ${stats.metadata.ExtractType}`)
    console.log(`\nTotal Deletes: ${stats.totalDeletes}`)
    console.log(`Total Inserts: ${stats.totalInserts}`)
    console.log(`Total Changes: ${stats.totalDeletes + stats.totalInserts}`)

    // Size analysis
    console.log('\n📏 Change Distribution:')
    const tables = Array.from(new Set([
      ...Object.keys(stats.deletes),
      ...Object.keys(stats.inserts)
    ])).sort()

    for (const table of tables) {
      const deletes = stats.deletes[table] || 0
      const inserts = stats.inserts[table] || 0
      const total = deletes + inserts
      console.log(`   ${table.padEnd(20)} | -${String(deletes).padStart(6)} | +${String(inserts).padStart(6)} | Δ${String(total).padStart(6)}`)
    }

    // Sample records
    if (Object.keys(stats.sampleRecords).length > 0) {
      console.log('\n📝 Sample Records (first 3 of each type):')
      for (const [key, records] of Object.entries(stats.sampleRecords)) {
        if (records.length > 0) {
          console.log(`\n   ${key}:`)
          console.log('   ', JSON.stringify(records[0], null, 2).split('\n').join('\n    '))
          if (records.length > 1) {
            console.log(`   ... and ${records.length - 1} more`)
          }
        }
      }
    }

    console.log('\n' + '='.repeat(60))
    console.log('✅ Analysis complete')
    console.log('='.repeat(60) + '\n')

  } catch (error) {
    console.error('\n❌ Analysis failed:', error)
    process.exit(1)
  }
}

main()
