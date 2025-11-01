#!/usr/bin/env tsx

/**
 * Export current denominations to CSV in original KBO format
 * Excludes denominations for deleted/inactive enterprises
 *
 * Usage:
 *   npx tsx scripts/export-current-denominations.ts [output-file.csv]
 *
 * Output format matches: denomination.csv
 *   - EntityNumber, Language, TypeOfDenomination, Denomination
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { connectMotherduck, closeMotherduck, executeQuery, getMotherduckConfig } from '../lib/motherduck'
import { formatUserError } from '../lib/errors'
import * as fs from 'fs'

interface DenominationRow {
  entity_number: string
  language: string
  denomination_type: string
  denomination: string
}

async function exportDenominations() {
  const outputPath = process.argv[2] || './current-denominations.csv'

  console.log('📤 Exporting Current Denominations\n')
  console.log(`📁 Output: ${outputPath}\n`)

  try {
    // Connect to Motherduck
    console.log('1️⃣  Connecting to Motherduck...')
    const mdConfig = getMotherduckConfig()
    const db = await connectMotherduck()
    // Note: connectMotherduck() already does "USE md" internally
    console.log(`   ✅ Connected to database: ${mdConfig.database}\n`)

    // Query current denominations, excluding those for inactive/deleted enterprises
    console.log('2️⃣  Querying current denominations...')

    const sql = `
      SELECT
        d.entity_number,
        d.language,
        d.denomination_type,
        d.denomination
      FROM denominations d
      -- Only current denominations
      WHERE d._is_current = true
      -- For enterprises: only include if enterprise is active
      AND (
        -- Establishments: always include if current
        d.entity_type = 'establishment'
        OR
        -- Enterprises: only if currently active (status = 'AC')
        (
          d.entity_type = 'enterprise'
          AND EXISTS (
            SELECT 1
            FROM enterprises e
            WHERE e.enterprise_number = d.entity_number
              AND e._is_current = true
              AND e.status = 'AC'
          )
        )
      )
      -- Match original CSV column order
      ORDER BY d.entity_number, d.denomination_type, d.language
    `

    const rows = await executeQuery<DenominationRow>(db, sql)

    console.log(`   ✅ Found ${rows.length.toLocaleString()} current denominations\n`)

    // Build CSV content
    console.log('3️⃣  Writing CSV file...')

    // Header matching original format
    const header = '"EntityNumber","Language","TypeOfDenomination","Denomination"\n'

    // Data rows - escape quotes in denomination text
    const dataRows = rows.map(row => {
      const escapedDenomination = row.denomination.replace(/"/g, '""')
      return `"${row.entity_number}","${row.language}","${row.denomination_type}","${escapedDenomination}"`
    }).join('\n')

    const csvContent = header + dataRows

    // Write to file
    fs.writeFileSync(outputPath, csvContent, 'utf-8')

    console.log(`   ✅ Written ${rows.length.toLocaleString()} rows\n`)

    // Get statistics
    console.log('4️⃣  Statistics...')

    const stats = await executeQuery<{
      entity_type: string
      language: string
      denomination_type: string
      count: number
    }>(db, `
      SELECT
        d.entity_type,
        d.language,
        d.denomination_type,
        COUNT(*) as count
      FROM denominations d
      WHERE d._is_current = true
      AND (
        d.entity_type = 'establishment'
        OR
        (
          d.entity_type = 'enterprise'
          AND EXISTS (
            SELECT 1
            FROM enterprises e
            WHERE e.enterprise_number = d.entity_number
              AND e._is_current = true
              AND e.status = 'AC'
          )
        )
      )
      GROUP BY d.entity_type, d.language, d.denomination_type
      ORDER BY d.entity_type, d.denomination_type, d.language
    `)

    console.log('\n   Breakdown by type and language:')
    console.log('   ' + '─'.repeat(60))

    let currentType = ''
    for (const stat of stats) {
      if (stat.entity_type !== currentType) {
        currentType = stat.entity_type
        console.log(`\n   ${stat.entity_type.toUpperCase()}:`)
      }

      const langName = {
        '0': 'Unknown',
        '1': 'French',
        '2': 'Dutch',
        '3': 'German',
        '4': 'English'
      }[stat.language] || stat.language

      const typeName = {
        '001': 'Legal name',
        '002': 'Abbreviation',
        '003': 'Commercial name',
        '004': 'Branch name'
      }[stat.denomination_type] || stat.denomination_type

      console.log(`      ${typeName.padEnd(20)} | ${langName.padEnd(10)} | ${stat.count.toLocaleString().padStart(10)} rows`)
    }
    console.log('   ' + '─'.repeat(60))

    // Close connection
    await closeMotherduck(db)

    // Summary
    console.log('\n' + '='.repeat(60))
    console.log('✅ Export Complete!')
    console.log('='.repeat(60))
    console.log(`📁 File: ${outputPath}`)
    console.log(`📊 Total rows: ${rows.length.toLocaleString()}`)
    console.log(`💾 File size: ${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(2)} MB`)
    console.log('='.repeat(60) + '\n')

  } catch (error) {
    console.error('\n❌ Export failed!\n')

    if (error instanceof Error) {
      console.error(`Error: ${formatUserError(error)}\n`)

      if (process.env.NODE_ENV === 'development') {
        console.error('Stack trace:')
        console.error(error.stack)
      }
    }

    process.exit(1)
  }
}

exportDenominations()
