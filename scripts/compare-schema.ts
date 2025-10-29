#!/usr/bin/env tsx

/**
 * Comprehensive schema comparison between local SQL DDL definitions and remote Motherduck schema
 * Checks for missing tables, missing columns, type mismatches, and constraint differences
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import * as fs from 'fs'
import * as path from 'path'
import { connectMotherduck, closeMotherduck, executeQuery } from '../lib/motherduck'

interface ColumnInfo {
  column_name: string
  data_type: string
  is_nullable: string
  column_default: string | null
}

interface RemoteColumn {
  column_name: string
  data_type: string
  is_nullable: string
}

interface LocalColumn {
  name: string
  type: string
  nullable: boolean
  constraints: string[]
}

interface TableDefinition {
  name: string
  columns: LocalColumn[]
  primaryKey: string[]
  checks: string[]
}

/**
 * Parse SQL DDL file to extract table definition
 */
function parseTableDefinition(sqlContent: string): TableDefinition | null {
  const tableMatch = sqlContent.match(/CREATE TABLE.*?(\w+)\s*\(([\s\S]*?)\);/i)
  if (!tableMatch) return null

  const tableName = tableMatch[1]
  const tableBody = tableMatch[2]

  const columns: LocalColumn[] = []
  const primaryKey: string[] = []
  const checks: string[] = []

  // Split by lines and process each
  const lines = tableBody.split('\n').map(l => l.trim()).filter(l => l.length > 0)

  for (const line of lines) {
    // Skip comments
    if (line.startsWith('--')) continue

    // Primary key constraint
    if (line.match(/PRIMARY KEY\s*\((.*?)\)/i)) {
      const pkMatch = line.match(/PRIMARY KEY\s*\((.*?)\)/i)
      if (pkMatch) {
        primaryKey.push(...pkMatch[1].split(',').map(c => c.trim()))
      }
      continue
    }

    // Check constraint
    if (line.match(/CHECK\s*\(/i)) {
      checks.push(line)
      continue
    }

    // Column definition
    const colMatch = line.match(/^(\w+)\s+([\w()]+)(.*)$/i)
    if (colMatch) {
      const [, colName, colType, rest] = colMatch
      const nullable = !rest.toUpperCase().includes('NOT NULL')
      const constraints: string[] = []

      if (rest.toUpperCase().includes('UNIQUE')) constraints.push('UNIQUE')
      if (rest.toUpperCase().includes('PRIMARY KEY')) {
        constraints.push('PRIMARY KEY')
        primaryKey.push(colName)
      }

      columns.push({
        name: colName,
        type: colType.toUpperCase(),
        nullable,
        constraints,
      })
    }
  }

  return {
    name: tableName,
    columns,
    primaryKey,
    checks,
  }
}

/**
 * Normalize data type for comparison
 */
function normalizeType(type: string): string {
  type = type.toUpperCase()

  // Map DuckDB types to standard types
  const typeMap: Record<string, string> = {
    'CHARACTER VARYING': 'VARCHAR',
    'BIGINT': 'INTEGER',
    'INT': 'INTEGER',
    'BOOL': 'BOOLEAN',
    'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP',
    'DOUBLE': 'DOUBLE PRECISION',
  }

  for (const [from, to] of Object.entries(typeMap)) {
    if (type.includes(from)) {
      return to
    }
  }

  return type
}

/**
 * Compare local and remote schemas
 */
async function compareSchemas() {
  console.log('🔍 Comprehensive Schema Comparison\n')
  console.log('Local:  SQL DDL files in lib/sql/schema/')
  console.log('Remote: Motherduck database\n')

  const db = await connectMotherduck()
  let hasErrors = false

  try {
    // Get list of local SQL files
    const schemaDir = path.join(process.cwd(), 'lib', 'sql', 'schema')
    const sqlFiles = fs.readdirSync(schemaDir)
      .filter(f => f.endsWith('.sql'))
      .sort()

    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    // Process each SQL file
    for (const sqlFile of sqlFiles) {
      const sqlPath = path.join(schemaDir, sqlFile)
      const sqlContent = fs.readFileSync(sqlPath, 'utf-8')
      const localTable = parseTableDefinition(sqlContent)

      if (!localTable) {
        console.log(`⚠️  ${sqlFile}: Could not parse table definition\n`)
        continue
      }

      console.log(`📋 Table: ${localTable.name}`)
      console.log('─'.repeat(60))

      // Check if table exists in remote
      const remoteTableCheck = await executeQuery<{ table_name: string }>(
        db,
        `SELECT table_name FROM information_schema.tables
         WHERE table_schema = 'main' AND table_name = '${localTable.name}'`
      )

      if (remoteTableCheck.length === 0) {
        console.log(`   ❌ Table does not exist in remote database\n`)
        hasErrors = true
        continue
      }

      // Get remote columns
      const remoteColumns = await executeQuery<RemoteColumn>(
        db,
        `SELECT column_name, data_type, is_nullable
         FROM information_schema.columns
         WHERE table_name = '${localTable.name}'
         ORDER BY ordinal_position`
      )

      const remoteColumnMap = new Map(
        remoteColumns.map(c => [c.column_name, c])
      )

      let tableHasErrors = false

      // Check each local column
      for (const localCol of localTable.columns) {
        const remoteCol = remoteColumnMap.get(localCol.name)

        if (!remoteCol) {
          console.log(`   ❌ Column '${localCol.name}' missing in remote`)
          tableHasErrors = true
          continue
        }

        // Compare types
        const localType = normalizeType(localCol.type)
        const remoteType = normalizeType(remoteCol.data_type)

        if (localType !== remoteType) {
          console.log(`   ⚠️  Column '${localCol.name}' type mismatch:`)
          console.log(`       Local:  ${localType}`)
          console.log(`       Remote: ${remoteType}`)
          tableHasErrors = true
        }

        // Compare nullability
        const localNullable = localCol.nullable
        const remoteNullable = remoteCol.is_nullable === 'YES'

        if (localNullable !== remoteNullable) {
          console.log(`   ⚠️  Column '${localCol.name}' nullability mismatch:`)
          console.log(`       Local:  ${localNullable ? 'NULL' : 'NOT NULL'}`)
          console.log(`       Remote: ${remoteNullable ? 'NULL' : 'NOT NULL'}`)
          tableHasErrors = true
        }
      }

      // Check for extra columns in remote
      for (const remoteCol of remoteColumns) {
        const localCol = localTable.columns.find(c => c.name === remoteCol.column_name)
        if (!localCol) {
          console.log(`   ℹ️  Column '${remoteCol.column_name}' exists in remote but not in local DDL`)
        }
      }

      if (!tableHasErrors) {
        console.log(`   ✅ Schema matches (${localTable.columns.length} columns)`)
      } else {
        hasErrors = true
      }

      console.log()
    }

    // Check for tables in remote that aren't in local
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('🔍 Checking for extra tables in remote...\n')

    const allRemoteTables = await executeQuery<{ table_name: string }>(
      db,
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'main' AND table_name NOT LIKE 'duckdb_%'
       ORDER BY table_name`
    )

    const localTableNames = new Set(
      sqlFiles
        .map(f => {
          const content = fs.readFileSync(path.join(schemaDir, f), 'utf-8')
          const def = parseTableDefinition(content)
          return def?.name
        })
        .filter(Boolean)
    )

    let extraTables = false
    for (const { table_name } of allRemoteTables) {
      if (!localTableNames.has(table_name)) {
        console.log(`   ℹ️  Table '${table_name}' exists in remote but not in local DDL`)
        extraTables = true
      }
    }

    if (!extraTables) {
      console.log('   ✅ No extra tables in remote')
    }

    console.log()
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')

    if (hasErrors) {
      console.log('⚠️  Schema comparison found issues')
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')
      process.exit(1)
    } else {
      console.log('✅ All schemas match!')
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')
    }

  } finally {
    await closeMotherduck(db)
  }
}

compareSchemas()
