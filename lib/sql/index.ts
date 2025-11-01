/**
 * SQL schema management utilities
 * Loads and executes schema DDL files
 */

import { promises as fs } from 'fs'
import path from 'path'

/**
 * Schema file execution order
 */
const SCHEMA_FILES = [
  '01_enterprises.sql',
  '02_establishments.sql',
  '03_denominations.sql',
  '04_addresses.sql',
  '05_activities.sql',
  '06_nace_codes.sql',
  '07_contacts.sql',
  '08_branches.sql',
  '09_codes.sql',
  '10_import_jobs.sql',
  '11_batched_import.sql',
] as const

/**
 * Load a single SQL schema file
 */
export async function loadSchemaFile(filename: string): Promise<string> {
  const filePath = path.join(process.cwd(), 'lib', 'sql', 'schema', filename)
  return await fs.readFile(filePath, 'utf-8')
}

/**
 * Load all schema files in order
 * Returns array of SQL statements ready for execution
 */
export async function loadAllSchemas(): Promise<string[]> {
  const schemas: string[] = []

  for (const filename of SCHEMA_FILES) {
    const sql = await loadSchemaFile(filename)
    schemas.push(sql)
  }

  return schemas
}

/**
 * Load init script (master schema file)
 */
export async function loadInitScript(): Promise<string> {
  return await loadSchemaFile('00_init.sql')
}

/**
 * Get CREATE VIEW statements from init script
 */
export async function loadViewStatements(): Promise<string> {
  const initScript = await loadInitScript()

  // Extract CREATE VIEW statements
  const viewPattern = /CREATE OR REPLACE VIEW[\s\S]*?;/gi
  const views = initScript.match(viewPattern) || []

  return views.join('\n\n')
}

/**
 * Parse SQL file and split into individual statements
 * Useful for executing schemas one statement at a time
 */
export function splitSqlStatements(sql: string): string[] {
  // Remove comments
  const withoutComments = sql
    .split('\n')
    .filter(line => !line.trim().startsWith('--'))
    .join('\n')

  // Split by semicolons (statements)
  return withoutComments
    .split(';')
    .map(stmt => stmt.trim())
    .filter(stmt => stmt.length > 0)
}

/**
 * Get all table names from schema files
 */
export function extractTableNames(): string[] {
  return [
    'enterprises',
    'establishments',
    'denominations',
    'addresses',
    'activities',
    'nace_codes',
    'contacts',
    'branches',
    'codes',
    'import_jobs',
    'import_job_batches',
    'import_staging_enterprises',
    'import_staging_establishments',
    'import_staging_denominations',
    'import_staging_addresses',
    'import_staging_contacts',
    'import_staging_activities',
    'import_staging_branches',
  ]
}

/**
 * Get all view names
 */
export function extractViewNames(): string[] {
  return [
    'enterprises_current',
    'establishments_current',
    'denominations_current',
    'addresses_current',
    'activities_current',
    'contacts_current',
    'branches_current',
  ]
}
