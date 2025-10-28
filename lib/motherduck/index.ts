/**
 * Motherduck connection utilities
 * Uses @duckdb/node-api for serverless compatibility
 *
 * Note: Uses @duckdb/node-api which works in Vercel serverless functions
 */

import { DuckDBInstance } from '@duckdb/node-api'
import type { DuckDBConnection } from '@duckdb/node-api'
import { MotherduckError } from '@/lib/errors'

/**
 * Motherduck connection configuration
 */
export interface MotherduckConfig {
  token: string
  database?: string
}

/**
 * Get Motherduck configuration from environment
 */
export function getMotherduckConfig(): MotherduckConfig {
  const token = process.env.MOTHERDUCK_TOKEN

  if (!token) {
    throw new MotherduckError(
      'MOTHERDUCK_TOKEN environment variable is not set. Please add it to .env.local'
    )
  }

  return {
    token,
    database: process.env.MOTHERDUCK_DATABASE || 'kbo',
  }
}

/**
 * Connect to Motherduck
 * Returns a Promise that resolves to a DuckDB connection
 *
 * For serverless environments (Vercel), we use a special connection sequence:
 * 1. Create in-memory DuckDB instance (no filesystem access needed)
 * 2. Set all directory configs to /tmp (required before Motherduck extension loads)
 * 3. Attach to Motherduck database
 * 4. Switch to using the Motherduck database
 *
 * This approach avoids "home directory not found" errors in serverless environments
 * where the filesystem is read-only except for /tmp.
 */
export async function connectMotherduck(
  config?: MotherduckConfig
): Promise<DuckDBConnection> {
  const mdConfig = config || getMotherduckConfig()

  try {
    // Create in-memory database first, set all directories, then attach Motherduck
    // This avoids the home directory error during Motherduck extension initialization
    const instance = await DuckDBInstance.create(':memory:')
    const connection = await instance.connect()

    // CRITICAL: Set all directory configurations BEFORE attaching to Motherduck
    // The Motherduck extension checks home_directory during motherduck_init()
    await connection.run("SET home_directory='/tmp'")
    await connection.run("SET extension_directory='/tmp/.duckdb/extensions'")
    await connection.run("SET temp_directory='/tmp'")

    // Set Motherduck token as environment variable (DuckDB will pick it up automatically)
    process.env.motherduck_token = mdConfig.token

    // Attach to Motherduck database (extension will be auto-installed to /tmp)
    await connection.run(`ATTACH 'md:${mdConfig.database}' AS md`)

    // Switch to using the Motherduck database
    await connection.run(`USE md`)

    return connection
  } catch (error) {
    throw new MotherduckError(
      `Failed to connect to Motherduck: ${error instanceof Error ? error.message : String(error)}`,
      error instanceof Error ? error : undefined
    )
  }
}

/**
 * Execute a SQL query
 */
export async function executeQuery<T = unknown>(
  connection: DuckDBConnection,
  sql: string
): Promise<T[]> {
  try {
    const result = await connection.run(sql)
    const chunks = await result.fetchAllChunks()

    // Get column names from the result
    const columnNames = result.columnNames()

    // Get rows as array of objects
    const rows: T[] = []
    for (const chunk of chunks) {
      // Get rows as arrays
      const rowArrays = chunk.getRows()
      // Convert to objects using column names
      for (const rowArray of rowArrays) {
        const rowObject: Record<string, unknown> = {}
        columnNames.forEach((colName, idx) => {
          rowObject[colName] = rowArray[idx]
        })
        rows.push(rowObject as T)
      }
    }

    return rows
  } catch (error) {
    throw new MotherduckError(
      `Query execution failed: ${error instanceof Error ? error.message : String(error)}`,
      error instanceof Error ? error : undefined
    )
  }
}

/**
 * Execute a SQL statement (no results expected)
 */
export async function executeStatement(
  connection: DuckDBConnection,
  sql: string
): Promise<void> {
  try {
    await connection.run(sql)
  } catch (error) {
    throw new MotherduckError(
      `Statement execution failed: ${error instanceof Error ? error.message : String(error)}`,
      error instanceof Error ? error : undefined
    )
  }
}

/**
 * Execute multiple SQL statements in a transaction
 */
export async function executeTransaction(
  connection: DuckDBConnection,
  statements: string[]
): Promise<void> {
  await executeStatement(connection, 'BEGIN TRANSACTION')

  try {
    for (const sql of statements) {
      await executeStatement(connection, sql)
    }
    await executeStatement(connection, 'COMMIT')
  } catch (error) {
    await executeStatement(connection, 'ROLLBACK')
    throw error
  }
}

/**
 * Close database connection
 */
export async function closeMotherduck(connection: DuckDBConnection): Promise<void> {
  try {
    connection.closeSync()
  } catch (error) {
    throw new MotherduckError(
      `Failed to close connection: ${error instanceof Error ? error.message : String(error)}`,
      error instanceof Error ? error : undefined
    )
  }
}

/**
 * Check if database exists
 */
export async function databaseExists(
  connection: DuckDBConnection,
  dbName: string
): Promise<boolean> {
  const result = await executeQuery<{ database_name: string }>(
    connection,
    'SHOW DATABASES'
  )
  return result.some((row) => row.database_name === dbName)
}

/**
 * Create database if it doesn't exist
 */
export async function ensureDatabase(
  connection: DuckDBConnection,
  dbName: string
): Promise<void> {
  const exists = await databaseExists(connection, dbName)

  if (!exists) {
    await executeStatement(connection, `CREATE DATABASE IF NOT EXISTS ${dbName}`)
    console.log(`Created database: ${dbName}`)
  }
}

/**
 * Check if table exists
 */
export async function tableExists(
  connection: DuckDBConnection,
  tableName: string
): Promise<boolean> {
  const result = await executeQuery<{ table_name: string }>(
    connection,
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
  )
  return result.some((row) => row.table_name === tableName)
}

/**
 * Get table row count
 */
export async function getTableCount(
  connection: DuckDBConnection,
  tableName: string
): Promise<number> {
  const result = await executeQuery<{ count: number }>(
    connection,
    `SELECT COUNT(*) as count FROM ${tableName}`
  )
  return result[0]?.count || 0
}

/**
 * Get database statistics
 */
export async function getDatabaseStats(
  connection: DuckDBConnection
): Promise<{ table_name: string; row_count: number }[]> {
  try {
    const tables = await executeQuery<{ table_name: string }>(
      connection,
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    )

    const stats = []
    for (const { table_name } of tables) {
      // Skip system tables that start with underscore
      if (table_name.startsWith('_')) {
        continue
      }
      try {
        const count = await getTableCount(connection, table_name)
        stats.push({ table_name, row_count: count })
      } catch {
        // Skip tables that can't be counted
        console.warn(`Warning: Could not count rows in table ${table_name}`)
      }
    }

    return stats
  } catch {
    // If we can't query information_schema, return empty array
    return []
  }
}
