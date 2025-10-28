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
 * Create Motherduck connection string
 */
export function createConnectionString(
  config: MotherduckConfig,
  includeDatabase: boolean = true
): string {
  const { database } = config
  if (includeDatabase && database) {
    return `md:${database}`
  }
  return `md:`
}

/**
 * Connect to Motherduck
 * Returns a Promise that resolves to a DuckDB connection
 *
 * For serverless environments (Vercel), we use @duckdb/node-api which
 * has better compatibility with serverless platforms.
 */
export async function connectMotherduck(
  config?: MotherduckConfig
): Promise<DuckDBConnection> {
  const mdConfig = config || getMotherduckConfig()

  try {
    console.log(`Connecting to Motherduck with database: ${mdConfig.database}`)

    // Create connection string
    const connectionString = createConnectionString(mdConfig, true)

    // Create DuckDB instance with Motherduck connection
    // CRITICAL: Set home_directory in config for serverless environments
    const instance = await DuckDBInstance.create(connectionString, {
      motherduck_token: mdConfig.token,
      home_directory: '/tmp',
    })

    // Create connection
    const connection = await instance.connect()

    console.log(`Successfully connected to Motherduck database: ${mdConfig.database}`)

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
