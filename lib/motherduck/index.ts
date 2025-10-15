/**
 * Motherduck connection utilities
 * Wraps DuckDB Node.js client with Motherduck-specific configuration
 */

import * as duckdb from 'duckdb'
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
  const { token, database } = config
  if (includeDatabase && database) {
    return `md:${database}?motherduck_token=${token}`
  }
  return `md:?motherduck_token=${token}`
}

/**
 * Connect to Motherduck
 * Returns a Promise that resolves to a DuckDB database connection
 * Note: Connects without attaching to a specific database initially
 */
export async function connectMotherduck(
  config?: MotherduckConfig
): Promise<duckdb.Database> {
  const mdConfig = config || getMotherduckConfig()
  // Connect without specifying database - we'll create/attach it later
  const connectionString = createConnectionString(mdConfig, false)

  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(connectionString, (err) => {
      if (err) {
        reject(
          new MotherduckError(
            `Failed to connect to Motherduck: ${err.message}`,
            err
          )
        )
      } else {
        resolve(db)
      }
    })
  })
}

/**
 * Execute a SQL query
 */
export async function executeQuery<T = unknown>(
  db: duckdb.Database,
  sql: string
): Promise<T[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, (err, rows) => {
      if (err) {
        reject(
          new MotherduckError(`Query execution failed: ${err.message}`, err)
        )
      } else {
        resolve(rows as T[])
      }
    })
  })
}

/**
 * Execute a SQL statement (no results expected)
 */
export async function executeStatement(
  db: duckdb.Database,
  sql: string
): Promise<void> {
  return new Promise((resolve, reject) => {
    db.run(sql, (err) => {
      if (err) {
        reject(
          new MotherduckError(
            `Statement execution failed: ${err.message}`,
            err
          )
        )
      } else {
        resolve()
      }
    })
  })
}

/**
 * Execute multiple SQL statements in a transaction
 */
export async function executeTransaction(
  db: duckdb.Database,
  statements: string[]
): Promise<void> {
  await executeStatement(db, 'BEGIN TRANSACTION')

  try {
    for (const sql of statements) {
      await executeStatement(db, sql)
    }
    await executeStatement(db, 'COMMIT')
  } catch (error) {
    await executeStatement(db, 'ROLLBACK')
    throw error
  }
}

/**
 * Close database connection
 */
export async function closeMotherduck(db: duckdb.Database): Promise<void> {
  return new Promise((resolve, reject) => {
    db.close((err) => {
      if (err) {
        reject(
          new MotherduckError(`Failed to close connection: ${err.message}`, err)
        )
      } else {
        resolve()
      }
    })
  })
}

/**
 * Check if database exists
 */
export async function databaseExists(
  db: duckdb.Database,
  dbName: string
): Promise<boolean> {
  const result = await executeQuery<{ database_name: string }>(
    db,
    'SHOW DATABASES'
  )
  return result.some((row) => row.database_name === dbName)
}

/**
 * Create database if it doesn't exist
 */
export async function ensureDatabase(
  db: duckdb.Database,
  dbName: string
): Promise<void> {
  const exists = await databaseExists(db, dbName)

  if (!exists) {
    await executeStatement(db, `CREATE DATABASE IF NOT EXISTS ${dbName}`)
    console.log(`Created database: ${dbName}`)
  }
}

/**
 * Check if table exists
 */
export async function tableExists(
  db: duckdb.Database,
  tableName: string
): Promise<boolean> {
  const result = await executeQuery<{ table_name: string }>(
    db,
    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
  )
  return result.some((row) => row.table_name === tableName)
}

/**
 * Get table row count
 */
export async function getTableCount(
  db: duckdb.Database,
  tableName: string
): Promise<number> {
  const result = await executeQuery<{ count: number }>(
    db,
    `SELECT COUNT(*) as count FROM ${tableName}`
  )
  return result[0]?.count || 0
}

/**
 * Get database statistics
 */
export async function getDatabaseStats(
  db: duckdb.Database
): Promise<{ table_name: string; row_count: number }[]> {
  try {
    const tables = await executeQuery<{ table_name: string }>(
      db,
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    )

    const stats = []
    for (const { table_name } of tables) {
      // Skip system tables that start with underscore
      if (table_name.startsWith('_')) {
        continue
      }
      try {
        const count = await getTableCount(db, table_name)
        stats.push({ table_name, row_count: count })
      } catch (error) {
        // Skip tables that can't be counted
        console.warn(`Warning: Could not count rows in table ${table_name}`)
      }
    }

    return stats
  } catch (error) {
    // If we can't query information_schema, return empty array
    return []
  }
}
