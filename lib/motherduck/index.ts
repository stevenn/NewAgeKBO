/**
 * Motherduck connection utilities
 * Wraps DuckDB Node.js client with Motherduck-specific configuration
 *
 * Note: Uses dynamic import for DuckDB to avoid loading native module during build
 */

import { MotherduckError } from '@/lib/errors'

// Lazy-load DuckDB at runtime to avoid build-time native module loading
let duckdb: typeof import('duckdb') | null = null

async function getDuckDB() {
  if (!duckdb) {
    duckdb = await import('duckdb')
  }
  return duckdb
}

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
 *
 * For serverless environments (Vercel), we configure DuckDB to use /tmp
 * for all filesystem operations and disable extension autoloading.
 */
export async function connectMotherduck(
  config?: MotherduckConfig
): Promise<import('duckdb').Database> {
  const DuckDB = await getDuckDB()
  const mdConfig = config || getMotherduckConfig()

  // Set Motherduck token as environment variable for DuckDB to use
  process.env.motherduck_token = mdConfig.token

  return new Promise((resolve, reject) => {
    // Use in-memory database to avoid local filesystem for data
    const db = new DuckDB.Database(':memory:', (err) => {
      if (err) {
        reject(
          new MotherduckError(
            `Failed to initialize DuckDB: ${err.message}`,
            err
          )
        )
        return
      }

      // Configure DuckDB for serverless before attaching to Motherduck
      const configStatements = [
        // Set all directory paths to /tmp (writable in Vercel)
        "SET home_directory='/tmp'",
        "SET extension_directory='/tmp/.duckdb/extensions'",
        "SET temp_directory='/tmp'",
      ]

      // Execute all config statements sequentially
      const executeConfigs = async () => {
        for (const sql of configStatements) {
          try {
            await new Promise<void>((resolveStmt) => {
              db.run(sql, (stmtErr) => {
                if (stmtErr) {
                  console.warn(`Warning: ${sql} failed:`, stmtErr.message)
                }
                resolveStmt()
              })
            })
          } catch (e) {
            console.warn(`Config statement failed: ${sql}`, e)
          }
        }

        // Install Motherduck extension explicitly
        await new Promise<void>((resolveInstall, rejectInstall) => {
          db.run("INSTALL motherduck", (installErr) => {
            if (installErr) {
              // Extension might already be installed
              console.log('Motherduck extension install:', installErr.message)
            }
            resolveInstall()
          })
        })

        // Load the Motherduck extension
        await new Promise<void>((resolveLoad, rejectLoad) => {
          db.run("LOAD motherduck", (loadErr) => {
            if (loadErr) {
              rejectLoad(
                new MotherduckError(
                  `Failed to load Motherduck extension: ${loadErr.message}`,
                  loadErr
                )
              )
            } else {
              resolveLoad()
            }
          })
        })

        // Now attach to Motherduck database
        const attachSql = mdConfig.database
          ? `ATTACH 'md:${mdConfig.database}' AS md`
          : `ATTACH 'md:' AS md`

        db.run(attachSql, (attachErr) => {
          if (attachErr) {
            reject(
              new MotherduckError(
                `Failed to attach to Motherduck: ${attachErr.message}`,
                attachErr
              )
            )
          } else {
            resolve(db)
          }
        })
      }

      executeConfigs().catch(reject)
    })
  })
}

/**
 * Execute a SQL query
 */
export async function executeQuery<T = unknown>(
  db: import('duckdb').Database,
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
  db: import('duckdb').Database,
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
  db: import('duckdb').Database,
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
export async function closeMotherduck(db: import('duckdb').Database): Promise<void> {
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
  db: import('duckdb').Database,
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
  db: import('duckdb').Database,
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
  db: import('duckdb').Database,
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
  db: import('duckdb').Database,
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
  db: import('duckdb').Database
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
