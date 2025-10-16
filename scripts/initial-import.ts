#!/usr/bin/env tsx

/**
 * Initial KBO data import
 *
 * Downloads full dataset from KBO portal, processes with local DuckDB,
 * and streams directly to Motherduck
 *
 * Usage:
 *   npx tsx scripts/initial-import.ts <path-to-extracted-kbo-data>
 *
 * Example:
 *   npx tsx scripts/initial-import.ts ./sampledata/KboOpenData_0140_2025_10_05_Full
 */

// Load environment variables (.env.local takes precedence, then .env)
import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import * as duckdb from 'duckdb'
import { existsSync } from 'fs'
import { join } from 'path'
import {
  connectMotherduck,
  closeMotherduck,
  getMotherduckConfig,
  executeQuery,
  tableExists,
} from '../lib/motherduck'
import { formatUserError } from '../lib/errors'

interface ImportProgress {
  table: string
  phase: 'checking' | 'loading' | 'transforming' | 'uploading' | 'complete'
  rowsProcessed?: number
  totalRows?: number
}

interface ImportStats {
  table: string
  rowsInserted: number
  durationMs: number
}

interface MetaData {
  snapshotDate: string  // YYYY-MM-DD format
  extractNumber: number
  extractType: string
  version: string
}

/**
 * Parse meta.csv using DuckDB to extract snapshot date and extract number
 */
async function parseMetaCsv(db: duckdb.Database, dataPath: string): Promise<MetaData> {
  const metaPath = join(dataPath, 'meta.csv')

  // Load meta.csv and pivot it to get values by variable name
  const rows = await new Promise<any[]>((resolve, reject) => {
    db.all(
      `SELECT Variable, Value
       FROM read_csv('${metaPath}', AUTO_DETECT=TRUE, HEADER=TRUE)`,
      (err, rows) => {
        if (err) reject(err)
        else resolve(rows)
      }
    )
  })

  // Convert rows to key-value map
  const metadata: Record<string, string> = {}
  for (const row of rows) {
    metadata[row.Variable] = row.Value
  }

  // Convert DD-MM-YYYY to YYYY-MM-DD
  const snapshotDateParts = metadata.SnapshotDate.split('-')
  const snapshotDate = `${snapshotDateParts[2]}-${snapshotDateParts[1]}-${snapshotDateParts[0]}`

  return {
    snapshotDate,
    extractNumber: parseInt(metadata.ExtractNumber, 10),
    extractType: metadata.ExtractType,
    version: metadata.Version
  }
}

/**
 * Show progress indicator
 */
function showProgress(progress: ImportProgress): void {
  const phases = {
    checking: '🔍',
    loading: '📥',
    transforming: '⚙️',
    uploading: '☁️',
    complete: '✅',
  }

  const icon = phases[progress.phase]
  let message = `${icon}  ${progress.table.padEnd(15)} - ${progress.phase}`

  if (progress.rowsProcessed && progress.totalRows) {
    const percent = Math.round((progress.rowsProcessed / progress.totalRows) * 100)
    message += ` (${progress.rowsProcessed.toLocaleString()}/${progress.totalRows.toLocaleString()} - ${percent}%)`
  }

  console.log(message)
}

/**
 * Verify all required CSV files exist
 */
function verifyCsvFiles(dataPath: string): string[] {
  const requiredFiles = [
    'meta.csv',
    'code.csv',
    'enterprise.csv',
    'establishment.csv',
    'denomination.csv',
    'address.csv',
    'activity.csv',
    'contact.csv',
    'branch.csv',
  ]

  const missingFiles: string[] = []

  for (const file of requiredFiles) {
    const filePath = join(dataPath, file)
    if (!existsSync(filePath)) {
      missingFiles.push(file)
    }
  }

  return missingFiles
}

/**
 * Main import function
 */
async function initialImport() {
  console.log('🚀 KBO Initial Data Import\n')

  // Get data path from command line
  const dataPath = process.argv[2]

  if (!dataPath) {
    console.error('❌ Error: Data path not provided\n')
    console.error('Usage: npx tsx scripts/initial-import.ts <path-to-extracted-kbo-data>\n')
    console.error('Example: npx tsx scripts/initial-import.ts ./sampledata/KboOpenData_0140_2025_10_05_Full')
    process.exit(1)
  }

  if (!existsSync(dataPath)) {
    console.error(`❌ Error: Data path does not exist: ${dataPath}`)
    process.exit(1)
  }

  console.log(`📂 Data path: ${dataPath}\n`)

  try {
    // Step 1: Verify CSV files
    console.log('1️⃣  Verifying CSV files...')
    const missingFiles = verifyCsvFiles(dataPath)

    if (missingFiles.length > 0) {
      console.error(`   ❌ Missing required files:`)
      for (const file of missingFiles) {
        console.error(`      • ${file}`)
      }
      console.error()
      process.exit(1)
    }

    console.log('   ✅ All required CSV files found\n')

    // Step 2: Connect to Motherduck
    console.log('2️⃣  Connecting to Motherduck...')
    const mdConfig = getMotherduckConfig()
    const motherduckDb = await connectMotherduck()

    // Use the database
    await executeQuery(motherduckDb, `USE ${mdConfig.database}`)
    console.log(`   ✅ Connected to database: ${mdConfig.database}\n`)

    // Step 3: Verify schema exists
    console.log('3️⃣  Verifying database schema...')
    const requiredTables = [
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
    ]

    const missingTables: string[] = []
    for (const tableName of requiredTables) {
      const exists = await tableExists(motherduckDb, tableName)
      if (!exists) {
        missingTables.push(tableName)
      }
    }

    if (missingTables.length > 0) {
      console.error('   ❌ Missing required tables:')
      for (const table of missingTables) {
        console.error(`      • ${table}`)
      }
      console.error('\n   💡 Run: npx tsx scripts/create-schema.ts\n')
      process.exit(1)
    }

    console.log('   ✅ All required tables exist\n')

    // Step 4: Check if database already has data
    console.log('4️⃣  Checking for existing data...')

    // Check multiple tables to ensure a clean import
    const tablesToCheck = ['enterprises', 'codes', 'nace_codes']
    let totalRows = 0

    for (const table of tablesToCheck) {
      const result = await executeQuery<{ count: number }>(
        motherduckDb,
        `SELECT COUNT(*) as count FROM ${table}`
      )
      totalRows += Number(result[0].count)  // Convert BigInt to Number
    }

    if (totalRows > 0) {
      console.error(`   ⚠️  Database already contains data (${totalRows} total rows across tables)`)
      console.error('\n   This script is for INITIAL import only.')
      console.error('   Run: npx tsx scripts/cleanup-data.ts (to clean existing data)\n')
      process.exit(1)
    }

    console.log('   ✅ Database is empty and ready for import\n')

    // Step 5: Create local DuckDB instance connected to Motherduck
    console.log('5️⃣  Initializing DuckDB with Motherduck connection...')

    // Set Motherduck token in environment (required for DuckDB motherduck extension)
    // Must be uppercase MOTHERDUCK_TOKEN
    process.env.MOTHERDUCK_TOKEN = mdConfig.token

    // Create local DuckDB that can also access Motherduck
    const localDb = new duckdb.Database(':memory:')

    // Install and load Motherduck extension
    console.log('   📦 Loading Motherduck extension...')
    await new Promise<void>((resolve, reject) => {
      localDb.exec(
        `INSTALL motherduck;
         LOAD motherduck;
         PRAGMA enable_progress_bar;`,
        (err) => {
          if (err) reject(err)
          else resolve()
        }
      )
    })

    // Attach Motherduck to local DuckDB instance
    console.log('   🔗 Attaching Motherduck database...')
    await new Promise<void>((resolve, reject) => {
      localDb.exec(
        `ATTACH 'md:${mdConfig.database}' AS motherduck;`,
        (err) => {
          if (err) reject(err)
          else resolve()
        }
      )
    })

    console.log('   ✅ Connected to Motherduck via local DuckDB\n')

    // Step 6: Parse metadata from meta.csv
    console.log('6️⃣  Reading metadata from meta.csv...')
    const metadata = await parseMetaCsv(localDb, dataPath)
    console.log(`   ✅ Extract #${metadata.extractNumber} (${metadata.snapshotDate})`)
    console.log(`   📅 Snapshot date: ${metadata.snapshotDate}`)
    console.log(`   📦 Extract type: ${metadata.extractType}`)
    console.log(`   🔢 Version: ${metadata.version}\n`)

    // Step 7: Load and process data
    console.log('7️⃣  Processing data...\n')

    const stats: ImportStats[] = []
    const startTime = Date.now()

    // Table 1: Codes (static lookup)
    await processTable(
      localDb,
      motherduckDb,
      'codes',
      join(dataPath, 'code.csv'),
      `
        SELECT
          Category as category,
          Code as code,
          Language as language,
          MAX(Description) as description  -- Handle potential duplicates in code.csv
        FROM staged_codes
        GROUP BY Category, Code, Language
      `,
      stats,
      metadata
    )

    // Table 2: NACE Codes (static lookup - KBO only provides NL and FR)
    await processTable(
      localDb,
      motherduckDb,
      'nace_codes',
      join(dataPath, 'code.csv'),
      `
        SELECT DISTINCT
          CASE
            WHEN Category = 'Nace2003' THEN '2003'
            WHEN Category = 'Nace2008' THEN '2008'
            WHEN Category = 'Nace2025' THEN '2025'
          END as nace_version,
          Code as nace_code,
          MAX(CASE WHEN Language = 'NL' THEN Description END) as description_nl,
          MAX(CASE WHEN Language = 'FR' THEN Description END) as description_fr
        FROM staged_codes
        WHERE Category IN ('Nace2003', 'Nace2008', 'Nace2025')
        GROUP BY nace_version, nace_code
      `,
      stats,
      metadata
    )

    // Table 3: Load enterprises and denominations for primary name selection
    console.log('   📝 Loading enterprises and denominations for primary name selection...')

    // Load enterprise.csv first
    await new Promise<void>((resolve, reject) => {
      localDb.exec(
        `
        CREATE TEMP TABLE staged_enterprises AS
        SELECT * FROM read_csv('${join(dataPath, 'enterprise.csv')}', AUTO_DETECT=TRUE, HEADER=TRUE);
        `,
        (err) => {
          if (err) reject(err)
          else resolve()
        }
      )
    })

    // Load denomination.csv
    await new Promise<void>((resolve, reject) => {
      localDb.exec(
        `
        CREATE TEMP TABLE staged_denominations AS
        SELECT * FROM read_csv('${join(dataPath, 'denomination.csv')}', AUTO_DETECT=TRUE, HEADER=TRUE);
        `,
        (err) => {
          if (err) reject(err)
          else resolve()
        }
      )
    })

    // Create ranked denominations for primary selection
    await new Promise<void>((resolve, reject) => {
      localDb.exec(
        `
        CREATE TEMP TABLE ranked_denominations AS
        SELECT
          EntityNumber,
          Language,
          TypeOfDenomination,
          Denomination,
          ROW_NUMBER() OVER (
            PARTITION BY EntityNumber
            ORDER BY
              CASE TypeOfDenomination
                WHEN '001' THEN 1  -- Legal name (highest priority)
                WHEN '003' THEN 2  -- Commercial name
                WHEN '002' THEN 3  -- Abbreviation
                WHEN '004' THEN 4  -- Branch name
                ELSE 5
              END,
              CASE Language
                WHEN '2' THEN 1  -- Dutch (highest priority)
                WHEN '1' THEN 2  -- French
                WHEN '3' THEN 3  -- German
                WHEN '4' THEN 4  -- English
                WHEN '0' THEN 5  -- Unknown
                ELSE 6
              END
          ) as priority_rank
        FROM staged_denominations
        WHERE EntityNumber IN (SELECT EnterpriseNumber FROM staged_enterprises);
        `,
        (err) => {
          if (err) reject(err)
          else resolve()
        }
      )
    })

    // Table 3: Enterprises (with denormalized primary name)
    await processTable(
      localDb,
      motherduckDb,
      'enterprises',
      '', // Already staged above
      `
        SELECT
          e.EnterpriseNumber as enterprise_number,
          e.Status as status,
          e.JuridicalSituation as juridical_situation,
          e.TypeOfEnterprise as type_of_enterprise,
          e.JuridicalForm as juridical_form,
          e.JuridicalFormCAC as juridical_form_cac,
          TRY_CAST(e.StartDate AS DATE) as start_date,
          -- Primary name: first available in priority order (Language 2=NL, 1=FR, 3=DE, 4=EN, 0=Unknown)
          -- Store the actual name used as primary_name (never NULL)
          COALESCE(
            MAX(CASE WHEN d.Language = '2' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '1' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '0' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '3' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '4' THEN d.Denomination END),
            e.EnterpriseNumber
          ) as primary_name,
          -- Track which language the primary_name is in
          COALESCE(
            MAX(CASE WHEN d.Language = '2' THEN '2' END),
            MAX(CASE WHEN d.Language = '1' THEN '1' END),
            MAX(CASE WHEN d.Language = '0' THEN '0' END),
            MAX(CASE WHEN d.Language = '3' THEN '3' END),
            MAX(CASE WHEN d.Language = '4' THEN '4' END),
            NULL
          ) as primary_name_language,
          -- Store each language variant separately (NULL if not available)
          MAX(CASE WHEN d.Language = '2' THEN d.Denomination END) as primary_name_nl,
          MAX(CASE WHEN d.Language = '1' THEN d.Denomination END) as primary_name_fr,
          MAX(CASE WHEN d.Language = '3' THEN d.Denomination END) as primary_name_de,
          MAX(d.TypeOfDenomination) as primary_name_type,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_enterprises e
        LEFT JOIN ranked_denominations d
          ON e.EnterpriseNumber = d.EntityNumber
          AND d.priority_rank = 1
        GROUP BY
          e.EnterpriseNumber,
          e.Status,
          e.JuridicalSituation,
          e.TypeOfEnterprise,
          e.JuridicalForm,
          e.JuridicalFormCAC,
          e.StartDate
      `,
      stats,
      metadata
    )

    // Table 4: Load other CSV files for transformation
    console.log('   📝 Loading remaining CSV files...')
    const csvFiles = [
      { name: 'establishments', file: 'establishment.csv' },
      { name: 'addresses', file: 'address.csv' },
      { name: 'activities', file: 'activity.csv' },
      { name: 'contacts', file: 'contact.csv' },
      { name: 'branches', file: 'branch.csv' },
    ]

    for (const { name, file } of csvFiles) {
      await new Promise<void>((resolve, reject) => {
        localDb.exec(
          `CREATE TEMP TABLE staged_${name} AS SELECT * FROM read_csv('${join(dataPath, file)}', AUTO_DETECT=TRUE, HEADER=TRUE);`,
          (err) => {
            if (err) reject(err)
            else resolve()
          }
        )
      })
    }
    console.log('   ✅ All CSV files loaded\n')

    // Table 4: Establishments (with commercial names from denominations)
    await processTable(
      localDb,
      motherduckDb,
      'establishments',
      '', // Already staged
      `
        SELECT
          e.EstablishmentNumber as establishment_number,
          e.EnterpriseNumber as enterprise_number,
          TRY_CAST(e.StartDate AS DATE) as start_date,
          -- Extract primary commercial name (Type 003) - Priority: Dutch -> French -> Unknown -> German -> English
          COALESCE(
            MAX(CASE WHEN d.Language = '2' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '1' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '0' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '3' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
            MAX(CASE WHEN d.Language = '4' AND d.TypeOfDenomination = '003' THEN d.Denomination END)
          ) as commercial_name,
          -- Track which language the commercial_name is in
          COALESCE(
            MAX(CASE WHEN d.Language = '2' AND d.TypeOfDenomination = '003' THEN '2' END),
            MAX(CASE WHEN d.Language = '1' AND d.TypeOfDenomination = '003' THEN '1' END),
            MAX(CASE WHEN d.Language = '0' AND d.TypeOfDenomination = '003' THEN '0' END),
            MAX(CASE WHEN d.Language = '3' AND d.TypeOfDenomination = '003' THEN '3' END),
            MAX(CASE WHEN d.Language = '4' AND d.TypeOfDenomination = '003' THEN '4' END)
          ) as commercial_name_language,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_establishments e
        LEFT JOIN staged_denominations d
          ON e.EstablishmentNumber = d.EntityNumber
          AND d.TypeOfDenomination = '003'  -- Commercial name only
        GROUP BY e.EstablishmentNumber, e.EnterpriseNumber, e.StartDate
      `,
      stats,
      metadata
    )

    // Table 5: Denominations (all names)
    await processTable(
      localDb,
      motherduckDb,
      'denominations',
      '', // Already staged
      `
        SELECT
          EntityNumber || '_' || TypeOfDenomination || '_' || Language || '_' ||
          ROW_NUMBER() OVER (PARTITION BY EntityNumber, TypeOfDenomination, Language ORDER BY Denomination) as id,
          EntityNumber as entity_number,
          CASE
            WHEN EntityNumber LIKE '2.%' THEN 'establishment'
            ELSE 'enterprise'
          END as entity_type,
          TypeOfDenomination as denomination_type,
          Language as language,
          Denomination as denomination,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_denominations
      `,
      stats,
      metadata
    )

    // Table 6: Addresses
    await processTable(
      localDb,
      motherduckDb,
      'addresses',
      '', // Already staged
      `
        SELECT
          EntityNumber || '_' || TypeOfAddress as id,
          EntityNumber as entity_number,
          CASE
            WHEN EntityNumber LIKE '2.%' THEN 'establishment'
            ELSE 'enterprise'
          END as entity_type,
          TypeOfAddress as type_of_address,
          CountryNL as country_nl,
          CountryFR as country_fr,
          Zipcode as zipcode,
          MunicipalityNL as municipality_nl,
          MunicipalityFR as municipality_fr,
          StreetNL as street_nl,
          StreetFR as street_fr,
          HouseNumber as house_number,
          Box as box,
          ExtraAddressInfo as extra_address_info,
          TRY_CAST(DateStrikingOff AS DATE) as date_striking_off,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_addresses
      `,
      stats,
      metadata
    )

    // Table 7: Activities (link table with NACE codes)
    await processTable(
      localDb,
      motherduckDb,
      'activities',
      '', // Already staged
      `
        SELECT
          EntityNumber || '_' || ActivityGroup || '_' || NaceVersion || '_' || NaceCode || '_' ||
          ROW_NUMBER() OVER (PARTITION BY EntityNumber, ActivityGroup, NaceVersion, NaceCode ORDER BY Classification) as id,
          EntityNumber as entity_number,
          CASE
            WHEN EntityNumber LIKE '2.%' THEN 'establishment'
            ELSE 'enterprise'
          END as entity_type,
          ActivityGroup as activity_group,
          NaceVersion as nace_version,
          NaceCode as nace_code,
          Classification as classification,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_activities
      `,
      stats,
      metadata
    )

    // Table 8: Contacts
    await processTable(
      localDb,
      motherduckDb,
      'contacts',
      '', // Already staged
      `
        SELECT
          EntityNumber || '_' || EntityContact || '_' || ContactType || '_' || ROW_NUMBER() OVER (PARTITION BY EntityNumber, EntityContact, ContactType ORDER BY Value) as id,
          EntityNumber as entity_number,
          CASE
            WHEN EntityNumber LIKE '2.%' THEN 'establishment'
            ELSE 'enterprise'
          END as entity_type,
          EntityContact as entity_contact,
          ContactType as contact_type,
          Value as contact_value,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_contacts
      `,
      stats,
      metadata
    )

    // Table 9: Branches
    await processTable(
      localDb,
      motherduckDb,
      'branches',
      '', // Already staged
      `
        SELECT
          Id as id,
          EnterpriseNumber as enterprise_number,
          TRY_CAST(StartDate AS DATE) as start_date,
          CURRENT_DATE as _snapshot_date,
          0 as _extract_number,
          TRUE as _is_current
        FROM staged_branches
      `,
      stats,
      metadata
    )

    // Close connections
    await closeMotherduck(motherduckDb)

    // Summary
    const totalDuration = Date.now() - startTime
    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━')
    console.log('✨ SUCCESS! Initial import complete')
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')

    console.log('📊 Import Statistics:\n')
    for (const stat of stats) {
      console.log(
        `   ${stat.table.padEnd(15)} ${stat.rowsInserted.toLocaleString().padStart(10)} rows  (${(stat.durationMs / 1000).toFixed(2)}s)`
      )
    }

    console.log(`\n   Total duration: ${(totalDuration / 1000).toFixed(2)}s`)
    console.log()

  } catch (error) {
    console.error('\n❌ Import failed!\n')

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

/**
 * Process a single table: load CSV (optional), transform, stream to Motherduck
 */
async function processTable(
  localDb: duckdb.Database,
  motherduckDb: duckdb.Database,
  tableName: string,
  csvPath: string,
  transformSql: string,
  stats: ImportStats[],
  metadata?: MetaData
): Promise<void> {
  const startTime = Date.now()

  // Load CSV into local DuckDB temp table (if path provided)
  if (csvPath) {
    showProgress({ table: tableName, phase: 'loading' })

    await new Promise<void>((resolve, reject) => {
      localDb.exec(
        `
        DROP TABLE IF EXISTS staged_${tableName};
        CREATE TEMP TABLE staged_${tableName} AS
        SELECT * FROM read_csv('${csvPath}', AUTO_DETECT=TRUE, HEADER=TRUE);
        `,
        (err) => {
          if (err) reject(err)
          else resolve()
        }
      )
    })
  }

  showProgress({ table: tableName, phase: 'transforming' })

  // Replace metadata placeholders in SQL if metadata is provided
  let finalSql = transformSql
  if (metadata) {
    finalSql = finalSql
      .replace(/CURRENT_DATE as _snapshot_date/g, `DATE '${metadata.snapshotDate}' as _snapshot_date`)
      .replace(/0 as _extract_number/g, `${metadata.extractNumber} as _extract_number`)
  }

  // Apply transformations
  await new Promise<void>((resolve, reject) => {
    localDb.exec(
      `
      DROP TABLE IF EXISTS transformed_${tableName};
      CREATE TEMP TABLE transformed_${tableName} AS
      ${finalSql}
      `,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })

  // Get row count for progress tracking
  const rowCount = await new Promise<number>((resolve, reject) => {
    localDb.all(
      `SELECT COUNT(*) as count FROM transformed_${tableName}`,
      (err, rows: any[]) => {
        if (err) reject(err)
        else resolve(Number(rows[0].count))  // Convert BigInt to Number
      }
    )
  })

  showProgress({
    table: tableName,
    phase: 'uploading',
    rowsProcessed: 0,
    totalRows: rowCount,
  })

  // Direct INSERT SELECT from local to Motherduck (via ATTACH)
  // This is the most efficient way - DuckDB handles the streaming internally
  await new Promise<void>((resolve, reject) => {
    localDb.exec(
      `INSERT INTO motherduck.${tableName} SELECT * FROM transformed_${tableName}`,
      (err) => {
        if (err) reject(err)
        else resolve()
      }
    )
  })

  const duration = Date.now() - startTime

  showProgress({
    table: tableName,
    phase: 'uploading',
    rowsProcessed: rowCount,
    totalRows: rowCount,
  })

  showProgress({ table: tableName, phase: 'complete' })

  stats.push({
    table: tableName,
    rowsInserted: rowCount,
    durationMs: duration,
  })
}

// Run the import
initialImport()
