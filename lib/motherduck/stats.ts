import { connectMotherduck, closeMotherduck, executeQuery } from './index'
import { generateProvinceSQLCase } from '../config/provinces'

export interface JuridicalFormStat {
  code: string
  description_nl: string | null
  description_fr: string | null
  count: number
}

export interface LanguageStat {
  language: string
  count: number
  percentage: number
}

export interface ProvinceStat {
  province: string
  count: number
  percentage: number
}

export interface DatabaseStats {
  totalEnterprises: number
  totalEstablishments: number
  totalActivities: number
  currentExtract: number
  lastUpdate: string
  databaseSize: string
  recordCounts: {
    enterprises: number
    establishments: number
    activities: number
    addresses: number
    denominations: number
    contacts: number
  }
  juridicalForms: {
    all: JuridicalFormStat[]
  }
  languageDistribution: LanguageStat[]
  provinceDistribution: ProvinceStat[]
}

export async function getDatabaseStats(): Promise<DatabaseStats> {
  const connection = await connectMotherduck()

  try {
    const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'

    // Get latest extract number and snapshot date
    const extractInfo = await executeQuery<{ extract_number: number; snapshot_date: string | Date }>(
      connection,
      `SELECT MAX(_extract_number) as extract_number, MAX(_snapshot_date)::VARCHAR as snapshot_date
       FROM enterprises
       WHERE _is_current = true`
    )

    const currentExtract = extractInfo[0]?.extract_number || 0
    const snapshotDate = extractInfo[0]?.snapshot_date
    const lastUpdate = snapshotDate
      ? (typeof snapshotDate === 'string' ? snapshotDate : snapshotDate.toISOString().split('T')[0])
      : 'Unknown'

    // Get actual database size from Motherduck
    // Use md_information_schema.storage_info which is Motherduck-specific
    const sizeInfo = await executeQuery<{ active_bytes: string; historical_bytes: string }>(
      connection,
      `SELECT CAST(active_bytes AS VARCHAR) as active_bytes,
              CAST(historical_bytes AS VARCHAR) as historical_bytes
       FROM md_information_schema.storage_info
       WHERE database_name = '${dbName}'`
    )

    // Format bytes to human readable size
    const formatBytes = (bytes: string): string => {
      const num = parseInt(bytes)
      if (isNaN(num)) return 'Unknown'
      const units = ['B', 'KB', 'MB', 'GB', 'TB']
      let value = num
      let unitIndex = 0
      while (value >= 1024 && unitIndex < units.length - 1) {
        value /= 1024
        unitIndex++
      }
      return `${value.toFixed(2)} ${units[unitIndex]}`
    }

    const activeBytes = sizeInfo[0]?.active_bytes || '0'
    const historicalBytes = sizeInfo[0]?.historical_bytes || '0'
    const totalBytes = (parseInt(activeBytes) + parseInt(historicalBytes)).toString()
    const databaseSize = formatBytes(totalBytes)

    // Get record counts for current data
    const counts = await Promise.all([
      executeQuery<{ count: number }>(connection, `SELECT COUNT(*) as count FROM enterprises WHERE _is_current = true`),
      executeQuery<{ count: number }>(connection, `SELECT COUNT(*) as count FROM establishments WHERE _is_current = true`),
      executeQuery<{ count: number }>(connection, `SELECT COUNT(*) as count FROM activities WHERE _is_current = true`),
      executeQuery<{ count: number }>(connection, `SELECT COUNT(*) as count FROM addresses WHERE _is_current = true`),
      executeQuery<{ count: number }>(connection, `SELECT COUNT(*) as count FROM denominations WHERE _is_current = true`),
      executeQuery<{ count: number }>(connection, `SELECT COUNT(*) as count FROM contacts WHERE _is_current = true`),
    ])

    // Get juridical form statistics (histogram)
    // Note: NULL juridical_form = Natural Person (type_of_enterprise = '1')
    const juridicalFormsAll = await executeQuery<JuridicalFormStat>(
      connection,
      `SELECT
        COALESCE(e.juridical_form, 'NATURAL_PERSON') as code,
        CASE
          WHEN e.juridical_form IS NULL THEN 'Natuurlijk Persoon (geen rechtsvorm)'
          ELSE MAX(c_nl.description)
        END as description_nl,
        CASE
          WHEN e.juridical_form IS NULL THEN 'Personne physique (pas de forme juridique)'
          ELSE MAX(c_fr.description)
        END as description_fr,
        COUNT(*) as count
      FROM enterprises e
      LEFT JOIN codes c_nl ON c_nl.category = 'JuridicalForm'
        AND c_nl.code = e.juridical_form
        AND c_nl.language = 'NL'
      LEFT JOIN codes c_fr ON c_fr.category = 'JuridicalForm'
        AND c_fr.code = e.juridical_form
        AND c_fr.language = 'FR'
      WHERE e._is_current = true
      GROUP BY e.juridical_form
      ORDER BY count DESC`
    )

    // Get language distribution (legal persons only - natural persons have no language data)
    const languageDistribution = await executeQuery<{ language: string; count: number }>(
      connection,
      `SELECT
        CASE
          WHEN primary_name_language = '2' THEN 'NL'
          WHEN primary_name_language = '1' THEN 'FR'
          WHEN primary_name_language = '3' THEN 'DE'
          WHEN primary_name_language = '4' THEN 'EN'
          WHEN primary_name_language = '0' OR primary_name_language IS NULL THEN 'Unknown'
          ELSE 'Other'
        END as language,
        COUNT(*) as count
      FROM enterprises
      WHERE _is_current = true
        AND type_of_enterprise = '2'  -- Legal persons only
      GROUP BY primary_name_language
      ORDER BY count DESC`
    )

    const totalLegalPersons = languageDistribution.reduce(
      (sum, item) => sum + Number(item.count),
      0
    )

    const languageStats: LanguageStat[] = languageDistribution.map((item) => ({
      language: item.language,
      count: Number(item.count),
      percentage: totalLegalPersons > 0 ? (Number(item.count) / totalLegalPersons) * 100 : 0,
    }))

    // Get province distribution from addresses
    const provinceSQLCase = generateProvinceSQLCase()
    const totalAddressesResult = await executeQuery<{ count: number }>(
      connection,
      `SELECT COUNT(*) as count
       FROM addresses
       WHERE _is_current = true
         AND zipcode IS NOT NULL
         AND TRY_CAST(zipcode AS INTEGER) IS NOT NULL`
    )
    const totalForProvinces = Number(totalAddressesResult[0]?.count || 0)

    const provinceDistribution = await executeQuery<{ province: string; count: number }>(
      connection,
      `SELECT
        ${provinceSQLCase} as province,
        COUNT(*) as count
      FROM addresses
      WHERE _is_current = true
        AND zipcode IS NOT NULL
        AND TRY_CAST(zipcode AS INTEGER) IS NOT NULL
      GROUP BY province
      ORDER BY count DESC`
    )

    const provinceStats: ProvinceStat[] = provinceDistribution.map((item) => ({
      province: item.province,
      count: Number(item.count),
      percentage: totalForProvinces > 0 ? (Number(item.count) / totalForProvinces) * 100 : 0,
    }))

    const stats: DatabaseStats = {
      totalEnterprises: Number(counts[0][0].count),
      totalEstablishments: Number(counts[1][0].count),
      totalActivities: Number(counts[2][0].count),
      currentExtract,
      lastUpdate,
      databaseSize,
      recordCounts: {
        enterprises: Number(counts[0][0].count),
        establishments: Number(counts[1][0].count),
        activities: Number(counts[2][0].count),
        addresses: Number(counts[3][0].count),
        denominations: Number(counts[4][0].count),
        contacts: Number(counts[5][0].count),
      },
      juridicalForms: {
        all: juridicalFormsAll.map((item) => ({
          ...item,
          count: Number(item.count),
        })),
      },
      languageDistribution: languageStats,
      provinceDistribution: provinceStats,
    }

    return stats
  } finally {
    await closeMotherduck(connection)
  }
}
