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
  const db = await connectMotherduck()

  try {
    const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
    await executeQuery(db, `USE ${dbName}`)

    // Get latest extract number and snapshot date
    const extractInfo = await executeQuery<{ extract_number: number; snapshot_date: string | Date }>(
      db,
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
    const sizeInfo = await executeQuery<{ database_size: string }>(
      db,
      `SELECT database_size FROM pragma_database_size() WHERE database_name = '${dbName}'`
    )
    const databaseSize = sizeInfo[0]?.database_size || 'Unknown'

    // Get record counts for current data
    const counts = await Promise.all([
      executeQuery<{ count: number }>(db, `SELECT COUNT(*) as count FROM enterprises WHERE _is_current = true`),
      executeQuery<{ count: number }>(db, `SELECT COUNT(*) as count FROM establishments WHERE _is_current = true`),
      executeQuery<{ count: number }>(db, `SELECT COUNT(*) as count FROM activities WHERE _is_current = true`),
      executeQuery<{ count: number }>(db, `SELECT COUNT(*) as count FROM addresses WHERE _is_current = true`),
      executeQuery<{ count: number }>(db, `SELECT COUNT(*) as count FROM denominations WHERE _is_current = true`),
      executeQuery<{ count: number }>(db, `SELECT COUNT(*) as count FROM contacts WHERE _is_current = true`),
    ])

    // Get juridical form statistics (histogram)
    // Note: NULL juridical_form = Natural Person (type_of_enterprise = '1')
    const juridicalFormsAll = await executeQuery<JuridicalFormStat>(
      db,
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
      db,
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
      db,
      `SELECT COUNT(*) as count
       FROM addresses
       WHERE _is_current = true
         AND zipcode IS NOT NULL
         AND TRY_CAST(zipcode AS INTEGER) IS NOT NULL`
    )
    const totalForProvinces = Number(totalAddressesResult[0]?.count || 0)

    const provinceDistribution = await executeQuery<{ province: string; count: number }>(
      db,
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
    await closeMotherduck(db)
  }
}
