import { connectMotherduck, closeMotherduck, executeQuery } from './index'

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
    }

    await closeMotherduck(db)

    return stats
  } catch (error) {
    await closeMotherduck(db)
    throw error
  }
}
