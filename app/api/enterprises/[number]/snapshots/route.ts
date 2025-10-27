import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'

export interface Snapshot {
  snapshotDate: string
  extractNumber: number
  isCurrent: boolean
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ number: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const { number } = await params

    // Connect to Motherduck
    const connection = await connectMotherduck()

    try {
      // Fetch all snapshots for this enterprise
      // Check all tables because in incremental updates, only changed records appear
      // (e.g., if only a denomination changed, the enterprise record won't be in that extract)
      const snapshots = await executeQuery<{
        _snapshot_date: string
        _extract_number: number
        _is_current: boolean
      }>(
        connection,
        `WITH all_extracts AS (
          SELECT DISTINCT _extract_number, _snapshot_date
          FROM enterprises WHERE enterprise_number = '${number}'
          UNION
          SELECT DISTINCT _extract_number, _snapshot_date
          FROM denominations WHERE entity_number = '${number}'
          UNION
          SELECT DISTINCT _extract_number, _snapshot_date
          FROM addresses WHERE entity_number = '${number}'
          UNION
          SELECT DISTINCT _extract_number, _snapshot_date
          FROM activities WHERE entity_number = '${number}'
          UNION
          SELECT DISTINCT _extract_number, _snapshot_date
          FROM contacts WHERE entity_number = '${number}'
          UNION
          SELECT DISTINCT _extract_number, _snapshot_date
          FROM establishments WHERE enterprise_number = '${number}'
        ),
        max_extract AS (
          SELECT MAX(_extract_number) as max_extract_num
          FROM all_extracts
        )
        SELECT
          _snapshot_date::VARCHAR as _snapshot_date,
          _extract_number,
          (_extract_number = (SELECT max_extract_num FROM max_extract)) as _is_current
        FROM all_extracts
        ORDER BY _snapshot_date DESC, _extract_number DESC`
      )

      if (snapshots.length === 0) {
        return NextResponse.json({ error: 'Enterprise not found' }, { status: 404 })
      }

      const formattedSnapshots: Snapshot[] = snapshots.map((s) => ({
        snapshotDate: s._snapshot_date,
        extractNumber: s._extract_number,
        isCurrent: s._is_current,
      }))

      return NextResponse.json({
        snapshots: formattedSnapshots,
      })
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error) {
    console.error('Failed to fetch enterprise snapshots:', error)
    return NextResponse.json(
      { error: 'Failed to fetch enterprise snapshots' },
      { status: 500 }
    )
  }
}
