import { NextResponse } from 'next/server'
import { auth } from '@clerk/nextjs/server'
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
    // Check authentication
    const { userId } = await auth()
    if (!userId) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { number } = await params

    // Connect to Motherduck
    const db = await connectMotherduck()

    try {
      const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
      await executeQuery(db, `USE ${dbName}`)

      // Fetch all snapshots for this enterprise
      const snapshots = await executeQuery<{
        _snapshot_date: string
        _extract_number: number
        _is_current: boolean
      }>(
        db,
        `SELECT DISTINCT
          _snapshot_date,
          _extract_number,
          _is_current
        FROM enterprises
        WHERE enterprise_number = '${number}'
        ORDER BY _snapshot_date DESC, _extract_number DESC`
      )

      if (snapshots.length === 0) {
        await closeMotherduck(db)
        return NextResponse.json({ error: 'Enterprise not found' }, { status: 404 })
      }

      const formattedSnapshots: Snapshot[] = snapshots.map((s) => ({
        snapshotDate: s._snapshot_date,
        extractNumber: s._extract_number,
        isCurrent: s._is_current,
      }))

      await closeMotherduck(db)

      return NextResponse.json({
        snapshots: formattedSnapshots,
      })
    } catch (error) {
      await closeMotherduck(db)
      throw error
    }
  } catch (error) {
    console.error('Failed to fetch enterprise snapshots:', error)
    return NextResponse.json(
      { error: 'Failed to fetch enterprise snapshots' },
      { status: 500 }
    )
  }
}
