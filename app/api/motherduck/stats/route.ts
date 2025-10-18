import { NextResponse } from 'next/server'
import { auth } from '@clerk/nextjs/server'
import { getDatabaseStats } from '@/lib/motherduck/stats'

export type { DatabaseStats } from '@/lib/motherduck/stats'

export async function GET() {
  try {
    // Check authentication
    const { userId } = await auth()
    if (!userId) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const stats = await getDatabaseStats()
    return NextResponse.json(stats)
  } catch (error) {
    console.error('Failed to fetch database stats:', error)
    return NextResponse.json(
      { error: 'Failed to fetch database stats' },
      { status: 500 }
    )
  }
}
