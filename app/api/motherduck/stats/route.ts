import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { getDatabaseStats } from '@/lib/motherduck/stats'

export type { DatabaseStats } from '@/lib/motherduck/stats'

export async function GET() {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

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
