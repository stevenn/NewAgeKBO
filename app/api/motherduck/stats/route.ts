import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { getDatabaseStats } from '@/lib/motherduck/stats'
import type { Language } from '@/lib/types/codes'

export type { DatabaseStats } from '@/lib/motherduck/stats'

export async function GET(request: Request) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Parse language parameter
    const { searchParams } = new URL(request.url)
    const languageParam = searchParams.get('language')

    // Validate language parameter (default to NL)
    const language: Language = ['NL', 'FR', 'DE'].includes(languageParam?.toUpperCase() || '')
      ? (languageParam?.toUpperCase() as Language)
      : 'NL'

    const stats = await getDatabaseStats(language)
    return NextResponse.json(stats)
  } catch (error) {
    console.error('Failed to fetch database stats:', error)
    return NextResponse.json(
      { error: 'Failed to fetch database stats' },
      { status: 500 }
    )
  }
}
