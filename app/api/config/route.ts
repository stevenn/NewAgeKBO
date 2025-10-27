import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'

export async function GET() {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const config = {
      motherduckDatabase: process.env.MOTHERDUCK_DATABASE || 'kbo',
    }

    return NextResponse.json(config)
  } catch (error) {
    console.error('Failed to get config:', error)
    return NextResponse.json({ error: 'Failed to get config' }, { status: 500 })
  }
}
