import { auth } from '@clerk/nextjs/server'
import { NextResponse } from 'next/server'

/**
 * Checks if the current user is authenticated and has admin role
 * Returns NextResponse with error if not authorized, or null if authorized
 *
 * Note: Requires custom session claims configured in Clerk Dashboard:
 * { "metadata": "{{user.public_metadata}}" }
 */
export async function checkAdminAccess(): Promise<NextResponse | null> {
  // Check authentication
  const { userId, sessionClaims } = await auth()
  if (!userId) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
  }

  // Check admin role in session claims (requires custom session token config)
  const metadata = sessionClaims?.metadata as { role?: string } | undefined

  if (metadata?.role !== 'admin') {
    return NextResponse.json(
      { error: 'Forbidden: Admin access required' },
      { status: 403 }
    )
  }

  return null // Authorized
}
