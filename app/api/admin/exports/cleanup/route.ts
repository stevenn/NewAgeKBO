import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { cleanupExpiredExports, countExpiredExports } from '@/lib/export/cleanup'

/**
 * POST /api/admin/exports/cleanup
 * Manually trigger cleanup of expired export tables
 */
export async function POST(request: Request) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Parse request body (optional: deleteJobRecords flag)
    const body = await request.json().catch(() => ({}))
    const deleteJobRecords = body.deleteJobRecords || false

    console.log(`🧹 Starting manual export cleanup (deleteJobRecords: ${deleteJobRecords})`)

    // Execute cleanup
    const result = await cleanupExpiredExports(deleteJobRecords)

    console.log(`✅ Cleanup complete: ${result.tables_dropped} table(s) dropped`)

    return NextResponse.json({
      success: true,
      ...result,
    })
  } catch (error: unknown) {
    console.error('❌ Cleanup failed:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Cleanup failed',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}

/**
 * GET /api/admin/exports/cleanup
 * Get count of expired exports waiting for cleanup
 */
export async function GET() {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const count = await countExpiredExports()

    return NextResponse.json({
      expired_count: count,
    })
  } catch (error: unknown) {
    console.error('Failed to count expired exports:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Failed to count expired exports',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
