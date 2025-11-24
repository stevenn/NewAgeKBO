import { NextResponse } from 'next/server'
import { currentUser } from '@clerk/nextjs/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { exportVatEntities } from '@/lib/export/vat-entities'
import type { WorkerType } from '@/lib/export/types'

/**
 * POST /api/admin/exports/vat-entities
 * Trigger export of VAT-liable entities to MotherDuck table
 */
export async function POST(request: Request) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Get user ID for tracking
    const user = await currentUser()
    const userId = user?.id

    // Parse request body (optional worker type)
    const body = await request.json().catch(() => ({}))
    const workerType: WorkerType = body.workerType || 'web_manual'

    console.log(`📤 Starting VAT entities export (worker: ${workerType}, user: ${userId})`)

    // Execute export
    const result = await exportVatEntities(workerType, userId)

    console.log(`✅ Export completed: ${result.records_exported.toLocaleString()} records`)

    return NextResponse.json({
      success: true,
      ...result,
    })
  } catch (error: unknown) {
    console.error('❌ Export failed:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Export failed',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
