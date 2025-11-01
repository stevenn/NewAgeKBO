import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { finalizeImport } from '@/lib/import/batched-update'

/**
 * POST /api/admin/imports/[jobId]/finalize
 *
 * Finalizes an import job after all batches are completed.
 * Resolves primary names for enterprises and cleans up staging data.
 *
 * Response: FinalizeResult with success status and counts
 */
export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ jobId: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const { jobId } = await params

    // Finalize the import
    console.log(`[API] Finalizing import job ${jobId}`)
    const result = await finalizeImport(jobId)

    return NextResponse.json(result)

  } catch (error) {
    console.error('[API] Failed to finalize import:', error)

    if (error instanceof Error) {
      return NextResponse.json(
        { error: error.message },
        { status: 500 }
      )
    }

    return NextResponse.json(
      { error: 'Failed to finalize import' },
      { status: 500 }
    )
  }
}
