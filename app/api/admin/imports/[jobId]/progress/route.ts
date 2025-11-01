import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { getImportProgress } from '@/lib/import/batched-update'

/**
 * GET /api/admin/imports/[jobId]/progress
 *
 * Gets the current progress status for an import job.
 * Returns batch completion status, overall progress percentage, and next batch info.
 *
 * Response: ImportProgress with job status, batch counts, and progress details
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ jobId: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const { jobId } = await params

    // Get the import progress
    const progress = await getImportProgress(jobId)

    return NextResponse.json(progress)

  } catch (error) {
    console.error('[API] Failed to get import progress:', error)

    if (error instanceof Error) {
      return NextResponse.json(
        { error: error.message },
        { status: 500 }
      )
    }

    return NextResponse.json(
      { error: 'Failed to get import progress' },
      { status: 500 }
    )
  }
}
