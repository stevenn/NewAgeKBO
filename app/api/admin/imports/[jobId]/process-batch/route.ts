import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { processBatch } from '@/lib/import/batched-update'

/**
 * POST /api/admin/imports/[jobId]/process-batch
 *
 * Processes a single batch for an import job.
 * If no specific batch is provided, processes the next pending batch.
 *
 * Query params (optional):
 * - table: specific table name to process
 * - batch: specific batch number to process
 *
 * Response: ProcessBatchResult with batch details and progress
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

    // Get optional query parameters
    const searchParams = request.nextUrl.searchParams
    const tableName = searchParams.get('table') || undefined
    const batchNumber = searchParams.get('batch')
      ? parseInt(searchParams.get('batch')!, 10)
      : undefined

    // Validate batch number if provided
    if (batchNumber !== undefined && (isNaN(batchNumber) || batchNumber < 1)) {
      return NextResponse.json(
        { error: 'Invalid batch number' },
        { status: 400 }
      )
    }

    // Process the batch
    console.log(
      `[API] Processing batch for job ${jobId}`,
      tableName || batchNumber
        ? { tableName, batchNumber }
        : { mode: 'auto (next pending)' }
    )
    const result = await processBatch(jobId, tableName, batchNumber)

    return NextResponse.json(result)

  } catch (error) {
    console.error('[API] Failed to process batch:', error)

    if (error instanceof Error) {
      return NextResponse.json(
        { error: error.message },
        { status: 500 }
      )
    }

    return NextResponse.json(
      { error: 'Failed to process batch' },
      { status: 500 }
    )
  }
}
