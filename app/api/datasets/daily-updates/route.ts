import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import { listDailyUpdates } from '@/lib/kbo-client'

/**
 * GET /api/datasets/daily-updates
 * List available daily update files from KBO portal with import status
 */
export async function GET() {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Fetch available files from KBO portal
    let files = await listDailyUpdates()

    // Connect to Motherduck to get list of already imported extracts
    const connection = await connectMotherduck()

    try {
      // Get all imported extract numbers
      const importedExtracts = await executeQuery<{ extract_number: number }>(
        connection,
        `SELECT DISTINCT extract_number FROM import_jobs WHERE extract_type = 'update' ORDER BY extract_number DESC`
      )

      const importedSet = new Set(importedExtracts.map(r => r.extract_number))

      // Mark files as imported if they're in the database
      files = files.map(file => ({
        ...file,
        imported: importedSet.has(file.extract_number)
      }))

      return NextResponse.json({
        files,
        imported_count: importedSet.size,
        total_count: files.length
      })
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error: unknown) {
    console.error('Failed to fetch daily updates:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Failed to fetch daily updates',
        details: errorMessage
      },
      { status: 500 }
    )
  }
}
