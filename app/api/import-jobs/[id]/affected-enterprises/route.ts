import { NextRequest, NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import { getAffectedEnterprises } from '@/lib/motherduck/import-job-analysis'

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Await params (Next.js 15 requirement)
    const { id } = await params

    // Get query parameters
    const searchParams = request.nextUrl.searchParams
    const page = parseInt(searchParams.get('page') || '1', 10)
    const limit = parseInt(searchParams.get('limit') || '50', 10)

    // Validate pagination parameters
    if (page < 1 || limit < 1 || limit > 200) {
      return NextResponse.json(
        { error: 'Invalid pagination parameters' },
        { status: 400 }
      )
    }

    const connection = await connectMotherduck()

    try {
      // First, get the import job to extract the extract_number
      const jobResults = await executeQuery<{ extract_number: number }>(
        connection,
        `SELECT extract_number FROM import_jobs WHERE id = '${id}'`
      )

      if (jobResults.length === 0) {
        return NextResponse.json(
          { error: 'Import job not found' },
          { status: 404 }
        )
      }

      const extractNumber = jobResults[0].extract_number

      // Get affected enterprises with pagination
      const result = await getAffectedEnterprises(connection, extractNumber, page, limit)

      // Convert result to JSON-serializable format (handle BigInt)
      return NextResponse.json(JSON.parse(JSON.stringify(result, (_, value) =>
        typeof value === 'bigint' ? Number(value) : value
      )))
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error) {
    console.error('Failed to fetch affected enterprises:', error)
    return NextResponse.json(
      { error: 'Failed to fetch affected enterprises' },
      { status: 500 }
    )
  }
}
