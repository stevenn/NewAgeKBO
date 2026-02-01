import { NextResponse } from 'next/server'
import {
  checkMachineAccess,
  isMachineAuthError,
} from '@/lib/auth/machine-auth'
import {
  connectMotherduck,
  closeMotherduck,
  executeQuery,
} from '@/lib/motherduck'
import type { ExportJob } from '@/lib/export/types'

interface ExportListItem {
  id: string
  export_type: string
  status: string
  records_exported: number
  created_at: string
  expires_at: string | null
  table_name: string | null
}

/**
 * GET /api/machine/exports
 * List available exports for machine/API access
 * Protected by X-API-Key header authentication
 */
export async function GET(request: Request) {
  try {
    // Check machine API authentication
    const authResult = await checkMachineAccess(request)
    if (isMachineAuthError(authResult)) return authResult

    const client = authResult
    console.log(`🤖 Machine API: ${client.name} listing exports`)

    // Parse query params for pagination
    const url = new URL(request.url)
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '20'), 100)
    const status = url.searchParams.get('status') || 'completed'

    // Connect to Motherduck
    const connection = await connectMotherduck()

    try {
      // Get recent export jobs
      const jobs = await executeQuery<ExportJob>(
        connection,
        `
          SELECT
            id,
            export_type,
            status,
            records_exported,
            created_at,
            expires_at,
            table_name
          FROM export_jobs
          WHERE status = '${status}'
            AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
          ORDER BY created_at DESC
          LIMIT ${limit}
        `
      )

      const exports: ExportListItem[] = jobs.map((job) => ({
        id: job.id,
        export_type: job.export_type,
        status: job.status,
        records_exported: Number(job.records_exported),
        created_at: String(job.created_at),
        expires_at: job.expires_at ? String(job.expires_at) : null,
        table_name: job.table_name,
      }))

      return NextResponse.json({
        exports,
        count: exports.length,
        client: client.clientId,
      })
    } finally {
      await closeMotherduck(connection)
    }
  } catch (error: unknown) {
    console.error('Machine API exports list failed:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Failed to list exports',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
