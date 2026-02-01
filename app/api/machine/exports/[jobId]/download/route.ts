import { NextResponse } from 'next/server'
import {
  checkMachineAccess,
  isMachineAuthError,
} from '@/lib/auth/machine-auth'
import {
  connectMotherduck,
  closeMotherduck,
  executeQuery,
  executeQueryStreaming,
} from '@/lib/motherduck'
import type { ExportJob } from '@/lib/export/types'

/**
 * GET /api/machine/exports/[jobId]/download
 * Download exported CSV file from MotherDuck table (streaming)
 * Protected by X-API-Key header authentication
 */
export async function GET(
  request: Request,
  { params }: { params: Promise<{ jobId: string }> }
) {
  try {
    // Check machine API authentication
    const authResult = await checkMachineAccess(request)
    if (isMachineAuthError(authResult)) return authResult

    const client = authResult
    const { jobId } = await params

    console.log(`🤖 Machine API: ${client.name} downloading export ${jobId}`)

    // Connect to Motherduck
    const connection = await connectMotherduck()

    try {
      // Get export job details
      const jobs = await executeQuery<ExportJob>(
        connection,
        `
          SELECT id, status, table_name, export_type, expires_at, records_exported
          FROM export_jobs
          WHERE id = '${jobId}'
        `
      )

      if (jobs.length === 0) {
        return NextResponse.json(
          { error: 'Export job not found' },
          { status: 404 }
        )
      }

      const job = jobs[0]

      // Check if export is completed
      if (job.status !== 'completed') {
        return NextResponse.json(
          { error: 'Export not completed yet', status: job.status },
          { status: 400 }
        )
      }

      // Check if table exists
      if (!job.table_name) {
        return NextResponse.json(
          { error: 'No export table associated with this job' },
          { status: 400 }
        )
      }

      // Check if export has expired
      if (job.expires_at && new Date(job.expires_at) < new Date()) {
        return NextResponse.json(
          { error: 'Export has expired and been deleted' },
          { status: 410 } // 410 Gone
        )
      }

      console.log(`📥 Streaming export from table: ${job.table_name}`)

      // Get column names from first row
      const sampleData = await executeQuery<Record<string, unknown>>(
        connection,
        `SELECT * FROM ${job.table_name} LIMIT 1`
      )

      if (sampleData.length === 0) {
        return NextResponse.json(
          { error: 'Export table is empty' },
          { status: 404 }
        )
      }

      const columns = Object.keys(sampleData[0])
      const header = columns.map((col) => `"${col}"`).join(',') + '\n'

      // Helper to convert a row to CSV line
      const rowToCsv = (row: Record<string, unknown>): string => {
        return columns
          .map((col) => {
            const value = row[col]
            if (value === null || value === undefined) return '""'
            const str = String(value)
            // Escape quotes by doubling them
            const escaped = str.replace(/"/g, '""')
            return `"${escaped}"`
          })
          .join(',')
      }

      // Create a streaming response
      const encoder = new TextEncoder()
      const stream = new ReadableStream({
        async start(controller) {
          try {
            // Send header first
            controller.enqueue(encoder.encode(header))

            // Stream data chunks
            const dataStream = executeQueryStreaming<Record<string, unknown>>(
              connection,
              `SELECT * FROM ${job.table_name} ORDER BY "EntityNumber"`
            )

            for await (const chunk of dataStream) {
              const csvChunk = chunk.map(rowToCsv).join('\n') + '\n'
              controller.enqueue(encoder.encode(csvChunk))
            }

            controller.close()
          } catch (error) {
            console.error('❌ Stream error:', error)
            controller.error(error)
          } finally {
            await closeMotherduck(connection)
          }
        },
      })

      // Generate filename with date
      const date = new Date().toISOString().split('T')[0]
      const filename = `kbo-vat-entities-${date}.csv`

      // Return as streaming downloadable file
      return new NextResponse(stream, {
        status: 200,
        headers: {
          'Content-Type': 'text/csv; charset=utf-8',
          'Content-Disposition': `attachment; filename="${filename}"`,
          'Transfer-Encoding': 'chunked',
        },
      })
    } catch (error) {
      await closeMotherduck(connection)
      throw error
    }
  } catch (error: unknown) {
    console.error('❌ Machine API download failed:', error)
    const errorMessage = error instanceof Error ? error.message : 'Unknown error'
    return NextResponse.json(
      {
        error: 'Download failed',
        details: errorMessage,
      },
      { status: 500 }
    )
  }
}
