'use client'

import React, { useState, useEffect, use } from 'react'
import Link from 'next/link'

interface PreparationProgress {
  job_status?: string
  staging_counts?: Record<string, number>
  extract_number?: number
  snapshot_date?: string
}

interface WorkflowProgress {
  workflow_id: string
  status: 'pending' | 'downloading' | 'preparing' | 'processing' | 'finalizing' | 'completed' | 'failed'
  job_id?: string
  completed_batches: number
  total_batches: number
  current_table?: string
  current_batch?: number
  error?: string
  preparation?: PreparationProgress
}

export default function WorkflowStatusPage({
  params,
}: {
  params: Promise<{ workflowId: string }>
}) {
  const { workflowId } = use(params)
  const [progress, setProgress] = useState<WorkflowProgress | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const fetchProgress = async () => {
    try {
      const response = await fetch(`/api/admin/workflows/${workflowId}/status`)
      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || 'Failed to fetch progress')
      }

      setProgress(data)
      setError(null)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchProgress()

    // Poll every 2 seconds while workflow is running
    const interval = setInterval(() => {
      if (progress?.status !== 'completed' && progress?.status !== 'failed') {
        fetchProgress()
      }
    }, 2000)

    return () => clearInterval(interval)
  }, [workflowId, progress?.status])

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800'
      case 'failed':
        return 'bg-red-100 text-red-800'
      case 'processing':
        return 'bg-blue-100 text-blue-800'
      default:
        return 'bg-yellow-100 text-yellow-800'
    }
  }

  const getStatusLabel = (status: string) => {
    switch (status) {
      case 'downloading':
        return 'Downloading ZIP...'
      case 'preparing':
        return 'Preparing import...'
      case 'processing':
        return 'Processing batches...'
      case 'finalizing':
        return 'Finalizing...'
      case 'completed':
        return 'Completed'
      case 'failed':
        return 'Failed'
      default:
        return 'Pending'
    }
  }

  const percentComplete = progress?.total_batches
    ? Math.round((progress.completed_batches / progress.total_batches) * 100)
    : 0

  return (
    <div>
      <div className="mb-6">
        <Link
          href="/admin/imports"
          className="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1"
        >
          <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
            <path d="M15 19l-7-7 7-7" />
          </svg>
          Back to Imports
        </Link>
      </div>

      <h1 className="text-3xl font-bold mb-6">Workflow Status</h1>

      <div className="bg-white rounded-lg border p-6">
        <div className="mb-6">
          <p className="text-sm text-gray-600 mb-1">Workflow ID</p>
          <p className="font-mono text-lg">{workflowId}</p>
        </div>

        {loading && !progress ? (
          <div className="text-center py-8">
            <svg className="animate-spin h-8 w-8 mx-auto text-blue-600" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
            <p className="text-gray-600 mt-2">Loading workflow status...</p>
          </div>
        ) : error ? (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <p className="text-red-800 font-medium">Error</p>
            <p className="text-red-700 text-sm mt-1">{error}</p>
          </div>
        ) : progress ? (
          <div className="space-y-6">
            {/* Status Badge */}
            <div>
              <p className="text-sm text-gray-600 mb-2">Status</p>
              <span className={`inline-block px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(progress.status)}`}>
                {getStatusLabel(progress.status)}
              </span>
            </div>

            {/* Preparation Progress */}
            {progress.status === 'preparing' && progress.preparation && (
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <svg className="animate-spin h-4 w-4 text-blue-600" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  <span className="text-blue-800 font-medium">Preparing Import</span>
                </div>

                {progress.preparation.extract_number && (
                  <p className="text-sm text-blue-700 mb-2">
                    Extract #{progress.preparation.extract_number} ({progress.preparation.snapshot_date})
                  </p>
                )}

                {progress.preparation.staging_counts && Object.keys(progress.preparation.staging_counts).length > 0 ? (
                  <div>
                    <p className="text-sm text-blue-700 mb-2">Staging tables populated:</p>
                    <div className="grid grid-cols-2 gap-2">
                      {Object.entries(progress.preparation.staging_counts).map(([table, count]) => (
                        <div key={table} className="flex justify-between text-sm bg-white rounded px-2 py-1">
                          <span className="text-gray-600">{table}</span>
                          <span className="font-mono text-gray-800">{count.toLocaleString()}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : (
                  <p className="text-sm text-blue-600">
                    Downloading and parsing ZIP file...
                  </p>
                )}
              </div>
            )}

            {/* Progress Bar */}
            {progress.total_batches > 0 && (
              <div>
                <div className="flex justify-between text-sm text-gray-600 mb-2">
                  <span>Progress</span>
                  <span>{progress.completed_batches} / {progress.total_batches} batches ({percentComplete}%)</span>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-4">
                  <div
                    className="bg-blue-600 h-4 rounded-full transition-all duration-300"
                    style={{ width: `${percentComplete}%` }}
                  />
                </div>
              </div>
            )}

            {/* Current Activity */}
            {progress.current_table && (
              <div>
                <p className="text-sm text-gray-600 mb-1">Currently Processing</p>
                <p className="font-mono">
                  {progress.current_table} - Batch {progress.current_batch}
                </p>
              </div>
            )}

            {/* Job ID */}
            {progress.job_id && (
              <div>
                <p className="text-sm text-gray-600 mb-1">Database Job ID</p>
                <p className="font-mono text-sm">{progress.job_id}</p>
              </div>
            )}

            {/* Error Message */}
            {progress.error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-4">
                <p className="text-red-800 font-medium">Error</p>
                <p className="text-red-700 text-sm mt-1">{progress.error}</p>
              </div>
            )}

            {/* Success Message */}
            {progress.status === 'completed' && (
              <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                <p className="text-green-800 font-medium">Import Completed Successfully</p>
                <p className="text-green-700 text-sm mt-1">
                  All {progress.total_batches} batches have been processed.
                </p>
                {progress.job_id && (
                  <Link
                    href={`/admin/imports/${progress.job_id}/progress`}
                    className="text-green-700 hover:text-green-900 text-sm underline mt-2 inline-block"
                  >
                    View detailed job progress →
                  </Link>
                )}
              </div>
            )}
          </div>
        ) : null}
      </div>
    </div>
  )
}
