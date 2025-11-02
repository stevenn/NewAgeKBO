'use client'

import React, { useState, useEffect, use } from 'react'
import { useRouter } from 'next/navigation'

interface TableStatus {
  completed: number
  total: number
  status: 'pending' | 'processing' | 'completed'
}

interface ImportProgress {
  job_id: string
  status: 'pending' | 'processing' | 'completed' | 'failed'
  overall_progress: {
    completed_batches: number
    total_batches: number
    percentage: number
  }
  tables: Record<string, TableStatus>
  current_batch: {
    table: string
    batch: number
    operation: string
  } | null
  next_batch: {
    table: string
    batch: number
    operation: string
  } | null
}

export default function ImportProgressPage({
  params,
}: {
  params: Promise<{ jobId: string }>
}) {
  const resolvedParams = use(params)
  const router = useRouter()
  const [progress, setProgress] = useState<ImportProgress | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [processing, setProcessing] = useState(false)
  const [processingError, setProcessingError] = useState<string | null>(null)
  const [autoProcess, setAutoProcess] = useState(false)
  const [finalizing, setFinalizing] = useState(false)
  const [finalizeError, setFinalizeError] = useState<string | null>(null)
  const [showFinalizeConfirm, setShowFinalizeConfirm] = useState(false)
  const [finalizeResult, setFinalizeResult] = useState<{
    success: boolean
    names_resolved: number
  } | null>(null)

  // Auto-refresh progress every 2 seconds (stop when completed)
  useEffect(() => {
    const fetchProgress = async () => {
      try {
        const response = await fetch(`/api/admin/imports/${resolvedParams.jobId}/progress`)
        if (!response.ok) {
          throw new Error('Failed to fetch progress')
        }
        const data = await response.json()
        setProgress(data)
        setError(null)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Unknown error')
      } finally {
        setLoading(false)
      }
    }

    fetchProgress()

    // Stop polling if job is completed
    if (progress?.status === 'completed') {
      return
    }

    const interval = setInterval(fetchProgress, 2000)
    return () => clearInterval(interval)
  }, [resolvedParams.jobId, progress?.status])

  // Auto-process batches when enabled
  useEffect(() => {
    if (!autoProcess || !progress || processing) return
    if (progress.overall_progress.percentage >= 100 || progress.status === 'completed') {
      setAutoProcess(false)
      return
    }

    const processBatch = async () => {
      setProcessing(true)
      try {
        const response = await fetch(
          `/api/admin/imports/${resolvedParams.jobId}/process-batch`,
          { method: 'POST' }
        )
        if (!response.ok) {
          throw new Error('Failed to process batch')
        }
        // Wait a bit before the next batch to let progress update
        await new Promise(resolve => setTimeout(resolve, 500))
      } catch (err) {
        console.error('Batch processing error:', err)
        setAutoProcess(false)
      } finally {
        setProcessing(false)
      }
    }

    processBatch()
  }, [autoProcess, progress, resolvedParams.jobId])

  const handleProcessBatch = async () => {
    setProcessing(true)
    setProcessingError(null)
    try {
      const response = await fetch(
        `/api/admin/imports/${resolvedParams.jobId}/process-batch`,
        { method: 'POST' }
      )
      if (!response.ok) {
        throw new Error('Failed to process batch')
      }
    } catch (err) {
      setProcessingError(err instanceof Error ? err.message : 'Failed to process batch')
    } finally {
      setProcessing(false)
    }
  }

  const handleFinalizeClick = () => {
    setShowFinalizeConfirm(true)
  }

  const handleFinalizeConfirm = async () => {
    setShowFinalizeConfirm(false)
    setFinalizing(true)
    setFinalizeError(null)
    try {
      const response = await fetch(
        `/api/admin/imports/${resolvedParams.jobId}/finalize`,
        { method: 'POST' }
      )
      if (!response.ok) {
        throw new Error('Failed to finalize import')
      }
      const data = await response.json()
      setFinalizeResult(data)
    } catch (err) {
      setFinalizeError(err instanceof Error ? err.message : 'Failed to finalize import')
    } finally {
      setFinalizing(false)
    }
  }

  const handleFinalizeCancel = () => {
    setShowFinalizeConfirm(false)
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <svg className="animate-spin h-12 w-12 mx-auto text-blue-600" fill="none" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
          </svg>
          <p className="text-gray-600 mt-4">Loading progress...</p>
        </div>
      </div>
    )
  }

  if (error || !progress) {
    return (
      <div className="max-w-4xl mx-auto p-6">
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <h2 className="text-red-800 font-bold text-lg">Error</h2>
          <p className="text-red-700 mt-2">{error || 'Failed to load progress'}</p>
          <button
            type="button"
            onClick={() => router.push('/admin/imports')}
            className="mt-4 text-red-700 hover:text-red-900 underline"
          >
            ← Back to Imports
          </button>
        </div>
      </div>
    )
  }

  const isComplete = progress.overall_progress.percentage >= 100
  const canFinalize = isComplete && progress.status !== 'completed'
  const isCompleted = progress.status === 'completed'

  return (
    <div className="max-w-5xl mx-auto p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <button
            type="button"
            onClick={() => router.push('/admin/imports')}
            className="text-blue-600 hover:text-blue-800 text-sm mb-2 flex items-center gap-1"
          >
            <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
              <path d="M15 19l-7-7 7-7" />
            </svg>
            Back to Imports
          </button>
          <h1 className="text-3xl font-bold">Import Progress</h1>
          <p className="text-gray-600 text-sm mt-1 font-mono">Job ID: {resolvedParams.jobId}</p>
        </div>
        <div className="text-right">
          <div className={`inline-block px-3 py-1 rounded text-sm font-medium ${
            progress.status === 'completed' ? 'bg-green-100 text-green-800' :
            progress.status === 'failed' ? 'bg-red-100 text-red-800' :
            progress.status === 'processing' ? 'bg-blue-100 text-blue-800' :
            'bg-gray-100 text-gray-800'
          }`}>
            {progress.status.charAt(0).toUpperCase() + progress.status.slice(1)}
          </div>
        </div>
      </div>

      {/* Overall Progress */}
      <div className="bg-white rounded-lg border p-6 mb-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold">Overall Progress</h2>
          <span className="text-3xl font-bold text-blue-600">
            {progress.overall_progress.percentage}%
          </span>
        </div>

        {/* Progress Bar */}
        <div className="w-full bg-gray-200 rounded-full h-4 mb-4">
          <div
            className="bg-blue-600 h-4 rounded-full transition-all duration-300"
            style={{ width: `${progress.overall_progress.percentage}%` }}
          />
        </div>

        <div className="flex items-center justify-between text-sm text-gray-600">
          <span>
            {progress.overall_progress.completed_batches} / {progress.overall_progress.total_batches} batches completed
          </span>
          {progress.next_batch && (
            <span className="text-blue-600 font-medium">
              Next: {progress.next_batch.table} #{progress.next_batch.batch} ({progress.next_batch.operation})
            </span>
          )}
        </div>
      </div>

      {/* Processing Controls */}
      {!isComplete && !isCompleted && (
        <div className="bg-white rounded-lg border p-6 mb-6">
          <h2 className="text-lg font-semibold mb-4">Batch Processing</h2>
          <div className="flex gap-4">
            <button
              type="button"
              onClick={handleProcessBatch}
              disabled={processing || autoProcess || isCompleted}
              className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
            >
              {processing ? (
                <>
                  <svg className="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                  </svg>
                  Processing...
                </>
              ) : (
                <>
                  <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                    <path d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                  Process Next Batch
                </>
              )}
            </button>

            <button
              type="button"
              onClick={() => setAutoProcess(!autoProcess)}
              disabled={processing || isCompleted}
              className={`px-6 py-2 rounded-lg border font-medium flex items-center gap-2 ${
                autoProcess
                  ? 'bg-red-50 text-red-700 border-red-300 hover:bg-red-100'
                  : 'bg-green-50 text-green-700 border-green-300 hover:bg-green-100'
              }`}
            >
              {autoProcess ? (
                <>
                  <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                    <path d="M10 9v6m4-6v6m7-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  Stop Auto-Processing
                </>
              ) : (
                <>
                  <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                    <path d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z" />
                    <path d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  Auto-Process All
                </>
              )}
            </button>
          </div>
          <p className="text-sm text-gray-600 mt-3">
            {autoProcess
              ? '⚡ Auto-processing enabled - batches will process automatically'
              : 'Process batches one at a time, or enable auto-processing to complete all batches'}
          </p>
          {processingError && (
            <div className="mt-4 bg-red-50 border border-red-200 rounded p-3 text-sm text-red-700">
              <span className="font-medium">Error:</span> {processingError}
            </div>
          )}
        </div>
      )}

      {/* Finalize */}
      {canFinalize && !finalizeResult && !showFinalizeConfirm && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-6 mb-6">
          <h2 className="text-lg font-semibold text-green-800 mb-2">
            ✓ All Batches Completed!
          </h2>
          <p className="text-green-700 text-sm mb-4">
            All batches have been processed. Finalize the import to resolve primary names and clean up staging data.
          </p>
          <button
            type="button"
            onClick={handleFinalizeClick}
            disabled={finalizing}
            className="bg-green-600 text-white px-6 py-2 rounded-lg hover:bg-green-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {finalizing ? (
              <>
                <svg className="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                Finalizing...
              </>
            ) : (
              <>
                <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                  <path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                Finalize Import
              </>
            )}
          </button>
          {finalizeError && (
            <div className="mt-4 bg-red-50 border border-red-200 rounded p-3 text-sm text-red-700">
              <span className="font-medium">Error:</span> {finalizeError}
            </div>
          )}
        </div>
      )}

      {/* Finalize Confirmation */}
      {showFinalizeConfirm && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6 mb-6">
          <h2 className="text-lg font-semibold text-yellow-800 mb-2">
            ⚠️ Confirm Finalization
          </h2>
          <p className="text-yellow-700 text-sm mb-4">
            This will resolve primary names for all enterprises and clean up staging data. This action cannot be undone.
          </p>
          <div className="flex gap-3">
            <button
              type="button"
              onClick={handleFinalizeConfirm}
              className="bg-green-600 text-white px-6 py-2 rounded-lg hover:bg-green-700 flex items-center gap-2"
            >
              <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                <path d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
              Yes, Finalize Import
            </button>
            <button
              type="button"
              onClick={handleFinalizeCancel}
              className="bg-gray-200 text-gray-700 px-6 py-2 rounded-lg hover:bg-gray-300 flex items-center gap-2"
            >
              <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                <path d="M6 18L18 6M6 6l12 12" />
              </svg>
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Finalize Result */}
      {finalizeResult && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-6 mb-6">
          <h2 className="text-lg font-semibold text-green-800 mb-2">
            ✓ Import Finalized Successfully!
          </h2>
          <p className="text-green-700 text-sm">
            • Primary names resolved: {finalizeResult.names_resolved}
            <br />
            • Staging data cleaned up
          </p>
        </div>
      )}

      {/* Table Progress */}
      <div className="bg-white rounded-lg border overflow-hidden">
        <div className="border-b p-4">
          <h2 className="text-lg font-semibold">Table Progress</h2>
        </div>
        <div className="divide-y">
          {Object.entries(progress.tables).map(([tableName, status]) => {
            const percentage = status.total > 0 ? Math.round((status.completed / status.total) * 100) : 0
            return (
              <div key={tableName} className="p-4">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-3">
                    <span className="font-medium capitalize">{tableName}</span>
                    <span className={`text-xs px-2 py-0.5 rounded font-medium ${
                      status.status === 'completed' ? 'bg-green-100 text-green-800' :
                      status.status === 'processing' ? 'bg-blue-100 text-blue-800' :
                      'bg-gray-100 text-gray-800'
                    }`}>
                      {status.status}
                    </span>
                  </div>
                  <div className="text-sm text-gray-600">
                    {status.completed} / {status.total} batches ({percentage}%)
                  </div>
                </div>
                <div className="w-full bg-gray-200 rounded-full h-2">
                  <div
                    className={`h-2 rounded-full transition-all duration-300 ${
                      status.status === 'completed' ? 'bg-green-600' :
                      status.status === 'processing' ? 'bg-blue-600' :
                      'bg-gray-400'
                    }`}
                    style={{ width: `${percentage}%` }}
                  />
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
