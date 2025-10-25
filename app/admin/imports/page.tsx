'use client'

import { useState, useEffect } from 'react'

interface ImportJob {
  id: string
  type: 'daily' | 'monthly'
  status: 'pending' | 'running' | 'completed' | 'failed'
  startedAt: string | null
  completedAt: string | null
  extractNumber: number
  snapshotDate: string
  recordsProcessed: number
  errorMessage: string | null
}

export default function ImportsPage() {
  const [jobs, setJobs] = useState<ImportJob[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [triggerLoading, setTriggerLoading] = useState(false)
  const [triggerError, setTriggerError] = useState<string | null>(null)
  const [triggerSuccess, setTriggerSuccess] = useState(false)

  // Fetch import jobs from API
  useEffect(() => {
    fetch('/api/import-jobs')
      .then((res) => res.json())
      .then((data) => {
        if (data.error) {
          throw new Error(data.error)
        }
        setJobs(data.jobs)
      })
      .catch((err) => {
        console.error('Failed to load import jobs:', err)
        setError(err.message || 'Failed to load import jobs')
      })
      .finally(() => {
        setLoading(false)
      })
  }, [])

  const handleTriggerDailyUpdate = async () => {
    setTriggerLoading(true)
    setTriggerError(null)
    setTriggerSuccess(false)

    try {
      // This will be implemented to call the API route
      // For now, show a message that it's not yet implemented
      await new Promise((resolve) => setTimeout(resolve, 1000))
      throw new Error('Manual trigger not yet implemented - use CLI scripts for now')
    } catch (err) {
      setTriggerError(err instanceof Error ? err.message : 'Failed to trigger import')
    } finally {
      setTriggerLoading(false)
    }
  }

  const getStatusBadge = (status: ImportJob['status']) => {
    const styles = {
      pending: 'bg-gray-100 text-gray-800',
      running: 'bg-blue-100 text-blue-800',
      completed: 'bg-green-100 text-green-800',
      failed: 'bg-red-100 text-red-800',
    }

    return (
      <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${styles[status]}`}>
        {status.charAt(0).toUpperCase() + status.slice(1)}
      </span>
    )
  }

  const getTypeBadge = (type: ImportJob['type']) => {
    const styles = {
      daily: 'bg-blue-100 text-blue-800',
      monthly: 'bg-orange-100 text-orange-800',
    }

    return (
      <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${styles[type]}`}>
        {type.charAt(0).toUpperCase() + type.slice(1)}
      </span>
    )
  }

  const formatDuration = (start: string | null, end: string | null) => {
    if (!start || !end) return '-'
    const duration = new Date(end).getTime() - new Date(start).getTime()
    const minutes = Math.floor(duration / 60000)
    const seconds = Math.floor((duration % 60000) / 1000)
    return `${minutes}m ${seconds}s`
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold">Import Management</h1>
      </div>

      {/* Manual Trigger Section */}
      <div className="bg-white rounded-lg border p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4">Manual Import Trigger</h2>
        <p className="text-gray-600 mb-4">
          Manually trigger a daily update import. This will download the latest daily update file
          from the KBO Open Data service and apply it to the database.
        </p>

        {triggerError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
            <p className="text-red-800">{triggerError}</p>
          </div>
        )}

        {triggerSuccess && (
          <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-4">
            <p className="text-green-800">Daily update import triggered successfully!</p>
          </div>
        )}

        <div className="flex gap-4">
          <button
            onClick={handleTriggerDailyUpdate}
            disabled={triggerLoading}
            className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
          >
            {triggerLoading ? 'Triggering...' : 'Trigger Daily Update'}
          </button>
          <div className="flex items-center text-sm text-gray-600">
            <svg
              className="w-4 h-4 mr-2"
              fill="none"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="2"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            For now, use CLI scripts to run imports
          </div>
        </div>
      </div>

      {/* Import Jobs History */}
      <div className="bg-white rounded-lg border">
        <div className="border-b p-4">
          <h2 className="text-lg font-semibold">Import History</h2>
          <p className="text-sm text-gray-600 mt-1">
            Track all import jobs and their status
          </p>
        </div>

        {error && (
          <div className="p-6">
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="text-red-800">{error}</p>
            </div>
          </div>
        )}

        {loading ? (
          <div className="p-8 text-center text-gray-500">
            <div className="animate-pulse">Loading import jobs...</div>
          </div>
        ) : jobs.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-gray-50 border-b">
                <tr>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Type
                  </th>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Status
                  </th>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Snapshot Date
                  </th>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Extract #
                  </th>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Started At
                  </th>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Duration
                  </th>
                  <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                    Records
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {jobs.map((job) => (
                  <tr key={job.id} className="hover:bg-gray-50">
                    <td className="px-4 py-3 text-sm">{getTypeBadge(job.type)}</td>
                    <td className="px-4 py-3 text-sm">{getStatusBadge(job.status)}</td>
                    <td className="px-4 py-3 text-sm font-mono">
                      {job.snapshotDate}
                    </td>
                    <td className="px-4 py-3 text-sm">{job.extractNumber}</td>
                    <td className="px-4 py-3 text-sm">
                      {job.startedAt
                        ? new Date(job.startedAt).toLocaleString()
                        : '-'}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {formatDuration(job.startedAt, job.completedAt)}
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {job.recordsProcessed.toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="p-8 text-center text-gray-500">
            No import jobs found
          </div>
        )}
      </div>
    </div>
  )
}
