'use client'

import React, { useState, useEffect } from 'react'
import { ExpandableJobDetails } from './components/ExpandableJobDetails'

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

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(0)
  const [total, setTotal] = useState(0)
  const pageSize = 25

  // Expanded jobs state
  const [expandedJobIds, setExpandedJobIds] = useState<Set<string>>(new Set())

  const [triggerLoading, setTriggerLoading] = useState(false)
  const [triggerError, setTriggerError] = useState<string | null>(null)
  const [triggerSuccess, setTriggerSuccess] = useState(false)
  const [importUrl, setImportUrl] = useState('')

  const toggleJobExpansion = (jobId: string) => {
    setExpandedJobIds(prev => {
      const newSet = new Set(prev)
      if (newSet.has(jobId)) {
        newSet.delete(jobId)
      } else {
        newSet.add(jobId)
      }
      return newSet
    })
  }

  // Fetch import jobs from API
  const fetchJobs = (page: number) => {
    setLoading(true)
    setError(null)

    fetch(`/api/import-jobs?page=${page}&limit=${pageSize}`)
      .then((res) => res.json())
      .then((data) => {
        if (data.error) {
          throw new Error(data.error)
        }
        setJobs(data.jobs)
        setTotal(data.total)
        setCurrentPage(data.page)
        setTotalPages(data.totalPages)
      })
      .catch((err) => {
        console.error('Failed to load import jobs:', err)
        setError(err.message || 'Failed to load import jobs')
      })
      .finally(() => {
        setLoading(false)
      })
  }

  useEffect(() => {
    fetchJobs(1)
  }, [])

  const handlePageChange = (newPage: number) => {
    fetchJobs(newPage)
  }

  const handleTriggerDailyUpdate = async () => {
    if (!importUrl.trim()) {
      setTriggerError('Please enter a URL or filename')
      return
    }

    setTriggerLoading(true)
    setTriggerError(null)
    setTriggerSuccess(false)

    try {
      const response = await fetch('/api/import/daily-update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          url: importUrl.trim(),
        }),
      })

      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || data.details || 'Failed to trigger import')
      }

      setTriggerSuccess(true)
      setImportUrl('') // Clear the input

      // Refresh the job list after a short delay
      setTimeout(() => {
        fetchJobs(currentPage)
      }, 1000)
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
        <h2 className="text-xl font-semibold mb-4">Import Daily Update</h2>
        <p className="text-gray-600 mb-4">
          Import a daily update file from the KBO Open Data service. Enter the full URL or just the filename.
        </p>

        {triggerError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
            <p className="text-red-800 font-medium">Error</p>
            <p className="text-red-700 text-sm mt-1">{triggerError}</p>
          </div>
        )}

        {triggerSuccess && (
          <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-4">
            <p className="text-green-800 font-medium">Success!</p>
            <p className="text-green-700 text-sm mt-1">
              Daily update import completed successfully. The job has been added to the history below.
            </p>
          </div>
        )}

        <div className="space-y-4">
          <div>
            <label htmlFor="importUrl" className="block text-sm font-medium text-gray-700 mb-2">
              File URL or Filename
            </label>
            <input
              id="importUrl"
              type="text"
              value={importUrl}
              onChange={(e) => setImportUrl(e.target.value)}
              placeholder="https://kbopub.economie.fgov.be/.../KboOpenData_0141_2025_10_06_Update.zip"
              disabled={triggerLoading}
              className="w-full px-4 py-2 border rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 disabled:bg-gray-100 disabled:cursor-not-allowed font-mono text-sm"
              onKeyDown={(e) => {
                if (e.key === 'Enter' && !triggerLoading) {
                  handleTriggerDailyUpdate()
                }
              }}
            />
            <p className="text-xs text-gray-500 mt-1">
              Examples: <span className="font-mono">KboOpenData_0141_2025_10_06_Update.zip</span> or full URL
            </p>
          </div>

          <button
            onClick={handleTriggerDailyUpdate}
            disabled={triggerLoading || !importUrl.trim()}
            className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {triggerLoading ? (
              <>
                <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                <span>Importing...</span>
              </>
            ) : (
              <>
                <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                  <path d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                </svg>
                <span>Import Daily Update</span>
              </>
            )}
          </button>
        </div>
      </div>

      {/* Import Jobs History */}
      <div className="bg-white rounded-lg border">
        <div className="border-b p-4">
          <h2 className="text-lg font-semibold">Import History</h2>
          <p className="text-sm text-gray-600 mt-1">
            Track all import jobs and their status ({total.toLocaleString()} total)
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
          <>
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
                <tbody>
                  {jobs.map((job) => (
                    <React.Fragment key={job.id}>
                      <tr
                        className={`border-b transition-colors ${
                          job.status === 'completed'
                            ? 'cursor-pointer hover:bg-gray-50'
                            : 'bg-gray-50'
                        }`}
                        onClick={() => job.status === 'completed' && toggleJobExpansion(job.id)}
                      >
                        <td className="px-4 py-3 text-sm" style={{ width: '12%' }}>
                          {getTypeBadge(job.type)}
                        </td>
                        <td className="px-4 py-3 text-sm" style={{ width: '12%' }}>
                          {getStatusBadge(job.status)}
                        </td>
                        <td className="px-4 py-3 text-sm font-mono" style={{ width: '15%' }}>
                          {job.snapshotDate}
                        </td>
                        <td className="px-4 py-3 text-sm" style={{ width: '10%' }}>
                          {job.extractNumber}
                        </td>
                        <td className="px-4 py-3 text-sm" style={{ width: '20%' }}>
                          {job.startedAt
                            ? new Date(job.startedAt).toLocaleString()
                            : '-'}
                        </td>
                        <td className="px-4 py-3 text-sm" style={{ width: '12%' }}>
                          {formatDuration(job.startedAt, job.completedAt)}
                        </td>
                        <td className="px-4 py-3 text-sm" style={{ width: '15%' }}>
                          {job.recordsProcessed.toLocaleString()}
                        </td>
                      </tr>
                      {job.status === 'completed' && (
                        <tr>
                          <td colSpan={7} className="p-0">
                            <ExpandableJobDetails
                              jobId={job.id}
                              extractNumber={job.extractNumber}
                              isExpanded={expandedJobIds.has(job.id)}
                            />
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination Controls */}
            {totalPages > 1 && (
              <div className="border-t p-4 flex items-center justify-between">
                <div className="text-sm text-gray-600">
                  Page {currentPage} of {totalPages}
                </div>

                <div className="flex gap-2">
                  <button
                    onClick={() => handlePageChange(currentPage - 1)}
                    disabled={currentPage === 1 || loading}
                    className="px-4 py-2 text-sm border rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Previous
                  </button>

                  {/* Page numbers */}
                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                    // Show 5 pages centered around current page
                    let pageNum
                    if (totalPages <= 5) {
                      pageNum = i + 1
                    } else if (currentPage <= 3) {
                      pageNum = i + 1
                    } else if (currentPage >= totalPages - 2) {
                      pageNum = totalPages - 4 + i
                    } else {
                      pageNum = currentPage - 2 + i
                    }

                    return (
                      <button
                        key={pageNum}
                        onClick={() => handlePageChange(pageNum)}
                        disabled={loading}
                        className={`px-4 py-2 text-sm border rounded hover:bg-gray-50 disabled:opacity-50 ${
                          pageNum === currentPage
                            ? 'bg-blue-600 text-white hover:bg-blue-700'
                            : ''
                        }`}
                      >
                        {pageNum}
                      </button>
                    )
                  })}

                  <button
                    onClick={() => handlePageChange(currentPage + 1)}
                    disabled={currentPage === totalPages || loading}
                    className="px-4 py-2 text-sm border rounded hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </>
        ) : (
          <div className="p-8 text-center text-gray-500">
            No import jobs found
          </div>
        )}
      </div>
    </div>
  )
}
