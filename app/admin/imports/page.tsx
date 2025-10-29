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
