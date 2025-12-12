'use client'

import React, { useState, useEffect } from 'react'

interface ExportJob {
  id: string
  export_type: 'vat_entities' | 'all_entities'
  status: 'pending' | 'running' | 'completed' | 'failed'
  started_at: string | null
  completed_at: string | null
  records_exported: number
  table_name: string | null
  expires_at: string | null
  error_message: string | null
  created_at: string
}

export default function ExportsPage() {
  const [jobs, setJobs] = useState<ExportJob[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [generating, setGenerating] = useState(false)
  const [generateError, setGenerateError] = useState<string | null>(null)

  // Pagination state
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(0)
  const [total, setTotal] = useState(0)
  const pageSize = 25

  // CLI command modal state
  const [showCliModal, setShowCliModal] = useState(false)
  const [selectedJob, setSelectedJob] = useState<ExportJob | null>(null)

  // Delete state
  const [deletingJobId, setDeletingJobId] = useState<string | null>(null)
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null)

  // Prevent double-fetch in React Strict Mode
  const hasFetchedRef = React.useRef(false)

  // Fetch export jobs from API
  const fetchJobs = (page: number) => {
    setLoading(true)
    setError(null)

    fetch(`/api/export-jobs?page=${page}&limit=${pageSize}`)
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
        console.error('Failed to load export jobs:', err)
        setError(err.message || 'Failed to load export jobs')
      })
      .finally(() => {
        setLoading(false)
      })
  }

  useEffect(() => {
    // Prevent double-fetch in React Strict Mode
    if (hasFetchedRef.current) return
    hasFetchedRef.current = true

    fetchJobs(1)
  }, [])

  const handlePageChange = (newPage: number) => {
    fetchJobs(newPage)
  }

  const handleGenerateExport = async () => {
    setGenerating(true)
    setGenerateError(null)

    try {
      const response = await fetch('/api/admin/exports/vat-entities', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ workerType: 'web_manual' }),
      })

      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || data.details || 'Export generation failed')
      }

      // Refresh job list
      fetchJobs(1)
    } catch (err) {
      setGenerateError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setGenerating(false)
    }
  }

  const handleDownload = (jobId: string) => {
    window.location.href = `/api/admin/exports/${jobId}/download`
  }

  const handleShowCLI = (job: ExportJob) => {
    setSelectedJob(job)
    setShowCliModal(true)
  }

  const handleDeleteClick = (jobId: string) => {
    setConfirmDeleteId(jobId)
  }

  const handleDeleteCancel = () => {
    setConfirmDeleteId(null)
  }

  const handleDeleteConfirm = async (jobId: string) => {
    setConfirmDeleteId(null)
    setDeletingJobId(jobId)

    try {
      const response = await fetch(`/api/admin/exports/${jobId}`, {
        method: 'DELETE',
      })

      if (!response.ok) {
        const data = await response.json()
        throw new Error(data.error || 'Delete failed')
      }

      // Refresh job list
      fetchJobs(currentPage)
    } catch (err) {
      console.error('Delete failed:', err)
      alert(err instanceof Error ? err.message : 'Delete failed')
    } finally {
      setDeletingJobId(null)
    }
  }

  const getStatusBadge = (status: ExportJob['status']) => {
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

  const formatDuration = (start: string | null, end: string | null) => {
    if (!start || !end) return '-'
    const duration = new Date(end).getTime() - new Date(start).getTime()
    const minutes = Math.floor(duration / 60000)
    const seconds = Math.floor((duration % 60000) / 1000)
    return `${minutes}m ${seconds}s`
  }

  const formatExpiry = (expiresAt: string | null) => {
    if (!expiresAt) return '-'
    const expiry = new Date(expiresAt)
    const now = new Date()
    const diffMs = expiry.getTime() - now.getTime()
    const diffHours = Math.floor(diffMs / (1000 * 60 * 60))

    if (diffHours < 0) return 'Expired'
    if (diffHours < 1) return '<1 hour'
    return `${diffHours} hours`
  }

  return (
    <div>
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold">Export Jobs</h1>
      </div>

      {/* Create New Export Section */}
      <div className="bg-white rounded-lg border p-6 mb-6">
        <div className="mb-4">
          <h2 className="text-xl font-semibold">Create New Export</h2>
          <p className="text-gray-600 text-sm mt-1">
            Export all active entities with activity group flags (ag_001 through ag_007)
          </p>
        </div>

        {generateError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
            <p className="text-red-800 font-medium">Export failed</p>
            <p className="text-red-700 text-sm mt-1">{generateError}</p>
          </div>
        )}

        <button
          onClick={handleGenerateExport}
          disabled={generating}
          className="bg-blue-600 text-white px-6 py-3 rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed font-medium flex items-center gap-2"
        >
          {generating ? (
            <>
              <svg className="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
              </svg>
              Generating Export...
            </>
          ) : (
            <>
              <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                <path d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
              </svg>
              Generate All Entities Export
            </>
          )}
        </button>

        <p className="text-sm text-gray-500 mt-3">
          Expected output: ~2M+ entities | Estimated time: 30-90 seconds | Files expire after 24 hours
        </p>
      </div>

      {/* Export History */}
      <div className="bg-white rounded-lg border">
        <div className="border-b p-4">
          <h2 className="text-lg font-semibold">Export History</h2>
          <p className="text-sm text-gray-600 mt-1">
            Track all export jobs and download results ({total.toLocaleString()} total)
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
            <div className="animate-pulse">Loading export jobs...</div>
          </div>
        ) : jobs.length > 0 ? (
          <>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b">
                  <tr>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Status</th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Created At</th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Duration</th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Records</th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Table Name</th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Expires In</th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((job) => (
                    <tr key={job.id} className="border-b hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm">{getStatusBadge(job.status)}</td>
                      <td className="px-4 py-3 text-sm">
                        {job.created_at ? new Date(job.created_at).toLocaleString() : '-'}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {formatDuration(job.started_at, job.completed_at)}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {job.records_exported.toLocaleString()}
                      </td>
                      <td className="px-4 py-3 text-sm font-mono text-xs">
                        {job.table_name || '-'}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        {job.status === 'completed' ? formatExpiry(job.expires_at) : '-'}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        <div className="flex gap-2">
                          {job.status === 'completed' && (
                            <>
                              <button
                                onClick={() => handleDownload(job.id)}
                                className="text-blue-600 hover:text-blue-800 text-xs font-medium flex items-center gap-1"
                                title="Download CSV"
                              >
                                <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                                  <path d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                                </svg>
                                Download
                              </button>
                              <button
                                onClick={() => handleShowCLI(job)}
                                className="text-green-600 hover:text-green-800 text-xs font-medium flex items-center gap-1"
                                title="Show CLI command"
                              >
                                <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                                  <path d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
                                </svg>
                                CLI
                              </button>
                            </>
                          )}
                          {job.status === 'failed' && job.error_message && (
                            <span className="text-red-600 text-xs" title={job.error_message}>
                              Error
                            </span>
                          )}
                          {confirmDeleteId === job.id ? (
                            <div className="flex items-center gap-1">
                              <button
                                onClick={() => handleDeleteConfirm(job.id)}
                                className="text-white bg-red-600 hover:bg-red-700 text-xs font-medium px-2 py-0.5 rounded"
                              >
                                Confirm
                              </button>
                              <button
                                onClick={handleDeleteCancel}
                                className="text-gray-600 hover:text-gray-800 text-xs font-medium px-2 py-0.5 border rounded hover:bg-gray-100"
                              >
                                Cancel
                              </button>
                            </div>
                          ) : deletingJobId === job.id ? (
                            <span className="text-red-600 text-xs font-medium flex items-center gap-1">
                              <svg className="animate-spin w-4 h-4" fill="none" viewBox="0 0 24 24">
                                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                              </svg>
                              Deleting...
                            </span>
                          ) : (
                            <button
                              onClick={() => handleDeleteClick(job.id)}
                              disabled={job.status === 'running'}
                              className="text-red-600 hover:text-red-800 text-xs font-medium flex items-center gap-1 disabled:opacity-50 disabled:cursor-not-allowed"
                              title="Delete export"
                            >
                              <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                                <path d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                              </svg>
                              Delete
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
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

                  {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
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
          <div className="p-8 text-center text-gray-500">No export jobs found</div>
        )}
      </div>

      {/* CLI Command Modal */}
      {showCliModal && selectedJob && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg max-w-3xl w-full p-6">
            <div className="flex justify-between items-start mb-4">
              <h3 className="text-lg font-semibold">DuckDB CLI Command</h3>
              <button
                onClick={() => setShowCliModal(false)}
                className="text-gray-400 hover:text-gray-600"
              >
                <svg className="w-6 h-6" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                  <path d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>

            <p className="text-sm text-gray-600 mb-4">
              Use this command to download the export directly from MotherDuck using DuckDB CLI (faster for large files):
            </p>

            <div className="bg-gray-900 text-gray-100 p-4 rounded-lg font-mono text-sm overflow-x-auto mb-4">
              <code>
                {`duckdb -c "COPY (SELECT * FROM md:${process.env.NEXT_PUBLIC_MOTHERDUCK_DATABASE || 'newagekbo'}.${selectedJob.table_name}) TO 'vat-entities.csv' (FORMAT CSV, HEADER)"`}
              </code>
            </div>

            <div className="flex justify-end gap-3">
              <button
                onClick={() => {
                  navigator.clipboard.writeText(
                    `duckdb -c "COPY (SELECT * FROM md:${process.env.NEXT_PUBLIC_MOTHERDUCK_DATABASE || 'newagekbo'}.${selectedJob.table_name}) TO 'vat-entities.csv' (FORMAT CSV, HEADER)"`
                  )
                }}
                className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700 flex items-center gap-2"
              >
                <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                  <path d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3" />
                </svg>
                Copy to Clipboard
              </button>
              <button
                onClick={() => setShowCliModal(false)}
                className="border border-gray-300 px-4 py-2 rounded hover:bg-gray-50"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
