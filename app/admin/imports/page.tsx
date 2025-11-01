'use client'

import React, { useState, useEffect } from 'react'
import { useRouter } from 'next/navigation'
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
  workerType: string
}

interface AvailableFile {
  filename: string
  url: string
  extract_number: number
  snapshot_date: string
  file_type: 'full' | 'update'
  imported: boolean
}

export default function ImportsPage() {
  const router = useRouter()
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

  // Menu state for action dropdowns
  const [openMenuId, setOpenMenuId] = useState<string | null>(null)

  // Prevent double-fetch in React Strict Mode
  const hasFetchedRef = React.useRef(false)

  // Available files state
  const [availableFiles, setAvailableFiles] = useState<AvailableFile[]>([])
  const [filesLoading, setFilesLoading] = useState(false)
  const [filesError, setFilesError] = useState<string | null>(null)
  const [importingFiles, setImportingFiles] = useState<Set<number>>(new Set())
  const [fileErrors, setFileErrors] = useState<Map<number, string>>(new Map())
  const [showImportedFiles, setShowImportedFiles] = useState(false)

  // Batched import state
  const [uploadFile, setUploadFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [uploadError, setUploadError] = useState<string | null>(null)
  const [uploadSuccess, setUploadSuccess] = useState<{
    jobId: string
    extractNumber: number
    totalBatches: number
  } | null>(null)

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

  // Fetch available files from KBO portal
  const fetchAvailableFiles = async () => {
    setFilesLoading(true)
    setFilesError(null)

    try {
      const response = await fetch('/api/datasets/daily-updates')
      if (!response.ok) {
        throw new Error('Failed to fetch available files')
      }
      const data = await response.json()
      setAvailableFiles(data.files || [])
    } catch (err) {
      setFilesError(err instanceof Error ? err.message : 'Failed to fetch files')
    } finally {
      setFilesLoading(false)
    }
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

  const handleBatchedImport = async () => {
    if (!uploadFile) return

    setUploading(true)
    setUploadError(null)
    setUploadSuccess(null)

    try {
      const formData = new FormData()
      formData.append('file', uploadFile)

      const response = await fetch('/api/admin/imports/prepare?workerType=vercel', {
        method: 'POST',
        body: formData,
      })

      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || 'Failed to prepare import')
      }

      setUploadSuccess({
        jobId: data.job_id,
        extractNumber: data.extract_number,
        totalBatches: data.total_batches,
      })

      // Refresh import jobs list
      setTimeout(() => fetchJobs(currentPage), 1000)
    } catch (err) {
      setUploadError(err instanceof Error ? err.message : 'Unknown error')
    } finally {
      setUploading(false)
    }
  }

  const handleBatchedImportFromUrl = async (file: AvailableFile) => {
    // Add to importing set and clear any previous errors
    setImportingFiles(prev => new Set(prev).add(file.extract_number))
    setFileErrors(prev => {
      const newMap = new Map(prev)
      newMap.delete(file.extract_number)
      return newMap
    })

    try {
      const response = await fetch('/api/admin/imports/prepare?workerType=vercel', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          url: file.url,
        }),
      })

      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || data.details || 'Failed to prepare batched import')
      }

      // Navigate directly to progress page
      router.push(`/admin/imports/${data.job_id}/progress`)
    } catch (err) {
      // Set error state for this file
      setFileErrors(prev => {
        const newMap = new Map(prev)
        newMap.set(file.extract_number, err instanceof Error ? err.message : 'Unknown error')
        return newMap
      })
    } finally {
      // Remove from importing set
      setImportingFiles(prev => {
        const newSet = new Set(prev)
        newSet.delete(file.extract_number)
        return newSet
      })
    }
  }

  const handleImportFile = async (file: AvailableFile) => {
    // Add to importing set and clear any previous errors
    setImportingFiles(prev => new Set(prev).add(file.extract_number))
    setFileErrors(prev => {
      const newMap = new Map(prev)
      newMap.delete(file.extract_number)
      return newMap
    })

    try {
      const response = await fetch('/api/import/daily-update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          url: file.url,
        }),
      })

      const data = await response.json()

      if (!response.ok) {
        throw new Error(data.error || data.details || 'Failed to trigger import')
      }

      // Refresh both lists
      setTimeout(() => {
        fetchJobs(currentPage)
        fetchAvailableFiles()
      }, 1000)
    } catch (err) {
      // Set error state for this file
      setFileErrors(prev => {
        const newMap = new Map(prev)
        newMap.set(file.extract_number, err instanceof Error ? err.message : 'Unknown error')
        return newMap
      })
    } finally {
      // Remove from importing set
      setImportingFiles(prev => {
        const newSet = new Set(prev)
        newSet.delete(file.extract_number)
        return newSet
      })
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

      {/* Manual Batched Import Section */}
      <div className="bg-white rounded-lg border p-6 mb-6">
        <div className="mb-4">
          <h2 className="text-xl font-semibold">Manual Import Upload</h2>
          <p className="text-gray-600 text-sm mt-1">
            Upload a KBO update ZIP file for batched processing (avoids timeouts)
          </p>
        </div>

        <div className="space-y-4">
          {/* File Upload */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Select ZIP File
            </label>
            <input
              type="file"
              accept=".zip"
              onChange={(e) => {
                setUploadFile(e.target.files?.[0] || null)
                setUploadError(null)
                setUploadSuccess(null)
              }}
              disabled={uploading}
              className="block w-full text-sm text-gray-900 border border-gray-300 rounded-lg cursor-pointer bg-gray-50 focus:outline-none disabled:opacity-50 disabled:cursor-not-allowed"
            />
            {uploadFile && (
              <p className="mt-2 text-sm text-gray-600">
                Selected: {uploadFile.name} ({Math.round(uploadFile.size / 1024)}KB)
              </p>
            )}
          </div>

          {/* Upload Button */}
          <button
            onClick={handleBatchedImport}
            disabled={!uploadFile || uploading}
            className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
          >
            {uploading ? (
              <>
                <svg className="animate-spin h-5 w-5" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                Preparing Import...
              </>
            ) : (
              <>
                <svg className="w-5 h-5" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                  <path d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                </svg>
                Prepare Batched Import
              </>
            )}
          </button>

          {/* Error Message */}
          {uploadError && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4">
              <p className="text-red-800 font-medium">Upload Failed</p>
              <p className="text-red-700 text-sm mt-1">{uploadError}</p>
            </div>
          )}

          {/* Success Message */}
          {uploadSuccess && (
            <div className="bg-green-50 border border-green-200 rounded-lg p-4">
              <p className="text-green-800 font-medium">Import Prepared Successfully!</p>
              <div className="text-sm text-green-700 mt-2 space-y-1">
                <p>• Job ID: <span className="font-mono">{uploadSuccess.jobId}</span></p>
                <p>• Extract: #{uploadSuccess.extractNumber}</p>
                <p>• Total Batches: {uploadSuccess.totalBatches}</p>
              </div>
              <a
                href={`/admin/imports/${uploadSuccess.jobId}/progress`}
                className="inline-block mt-3 text-green-700 hover:text-green-900 font-medium text-sm underline"
              >
                → View Progress & Process Batches
              </a>
            </div>
          )}
        </div>
      </div>

      {/* Available Files List */}
      <div className="bg-white rounded-lg border p-6 mb-6">
        <div className="flex justify-between items-center mb-4">
          <div>
            <h2 className="text-xl font-semibold">Available Daily Updates</h2>
            <p className="text-gray-600 text-sm mt-1">
              Files available from the KBO Open Data portal
            </p>
          </div>
          <button
            onClick={fetchAvailableFiles}
            disabled={filesLoading}
            className="text-blue-600 hover:text-blue-800 text-sm font-medium flex items-center gap-1"
          >
            <svg className={`w-4 h-4 ${filesLoading ? 'animate-spin' : ''}`} fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
              <path d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
            Refresh
          </button>
        </div>

        {filesError && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
            <p className="text-red-800 font-medium">Error loading files</p>
            <p className="text-red-700 text-sm mt-1">{filesError}</p>
          </div>
        )}

        {filesLoading && (
          <div className="text-center py-8">
            <svg className="animate-spin h-8 w-8 mx-auto text-blue-600" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
            </svg>
            <p className="text-gray-600 mt-2">Loading available files...</p>
          </div>
        )}

        {!filesLoading && !filesError && availableFiles.length === 0 && (
          <div className="text-center py-8 text-gray-500">
            No files available
          </div>
        )}

        {!filesLoading && !filesError && availableFiles.length > 0 && (() => {
          const notImportedFiles = availableFiles.filter(f => !f.imported)
          const importedFiles = availableFiles.filter(f => f.imported)

          const FileRow = ({ file }: { file: AvailableFile }) => {
            const fileError = fileErrors.get(file.extract_number)
            return (
              <div key={file.extract_number} className="border rounded-lg hover:bg-gray-50">
                <div className="flex items-center justify-between p-4">
                  <div className="flex-1">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-mono text-sm font-medium">
                        #{file.extract_number}
                      </span>
                      <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
                        file.file_type === 'full'
                          ? 'bg-orange-100 text-orange-800'
                          : 'bg-blue-100 text-blue-800'
                      }`}>
                        {file.file_type === 'full' ? 'Full' : 'Update'}
                      </span>
                      {file.imported && (
                        <span className="inline-block px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-800">
                          ✓ Imported
                        </span>
                      )}
                    </div>
                    <p className="text-sm text-gray-600">
                      {file.snapshot_date}
                    </p>
                    <p className="text-xs text-gray-500 font-mono mt-1">
                      {file.filename}
                    </p>
                  </div>
              <div className="ml-4 flex gap-2">
                <button
                  onClick={() => handleImportFile(file)}
                  disabled={file.imported || importingFiles.has(file.extract_number)}
                  className="bg-blue-600 text-white px-4 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed text-sm flex items-center gap-2"
                  title="Quick import - may timeout on large files"
                >
                  {importingFiles.has(file.extract_number) ? (
                    <>
                      <svg className="animate-spin h-4 w-4" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                      </svg>
                      Importing...
                    </>
                  ) : file.imported ? (
                    'Imported'
                  ) : (
                    <>
                      <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                        <path d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                      </svg>
                      Quick Import
                    </>
                  )}
                </button>
                {!file.imported && !importingFiles.has(file.extract_number) && (
                  <button
                    onClick={() => handleBatchedImportFromUrl(file)}
                    className="bg-purple-600 text-white px-4 py-2 rounded-lg hover:bg-purple-700 text-sm flex items-center gap-2"
                    title="Batched import - processes in small chunks to avoid timeouts"
                  >
                    <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                      <path d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2m-3 7h3m-3 4h3m-6-4h.01M9 16h.01" />
                    </svg>
                    Batched Import
                  </button>
                )}
                  </div>
                </div>
                {fileError && (
                  <div className="px-4 pb-4">
                    <div className="bg-red-50 border border-red-200 rounded p-3 text-sm text-red-700">
                      <span className="font-medium">Error:</span> {fileError}
                    </div>
                  </div>
                )}
              </div>
            )
          }

          return (
            <div className="space-y-4">
              {/* Not Imported Files - Always Visible */}
              {notImportedFiles.length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-gray-700 mb-3 flex items-center gap-2">
                    <span className="flex items-center justify-center w-6 h-6 rounded-full bg-blue-100 text-blue-800 text-xs">
                      {notImportedFiles.length}
                    </span>
                    Pending Import
                  </h3>
                  <div className="space-y-2">
                    {notImportedFiles.map(file => <FileRow key={file.extract_number} file={file} />)}
                  </div>
                </div>
              )}

              {notImportedFiles.length === 0 && (
                <div className="text-center py-6 text-gray-500">
                  <p className="text-sm">All available files have been imported</p>
                </div>
              )}

              {/* Imported Files - Collapsible */}
              {importedFiles.length > 0 && (
                <div className="mt-6 pt-6 border-t">
                  <button
                    onClick={() => setShowImportedFiles(!showImportedFiles)}
                    className="w-full flex items-center justify-between text-sm font-semibold text-gray-700 hover:text-gray-900 mb-3"
                  >
                    <span className="flex items-center gap-2">
                      <span className="flex items-center justify-center w-6 h-6 rounded-full bg-green-100 text-green-800 text-xs">
                        {importedFiles.length}
                      </span>
                      Already Imported
                    </span>
                    <svg
                      className={`w-5 h-5 transition-transform ${showImportedFiles ? 'rotate-180' : ''}`}
                      fill="none"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth="2"
                      viewBox="0 0 24 24"
                      stroke="currentColor"
                    >
                      <path d="M19 9l-7 7-7-7" />
                    </svg>
                  </button>
                  {showImportedFiles && (
                    <div className="space-y-2">
                      {importedFiles.map(file => <FileRow key={file.extract_number} file={file} />)}
                    </div>
                  )}
                </div>
              )}
            </div>
          )
        })()}
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
                    <th className="w-12"></th>
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
                        <td className="px-4 py-3 text-sm relative">
                          {['vercel', 'local', 'backfill', 'web_manual'].includes(job.workerType) && (
                            <>
                              <button
                                type="button"
                                onClick={(e) => {
                                  e.stopPropagation()
                                  setOpenMenuId(openMenuId === job.id ? null : job.id)
                                }}
                                className="p-1 hover:bg-gray-100 rounded"
                                title="Actions"
                              >
                                <svg className="w-5 h-5 text-gray-600" fill="currentColor" viewBox="0 0 20 20">
                                  <path d="M10 6a2 2 0 110-4 2 2 0 010 4zM10 12a2 2 0 110-4 2 2 0 010 4zM10 18a2 2 0 110-4 2 2 0 010 4z" />
                                </svg>
                              </button>
                              {openMenuId === job.id && (
                                <>
                                  <div
                                    className="fixed inset-0 z-10"
                                    onClick={() => setOpenMenuId(null)}
                                  />
                                  <div className="absolute right-0 top-8 z-20 bg-white border border-gray-200 rounded-lg shadow-lg py-1 w-40">
                                    <button
                                      type="button"
                                      onClick={(e) => {
                                        e.stopPropagation()
                                        setOpenMenuId(null)
                                        router.push(`/admin/imports/${job.id}/progress`)
                                      }}
                                      className="w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 flex items-center gap-2"
                                    >
                                      <svg className="w-4 h-4" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
                                        <path d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                                      </svg>
                                      View Progress
                                    </button>
                                  </div>
                                </>
                              )}
                            </>
                          )}
                        </td>
                      </tr>
                      {job.status === 'completed' && (
                        <tr>
                          <td colSpan={8} className="p-0">
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
