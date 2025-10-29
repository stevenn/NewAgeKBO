'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'

interface AffectedEnterprise {
  enterpriseNumber: string
  primaryName: string
  changeType: 'insert' | 'update' | 'delete'
  affectedTables: Array<{
    tableName: string
    changeCount: number
  }>
}

interface ExpandableJobDetailsProps {
  jobId: string
  extractNumber: number
  isExpanded: boolean
}

export function ExpandableJobDetails({ jobId, extractNumber, isExpanded }: ExpandableJobDetailsProps) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [enterprises, setEnterprises] = useState<AffectedEnterprise[]>([])
  const [total, setTotal] = useState(0)
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(0)
  const [hasLoaded, setHasLoaded] = useState(false)

  const pageSize = 50

  const fetchAffectedEnterprises = async (page: number) => {
    setLoading(true)
    setError(null)

    try {
      const response = await fetch(
        `/api/import-jobs/${jobId}/affected-enterprises?page=${page}&limit=${pageSize}`
      )

      if (!response.ok) {
        throw new Error('Failed to fetch affected enterprises')
      }

      const data = await response.json()
      setEnterprises(data.enterprises)
      setTotal(data.total)
      setCurrentPage(data.page)
      setTotalPages(data.totalPages)
      setHasLoaded(true)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data')
    } finally {
      setLoading(false)
    }
  }

  // Fetch data when expanded for the first time
  useEffect(() => {
    if (isExpanded && !hasLoaded) {
      fetchAffectedEnterprises(1)
    }
    // fetchAffectedEnterprises is stable and doesn't need to be in deps
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isExpanded, hasLoaded, jobId])

  const handlePageChange = async (newPage: number) => {
    await fetchAffectedEnterprises(newPage)
  }

  const getChangeTypeBadge = (changeType: 'insert' | 'update' | 'delete') => {
    const styles = {
      insert: 'bg-green-100 text-green-800',
      update: 'bg-blue-100 text-blue-800',
      delete: 'bg-red-100 text-red-800',
    }

    return (
      <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${styles[changeType]}`}>
        {changeType.charAt(0).toUpperCase() + changeType.slice(1)}
      </span>
    )
  }

  const formatAffectedTables = (tables: Array<{ tableName: string; changeCount: number }> | null | undefined) => {
    if (!tables || !Array.isArray(tables) || tables.length === 0) return 'No details'

    const formatted = tables
      .map((t) => `${t.tableName} (${t.changeCount})`)
      .join(', ')

    return `${tables.length} tables: ${formatted}`
  }

  if (!isExpanded) {
    return null
  }

  return (
    <div className="border-t">
      <div className="bg-gray-50 px-4 py-3">
          {loading && (
            <div className="text-center py-4 text-gray-500">
              <div className="animate-pulse">Loading affected enterprises...</div>
            </div>
          )}

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
              <p className="text-red-800">{error}</p>
            </div>
          )}

          {!loading && !error && enterprises.length > 0 && (
            <>
              <div className="bg-white rounded border overflow-hidden mb-4">
                <table className="w-full text-sm">
                  <thead className="bg-gray-100 border-b">
                    <tr>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">
                        Enterprise
                      </th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">
                        Change
                      </th>
                      <th className="text-left px-3 py-2 font-medium text-gray-600">
                        Affected Tables
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {enterprises.map((ent) => (
                      <tr key={ent.enterpriseNumber} className="hover:bg-gray-50">
                        <td className="px-3 py-2">
                          <Link
                            href={`/admin/browse/${ent.enterpriseNumber}?from_extract=${extractNumber}`}
                            className="text-blue-600 hover:text-blue-800 hover:underline"
                          >
                            <div className="font-mono text-xs text-gray-500 mb-1">
                              {ent.enterpriseNumber}
                            </div>
                            <div className="font-medium">{ent.primaryName}</div>
                          </Link>
                        </td>
                        <td className="px-3 py-2">
                          {getChangeTypeBadge(ent.changeType)}
                        </td>
                        <td className="px-3 py-2 text-xs text-gray-600">
                          {formatAffectedTables(ent.affectedTables)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Pagination Controls */}
              {totalPages > 1 && (
                <div className="flex items-center justify-between">
                  <div className="text-sm text-gray-600">
                    Page {currentPage} of {totalPages} ({total.toLocaleString()} total)
                  </div>

                  <div className="flex gap-2">
                    <button
                      onClick={() => handlePageChange(currentPage - 1)}
                      disabled={currentPage === 1 || loading}
                      className="px-3 py-1 text-sm border rounded hover:bg-white disabled:opacity-50 disabled:cursor-not-allowed"
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
                          className={`px-3 py-1 text-sm border rounded hover:bg-white disabled:opacity-50 ${
                            pageNum === currentPage
                              ? 'bg-blue-600 text-white hover:bg-blue-700'
                              : 'bg-white'
                          }`}
                        >
                          {pageNum}
                        </button>
                      )
                    })}

                    <button
                      onClick={() => handlePageChange(currentPage + 1)}
                      disabled={currentPage === totalPages || loading}
                      className="px-3 py-1 text-sm border rounded hover:bg-white disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </>
          )}

        {!loading && !error && enterprises.length === 0 && (
          <div className="text-center py-4 text-gray-500">
            No affected enterprises found for this import
          </div>
        )}
      </div>
    </div>
  )
}
