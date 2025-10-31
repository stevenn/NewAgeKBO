'use client'

import { useState, useEffect, useRef } from 'react'
import Link from 'next/link'
import type { EnterpriseSearchResult } from '@/app/api/enterprises/search/route'
import { useLanguage } from '@/lib/contexts/language-context'

export default function BrowsePage() {
  const { language, isInitialized } = useLanguage()
  const [searchQuery, setSearchQuery] = useState('')
  const [searchType, setSearchType] = useState<'all' | 'number' | 'name' | 'nace'>('all')
  const [results, setResults] = useState<EnterpriseSearchResult[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [currentPage, setCurrentPage] = useState(1)
  const limit = 25
  const hasFetchedRef = useRef(false)
  const lastLanguageRef = useRef<string | null>(null)

  const executeSearch = async (page: number = 1) => {
    setLoading(true)
    setError(null)

    try {
      const offset = (page - 1) * limit
      const params = new URLSearchParams({
        q: searchQuery,
        type: searchType,
        limit: limit.toString(),
        offset: offset.toString(),
        language: language,
      })

      const res = await fetch(`/api/enterprises/search?${params}`)

      if (!res.ok) {
        throw new Error('Failed to search enterprises')
      }

      const data = await res.json()
      setResults(data.results)
      setTotal(data.total)
      setCurrentPage(page)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred')
      setResults([])
      setTotal(0)
    } finally {
      setLoading(false)
    }
  }

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    executeSearch(1)
  }

  // Load initial data on mount and when language changes
  useEffect(() => {
    if (!isInitialized) return

    // Reset hasFetched when language changes
    if (lastLanguageRef.current !== language) {
      hasFetchedRef.current = false
      lastLanguageRef.current = language
    }

    // Prevent double-fetch in React Strict Mode
    if (hasFetchedRef.current) return
    hasFetchedRef.current = true

    executeSearch(1)
  }, [language, isInitialized]) // eslint-disable-line react-hooks/exhaustive-deps

  const totalPages = Math.ceil(total / limit)

  return (
    <div>
      <h1 className="text-3xl font-bold mb-6">Browse Enterprises</h1>

      {/* Search Form */}
      <div className="bg-white rounded-lg border p-6 mb-6">
        <form onSubmit={handleSearch} className="space-y-4">
          <div className="flex gap-4">
            <div className="flex-1">
              <label htmlFor="search" className="block text-sm font-medium text-gray-700 mb-2">
                Search
              </label>
              <input
                id="search"
                type="text"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                placeholder="Enter enterprise number, name, or NACE code..."
                className="w-full rounded-lg border border-gray-300 px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div className="w-48">
              <label htmlFor="type" className="block text-sm font-medium text-gray-700 mb-2">
                Search Type
              </label>
              <select
                id="type"
                value={searchType}
                onChange={(e) => setSearchType(e.target.value as 'all' | 'number' | 'name' | 'nace')}
                className="w-full rounded-lg border border-gray-300 px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="all">All</option>
                <option value="number">Enterprise Number</option>
                <option value="name">Name</option>
                <option value="nace">NACE Code</option>
              </select>
            </div>
            <div className="flex items-end">
              <button
                type="submit"
                disabled={loading}
                className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
              >
                {loading ? 'Searching...' : 'Search'}
              </button>
            </div>
          </div>
        </form>
      </div>

      {/* Error Message */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
          <p className="text-red-800">{error}</p>
        </div>
      )}

      {/* Results */}
      <div className="bg-white rounded-lg border">
        {/* Results Header */}
        <div className="border-b p-4">
          <div className="flex justify-between items-center">
            <h2 className="text-lg font-semibold">
              {total > 0 ? `${total.toLocaleString()} enterprises found` : 'No results'}
            </h2>
            {totalPages > 1 && (
              <div className="text-sm text-gray-600">
                Page {currentPage} of {totalPages}
              </div>
            )}
          </div>
        </div>

        {/* Results Table */}
        {loading ? (
          <div className="p-8 text-center text-gray-500">
            <div className="animate-pulse">Loading...</div>
          </div>
        ) : results.length > 0 ? (
          <>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b">
                  <tr>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600 w-40">
                      Enterprise Number
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                      Name
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600 w-64">
                      Juridical Form
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600 w-24">
                      Status
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600 w-40">
                      Location
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600 w-32">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {results.map((enterprise) => (
                    <tr
                      key={enterprise.enterpriseNumber}
                      className={`hover:bg-gray-50 ${!enterprise.isCurrent ? 'opacity-60' : ''}`}
                    >
                      <td className="px-4 py-3 text-sm font-mono w-40">
                        {enterprise.enterpriseNumber}
                      </td>
                      <td className="px-4 py-3 text-sm">
                        <div className="flex items-center gap-2">
                          <span>{enterprise.primaryName}</span>
                          {!enterprise.isCurrent && (
                            <span className="inline-block px-1.5 py-0.5 rounded text-xs font-medium bg-red-100 text-red-700">
                              Ceased
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-sm w-64">
                        {enterprise.juridicalFormDescription || '-'}
                      </td>
                      <td className="px-4 py-3 text-sm w-24">
                        <span
                          className={`inline-block px-2 py-1 rounded text-xs font-medium ${
                            enterprise.status === 'AC'
                              ? 'bg-green-100 text-green-800'
                              : enterprise.status === 'ST'
                              ? 'bg-red-100 text-red-800'
                              : 'bg-gray-100 text-gray-800'
                          }`}
                        >
                          {enterprise.status === 'AC' ? 'Active' : enterprise.status === 'ST' ? 'Ceased' : enterprise.status}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-sm w-40">
                        {enterprise.municipality || '-'}
                      </td>
                      <td className="px-4 py-3 text-sm w-32">
                        <Link
                          href={`/admin/browse/${enterprise.enterpriseNumber}`}
                          className="text-blue-600 hover:text-blue-800 hover:underline"
                        >
                          View Details
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="border-t p-4">
                <div className="flex justify-center gap-2">
                  <button
                    onClick={() => executeSearch(currentPage - 1)}
                    disabled={currentPage === 1 || loading}
                    className="px-4 py-2 rounded-lg border hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Previous
                  </button>
                  <div className="flex items-center gap-2">
                    {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                      let pageNum: number
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
                          onClick={() => executeSearch(pageNum)}
                          disabled={loading}
                          className={`px-4 py-2 rounded-lg border ${
                            currentPage === pageNum
                              ? 'bg-blue-600 text-white border-blue-600'
                              : 'hover:bg-gray-50'
                          } disabled:opacity-50 disabled:cursor-not-allowed`}
                        >
                          {pageNum}
                        </button>
                      )
                    })}
                  </div>
                  <button
                    onClick={() => executeSearch(currentPage + 1)}
                    disabled={currentPage === totalPages || loading}
                    className="px-4 py-2 rounded-lg border hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </>
        ) : (
          <div className="p-8 text-center text-gray-500">
            No enterprises found. Try a different search.
          </div>
        )}
      </div>
    </div>
  )
}
