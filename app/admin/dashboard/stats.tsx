'use client'

import { useState, useEffect } from 'react'
import { useLanguage } from '@/lib/contexts/language-context'
import type { DatabaseStats as DatabaseStatsType } from '@/lib/motherduck/stats'
import { JuridicalFormsChart } from './juridical-forms-chart'

export function DatabaseStats() {
  const { language } = useLanguage()
  const [stats, setStats] = useState<DatabaseStatsType | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const fetchStats = async () => {
      setLoading(true)
      setError(null)

      try {
        const res = await fetch(`/api/motherduck/stats?language=${language}`)
        if (!res.ok) throw new Error('Failed to fetch stats')
        const data = await res.json()
        setStats(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load stats')
      } finally {
        setLoading(false)
      }
    }

    fetchStats()
  }, [language])

  if (loading || !stats) {
    return <LoadingStats />
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-6">
        <p className="text-red-800">{error}</p>
      </div>
    )
  }

  // Total current records across all tables (enterprises, establishments, activities, addresses, denominations, contacts)
  // Note: Multiple extracts may be current simultaneously during incremental updates
  const totalRecords = Object.values(stats.recordCounts).reduce((sum, count) => sum + count, 0)

  return (
    <>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        {/* Stats Cards */}
        <div className="rounded-lg border bg-white p-6">
          <h3 className="text-sm font-medium text-gray-600">Total Enterprises</h3>
          <p className="mt-2 text-3xl font-bold">{stats.totalEnterprises.toLocaleString()}</p>
          <p className="mt-1 text-sm text-gray-500">As of Extract #{stats.currentExtract}</p>
        </div>

        <div className="rounded-lg border bg-white p-6">
          <h3 className="text-sm font-medium text-gray-600">Latest Extract</h3>
          <p className="mt-2 text-3xl font-bold">#{stats.currentExtract}</p>
          <p className="mt-1 text-sm text-gray-500">Snapshot: {stats.lastUpdate}</p>
        </div>

        <div className="rounded-lg border bg-white p-6">
          <h3 className="text-sm font-medium text-gray-600">Database Size</h3>
          <p className="mt-2 text-3xl font-bold">{stats.databaseSize}</p>
          <p className="mt-1 text-sm text-gray-500">Motherduck Storage</p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        <div className="rounded-lg border bg-white p-6">
          <h2 className="text-xl font-semibold mb-4">System Status</h2>
          <div className="space-y-2">
            <div className="flex justify-between">
              <span className="text-gray-600">Motherduck Connection</span>
              <span className="text-green-600 font-medium">Connected</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Latest Snapshot Date</span>
              <span className="font-medium">{stats.lastUpdate}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Total Current Records</span>
              <span className="font-medium">{totalRecords.toLocaleString()}</span>
            </div>
          </div>
        </div>

        <div className="rounded-lg border bg-white p-6">
          <h2 className="text-xl font-semibold mb-4">Record Counts</h2>
          <div className="space-y-2">
            <div className="flex justify-between">
              <span className="text-gray-600">Enterprises</span>
              <span className="font-medium">{stats.recordCounts.enterprises.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Establishments</span>
              <span className="font-medium">{stats.recordCounts.establishments.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Activities</span>
              <span className="font-medium">{stats.recordCounts.activities.toLocaleString()}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Addresses</span>
              <span className="font-medium">{stats.recordCounts.addresses.toLocaleString()}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Distribution across juridical forms */}
      <div className="rounded-lg border bg-white p-6 mb-8">
        <h2 className="text-xl font-semibold mb-4">Distribution across juridical forms</h2>
        <JuridicalFormsChart forms={stats.juridicalForms.all} />
      </div>

      {/* Language and Province Distribution */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Language Distribution */}
        <div className="rounded-lg border bg-white p-6">
          <h2 className="text-xl font-semibold mb-4">Language Distribution</h2>
          <p className="text-xs text-gray-500 mb-3">
            Natural persons ({(stats.totalEnterprises - stats.languageDistribution.reduce((sum, l) => sum + l.count, 0)).toLocaleString()}) excluded - no language data.
          </p>
          <div className="space-y-3">
            {stats.languageDistribution.map((lang) => (
              <div key={lang.language} className="space-y-1">
                <div className="flex justify-between text-sm">
                  <span className="font-medium">{lang.language}</span>
                  <span className="text-gray-600">
                    {lang.count.toLocaleString()} ({lang.percentage.toFixed(1)}%)
                  </span>
                </div>
                <div className="relative h-2 bg-gray-100 rounded-full overflow-hidden">
                  <div
                    className="absolute h-full bg-green-500 rounded-full transition-all"
                    style={{ width: `${lang.percentage}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Province Distribution */}
        <div className="rounded-lg border bg-white p-6">
          <h2 className="text-xl font-semibold mb-4">Province Distribution</h2>
          <div className="space-y-2 max-h-96 overflow-y-auto">
            {stats.provinceDistribution.map((province) => (
              <div key={province.province} className="flex justify-between text-sm">
                <span className="text-gray-700">{province.province}</span>
                <span className="font-medium tabular-nums">
                  {province.count.toLocaleString()} ({province.percentage.toFixed(1)}%)
                </span>
              </div>
            ))}
          </div>
          <p className="mt-4 text-xs text-gray-500">Based on registered addresses with valid postal codes</p>
        </div>
      </div>
    </>
  )
}

function LoadingStats() {
  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        {[1, 2, 3].map((i) => (
          <div key={i} className="rounded-lg border bg-white p-6 animate-pulse">
            <div className="h-4 bg-gray-200 rounded w-1/2 mb-4"></div>
            <div className="h-8 bg-gray-200 rounded w-3/4 mb-2"></div>
            <div className="h-3 bg-gray-200 rounded w-1/3"></div>
          </div>
        ))}
      </div>

      <div className="rounded-lg border bg-white p-6 animate-pulse">
        <div className="h-6 bg-gray-200 rounded w-1/4 mb-4"></div>
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <div key={i} className="flex justify-between">
              <div className="h-4 bg-gray-200 rounded w-1/3"></div>
              <div className="h-4 bg-gray-200 rounded w-1/4"></div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
