'use client'

import { useState, useEffect } from 'react'
import Link from 'next/link'
import { useParams } from 'next/navigation'
import type { EnterpriseDetail } from '@/app/api/enterprises/[number]/route'
import type { Snapshot } from '@/app/api/enterprises/[number]/snapshots/route'

export default function EnterpriseDetailPage() {
  const params = useParams()
  const number = params.number as string

  const [detail, setDetail] = useState<EnterpriseDetail | null>(null)
  const [snapshots, setSnapshots] = useState<Snapshot[]>([])
  const [selectedSnapshot, setSelectedSnapshot] = useState<Snapshot | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Fetch snapshots on mount
  useEffect(() => {
    const fetchSnapshots = async () => {
      try {
        const res = await fetch(`/api/enterprises/${number}/snapshots`)
        if (!res.ok) throw new Error('Failed to fetch snapshots')

        const data = await res.json()
        setSnapshots(data.snapshots)

        // Select current snapshot by default
        const current = data.snapshots.find((s: Snapshot) => s.isCurrent)
        if (current) {
          setSelectedSnapshot(current)
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load snapshots')
      }
    }

    fetchSnapshots()
  }, [number])

  // Fetch enterprise details when selected snapshot changes
  useEffect(() => {
    if (!selectedSnapshot) return

    const fetchDetail = async () => {
      setLoading(true)
      setError(null)

      try {
        const params = new URLSearchParams({
          snapshot_date: selectedSnapshot.snapshotDate,
          extract_number: selectedSnapshot.extractNumber.toString(),
        })

        const res = await fetch(`/api/enterprises/${number}?${params}`)
        if (!res.ok) throw new Error('Failed to fetch enterprise details')

        const data = await res.json()
        setDetail(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load details')
      } finally {
        setLoading(false)
      }
    }

    fetchDetail()
  }, [number, selectedSnapshot])

  if (error) {
    return (
      <div>
        <Link href="/admin/browse" className="text-blue-600 hover:text-blue-800 hover:underline mb-6 inline-block">
          ← Back to Browse
        </Link>
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <p className="text-red-800">{error}</p>
        </div>
      </div>
    )
  }

  if (loading || !detail) {
    return (
      <div>
        <Link href="/admin/browse" className="text-blue-600 hover:text-blue-800 hover:underline mb-6 inline-block">
          ← Back to Browse
        </Link>
        <LoadingDetail />
      </div>
    )
  }

  return (
    <div>
      <div className="mb-6">
        <Link href="/admin/browse" className="text-blue-600 hover:text-blue-800 hover:underline">
          ← Back to Browse
        </Link>
      </div>

      {/* Temporal Navigation */}
      {snapshots.length > 1 && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-4">
            <span className="text-sm font-medium text-blue-900">View historical data:</span>
            <select
              value={`${selectedSnapshot?.snapshotDate}_${selectedSnapshot?.extractNumber}`}
              onChange={(e) => {
                const [date, extract] = e.target.value.split('_')
                const snapshot = snapshots.find(
                  (s) => s.snapshotDate === date && s.extractNumber === parseInt(extract)
                )
                if (snapshot) setSelectedSnapshot(snapshot)
              }}
              className="rounded-lg border border-blue-300 px-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              {snapshots.map((snapshot) => (
                <option
                  key={`${snapshot.snapshotDate}_${snapshot.extractNumber}`}
                  value={`${snapshot.snapshotDate}_${snapshot.extractNumber}`}
                >
                  {snapshot.snapshotDate} (Extract #{snapshot.extractNumber})
                  {snapshot.isCurrent ? ' - Current' : ''}
                </option>
              ))}
            </select>
            {!selectedSnapshot?.isCurrent && (
              <span className="text-xs text-blue-700 italic">
                Viewing historical snapshot
              </span>
            )}
          </div>
        </div>
      )}

      <div className="space-y-6">
        {/* Header */}
        <div className="bg-white rounded-lg border p-6">
          <div className="flex justify-between items-start mb-4">
            <div>
              <h1 className="text-3xl font-bold mb-2">
                {detail.denominations.find((d) => d.typeCode === '001')?.denomination ||
                  'Unknown Enterprise'}
              </h1>
              <p className="text-gray-600 font-mono">{detail.enterpriseNumber}</p>
            </div>
            <div className="text-right">
              <span
                className={`inline-block px-3 py-1 rounded text-sm font-medium ${
                  detail.status === 'AC'
                    ? 'bg-green-100 text-green-800'
                    : 'bg-gray-100 text-gray-800'
                }`}
              >
                {detail.status === 'AC' ? 'Active' : detail.status}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 pt-4 border-t">
            <div>
              <p className="text-sm text-gray-600">Juridical Form</p>
              <p className="font-medium">
                {detail.juridicalFormDescription || detail.juridicalForm || '-'}
              </p>
              {detail.juridicalFormDescription && detail.juridicalForm && (
                <p className="text-xs text-gray-500">{detail.juridicalForm}</p>
              )}
            </div>
            <div>
              <p className="text-sm text-gray-600">Type</p>
              <p className="font-medium">
                {detail.typeOfEnterpriseDescription || detail.typeOfEnterprise || '-'}
              </p>
              {detail.typeOfEnterpriseDescription && detail.typeOfEnterprise && (
                <p className="text-xs text-gray-500">{detail.typeOfEnterprise}</p>
              )}
            </div>
            <div>
              <p className="text-sm text-gray-600">Start Date</p>
              <p className="font-medium">{detail.startDate || '-'}</p>
            </div>
            <div>
              <p className="text-sm text-gray-600">Data As Of</p>
              <p className="font-medium">{detail.snapshotDate}</p>
            </div>
          </div>
        </div>

        {/* Denominations */}
        {detail.denominations.length > 0 && (
          <div className="bg-white rounded-lg border p-6">
            <h2 className="text-xl font-semibold mb-4">Names & Denominations</h2>
            <div className="space-y-3">
              {detail.denominations.map((denom, idx) => (
                <div key={idx} className="flex gap-4 pb-3 border-b last:border-b-0">
                  <div className="w-12">
                    <span className="inline-block px-2 py-1 bg-gray-100 rounded text-xs font-medium">
                      {denom.language}
                    </span>
                  </div>
                  <div className="flex-1">
                    <p className="text-sm text-gray-600">Type: {denom.typeCode}</p>
                    <p className="font-medium">{denom.denomination}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Addresses */}
        {detail.addresses.length > 0 && (
          <div className="bg-white rounded-lg border p-6">
            <h2 className="text-xl font-semibold mb-4">Addresses</h2>
            <div className="space-y-4">
              {detail.addresses.map((addr, idx) => (
                <div key={idx} className="pb-4 border-b last:border-b-0">
                  <p className="text-sm text-gray-600 mb-2">
                    Type: {addr.typeCode}
                    {addr.dateStrikingOff && ` (Struck off: ${addr.dateStrikingOff})`}
                  </p>
                  <div className="font-medium">
                    {addr.streetNL && (
                      <p>
                        {addr.streetNL} {addr.houseNumber}
                        {addr.box && ` box ${addr.box}`}
                      </p>
                    )}
                    {addr.zipcode && addr.municipalityNL && (
                      <p>
                        {addr.zipcode} {addr.municipalityNL}
                      </p>
                    )}
                    {addr.countryNL && <p>{addr.countryNL}</p>}
                    {addr.extraAddressInfo && (
                      <p className="text-sm text-gray-600 mt-1">{addr.extraAddressInfo}</p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Activities */}
        {detail.activities.length > 0 && (
          <div className="bg-white rounded-lg border p-6">
            <h2 className="text-xl font-semibold mb-4">Economic Activities</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {Object.entries(
                detail.activities.reduce((acc, activity) => {
                  const version = activity.naceVersion
                  if (!acc[version]) acc[version] = []
                  acc[version].push(activity)
                  return acc
                }, {} as Record<string, typeof detail.activities>)
              )
                .sort(([a], [b]) => b.localeCompare(a)) // Sort versions descending (2025, 2008, 2003)
                .map(([version, activities]) => (
                  <div key={version}>
                    <h3 className="text-sm font-semibold text-gray-700 mb-2">
                      NACE {version}
                    </h3>
                    <div className="space-y-1">
                      {activities.map((activity, idx) => (
                        <div
                          key={idx}
                          className="flex flex-col text-sm py-1.5 border-b border-gray-100 last:border-0"
                        >
                          <span className="font-mono font-medium text-gray-900">
                            {activity.naceCode}
                          </span>
                          {activity.naceDescriptionNL && (
                            <span className="text-gray-700 text-xs mt-0.5">
                              {activity.naceDescriptionNL}
                            </span>
                          )}
                          <span className="text-gray-500 text-xs mt-0.5">
                            {activity.classification} · {activity.activityGroup}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
            </div>
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">

          {/* Contacts */}
          {detail.contacts.length > 0 && (
            <div className="bg-white rounded-lg border p-6">
              <h2 className="text-xl font-semibold mb-4">Contact Information</h2>
              <div className="space-y-3">
                {detail.contacts.map((contact, idx) => (
                  <div key={idx} className="pb-3 border-b last:border-b-0">
                    <p className="text-sm text-gray-600">{contact.contactType}</p>
                    <p className="font-medium break-all">{contact.value}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Establishments */}
        {detail.establishments.length > 0 && (
          <div className="bg-white rounded-lg border p-6">
            <h2 className="text-xl font-semibold mb-4">
              Establishments ({detail.establishments.length})
            </h2>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b">
                  <tr>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                      Establishment Number
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                      Name
                    </th>
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                      Start Date
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {detail.establishments.map((est) => (
                    <tr key={est.establishmentNumber} className="hover:bg-gray-50">
                      <td className="px-4 py-3 text-sm font-mono">
                        {est.establishmentNumber}
                      </td>
                      <td className="px-4 py-3 text-sm">{est.primaryName || '-'}</td>
                      <td className="px-4 py-3 text-sm">{est.startDate || '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function LoadingDetail() {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg border p-6 animate-pulse">
        <div className="h-8 bg-gray-200 rounded w-1/2 mb-4"></div>
        <div className="h-4 bg-gray-200 rounded w-1/4 mb-6"></div>
        <div className="grid grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i}>
              <div className="h-3 bg-gray-200 rounded w-20 mb-2"></div>
              <div className="h-4 bg-gray-200 rounded w-24"></div>
            </div>
          ))}
        </div>
      </div>
      {[1, 2, 3].map((i) => (
        <div key={i} className="bg-white rounded-lg border p-6 animate-pulse">
          <div className="h-6 bg-gray-200 rounded w-1/3 mb-4"></div>
          <div className="space-y-3">
            <div className="h-4 bg-gray-200 rounded"></div>
            <div className="h-4 bg-gray-200 rounded w-3/4"></div>
          </div>
        </div>
      ))}
    </div>
  )
}
