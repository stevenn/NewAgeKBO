'use client'

import { useState, useEffect, useMemo } from 'react'
import Link from 'next/link'
import { useParams, useSearchParams } from 'next/navigation'
import type { EnterpriseDetail } from '@/app/api/enterprises/[number]/route'
import type { Snapshot } from '@/app/api/enterprises/[number]/snapshots/route'
import { compareEnterprises } from '@/lib/utils/compare-snapshots'

export default function EnterpriseDetailPage() {
  const params = useParams()
  const searchParams = useSearchParams()
  const number = params.number as string
  const fromExtract = searchParams.get('from_extract')

  const [detail, setDetail] = useState<EnterpriseDetail | null>(null)
  const [previousDetail, setPreviousDetail] = useState<EnterpriseDetail | null>(null)
  const [snapshots, setSnapshots] = useState<Snapshot[]>([])
  const [selectedSnapshot, setSelectedSnapshot] = useState<Snapshot | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Compute comparison between current and previous snapshot
  const comparison = useMemo(() => {
    if (!detail || !previousDetail) return null
    return compareEnterprises(detail, previousDetail)
  }, [detail, previousDetail])

  // Display the selected snapshot's data (backend now correctly reconstructs point-in-time state)
  const displayDetail = detail!

  // Fetch snapshots and current detail on mount
  useEffect(() => {
    const fetchSnapshotsAndDetail = async () => {
      try {
        const res = await fetch(`/api/enterprises/${number}/snapshots`)
        if (!res.ok) throw new Error('Failed to fetch snapshots')

        const data = await res.json()
        setSnapshots(data.snapshots)

        // If from_extract parameter is provided, auto-select that snapshot
        if (fromExtract) {
          const extractNum = parseInt(fromExtract, 10)
          const targetSnapshot = data.snapshots.find((s: Snapshot) => s.extractNumber === extractNum)
          if (targetSnapshot) {
            setSelectedSnapshot(targetSnapshot)
            return
          }
        }

        // Otherwise, select current snapshot by default (will trigger detail fetch in other useEffect)
        const current = data.snapshots.find((s: Snapshot) => s.isCurrent)
        if (current) {
          setSelectedSnapshot(current)
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load snapshots')
        setLoading(false)
      }
    }

    fetchSnapshotsAndDetail()
  }, [number, fromExtract])

  // Fetch enterprise details when selected snapshot changes
  useEffect(() => {
    if (!selectedSnapshot || !snapshots.length) return

    const fetchDetailAndPrevious = async () => {
      setLoading(true)
      setError(null)
      setPreviousDetail(null)

      try {
        const params = new URLSearchParams({
          snapshot_date: selectedSnapshot.snapshotDate,
          extract_number: selectedSnapshot.extractNumber.toString(),
        })

        // Fetch current selected snapshot
        const res = await fetch(`/api/enterprises/${number}?${params}`)
        if (!res.ok) throw new Error('Failed to fetch enterprise details')
        const data = await res.json()
        setDetail(data)

        // Find previous snapshot for comparison
        // We need to find the last snapshot that actually has data for this enterprise
        const currentIndex = snapshots.findIndex(
          (s) =>
            s.snapshotDate === selectedSnapshot.snapshotDate &&
            s.extractNumber === selectedSnapshot.extractNumber
        )

        if (currentIndex !== -1 && currentIndex < snapshots.length - 1) {
          // Try snapshots starting from the next one until we find one with data
          for (let i = currentIndex + 1; i < snapshots.length; i++) {
            const previousSnapshot = snapshots[i]
            const prevParams = new URLSearchParams({
              snapshot_date: previousSnapshot.snapshotDate,
              extract_number: previousSnapshot.extractNumber.toString(),
            })

            const prevRes = await fetch(`/api/enterprises/${number}?${prevParams}`)
            if (prevRes.ok) {
              const prevData = await prevRes.json()
              // Check if this snapshot actually has the enterprise data
              // (the API returns data even if enterprise doesn't exist in that extract)
              if (prevData && prevData.enterpriseNumber) {
                setPreviousDetail(prevData)
                break
              }
            }
          }
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load details')
      } finally {
        setLoading(false)
      }
    }

    fetchDetailAndPrevious()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [number, selectedSnapshot, snapshots])

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

      {/* From Extract Indicator */}
      {fromExtract && selectedSnapshot && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-4">
          <div className="flex items-center gap-2">
            <svg className="w-5 h-5 text-green-600" fill="none" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" viewBox="0 0 24 24" stroke="currentColor">
              <path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <span className="text-sm font-medium text-green-900">
              Viewing changes from Import Job Extract #{fromExtract}
            </span>
            <span className="text-sm text-green-700">
              ({selectedSnapshot.snapshotDate})
            </span>
          </div>
        </div>
      )}

      {/* Temporal Navigation */}
      {snapshots.length > 0 && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
          <div className="flex items-center gap-4">
            <span className="text-sm font-medium text-blue-900">
              {snapshots.length > 1 ? 'View historical data:' : 'Data version:'}
            </span>
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
              disabled={snapshots.length === 1}
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
            {!selectedSnapshot?.isCurrent && snapshots.length > 1 && (
              <span className="text-xs text-blue-700 italic">
                Viewing historical snapshot
              </span>
            )}
          </div>
          {comparison && previousDetail && (
            <div className="mt-3 pt-3 border-t border-blue-300">
              <p className="text-xs text-blue-800">
                Comparing with previous snapshot: {previousDetail.snapshotDate} (Extract #{previousDetail.extractNumber})
              </p>
              <div className="flex gap-4 mt-2 text-xs">
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-green-500"></span>
                  Added
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-yellow-500"></span>
                  Changed
                </span>
                <span className="flex items-center gap-1">
                  <span className="w-3 h-3 rounded bg-red-500"></span>
                  Removed
                </span>
              </div>
            </div>
          )}
        </div>
      )}

      <div className="space-y-6">
        {/* Header */}
        <div className="bg-white rounded-lg border p-6">
          <div className="flex justify-between items-start mb-4">
            <div>
              <h1 className="text-3xl font-bold mb-2">
                {displayDetail.denominations.find((d) => d.typeCode === '001')?.denomination ||
                  'Unknown Enterprise'}
              </h1>
              <p className="text-gray-600 font-mono">{displayDetail.enterpriseNumber}</p>
            </div>
            <div className="text-right">
              <div className="flex items-center gap-2 justify-end">
                {comparison && comparison.status.type === 'changed' && (
                  <span className="w-2 h-2 rounded-full bg-yellow-500" title="Status changed"></span>
                )}
                <span
                  className={`inline-block px-3 py-1 rounded text-sm font-medium ${
                    displayDetail.status === 'AC'
                      ? 'bg-green-100 text-green-800'
                      : 'bg-gray-100 text-gray-800'
                  }`}
                >
                  {displayDetail.status === 'AC' ? 'Active' : displayDetail.status}
                </span>
              </div>
              {comparison && comparison.status.type === 'changed' && (
                <p className="text-xs text-gray-500 mt-1">
                  Changed from: {comparison.status.oldValue} (on {previousDetail?.snapshotDate})
                </p>
              )}
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 pt-4 border-t">
            <div>
              <div className="flex items-center gap-2 mb-1">
                <p className="text-sm text-gray-600">Juridical Form</p>
                {comparison && comparison.juridicalForm.type === 'changed' && (
                  <span className="w-2 h-2 rounded-full bg-yellow-500" title="Changed"></span>
                )}
              </div>
              <p className="font-medium">
                {displayDetail.juridicalFormDescription || displayDetail.juridicalForm || '-'}
              </p>
              {displayDetail.juridicalFormDescription && displayDetail.juridicalForm && (
                <p className="text-xs text-gray-500">{displayDetail.juridicalForm}</p>
              )}
              {comparison && comparison.juridicalForm.type === 'changed' && (
                <p className="text-xs text-gray-500 mt-1">
                  Changed from: {comparison.juridicalForm.oldValue} (on {previousDetail?.snapshotDate})
                </p>
              )}
            </div>
            <div>
              <div className="flex items-center gap-2 mb-1">
                <p className="text-sm text-gray-600">Juridical Situation</p>
                {comparison && comparison.juridicalSituation.type === 'changed' && (
                  <span className="w-2 h-2 rounded-full bg-yellow-500" title="Changed"></span>
                )}
              </div>
              <p className="font-medium">
                {displayDetail.juridicalSituationDescription || displayDetail.juridicalSituation || '-'}
              </p>
              {displayDetail.juridicalSituationDescription && displayDetail.juridicalSituation && (
                <p className="text-xs text-gray-500">{displayDetail.juridicalSituation}</p>
              )}
              {comparison && comparison.juridicalSituation.type === 'changed' && (
                <p className="text-xs text-gray-500 mt-1">
                  Changed from: {comparison.juridicalSituation.oldValue} (on {previousDetail?.snapshotDate})
                </p>
              )}
            </div>
            <div>
              <div className="flex items-center gap-2 mb-1">
                <p className="text-sm text-gray-600">Type</p>
                {comparison && comparison.typeOfEnterprise.type === 'changed' && (
                  <span className="w-2 h-2 rounded-full bg-yellow-500" title="Changed"></span>
                )}
              </div>
              <p className="font-medium">
                {displayDetail.typeOfEnterpriseDescription || displayDetail.typeOfEnterprise || '-'}
              </p>
              {displayDetail.typeOfEnterpriseDescription && displayDetail.typeOfEnterprise && (
                <p className="text-xs text-gray-500">{displayDetail.typeOfEnterprise}</p>
              )}
              {comparison && comparison.typeOfEnterprise.type === 'changed' && (
                <p className="text-xs text-gray-500 mt-1">
                  Changed from: {comparison.typeOfEnterprise.oldValue} (on {previousDetail?.snapshotDate})
                </p>
              )}
            </div>
            <div>
              <div className="flex items-center gap-2 mb-1">
                <p className="text-sm text-gray-600">Start Date</p>
                {comparison && comparison.startDate.type === 'changed' && (
                  <span className="w-2 h-2 rounded-full bg-yellow-500" title="Changed"></span>
                )}
              </div>
              <p className="font-medium">{displayDetail.startDate || '-'}</p>
              {comparison && comparison.startDate.type === 'changed' && (
                <p className="text-xs text-gray-500 mt-1">
                  Changed from: {comparison.startDate.oldValue} (on {previousDetail?.snapshotDate})
                </p>
              )}
            </div>
            <div>
              <p className="text-sm text-gray-600">Data As Of</p>
              <p className="font-medium">{displayDetail.snapshotDate}</p>
            </div>
          </div>
        </div>

        {/* Denominations */}
        {displayDetail && (displayDetail.denominations.length > 0 || (comparison && comparison.denominations.removed.length > 0)) && (
          <div className="bg-white rounded-lg border p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-semibold">Names & Denominations</h2>
              {comparison && (comparison.denominations.added.length > 0 || comparison.denominations.removed.length > 0) && (
                <div className="flex gap-3 text-xs">
                  {comparison.denominations.added.length > 0 && (
                    <span className="bg-green-100 text-green-800 px-2 py-1 rounded">
                      +{comparison.denominations.added.length} New in selected
                    </span>
                  )}
                  {comparison.denominations.removed.length > 0 && (
                    <span className="bg-red-100 text-red-800 px-2 py-1 rounded">
                      -{comparison.denominations.removed.length} Removed in selected
                    </span>
                  )}
                </div>
              )}
            </div>
            <div className="space-y-3">
              {/* Show removed denominations from previous snapshot */}
              {comparison && comparison.denominations.removed.length > 0 && (
                <>
                  {comparison.denominations.removed.map((denom: typeof displayDetail.denominations[0], idx: number) => (
                    <div
                      key={`removed-${idx}`}
                      className="flex gap-4 pb-3 border-b last:border-b-0 bg-red-50 -mx-3 px-3 py-2 rounded opacity-60"
                    >
                      <div className="w-12">
                        <span className="inline-block px-2 py-1 bg-gray-100 rounded text-xs font-medium">
                          {denom.languageDescription || denom.language || ''}
                        </span>
                      </div>
                      <div className="flex-1">
                        <p className="text-sm text-gray-600">
                          Type: {denom.typeCode}
                          {denom.typeDescription && ` - ${denom.typeDescription}`}
                        </p>
                        <p className="font-medium line-through">{denom.denomination}</p>
                      </div>
                      <div className="flex items-center">
                        <span className="text-xs bg-red-500 text-white px-2 py-1 rounded">Removed in selected</span>
                      </div>
                    </div>
                  ))}
                </>
              )}
              {/* Show unchanged denominations from selected snapshot */}
              {displayDetail.denominations.map((denom, idx) => {
                const isAdded = comparison?.denominations.added.some(
                  (d: typeof denom) =>
                    d.language === denom.language &&
                    d.typeCode === denom.typeCode &&
                    d.denomination === denom.denomination
                )
                // Don't show if it's in the "added" list (will be shown separately below)
                if (isAdded) return null

                return (
                  <div key={idx} className="flex gap-4 pb-3 border-b last:border-b-0">
                    <div className="w-12">
                      <span className="inline-block px-2 py-1 bg-gray-100 rounded text-xs font-medium">
                        {denom.languageDescription || denom.language || ''}
                      </span>
                    </div>
                    <div className="flex-1">
                      <p className="text-sm text-gray-600">
                        Type: {denom.typeCode}
                        {denom.typeDescription && ` - ${denom.typeDescription}`}
                      </p>
                      <p className="font-medium">{denom.denomination}</p>
                    </div>
                  </div>
                )
              })}
              {/* Show added denominations from selected snapshot */}
              {comparison && comparison.denominations.added.length > 0 && (
                <>
                  {comparison.denominations.added.map((denom: typeof displayDetail.denominations[0], idx: number) => (
                    <div
                      key={`added-${idx}`}
                      className="flex gap-4 pb-3 border-b last:border-b-0 bg-green-50 -mx-3 px-3 py-2 rounded"
                    >
                      <div className="w-12">
                        <span className="inline-block px-2 py-1 bg-gray-100 rounded text-xs font-medium">
                          {denom.languageDescription || denom.language || ''}
                        </span>
                      </div>
                      <div className="flex-1">
                        <p className="text-sm text-gray-600">
                          Type: {denom.typeCode}
                          {denom.typeDescription && ` - ${denom.typeDescription}`}
                        </p>
                        <p className="font-medium">{denom.denomination}</p>
                      </div>
                      <div className="flex items-center">
                        <span className="text-xs bg-green-500 text-white px-2 py-1 rounded">New in selected</span>
                      </div>
                    </div>
                  ))}
                </>
              )}
            </div>
          </div>
        )}

        {/* Addresses */}
        {displayDetail && (displayDetail.addresses.length > 0 || (comparison && comparison.addresses.removed.length > 0)) && (
          <div className="bg-white rounded-lg border p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-semibold">Addresses</h2>
              {comparison && (comparison.addresses.added.length > 0 || comparison.addresses.removed.length > 0) && (
                <div className="flex gap-3 text-xs">
                  {comparison.addresses.added.length > 0 && (
                    <span className="bg-green-100 text-green-800 px-2 py-1 rounded">
                      +{comparison.addresses.added.length} New in selected
                    </span>
                  )}
                  {comparison.addresses.removed.length > 0 && (
                    <span className="bg-red-100 text-red-800 px-2 py-1 rounded">
                      -{comparison.addresses.removed.length} Removed in selected
                    </span>
                  )}
                </div>
              )}
            </div>
            <div className="space-y-4">
              {/* Show removed addresses from previous snapshot */}
              {comparison && comparison.addresses.removed.length > 0 && (
                <>
                  {comparison.addresses.removed.map((addr: typeof displayDetail.addresses[0], idx: number) => (
                    <div
                      key={`removed-${idx}`}
                      className="pb-4 border-b last:border-b-0 bg-red-50 -mx-3 px-3 py-2 rounded opacity-60"
                    >
                      <div className="flex items-start justify-between mb-2">
                        <p className="text-sm text-gray-600">
                          Type: {addr.typeCode}
                          {addr.dateStrikingOff && ` (Struck off: ${addr.dateStrikingOff})`}
                        </p>
                        <span className="text-xs bg-red-500 text-white px-2 py-1 rounded ml-2">Removed in selected</span>
                      </div>
                      <div className="font-medium line-through">
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
                </>
              )}
              {/* Show unchanged addresses from selected snapshot */}
              {displayDetail.addresses.map((addr, idx) => {
                const key = `${addr.typeCode}-${addr.streetNL}-${addr.zipcode}`
                const isAdded = comparison?.addresses.added.some(
                  (a: typeof addr) => `${a.typeCode}-${a.streetNL}-${a.zipcode}` === key
                )
                // Don't show if it's in the "added" list (will be shown separately below)
                if (isAdded) return null

                return (
                  <div key={idx} className="pb-4 border-b last:border-b-0">
                    <div className="flex items-start justify-between mb-2">
                      <p className="text-sm text-gray-600">
                        Type: {addr.typeCode}
                        {addr.dateStrikingOff && ` (Struck off: ${addr.dateStrikingOff})`}
                      </p>
                    </div>
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
                )
              })}
              {/* Show added addresses from selected snapshot */}
              {comparison && comparison.addresses.added.length > 0 && (
                <>
                  {comparison.addresses.added.map((addr: typeof displayDetail.addresses[0], idx: number) => (
                    <div
                      key={`added-${idx}`}
                      className="pb-4 border-b last:border-b-0 bg-green-50 -mx-3 px-3 py-2 rounded"
                    >
                      <div className="flex items-start justify-between mb-2">
                        <p className="text-sm text-gray-600">
                          Type: {addr.typeCode}
                          {addr.dateStrikingOff && ` (Struck off: ${addr.dateStrikingOff})`}
                        </p>
                        <span className="text-xs bg-green-500 text-white px-2 py-1 rounded ml-2">New in selected</span>
                      </div>
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
                      </div>
                    </div>
                  ))}
                </>
              )}
            </div>
          </div>
        )}

        {/* Activities */}
        {displayDetail && (displayDetail.activities.length > 0 || (comparison && comparison.activities.removed.length > 0)) && (
          <div className="bg-white rounded-lg border p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-semibold">Economic Activities</h2>
              {comparison && (comparison.activities.added.length > 0 || comparison.activities.removed.length > 0) && (
                <div className="flex gap-3 text-xs">
                  {comparison.activities.added.length > 0 && (
                    <span className="bg-green-100 text-green-800 px-2 py-1 rounded">
                      +{comparison.activities.added.length} New in selected
                    </span>
                  )}
                  {comparison.activities.removed.length > 0 && (
                    <span className="bg-red-100 text-red-800 px-2 py-1 rounded">
                      -{comparison.activities.removed.length} Removed in selected
                    </span>
                  )}
                </div>
              )}
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {/* Combine baseline (displayDetail), added activities, and removed activities, grouped by version */}
              {(Object.entries(
                [...displayDetail.activities, ...(comparison?.activities.added || []), ...(comparison?.activities.removed || [])].reduce((acc, activity) => {
                  const version = activity.naceVersion
                  if (!acc[version]) acc[version] = []
                  acc[version].push(activity)
                  return acc
                }, {} as Record<string, Array<typeof displayDetail.activities[0]>>)
              ) as [string, Array<typeof displayDetail.activities[0]>][])
                .sort(([a], [b]) => b.localeCompare(a)) // Sort versions descending (2025, 2008, 2003)
                .map(([version, activities]) => (
                  <div key={version}>
                    <h3 className="text-sm font-semibold text-gray-700 mb-2">
                      NACE {version}
                    </h3>
                    <div className="space-y-1">
                      {activities.map((activity: typeof displayDetail.activities[0], idx: number) => {
                        const key = `${activity.activityGroup}-${activity.naceVersion}-${activity.naceCode}-${activity.classification}`
                        const isRemoved = comparison?.activities.removed.some(
                          (a: typeof activity) => `${a.activityGroup}-${a.naceVersion}-${a.naceCode}-${a.classification}` === key
                        )
                        const isAdded = comparison?.activities.added.some(
                          (a: typeof activity) => `${a.activityGroup}-${a.naceVersion}-${a.naceCode}-${a.classification}` === key
                        )
                        return (
                          <div
                            key={idx}
                            className={`flex flex-col text-sm py-1.5 border-b border-gray-100 last:border-0 ${
                              isAdded ? 'bg-green-50 -mx-2 px-2 rounded' : isRemoved ? 'bg-red-50 -mx-2 px-2 rounded opacity-60' : ''
                            }`}
                          >
                            <div className="flex items-center justify-between">
                              <span className={`font-mono font-medium text-gray-900 ${isRemoved ? 'line-through' : ''}`}>
                                {activity.naceCode}
                              </span>
                              {isAdded && (
                                <span className="text-xs bg-green-500 text-white px-1.5 py-0.5 rounded">New in selected</span>
                              )}
                              {isRemoved && (
                                <span className="text-xs bg-red-500 text-white px-1.5 py-0.5 rounded">Removed in selected</span>
                              )}
                            </div>
                            {activity.naceDescriptionNL && (
                              <span className={`text-gray-700 text-xs mt-0.5 ${isRemoved ? 'line-through' : ''}`}>
                                {activity.naceDescriptionNL}
                              </span>
                            )}
                            <span className="text-gray-500 text-xs mt-0.5">
                              {activity.classification} · {activity.activityGroup}
                            </span>
                          </div>
                        )
                      })}
                    </div>
                  </div>
                ))}
            </div>
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">

          {/* Contacts */}
          {displayDetail && (displayDetail.contacts.length > 0 || (comparison && comparison.contacts.removed.length > 0)) && (
            <div className="bg-white rounded-lg border p-6">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold">Contact Information</h2>
                {comparison && (comparison.contacts.added.length > 0 || comparison.contacts.removed.length > 0) && (
                  <div className="flex gap-3 text-xs">
                    {comparison.contacts.added.length > 0 && (
                      <span className="bg-green-100 text-green-800 px-2 py-1 rounded">
                        +{comparison.contacts.added.length}
                      </span>
                    )}
                    {comparison.contacts.removed.length > 0 && (
                      <span className="bg-red-100 text-red-800 px-2 py-1 rounded">
                        -{comparison.contacts.removed.length}
                      </span>
                    )}
                  </div>
                )}
              </div>
              <div className="space-y-3">
                {/* Show removed contacts from previous snapshot */}
                {comparison && comparison.contacts.removed.length > 0 && (
                  <>
                    {comparison.contacts.removed.map((contact: typeof displayDetail.contacts[0], idx: number) => (
                      <div
                        key={`removed-${idx}`}
                        className="pb-3 border-b last:border-b-0 bg-red-50 -mx-3 px-3 py-2 rounded opacity-60"
                      >
                        <div className="flex items-center justify-between">
                          <p className="text-sm text-gray-600">{contact.contactType}</p>
                          <span className="text-xs bg-red-500 text-white px-2 py-1 rounded">Removed in selected</span>
                        </div>
                        <p className="font-medium break-all line-through">{contact.value}</p>
                      </div>
                    ))}
                  </>
                )}
                {/* Show unchanged contacts from selected snapshot */}
                {displayDetail.contacts.map((contact, idx) => {
                  const key = `${contact.contactType}-${contact.value}`
                  const isAdded = comparison?.contacts.added.some(
                    (c: typeof contact) => `${c.contactType}-${c.value}` === key
                  )
                  // Don't show if it's in the "added" list (will be shown separately below)
                  if (isAdded) return null

                  return (
                    <div key={idx} className="pb-3 border-b last:border-b-0">
                      <div className="flex items-center justify-between">
                        <p className="text-sm text-gray-600">{contact.contactType}</p>
                      </div>
                      <p className="font-medium break-all">{contact.value}</p>
                    </div>
                  )
                })}
                {/* Show added contacts from selected snapshot */}
                {comparison && comparison.contacts.added.length > 0 && (
                  <>
                    {comparison.contacts.added.map((contact: typeof displayDetail.contacts[0], idx: number) => (
                      <div
                        key={`added-${idx}`}
                        className="pb-3 border-b last:border-b-0 bg-green-50 -mx-3 px-3 py-2 rounded"
                      >
                        <div className="flex items-center justify-between">
                          <p className="text-sm text-gray-600">{contact.contactType}</p>
                          <span className="text-xs bg-green-500 text-white px-2 py-1 rounded">New in selected</span>
                        </div>
                        <p className="font-medium break-all">{contact.value}</p>
                      </div>
                    ))}
                  </>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Establishments */}
        {displayDetail && (displayDetail.establishments.length > 0 || (comparison && comparison.establishments.removed.length > 0)) && (
          <div className="bg-white rounded-lg border p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-semibold">
                Establishments ({displayDetail.establishments.length + (comparison?.establishments.added.length || 0)})
              </h2>
              {comparison && (comparison.establishments.added.length > 0 || comparison.establishments.removed.length > 0) && (
                <div className="flex gap-3 text-xs">
                  {comparison.establishments.added.length > 0 && (
                    <span className="bg-green-100 text-green-800 px-2 py-1 rounded">
                      +{comparison.establishments.added.length} New in selected
                    </span>
                  )}
                  {comparison.establishments.removed.length > 0 && (
                    <span className="bg-red-100 text-red-800 px-2 py-1 rounded">
                      -{comparison.establishments.removed.length} Removed in selected
                    </span>
                  )}
                </div>
              )}
            </div>
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
                    <th className="text-left px-4 py-3 text-sm font-medium text-gray-600">
                      Status
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {/* Show removed establishments from previous snapshot */}
                  {comparison && comparison.establishments.removed.length > 0 && (
                    <>
                      {comparison.establishments.removed.map((est: typeof displayDetail.establishments[0]) => (
                        <tr key={`removed-${est.establishmentNumber}`} className="hover:bg-gray-50 bg-red-50 opacity-60">
                          <td className="px-4 py-3 text-sm font-mono line-through">
                            {est.establishmentNumber}
                          </td>
                          <td className="px-4 py-3 text-sm line-through">{est.primaryName || '-'}</td>
                          <td className="px-4 py-3 text-sm line-through">{est.startDate || '-'}</td>
                          <td className="px-4 py-3 text-sm">
                            <span className="text-xs bg-red-500 text-white px-2 py-1 rounded">Removed in selected</span>
                          </td>
                        </tr>
                      ))}
                    </>
                  )}
                  {/* Show unchanged establishments from selected snapshot */}
                  {displayDetail.establishments.map((est) => {
                    const isAdded = comparison?.establishments.added.some(
                      (e: typeof est) => e.establishmentNumber === est.establishmentNumber
                    )
                    // Don't show if it's in the "added" list (will be shown separately below)
                    if (isAdded) return null

                    return (
                      <tr key={est.establishmentNumber} className="hover:bg-gray-50">
                        <td className="px-4 py-3 text-sm font-mono">{est.establishmentNumber}</td>
                        <td className="px-4 py-3 text-sm">{est.primaryName || '-'}</td>
                        <td className="px-4 py-3 text-sm">{est.startDate || '-'}</td>
                        <td className="px-4 py-3 text-sm"></td>
                      </tr>
                    )
                  })}
                  {/* Show added establishments from selected snapshot */}
                  {comparison && comparison.establishments.added.length > 0 && (
                    <>
                      {comparison.establishments.added.map((est: typeof displayDetail.establishments[0]) => (
                        <tr
                          key={`added-${est.establishmentNumber}`}
                          className="hover:bg-gray-50 bg-green-50"
                        >
                          <td className="px-4 py-3 text-sm font-mono">
                            {est.establishmentNumber}
                          </td>
                          <td className="px-4 py-3 text-sm">{est.primaryName || '-'}</td>
                          <td className="px-4 py-3 text-sm">{est.startDate || '-'}</td>
                          <td className="px-4 py-3 text-sm">
                            <span className="text-xs bg-green-500 text-white px-2 py-1 rounded">New in selected</span>
                          </td>
                        </tr>
                      ))}
                    </>
                  )}
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
