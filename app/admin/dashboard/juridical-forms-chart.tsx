'use client'

import { useState } from 'react'
import type { JuridicalFormStat } from '@/lib/motherduck/stats'

interface JuridicalFormsChartProps {
  forms: JuridicalFormStat[]
}

export function JuridicalFormsChart({ forms }: JuridicalFormsChartProps) {
  const [visibleCount, setVisibleCount] = useState(10)
  const maxCount = forms[0]?.count || 1

  const visibleForms = forms.slice(0, visibleCount)
  const hasMore = visibleCount < forms.length
  const isExpanded = visibleCount > 10

  const loadMore = () => {
    setVisibleCount((prev) => Math.min(prev + 20, forms.length))
  }

  const collapse = () => {
    setVisibleCount(10)
  }

  return (
    <>
      <div className="space-y-3">
        {visibleForms.map((form, index) => {
          const percentage = (form.count / maxCount) * 100
          const isNaturalPerson = form.code === 'NATURAL_PERSON'
          return (
            <div key={form.code} className="space-y-1">
              <div className="flex justify-between items-baseline text-sm">
                <div className="flex items-baseline gap-2">
                  <span className="text-gray-500 font-mono text-xs">#{index + 1}</span>
                  <span className="font-medium">{form.description || form.code}</span>
                  {!isNaturalPerson && (
                    <span className="text-gray-400 text-xs">({form.code})</span>
                  )}
                </div>
                <span className="font-medium tabular-nums">{form.count.toLocaleString()}</span>
              </div>
              <div className="relative h-2 bg-gray-100 rounded-full overflow-hidden">
                <div
                  className={`absolute h-full rounded-full transition-all ${
                    isNaturalPerson ? 'bg-blue-300' : 'bg-blue-500'
                  }`}
                  style={{ width: `${percentage}%` }}
                />
              </div>
            </div>
          )
        })}
      </div>

      <div className="mt-4 flex gap-2">
        {hasMore && (
          <button
            onClick={loadMore}
            className="flex-1 py-2 px-4 bg-gray-50 hover:bg-gray-100 border border-gray-200 rounded-md text-sm font-medium text-gray-700 transition-colors"
          >
            Show more ({forms.length - visibleCount} remaining)
          </button>
        )}
        {isExpanded && (
          <button
            onClick={collapse}
            className="flex-1 py-2 px-4 bg-gray-50 hover:bg-gray-100 border border-gray-200 rounded-md text-sm font-medium text-gray-700 transition-colors"
          >
            Collapse
          </button>
        )}
      </div>

      <p className="mt-4 text-xs text-gray-500">
        Total juridical forms: {forms.length} (including natural persons)
      </p>
    </>
  )
}
