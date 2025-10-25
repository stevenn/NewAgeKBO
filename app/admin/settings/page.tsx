'use client'

import { useState, useEffect } from 'react'

export default function SettingsPage() {
  const [config, setConfig] = useState({
    motherduckDatabase: '',
    dataRetentionMonths: 24,
    autoUpdateEnabled: false,
    notificationsEnabled: true,
  })

  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    // Fetch actual config from API
    fetch('/api/config')
      .then((res) => res.json())
      .then((data) => {
        setConfig((prev) => ({
          ...prev,
          motherduckDatabase: data.motherduckDatabase,
        }))
      })
      .catch((err) => {
        console.error('Failed to load config:', err)
      })
      .finally(() => {
        setLoading(false)
      })
  }, [])

  const handleSave = async () => {
    setSaving(true)
    setSaved(false)

    // Simulate save - in production this would save to database or config
    await new Promise((resolve) => setTimeout(resolve, 1000))

    setSaving(false)
    setSaved(true)

    setTimeout(() => setSaved(false), 3000)
  }

  return (
    <div>
      <h1 className="text-3xl font-bold mb-6">Settings</h1>

      <div className="space-y-6">
        {/* Database Configuration */}
        <div className="bg-white rounded-lg border p-6">
          <h2 className="text-xl font-semibold mb-4">Database Configuration</h2>
          <div className="space-y-4">
            <div>
              <label
                htmlFor="database"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Motherduck Database Name
              </label>
              <input
                id="database"
                type="text"
                value={loading ? 'Loading...' : config.motherduckDatabase}
                onChange={(e) =>
                  setConfig({ ...config, motherduckDatabase: e.target.value })
                }
                className="w-full max-w-md rounded-lg border border-gray-300 px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                disabled
              />
              <p className="text-sm text-gray-500 mt-1">
                Currently configured via environment variables
              </p>
            </div>

            <div>
              <label
                htmlFor="retention"
                className="block text-sm font-medium text-gray-700 mb-2"
              >
                Data Retention Period (months)
              </label>
              <input
                id="retention"
                type="number"
                value={config.dataRetentionMonths}
                onChange={(e) =>
                  setConfig({
                    ...config,
                    dataRetentionMonths: parseInt(e.target.value),
                  })
                }
                className="w-full max-w-md rounded-lg border border-gray-300 px-4 py-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
                min={1}
                max={120}
                disabled
              />
              <p className="text-sm text-gray-500 mt-1">
                Historical data older than this will be automatically cleaned up
              </p>
            </div>
          </div>
        </div>

        {/* Import Configuration */}
        <div className="bg-white rounded-lg border p-6">
          <h2 className="text-xl font-semibold mb-4">Import Configuration</h2>
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="font-medium">Automatic Daily Updates</h3>
                <p className="text-sm text-gray-600">
                  Automatically download and apply daily updates from KBO
                </p>
              </div>
              <button
                onClick={() =>
                  setConfig({
                    ...config,
                    autoUpdateEnabled: !config.autoUpdateEnabled,
                  })
                }
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                  config.autoUpdateEnabled ? 'bg-blue-600' : 'bg-gray-200'
                }`}
              >
                <span
                  className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                    config.autoUpdateEnabled ? 'translate-x-6' : 'translate-x-1'
                  }`}
                />
              </button>
            </div>

            <div className="flex items-center justify-between">
              <div>
                <h3 className="font-medium">Email Notifications</h3>
                <p className="text-sm text-gray-600">
                  Receive email notifications about import job status
                </p>
              </div>
              <button
                onClick={() =>
                  setConfig({
                    ...config,
                    notificationsEnabled: !config.notificationsEnabled,
                  })
                }
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                  config.notificationsEnabled ? 'bg-blue-600' : 'bg-gray-200'
                }`}
              >
                <span
                  className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                    config.notificationsEnabled ? 'translate-x-6' : 'translate-x-1'
                  }`}
                />
              </button>
            </div>
          </div>
        </div>

        {/* System Information */}
        <div className="bg-white rounded-lg border p-6">
          <h2 className="text-xl font-semibold mb-4">System Information</h2>
          <div className="space-y-3">
            <div className="flex justify-between py-2 border-b">
              <span className="text-gray-600">Application Version</span>
              <span className="font-medium">1.0.0-alpha</span>
            </div>
            <div className="flex justify-between py-2 border-b">
              <span className="text-gray-600">Database Type</span>
              <span className="font-medium">Motherduck (DuckDB)</span>
            </div>
            <div className="flex justify-between py-2 border-b">
              <span className="text-gray-600">Deployment</span>
              <span className="font-medium">Vercel</span>
            </div>
            <div className="flex justify-between py-2">
              <span className="text-gray-600">Framework</span>
              <span className="font-medium">Next.js 15</span>
            </div>
          </div>
        </div>

        {/* Save Button */}
        <div className="flex items-center gap-4">
          <button
            onClick={handleSave}
            disabled={saving}
            className="bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
          >
            {saving ? 'Saving...' : 'Save Settings'}
          </button>
          {saved && (
            <span className="text-green-600 text-sm font-medium">
              Settings saved successfully!
            </span>
          )}
        </div>

        {/* Note */}
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4">
          <div className="flex items-start gap-2">
            <svg
              className="w-5 h-5 text-yellow-600 mt-0.5"
              fill="none"
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth="2"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <div className="text-sm text-yellow-800">
              <p className="font-medium">Note: Settings Persistence</p>
              <p className="mt-1">
                Settings persistence is not yet fully implemented. Changes made here are
                stored in local state only. In a future update, settings will be persisted
                to the database and respected by import jobs and scheduled tasks.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
