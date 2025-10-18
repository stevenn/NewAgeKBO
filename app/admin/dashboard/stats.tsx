import { getDatabaseStats } from '@/lib/motherduck/stats'

export async function DatabaseStats() {
  const stats = await getDatabaseStats()

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
          <p className="mt-1 text-sm text-gray-500">{stats.lastUpdate}</p>
        </div>

        <div className="rounded-lg border bg-white p-6">
          <h3 className="text-sm font-medium text-gray-600">Database Size</h3>
          <p className="mt-2 text-3xl font-bold">{stats.databaseSize}</p>
          <p className="mt-1 text-sm text-gray-500">Parquet + ZSTD</p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="rounded-lg border bg-white p-6">
          <h2 className="text-xl font-semibold mb-4">System Status</h2>
          <div className="space-y-2">
            <div className="flex justify-between">
              <span className="text-gray-600">Motherduck Connection</span>
              <span className="text-green-600 font-medium">Connected</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Last Update</span>
              <span className="font-medium">{stats.lastUpdate}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Total Records</span>
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
    </>
  )
}
