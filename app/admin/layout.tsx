import { UserButton } from '@clerk/nextjs'
import Link from 'next/link'

export default function AdminLayout({
  children,
}: {
  children: React.ReactNode
}) {
  // Role check handled by middleware
  return (
    <div className="flex min-h-screen">
      {/* Sidebar */}
      <aside className="w-64 border-r bg-gray-50 p-6">
        <div className="mb-8">
          <h1 className="text-xl font-bold">CBE Admin</h1>
          <p className="text-sm text-gray-600">Data Platform for the New Age</p>
        </div>

        <nav className="space-y-2">
          <Link
            href="/admin/dashboard"
            className="block rounded-lg px-4 py-2 text-gray-700 hover:bg-gray-100"
          >
            Dashboard
          </Link>
          <Link
            href="/admin/browse"
            className="block rounded-lg px-4 py-2 text-gray-700 hover:bg-gray-100"
          >
            Browse Data
          </Link>
          <Link
            href="/admin/imports"
            className="block rounded-lg px-4 py-2 text-gray-700 hover:bg-gray-100"
          >
            Import Jobs
          </Link>
          <Link
            href="/admin/settings"
            className="block rounded-lg px-4 py-2 text-gray-700 hover:bg-gray-100"
          >
            Settings
          </Link>
        </nav>

        <div className="mt-auto pt-8">
          <div className="flex items-center gap-3">
            <UserButton afterSignOutUrl="/" />
            <span className="text-sm text-gray-600">Account</span>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 p-8">
        {children}
      </main>
    </div>
  )
}
