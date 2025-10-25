import Link from 'next/link'
import { auth } from '@clerk/nextjs/server'
import { redirect } from 'next/navigation'

export default async function Home() {
  const { userId } = await auth()

  // If user is signed in, redirect to admin dashboard
  if (userId) {
    redirect('/admin/dashboard')
  }

  return (
    <main className="min-h-screen p-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-4xl font-bold mb-4">KBO/BCE/CBE for the New Age</h1>
        <p className="text-gray-600 mb-8">
          Modern administration interface for Belgian Crossroads Bank for Enterprises Open Data
        </p>

        <div className="space-y-4">
          <div className="bg-white border rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-2">Features</h2>
            <ul className="space-y-2 text-gray-700">
              <li>• Browse 1.9M+ Belgian enterprises</li>
              <li>• Search by name, number, or activity</li>
              <li>• View historical data with temporal tracking</li>
              <li>• Manage daily and monthly data imports</li>
            </ul>
          </div>

          <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
            <h2 className="text-xl font-semibold mb-2">Admin Access</h2>
            <p className="text-gray-700 mb-4">
              Sign in to access the admin interface and manage KBO data.
            </p>
            <Link
              href="/sign-in"
              className="inline-block bg-blue-600 text-white px-6 py-2 rounded-lg hover:bg-blue-700"
            >
              Sign In
            </Link>
          </div>
        </div>
      </div>
    </main>
  )
}
