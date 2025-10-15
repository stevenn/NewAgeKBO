export default function Home() {
  return (
    <main className="min-h-screen p-8">
      <div className="max-w-4xl mx-auto">
        <h1 className="text-4xl font-bold mb-4">KBO for the New Age</h1>
        <p className="text-gray-600 mb-8">
          Modern administration interface for KBO Open Data
        </p>

        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
          <h2 className="text-xl font-semibold mb-2">Setup Required</h2>
          <p className="text-gray-700">
            This application requires Motherduck, Vercel, and Clerk accounts to be configured.
            Please complete the initial setup before proceeding.
          </p>
        </div>
      </div>
    </main>
  )
}
