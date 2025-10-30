import { DatabaseStats } from './stats'

export default function DashboardPage() {
  return (
    <div>
      <h1 className="text-3xl font-bold mb-6">Dashboard</h1>
      <DatabaseStats />
    </div>
  )
}
