import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'

export interface EnterpriseDetail {
  enterpriseNumber: string
  status: string
  statusDescription: string | null
  juridicalForm: string | null
  juridicalFormDescription: string | null
  juridicalSituation: string | null
  juridicalSituationDescription: string | null
  typeOfEnterprise: string | null
  typeOfEnterpriseDescription: string | null
  startDate: string | null
  snapshotDate: string
  extractNumber: number

  denominations: Denomination[]
  addresses: Address[]
  activities: Activity[]
  contacts: Contact[]
  establishments: Establishment[]
}

export interface Denomination {
  language: string
  typeCode: string
  denomination: string
}

export interface Address {
  typeCode: string
  countryNL: string | null
  countryFR: string | null
  zipcode: string | null
  municipalityNL: string | null
  municipalityFR: string | null
  streetNL: string | null
  streetFR: string | null
  houseNumber: string | null
  box: string | null
  extraAddressInfo: string | null
  dateStrikingOff: string | null
}

export interface Activity {
  entityNumber: string
  activityGroup: string
  naceVersion: string
  naceCode: string
  naceDescriptionNL: string | null
  naceDescriptionFR: string | null
  classification: string
}

export interface Contact {
  entityNumber: string
  contactType: string
  value: string
}

export interface Establishment {
  establishmentNumber: string
  startDate: string | null
  primaryName: string | null
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ number: string }> }
) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    const { number } = await params

    // Parse query parameters for temporal navigation
    const { searchParams } = new URL(request.url)
    const snapshotDate = searchParams.get('snapshot_date')
    const extractNumber = searchParams.get('extract_number')

    // Build temporal filter object
    const filter = extractNumber
      ? {
          type: 'point-in-time' as const,
          extractNumber: parseInt(extractNumber),
          snapshotDate: snapshotDate || undefined,
        }
      : { type: 'current' as const }

    // Connect to Motherduck
    const db = await connectMotherduck()

    try {
      const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
      await executeQuery(db, `USE ${dbName}`)

      // Use shared helper function to fetch enterprise details
      const { fetchEnterpriseDetail } = await import('@/lib/motherduck/enterprise-detail')
      const detail = await fetchEnterpriseDetail(db, number, filter)

      if (!detail) {
        await closeMotherduck(db)
        return NextResponse.json({ error: 'Enterprise not found' }, { status: 404 })
      }

      await closeMotherduck(db)
      return NextResponse.json(detail)
    } catch (error) {
      await closeMotherduck(db)
      throw error
    }
  } catch (error) {
    console.error('Failed to fetch enterprise details:', error)
    return NextResponse.json({ error: 'Failed to fetch enterprise details' }, { status: 500 })
  }
}
