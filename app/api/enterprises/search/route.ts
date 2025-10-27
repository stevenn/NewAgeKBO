import { NextResponse } from 'next/server'
import { checkAdminAccess } from '@/lib/auth/check-admin'
import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'
import { getJuridicalFormDescription } from '@/lib/cache/codes'

export interface EnterpriseSearchResult {
  enterpriseNumber: string
  primaryName: string
  juridicalForm: string | null
  juridicalFormDescription: string | null
  status: string
  startDate: string | null
  address: string | null
  municipality: string | null
}

export async function GET(request: Request) {
  try {
    // Check authentication and admin role
    const authError = await checkAdminAccess()
    if (authError) return authError

    // Parse search parameters
    const { searchParams } = new URL(request.url)
    const query = searchParams.get('q') || ''
    const searchType = searchParams.get('type') || 'all' // all, number, name, nace
    const limit = Math.min(Math.max(parseInt(searchParams.get('limit') || '50') || 50, 1), 200)
    const offset = Math.max(parseInt(searchParams.get('offset') || '0') || 0, 0)

    if (!query && searchType !== 'all') {
      return NextResponse.json({ results: [], total: 0 })
    }

    // Connect to Motherduck
    const db = await connectMotherduck()

    try {
      const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
      await executeQuery(db, `USE ${dbName}`)

      let searchSql = ''
      let countSql = ''

      // Build query based on search type
      if (searchType === 'number') {
        // Search by enterprise number (remove dots and spaces for flexible matching)
        const cleanNumber = query.replace(/[.\s]/g, '')
        searchSql = `
          SELECT
            e.enterprise_number,
            e.primary_name,
            e.juridical_form,
            e.status,
            e.start_date,
            a.street_nl as address,
            a.municipality_nl as municipality
          FROM enterprises_current e
          LEFT JOIN addresses_current a ON e.enterprise_number = a.entity_number
            AND a.type_of_address = 'REGO'
          WHERE REPLACE(REPLACE(e.enterprise_number, '.', ''), ' ', '') LIKE '%${cleanNumber}%'
          ORDER BY e.enterprise_number
          LIMIT ${limit} OFFSET ${offset}
        `
        countSql = `
          SELECT COUNT(*) as count
          FROM enterprises_current
          WHERE REPLACE(REPLACE(enterprise_number, '.', ''), ' ', '') LIKE '%${cleanNumber}%'
        `
      } else if (searchType === 'name') {
        // Search by denomination (case-insensitive)
        const searchTerm = query.toLowerCase()
        searchSql = `
          SELECT DISTINCT
            e.enterprise_number,
            e.primary_name,
            e.juridical_form,
            e.status,
            e.start_date,
            a.street_nl as address,
            a.municipality_nl as municipality
          FROM enterprises_current e
          LEFT JOIN denominations_current d ON e.enterprise_number = d.entity_number
          LEFT JOIN addresses_current a ON e.enterprise_number = a.entity_number
            AND a.type_of_address = 'REGO'
          WHERE LOWER(d.denomination) LIKE '%${searchTerm}%'
          ORDER BY e.primary_name
          LIMIT ${limit} OFFSET ${offset}
        `
        countSql = `
          SELECT COUNT(DISTINCT e.enterprise_number) as count
          FROM enterprises_current e
          LEFT JOIN denominations_current d ON e.enterprise_number = d.entity_number
          WHERE LOWER(d.denomination) LIKE '%${searchTerm}%'
        `
      } else if (searchType === 'nace') {
        // Search by NACE code
        const naceCode = query.replace(/\./g, '')
        searchSql = `
          SELECT DISTINCT
            e.enterprise_number,
            e.primary_name,
            e.juridical_form,
            e.status,
            e.start_date,
            a.street_nl as address,
            a.municipality_nl as municipality
          FROM enterprises_current e
          INNER JOIN activities_current act ON e.enterprise_number = act.entity_number
          LEFT JOIN addresses_current a ON e.enterprise_number = a.entity_number
            AND a.type_of_address = 'REGO'
          WHERE REPLACE(act.nace_code, '.', '') LIKE '${naceCode}%'
          ORDER BY e.primary_name
          LIMIT ${limit} OFFSET ${offset}
        `
        countSql = `
          SELECT COUNT(DISTINCT e.enterprise_number) as count
          FROM enterprises_current e
          INNER JOIN activities_current act ON e.enterprise_number = act.entity_number
          WHERE REPLACE(act.nace_code, '.', '') LIKE '${naceCode}%'
        `
      } else {
        // Search all: if query provided, search across number and name; otherwise list all
        if (query) {
          const searchTerm = query.toLowerCase()
          const cleanNumber = query.replace(/[.\s]/g, '')
          searchSql = `
            SELECT DISTINCT
              e.enterprise_number,
              e.primary_name,
              e.juridical_form,
              e.status,
              e.start_date,
              a.street_nl as address,
              a.municipality_nl as municipality
            FROM enterprises_current e
            LEFT JOIN denominations_current d ON e.enterprise_number = d.entity_number
            LEFT JOIN addresses_current a ON e.enterprise_number = a.entity_number
              AND a.type_of_address = 'REGO'
            WHERE (
              REPLACE(REPLACE(e.enterprise_number, '.', ''), ' ', '') LIKE '%${cleanNumber}%'
              OR LOWER(d.denomination) LIKE '%${searchTerm}%'
            )
            ORDER BY e.primary_name
            LIMIT ${limit} OFFSET ${offset}
          `
          countSql = `
            SELECT COUNT(DISTINCT e.enterprise_number) as count
            FROM enterprises_current e
            LEFT JOIN denominations_current d ON e.enterprise_number = d.entity_number
            WHERE (
              REPLACE(REPLACE(e.enterprise_number, '.', ''), ' ', '') LIKE '%${cleanNumber}%'
              OR LOWER(d.denomination) LIKE '%${searchTerm}%'
            )
          `
        } else {
          // No query: list all enterprises with pagination
          searchSql = `
            SELECT
              e.enterprise_number,
              e.primary_name,
              e.juridical_form,
              e.status,
              e.start_date,
              a.street_nl as address,
              a.municipality_nl as municipality
            FROM enterprises_current e
            LEFT JOIN addresses_current a ON e.enterprise_number = a.entity_number
              AND a.type_of_address = 'REGO'
            ORDER BY e.enterprise_number
            LIMIT ${limit} OFFSET ${offset}
          `
          countSql = `
            SELECT COUNT(*) as count
            FROM enterprises_current
          `
        }
      }

      // Execute search and count queries in parallel
      const [results, countResult] = await Promise.all([
        executeQuery<{
          enterprise_number: string
          primary_name: string
          juridical_form: string | null
          status: string
          start_date: string | null
          address: string | null
          municipality: string | null
        }>(db, searchSql),
        executeQuery<{ count: number }>(db, countSql),
      ])

      const total = Number(countResult[0].count)

      // Enrich results with juridical form descriptions from cache
      const formattedResults: EnterpriseSearchResult[] = await Promise.all(
        results.map(async (row) => ({
          enterpriseNumber: row.enterprise_number,
          primaryName: row.primary_name || 'Unknown',
          juridicalForm: row.juridical_form,
          juridicalFormDescription: await getJuridicalFormDescription(row.juridical_form),
          status: row.status,
          startDate: row.start_date,
          address: row.address,
          municipality: row.municipality,
        }))
      )

      return NextResponse.json({
        results: formattedResults,
        total,
        limit,
        offset,
      })
    } finally {
      await closeMotherduck(db)
    }
  } catch (error) {
    console.error('Failed to search enterprises:', error)
    return NextResponse.json(
      { error: 'Failed to search enterprises' },
      { status: 500 }
    )
  }
}
