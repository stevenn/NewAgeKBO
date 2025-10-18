import { NextResponse } from 'next/server'
import { auth } from '@clerk/nextjs/server'
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
    // Check authentication
    const { userId } = await auth()
    if (!userId) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 })
    }

    const { number } = await params

    // Parse query parameters for temporal navigation
    const { searchParams } = new URL(request.url)
    const snapshotDate = searchParams.get('snapshot_date')
    const extractNumber = searchParams.get('extract_number')

    // Build temporal filter condition
    let temporalFilter = ''
    if (snapshotDate && extractNumber) {
      temporalFilter = `AND _snapshot_date = '${snapshotDate}' AND _extract_number = ${extractNumber}`
    } else {
      temporalFilter = 'AND _is_current = true'
    }

    // Connect to Motherduck
    const db = await connectMotherduck()

    try {
      const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
      await executeQuery(db, `USE ${dbName}`)

      // Fetch enterprise details with code lookups
      const enterprises = await executeQuery<{
        enterprise_number: string
        status: string
        status_description: string | null
        juridical_form: string | null
        juridical_form_description: string | null
        juridical_situation: string | null
        juridical_situation_description: string | null
        type_of_enterprise: string | null
        type_of_enterprise_description: string | null
        start_date: string | null
        _snapshot_date: string
        _extract_number: number
      }>(
        db,
        `SELECT
          e.enterprise_number,
          e.status,
          c_status.description as status_description,
          e.juridical_form,
          c_jf.description as juridical_form_description,
          e.juridical_situation,
          c_js.description as juridical_situation_description,
          e.type_of_enterprise,
          c_type.description as type_of_enterprise_description,
          e.start_date::VARCHAR as start_date,
          e._snapshot_date::VARCHAR as _snapshot_date,
          e._extract_number
        FROM enterprises e
        LEFT JOIN codes c_status ON c_status.category = 'Status'
          AND c_status.code = e.status
          AND c_status.language = 'NL'
        LEFT JOIN codes c_jf ON c_jf.category = 'JuridicalForm'
          AND c_jf.code = e.juridical_form
          AND c_jf.language = 'NL'
        LEFT JOIN codes c_js ON c_js.category = 'JuridicalSituation'
          AND c_js.code = e.juridical_situation
          AND c_js.language = 'NL'
        LEFT JOIN codes c_type ON c_type.category = 'TypeOfEnterprise'
          AND c_type.code = e.type_of_enterprise
          AND c_type.language = 'NL'
        WHERE e.enterprise_number = '${number}'
          ${temporalFilter}
        LIMIT 1`
      )

      if (enterprises.length === 0) {
        await closeMotherduck(db)
        return NextResponse.json({ error: 'Enterprise not found' }, { status: 404 })
      }

      const enterprise = enterprises[0]

      // Fetch all related data in parallel
      const [denominations, addresses, activities, contacts, establishments] = await Promise.all([
        // Denominations
        executeQuery<{
          language: string
          denomination_type: string
          denomination: string
        }>(
          db,
          `SELECT language, denomination_type, denomination
          FROM denominations
          WHERE entity_number = '${number}'
            ${temporalFilter}
          ORDER BY denomination_type, language`
        ),

        // Addresses
        executeQuery<{
          type_of_address: string
          country_nl: string | null
          country_fr: string | null
          zipcode: string | null
          municipality_nl: string | null
          municipality_fr: string | null
          street_nl: string | null
          street_fr: string | null
          house_number: string | null
          box: string | null
          extra_address_info: string | null
          date_striking_off: string | null
        }>(
          db,
          `SELECT
            type_of_address,
            country_nl,
            country_fr,
            zipcode,
            municipality_nl,
            municipality_fr,
            street_nl,
            street_fr,
            house_number,
            box,
            extra_address_info,
            date_striking_off::VARCHAR as date_striking_off
          FROM addresses
          WHERE entity_number = '${number}'
            ${temporalFilter}
          ORDER BY type_of_address`
        ),

        // Activities with NACE descriptions
        executeQuery<{
          entity_number: string
          activity_group: string
          nace_version: string
          nace_code: string
          nace_description_nl: string | null
          nace_description_fr: string | null
          classification: string
        }>(
          db,
          `SELECT
            a.entity_number,
            a.activity_group,
            a.nace_version,
            a.nace_code,
            n.description_nl as nace_description_nl,
            n.description_fr as nace_description_fr,
            a.classification
          FROM activities a
          LEFT JOIN nace_codes n ON n.nace_version = a.nace_version
            AND n.nace_code = a.nace_code
          WHERE a.entity_number = '${number}'
            ${temporalFilter}
          ORDER BY a.nace_version DESC, a.classification, a.nace_code`
        ),

        // Contacts
        executeQuery<{
          entity_number: string
          contact_type: string
          contact_value: string
        }>(
          db,
          `SELECT entity_number, contact_type, contact_value
          FROM contacts
          WHERE entity_number = '${number}'
            ${temporalFilter}
          ORDER BY contact_type`
        ),

        // Establishments
        executeQuery<{
          establishment_number: string
          start_date: string | null
          commercial_name: string | null
        }>(
          db,
          `SELECT
            establishment_number,
            start_date::VARCHAR as start_date,
            commercial_name
          FROM establishments
          WHERE enterprise_number = '${number}'
            ${temporalFilter}
          ORDER BY start_date DESC`
        ),
      ])

      const detail: EnterpriseDetail = {
        enterpriseNumber: enterprise.enterprise_number,
        status: enterprise.status,
        statusDescription: enterprise.status_description,
        juridicalForm: enterprise.juridical_form,
        juridicalFormDescription: enterprise.juridical_form_description,
        juridicalSituation: enterprise.juridical_situation,
        juridicalSituationDescription: enterprise.juridical_situation_description,
        typeOfEnterprise: enterprise.type_of_enterprise,
        typeOfEnterpriseDescription: enterprise.type_of_enterprise_description,
        startDate: enterprise.start_date,
        snapshotDate: enterprise._snapshot_date,
        extractNumber: enterprise._extract_number,

        denominations: denominations.map((d) => ({
          language: d.language,
          typeCode: d.denomination_type,
          denomination: d.denomination,
        })),

        addresses: addresses.map((a) => ({
          typeCode: a.type_of_address,
          countryNL: a.country_nl,
          countryFR: a.country_fr,
          zipcode: a.zipcode,
          municipalityNL: a.municipality_nl,
          municipalityFR: a.municipality_fr,
          streetNL: a.street_nl,
          streetFR: a.street_fr,
          houseNumber: a.house_number,
          box: a.box,
          extraAddressInfo: a.extra_address_info,
          dateStrikingOff: a.date_striking_off,
        })),

        activities: activities.map((a) => ({
          entityNumber: a.entity_number,
          activityGroup: a.activity_group,
          naceVersion: a.nace_version,
          naceCode: a.nace_code,
          naceDescriptionNL: a.nace_description_nl,
          naceDescriptionFR: a.nace_description_fr,
          classification: a.classification,
        })),

        contacts: contacts.map((c) => ({
          entityNumber: c.entity_number,
          contactType: c.contact_type,
          value: c.contact_value,
        })),

        establishments: establishments.map((e) => ({
          establishmentNumber: e.establishment_number,
          startDate: e.start_date,
          primaryName: e.commercial_name,
        })),
      }

      await closeMotherduck(db)

      return NextResponse.json(detail)
    } catch (error) {
      await closeMotherduck(db)
      throw error
    }
  } catch (error) {
    console.error('Failed to fetch enterprise details:', error)
    return NextResponse.json(
      { error: 'Failed to fetch enterprise details' },
      { status: 500 }
    )
  }
}
