import { executeQuery } from './index'
import type { DuckDBConnection } from '@duckdb/node-api'
import type { EnterpriseDetail } from '@/app/api/enterprises/[number]/route'
import type { Language } from '@/lib/types/codes'
import {
  buildChildTableQuery,
  buildPointInTimeQuery,
  buildPointInTimeQueryByNaturalKey,
  buildTemporalFilter,
  type TemporalFilter
} from './temporal-query'
import { getDenominationTypeDescription, getLanguageAbbreviation } from '@/lib/cache/codes'

/**
 * Fetches complete enterprise details with all related data
 * Reusable helper for both detail and snapshots endpoints
 *
 * @param connection - Database connection
 * @param enterpriseNumber - Enterprise number to fetch
 * @param filter - Temporal filter (current or point-in-time)
 * @param language - Language for code descriptions (NL, FR, or DE)
 */
export async function fetchEnterpriseDetail(
  connection: DuckDBConnection,
  enterpriseNumber: string,
  filter: TemporalFilter,
  language: Language = 'NL'
): Promise<EnterpriseDetail | null> {
  const usePointInTime = filter.type === 'point-in-time'

  // For point-in-time queries, wrap with window function to get latest version
  const enterpriseQuery = usePointInTime
    ? buildPointInTimeQueryByNaturalKey(
        `sub.enterprise_number,
         sub.status,
         sub.status_description,
         sub.juridical_form,
         sub.juridical_form_description,
         sub.juridical_situation,
         sub.juridical_situation_description,
         sub.type_of_enterprise,
         sub.type_of_enterprise_description,
         sub.start_date::VARCHAR as start_date,
         sub._snapshot_date::VARCHAR as _snapshot_date,
         sub._extract_number,
         sub._is_current,
         sub._deleted_at_extract`,
        `(SELECT
           e.*,
           c_status.description as status_description,
           c_jf.description as juridical_form_description,
           c_js.description as juridical_situation_description,
           c_type.description as type_of_enterprise_description
         FROM enterprises e
         LEFT JOIN codes c_status ON c_status.category = 'Status'
           AND c_status.code = e.status
           AND c_status.language = '${language}'
         LEFT JOIN codes c_jf ON c_jf.category = 'JuridicalForm'
           AND c_jf.code = e.juridical_form
           AND c_jf.language = '${language}'
         LEFT JOIN codes c_js ON c_js.category = 'JuridicalSituation'
           AND c_js.code = e.juridical_situation
           AND c_js.language = '${language}'
         LEFT JOIN codes c_type ON c_type.category = 'TypeOfEnterprise'
           AND c_type.code = e.type_of_enterprise
           AND c_type.language = '${language}'
         WHERE e.enterprise_number = '${enterpriseNumber}' AND ${buildTemporalFilter(filter, 'e')})`,
        `1=1`, // WHERE clause already in subquery
        'enterprise_number'
      )
    : `SELECT
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
        e._extract_number,
        e._is_current,
        e._deleted_at_extract
      FROM enterprises e
      LEFT JOIN codes c_status ON c_status.category = 'Status'
        AND c_status.code = e.status
        AND c_status.language = '${language}'
      LEFT JOIN codes c_jf ON c_jf.category = 'JuridicalForm'
        AND c_jf.code = e.juridical_form
        AND c_jf.language = '${language}'
      LEFT JOIN codes c_js ON c_js.category = 'JuridicalSituation'
        AND c_js.code = e.juridical_situation
        AND c_js.language = '${language}'
      LEFT JOIN codes c_type ON c_type.category = 'TypeOfEnterprise'
        AND c_type.code = e.type_of_enterprise
        AND c_type.language = '${language}'
      WHERE e.enterprise_number = '${enterpriseNumber}'
        AND ${buildTemporalFilter(filter, 'e')}
      LIMIT 1`

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
    _is_current: boolean
    _deleted_at_extract: number | null
  }>(connection, enterpriseQuery)

  if (enterprises.length === 0) {
    return null
  }

  const enterprise = enterprises[0]

  // For deleted enterprises, find the last snapshot date where they appeared
  let lastSnapshotDate: string | null = null
  if (!enterprise._is_current) {
    const lastSnapshot = await executeQuery<{ last_snapshot_date: string }>(
      connection,
      `SELECT MAX(_snapshot_date)::VARCHAR as last_snapshot_date
       FROM enterprises
       WHERE enterprise_number = '${enterpriseNumber}'`
    )
    lastSnapshotDate = lastSnapshot[0]?.last_snapshot_date || null
  }

  // Fetch all related data in parallel
  const [denominations, addresses, activities, contacts, establishments, establishmentActivities] = await Promise.all([
    // Denominations
    executeQuery<{
      language: string
      denomination_type: string
      denomination: string
    }>(
      connection,
      buildChildTableQuery(
        'denominations',
        'language, denomination_type, denomination',
        enterpriseNumber,
        filter,
        'denomination_type, language',
        'entity_number, language, denomination_type'
      )
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
      connection,
      buildChildTableQuery(
        'addresses',
        `type_of_address,
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
        date_striking_off::VARCHAR as date_striking_off`,
        enterpriseNumber,
        filter,
        'type_of_address',
        'id'
      )
    ),

    // Activities with NACE descriptions and activity group descriptions - needs custom query due to JOINs
    executeQuery<{
      entity_number: string
      activity_group: string
      activity_group_description_nl: string | null
      activity_group_description_fr: string | null
      nace_version: string
      nace_code: string
      nace_description_nl: string | null
      nace_description_fr: string | null
      classification: string
    }>(
      connection,
      (() => {
        const temporalWhere = buildTemporalFilter(filter, 'a')
        const baseWhere = `a.entity_number = '${enterpriseNumber}' AND ${temporalWhere}`

        if (filter.type === 'current') {
          return `
            SELECT
              a.entity_number,
              a.activity_group,
              c_ag_nl.description as activity_group_description_nl,
              c_ag_fr.description as activity_group_description_fr,
              a.nace_version,
              a.nace_code,
              n.description_nl as nace_description_nl,
              n.description_fr as nace_description_fr,
              a.classification
            FROM activities a
            LEFT JOIN nace_codes n ON n.nace_version = a.nace_version
              AND n.nace_code = a.nace_code
            LEFT JOIN codes c_ag_nl ON c_ag_nl.category = 'ActivityGroup'
              AND c_ag_nl.code = a.activity_group AND c_ag_nl.language = 'NL'
            LEFT JOIN codes c_ag_fr ON c_ag_fr.category = 'ActivityGroup'
              AND c_ag_fr.code = a.activity_group AND c_ag_fr.language = 'FR'
            WHERE ${baseWhere}
            ORDER BY a.nace_version DESC, a.classification, a.nace_code
          `.trim()
        }

        // Point-in-time query with window function
        return buildPointInTimeQuery(
          `entity_number,
          activity_group,
          activity_group_description_nl,
          activity_group_description_fr,
          nace_version,
          nace_code,
          nace_description_nl,
          nace_description_fr,
          classification`,
          `(
            SELECT
              a.*,
              c_ag_nl.description as activity_group_description_nl,
              c_ag_fr.description as activity_group_description_fr,
              n.description_nl as nace_description_nl,
              n.description_fr as nace_description_fr
            FROM activities a
            LEFT JOIN nace_codes n ON n.nace_version = a.nace_version
              AND n.nace_code = a.nace_code
            LEFT JOIN codes c_ag_nl ON c_ag_nl.category = 'ActivityGroup'
              AND c_ag_nl.code = a.activity_group AND c_ag_nl.language = 'NL'
            LEFT JOIN codes c_ag_fr ON c_ag_fr.category = 'ActivityGroup'
              AND c_ag_fr.code = a.activity_group AND c_ag_fr.language = 'FR'
            WHERE ${baseWhere}
          )`,
          '1=1',
          'id',
          'nace_version DESC, classification, nace_code'
        )
      })()
    ),

    // Contacts
    executeQuery<{
      entity_number: string
      contact_type: string
      contact_value: string
    }>(
      connection,
      buildChildTableQuery(
        'contacts',
        'entity_number, contact_type, contact_value',
        enterpriseNumber,
        filter,
        'contact_type',
        'id'
      )
    ),

    // Establishments - uses enterprise_number FK, not entity_number
    executeQuery<{
      establishment_number: string
      start_date: string | null
      commercial_name: string | null
    }>(
      connection,
      (() => {
        const temporalWhere = buildTemporalFilter(filter)
        const baseWhere = `enterprise_number = '${enterpriseNumber}' AND ${temporalWhere}`

        if (filter.type === 'current') {
          return `
            SELECT
              establishment_number,
              start_date::VARCHAR as start_date,
              commercial_name
            FROM establishments
            WHERE ${baseWhere}
            ORDER BY start_date DESC
          `.trim()
        }

        // Point-in-time query with window function
        return buildPointInTimeQuery(
          `establishment_number,
          start_date::VARCHAR as start_date,
          commercial_name`,
          'establishments',
          baseWhere,
          'establishment_number',
          'start_date DESC'
        )
      })()
    ),

    // Establishment activities - fetch all activities for establishments of this enterprise
    executeQuery<{
      entity_number: string
      activity_group: string
      activity_group_description_nl: string | null
      activity_group_description_fr: string | null
      nace_version: string
      nace_code: string
      nace_description_nl: string | null
      nace_description_fr: string | null
      classification: string
    }>(
      connection,
      (() => {
        const estTemporalWhere = buildTemporalFilter(filter, 'est')
        const actTemporalWhere = buildTemporalFilter(filter, 'a')

        if (filter.type === 'current') {
          return `
            SELECT
              a.entity_number,
              a.activity_group,
              c_ag_nl.description as activity_group_description_nl,
              c_ag_fr.description as activity_group_description_fr,
              a.nace_version,
              a.nace_code,
              n.description_nl as nace_description_nl,
              n.description_fr as nace_description_fr,
              a.classification
            FROM activities a
            INNER JOIN establishments est ON a.entity_number = est.establishment_number
            LEFT JOIN nace_codes n ON n.nace_version = a.nace_version AND n.nace_code = a.nace_code
            LEFT JOIN codes c_ag_nl ON c_ag_nl.category = 'ActivityGroup'
              AND c_ag_nl.code = a.activity_group AND c_ag_nl.language = 'NL'
            LEFT JOIN codes c_ag_fr ON c_ag_fr.category = 'ActivityGroup'
              AND c_ag_fr.code = a.activity_group AND c_ag_fr.language = 'FR'
            WHERE est.enterprise_number = '${enterpriseNumber}'
              AND ${estTemporalWhere}
              AND ${actTemporalWhere}
            ORDER BY a.entity_number, a.activity_group, a.nace_version DESC, a.nace_code
          `.trim()
        }

        // For point-in-time, we need a more complex query
        return `
          WITH current_establishments AS (
            SELECT DISTINCT establishment_number
            FROM establishments est
            WHERE enterprise_number = '${enterpriseNumber}'
              AND ${estTemporalWhere}
          )
          SELECT
            a.entity_number,
            a.activity_group,
            c_ag_nl.description as activity_group_description_nl,
            c_ag_fr.description as activity_group_description_fr,
            a.nace_version,
            a.nace_code,
            n.description_nl as nace_description_nl,
            n.description_fr as nace_description_fr,
            a.classification
          FROM activities a
          INNER JOIN current_establishments ce ON a.entity_number = ce.establishment_number
          LEFT JOIN nace_codes n ON n.nace_version = a.nace_version AND n.nace_code = a.nace_code
          LEFT JOIN codes c_ag_nl ON c_ag_nl.category = 'ActivityGroup'
            AND c_ag_nl.code = a.activity_group AND c_ag_nl.language = 'NL'
          LEFT JOIN codes c_ag_fr ON c_ag_fr.category = 'ActivityGroup'
            AND c_ag_fr.code = a.activity_group AND c_ag_fr.language = 'FR'
          WHERE ${actTemporalWhere}
          ORDER BY a.entity_number, a.activity_group, a.nace_version DESC, a.nace_code
        `.trim()
      })()
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

    // Cessation/deletion tracking
    isCurrent: enterprise._is_current,
    deletedAtExtract: enterprise._deleted_at_extract,
    lastSnapshotDate: lastSnapshotDate,

    denominations: await Promise.all(
      denominations.map(async (d) => ({
        language: d.language,
        languageDescription: getLanguageAbbreviation(d.language),
        typeCode: d.denomination_type,
        typeDescription: await getDenominationTypeDescription(d.denomination_type, language),
        denomination: d.denomination,
      }))
    ),

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
      activityGroupDescriptionNL: a.activity_group_description_nl,
      activityGroupDescriptionFR: a.activity_group_description_fr,
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

    establishments: establishments.map((e) => {
      // Group establishment activities by establishment number
      const estActivities = establishmentActivities
        .filter((a) => a.entity_number === e.establishment_number)
        .map((a) => ({
          entityNumber: a.entity_number,
          activityGroup: a.activity_group,
          activityGroupDescriptionNL: a.activity_group_description_nl,
          activityGroupDescriptionFR: a.activity_group_description_fr,
          naceVersion: a.nace_version,
          naceCode: a.nace_code,
          naceDescriptionNL: a.nace_description_nl,
          naceDescriptionFR: a.nace_description_fr,
          classification: a.classification,
        }))

      return {
        establishmentNumber: e.establishment_number,
        startDate: e.start_date,
        primaryName: e.commercial_name,
        activities: estActivities,
      }
    }),
  }

  return detail
}
