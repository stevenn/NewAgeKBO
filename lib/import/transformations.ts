/**
 * KBO Data Transformation SQL Definitions
 *
 * Contains all SQL transformation logic for converting raw KBO CSV data
 * into the normalized temporal schema.
 *
 * These transformations:
 * - Generate computed IDs for link tables
 * - Compute entity_type (enterprise vs establishment)
 * - Handle primary name selection from denominations
 * - Normalize NACE codes
 * - Convert date formats
 * - Deduplicate data
 */

import { Metadata } from './metadata'

/**
 * Table transformation definition
 */
export interface TableTransformation {
  /** Database table name */
  tableName: string
  /** Source CSV file name (empty string if already staged) */
  csvFile: string
  /** SQL SELECT statement for transformation */
  transformSql: string
}

/**
 * Get SQL transformation for codes table (static lookup)
 */
export function getCodesTransformation(): TableTransformation {
  return {
    tableName: 'codes',
    csvFile: 'code.csv',
    transformSql: `
      SELECT
        Category as category,
        Code as code,
        Language as language,
        MAX(Description) as description  -- Handle potential duplicates in code.csv
      FROM staged_codes
      GROUP BY Category, Code, Language
    `
  }
}

/**
 * Get SQL transformation for NACE codes (static lookup - KBO only provides NL and FR)
 */
export function getNaceCodesTransformation(): TableTransformation {
  return {
    tableName: 'nace_codes',
    csvFile: 'code.csv',
    transformSql: `
      SELECT DISTINCT
        CASE
          WHEN Category = 'Nace2003' THEN '2003'
          WHEN Category = 'Nace2008' THEN '2008'
          WHEN Category = 'Nace2025' THEN '2025'
        END as nace_version,
        Code as nace_code,
        MAX(CASE WHEN Language = 'NL' THEN Description END) as description_nl,
        MAX(CASE WHEN Language = 'FR' THEN Description END) as description_fr
      FROM staged_codes
      WHERE Category IN ('Nace2003', 'Nace2008', 'Nace2025')
      GROUP BY nace_version, nace_code
    `
  }
}

/**
 * Get SQL transformation for enterprises (with denormalized primary name)
 * NOTE: Requires staged_enterprises and ranked_denominations temp tables
 */
export function getEnterprisesTransformation(): TableTransformation {
  return {
    tableName: 'enterprises',
    csvFile: '', // Already staged
    transformSql: `
      SELECT
        e.EnterpriseNumber as enterprise_number,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        e.Status as status,
        e.JuridicalSituation as juridical_situation,
        e.TypeOfEnterprise as type_of_enterprise,
        e.JuridicalForm as juridical_form,
        e.JuridicalFormCAC as juridical_form_cac,
        TRY_CAST(e.StartDate AS DATE) as start_date,
        -- Primary name: first available in priority order (Language 2=NL, 1=FR, 3=DE, 4=EN, 0=Unknown)
        -- Store the actual name used as primary_name (never NULL)
        COALESCE(
          MAX(CASE WHEN d.Language = '2' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '1' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '0' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '3' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '4' THEN d.Denomination END),
          e.EnterpriseNumber
        ) as primary_name,
        -- Track which language the primary_name is in
        COALESCE(
          MAX(CASE WHEN d.Language = '2' THEN '2' END),
          MAX(CASE WHEN d.Language = '1' THEN '1' END),
          MAX(CASE WHEN d.Language = '0' THEN '0' END),
          MAX(CASE WHEN d.Language = '3' THEN '3' END),
          MAX(CASE WHEN d.Language = '4' THEN '4' END),
          NULL
        ) as primary_name_language,
        -- Store each language variant separately (NULL if not available)
        MAX(CASE WHEN d.Language = '2' THEN d.Denomination END) as primary_name_nl,
        MAX(CASE WHEN d.Language = '1' THEN d.Denomination END) as primary_name_fr,
        MAX(CASE WHEN d.Language = '3' THEN d.Denomination END) as primary_name_de,
        TRUE as _is_current
      FROM staged_enterprises e
      LEFT JOIN ranked_denominations d
        ON e.EnterpriseNumber = d.EntityNumber
        AND d.priority_rank = 1
      GROUP BY
        e.EnterpriseNumber,
        e.Status,
        e.JuridicalSituation,
        e.TypeOfEnterprise,
        e.JuridicalForm,
        e.JuridicalFormCAC,
        e.StartDate
    `
  }
}

/**
 * Get SQL transformation for establishments (with commercial names from denominations)
 * NOTE: Requires staged_establishments and staged_denominations temp tables
 */
export function getEstablishmentsTransformation(): TableTransformation {
  return {
    tableName: 'establishments',
    csvFile: '', // Already staged
    transformSql: `
      SELECT
        e.EstablishmentNumber as establishment_number,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        e.EnterpriseNumber as enterprise_number,
        TRY_CAST(e.StartDate AS DATE) as start_date,
        -- Extract primary commercial name (Type 003) - Priority: Dutch -> French -> Unknown -> German -> English
        COALESCE(
          MAX(CASE WHEN d.Language = '2' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '1' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '0' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '3' AND d.TypeOfDenomination = '003' THEN d.Denomination END),
          MAX(CASE WHEN d.Language = '4' AND d.TypeOfDenomination = '003' THEN d.Denomination END)
        ) as commercial_name,
        -- Track which language the commercial_name is in
        COALESCE(
          MAX(CASE WHEN d.Language = '2' AND d.TypeOfDenomination = '003' THEN '2' END),
          MAX(CASE WHEN d.Language = '1' AND d.TypeOfDenomination = '003' THEN '1' END),
          MAX(CASE WHEN d.Language = '0' AND d.TypeOfDenomination = '003' THEN '0' END),
          MAX(CASE WHEN d.Language = '3' AND d.TypeOfDenomination = '003' THEN '3' END),
          MAX(CASE WHEN d.Language = '4' AND d.TypeOfDenomination = '003' THEN '4' END)
        ) as commercial_name_language,
        TRUE as _is_current
      FROM staged_establishments e
      LEFT JOIN staged_denominations d
        ON e.EstablishmentNumber = d.EntityNumber
        AND d.TypeOfDenomination = '003'  -- Commercial name only
      GROUP BY e.EstablishmentNumber, e.EnterpriseNumber, e.StartDate
    `
  }
}

/**
 * Get SQL transformation for denominations (all names)
 */
export function getDenominationsTransformation(): TableTransformation {
  return {
    tableName: 'denominations',
    csvFile: '', // Already staged
    transformSql: `
      SELECT
        EntityNumber || '_' || TypeOfDenomination || '_' || Language || '_' ||
        SUBSTRING(MD5(Denomination), 1, 8) as id,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        EntityNumber as entity_number,
        CASE
          WHEN EntityNumber LIKE '2.%' THEN 'establishment'
          ELSE 'enterprise'
        END as entity_type,
        TypeOfDenomination as denomination_type,
        Language as language,
        Denomination as denomination,
        TRUE as _is_current
      FROM staged_denominations
    `
  }
}

/**
 * Get SQL transformation for addresses
 */
export function getAddressesTransformation(): TableTransformation {
  return {
    tableName: 'addresses',
    csvFile: '', // Already staged
    transformSql: `
      SELECT
        EntityNumber || '_' || TypeOfAddress as id,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        EntityNumber as entity_number,
        CASE
          WHEN EntityNumber LIKE '2.%' THEN 'establishment'
          ELSE 'enterprise'
        END as entity_type,
        TypeOfAddress as type_of_address,
        CountryNL as country_nl,
        CountryFR as country_fr,
        Zipcode as zipcode,
        MunicipalityNL as municipality_nl,
        MunicipalityFR as municipality_fr,
        StreetNL as street_nl,
        StreetFR as street_fr,
        HouseNumber as house_number,
        Box as box,
        ExtraAddressInfo as extra_address_info,
        TRY_CAST(DateStrikingOff AS DATE) as date_striking_off,
        TRUE as _is_current
      FROM staged_addresses
    `
  }
}

/**
 * Get SQL transformation for activities (link table with NACE codes)
 */
export function getActivitiesTransformation(): TableTransformation {
  return {
    tableName: 'activities',
    csvFile: '', // Already staged
    transformSql: `
      SELECT DISTINCT
        EntityNumber || '_' || ActivityGroup || '_' || NaceVersion || '_' || NaceCode || '_' || Classification as id,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        EntityNumber as entity_number,
        CASE
          WHEN EntityNumber LIKE '2.%' THEN 'establishment'
          ELSE 'enterprise'
        END as entity_type,
        ActivityGroup as activity_group,
        NaceVersion as nace_version,
        NaceCode as nace_code,
        Classification as classification,
        TRUE as _is_current
      FROM staged_activities
    `
  }
}

/**
 * Get SQL transformation for contacts
 */
export function getContactsTransformation(): TableTransformation {
  return {
    tableName: 'contacts',
    csvFile: '', // Already staged
    transformSql: `
      SELECT DISTINCT
        EntityNumber || '_' || EntityContact || '_' || ContactType || '_' || Value as id,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        EntityNumber as entity_number,
        CASE
          WHEN EntityNumber LIKE '2.%' THEN 'establishment'
          ELSE 'enterprise'
        END as entity_type,
        EntityContact as entity_contact,
        ContactType as contact_type,
        Value as contact_value,
        TRUE as _is_current
      FROM staged_contacts
    `
  }
}

/**
 * Get SQL transformation for branches
 */
export function getBranchesTransformation(): TableTransformation {
  return {
    tableName: 'branches',
    csvFile: '', // Already staged
    transformSql: `
      SELECT
        Id as id,
        CURRENT_DATE as _snapshot_date,
        0 as _extract_number,
        EnterpriseNumber as enterprise_number,
        TRY_CAST(StartDate AS DATE) as start_date,
        TRUE as _is_current
      FROM staged_branches
    `
  }
}

/**
 * Replace metadata placeholders in SQL with actual values
 */
export function injectMetadata(sql: string, metadata: Metadata): string {
  return sql
    .replace(/CURRENT_DATE as _snapshot_date/g, `DATE '${metadata.snapshotDate}' as _snapshot_date`)
    .replace(/0 as _extract_number/g, `${metadata.extractNumber} as _extract_number`)
}

/**
 * Get all table transformations in dependency order
 * (tables that depend on others are processed later)
 */
export function getAllTransformations(): TableTransformation[] {
  return [
    getCodesTransformation(),
    getNaceCodesTransformation(),
    getEnterprisesTransformation(),
    getEstablishmentsTransformation(),
    getDenominationsTransformation(),
    getAddressesTransformation(),
    getActivitiesTransformation(),
    getContactsTransformation(),
    getBranchesTransformation()
  ]
}
