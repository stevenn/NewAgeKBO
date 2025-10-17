/**
 * Shared column mapping utilities
 * Maps between KBO CSV column names (PascalCase) and database column names (snake_case)
 */

/**
 * Special case column mappings where simple snake_case conversion doesn't work
 */
const SPECIAL_COLUMN_MAPPINGS: Record<string, string> = {
  // Address columns with language suffixes
  'CountryNL': 'country_nl',
  'CountryFR': 'country_fr',
  'MunicipalityNL': 'municipality_nl',
  'MunicipalityFR': 'municipality_fr',
  'StreetNL': 'street_nl',
  'StreetFR': 'street_fr',

  // Denomination columns
  'TypeOfDenomination': 'denomination_type',
  'Denomination': 'denomination',

  // Enterprise/Establishment columns
  'JuridicalFormCAC': 'juridical_form_cac',

  // Contact columns
  'EntityContact': 'entity_contact',
  'ContactType': 'contact_type',
  'Value': 'contact_value',

  // Common columns
  'EntityNumber': 'entity_number',
  'EnterpriseNumber': 'enterprise_number',
  'EstablishmentNumber': 'establishment_number',
  'ActivityGroup': 'activity_group',
  'NaceVersion': 'nace_version',
  'NaceCode': 'nace_code',
  'Classification': 'classification',
  'TypeOfAddress': 'type_of_address',
  'StartDate': 'start_date',
  'Status': 'status',
  'JuridicalSituation': 'juridical_situation',
  'TypeOfEnterprise': 'type_of_enterprise',
  'JuridicalForm': 'juridical_form',
  'HouseNumber': 'house_number',
  'Box': 'box',
  'ExtraAddressInfo': 'extra_address_info',
  'DateStrikingOff': 'date_striking_off',
  'Zipcode': 'zipcode',
  'Language': 'language',
  'Id': 'id'
}

/**
 * Table name mappings: CSV file names (singular) to database table names (plural)
 */
export const CSV_TO_DB_TABLE_NAMES: Record<string, string> = {
  'activity': 'activities',
  'address': 'addresses',
  'contact': 'contacts',
  'denomination': 'denominations',
  'enterprise': 'enterprises',
  'establishment': 'establishments',
  'branch': 'branches',
  'code': 'codes'
}

/**
 * Convert PascalCase column name to snake_case database column name
 */
export function csvColumnToDbColumn(csvColumn: string): string {
  // Check special cases first
  if (SPECIAL_COLUMN_MAPPINGS[csvColumn]) {
    return SPECIAL_COLUMN_MAPPINGS[csvColumn]
  }

  // Default: simple snake_case conversion
  return csvColumn
    .replace(/([A-Z])/g, '_$1')
    .toLowerCase()
    .replace(/^_/, '')
}

/**
 * Convert CSV table name to database table name
 */
export function csvTableToDbTable(csvTable: string): string {
  return CSV_TO_DB_TABLE_NAMES[csvTable] || csvTable
}

/**
 * Compute entity_type from entity number
 * Establishments start with "2.", everything else is enterprise
 */
export function computeEntityType(entityNumber: string): 'enterprise' | 'establishment' {
  return entityNumber.startsWith('2.') ? 'establishment' : 'enterprise'
}

/**
 * Convert DD-MM-YYYY date format to YYYY-MM-DD
 */
export function convertKboDateFormat(ddmmyyyy: string): string {
  const [day, month, year] = ddmmyyyy.split('-')
  return `${year}-${month}-${day}`
}

/**
 * Check if a value looks like a KBO date (DD-MM-YYYY format)
 */
export function isKboDateFormat(value: string): boolean {
  return /^\d{2}-\d{2}-\d{4}$/.test(value)
}
