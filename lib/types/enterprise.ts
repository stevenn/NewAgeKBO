/**
 * Enterprise entity types based on the KBO Open Data schema
 */

export interface Enterprise {
  enterprise_number: string

  // Basic info (codes only - descriptions via JOIN to codes table)
  status: string // AC (active) or ST (stopped)
  juridical_situation: string | null // code e.g., "000"
  type_of_enterprise: string | null // 1=natural person, 2=legal person
  juridical_form: string | null // code e.g., "030"
  juridical_form_cac: string | null // code
  start_date: Date | null

  // Primary denomination (denormalized - always exists, 100% coverage)
  // Note: All enterprises have a legal name (Type 001), so no need to store type
  primary_name: string // Primary name in any language - never NULL
  primary_name_language: string | null // Language code: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN
  primary_name_nl: string | null // Dutch version if available
  primary_name_fr: string | null // French version if available
  primary_name_de: string | null // German version if available

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean // true for current snapshot, false for historical
}

export interface Establishment {
  establishment_number: string
  enterprise_number: string
  start_date: Date | null

  // Commercial name (if different from enterprise)
  commercial_name: string | null // Commercial name (Type 003, any language)
  commercial_name_language: string | null // Language code: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Denomination {
  id: string // Concatenated string ID: entity_number_type_language_row_number
  entity_number: string // enterprise or establishment number
  entity_type: 'enterprise' | 'establishment'
  denomination_type: string // 001, 002, 003, 004
  language: string // 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN
  denomination: string

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Address {
  id: string // Concatenated string ID: entity_number_type_of_address
  entity_number: string
  entity_type: 'enterprise' | 'establishment'
  type_of_address: string // REGO, BAET, ABBR, OBAD

  // Address components (multi-language)
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
  date_striking_off: Date | null

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Activity {
  id: string // Concatenated string ID: entity_number_group_version_code_classification
  entity_number: string
  entity_type: 'enterprise' | 'establishment'
  activity_group: string // 001-007
  nace_version: string // 2003, 2008, 2025
  nace_code: string
  classification: string // MAIN, SECO, ANCI

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Contact {
  id: string // Concatenated string ID: entity_number_entity_contact_contact_type_value
  entity_number: string
  entity_type: 'enterprise' | 'establishment'
  entity_contact: string // ENT=Enterprise, ESTB=Establishment, BRANCH=Branch
  contact_type: string // TEL, EMAIL, WEB, etc.
  contact_value: string

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Branch {
  id: string
  enterprise_number: string | null
  start_date: Date | null

  // Note: Branch details (name, address) are NOT provided in KBO Open Data
  // Only ID, enterprise link, and start date are available

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}
