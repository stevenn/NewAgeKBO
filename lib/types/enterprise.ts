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
  primary_name_nl: string
  primary_name_fr: string | null
  primary_name_de: string | null // For German-speaking regions
  primary_name_type: string | null // 001, 002, 003, 004

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean // true for current snapshot, false for historical
}

export interface Establishment {
  establishment_number: string
  enterprise_number: string
  start_date: Date | null

  // Primary name (if different from enterprise)
  commercial_name: string | null

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Denomination {
  id: string // UUID
  entity_number: string // enterprise or establishment number
  entity_type: 'enterprise' | 'establishment'
  denomination_type: string // 001, 002, 003, 004
  language: string // 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN
  denomination: string

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}

export interface Address {
  id: string // UUID
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
  id: string // UUID
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
  id: string // UUID
  entity_number: string
  entity_type: 'enterprise' | 'establishment'
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
  branch_name: string | null

  // Address (denormalized - branches are rare, only 7k)
  street_nl: string | null
  street_fr: string | null
  house_number: string | null
  box: string | null
  zipcode: string | null
  municipality_nl: string | null
  municipality_fr: string | null

  // Temporal tracking
  _snapshot_date: Date
  _extract_number: number
  _is_current: boolean
}
