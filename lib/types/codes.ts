/**
 * Code lookup table types
 */

export interface NaceCode {
  nace_version: string // 2003, 2008, 2025
  nace_code: string
  description_nl: string | null
  description_fr: string | null
  description_de: string | null
  description_en: string | null
}

export interface Code {
  category: string // JuridicalForm, JuridicalSituation, etc.
  code: string
  language: string // NL, FR, DE, EN
  description: string
}

export type CodeCategory =
  | 'JuridicalForm'
  | 'JuridicalSituation'
  | 'ActivityGroup'
  | 'TypeOfAddress'
  | 'TypeOfDenomination'
  | 'ContactType'
  | 'EntityContact'
  | 'TypeOfEnterprise'
  | 'Classification'
  | 'Status'
  | 'Language'

export type Language = 'NL' | 'FR' | 'DE' | 'EN'

// KBO uses numeric codes for languages
export type LanguageCode = '0' | '1' | '2' | '3' | '4'

// Language mapping
export const LANGUAGE_MAP: Record<LanguageCode, string> = {
  '0': 'Unknown',
  '1': 'FR',
  '2': 'NL',
  '3': 'DE',
  '4': 'EN',
}

export const REVERSE_LANGUAGE_MAP: Record<string, LanguageCode> = {
  Unknown: '0',
  FR: '1',
  NL: '2',
  DE: '3',
  EN: '4',
}
