/**
 * Type definitions for KBO VAT extractor
 */

/**
 * VAT status information for an enterprise
 */
export interface EnterpriseVATStatus {
  enterpriseNumber: string
  vatLiable: boolean
  authorizationPhase: string | null
  validityStart: Date | null
  validityEnd: Date | null
}

/**
 * Parser state tracking for XML streaming
 */
export interface ParserState {
  // Current position in XML tree
  path: string[]

  // Enterprise being processed
  currentEnterprise: string | null

  // Authorization tracking
  inAuthorization: boolean
  currentAuthCode: string | null
  currentAuthPhase: string | null
  currentValidityStart: string | null
  currentValidityEnd: string | null

  // Text content accumulator
  textContent: string
}

/**
 * Progress statistics for terminal display
 */
export interface ProgressStats {
  // File processing
  bytesProcessed: number
  totalBytes: number
  percentComplete: number

  // Enterprise counts
  enterprisesProcessed: number
  vatLiableFound: number
  vatNotLiable: number
  noVatInfo: number

  // Performance metrics
  startTime: number
  elapsedTimeMs: number
  estimatedRemainingMs: number
  processingRatePerSec: number

  // System metrics
  memoryUsageMB: number

  // Last processed
  lastEnterpriseNumber: string | null
  lastVatStatus: string | null
}

/**
 * Configuration for VAT extraction
 */
export interface ExtractorConfig {
  zipFilePath: string
  outputFilePath: string
  verbose?: boolean
  progressUpdateIntervalMs?: number
  batchSize?: number
}

/**
 * Events emitted during extraction
 */
export interface ExtractorEvents {
  onProgress: (stats: ProgressStats) => void
  onVATFound: (status: EnterpriseVATStatus) => void
  onError: (error: Error) => void
  onComplete: (stats: ProgressStats) => void
}

/**
 * VAT Authorization codes from KBO spec
 * Source: Annex 2 - PermissionCodes
 */
export const VAT_AUTHORIZATION_CODE = '00001' // VAT liable code

/**
 * Authorization phase codes
 */
export enum AuthorizationPhase {
  GRANTED = '001',
  REFUSED = '002',
  IN_APPLICATION = '003',
  WITHDRAWN = '004',
}

/**
 * XML element paths we care about
 */
export const XML_PATHS = {
  ENTERPRISE: '/CommercialisationFileType/Enterprises/Enterprise',
  ENTERPRISE_NUMBER: '/CommercialisationFileType/Enterprises/Enterprise/Nbr',
  AUTHORIZATION: '/CommercialisationFileType/Enterprises/Enterprise/Authorizations/Authorization',
  AUTH_CODE: '/CommercialisationFileType/Enterprises/Enterprise/Authorizations/Authorization/Code',
  AUTH_PHASE: '/CommercialisationFileType/Enterprises/Enterprise/Authorizations/Authorization/PhaseCode',
  AUTH_VALIDITY_BEGIN: '/CommercialisationFileType/Enterprises/Enterprise/Authorizations/Authorization/Validity/Begin',
  AUTH_VALIDITY_END: '/CommercialisationFileType/Enterprises/Enterprise/Authorizations/Authorization/Validity/End',
} as const
