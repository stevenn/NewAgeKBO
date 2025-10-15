/**
 * Enterprise number validation
 * Format: 9999.999.999 (10 digits with dots)
 */

/**
 * Validate enterprise number format
 * @param number Enterprise number with dots (e.g., "0200.065.765")
 * @returns true if valid format
 */
export function validateEnterpriseNumber(number: string): boolean {
  if (!number) return false

  // Check format: 9999.999.999
  const pattern = /^\d{4}\.\d{3}\.\d{3}$/
  if (!pattern.test(number)) return false

  // Remove dots for checksum validation
  const digits = number.replace(/\./g, '')
  return validateChecksum(digits)
}

/**
 * Validate establishment number format
 * Format: 9.999.999.999 (10 digits with dots, leading single digit)
 */
export function validateEstablishmentNumber(number: string): boolean {
  if (!number) return false

  // Check format: 9.999.999.999
  const pattern = /^\d\.\d{3}\.\d{3}\.\d{3}$/
  if (!pattern.test(number)) return false

  // Remove dots for checksum validation
  const digits = number.replace(/\./g, '')
  return validateChecksum(digits)
}

/**
 * Validate KBO number checksum (modulo 97)
 * @param digits 10-digit string without dots
 */
function validateChecksum(digits: string): boolean {
  if (digits.length !== 10) return false

  const num = parseInt(digits.substring(0, 8), 10)
  const checksum = parseInt(digits.substring(8, 10), 10)

  return 97 - (num % 97) === checksum
}

/**
 * Format enterprise number with dots
 * @param number Raw enterprise number (may or may not have dots)
 * @returns Formatted enterprise number (9999.999.999)
 */
export function formatEnterpriseNumber(number: string): string {
  const digits = number.replace(/\D/g, '')
  if (digits.length !== 10) {
    throw new Error(`Invalid enterprise number length: ${digits.length}`)
  }
  return `${digits.substring(0, 4)}.${digits.substring(4, 7)}.${digits.substring(7, 10)}`
}

/**
 * Format establishment number with dots
 * @param number Raw establishment number (may or may not have dots)
 * @returns Formatted establishment number (9.999.999.999)
 */
export function formatEstablishmentNumber(number: string): string {
  const digits = number.replace(/\D/g, '')
  if (digits.length !== 10) {
    throw new Error(`Invalid establishment number length: ${digits.length}`)
  }
  return `${digits.substring(0, 1)}.${digits.substring(1, 4)}.${digits.substring(4, 7)}.${digits.substring(7, 10)}`
}
