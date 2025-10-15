/**
 * Extract number utilities
 */

/**
 * Parse extract number from filename
 * Example: "KboOpenData_0140_2025_10_05_Full.zip" -> 140
 */
export function parseExtractNumber(filename: string): number {
  const match = filename.match(/KboOpenData_(\d+)_/)
  if (!match) {
    throw new Error(`Cannot parse extract number from filename: ${filename}`)
  }
  return parseInt(match[1], 10)
}

/**
 * Compare extract numbers
 * Returns: negative if a < b, 0 if equal, positive if a > b
 */
export function compareExtractNumbers(a: number, b: number): number {
  return a - b
}

/**
 * Check if extract is newer than another
 */
export function isNewerExtract(newExtract: number, oldExtract: number): boolean {
  return newExtract > oldExtract
}
