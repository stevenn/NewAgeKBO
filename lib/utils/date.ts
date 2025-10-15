/**
 * Date utilities for KBO data processing
 */

/**
 * Get current date/time in CET/CEST timezone
 */
export function getCurrentCET(): Date {
  return new Date(
    new Date().toLocaleString('en-US', { timeZone: 'Europe/Brussels' })
  )
}

/**
 * Format date for snapshot identifier (YYYY-MM-DD)
 */
export function formatSnapshotDate(date: Date): string {
  return date.toISOString().split('T')[0]
}

/**
 * Parse KBO date format (dd-mm-yyyy) to Date object
 */
export function parseKBODate(dateStr: string): Date | null {
  if (!dateStr || dateStr.trim() === '') {
    return null
  }

  const parts = dateStr.split('-')
  if (parts.length !== 3) {
    throw new Error(`Invalid date format: ${dateStr}. Expected dd-mm-yyyy`)
  }

  const [day, month, year] = parts.map((p) => parseInt(p, 10))
  if (isNaN(day) || isNaN(month) || isNaN(year)) {
    throw new Error(`Invalid date values: ${dateStr}`)
  }

  // Month is 0-indexed in JavaScript Date
  return new Date(year, month - 1, day)
}

/**
 * Format Date object to KBO format (dd-mm-yyyy)
 */
export function formatKBODate(date: Date): string {
  const day = date.getDate().toString().padStart(2, '0')
  const month = (date.getMonth() + 1).toString().padStart(2, '0')
  const year = date.getFullYear()
  return `${day}-${month}-${year}`
}

/**
 * Parse KBO timestamp format (dd-mm-yyyy HH:MM:SS) to Date object
 */
export function parseKBOTimestamp(timestampStr: string): Date {
  if (!timestampStr || timestampStr.trim() === '') {
    throw new Error('Timestamp string is empty')
  }

  const parts = timestampStr.split(' ')
  if (parts.length !== 2) {
    throw new Error(
      `Invalid timestamp format: ${timestampStr}. Expected dd-mm-yyyy HH:MM:SS`
    )
  }

  const [datePart, timePart] = parts
  const date = parseKBODate(datePart)
  if (!date) {
    throw new Error(`Invalid date part in timestamp: ${datePart}`)
  }

  const timeParts = timePart.split(':')
  if (timeParts.length !== 3) {
    throw new Error(`Invalid time part in timestamp: ${timePart}`)
  }

  const [hours, minutes, seconds] = timeParts.map((p) => parseInt(p, 10))
  if (isNaN(hours) || isNaN(minutes) || isNaN(seconds)) {
    throw new Error(`Invalid time values: ${timePart}`)
  }

  date.setHours(hours, minutes, seconds, 0)
  return date
}
