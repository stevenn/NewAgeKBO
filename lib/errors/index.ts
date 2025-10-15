/**
 * Custom error classes for the application
 */

export class KBOPortalError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message)
    this.name = 'KBOPortalError'
  }
}

export class MotherduckError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message)
    this.name = 'MotherduckError'
  }
}

export class ValidationError extends Error {
  constructor(message: string, public readonly details?: Record<string, unknown>) {
    super(message)
    this.name = 'ValidationError'
  }
}

export class TransformationError extends Error {
  constructor(message: string, public readonly cause?: Error) {
    super(message)
    this.name = 'TransformationError'
  }
}

export class CSVParsingError extends Error {
  constructor(
    message: string,
    public readonly filename?: string,
    public readonly lineNumber?: number
  ) {
    super(message)
    this.name = 'CSVParsingError'
  }
}

/**
 * Log error with context
 */
export function logError(error: Error, context?: Record<string, unknown>): void {
  console.error({
    error: {
      name: error.name,
      message: error.message,
      stack: error.stack,
    },
    context,
    timestamp: new Date().toISOString(),
  })
}

/**
 * Format error message for user display
 */
export function formatUserError(error: Error): string {
  if (error instanceof ValidationError) {
    return `Validation failed: ${error.message}`
  }
  if (error instanceof KBOPortalError) {
    return `KBO Portal error: ${error.message}`
  }
  if (error instanceof MotherduckError) {
    return `Database error: ${error.message}`
  }
  if (error instanceof CSVParsingError) {
    return `CSV parsing error: ${error.message}${
      error.filename ? ` in ${error.filename}` : ''
    }${error.lineNumber ? ` at line ${error.lineNumber}` : ''}`
  }
  return `An error occurred: ${error.message}`
}
