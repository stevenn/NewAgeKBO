/**
 * In-memory cache for KBO codes table
 * The codes table is static (21,501 rows) and never changes per snapshot
 * Caching eliminates expensive JOINs at query time
 */

import { connectMotherduck, closeMotherduck, executeQuery } from '@/lib/motherduck'

// Cache structure: category -> code -> language -> description
type CodesCache = Map<string, Map<string, Map<string, string>>>

let codesCache: CodesCache | null = null
let cacheInitializing: Promise<void> | null = null

/**
 * Initialize the codes cache by loading all codes from the database
 * This is called lazily on first use
 */
async function initializeCache(): Promise<void> {
  if (codesCache) {
    return // Already initialized
  }

  if (cacheInitializing) {
    return cacheInitializing // Already initializing, wait for it
  }

  cacheInitializing = (async () => {
    console.log('[CodesCache] Initializing codes cache...')
    const startTime = Date.now()

    const db = await connectMotherduck()

    try {
      const dbName = process.env.MOTHERDUCK_DATABASE || 'kbo'
      await executeQuery(db, `USE ${dbName}`)

      const results = await executeQuery<{
        category: string
        code: string
        language: string
        description: string
      }>(db, 'SELECT category, code, language, description FROM codes')

      const cache: CodesCache = new Map()

      for (const row of results) {
        if (!cache.has(row.category)) {
          cache.set(row.category, new Map())
        }

        const categoryMap = cache.get(row.category)!
        if (!categoryMap.has(row.code)) {
          categoryMap.set(row.code, new Map())
        }

        const codeMap = categoryMap.get(row.code)!
        codeMap.set(row.language, row.description)
      }

      codesCache = cache

      const duration = Date.now() - startTime
      console.log(`[CodesCache] Initialized ${results.length} codes in ${duration}ms`)
      console.log(`[CodesCache] Categories: ${Array.from(cache.keys()).join(', ')}`)
    } finally {
      await closeMotherduck(db)
    }
  })()

  return cacheInitializing
}

/**
 * Get a code description from the cache
 * Lazily initializes the cache on first call
 *
 * @param category - Code category (e.g., 'JuridicalForm', 'Status')
 * @param code - Code value (e.g., '030', 'AC')
 * @param language - Language code ('NL', 'FR', 'DE')
 * @returns The description or null if not found
 */
export async function getCodeDescription(
  category: string,
  code: string | null | undefined,
  language: string = 'NL'
): Promise<string | null> {
  if (!code) {
    return null
  }

  await initializeCache()

  const categoryMap = codesCache?.get(category)
  if (!categoryMap) {
    return null
  }

  const codeMap = categoryMap.get(code)
  if (!codeMap) {
    return null
  }

  return codeMap.get(language) || null
}

/**
 * Get juridical form description
 * Convenience wrapper for the most common use case
 */
export async function getJuridicalFormDescription(
  code: string | null | undefined,
  language: string = 'NL'
): Promise<string | null> {
  return getCodeDescription('JuridicalForm', code, language)
}

/**
 * Get status description
 */
export async function getStatusDescription(
  code: string | null | undefined,
  language: string = 'NL'
): Promise<string | null> {
  return getCodeDescription('Status', code, language)
}

/**
 * Clear the cache (useful for testing or if codes are updated)
 */
export function clearCodesCache(): void {
  codesCache = null
  cacheInitializing = null
  console.log('[CodesCache] Cache cleared')
}

/**
 * Check if cache is initialized
 */
export function isCacheInitialized(): boolean {
  return codesCache !== null
}
