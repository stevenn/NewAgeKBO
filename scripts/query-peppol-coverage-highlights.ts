/**
 * Query: Peppol registration coverage highlights
 * Shows NACE codes grouped by coverage: HIGH (>50%) and LOW (<30%)
 *
 * Coverage = (entities with non-Hermes Peppol registration) / (total KBO entities with this NACE)
 *
 * Usage:
 *   npx tsx scripts/query-peppol-coverage-highlights.ts [min-entities]
 *
 * Args:
 *   min-entities: Minimum entities to include a NACE code (default: 1000)
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

config({ path: resolve(__dirname, '../.env.local') })

interface NaceRow {
  nace_code: string
  nace_description_nl: string
  nace_description_fr: string
  total_entities: number
  peppol_registered: number
  coverage_pct: number
}

async function queryCoverageHighlights(minEntities: number) {
  const token = process.env.MOTHERDUCK_TOKEN
  const database = process.env.MOTHERDUCK_DATABASE || 'newagekbo'

  if (!token) {
    throw new Error('MOTHERDUCK_TOKEN not set in .env.local')
  }

  const db = await DuckDBInstance.create(':memory:')
  const conn = await db.connect()

  try {
    await conn.run(`SET home_directory='/tmp'`)
    await conn.run(`SET extension_directory='/tmp/.duckdb/extensions'`)
    await conn.run(`SET temp_directory='/tmp'`)
    process.env.motherduck_token = token

    await conn.run(`ATTACH 'md:${database}' AS md`)
    await conn.run(`USE md`)

    console.log('\n=== Peppol Coverage Highlights - NACE 2025 / Activity Group 001 (excluding Hermes) ===\n')
    console.log('Coverage = Peppol registered (non-Hermes) / Total KBO entities')
    console.log(`Showing NACE codes with >= ${minEntities} entities\n`)

    const query = `
      WITH
      -- Peppol registered companies (excluding Hermes)
      peppol_companies AS (
        SELECT DISTINCT
          CONCAT(
            SUBSTR(company_id, 1, 4), '.',
            SUBSTR(company_id, 5, 3), '.',
            SUBSTR(company_id, 8, 3)
          ) AS enterprise_number
        FROM peppol_registrations
        WHERE registration_status = 'active'
          AND as4_endpoint_url NOT LIKE '%hermes%'
      ),
      -- Deduplicate: assign each entity to its first NACE code (by nace_code)
      -- Filtered to NACE 2025 and activity_group 001 (BTW)
      entity_primary_nace AS (
        SELECT
          a.entity_number,
          a.nace_code,
          ROW_NUMBER() OVER (PARTITION BY a.entity_number ORDER BY a.nace_code) AS rn
        FROM activities_current a
        WHERE a._is_current = true
          AND a.classification = 'MAIN'
          AND a.nace_version = '2025'
          AND a.activity_group = '001'
      ),
      deduplicated AS (
        SELECT entity_number, nace_code
        FROM entity_primary_nace
        WHERE rn = 1
      ),
      -- Count per NACE code (deduplicated entities)
      nace_counts AS (
        SELECT
          d.nace_code,
          COUNT(*) AS total_entities,
          COUNT(CASE WHEN p.enterprise_number IS NOT NULL THEN 1 END) AS peppol_registered,
          ROUND(100.0 * COUNT(CASE WHEN p.enterprise_number IS NOT NULL THEN 1 END) / COUNT(*), 1) AS coverage_pct
        FROM deduplicated d
        LEFT JOIN peppol_companies p ON d.entity_number = p.enterprise_number
        GROUP BY d.nace_code
        HAVING COUNT(*) >= ${minEntities}
      )
      SELECT
        nc.nace_code,
        COALESCE(
          (SELECT description_nl FROM nace_codes WHERE nace_code = nc.nace_code LIMIT 1),
          '(no description)'
        ) AS nace_description_nl,
        COALESCE(
          (SELECT description_fr FROM nace_codes WHERE nace_code = nc.nace_code LIMIT 1),
          '(no description)'
        ) AS nace_description_fr,
        nc.total_entities,
        nc.peppol_registered,
        nc.coverage_pct
      FROM nace_counts nc
      ORDER BY nc.coverage_pct DESC, nc.nace_code
    `

    const result = await conn.run(query)
    const allRows: NaceRow[] = []

    for (const chunk of await result.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        allRows.push({
          nace_code: row[0] as string,
          nace_description_nl: String(row[1]),
          nace_description_fr: String(row[2]),
          total_entities: Number(row[3]),
          peppol_registered: Number(row[4]),
          coverage_pct: Number(row[5])
        })
      }
    }

    // Already sorted by coverage DESC from query, but ensure it
    allRows.sort((a, b) => b.coverage_pct - a.coverage_pct)

    // Print header
    console.log('='.repeat(125))
    console.log('PEPPOL COVERAGE RANKING')
    console.log('='.repeat(125))
    console.log('  ' + '#'.padStart(3) + '  ' + 'NACE'.padEnd(10) + 'Total'.padStart(10) + 'Peppol'.padStart(10) + 'Coverage'.padStart(10) + '  Description')
    console.log('  ' + '-'.repeat(121))

    let totalEntities = 0, totalPeppol = 0
    let highCount = 0, highEntities = 0, highPeppol = 0
    let midCount = 0, midEntities = 0, midPeppol = 0
    let lowCount = 0, lowEntities = 0, lowPeppolCount = 0
    let passedHighThreshold = false
    let passedLowThreshold = false

    for (let i = 0; i < allRows.length; i++) {
      const item = allRows[i]
      const rank = i + 1

      // Check for threshold crossings and print markers
      if (!passedHighThreshold && item.coverage_pct <= 50) {
        passedHighThreshold = true
        console.log('  ' + '-'.repeat(121))
        console.log(`  ${''.padStart(3)}  ${'HIGH >50%'.padEnd(10)}${String(highEntities).padStart(10)}${String(highPeppol).padStart(10)}${(Math.round(1000 * highPeppol / highEntities) / 10 + '%').padStart(10)}  (${highCount} NACE codes)`)
        console.log('  ' + '='.repeat(121))
      }

      if (!passedLowThreshold && item.coverage_pct < 30) {
        passedLowThreshold = true
        console.log('  ' + '-'.repeat(121))
        console.log(`  ${''.padStart(3)}  ${'MID 30-50%'.padEnd(10)}${String(midEntities).padStart(10)}${String(midPeppol).padStart(10)}${(Math.round(1000 * midPeppol / midEntities) / 10 + '%').padStart(10)}  (${midCount} NACE codes)`)
        console.log('  ' + '='.repeat(121))
      }

      // Track totals by group
      if (item.coverage_pct > 50) {
        highCount++
        highEntities += item.total_entities
        highPeppol += item.peppol_registered
      } else if (item.coverage_pct >= 30) {
        midCount++
        midEntities += item.total_entities
        midPeppol += item.peppol_registered
      } else {
        lowCount++
        lowEntities += item.total_entities
        lowPeppolCount += item.peppol_registered
      }

      totalEntities += item.total_entities
      totalPeppol += item.peppol_registered

      const rankStr = String(rank).padStart(3)
      const naceStr = item.nace_code.padEnd(10)
      const totalStr = String(item.total_entities).padStart(10)
      const peppolStr = String(item.peppol_registered).padStart(10)
      const coverageStr = (item.coverage_pct + '%').padStart(10)
      console.log(`  ${rankStr}  ${naceStr}${totalStr}${peppolStr}${coverageStr}  ${item.nace_description_nl}`)
      console.log(`  ${''.padStart(3)}  ${''.padEnd(10)}${''.padStart(10)}${''.padStart(10)}${''.padStart(10)}  ${item.nace_description_fr}`)
    }

    // Final subtotal for low coverage
    console.log('  ' + '-'.repeat(121))
    console.log(`  ${''.padStart(3)}  ${'LOW <30%'.padEnd(10)}${String(lowEntities).padStart(10)}${String(lowPeppolCount).padStart(10)}${(Math.round(1000 * lowPeppolCount / lowEntities) / 10 + '%').padStart(10)}  (${lowCount} NACE codes)`)

    // Summary
    console.log('  ' + '='.repeat(121))
    console.log(`  ${''.padStart(3)}  ${'TOTAL'.padEnd(10)}${String(totalEntities).padStart(10)}${String(totalPeppol).padStart(10)}${(Math.round(1000 * totalPeppol / totalEntities) / 10 + '%').padStart(10)}  (${allRows.length} NACE codes)`)

  } finally {
    conn.closeSync()
  }
}

const minEntities = parseInt(process.argv[2] || '1000', 10)
queryCoverageHighlights(minEntities).catch(console.error)
