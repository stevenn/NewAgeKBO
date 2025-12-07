/**
 * Query: Peppol registration coverage by NACE code hierarchy
 * Shows total KBO entities vs Peppol-registered entities (excluding Hermes)
 *
 * Coverage = (entities with non-Hermes Peppol registration) / (total KBO entities with this NACE)
 *
 * NACE Hierarchy:
 *   - Section (2 digits): e.g., 43 = Construction
 *   - Division/Group (3-4 digits): e.g., 432 = Electrical installation
 *   - Class (5 digits): e.g., 43211 = General electrical installation
 *
 * Usage:
 *   npx tsx scripts/query-peppol-coverage-by-nace.ts [min-entities]
 *
 * Args:
 *   min-entities: Minimum entities to include a NACE code (default: 1000)
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

config({ path: resolve(__dirname, '../.env.local') })

async function queryCoverage(minEntities: number) {
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

    console.log('\n=== Peppol Registration Coverage by NACE 2025 / Activity Group 001 (excluding Hermes) ===\n')
    console.log('Coverage = Peppol registered (non-Hermes) / Total KBO entities')
    console.log(`Showing NACE codes with >= ${minEntities} entities\n`)

    // Query grouped by NACE section (first 2 digits), with all detailed codes below
    // Each entity is deduplicated - only counted once under its first NACE code
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
      -- Count per NACE code (deduplicated entities) - ALL codes, no filter yet
      nace_counts_all AS (
        SELECT
          d.nace_code,
          SUBSTR(d.nace_code, 1, 2) AS nace_section,
          COUNT(*) AS total_entities,
          COUNT(CASE WHEN p.enterprise_number IS NOT NULL THEN 1 END) AS peppol_registered
        FROM deduplicated d
        LEFT JOIN peppol_companies p ON d.entity_number = p.enterprise_number
        GROUP BY d.nace_code
      ),
      -- Section totals (from ALL codes, not filtered)
      section_totals AS (
        SELECT
          nace_section,
          SUM(total_entities) AS section_total,
          SUM(peppol_registered) AS section_peppol
        FROM nace_counts_all
        GROUP BY nace_section
      ),
      -- Filtered NACE codes for display
      nace_counts_filtered AS (
        SELECT * FROM nace_counts_all
        WHERE total_entities >= ${minEntities}
      ),
      -- Sum of filtered codes per section (for calculating "Other")
      filtered_totals AS (
        SELECT
          nace_section,
          SUM(total_entities) AS filtered_total,
          SUM(peppol_registered) AS filtered_peppol
        FROM nace_counts_filtered
        GROUP BY nace_section
      ),
      -- Combine filtered codes with "Other" row
      combined AS (
        -- Regular NACE codes
        SELECT
          nc.nace_section,
          nc.nace_code,
          nc.total_entities,
          nc.peppol_registered,
          0 AS is_other
        FROM nace_counts_filtered nc
        UNION ALL
        -- "Other" row per section
        SELECT
          st.nace_section,
          'Other' AS nace_code,
          st.section_total - COALESCE(ft.filtered_total, 0) AS total_entities,
          st.section_peppol - COALESCE(ft.filtered_peppol, 0) AS peppol_registered,
          1 AS is_other
        FROM section_totals st
        LEFT JOIN filtered_totals ft ON st.nace_section = ft.nace_section
        WHERE st.section_total - COALESCE(ft.filtered_total, 0) > 0
      )
      SELECT
        c.nace_section,
        COALESCE(
          (SELECT description_nl FROM nace_codes WHERE nace_code = c.nace_section LIMIT 1),
          '(no description)'
        ) AS section_description,
        st.section_total,
        st.section_peppol,
        ROUND(100.0 * st.section_peppol / st.section_total, 1) AS section_coverage,
        c.nace_code,
        CASE WHEN c.is_other = 1 THEN '(codes below threshold)'
             ELSE COALESCE(
               (SELECT description_nl FROM nace_codes WHERE nace_code = c.nace_code LIMIT 1),
               '(no description)'
             )
        END AS nace_description,
        c.total_entities,
        c.peppol_registered,
        ROUND(100.0 * c.peppol_registered / NULLIF(c.total_entities, 0), 1) AS coverage_pct
      FROM combined c
      JOIN section_totals st ON c.nace_section = st.nace_section
      ORDER BY c.nace_section, c.is_other, c.nace_code
    `

    const result = await conn.run(query)
    let currentSection = ''

    for (const chunk of await result.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        const section = row[0] as string
        const sectionDesc = String(row[1])
        const sectionTotal = row[2]
        const sectionPeppol = row[3]
        const sectionCoverage = row[4]
        const nace = row[5] as string
        const desc = String(row[6]).substring(0, 45)
        const total = row[7]
        const peppol = row[8]
        const coverage = row[9]

        if (section !== currentSection) {
          currentSection = section
          console.log(`\n${'='.repeat(95)}`)
          console.log(`SECTION ${section} - ${sectionDesc}`)
          console.log(`Total: ${sectionTotal}  |  Peppol: ${sectionPeppol}  |  Coverage: ${sectionCoverage}%`)
          console.log(`${'='.repeat(95)}`)
          console.log('  NACE'.padEnd(12) + 'Description'.padEnd(47) + 'Total'.padStart(10) + 'Peppol'.padStart(10) + 'Coverage'.padStart(10))
          console.log('  ' + '-'.repeat(91))
        }

        const indent = nace.length <= 2 ? '' : '  '
        const naceStr = (indent + nace).padEnd(12)
        const descStr = desc.padEnd(47)
        const totalStr = String(total).padStart(10)
        const peppolStr = String(peppol).padStart(10)
        const coverageStr = (coverage + '%').padStart(10)
        console.log(`  ${naceStr}${descStr}${totalStr}${peppolStr}${coverageStr}`)
      }
    }

    // Overall summary
    console.log('\n\n' + '='.repeat(95))
    console.log('OVERALL SUMMARY')
    console.log('='.repeat(95))

    const summaryQuery = `
      WITH peppol_companies AS (
        SELECT DISTINCT
          CONCAT(
            SUBSTR(company_id, 1, 4), '.',
            SUBSTR(company_id, 5, 3), '.',
            SUBSTR(company_id, 8, 3)
          ) AS enterprise_number
        FROM peppol_registrations
        WHERE registration_status = 'active'
          AND as4_endpoint_url NOT LIKE '%hermes%'
      )
      SELECT
        COUNT(DISTINCT e.enterprise_number) AS total_enterprises,
        COUNT(DISTINCT CASE WHEN p.enterprise_number IS NOT NULL THEN e.enterprise_number END) AS peppol_registered,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.enterprise_number IS NOT NULL THEN e.enterprise_number END) / COUNT(DISTINCT e.enterprise_number), 2) AS coverage_pct
      FROM enterprises_current e
      LEFT JOIN peppol_companies p ON e.enterprise_number = p.enterprise_number
      WHERE e._is_current = true
    `
    const summaryResult = await conn.run(summaryQuery)
    for (const chunk of await summaryResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        console.log(`\nTotal KBO enterprises:          ${row[0]}`)
        console.log(`Peppol registered (non-Hermes): ${row[1]}`)
        console.log(`Overall coverage:               ${row[2]}%`)
      }
    }

  } finally {
    conn.closeSync()
  }
}

const minEntities = parseInt(process.argv[2] || '1000', 10)
queryCoverage(minEntities).catch(console.error)
