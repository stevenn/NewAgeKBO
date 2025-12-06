/**
 * Export: Peppol sector adoption data to JSON
 * Generates static JSON for peppolcheck's Sector Adoption page
 *
 * Usage:
 *   npx tsx scripts/export-sector-adoption-json.ts [output-path]
 *
 * Args:
 *   output-path: Path to output JSON file (default: ../peppolcheck/data/sector-adoption-data.json)
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'
import { writeFileSync } from 'fs'

config({ path: resolve(__dirname, '../.env.local') })

interface NaceCodeRecord {
  naceCode: string
  descriptionNl: string
  descriptionFr: string
  totalEntities: number
  peppolRegistered: number
  coveragePct: number
}

interface SectorGroup {
  sectionCode: string
  sectionDescriptionNl: string
  sectionDescriptionFr: string
  sectionTotal: number
  sectionPeppol: number
  sectionCoverage: number
  codes: NaceCodeRecord[]
}

interface SectorAdoptionData {
  snapshotDate: string
  sections: SectorGroup[]
}

const MIN_ENTITIES = 100

async function exportSectorAdoption(outputPath: string) {
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

    console.log('Querying sector adoption data...')

    // Query for ALL NACE codes with their counts
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
          SUBSTR(d.nace_code, 1, 2) AS nace_section,
          COUNT(*) AS total_entities,
          COUNT(CASE WHEN p.enterprise_number IS NOT NULL THEN 1 END) AS peppol_registered,
          ROUND(100.0 * COUNT(CASE WHEN p.enterprise_number IS NOT NULL THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS coverage_pct
        FROM deduplicated d
        LEFT JOIN peppol_companies p ON d.entity_number = p.enterprise_number
        GROUP BY d.nace_code
      )
      SELECT
        nc.nace_code,
        nc.nace_section,
        COALESCE(
          (SELECT description_nl FROM nace_codes WHERE nace_code = nc.nace_code AND nace_version = '2025' LIMIT 1),
          '(no description)'
        ) AS nace_description_nl,
        COALESCE(
          (SELECT description_fr FROM nace_codes WHERE nace_code = nc.nace_code AND nace_version = '2025' LIMIT 1),
          '(no description)'
        ) AS nace_description_fr,
        nc.total_entities,
        nc.peppol_registered,
        nc.coverage_pct
      FROM nace_counts nc
      ORDER BY nc.nace_section, nc.coverage_pct DESC, nc.nace_code
    `

    const result = await conn.run(query)

    interface RawRow {
      naceCode: string
      naceSection: string
      descriptionNl: string
      descriptionFr: string
      totalEntities: number
      peppolRegistered: number
      coveragePct: number
    }

    const allRows: RawRow[] = []

    for (const chunk of await result.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        allRows.push({
          naceCode: row[0] as string,
          naceSection: row[1] as string,
          descriptionNl: String(row[2]),
          descriptionFr: String(row[3]),
          totalEntities: Number(row[4]),
          peppolRegistered: Number(row[5]),
          coveragePct: Number(row[6])
        })
      }
    }

    console.log(`Found ${allRows.length} total NACE codes`)

    // Get ALL section descriptions (for all 2-digit codes, NACE 2025 version)
    const sectionDescQuery = `
      SELECT
        nace_code AS section_code,
        description_nl AS desc_nl,
        description_fr AS desc_fr
      FROM nace_codes
      WHERE LENGTH(nace_code) = 2
        AND nace_version = '2025'
      ORDER BY nace_code
    `
    const sectionDescResult = await conn.run(sectionDescQuery)
    const sectionDescs: Map<string, { nl: string; fr: string }> = new Map()

    for (const chunk of await sectionDescResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        sectionDescs.set(row[0] as string, {
          nl: String(row[1] || '(no description)'),
          fr: String(row[2] || '(no description)')
        })
      }
    }

    // Group by section and build output structure
    // For each section: codes with >=100 peppol + "Other" for the rest
    const sectionMap: Map<string, {
      codes: RawRow[]
      otherTotal: number
      otherPeppol: number
    }> = new Map()

    for (const row of allRows) {
      const sectionCode = row.naceSection

      if (!sectionMap.has(sectionCode)) {
        sectionMap.set(sectionCode, {
          codes: [],
          otherTotal: 0,
          otherPeppol: 0
        })
      }

      const section = sectionMap.get(sectionCode)!

      if (row.totalEntities >= MIN_ENTITIES) {
        // Individual code row
        section.codes.push(row)
      } else {
        // Aggregate into "Other"
        section.otherTotal += row.totalEntities
        section.otherPeppol += row.peppolRegistered
      }
    }

    // Build final sections array with all 2-digit NACE codes
    const sections: SectorGroup[] = []
    let totalDetailedCodes = 0
    let sectionsWithOther = 0

    for (const [sectionCode, desc] of sectionDescs) {
      const sectionData = sectionMap.get(sectionCode)

      // Calculate totals from all codes (detailed + other)
      const detailedTotal = sectionData?.codes.reduce((sum, c) => sum + c.totalEntities, 0) || 0
      const detailedPeppol = sectionData?.codes.reduce((sum, c) => sum + c.peppolRegistered, 0) || 0
      const otherTotal = sectionData?.otherTotal || 0
      const otherPeppol = sectionData?.otherPeppol || 0

      const sectionTotal = detailedTotal + otherTotal
      const sectionPeppol = detailedPeppol + otherPeppol

      // Skip sections with no data at all
      if (sectionTotal === 0) continue

      const codes: NaceCodeRecord[] = []

      // Add detailed codes (sorted by coverage desc)
      if (sectionData) {
        const sortedCodes = [...sectionData.codes].sort((a, b) => b.coveragePct - a.coveragePct)
        for (const code of sortedCodes) {
          codes.push({
            naceCode: code.naceCode,
            descriptionNl: code.descriptionNl,
            descriptionFr: code.descriptionFr,
            totalEntities: code.totalEntities,
            peppolRegistered: code.peppolRegistered,
            coveragePct: code.coveragePct
          })
        }
        totalDetailedCodes += sortedCodes.length
      }

      // Add "Other" row if there are aggregated codes
      if (otherTotal > 0) {
        const otherCoverage = otherTotal > 0
          ? Math.round(1000 * otherPeppol / otherTotal) / 10
          : 0
        codes.push({
          naceCode: `${sectionCode}-other`,
          descriptionNl: 'Overige activiteiten',
          descriptionFr: 'Autres activités',
          totalEntities: otherTotal,
          peppolRegistered: otherPeppol,
          coveragePct: otherCoverage
        })
        sectionsWithOther++
      }

      sections.push({
        sectionCode,
        sectionDescriptionNl: desc.nl,
        sectionDescriptionFr: desc.fr,
        sectionTotal,
        sectionPeppol,
        sectionCoverage: sectionTotal > 0
          ? Math.round(1000 * sectionPeppol / sectionTotal) / 10
          : 0,
        codes
      })
    }

    // Sort sections by section code
    sections.sort((a, b) => a.sectionCode.localeCompare(b.sectionCode))

    // Build final output
    const output: SectorAdoptionData = {
      snapshotDate: new Date().toISOString().split('T')[0],
      sections
    }

    // Write to file
    writeFileSync(outputPath, JSON.stringify(output, null, 2))
    console.log(`\nExported to: ${outputPath}`)
    console.log(`Sections: ${sections.length}`)
    console.log(`Detailed NACE codes (>=${MIN_ENTITIES} entities): ${totalDetailedCodes}`)
    console.log(`Sections with "Other" row: ${sectionsWithOther}`)

    // Summary stats
    const totalEntities = sections.reduce((sum, s) => sum + s.sectionTotal, 0)
    const totalPeppol = sections.reduce((sum, s) => sum + s.sectionPeppol, 0)
    console.log(`Total entities: ${totalEntities.toLocaleString()}`)
    console.log(`Total Peppol registrations: ${totalPeppol.toLocaleString()}`)
    console.log(`Overall coverage: ${Math.round(1000 * totalPeppol / totalEntities) / 10}%`)

  } finally {
    conn.closeSync()
  }
}

const outputPath = process.argv[2] || resolve(__dirname, '../../peppolcheck/data/sector-adoption-data.json')
exportSectorAdoption(outputPath).catch(console.error)
