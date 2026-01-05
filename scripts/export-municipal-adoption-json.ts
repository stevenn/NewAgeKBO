/**
 * Export: Municipal Peppol adoption data to JSON
 * Generates static JSON for peppolcheck's Regional Adoption Map (municipality view)
 *
 * Usage:
 *   npx tsx scripts/export-municipal-adoption-json.ts [output-path]
 *
 * Args:
 *   output-path: Path to output JSON file (default: ../peppolcheck/data/municipal-adoption-data.json)
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'
import { writeFileSync } from 'fs'

config({ path: resolve(__dirname, '../.env.local') })

interface MunicipalityData {
  nisCode: string
  nameNl: string
  nameFr: string
  arrNis: string
  totalEntities: number
  peppolRegistered: number
  adoptionPct: number
}

interface MunicipalAdoptionData {
  snapshotDate: string
  totalWithAddress: number
  totalWithoutAddress: number
  totalPeppolWithAddress: number
  totalPeppolWithoutAddress: number
  municipalities: MunicipalityData[]
}

async function exportMunicipalAdoption(outputPath: string) {
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

    console.log('Querying municipal adoption data...')

    // Main query with establishment fallback and postal-to-municipality mapping
    const postalMappingPath = resolve(__dirname, '../data/postal-mapping.json')
    const query = `
      WITH
      -- Load postal code to municipality mapping from local JSON file
      postal_mapping AS (
        SELECT DISTINCT
          postal_code,
          mun_nis AS municipality_nis,
          mun_nl AS municipality_name_nl,
          mun_fr AS municipality_name_fr,
          arr_nis AS arrondissement_nis
        FROM read_json('${postalMappingPath}')
      ),

      -- Peppol registered companies
      peppol_companies AS (
        SELECT DISTINCT
          CONCAT(
            SUBSTR(company_id, 1, 4), '.',
            SUBSTR(company_id, 5, 3), '.',
            SUBSTR(company_id, 8, 3)
          ) AS enterprise_number
        FROM peppol_registrations
        WHERE registration_status IN ('active', 'parked')
      ),

      -- VAT entities (activity_group='001')
      vat_entities AS (
        SELECT DISTINCT entity_number
        FROM activities
        WHERE _is_current = true AND activity_group = '001'
      ),

      -- Enterprise REGO addresses (priority 1)
      enterprise_rego AS (
        SELECT
          entity_number,
          zipcode,
          ROW_NUMBER() OVER (PARTITION BY entity_number ORDER BY _extract_number DESC) AS rn
        FROM addresses
        WHERE _is_current = true
          AND entity_type = 'enterprise'
          AND type_of_address = 'REGO'
          AND zipcode IS NOT NULL
      ),

      -- Establishment addresses (priority 2 - fallback)
      establishment_addresses AS (
        SELECT
          e.enterprise_number,
          a.zipcode,
          ROW_NUMBER() OVER (PARTITION BY e.enterprise_number ORDER BY e.establishment_number) AS rn
        FROM establishments e
        JOIN addresses a ON a.entity_number = e.establishment_number
        WHERE e._is_current = true
          AND a._is_current = true
          AND a.entity_type = 'establishment'
          AND a.zipcode IS NOT NULL
      ),

      -- Combine with fallback logic
      entity_zipcode AS (
        SELECT
          v.entity_number,
          COALESCE(
            (SELECT zipcode FROM enterprise_rego er WHERE er.entity_number = v.entity_number AND er.rn = 1),
            (SELECT zipcode FROM establishment_addresses ea WHERE ea.enterprise_number = v.entity_number AND ea.rn = 1)
          ) AS zipcode
        FROM vat_entities v
      ),

      -- Join with postal mapping (explicit type casting to ensure match)
      entity_municipality AS (
        SELECT
          ez.entity_number,
          ez.zipcode,
          pm.municipality_nis,
          pm.municipality_name_nl,
          pm.municipality_name_fr,
          pm.arrondissement_nis
        FROM entity_zipcode ez
        LEFT JOIN postal_mapping pm ON TRIM(CAST(ez.zipcode AS VARCHAR)) = pm.postal_code
      )

      -- Final aggregation by municipality
      SELECT
        em.municipality_nis,
        MAX(em.municipality_name_nl) AS municipality_name_nl,
        MAX(em.municipality_name_fr) AS municipality_name_fr,
        MAX(em.arrondissement_nis) AS arrondissement_nis,
        COUNT(DISTINCT em.entity_number) AS total_entities,
        COUNT(DISTINCT CASE WHEN p.enterprise_number IS NOT NULL THEN em.entity_number END) AS peppol_registered,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN p.enterprise_number IS NOT NULL THEN em.entity_number END) /
              NULLIF(COUNT(DISTINCT em.entity_number), 0), 1) AS adoption_pct
      FROM entity_municipality em
      LEFT JOIN peppol_companies p ON em.entity_number = p.enterprise_number
      WHERE em.municipality_nis IS NOT NULL
      GROUP BY em.municipality_nis
      ORDER BY em.municipality_nis
    `

    const result = await conn.run(query)

    const municipalities: MunicipalityData[] = []

    for (const chunk of await result.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        municipalities.push({
          nisCode: String(row[0]),
          nameNl: String(row[1]),
          nameFr: String(row[2]),
          arrNis: String(row[3]),
          totalEntities: Number(row[4]),
          peppolRegistered: Number(row[5]),
          adoptionPct: Number(row[6])
        })
      }
    }

    console.log(`Found ${municipalities.length} municipalities`)

    // Query for coverage stats (reuse from regional script)
    const coverageQuery = `
      WITH
      vat_entities AS (
        SELECT DISTINCT entity_number
        FROM activities
        WHERE _is_current = true AND activity_group = '001'
      ),
      enterprise_rego AS (
        SELECT entity_number, zipcode,
          ROW_NUMBER() OVER (PARTITION BY entity_number ORDER BY _extract_number DESC) AS rn
        FROM addresses
        WHERE _is_current = true AND entity_type = 'enterprise'
          AND type_of_address = 'REGO' AND zipcode IS NOT NULL
      ),
      establishment_addresses AS (
        SELECT e.enterprise_number, a.zipcode,
          ROW_NUMBER() OVER (PARTITION BY e.enterprise_number ORDER BY e.establishment_number) AS rn
        FROM establishments e
        JOIN addresses a ON a.entity_number = e.establishment_number
        WHERE e._is_current = true AND a._is_current = true
          AND a.entity_type = 'establishment' AND a.zipcode IS NOT NULL
      ),
      entity_zipcode AS (
        SELECT v.entity_number,
          COALESCE(
            (SELECT zipcode FROM enterprise_rego er WHERE er.entity_number = v.entity_number AND er.rn = 1),
            (SELECT zipcode FROM establishment_addresses ea WHERE ea.enterprise_number = v.entity_number AND ea.rn = 1)
          ) AS zipcode
        FROM vat_entities v
      ),
      peppol_companies AS (
        SELECT DISTINCT
          CONCAT(SUBSTR(company_id, 1, 4), '.', SUBSTR(company_id, 5, 3), '.', SUBSTR(company_id, 8, 3)) AS enterprise_number
        FROM peppol_registrations
        WHERE registration_status IN ('active', 'parked')
      )
      SELECT
        COUNT(*) AS total_vat,
        COUNT(zipcode) AS with_address,
        (SELECT COUNT(*) FROM peppol_companies) AS total_peppol,
        (SELECT COUNT(*) FROM peppol_companies p
         JOIN entity_zipcode ez ON p.enterprise_number = ez.entity_number
         WHERE ez.zipcode IS NOT NULL) AS peppol_with_address
      FROM entity_zipcode
    `

    const coverageResult = await conn.run(coverageQuery)
    let totalWithAddress = 0
    let totalWithoutAddress = 0
    let totalPeppolWithAddress = 0
    let totalPeppolWithoutAddress = 0

    for (const chunk of await coverageResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        const totalVat = Number(row[0])
        totalWithAddress = Number(row[1])
        totalWithoutAddress = totalVat - totalWithAddress
        const totalPeppol = Number(row[2])
        totalPeppolWithAddress = Number(row[3])
        totalPeppolWithoutAddress = totalPeppol - totalPeppolWithAddress
      }
    }

    // Build final output
    const output: MunicipalAdoptionData = {
      snapshotDate: new Date().toISOString().split('T')[0],
      totalWithAddress,
      totalWithoutAddress,
      totalPeppolWithAddress,
      totalPeppolWithoutAddress,
      municipalities
    }

    // Write to file
    writeFileSync(outputPath, JSON.stringify(output, null, 2))
    console.log(`\nExported to: ${outputPath}`)
    console.log(`Municipalities: ${municipalities.length}`)
    console.log(`VAT entities with address: ${totalWithAddress.toLocaleString()} (${Math.round(1000 * totalWithAddress / (totalWithAddress + totalWithoutAddress)) / 10}%)`)
    console.log(`PEPPOL entities with address: ${totalPeppolWithAddress.toLocaleString()} (${Math.round(1000 * totalPeppolWithAddress / (totalPeppolWithAddress + totalPeppolWithoutAddress)) / 10}%)`)

    // Summary stats
    const totalEntities = municipalities.reduce((sum, m) => sum + m.totalEntities, 0)
    const totalPeppol = municipalities.reduce((sum, m) => sum + m.peppolRegistered, 0)
    console.log(`Total mapped entities: ${totalEntities.toLocaleString()}`)
    console.log(`Total mapped PEPPOL: ${totalPeppol.toLocaleString()}`)
    console.log(`Overall adoption: ${Math.round(1000 * totalPeppol / totalEntities) / 10}%`)

    // Show top/bottom municipalities
    const sorted = [...municipalities].sort((a, b) => b.adoptionPct - a.adoptionPct)
    console.log('\nTop 5 municipalities:')
    for (const mun of sorted.slice(0, 5)) {
      console.log(`  ${mun.nameNl}: ${mun.adoptionPct}% (${mun.peppolRegistered.toLocaleString()}/${mun.totalEntities.toLocaleString()})`)
    }
    console.log('\nBottom 5 municipalities:')
    for (const mun of sorted.slice(-5).reverse()) {
      console.log(`  ${mun.nameNl}: ${mun.adoptionPct}% (${mun.peppolRegistered.toLocaleString()}/${mun.totalEntities.toLocaleString()})`)
    }

  } finally {
    conn.closeSync()
  }
}

const outputPath = process.argv[2] || resolve(__dirname, '../../peppolcheck/data/municipal-adoption-data.json')
exportMunicipalAdoption(outputPath).catch(console.error)
