/**
 * Query: Distribution of Peppol registrations by AS4 provider and NACE activity
 *
 * Usage:
 *   npx tsx scripts/query-peppol-nace-distribution.ts
 */

import { DuckDBInstance } from '@duckdb/node-api'
import { config } from 'dotenv'
import { resolve } from 'path'

config({ path: resolve(__dirname, '../.env.local') })

async function queryDistribution() {
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

    console.log('\n=== Peppol Registration Demographics (excluding Hermes) ===\n')

    // Summary by AS4 provider (extract hostname from as4_endpoint_url)
    console.log('--- Registrations by AS4 Provider ---\n')
    const summaryQuery = `
      SELECT
        -- Extract hostname from as4_endpoint_url
        REGEXP_EXTRACT(as4_endpoint_url, 'https?://([^/]+)', 1) AS as4_provider,
        COUNT(*) AS total_registrations,
        COUNT(DISTINCT company_id) AS unique_companies
      FROM peppol_registrations
      WHERE registration_status = 'active'
        AND as4_endpoint_url NOT LIKE '%hermes%'
      GROUP BY as4_provider
      ORDER BY total_registrations DESC
    `
    const summaryResult = await conn.run(summaryQuery)
    console.log('Provider'.padEnd(45) + 'Registrations'.padStart(15) + 'Companies'.padStart(12))
    console.log('-'.repeat(72))
    for (const chunk of await summaryResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        const provider = String(row[0]).padEnd(45)
        const regs = String(row[1]).padStart(15)
        const companies = String(row[2]).padStart(12)
        console.log(`${provider}${regs}${companies}`)
      }
    }

    // Top NACE activities overall (non-Hermes)
    console.log('\n\n--- Top 25 NACE Activities ---\n')
    const naceQuery = `
      WITH peppol_with_kbo AS (
        SELECT
          p.company_id,
          CONCAT(
            SUBSTR(p.company_id, 1, 4), '.',
            SUBSTR(p.company_id, 5, 3), '.',
            SUBSTR(p.company_id, 8, 3)
          ) AS enterprise_number
        FROM peppol_registrations p
        WHERE p.registration_status = 'active'
          AND p.as4_endpoint_url NOT LIKE '%hermes%'
      )
      SELECT
        a.nace_code,
        COALESCE(n.description_nl, '(no description)') AS description,
        COUNT(*) AS count
      FROM peppol_with_kbo pk
      JOIN activities_current a
        ON pk.enterprise_number = a.entity_number
        AND a.classification = 'MAIN'
        AND a._is_current = true
      LEFT JOIN nace_codes n
        ON a.nace_code = n.nace_code
        AND a.nace_version = n.nace_version
      GROUP BY a.nace_code, n.description_nl
      ORDER BY count DESC
      LIMIT 25
    `
    console.log('NACE'.padEnd(8) + 'Count'.padStart(10) + '  Description')
    console.log('-'.repeat(80))
    const naceResult = await conn.run(naceQuery)
    for (const chunk of await naceResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        const code = String(row[0]).padEnd(8)
        const count = String(row[2]).padStart(10)
        const desc = String(row[1]).substring(0, 58)
        console.log(`${code}${count}  ${desc}`)
      }
    }

    // Grouped by activity_group (001-007), then NACE code, then top 5 AS4 providers
    console.log('\n\n--- Activity Group → NACE Code → Top 5 AS4 Providers ---\n')
    const crossQuery = `
      WITH peppol_with_kbo AS (
        SELECT
          p.company_id,
          REGEXP_EXTRACT(p.as4_endpoint_url, 'https?://([^/]+)', 1) AS as4_provider,
          CONCAT(
            SUBSTR(p.company_id, 1, 4), '.',
            SUBSTR(p.company_id, 5, 3), '.',
            SUBSTR(p.company_id, 8, 3)
          ) AS enterprise_number
        FROM peppol_registrations p
        WHERE p.registration_status = 'active'
          AND p.as4_endpoint_url NOT LIKE '%hermes%'
      ),
      -- Get activity group totals
      group_totals AS (
        SELECT
          a.activity_group,
          COUNT(*) AS group_total
        FROM peppol_with_kbo pk
        JOIN activities_current a
          ON pk.enterprise_number = a.entity_number
          AND a._is_current = true
        GROUP BY a.activity_group
      ),
      -- Get NACE code totals within each activity group
      nace_totals AS (
        SELECT
          a.activity_group,
          a.nace_code,
          COUNT(*) AS nace_total,
          ROW_NUMBER() OVER (PARTITION BY a.activity_group ORDER BY COUNT(*) DESC) AS nace_rank
        FROM peppol_with_kbo pk
        JOIN activities_current a
          ON pk.enterprise_number = a.entity_number
          AND a._is_current = true
        GROUP BY a.activity_group, a.nace_code
      ),
      -- Get top 5 NACE codes per activity group
      top_nace AS (
        SELECT activity_group, nace_code, nace_total
        FROM nace_totals
        WHERE nace_rank <= 5
      ),
      -- Get provider counts per activity group + NACE code
      nace_provider_counts AS (
        SELECT
          a.activity_group,
          a.nace_code,
          pk.as4_provider,
          COUNT(*) AS provider_count,
          ROW_NUMBER() OVER (PARTITION BY a.activity_group, a.nace_code ORDER BY COUNT(*) DESC) AS provider_rank
        FROM peppol_with_kbo pk
        JOIN activities_current a
          ON pk.enterprise_number = a.entity_number
          AND a._is_current = true
        JOIN top_nace tn ON a.activity_group = tn.activity_group AND a.nace_code = tn.nace_code
        GROUP BY a.activity_group, a.nace_code, pk.as4_provider
      )
      SELECT
        gt.activity_group,
        COALESCE(
          (SELECT description FROM codes WHERE category = 'ActivityGroup' AND code = gt.activity_group AND language = 'NL' LIMIT 1),
          '(no description)'
        ) AS group_description,
        gt.group_total,
        tn.nace_code,
        COALESCE(
          (SELECT description_nl FROM nace_codes WHERE nace_code = tn.nace_code LIMIT 1),
          '(no description)'
        ) AS nace_description,
        tn.nace_total,
        npc.as4_provider,
        npc.provider_count,
        ROUND(100.0 * npc.provider_count / tn.nace_total, 1) AS pct
      FROM group_totals gt
      JOIN top_nace tn ON gt.activity_group = tn.activity_group
      JOIN nace_provider_counts npc ON tn.activity_group = npc.activity_group
        AND tn.nace_code = npc.nace_code
        AND npc.provider_rank <= 5
      ORDER BY gt.group_total DESC, gt.activity_group, tn.nace_total DESC, tn.nace_code, npc.provider_rank
    `
    const crossResult = await conn.run(crossQuery)
    let currentGroup = ''
    let currentNace = ''
    for (const chunk of await crossResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        const group = row[0] as string
        const groupDesc = row[1] as string
        const groupTotal = row[2]
        const nace = row[3] as string
        const naceDesc = String(row[4]).substring(0, 40)
        const naceTotal = row[5]

        if (group !== currentGroup) {
          currentGroup = group
          currentNace = ''
          console.log(`\n${'='.repeat(70)}`)
          console.log(`ACTIVITY GROUP ${group} - ${groupDesc} (${groupTotal} total)`)
          console.log(`${'='.repeat(70)}`)
        }

        if (nace !== currentNace) {
          currentNace = nace
          console.log(`\n  ${nace} - ${naceDesc} (${naceTotal})`)
        }

        const provider = String(row[6]).substring(0, 40).padEnd(42)
        const count = String(row[7]).padStart(6)
        const pct = String(row[8]).padStart(5)
        console.log(`    ${provider} ${count} (${pct}%)`)
      }
    }

    // Stats summary
    console.log('\n\n--- Overall Stats ---\n')
    const statsQuery = `
      WITH peppol_with_kbo AS (
        SELECT
          p.company_id,
          CONCAT(
            SUBSTR(p.company_id, 1, 4), '.',
            SUBSTR(p.company_id, 5, 3), '.',
            SUBSTR(p.company_id, 8, 3)
          ) AS enterprise_number
        FROM peppol_registrations p
        WHERE p.registration_status = 'active'
          AND p.as4_endpoint_url NOT LIKE '%hermes%'
      )
      SELECT
        COUNT(DISTINCT pk.company_id) AS total_companies,
        COUNT(DISTINCT CASE WHEN a.nace_code IS NOT NULL THEN pk.company_id END) AS with_nace,
        COUNT(DISTINCT a.nace_code) AS unique_nace_codes
      FROM peppol_with_kbo pk
      LEFT JOIN activities_current a
        ON pk.enterprise_number = a.entity_number
        AND a.classification = 'MAIN'
        AND a._is_current = true
    `
    const statsResult = await conn.run(statsQuery)
    for (const chunk of await statsResult.fetchAllChunks()) {
      for (const row of chunk.getRows()) {
        console.log(`Total companies (non-Hermes): ${row[0]}`)
        console.log(`Companies with NACE match:    ${row[1]}`)
        console.log(`Unique NACE codes:            ${row[2]}`)
      }
    }

  } finally {
    conn.closeSync()
  }
}

queryDistribution().catch(console.error)
