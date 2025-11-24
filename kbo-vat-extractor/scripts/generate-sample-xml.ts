#!/usr/bin/env tsx

/**
 * Generate a sample KBO XML file for testing the VAT extractor
 *
 * Creates an XML file with:
 * - Mix of enterprises with/without VAT authorizations
 * - Various VAT authorization scenarios (active, expired, etc.)
 * - Realistic Belgian enterprise numbers
 * - Compressed as .gz file
 */

import { createWriteStream } from 'fs'
import { createGzip } from 'zlib'
import { pipeline } from 'stream/promises'

interface SampleEnterprise {
  number: string
  hasVAT: boolean
  phase?: '001' | '002' | '004' // 001=Granted, 002=Refused, 004=Withdrawn
  validityStart?: string
  validityEnd?: string
}

/**
 * Generate a valid Belgian enterprise number (10 digits)
 */
function generateEnterpriseNumber(index: number): string {
  const base = 100000000 + index
  const checkDigit = 97 - (base % 97)
  const number = base.toString() + checkDigit.toString().padStart(2, '0')
  return number.substring(0, 4) + '.' + number.substring(4, 7) + '.' + number.substring(7)
}

/**
 * Generate random date in YYYY
 */
function randomDate(startYear: number, endYear: number): string {
  const year = startYear + Math.floor(Math.random() * (endYear - startYear + 1))
  const month = 1 + Math.floor(Math.random() * 12)
  const day = 1 + Math.floor(Math.random() * 28)
  return `${day.toString().padStart(2, '0')}-${month.toString().padStart(2, '0')}-${year}`
}

/**
 * Generate sample enterprises with various scenarios
 */
function generateSampleEnterprises(count: number): SampleEnterprise[] {
  const enterprises: SampleEnterprise[] = []

  for (let i = 0; i < count; i++) {
    const number = generateEnterpriseNumber(i)
    const scenario = Math.random()

    if (scenario < 0.40) {
      // 40% - VAT liable (active authorization)
      enterprises.push({
        number,
        hasVAT: true,
        phase: '001',
        validityStart: randomDate(2015, 2024),
        validityEnd: '', // No end date = still active
      })
    } else if (scenario < 0.45) {
      // 5% - VAT liable with expired authorization
      enterprises.push({
        number,
        hasVAT: true,
        phase: '001',
        validityStart: randomDate(2010, 2020),
        validityEnd: randomDate(2021, 2024),
      })
    } else if (scenario < 0.47) {
      // 2% - VAT refused
      enterprises.push({
        number,
        hasVAT: true,
        phase: '002',
        validityStart: randomDate(2020, 2024),
      })
    } else if (scenario < 0.48) {
      // 1% - VAT withdrawn
      enterprises.push({
        number,
        hasVAT: true,
        phase: '004',
        validityStart: randomDate(2015, 2022),
        validityEnd: randomDate(2023, 2024),
      })
    } else {
      // 52% - No VAT authorization
      enterprises.push({
        number,
        hasVAT: false,
      })
    }
  }

  return enterprises
}

/**
 * Generate XML content
 */
function generateXML(enterprises: SampleEnterprise[]): string {
  let xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
  xml += '<CommercialisationFileType>\n'
  xml += '  <Header>\n'
  xml += '    <ExecutionDate>05-11-2025</ExecutionDate>\n'
  xml += '    <SequenceNumber>SAMPLE</SequenceNumber>\n'
  xml += '    <ExtractVersion>1.0.0</ExtractVersion>\n'
  xml += '    <ExtractType>F</ExtractType>\n'
  xml += '  </Header>\n'
  xml += '  <Enterprises>\n'

  for (const ent of enterprises) {
    xml += '    <Enterprise>\n'
    xml += `      <Nbr>${ent.number.replace(/\./g, '')}</Nbr>\n`

    if (ent.hasVAT) {
      xml += '      <Authorizations>\n'
      xml += '        <Authorization>\n'
      xml += '          <Code>00001</Code>\n'
      xml += `          <PhaseCode>${ent.phase}</PhaseCode>\n`

      if (ent.validityStart || ent.validityEnd) {
        xml += '          <Validity>\n'
        if (ent.validityStart) {
          xml += `            <Begin>${ent.validityStart}</Begin>\n`
        }
        if (ent.validityEnd) {
          xml += `            <End>${ent.validityEnd}</End>\n`
        }
        xml += '          </Validity>\n'
      }

      xml += '        </Authorization>\n'
      xml += '      </Authorizations>\n'
    }

    xml += '    </Enterprise>\n'
  }

  xml += '  </Enterprises>\n'
  xml += '</CommercialisationFileType>\n'

  return xml
}

/**
 * Main execution
 */
async function main() {
  const enterpriseCount = parseInt(process.argv[2]) || 1000
  const outputFile = process.argv[3] || 'sample-kbo-data.xml.gz'

  console.log(`🏭 Generating ${enterpriseCount.toLocaleString()} sample enterprises...`)

  const enterprises = generateSampleEnterprises(enterpriseCount)

  // Count statistics
  const withVAT = enterprises.filter(e => e.hasVAT && e.phase === '001' && !e.validityEnd).length
  const expired = enterprises.filter(e => e.hasVAT && e.validityEnd).length
  const refused = enterprises.filter(e => e.hasVAT && e.phase === '002').length
  const withdrawn = enterprises.filter(e => e.hasVAT && e.phase === '004').length
  const noVAT = enterprises.filter(e => !e.hasVAT).length

  console.log(`\n📊 Sample data composition:`)
  console.log(`   ✓ VAT liable (active):     ${withVAT.toLocaleString()} (${((withVAT/enterpriseCount)*100).toFixed(1)}%)`)
  console.log(`   ⌛ VAT liable (expired):    ${expired.toLocaleString()} (${((expired/enterpriseCount)*100).toFixed(1)}%)`)
  console.log(`   ✗ VAT refused:             ${refused.toLocaleString()} (${((refused/enterpriseCount)*100).toFixed(1)}%)`)
  console.log(`   ⊗ VAT withdrawn:           ${withdrawn.toLocaleString()} (${((withdrawn/enterpriseCount)*100).toFixed(1)}%)`)
  console.log(`   − No VAT info:             ${noVAT.toLocaleString()} (${((noVAT/enterpriseCount)*100).toFixed(1)}%)`)

  console.log(`\n📝 Generating XML...`)
  const xml = generateXML(enterprises)

  console.log(`💾 Writing compressed file: ${outputFile}`)
  const gzipStream = createGzip()
  const outputStream = createWriteStream(outputFile)

  gzipStream.write(xml)
  gzipStream.end()

  await pipeline(gzipStream, outputStream)

  const sizeKB = Buffer.byteLength(xml) / 1024
  console.log(`\n✅ Sample file created successfully!`)
  console.log(`   Size (uncompressed): ${sizeKB.toFixed(1)} KB`)
  console.log(`   Enterprises: ${enterpriseCount.toLocaleString()}`)
  console.log(`\n🚀 Test with: npx tsx examples/extract-vat.ts ${outputFile}`)
}

main().catch(console.error)
