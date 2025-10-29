#!/usr/bin/env tsx

/**
 * Test KBO Portal Authentication and File Download
 * This script tests the KBO client library authentication and download functionality
 */

import { config } from 'dotenv'
config({ path: ['.env.local', '.env'] })

import { downloadFile, extractFileMetadata } from '../lib/kbo-client'

async function main() {
  console.log('🔐 Testing KBO Portal Authentication\n')

  // Test URL - the one provided by the user
  const testUrl = 'https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/KboOpenData_0141_2025_10_06_Update.zip'

  console.log(`Test URL: ${testUrl}`)
  console.log('')

  // Extract and display metadata
  const filename = testUrl.split('/').pop() || ''
  const metadata = extractFileMetadata(filename)

  if (metadata) {
    console.log('📋 File Metadata:')
    console.log(`   Extract Number: ${metadata.extract_number}`)
    console.log(`   Snapshot Date: ${metadata.snapshot_date}`)
    console.log(`   File Type: ${metadata.file_type}`)
    console.log('')
  }

  // Test download
  console.log('📥 Attempting download...')
  console.log(`   Credentials: ${process.env.KBO_USERNAME ? '✓ Username set' : '✗ Username missing'}`)
  console.log(`   Credentials: ${process.env.KBO_PASSWORD ? '✓ Password set' : '✗ Password missing'}`)
  console.log('')

  try {
    const startTime = Date.now()
    const buffer = await downloadFile(testUrl)
    const duration = Date.now() - startTime

    console.log('✅ Download successful!')
    console.log(`   Size: ${(buffer.length / 1024).toFixed(2)} KB`)
    console.log(`   Duration: ${duration}ms`)
    console.log('')
    console.log('🎉 Authentication and download test passed!')

  } catch (error: any) {
    console.error('❌ Download failed!')
    console.error(`   Error: ${error.message}`)

    if (error.statusCode === 401 || error.statusCode === 403) {
      console.error('')
      console.error('   This appears to be an authentication error.')
      console.error('   Please check your KBO_USERNAME and KBO_PASSWORD in .env.local')
    }

    process.exit(1)
  }
}

main()
