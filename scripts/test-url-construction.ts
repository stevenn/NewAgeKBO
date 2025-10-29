#!/usr/bin/env tsx

/**
 * Quick test to verify URL construction matches expected format
 */

const KBO_BASE_URL = 'https://kbopub.economie.fgov.be/kbo-open-data'

// Test the URL construction logic from the library
function constructUrl(linkPath: string): string {
  let url: string
  if (linkPath.startsWith('http')) {
    url = linkPath
  } else if (linkPath.startsWith('/')) {
    url = `${KBO_BASE_URL}${linkPath}`
  } else {
    // Relative path like "files/KboOpenData_..."
    url = `${KBO_BASE_URL}/affiliation/xml/${linkPath}`
  }
  return url
}

// Test cases from the actual HTML
const testCases = [
  'files/KboOpenData_0139_2025_09_Update.zip',
  'files/KboOpenData_0140_2025_10_05_Full.zip',
  'files/KboOpenData_0165_2025_10_29_Update.zip',
]

console.log('Testing URL construction:\n')

testCases.forEach(linkPath => {
  const url = constructUrl(linkPath)
  console.log(`Input:  ${linkPath}`)
  console.log(`Output: ${url}`)
  console.log('')
})

// Expected format
console.log('Expected format: https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/files/KboOpenData_...zip')
