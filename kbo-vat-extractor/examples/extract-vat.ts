#!/usr/bin/env tsx

/**
 * CLI tool to extract VAT statuses from KBO XML files
 *
 * Usage:
 *   npx tsx examples/extract-vat.ts <zip-file> [options]
 *
 * Options:
 *   --output <file>    Output CSV file (default: vat-statuses.csv)
 *   --verbose          Enable verbose logging
 *   --help             Show this help message
 *
 * Example:
 *   npx tsx examples/extract-vat.ts ./KboOpenData_Full.zip --output results.csv
 */

import { VATExtractor } from '../src/vat-extractor.js'
import chalk from 'chalk'
import { existsSync } from 'fs'

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2)

  if (args.includes('--help') || args.includes('-h')) {
    showHelp()
    process.exit(0)
  }

  if (args.length === 0) {
    console.error(chalk.red('❌ Error: ZIP file path required\n'))
    showHelp()
    process.exit(1)
  }

  const zipFilePath = args[0]
  const outputIndex = args.indexOf('--output')
  const outputFilePath =
    outputIndex >= 0 && args[outputIndex + 1]
      ? args[outputIndex + 1]
      : 'vat-statuses.csv'
  const verbose = args.includes('--verbose') || args.includes('-v')

  // Validate ZIP file exists
  if (!existsSync(zipFilePath)) {
    console.error(chalk.red(`❌ Error: File not found: ${zipFilePath}\n`))
    process.exit(1)
  }

  return {
    zipFilePath,
    outputFilePath,
    verbose,
  }
}

function showHelp() {
  console.log(chalk.bold('\nKBO VAT Status Extractor\n'))
  console.log('Usage:')
  console.log('  npx tsx examples/extract-vat.ts <zip-file> [options]\n')
  console.log('Options:')
  console.log('  --output <file>    Output CSV file (default: vat-statuses.csv)')
  console.log('  --verbose, -v      Enable verbose logging')
  console.log('  --help, -h         Show this help message\n')
  console.log('Example:')
  console.log('  npx tsx examples/extract-vat.ts ./KboOpenData_Full.zip --output results.csv\n')
}

// Main execution
async function main() {
  try {
    const config = parseArgs()

    console.log(
      chalk.bold.blue('\n╔═══════════════════════════════════════════════════════════╗')
    )
    console.log(
      chalk.bold.blue('║         KBO VAT Status Extractor v1.0.0                   ║')
    )
    console.log(
      chalk.bold.blue('╚═══════════════════════════════════════════════════════════╝\n')
    )

    const extractor = new VATExtractor(config)
    const stats = await extractor.extract()

    // Success! Stats already displayed by progress tracker
    process.exit(0)
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error))
    console.error(chalk.red(`\n❌ Fatal error: ${err.message}\n`))
    process.exit(1)
  }
}

main()
