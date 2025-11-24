/**
 * Main VAT extractor - coordinates parsing, progress tracking, and CSV output
 */

import { createReadStream, statSync } from 'fs'
import { createWriteStream } from 'fs'
import { createGunzip } from 'zlib'
import { pipeline } from 'stream/promises'
import { createObjectCsvWriter } from 'csv-writer'
import type { EnterpriseVATStatus, ExtractorConfig, ProgressStats } from './types.js'
import { KBOXMLParser } from './parser.js'
import { ProgressTracker } from './progress.js'
import chalk from 'chalk'

export class VATExtractor {
  private config: ExtractorConfig
  private parser: KBOXMLParser
  private progress: ProgressTracker
  private csvWriter: any
  private enterprisesProcessed = 0
  private vatLiableCount = 0
  private vatNotLiableCount = 0
  private noVatInfoCount = 0
  private enterprisesWithVat = new Set<string>()

  constructor(config: ExtractorConfig) {
    this.config = {
      progressUpdateIntervalMs: 100,
      ...config,
    }

    // Get file size for progress tracking
    const stats = statSync(this.config.zipFilePath)
    const totalBytes = stats.size

    // Create progress tracker
    this.progress = new ProgressTracker(totalBytes, this.config.progressUpdateIntervalMs)

    // Create CSV writer
    this.csvWriter = createObjectCsvWriter({
      path: this.config.outputFilePath,
      header: [
        { id: 'enterpriseNumber', title: 'Enterprise Number' },
        { id: 'vatLiable', title: 'VAT Liable' },
        { id: 'authorizationPhase', title: 'Authorization Phase' },
        { id: 'validityStart', title: 'Validity Start' },
        { id: 'validityEnd', title: 'Validity End' },
      ],
    })

    // Create parser with event handlers
    this.parser = new KBOXMLParser({
      onEnterprise: (enterpriseNumber) => {
        this.handleEnterpriseStart(enterpriseNumber)
      },
      onVATStatus: (status) => {
        this.handleVATStatus(status)
      },
      onBytesProcessed: (bytes) => {
        this.progress.update({ bytesProcessed: bytes })
      },
      onError: (error) => {
        console.error(chalk.red(`\n❌ Parser error: ${error.message}`))
        if (this.config.verbose) {
          console.error(error.stack)
        }
      },
    })
  }

  /**
   * Handle enterprise being processed
   */
  private handleEnterpriseStart(enterpriseNumber: string): void {
    this.enterprisesProcessed++

    // Update progress stats
    this.progress.update({
      enterprisesProcessed: this.enterprisesProcessed,
      lastEnterpriseNumber: enterpriseNumber,
    })
  }

  /**
   * Handle VAT status found
   */
  private async handleVATStatus(status: EnterpriseVATStatus): Promise<void> {
    // Track this enterprise as having VAT info
    if (!this.enterprisesWithVat.has(status.enterpriseNumber)) {
      this.enterprisesWithVat.add(status.enterpriseNumber)

      // Write all VAT authorizations to CSV (both liable and not liable)
      await this.csvWriter.writeRecords([
        {
          enterpriseNumber: status.enterpriseNumber,
          vatLiable: status.vatLiable ? 'YES' : 'NO',
          authorizationPhase: status.authorizationPhase || '',
          validityStart: status.validityStart?.toISOString().split('T')[0] || '',
          validityEnd: status.validityEnd?.toISOString().split('T')[0] || '',
        },
      ])

      if (status.vatLiable) {
        this.vatLiableCount++

        // Update progress
        this.progress.update({
          vatLiableFound: this.vatLiableCount,
          lastVatStatus: chalk.green(
            `(✓ VAT liable${status.validityStart ? ` since ${status.validityStart.toISOString().split('T')[0]}` : ''})`
          ),
        })
      } else {
        this.vatNotLiableCount++

        // Show reason for not liable
        let reason = ''
        if (status.authorizationPhase === '002') reason = 'refused'
        else if (status.authorizationPhase === '004') reason = 'withdrawn'
        else if (status.validityEnd) reason = `expired ${status.validityEnd.toISOString().split('T')[0]}`
        else reason = 'inactive'

        this.progress.update({
          vatNotLiable: this.vatNotLiableCount,
          lastVatStatus: chalk.red(`(✗ VAT not liable: ${reason})`),
        })
      }
    }
  }

  /**
   * Extract VAT statuses from KBO XML file
   */
  async extract(): Promise<ProgressStats> {
    try {
      console.log(chalk.bold(`📦 File: ${this.config.zipFilePath}`))
      console.log(chalk.bold(`📝 Output: ${this.config.outputFilePath}\n`))

      // Initialize parser
      await this.parser.initialize()

      // Start progress tracking
      this.progress.start()

      // Create read stream from ZIP file
      const fileStream = createReadStream(this.config.zipFilePath)

      // Decompress stream (assuming .zip is actually gzipped, or handle differently)
      // Note: For actual ZIP files, we'd need unzipper/yauzl, but for gzipped files:
      const decompressStream = createGunzip()

      // Setup graceful shutdown on Ctrl+C
      let isShuttingDown = false
      process.on('SIGINT', () => {
        if (!isShuttingDown) {
          isShuttingDown = true
          console.log(chalk.yellow('\n\n⚠️  Graceful shutdown initiated...\n'))
          fileStream.destroy()
          this.parser.end()
          this.progress.stop()
          process.exit(0)
        }
      })

      // Pipe file through decompression to parser
      await pipeline(fileStream, decompressStream, this.parser.writable)

      // Calculate final stats
      this.noVatInfoCount = this.enterprisesProcessed - this.enterprisesWithVat.size

      this.progress.update({
        noVatInfo: this.noVatInfoCount,
        bytesProcessed: this.progress.getStats().totalBytes, // 100% complete
      })

      // Stop progress display
      this.progress.stop()

      // Display final summary
      console.log(chalk.green(`✓ Successfully extracted VAT statuses`))
      console.log(
        chalk.gray(`  Output file: ${this.config.outputFilePath}\n`)
      )

      return this.progress.getStats()
    } catch (error) {
      this.progress.stop()
      const err = error instanceof Error ? error : new Error(String(error))
      console.error(chalk.red(`\n❌ Extraction failed: ${err.message}`))
      if (this.config.verbose) {
        console.error(err.stack)
      }
      throw err
    }
  }
}
