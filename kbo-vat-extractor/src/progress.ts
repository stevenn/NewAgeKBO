/**
 * Terminal progress tracker with live statistics display
 */

import cliProgress from 'cli-progress'
import chalk from 'chalk'
import type { ProgressStats } from './types.js'

export class ProgressTracker {
  private progressBar: cliProgress.SingleBar
  private stats: ProgressStats
  private lastUpdateTime: number = 0
  private updateIntervalMs: number
  private statsInterval: NodeJS.Timeout | null = null

  constructor(totalBytes: number, updateIntervalMs: number = 100) {
    this.updateIntervalMs = updateIntervalMs

    // Initialize stats
    this.stats = {
      bytesProcessed: 0,
      totalBytes,
      percentComplete: 0,
      enterprisesProcessed: 0,
      vatLiableFound: 0,
      vatNotLiable: 0,
      noVatInfo: 0,
      startTime: Date.now(),
      elapsedTimeMs: 0,
      estimatedRemainingMs: 0,
      processingRatePerSec: 0,
      memoryUsageMB: 0,
      lastEnterpriseNumber: null,
      lastVatStatus: null,
    }

    // Create progress bar
    this.progressBar = new cliProgress.SingleBar({
      format:
        chalk.cyan('{bar}') +
        ' {percentage}% | {bytesProcessedGB} GB / {totalBytesGB} GB',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      hideCursor: true,
    })
  }

  /**
   * Start the progress display
   */
  start(): void {
    console.log(chalk.bold.blue('\n🚀 Starting VAT extraction from KBO XML...\n'))

    this.progressBar.start(this.stats.totalBytes, 0, {
      bytesProcessedGB: '0.0',
      totalBytesGB: (this.stats.totalBytes / 1e9).toFixed(1),
    })

    // Start periodic stats display (every second)
    this.statsInterval = setInterval(() => {
      this.displayStats()
    }, 1000)
  }

  /**
   * Update progress with new statistics
   */
  update(updates: Partial<ProgressStats>): void {
    // Merge updates
    Object.assign(this.stats, updates)

    // Calculate derived stats
    this.stats.elapsedTimeMs = Date.now() - this.stats.startTime
    this.stats.percentComplete = this.stats.totalBytes > 0
      ? (this.stats.bytesProcessed / this.stats.totalBytes) * 100
      : 0

    // Calculate processing rate
    if (this.stats.elapsedTimeMs > 0) {
      this.stats.processingRatePerSec =
        (this.stats.enterprisesProcessed / this.stats.elapsedTimeMs) * 1000
    }

    // Estimate remaining time
    if (this.stats.processingRatePerSec > 0 && this.stats.bytesProcessed > 0) {
      const bytesRemaining = this.stats.totalBytes - this.stats.bytesProcessed
      const bytesPerMs = this.stats.bytesProcessed / this.stats.elapsedTimeMs
      this.stats.estimatedRemainingMs = bytesPerMs > 0 ? bytesRemaining / bytesPerMs : 0
    }

    // Update memory usage
    const memUsage = process.memoryUsage()
    this.stats.memoryUsageMB = memUsage.heapUsed / 1024 / 1024

    // Throttle progress bar updates
    const now = Date.now()
    if (now - this.lastUpdateTime >= this.updateIntervalMs) {
      this.progressBar.update(this.stats.bytesProcessed, {
        bytesProcessedGB: (this.stats.bytesProcessed / 1e9).toFixed(1),
        totalBytesGB: (this.stats.totalBytes / 1e9).toFixed(1),
      })
      this.lastUpdateTime = now
    }
  }

  /**
   * Display current statistics below progress bar
   */
  private displayStats(): void {
    // Move cursor up to overwrite previous stats
    process.stdout.write('\n')

    console.log(chalk.bold('Statistics:'))
    console.log(
      `├─ Enterprises processed:    ${chalk.yellow(
        this.stats.enterprisesProcessed.toLocaleString()
      )}`
    )

    const vatPercent =
      this.stats.enterprisesProcessed > 0
        ? ((this.stats.vatLiableFound / this.stats.enterprisesProcessed) * 100).toFixed(1)
        : '0.0'
    console.log(
      `├─ VAT liable found:          ${chalk.green(
        this.stats.vatLiableFound.toLocaleString()
      )} (${vatPercent}%)`
    )

    const notLiablePercent =
      this.stats.enterprisesProcessed > 0
        ? ((this.stats.vatNotLiable / this.stats.enterprisesProcessed) * 100).toFixed(1)
        : '0.0'
    console.log(
      `├─ VAT not liable:            ${chalk.red(
        this.stats.vatNotLiable.toLocaleString()
      )} (${notLiablePercent}%)`
    )

    const noInfoPercent =
      this.stats.enterprisesProcessed > 0
        ? ((this.stats.noVatInfo / this.stats.enterprisesProcessed) * 100).toFixed(1)
        : '0.0'
    console.log(
      `├─ No VAT info:               ${chalk.gray(
        this.stats.noVatInfo.toLocaleString()
      )} (${noInfoPercent}%)`
    )

    console.log(
      `├─ Processing rate:           ${chalk.cyan(
        Math.round(this.stats.processingRatePerSec).toLocaleString()
      )} ent/sec`
    )

    console.log(`├─ Elapsed:                   ${this.formatTime(this.stats.elapsedTimeMs)}`)

    if (this.stats.estimatedRemainingMs > 0) {
      console.log(
        `├─ Est. remaining:            ${this.formatTime(
          this.stats.estimatedRemainingMs
        )}`
      )
    }

    console.log(
      `└─ Memory:                    ${chalk.magenta(
        this.stats.memoryUsageMB.toFixed(0)
      )} MB`
    )

    if (this.stats.lastEnterpriseNumber && this.stats.lastVatStatus) {
      console.log(
        `\nLast: ${chalk.bold(this.stats.lastEnterpriseNumber)} ${this.stats.lastVatStatus}`
      )
    }

    console.log(chalk.gray('\nPress Ctrl+C to stop gracefully...\n'))
  }

  /**
   * Format milliseconds to HH:MM:SS
   */
  private formatTime(ms: number): string {
    const totalSeconds = Math.floor(ms / 1000)
    const hours = Math.floor(totalSeconds / 3600)
    const minutes = Math.floor((totalSeconds % 3600) / 60)
    const seconds = totalSeconds % 60

    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(
      2,
      '0'
    )}:${String(seconds).padStart(2, '0')}`
  }

  /**
   * Get current statistics
   */
  getStats(): ProgressStats {
    return { ...this.stats }
  }

  /**
   * Stop and display final summary
   */
  stop(): void {
    if (this.statsInterval) {
      clearInterval(this.statsInterval)
      this.statsInterval = null
    }

    this.progressBar.stop()

    console.log(chalk.bold.green('\n✅ Extraction complete!\n'))
    console.log(chalk.bold('Final Statistics:'))
    console.log(
      `├─ Total enterprises:         ${chalk.yellow(
        this.stats.enterprisesProcessed.toLocaleString()
      )}`
    )

    const vatPercent =
      this.stats.enterprisesProcessed > 0
        ? ((this.stats.vatLiableFound / this.stats.enterprisesProcessed) * 100).toFixed(1)
        : '0.0'
    console.log(
      `├─ VAT liable:                ${chalk.green(
        this.stats.vatLiableFound.toLocaleString()
      )} (${vatPercent}%)`
    )

    const notLiablePercent =
      this.stats.enterprisesProcessed > 0
        ? ((this.stats.vatNotLiable / this.stats.enterprisesProcessed) * 100).toFixed(1)
        : '0.0'
    console.log(
      `├─ VAT not liable:            ${chalk.red(
        this.stats.vatNotLiable.toLocaleString()
      )} (${notLiablePercent}%)`
    )

    const noInfoPercent =
      this.stats.enterprisesProcessed > 0
        ? ((this.stats.noVatInfo / this.stats.enterprisesProcessed) * 100).toFixed(1)
        : '0.0'
    console.log(
      `├─ No VAT info:               ${chalk.gray(
        this.stats.noVatInfo.toLocaleString()
      )} (${noInfoPercent}%)`
    )

    console.log(`├─ Total time:                ${this.formatTime(this.stats.elapsedTimeMs)}`)
    console.log(
      `├─ Avg. rate:                 ${chalk.cyan(
        Math.round(this.stats.processingRatePerSec).toLocaleString()
      )} ent/sec`
    )
    console.log(
      `└─ Peak memory:               ${chalk.magenta(this.stats.memoryUsageMB.toFixed(0))} MB\n`
    )
  }
}
