/**
 * SAX-WASM based streaming XML parser for KBO data
 */

import { SAXParser, SaxEventType, Tag, Text } from 'sax-wasm'
import { readFile } from 'fs/promises'
import { resolve, dirname } from 'path'
import { fileURLToPath } from 'url'
import { Writable } from 'stream'
import type { ParserState, EnterpriseVATStatus } from './types.js'
import { VAT_AUTHORIZATION_CODE } from './types.js'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

export interface ParserEvents {
  onEnterprise?: (enterpriseNumber: string) => void
  onVATStatus?: (status: EnterpriseVATStatus) => void
  onBytesProcessed?: (bytes: number) => void
  onError?: (error: Error) => void
}

export class KBOXMLParser {
  private parser: SAXParser
  private state: ParserState
  private events: ParserEvents
  private bytesProcessed: number = 0
  private writableStream: Writable

  constructor(events: ParserEvents = {}) {
    this.events = events

    // Initialize parser state
    this.state = {
      path: [],
      currentEnterprise: null,
      inAuthorization: false,
      currentAuthCode: null,
      currentAuthPhase: null,
      currentValidityStart: null,
      currentValidityEnd: null,
      textContent: '',
    }

    // Create SAX parser (will be initialized with WASM)
    this.parser = new SAXParser(
      SaxEventType.OpenTag | SaxEventType.CloseTag | SaxEventType.Text,
      {
        highWaterMark: 256 * 1024, // 256KB buffer for performance
      }
    )

    // Create writable stream that feeds the parser
    this.writableStream = new Writable({
      write: (chunk: Buffer, encoding: string, callback: (error?: Error | null) => void) => {
        try {
          this.write(chunk)
          callback()
        } catch (error) {
          callback(error instanceof Error ? error : new Error(String(error)))
        }
      },
      final: (callback: (error?: Error | null) => void) => {
        try {
          this.end()
          callback()
        } catch (error) {
          callback(error instanceof Error ? error : new Error(String(error)))
        }
      }
    })
  }

  /**
   * Initialize the parser with WASM binary
   */
  async initialize(): Promise<void> {
    try {
      // Load WASM binary from node_modules
      const wasmPath = resolve(
        __dirname,
        '../node_modules/sax-wasm/lib/sax-wasm.wasm'
      )
      const saxWasmBuffer = await readFile(wasmPath)

      // Prepare WASM
      const ready = await this.parser.prepareWasm(saxWasmBuffer)
      if (!ready) {
        throw new Error('Failed to initialize SAX-WASM')
      }

      // Set up event handler
      this.parser.eventHandler = (event: SaxEventType, data: Tag | Text) => {
        this.handleEvent(event, data)
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      this.events.onError?.(err)
      throw err
    }
  }

  /**
   * Handle SAX events
   */
  private handleEvent(event: SaxEventType, data: Tag | Text): void {
    try {
      if (event === SaxEventType.OpenTag) {
        this.handleOpenTag(data as Tag)
      } else if (event === SaxEventType.CloseTag) {
        this.handleCloseTag(data as Tag)
      } else if (event === SaxEventType.Text) {
        this.handleText(data as Text)
      }
    } catch (error) {
      const err = error instanceof Error ? error : new Error(String(error))
      this.events.onError?.(err)
    }
  }

  /**
   * Handle opening tag
   */
  private handleOpenTag(tag: Tag): void {
    const tagName = tag.name
    this.state.path.push(tagName)
    this.state.textContent = '' // Reset text accumulator

    // Track when we enter an Enterprise element
    if (tagName === 'Enterprise') {
      this.state.currentEnterprise = null
    }

    // Track when we enter an Authorization element
    if (tagName === 'Authorization') {
      this.state.inAuthorization = true
      this.state.currentAuthCode = null
      this.state.currentAuthPhase = null
      this.state.currentValidityStart = null
      this.state.currentValidityEnd = null
    }
  }

  /**
   * Handle closing tag
   */
  private handleCloseTag(tag: Tag): void {
    const tagName = tag.name
    const currentPath = this.state.path.join('/')
    const text = this.state.textContent.trim()

    // Extract enterprise number
    if (tagName === 'Nbr' && currentPath.includes('Enterprise/Nbr')) {
      this.state.currentEnterprise = text
      this.events.onEnterprise?.(text)
    }

    // Extract authorization code
    if (
      tagName === 'Code' &&
      this.state.inAuthorization &&
      currentPath.includes('Authorization/Code')
    ) {
      this.state.currentAuthCode = text
    }

    // Extract authorization phase
    if (
      tagName === 'PhaseCode' &&
      this.state.inAuthorization &&
      currentPath.includes('Authorization/PhaseCode')
    ) {
      this.state.currentAuthPhase = text
    }

    // Extract validity start date
    if (
      tagName === 'Begin' &&
      this.state.inAuthorization &&
      currentPath.includes('Authorization/Validity/Begin')
    ) {
      this.state.currentValidityStart = text
    }

    // Extract validity end date
    if (
      tagName === 'End' &&
      this.state.inAuthorization &&
      currentPath.includes('Authorization/Validity/End')
    ) {
      this.state.currentValidityEnd = text
    }

    // When Authorization closes, check if it's VAT-related
    if (tagName === 'Authorization') {
      this.processAuthorization()
      this.state.inAuthorization = false
    }

    // Pop from path
    if (this.state.path.length > 0 && this.state.path[this.state.path.length - 1] === tagName) {
      this.state.path.pop()
    }

    // Reset text accumulator
    this.state.textContent = ''
  }

  /**
   * Handle text content
   */
  private handleText(text: Text): void {
    this.state.textContent += text.value
  }

  /**
   * Process completed authorization data
   */
  private processAuthorization(): void {
    // Check if this is a VAT authorization
    if (this.state.currentAuthCode === VAT_AUTHORIZATION_CODE && this.state.currentEnterprise) {
      const validityStart = this.parseDate(this.state.currentValidityStart)
      const validityEnd = this.parseDate(this.state.currentValidityEnd)

      // Determine if enterprise is VAT liable:
      // - Must have phase code 001 (Granted)
      // - Must be currently active (no end date, or end date in the future)
      const isGranted = this.state.currentAuthPhase === '001'
      const isActive = !validityEnd || validityEnd > new Date()
      const vatLiable = isGranted && isActive

      const status: EnterpriseVATStatus = {
        enterpriseNumber: this.state.currentEnterprise,
        vatLiable,
        authorizationPhase: this.state.currentAuthPhase,
        validityStart,
        validityEnd,
      }

      this.events.onVATStatus?.(status)
    }
  }

  /**
   * Parse KBO date format (DD-MM-YYYY) to Date object
   */
  private parseDate(dateStr: string | null): Date | null {
    if (!dateStr) return null

    try {
      const parts = dateStr.split('-')
      if (parts.length !== 3) return null

      const day = parseInt(parts[0], 10)
      const month = parseInt(parts[1], 10) - 1 // JavaScript months are 0-indexed
      const year = parseInt(parts[2], 10)

      return new Date(year, month, day)
    } catch {
      return null
    }
  }

  /**
   * Write chunk of XML data to parser
   */
  write(chunk: Buffer): void {
    this.bytesProcessed += chunk.length
    this.parser.write(chunk)
    this.events.onBytesProcessed?.(this.bytesProcessed)
  }

  /**
   * Get writable stream for piping
   */
  get writable() {
    return this.writableStream
  }

  /**
   * Get total bytes processed
   */
  getBytesProcessed(): number {
    return this.bytesProcessed
  }

  /**
   * End parsing
   */
  end(): void {
    this.parser.end()
  }
}
