# KBO VAT Status Extractor - Technical Design

## Overview

The KBO VAT Status Extractor is a high-performance streaming XML parser designed to extract VAT liability information from Belgian KBO (Crossroads Bank for Enterprises) XML data files.

### Problem Statement

- KBO full XML files can be ~28 GB uncompressed
- Need to extract only VAT authorization status from ~2M enterprises
- Cannot load entire XML into memory
- Must provide real-time progress feedback during processing

### Solution

Stream-based XML parsing using WebAssembly (sax-wasm) for maximum performance with constant memory usage.

---

## Architecture

### Streaming Pipeline

```
┌─────────────────┐
│  ZIP/GZ File    │
│   (28 GB)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Decompression   │
│   (gunzip)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SAX-WASM       │
│   Parser        │
│  (streaming)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ VAT Filter      │
│  & Extractor    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   CSV Writer    │
│  (incremental)  │
└─────────────────┘
```

### Component Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     CLI Interface                        │
│               (examples/extract-vat.ts)                  │
└───────────────────┬──────────────────────────────────────┘
                    │
         ┌──────────┴──────────┐
         │                     │
         ▼                     ▼
┌────────────────┐    ┌────────────────┐
│  VATExtractor  │    │ ProgressTracker│
│                │◄───┤  (Terminal UI) │
└────────┬───────┘    └────────────────┘
         │
         ▼
┌────────────────┐
│ KBOXMLParser   │
│  (sax-wasm)    │
└────────────────┘
```

---

## XML Structure Analysis

### Target Data Path

The VAT authorization information is nested deep in the XML structure:

```xml
<CommercialisationFileType>
  <Enterprises>
    <Enterprise>
      <Nbr>0123456789</Nbr>
      <Authorizations>
        <Authorization>
          <Code>00001</Code>              <!-- VAT authorization code -->
          <PhaseCode>001</PhaseCode>       <!-- Granted/Refused/etc -->
          <Validity>
            <Begin>01-01-2020</Begin>
            <End></End>                    <!-- Optional -->
          </Validity>
        </Authorization>
      </Authorizations>
    </Enterprise>
  </Enterprises>
</CommercialisationFileType>
```

### Key XML Paths

- Enterprise Number: `/CommercialisationFileType/Enterprises/Enterprise/Nbr`
- Authorization Code: `.../Enterprise/Authorizations/Authorization/Code`
- Phase Code: `.../Authorization/PhaseCode`
- Validity: `.../Authorization/Validity/Begin` and `.../End`

### Authorization Codes

Per KBO specification (Annex 2):
- **Code 00001** = "VAT liable" authorization
- Phase codes:
  - `001` = Granted
  - `002` = Refused
  - `003` = In application
  - `004` = Withdrawn

---

## Memory Management Strategy

### Constant Memory Architecture

**Goal**: Process 28 GB files with < 100 MB memory usage

**Approach**:
1. **No DOM building** - SAX-style event-driven parsing
2. **Selective data extraction** - Only track relevant paths
3. **Incremental output** - Write CSV records as found (no accumulation)
4. **Stream processing** - Process data as it flows, never buffer entire file

### State Management

Minimal state tracking:
```typescript
ParserState {
  path: string[]                 // Current XML path (depth ~10)
  currentEnterprise: string      // Current enterprise number being processed
  inAuthorization: boolean       // Are we in an Authorization element?
  currentAuthCode: string        // Current auth code being accumulated
  // ... ~200 bytes total
}
```

### Memory Profile

| Component | Memory Usage |
|-----------|--------------|
| SAX-WASM Parser Buffer | 256 KB (configurable) |
| Parser State | < 1 KB |
| CSV Writer Buffer | ~10 KB |
| Progress Tracker | < 5 KB |
| Node.js Overhead | ~30 MB |
| **Total** | **< 50 MB** |

---

## Performance Targets

### Throughput

- **Processing Rate**: 1,000 - 2,000 enterprises/second
- **28 GB File**: 10-20 minutes on modern Mac M1/M2
- **Memory Usage**: < 100 MB constant (independent of file size)

### Optimization Techniques

1. **WebAssembly Performance**
   - sax-wasm is 2-3x faster than pure JavaScript parsers
   - Native-level string processing

2. **Buffering Strategy**
   - 256 KB high-water mark for parser
   - Balances memory vs. throughput

3. **Selective Parsing**
   - Only process `<Enterprise>` and `<Authorization>` subtrees
   - Skip irrelevant data (addresses, contacts, etc.)

4. **Incremental I/O**
   - Write CSV records immediately (no buffering)
   - Stream decompression (no temp files)

---

## Progress Tracking

### Metrics Collected

| Metric | Update Frequency | Source |
|--------|------------------|--------|
| Bytes Processed | Every chunk | Stream position |
| Enterprises Processed | Per enterprise | Parser events |
| VAT Statuses Found | Per VAT record | Filter logic |
| Memory Usage | Every 5 seconds | process.memoryUsage() |
| Processing Rate | Calculated | enterprises/elapsed time |

### UI Update Strategy

**Throttling**:
- Progress bar: Update every 100ms (smooth animation, no flicker)
- Statistics: Update every 1 second (readable)
- Memory check: Every 5 seconds (low overhead)

**Terminal Output**:
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 45% | 12.6 GB / 28 GB

Statistics:
├─ Enterprises processed:    876,543
├─ VAT liable found:          234,567 (26.8%)
├─ VAT not liable:            12,345 (1.4%)
├─ No VAT info:               629,631 (71.8%)
├─ Processing rate:           1,234 ent/sec
├─ Elapsed:                   00:11:53
├─ Est. remaining:            00:14:22
└─ Memory:                    87 MB
```

---

## Error Handling

### Error Categories

1. **File Errors**
   - File not found → Exit with error message
   - Permission denied → Exit with error message
   - Corrupt ZIP → Exit with error message

2. **XML Parsing Errors**
   - Malformed XML → Log error, skip element, continue
   - Missing required fields → Log warning, skip record, continue
   - Invalid date format → Set field to null, continue

3. **System Errors**
   - Out of memory → Should not happen (constant memory)
   - Disk full → Catch CSV write error, exit gracefully
   - Ctrl+C → Graceful shutdown (finish current enterprise, close CSV)

### Graceful Shutdown

On SIGINT (Ctrl+C):
1. Stop accepting new data
2. Finish processing current enterprise
3. Close CSV file properly
4. Display progress summary
5. Exit cleanly

---

## Output Format

### CSV Structure

```csv
Enterprise Number,VAT Liable,Authorization Phase,Validity Start,Validity End
0123.456.789,YES,001,2020-01-15,
0234.567.890,YES,001,2018-06-01,2023-12-31
```

**Fields**:
- `Enterprise Number`: KBO enterprise number (10 digits with dots)
- `VAT Liable`: YES/NO
- `Authorization Phase`: Phase code (001=granted, etc.)
- `Validity Start`: ISO date (YYYY-MM-DD) or empty
- `Validity End`: ISO date (YYYY-MM-DD) or empty

**Size Estimate**:
- ~2M enterprises with VAT → ~15-20 MB CSV file

---

## Testing Strategy

### Unit Testing

- [ ] Parser initialization
- [ ] XML path tracking
- [ ] VAT code detection
- [ ] Date parsing (DD-MM-YYYY format)
- [ ] CSV record generation

### Integration Testing

- [ ] Small XML file (1,000 enterprises)
- [ ] VAT found/not found cases
- [ ] Multiple authorizations per enterprise
- [ ] Expired authorizations
- [ ] Missing validity dates

### Performance Testing

- [ ] Memory profiling (should stay < 100 MB)
- [ ] Processing rate measurement
- [ ] Progress accuracy (bytes vs. actual progress)
- [ ] Graceful shutdown (Ctrl+C)

### Edge Cases

- Empty VAT authorization (phase = refused)
- Enterprise with multiple VAT authorizations (take first)
- Missing enterprise number (skip)
- Malformed dates (set to null)
- Very large files (> 28 GB)

---

## Deployment

### System Requirements

- **OS**: macOS, Linux, Windows
- **Node.js**: v18+ (for native ES modules)
- **RAM**: 512 MB minimum (constant usage ~50-100 MB)
- **Disk**: Enough for output CSV (~20 MB per 2M enterprises)
- **CPU**: Any modern CPU (benefits from multi-core for decompression)

### Installation

```bash
cd kbo-vat-extractor
npm install
```

### Usage

```bash
# Extract VAT statuses
npx tsx examples/extract-vat.ts ./KboOpenData_Full.zip

# Specify output file
npx tsx examples/extract-vat.ts ./data.zip --output vat-results.csv

# Verbose mode
npx tsx examples/extract-vat.ts ./data.zip --verbose
```

---

## Future Enhancements

### Potential Improvements

1. **Parallel Processing**
   - Split XML by enterprise ranges
   - Process multiple files concurrently

2. **Incremental Updates**
   - Compare with previous CSV
   - Output only changes (delta)

3. **Database Output**
   - Stream directly to PostgreSQL/SQLite
   - Skip CSV intermediate format

4. **Additional Filters**
   - Extract other authorization types
   - Filter by validity date range
   - Export multiple data points per enterprise

5. **Web UI**
   - Browser-based progress display
   - Real-time statistics dashboard

---

## References

- **KBO XML Specification**: [specs/KBO Technical User Manual for Data Reuse.md](../../specs/KBO%20Technical%20User%20Manual%20for%20Data%20Reuse.md)
- **sax-wasm Library**: https://github.com/justinwilaby/sax-wasm
- **Authorization Codes**: KBO Annex 2 - PermissionCodes table

---

## Version History

- **v1.0.0** (2025-01-04): Initial implementation
  - Stream-based VAT extraction
  - Progress tracking with terminal UI
  - CSV output format
  - Graceful shutdown support
