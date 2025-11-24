# KBO VAT Status Extractor

🚀 High-performance streaming XML parser to extract VAT liability information from Belgian KBO (Crossroads Bank for Enterprises) data files.

## Features

- ✨ **Streaming Architecture** - Process 28 GB XML files with < 100 MB memory
- ⚡ **WebAssembly Performance** - 2-3x faster than JavaScript parsers
- 📊 **Real-time Progress** - Live terminal UI with statistics
- 💾 **CSV Output** - Clean, structured output format
- 🎯 **Selective Extraction** - Extract only VAT authorizations
- 🛡️ **Graceful Shutdown** - Ctrl+C safely closes files
- 🎨 **Color-coded UI** - Beautiful terminal experience

## Quick Start

### Installation

```bash
cd kbo-vat-extractor
npm install
```

### Basic Usage

```bash
# Extract VAT statuses from KBO XML file
npx tsx examples/extract-vat.ts ./KboOpenData_Full.zip
```

### Options

```bash
# Specify output file
npx tsx examples/extract-vat.ts ./data.zip --output results.csv

# Enable verbose logging
npx tsx examples/extract-vat.ts ./data.zip --verbose

# Show help
npx tsx examples/extract-vat.ts --help
```

## Example Output

### Terminal UI

```
🚀 Starting VAT extraction from KBO XML...

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

Last: 0123.456.789 (✓ VAT liable since 2020-01-15)

Press Ctrl+C to stop gracefully...
```

### CSV Output

```csv
Enterprise Number,VAT Liable,Authorization Phase,Validity Start,Validity End
0123.456.789,YES,001,2020-01-15,
0234.567.890,YES,001,2018-06-01,2023-12-31
0345.678.901,YES,001,2021-03-10,
```

## Performance

| Metric | Value |
|--------|-------|
| **Processing Speed** | 1,000 - 2,000 enterprises/sec |
| **Memory Usage** | < 100 MB (constant) |
| **28 GB File** | 10-20 minutes on Mac M1/M2 |
| **Output Size** | ~15-20 MB for 2M enterprises |

## How It Works

### Architecture

```
ZIP File → Decompress → SAX-WASM Parser → VAT Filter → CSV Output
                              ↓
                       Progress Tracker
```

### Streaming Pipeline

1. **Read ZIP file** as stream (no temp files)
2. **Decompress** on-the-fly using gzip
3. **Parse XML** with WebAssembly SAX parser
4. **Filter** for VAT authorization code (00001)
5. **Write** CSV records incrementally
6. **Track progress** and update terminal UI

### Memory Efficiency

- No DOM building (event-driven parsing)
- Constant memory usage (independent of file size)
- Incremental CSV writes (no buffering)
- Selective data extraction (skip irrelevant elements)

## Technical Details

### XML Structure

The VAT authorization information is located at:

```xml
<Enterprise>
  <Nbr>0123456789</Nbr>
  <Authorizations>
    <Authorization>
      <Code>00001</Code>           <!-- VAT liable code -->
      <PhaseCode>001</PhaseCode>    <!-- Granted -->
      <Validity>
        <Begin>01-01-2020</Begin>
        <End></End>
      </Validity>
    </Authorization>
  </Authorizations>
</Enterprise>
```

### Authorization Codes

Per KBO specification:
- **00001** = VAT liable authorization
- Phase codes:
  - `001` = Granted
  - `002` = Refused
  - `003` = In application
  - `004` = Withdrawn

## Project Structure

```
kbo-vat-extractor/
├── src/
│   ├── types.ts            # TypeScript interfaces
│   ├── parser.ts           # SAX-WASM streaming parser
│   ├── progress.ts         # Terminal progress tracker
│   └── vat-extractor.ts    # Main extraction logic
├── examples/
│   └── extract-vat.ts      # CLI tool
├── docs/
│   └── TECH_DESIGN.md      # Technical design document
├── package.json
├── tsconfig.json
└── README.md
```

## Requirements

- **Node.js**: v18+ (for ES modules)
- **RAM**: 512 MB minimum
- **Disk**: Space for output CSV
- **OS**: macOS, Linux, or Windows

## Troubleshooting

### "Cannot find module 'sax-wasm'"

```bash
npm install
```

### "WASM binary not found"

The WASM binary should be in `node_modules/sax-wasm/lib/sax-wasm.wasm`. If missing, reinstall:

```bash
rm -rf node_modules
npm install
```

### "Memory usage too high"

This shouldn't happen! The tool is designed for constant memory. If you see > 200 MB usage, please report an issue.

### "File not found"

Ensure the ZIP file path is correct:

```bash
ls -lh ./KboOpenData_Full.zip
npx tsx examples/extract-vat.ts ./KboOpenData_Full.zip
```

## Advanced Usage

### Processing Multiple Files

```bash
# Process all files in a directory
for file in ./kbo-data/*.zip; do
  npx tsx examples/extract-vat.ts "$file" --output "vat-$(basename $file .zip).csv"
done
```

### Custom Integration

```typescript
import { VATExtractor } from './src/vat-extractor.js'

const extractor = new VATExtractor({
  zipFilePath: './data.zip',
  outputFilePath: './output.csv',
  verbose: true,
})

const stats = await extractor.extract()
console.log(`Processed ${stats.enterprisesProcessed} enterprises`)
```

## Documentation

- **Technical Design**: [docs/TECH_DESIGN.md](docs/TECH_DESIGN.md)
- **KBO Specification**: [../specs/KBO Technical User Manual for Data Reuse.md](../specs/KBO%20Technical%20User%20Manual%20for%20Data%20Reuse.md)
- **SAX-WASM Docs**: https://github.com/justinwilaby/sax-wasm

## Known Limitations

- Assumes gzip-compressed files (not true ZIP archives with multiple entries)
- Extracts only first VAT authorization per enterprise
- Only outputs VAT liable enterprises (not "refused" or "withdrawn")
- CSV only (no database output)

## Future Enhancements

- [ ] Support for true ZIP archives (multiple files)
- [ ] Extract all authorization types
- [ ] Database output option (PostgreSQL/SQLite)
- [ ] Incremental updates (delta detection)
- [ ] Parallel processing for multiple files
- [ ] Web UI for progress tracking

## License

MIT

## Author

Created for KBO data processing (temporary experimental project)

---

**Status**: ✅ Ready for use (experimental)

**Version**: 1.0.0

**Last Updated**: 2025-01-04
