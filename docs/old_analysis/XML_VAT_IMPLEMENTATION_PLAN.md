# XML VAT Enrichment Implementation Plan

**Date**: 2025-11-04
**Status**: Planning
**Related**: NOVEMBER_2025_STRATEGY.md

---

## Executive Summary

**Objective**: Add VAT liability tracking to the KBO database by importing weekly XML dumps from SFTP, using a simple join table architecture.

**Key Decisions**:
- ✅ Keep daily CSV delta imports unchanged (proven, cost-effective)
- ✅ Add weekly XML import as separate service
- ✅ Store current VAT status only (no temporal history)
- ✅ Use join table (no denormalization to enterprises)
- ✅ Support VAT filtering in search with indexes
- ✅ **Local execution** (not Vercel) - avoids function time limits and bandwidth costs

**Timeline**: 3 weeks (50-60 hours)
**Cost Impact**: +$4/year (total: $94/year)
**Risk Level**: Low (additive, non-breaking)

---

## Architecture Overview

### System Design

```
┌─────────────────────────────────────────────────┐
│ Weekly XML Full Dump (2GB, SFTP)               │
│ Downloaded and parsed LOCALLY                   │
└─────────────────┬───────────────────────────────┘
                  ↓
         Streaming XML Parser (local)
                  ↓
┌─────────────────────────────────────────────────┐
│ Local Parquet File (~50MB)                      │
│ - Built locally with DuckDB                     │
│ - ~800K VAT status records                      │
└─────────────────┬───────────────────────────────┘
                  ↓
         Bulk upload (single operation)
                  ↓
┌─────────────────────────────────────────────────┐
│ enterprise_vat_status table (MotherDuck)        │
│ - Replaced weekly (DROP + CREATE FROM Parquet)  │
│ - ~800K rows                                    │
└─────────────────┬───────────────────────────────┘
                  ↓
              JOIN on query
                  ↓
┌─────────────────────────────────────────────────┐
│ enterprises table (UNCHANGED)                   │
│ - No new columns                                │
│ - No writes from XML import                     │
└─────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Separation of Concerns**: CSV (daily) and XML (weekly) pipelines independent
2. **Local Execution**: Download and parse 2GB XML locally, not on Vercel (avoids function limits)
3. **Parquet-First**: Build Parquet locally, bulk upload to MotherDuck (no 800K network round-trips)
4. **No Denormalization**: VAT status in separate table, JOIN when needed
5. **Current Status Only**: No temporal history for VAT (simplicity)
6. **Fast Writes**: Replace entire table weekly via `DROP + CREATE FROM Parquet` (no UPDATE operations)
7. **Efficient Reads**: Indexed foreign keys for fast JOINs

### Why This Architecture?

| Decision | Rationale |
|----------|-----------|
| **Local execution** | Avoids Vercel function time limits (2GB takes 3-5 min), no bandwidth costs |
| **Parquet bulk upload** | 800K rows in 30 sec (vs hours with individual INSERTs over network) |
| **No temporal VAT history** | Simpler implementation, adequate for current needs |
| **Join table vs denormalize** | Avoids expensive UPDATEs on complex enterprises table |
| **Weekly vs daily XML** | VAT changes infrequent, 7-day lag acceptable |
| **DROP + CREATE vs UPDATE** | Faster than UPDATE in columnar database (DuckDB) |
| **Current CSV unchanged** | Proven system, don't touch what works |

---

## Database Schema

### New Table: enterprise_vat_status

```sql
-- migrations/006_add_vat_status_table.sql

CREATE TABLE IF NOT EXISTS enterprise_vat_status (
  entity_number VARCHAR PRIMARY KEY,
  vat_liable BOOLEAN NOT NULL,
  vat_number VARCHAR,
  liable_since DATE,
  phase_code VARCHAR,
  authorization_start DATE,
  authorization_end DATE,
  last_updated_extract INTEGER NOT NULL,
  last_updated_date DATE NOT NULL,

  CONSTRAINT fk_vat_enterprise
    FOREIGN KEY (entity_number)
    REFERENCES enterprises(enterprise_number)
);

-- Index for filtering VAT liable enterprises
CREATE INDEX idx_vat_status_liable
  ON enterprise_vat_status(vat_liable)
  WHERE vat_liable = true;

-- Index for JOIN performance
CREATE INDEX idx_vat_status_entity
  ON enterprise_vat_status(entity_number);

-- Index for monitoring
CREATE INDEX idx_vat_status_updated
  ON enterprise_vat_status(last_updated_extract);

COMMENT ON TABLE enterprise_vat_status IS
  'Current VAT liability status. Rebuilt weekly from XML Authorization data. No historical records.';
```

### No Changes to Existing Tables

- ✅ `enterprises` table: **No modifications**
- ✅ `establishments` table: **No modifications**
- ✅ All other tables: **No modifications**

---

## Implementation Phases

### Phase 1: Schema & Foundation (Week 1)

**Tasks**:
1. Create `enterprise_vat_status` table with indexes
2. Write migration script
3. Verify schema in MotherDuck

**Duration**: 4-8 hours

**Deliverables**:
- `migrations/006_add_vat_status_table.sql`
- `scripts/migrate-add-vat-status.ts`

**Success Criteria**:
- Table created with correct schema
- Indexes verified via `information_schema`
- Foreign key constraint works

---

### Phase 2: XML Parser (Week 1-2)

**Tasks**:
1. Implement streaming XML parser
2. Extract Authorization section (VAT code: '00001')
3. Handle large files (2GB) without memory issues
4. Unit tests for parser

**Duration**: 12-16 hours

**Files**:
- `lib/import/xml-parser.ts`
- `lib/import/xml-parser.test.ts`

**Key Requirements**:
- Stream processing (no full file in memory)
- Parse XML structure per KBO spec (see specs/KBO XML.md)
- Extract Authorization code '00001' (VAT liable)
- Extract phase code '001' (granted) vs others
- Extract validity periods (start/end dates)

**XML Structure Reference**:
```xml
<Enterprise>
  <Nbr>0123.456.789</Nbr>
  <Authorizations>
    <Authorization>
      <Code>00001</Code>  <!-- VAT liable -->
      <PhaseCode>001</PhaseCode>  <!-- Granted -->
      <Validity>
        <Begin>01.01.2020</Begin>
        <End></End>  <!-- Empty = ongoing -->
      </Validity>
    </Authorization>
  </Authorizations>
</Enterprise>
```

**Parser Interface**:
```typescript
export interface VATStatus {
  entity_number: string
  vat_liable: boolean
  phase_code: string
  authorization_start: Date | null
  authorization_end: Date | null
}

export class KBOXMLParser {
  async *parseVATStatuses(xmlPath: string): AsyncGenerator<VATStatus[]>
  async parseMetadata(xmlPath: string): Promise<XMLMetadata>
}
```

---

### Phase 3: Weekly Import Script (Week 2)

**Tasks**:
1. Build import script with streaming processing
2. Write parsed data to local Parquet file
3. Upload Parquet to MotherDuck and replace table
4. Add progress indicators
5. Dry-run mode for testing

**Duration**: 12-16 hours

**File**: `scripts/apply-weekly-xml.ts`

**Import Strategy**: Local Parquet → Bulk Upload

**Why Parquet?**
- ✅ **Fast**: Build locally, upload once (no 800K network round-trips)
- ✅ **Efficient**: Columnar format optimized for DuckDB
- ✅ **Simple**: Single `COPY FROM` or `CREATE TABLE AS`
- ✅ **Cost**: Minimal MotherDuck compute (bulk load vs incremental inserts)

**Import Logic**:
```typescript
async function applyWeeklyXML(xmlPath: string) {
  // 1. Parse metadata
  const metadata = await parser.parseMetadata(xmlPath)
  const snapshotDate = metadata.execution_date
  const extractNumber = metadata.sequence_number

  // 2. Parse XML and write to local Parquet file
  console.log('📥 Parsing XML to Parquet...')
  const localParquetPath = `/tmp/vat-status-${Date.now()}.parquet`

  // Create local DuckDB connection
  const localDb = new duckdb.Database(':memory:')
  await localDb.exec(`
    CREATE TABLE vat_status_temp AS
    SELECT * FROM read_json_auto('placeholder')
    WHERE 1=0
  `)

  // Stream parse XML and insert to local DB
  let recordCount = 0
  for await (const batch of parser.parseVATStatuses(xmlPath, 5000)) {
    const values = batch.map(v => ({
      entity_number: v.entity_number,
      vat_liable: v.vat_liable,
      vat_number: v.vat_liable ? `BE${v.entity_number}` : null,
      liable_since: v.authorization_start,
      phase_code: v.phase_code,
      authorization_start: v.authorization_start,
      authorization_end: v.authorization_end,
      last_updated_extract: extractNumber,
      last_updated_date: snapshotDate,
    }))

    // Insert to local DuckDB
    await localDb.exec(`INSERT INTO vat_status_temp SELECT * FROM json_auto(?)`, [JSON.stringify(values)])
    recordCount += batch.length

    if (recordCount % 50000 === 0) {
      console.log(`   Processed ${recordCount.toLocaleString()} records...`)
    }
  }

  // Export to Parquet
  await localDb.exec(`COPY vat_status_temp TO '${localParquetPath}' (FORMAT PARQUET)`)
  console.log(`   ✓ Wrote ${recordCount.toLocaleString()} records to Parquet`)

  // 3. Upload to MotherDuck and replace table
  console.log('📤 Uploading to MotherDuck...')
  const mdDb = await getDatabase() // MotherDuck connection

  // Drop and recreate from Parquet
  await mdDb.exec('DROP TABLE IF EXISTS enterprise_vat_status')
  await mdDb.exec(`
    CREATE TABLE enterprise_vat_status AS
    SELECT * FROM read_parquet('${localParquetPath}')
  `)

  // Recreate indexes
  await mdDb.exec(`
    CREATE INDEX idx_vat_status_liable ON enterprise_vat_status(vat_liable) WHERE vat_liable = true;
    CREATE INDEX idx_vat_status_entity ON enterprise_vat_status(entity_number);
  `)

  console.log('   ✓ Table replaced in MotherDuck')

  // 4. Verify counts
  const count = await mdDb.get('SELECT COUNT(*) FROM enterprise_vat_status')
  console.log(`   ✓ Verified ${count.count.toLocaleString()} records`)

  // 5. Clean up
  await fs.unlink(localParquetPath)
  console.log('   ✓ Cleaned up local Parquet file')
}
```

**Alternative: Direct File Upload to MotherDuck**

If MotherDuck supports file upload (check their docs), you can skip local DuckDB:

```typescript
// Write directly to Parquet using Apache Arrow
import * as arrow from 'apache-arrow'

const schema = arrow.Schema.from([
  { name: 'entity_number', type: new arrow.Utf8() },
  { name: 'vat_liable', type: new arrow.Bool() },
  // ... other fields
])

const writer = arrow.RecordBatchFileWriter.writeAll(
  schema,
  streamToArrow(parser.parseVATStatuses(xmlPath))
)

await writer.toFile(localParquetPath)

// Upload to MotherDuck storage, then load
// (Exact method depends on MotherDuck's upload API)
```

**Performance Target**:
- XML parsing: 2-3 minutes
- Parquet write: 30 seconds
- MotherDuck upload: 30 seconds
- **Total: < 5 minutes**

**CLI Usage**:
```bash
# Dry run (parse only, no DB writes)
npx tsx scripts/apply-weekly-xml.ts /path/to/KBO.xml --dry-run

# Real import
npx tsx scripts/apply-weekly-xml.ts /path/to/KBO.xml

# Custom batch size
npx tsx scripts/apply-weekly-xml.ts /path/to/KBO.xml --batch-size=2000
```

---

### Phase 4: API Integration (Week 3)

**Tasks**:
1. Update search API to support VAT filtering
2. Add VAT status to enterprise detail endpoint
3. Create VAT-specific endpoints
4. Update TypeScript types

**Duration**: 8-12 hours

**API Changes**:

#### 4.1 Search with VAT Filter

```typescript
// app/api/search/route.ts
export async function GET(request: NextRequest) {
  const query = request.nextUrl.searchParams.get('q')
  const vatOnly = request.nextUrl.searchParams.get('vat') === 'true'

  const db = await getDatabase()

  const results = await db.all(`
    SELECT
      e.enterprise_number,
      e.name,
      e.status,
      v.vat_liable,
      v.liable_since
    FROM enterprises e
    LEFT JOIN enterprise_vat_status v
      ON e.enterprise_number = v.entity_number
    WHERE e._is_current = true
      AND e.name LIKE ?
      ${vatOnly ? 'AND v.vat_liable = true' : ''}
    LIMIT 100
  `, [`%${query}%`])

  return Response.json(results)
}
```

#### 4.2 Enterprise Detail with VAT

```typescript
// app/api/enterprises/[number]/route.ts
export async function GET(request: NextRequest, { params }: { params: { number: string } }) {
  const { number } = params
  const db = await getDatabase()

  const enterprise = await db.get(`
    SELECT
      e.*,
      v.vat_liable,
      v.vat_number,
      v.liable_since,
      v.phase_code as vat_phase_code,
      v.authorization_start,
      v.authorization_end
    FROM enterprises e
    LEFT JOIN enterprise_vat_status v
      ON e.enterprise_number = v.entity_number
    WHERE e.enterprise_number = ?
      AND e._is_current = true
  `, [number])

  return Response.json(enterprise)
}
```

#### 4.3 VAT Statistics Endpoint

```typescript
// app/api/stats/vat/route.ts
export async function GET() {
  const db = await getDatabase()

  const stats = await db.get(`
    SELECT
      COUNT(*) as total_enterprises,
      SUM(CASE WHEN v.vat_liable = true THEN 1 ELSE 0 END) as vat_liable_count,
      MAX(v.last_updated_date) as last_updated
    FROM enterprises e
    LEFT JOIN enterprise_vat_status v
      ON e.enterprise_number = v.entity_number
    WHERE e._is_current = true
  `)

  return Response.json(stats)
}
```

---

### Phase 5: UI Integration (Week 3)

**Tasks**:
1. Add VAT badge to search results
2. Add VAT info to enterprise detail page
3. Add VAT filter checkbox to search
4. Show VAT statistics in dashboard

**Duration**: 8-12 hours

**UI Components**:

#### 5.1 VAT Badge Component

```typescript
// components/VATBadge.tsx
import { Badge } from '@/components/ui/badge'

export function VATBadge({ vatLiable }: { vatLiable: boolean | null }) {
  if (vatLiable === null) return null

  return (
    <Badge variant={vatLiable ? 'success' : 'secondary'} className="text-xs">
      {vatLiable ? 'VAT Liable' : 'Not VAT Liable'}
    </Badge>
  )
}
```

#### 5.2 Search with VAT Filter

```typescript
// app/search/page.tsx
export default function SearchPage() {
  const [searchQuery, setSearchQuery] = useState('')
  const [vatOnly, setVatOnly] = useState(false)

  return (
    <div className="space-y-4">
      <Input
        value={searchQuery}
        onChange={(e) => setSearchQuery(e.target.value)}
        placeholder="Search enterprises..."
      />

      <Checkbox
        checked={vatOnly}
        onCheckedChange={setVatOnly}
        label="VAT liable only"
      />

      {/* Search results with VAT badges */}
    </div>
  )
}
```

#### 5.3 Enterprise Detail VAT Section

```typescript
// app/enterprises/[number]/page.tsx
<Card>
  <CardHeader>
    <CardTitle className="flex items-center gap-2">
      VAT Status
      <VATBadge vatLiable={enterprise.vat_liable} />
    </CardTitle>
  </CardHeader>
  <CardContent>
    {enterprise.vat_liable ? (
      <dl className="space-y-2">
        <div>
          <dt className="text-sm text-gray-600">VAT Number</dt>
          <dd className="font-mono">{enterprise.vat_number}</dd>
        </div>
        <div>
          <dt className="text-sm text-gray-600">Liable Since</dt>
          <dd>{formatDate(enterprise.liable_since)}</dd>
        </div>
        <div>
          <dt className="text-sm text-gray-600">Authorization Period</dt>
          <dd>
            {formatDate(enterprise.authorization_start)} -
            {enterprise.authorization_end ? formatDate(enterprise.authorization_end) : 'ongoing'}
          </dd>
        </div>
      </dl>
    ) : (
      <p className="text-gray-600">Not VAT liable</p>
    )}
  </CardContent>
</Card>
```

---

### Phase 6: Local Execution & Monitoring (Week 4)

**Tasks**:
1. Implement SFTP download script
2. Create unified local import script
3. Add monitoring dashboard to admin UI
4. Document execution workflow

**Duration**: 8-12 hours

**Execution Model**: Local script execution (not Vercel)

**Why Local?**
- ✅ No Vercel function time limits (2GB XML takes 3-5 min to process)
- ✅ No bandwidth costs for 2GB downloads
- ✅ Better control over long-running batch jobs
- ✅ Can run from developer machine or dedicated server

**Unified Import Script**:

```typescript
// scripts/fetch-and-import-xml.ts
import Client from 'ssh2-sftp-client'
import { getDatabase } from '../lib/motherduck'
import { KBOXMLParser } from '../lib/import/xml-parser'
import fs from 'fs/promises'
import path from 'path'

interface ImportOptions {
  downloadDir?: string
  keepFile?: boolean
  dryRun?: boolean
}

async function downloadLatestXML(downloadDir: string): Promise<string> {
  console.log('🔽 Downloading XML from SFTP...')

  const sftp = new Client()

  await sftp.connect({
    host: process.env.KBO_SFTP_HOST!,
    port: 22,
    username: process.env.KBO_SFTP_USERNAME!,
    password: process.env.KBO_SFTP_PASSWORD!,
  })

  // List XML files
  const files = await sftp.list('/xml/')

  // Find latest full dump
  const latestXML = files
    .filter(f => f.name.includes('_Full.xml'))
    .sort((a, b) => b.modifyTime - a.modifyTime)[0]

  if (!latestXML) throw new Error('No XML full dump found on SFTP')

  console.log(`   Found: ${latestXML.name} (${(latestXML.size / 1024 / 1024).toFixed(0)}MB)`)

  // Download to specified directory
  const localPath = path.join(downloadDir, latestXML.name)
  await sftp.get(`/xml/${latestXML.name}`, localPath)
  await sftp.end()

  console.log(`   ✓ Downloaded to ${localPath}`)
  return localPath
}

async function fetchAndImportXML(options: ImportOptions = {}) {
  const {
    downloadDir = '/tmp/kbo-xml',
    keepFile = false,
    dryRun = false
  } = options

  const startTime = Date.now()

  try {
    // Ensure download directory exists
    await fs.mkdir(downloadDir, { recursive: true })

    // Download from SFTP
    const xmlPath = await downloadLatestXML(downloadDir)

    // Import
    console.log('\n📥 Importing XML data...')
    await applyWeeklyXML(xmlPath, dryRun)

    // Clean up
    if (!keepFile) {
      console.log('\n🧹 Cleaning up...')
      await fs.unlink(xmlPath)
      console.log('   ✓ Temporary file deleted')
    }

    const duration = Math.round((Date.now() - startTime) / 1000)
    console.log(`\n✅ Complete! Duration: ${duration}s`)

    // Log success for monitoring
    await logImportResult({
      status: 'success',
      duration,
      timestamp: new Date(),
    })

  } catch (error) {
    console.error('\n❌ Import failed:', error)

    // Log failure for monitoring
    await logImportResult({
      status: 'failed',
      error: error instanceof Error ? error.message : 'Unknown error',
      timestamp: new Date(),
    })

    throw error
  }
}

async function logImportResult(result: any) {
  // Write to log file for monitoring dashboard
  const logPath = path.join(process.cwd(), 'logs', 'xml-imports.jsonl')
  await fs.mkdir(path.dirname(logPath), { recursive: true })
  await fs.appendFile(logPath, JSON.stringify(result) + '\n')
}

// CLI
const args = process.argv.slice(2)
const dryRun = args.includes('--dry-run')
const keepFile = args.includes('--keep-file')
const downloadDir = args.find(a => a.startsWith('--dir='))?.split('=')[1] || '/tmp/kbo-xml'

console.log('🚀 KBO XML Import (Local Execution)')
console.log('=' .repeat(50))

fetchAndImportXML({ downloadDir, keepFile, dryRun })
```

**Usage**:

```bash
# Basic usage (download + import + cleanup)
npx tsx scripts/fetch-and-import-xml.ts

# Dry run (no database writes)
npx tsx scripts/fetch-and-import-xml.ts --dry-run

# Keep downloaded file for inspection
npx tsx scripts/fetch-and-import-xml.ts --keep-file

# Custom download directory
npx tsx scripts/fetch-and-import-xml.ts --dir=/data/kbo-xml
```

**Local Cron Setup** (Optional):

If you want to automate weekly on a local server:

```bash
# Edit crontab
crontab -e

# Add weekly execution (Sundays at 10 AM)
0 10 * * 0 cd /path/to/NewAgeKBO && npx tsx scripts/fetch-and-import-xml.ts >> logs/cron-xml.log 2>&1
```

**Or use systemd timer** (Linux):

```ini
# /etc/systemd/system/kbo-xml-import.timer
[Unit]
Description=Weekly KBO XML Import

[Timer]
OnCalendar=Sun *-*-* 10:00:00
Persistent=true

[Install]
WantedBy=timers.target
```

```ini
# /etc/systemd/system/kbo-xml-import.service
[Unit]
Description=KBO XML Import Service

[Service]
Type=oneshot
User=youruser
WorkingDirectory=/path/to/NewAgeKBO
ExecStart=/usr/bin/npx tsx scripts/fetch-and-import-xml.ts
StandardOutput=append:/var/log/kbo-xml-import.log
StandardError=append:/var/log/kbo-xml-import.log
```

**Admin UI Monitoring** (no trigger, just status):

```typescript
// app/admin/xml-import/page.tsx
'use client'

import { useQuery } from '@tanstack/react-query'
import { Card } from '@/components/ui/card'
import { Badge } from '@/components/ui/badge'
import { Alert, AlertDescription } from '@/components/ui/alert'

export default function XMLImportPage() {
  // Read import logs from API
  const { data: importStatus } = useQuery({
    queryKey: ['xml-import-status'],
    queryFn: async () => {
      const res = await fetch('/api/admin/xml-import-status')
      return res.json()
    },
    refetchInterval: 30000, // Refresh every 30s
  })

  return (
    <div className="container mx-auto p-6">
      <h1 className="text-3xl font-bold mb-6">XML Import Status</h1>

      <Alert className="mb-6">
        <AlertDescription>
          XML imports run locally via <code>scripts/fetch-and-import-xml.ts</code>.
          This dashboard shows the status of recent imports.
        </AlertDescription>
      </Alert>

      <Card className="p-6 mb-6">
        <div className="space-y-4">
          <div className="flex justify-between items-center">
            <h2 className="text-xl font-semibold">Last Import</h2>
            <Badge variant={importStatus?.last_status === 'success' ? 'success' : 'destructive'}>
              {importStatus?.last_status || 'unknown'}
            </Badge>
          </div>

          <dl className="grid grid-cols-2 gap-4">
            <div>
              <dt className="text-sm text-gray-600">Timestamp</dt>
              <dd className="font-mono">
                {importStatus?.last_timestamp
                  ? new Date(importStatus.last_timestamp).toLocaleString()
                  : 'Never'}
              </dd>
            </div>

            <div>
              <dt className="text-sm text-gray-600">Duration</dt>
              <dd>{importStatus?.last_duration || '-'}s</dd>
            </div>

            <div>
              <dt className="text-sm text-gray-600">Records Imported</dt>
              <dd>{importStatus?.last_record_count?.toLocaleString() || '-'}</dd>
            </div>

            <div>
              <dt className="text-sm text-gray-600">VAT Liable Count</dt>
              <dd>{importStatus?.vat_liable_count?.toLocaleString() || '-'}</dd>
            </div>
          </dl>
        </div>
      </Card>

      <Card className="p-6">
        <h2 className="text-xl font-semibold mb-4">Recent Import History</h2>
        <div className="space-y-2">
          {importStatus?.history?.map((imp: any, idx: number) => (
            <div key={idx} className="flex justify-between items-center border-b pb-2">
              <div>
                <p className="font-medium">{new Date(imp.timestamp).toLocaleString()}</p>
                {imp.error && <p className="text-sm text-red-600">{imp.error}</p>}
              </div>
              <div className="flex items-center gap-2">
                <span className="text-sm text-gray-600">{imp.duration}s</span>
                <Badge variant={imp.status === 'success' ? 'success' : 'destructive'}>
                  {imp.status}
                </Badge>
              </div>
            </div>
          ))}
        </div>
      </Card>

      <div className="mt-6">
        <h3 className="font-semibold mb-2">Manual Execution</h3>
        <pre className="bg-gray-100 p-4 rounded text-sm">
          {`cd /path/to/NewAgeKBO
npx tsx scripts/fetch-and-import-xml.ts`}
        </pre>
      </div>
    </div>
  )
}
```

---

## Testing Strategy

### Unit Tests

```bash
# XML parser tests
npm run test lib/import/xml-parser.test.ts

# Test with sample XML
npx tsx scripts/apply-weekly-xml.ts test-data/sample-xml.xml --dry-run
```

### Integration Tests

```typescript
// Test full import pipeline
describe('Weekly XML Import', () => {
  test('should import VAT statuses', async () => {
    await applyWeeklyXML('test-data/full.xml')

    const count = await db.get('SELECT COUNT(*) FROM enterprise_vat_status')
    expect(count.count).toBeGreaterThan(0)
  })

  test('should handle missing VAT data', async () => {
    // Enterprise without VAT authorization
    const enterprise = await db.get(`
      SELECT e.*, v.vat_liable
      FROM enterprises e
      LEFT JOIN enterprise_vat_status v ON e.enterprise_number = v.entity_number
      WHERE e.enterprise_number = '0123456789'
    `)

    expect(enterprise.vat_liable).toBe(null)
  })
})
```

### Performance Tests

```bash
# Time the import
time npx tsx scripts/apply-weekly-xml.ts /path/to/production.xml

# Expected: < 5 minutes for 2GB file
```

### Data Validation

```sql
-- Verify counts
SELECT
  COUNT(*) as total_vat_records,
  SUM(CASE WHEN vat_liable = true THEN 1 ELSE 0 END) as liable_count,
  COUNT(DISTINCT entity_number) as unique_enterprises
FROM enterprise_vat_status;

-- Check for orphaned records
SELECT COUNT(*)
FROM enterprise_vat_status v
WHERE NOT EXISTS (
  SELECT 1 FROM enterprises e
  WHERE e.enterprise_number = v.entity_number
);

-- Verify foreign key integrity
SELECT
  v.entity_number,
  v.vat_liable,
  e.name
FROM enterprise_vat_status v
LEFT JOIN enterprises e ON v.entity_number = e.enterprise_number
WHERE e.enterprise_number IS NULL;
```

---

## Rollback Plan

### If Import Fails

```bash
# Status table can be cleared and reimported
DELETE FROM enterprise_vat_status;

# Re-run import with previous XML dump
npx tsx scripts/apply-weekly-xml.ts /path/to/previous.xml
```

### If Data Is Wrong

```sql
-- Clear VAT status table
TRUNCATE TABLE enterprise_vat_status;

-- Re-import from known good XML
-- Run import script again
```

### No Risk to Existing Data

- ✅ CSV daily imports continue unaffected
- ✅ Enterprises table unchanged
- ✅ Only new table affected
- ✅ Can drop table and start over if needed

---

## Monitoring & Alerts

### Key Metrics

1. **Import Success Rate**: Weekly import success/failure
2. **Import Duration**: Alert if > 10 minutes
3. **VAT Count**: Track VAT liable enterprise count
4. **Record Count**: Monitor total status records

### Alert Conditions

```typescript
// Alert if import fails
if (importStatus === 'failed') {
  sendAlert('Weekly XML import failed')
}

// Alert if duration exceeds threshold
if (importDuration > 600) { // 10 minutes
  sendAlert('XML import taking too long')
}

// Alert if VAT count drops significantly
if (Math.abs(newCount - previousCount) / previousCount > 0.1) {
  sendAlert('VAT count anomaly detected')
}
```

### Monitoring Dashboard

```typescript
// app/admin/monitoring/page.tsx
<Card>
  <CardHeader>
    <CardTitle>XML Import Status</CardTitle>
  </CardHeader>
  <CardContent>
    <dl>
      <dt>Last Import</dt>
      <dd>{formatDate(lastImport)}</dd>

      <dt>Status</dt>
      <dd><Badge>{importStatus}</Badge></dd>

      <dt>VAT Liable Enterprises</dt>
      <dd>{vatLiableCount.toLocaleString()}</dd>

      <dt>Total Records</dt>
      <dd>{totalRecords.toLocaleString()}</dd>
    </dl>
  </CardContent>
</Card>
```

---

## Environment Variables

```bash
# .env.local

# XML/SFTP Configuration
KBO_SFTP_HOST=ftps.economie.fgov.be
KBO_SFTP_USERNAME=your_username
KBO_SFTP_PASSWORD=your_password

# Alerting (optional)
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
ALERT_EMAIL=admin@yourdomain.com
```

**Note**: These credentials are used by the local import script, not by Vercel.

---

## Cost Analysis

### Local Execution = Lower Costs

Running XML import locally eliminates Vercel compute/bandwidth costs.

### Storage (MotherDuck)

```
enterprise_vat_status: 800K rows × 40 bytes = 32MB
Compressed: ~10MB
MotherDuck cost: $0.50/GB/month × 0.01GB × 12 = $0.06/year
```

### Compute (MotherDuck)

```
Weekly imports: 52 × 5 min = 260 min/year
MotherDuck query execution: ~$4/year
```

### Bandwidth

```
Weekly SFTP downloads: 52 × 2GB = 104GB/year
SFTP bandwidth: Free (KBO provides SFTP access)
Local to MotherDuck: Minimal (only INSERT queries, not data transfer)
```

### Total Annual Cost

```
Storage (MotherDuck):       $0.06
Compute (MotherDuck):       $4.00
Bandwidth (SFTP):           $0.00
Local execution:            $0.00
--------------------------------
Total:                      $4.06/year

Combined with existing CSV: $90 + $4 = $94/year
```

**Savings vs Vercel execution**: ~$6/year (no Vercel function/bandwidth costs)

---

## Success Criteria

### Phase 1 Complete When:
- ✅ Schema migration runs without errors
- ✅ Table and indexes created
- ✅ Foreign key constraint verified

### Phase 2 Complete When:
- ✅ Parser handles 2GB XML files
- ✅ Unit tests pass
- ✅ Can extract VAT authorizations

### Phase 3 Complete When:
- ✅ Import completes in < 5 minutes
- ✅ Table successfully replaced
- ✅ Verification queries pass

### Phase 4 Complete When:
- ✅ Search API supports VAT filtering
- ✅ Enterprise detail shows VAT status
- ✅ Statistics endpoint works

### Phase 5 Complete When:
- ✅ VAT badge appears in search results
- ✅ VAT filter checkbox works
- ✅ Enterprise detail shows VAT info

### Phase 6 Complete When:
- ✅ Unified local script works (download + import)
- ✅ SFTP connection and file listing works
- ✅ Logs written for monitoring dashboard
- ✅ Monitoring dashboard displays import status
- ✅ Documentation complete (including cron setup)

---

## Timeline

```
Week 1: Foundation
├── Schema migration (1 day)
├── XML parser core (2 days)
└── Parser tests (1 day)

Week 2: Import Pipeline
├── Import script (2 days)
├── Performance testing (1 day)
└── Integration tests (1 day)

Week 3: API & UI
├── API updates (2 days)
└── UI components (2 days)

Week 4: Local Execution
├── SFTP integration (1 day)
├── Unified import script (1 day)
└── Monitoring dashboard (1 day)
```

**Total: 3 weeks (50-60 hours)**

**Note**: Week 4 is shorter since we removed Vercel cron integration.

---

## Next Steps

1. ✅ Complete November 2025 full dump validation (NOVEMBER_2025_STRATEGY.md)
2. ⏸️ Return to this plan for VAT implementation
3. 🟢 Execute Phase 1 (schema migration)
4. 🟢 Execute Phase 2-6 sequentially

---

## Future Enhancements

### After Initial Implementation

1. **Other Authorization Types**: Expand beyond VAT (employer status, permits)
2. **Temporal History**: Add if audit requirements emerge
3. **Change Notifications**: Alert when enterprise VAT status changes
4. **Bulk Export**: Export VAT lists for external use
5. **Analytics**: VAT trends, statistics by sector

### Potential Optimizations

1. **Incremental XML**: If KBO provides delta XML files (not available yet)
2. **Caching**: Redis cache for frequently accessed VAT statuses
3. **Materialized Views**: Pre-computed VAT statistics

---

**Ready to execute after November dump is processed.**
