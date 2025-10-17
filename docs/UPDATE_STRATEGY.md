# KBO Update Strategy

## Executive Summary

This document outlines the strategy for processing KBO Open Data updates, including both daily incremental updates and monthly full snapshots. The strategy is based on analysis of the actual data files and optimized for Motherduck deployment.

## Key Findings

### Extract 0140 Analysis

**Critical Discovery**: Extract 0140 Update (33 MB, 7.3M changes) is **REDUNDANT** with the full dump.

**Evidence**:
- Full dump already contains NACE 2025 migration data:
  - NACE 2025: 47.73% (17.3M activities)
  - NACE 2008: 45.85% (16.6M activities)
  - NACE 2003: 6.42% (2.3M activities)
- Both Extract 0140 Full and Update share same SnapshotDate (04-10-2025)
- Published 38 seconds apart (07:42:55 vs 07:43:33)
- The 7.1M activity changes in the update represent the NACE 2008→2025 migration
- The full dump was prepared AFTER the migration, so it already includes the updated activities

**Verdict**: ✅ SKIP Extract 0140 Update and start daily updates from Extract 0141

### Daily Update Volumes

| Extract | Snapshot Date | Total Changes | Notes |
|---------|---------------|---------------|-------|
| 0140 Update | 04-10-2025 | 7,279,279 | NACE migration - SKIP |
| 0141 Update | 05-10-2025 | 1,103 | Normal daily volume |
| 0142 Update | 06-10-2025 | 1,145 | Normal daily volume |
| 0143 Update | 07-10-2025 | 1,087 | Normal daily volume |
| 0147 Update | 11-10-2025 | 1,103 | Normal daily volume |

**Typical daily update**: ~1,100 changes across all tables

## Update Processing Strategy

### 1. Daily Updates (Incremental) - AUTOMATED

**File Format**: ZIP files (~400 KB - 2 MB)

**Approach**: Delete-then-insert pattern with history preservation

**Implementation**:
```typescript
// scripts/apply-daily-update.ts
async function processDailyUpdate(zipPath: string) {
  // 1. Read metadata from ZIP
  const metadata = await parseMetadata(zip)

  // 2. For each table:
  //    a. Process deletes: Mark as _is_current = false (preserve history)
  await applyDeletes(db, zip, tableName, metadata)

  //    b. Process inserts: Add with _is_current = true
  await applyInserts(db, zip, tableName, metadata)
}
```

**Key Principles**:
- ✅ Process ZIP files directly (no extraction)
- ✅ DELETE operations mark records as historical (don't actually delete)
- ✅ INSERT operations add new records with current snapshot date
- ✅ Update `_extract_number` and `_snapshot_date` from meta.csv

**Example**:
```bash
npx tsx scripts/apply-daily-update.ts sampledata/KboOpenData_0141_2025_10_05_Update.zip
```

**Automation Path**:
- CLI first (for testing)
- Then webapp API endpoint
- Then Vercel cron job for automatic daily processing

### 2. Monthly Snapshots (Full Dump) - MANUAL CLI ONLY

**File Format**: Directory with CSV files (2+ GB uncompressed)
**⚠️ NEVER treat as ZIP** - too large for ZIP processing

**Approach**: Manual CLI execution with direct ETL to Motherduck

**Implementation**:
```typescript
// CLI-only script (similar to initial import)
async function processMonthlySnapshot(dataDir: string) {
  // 1. Mark all current records as historical
  await markCurrentAsHistorical(db)

  // 2. Import CSV files directly from directory
  await importFullSnapshot(db, dataDir, metadata)

  // 3. Clean up snapshots older than 24 months
  await cleanupOldSnapshots(db, 24)
}
```

**Workflow**:
1. Download monthly full dump from KBO portal
2. Extract ZIP to directory (done externally, not in code)
3. Run CLI script pointing to extracted directory
4. Script performs direct ETL to Motherduck
5. No webapp involvement, no cron automation

**Storage Calculation**:
- Per snapshot: ~100 MB (Parquet + ZSTD compression)
- 24 snapshots: ~2.4 GB total
- Motherduck cost: ~$0.05/month (acceptable)

**Retention Policy**:
- Keep last 24 months of snapshots
- Automatic cleanup on each monthly import
- Preserves point-in-time analysis capability

**Example**:
```bash
# Extract ZIP externally first
unzip KboOpenData_0145_2025_11_03_Full.zip -d /path/to/extracted

# Then run CLI
npx tsx scripts/apply-monthly-snapshot.ts /path/to/extracted
```

## Time-Based Querying

### Current Data (Default)

```sql
SELECT *
FROM enterprises
WHERE _is_current = true
```

### Point-in-Time Query

```sql
-- Get data as of October 10, 2025
SELECT *
FROM enterprises
WHERE _snapshot_date <= '2025-10-10'
  AND (
    _is_current = true
    OR _snapshot_date = (
      SELECT MAX(_snapshot_date)
      FROM enterprises AS e2
      WHERE e2.id = enterprises.id
        AND e2._snapshot_date <= '2025-10-10'
    )
  )
```

### Historical Comparison

```sql
-- Compare current vs 6 months ago
WITH current AS (
  SELECT * FROM enterprises WHERE _is_current = true
),
historical AS (
  SELECT * FROM enterprises
  WHERE _snapshot_date = CURRENT_DATE - INTERVAL '6 months'
)
SELECT
  current.enterprise_number,
  current.status AS current_status,
  historical.status AS status_6mo_ago
FROM current
LEFT JOIN historical USING (enterprise_number)
WHERE current.status != historical.status
```

## Implementation Roadmap

### Phase 2: Daily Update Pipeline ✅ READY

**CLI Testing** (Do first):
1. Test with Extract 0141 (first normal daily update)
   ```bash
   npx tsx scripts/apply-daily-update.ts sampledata/KboOpenData_0141_2025_10_05_Update.zip
   ```
2. Verify records marked historical and new records inserted
3. Test with subsequent extracts (0142, 0143, etc.)
4. Validate data integrity and temporal tracking

**Vercel Webapp** (Do after CLI works):
1. Create API endpoint: `POST /api/kbo/update`
2. Upload ZIP to temporary storage
3. Trigger `apply-daily-update.ts` via serverless function
4. Display results in admin UI
5. Set up cron job for automatic daily updates

### Phase 3: Monthly Snapshot Pipeline - CLI ONLY

**Manual Process**:
1. Download monthly full dump from KBO portal (first Sunday of month)
2. Extract ZIP externally to directory
3. Run CLI script:
   ```bash
   npx tsx scripts/apply-monthly-snapshot.ts /path/to/extracted-csv-dir
   ```
4. Verify history preservation and cleanup
5. Monitor completion

**NO webapp automation** - always manual CLI execution

### Phase 4: Daily Update Automation

**Automatic Fetch** (requires KBO portal access):
1. Fetch daily updates from https://kbopub.economie.fgov.be/kbo-open-data/affiliation/xml/?files
2. Authenticate with username + password
3. Download new ZIP files
4. Trigger update pipeline

**Vercel Cron Jobs** (daily updates ONLY):
```typescript
// vercel.json
{
  "crons": [
    {
      "path": "/api/kbo/fetch-daily",
      "schedule": "0 9 * * *"  // Daily at 9 AM
    }
  ]
}
```

**Monthly snapshots**: Always handled manually via CLI

## File Handling

### ZIP Processing (Daily Updates Only)

**Library**: `node-stream-zip`

**Benefits**:
- No disk I/O for extraction
- Memory-efficient streaming
- Fast processing
- Works well with Vercel serverless

**Example**:
```typescript
import StreamZip from 'node-stream-zip'
import { parse } from 'csv-parse/sync'

const zip = new StreamZip.async({ file: zipPath })

// Read CSV directly from ZIP
const content = await zip.entryData('activity.csv')
const records = parse(content.toString(), {
  columns: true,
  skip_empty_lines: true
})

await zip.close()
```

### Directory Processing (Monthly Snapshots Only)

**Approach**: Read CSV files directly from extracted directory

**Example**:
```typescript
import * as fs from 'fs'
import * as path from 'path'
import { parse } from 'csv-parse/sync'

// Read CSV from directory
const csvPath = path.join(dataDir, 'activity.csv')
const content = fs.readFileSync(csvPath, 'utf-8')
const records = parse(content, {
  columns: true,
  skip_empty_lines: true
})
```

## Testing Sequence

### Test 1: Daily Update (Extract 0141) ⏳ NEXT

**Goal**: Verify daily update processing works correctly

**Steps**:
1. Check current record count:
   ```sql
   SELECT COUNT(*) FROM activities WHERE _is_current = true;
   ```
2. Run update:
   ```bash
   npx tsx scripts/apply-daily-update.ts sampledata/KboOpenData_0141_2025_10_05_Update.zip
   ```
3. Verify changes:
   ```sql
   -- Check new extract number
   SELECT DISTINCT _extract_number, _snapshot_date
   FROM activities
   ORDER BY _extract_number DESC
   LIMIT 5;

   -- Verify deletes marked as historical
   SELECT COUNT(*) FROM activities
   WHERE _extract_number = 140 AND _is_current = false;

   -- Verify inserts added as current
   SELECT COUNT(*) FROM activities
   WHERE _extract_number = 141 AND _is_current = true;
   ```

### Test 2: Subsequent Daily Updates

**Goal**: Verify sequential processing works

**Steps**:
1. Apply Extract 0142, 0143, 0147 in sequence
2. Verify extract numbers increment correctly
3. Check temporal tracking integrity

### Test 3: Monthly Snapshot (When Available)

**Goal**: Verify full snapshot with history retention

**Steps**:
1. Extract monthly ZIP to directory
2. Apply via CLI script pointing to directory
3. Verify all old records marked historical
4. Verify new records imported as current
5. Check 24-month cleanup worked correctly

## Error Handling

### Update Conflicts

**Issue**: Update file references extract number that doesn't exist in database

**Solution**: Validate extract number sequence before processing
```typescript
const lastExtract = await db.get(`
  SELECT MAX(_extract_number) as last_extract
  FROM activities
`)

if (parseInt(metadata.ExtractNumber) !== lastExtract + 1) {
  throw new Error(`Missing extract. Expected ${lastExtract + 1}, got ${metadata.ExtractNumber}`)
}
```

### Partial Failures

**Issue**: Update fails midway through processing

**Solution**: Wrap in transaction, rollback on error
```typescript
await db.exec('BEGIN TRANSACTION')
try {
  await processDailyUpdate(zipPath)
  await db.exec('COMMIT')
} catch (error) {
  await db.exec('ROLLBACK')
  throw error
}
```

### Missing Files

**Issue**: Expected CSV file not in ZIP

**Solution**: Log warning and continue (files may be empty)
```typescript
try {
  const content = await zip.entryData(fileName)
  // ... process
} catch (error) {
  if (error.message?.includes('Entry not found')) {
    console.log(`ℹ️  ${fileName}: Not found in ZIP (may be empty)`)
    return 0
  }
  throw error
}
```

## Monitoring & Validation

### Daily Checks

```sql
-- Record counts by extract number
SELECT
  _extract_number,
  _snapshot_date,
  COUNT(*) as total_records,
  SUM(CASE WHEN _is_current = true THEN 1 ELSE 0 END) as current_records
FROM activities
GROUP BY _extract_number, _snapshot_date
ORDER BY _extract_number DESC
LIMIT 10;

-- Gap detection
SELECT
  _extract_number,
  LAG(_extract_number) OVER (ORDER BY _extract_number) as prev_extract,
  _extract_number - LAG(_extract_number) OVER (ORDER BY _extract_number) as gap
FROM (SELECT DISTINCT _extract_number FROM activities)
WHERE gap > 1;
```

### Data Quality

```sql
-- NACE version distribution
SELECT
  nace_version,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*) OVER (), 2) as percentage
FROM activities
WHERE _is_current = true
GROUP BY nace_version;

-- Orphaned records (no current parent enterprise)
SELECT COUNT(*)
FROM establishments e
WHERE e._is_current = true
  AND NOT EXISTS (
    SELECT 1 FROM enterprises ent
    WHERE ent.enterprise_number = e.enterprise_number
      AND ent._is_current = true
  );
```

## Summary: Automation vs Manual

| Process | File Format | Automation | Method |
|---------|-------------|------------|--------|
| **Daily Updates** | ZIP (400 KB - 2 MB) | ✅ Automated | CLI → Webapp → Cron |
| **Monthly Snapshots** | Directory (2+ GB CSV) | ❌ Manual Only | CLI direct ETL |

**Key Constraints**:
- Full dumps are too large for ZIP processing in serverless environment
- Monthly imports require manual extraction and CLI execution
- Daily updates are small enough for automated ZIP processing

## Next Steps

1. ✅ **Complete**: Analysis and strategy documentation
2. ⏳ **Next**: Test daily update with Extract 0141
3. **Then**: Build Vercel admin UI for daily update management
4. **Then**: Implement automatic fetch from KBO portal (daily only)
5. **Finally**: Set up cron job for daily updates only

## References

- **Sample Data**: `/sampledata/KboOpenData_*`
- **Scripts**:
  - `/scripts/analyze-update.ts` - Analyze update ZIP contents
  - `/scripts/check-nace-versions.ts` - Check NACE version distribution
  - `/scripts/apply-daily-update.ts` - Apply daily update (ZIP-based)
  - `/scripts/apply-monthly-snapshot.ts` - Apply monthly snapshot (directory-based, CLI only)
- **Schema**: `/lib/sql/schema/*.sql`
- **Documentation**: `/specs/KBOCookbook_EN.md`
