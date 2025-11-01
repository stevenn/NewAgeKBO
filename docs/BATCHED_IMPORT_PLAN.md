# Batched Import System - Implementation Plan

**Status:** Planning Phase
**Date:** 2025-10-31
**Estimated Effort:** 10-12 hours

---

## Problem Statement

The current Vercel-based daily import system times out at 300 seconds (5 minutes) when processing large tables like `activities` (which can have 15,000+ records in a single daily update).

### Current Flow Issues
1. Vercel function downloads ZIP
2. Extracts CSVs in memory
3. Builds massive INSERT statements in Node.js
4. Times out during activities table processing
5. Results in partially applied imports that need rollback

### Why Two-Step (Prepare → Execute) Won't Work
- Even uploading to MotherDuck temp tables and executing SQL still requires synchronous connection
- MotherDuck doesn't support async query API
- If Vercel times out, the connection closes and query may be canceled
- No way to "fire and forget" a long-running SQL operation

---

## Solution: Micro-Batch Processing

Process imports in small batches (500-1000 records) that each complete in < 30 seconds. The UI polls and drives the import forward, providing real-time progress.

### Key Insight
Instead of one 5-minute operation, we run 50 operations of 6 seconds each, with the UI coordinating.

---

## Architecture

### Database Schema Changes

#### New Table: `import_job_batches`
Tracks individual batch processing status.

```sql
CREATE TABLE import_job_batches (
  job_id VARCHAR NOT NULL,
  table_name VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,      -- 'delete' or 'insert'
  status VARCHAR NOT NULL,          -- 'pending', 'processing', 'completed', 'failed'
  records_count INTEGER,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message TEXT,
  PRIMARY KEY (job_id, table_name, batch_number, operation)
);

CREATE INDEX idx_job_batches_status ON import_job_batches(job_id, status);
CREATE INDEX idx_job_batches_table ON import_job_batches(job_id, table_name);
```

#### New Tables: Typed Staging Tables Per Entity Type
Stores parsed CSV data with proper column types temporarily during import.

**Decision:** Use separate staging tables for each entity type (enterprise, establishment, denomination, address, contact, activity, branch) rather than a single table with JSON. This provides type safety, better performance, and clearer schema alignment with final tables.

```sql
-- Example: Enterprise staging
CREATE TABLE import_staging_enterprise (
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,       -- 'delete' or 'insert'
  processed BOOLEAN DEFAULT false,

  -- Enterprise-specific columns (matching enterprise table schema)
  entity_number VARCHAR,
  status VARCHAR,
  juridical_situation VARCHAR,
  type_of_enterprise VARCHAR,
  juridical_form VARCHAR,
  juridical_form_cac VARCHAR,
  start_date DATE,

  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_staging_enterprise_batch ON import_staging_enterprise(job_id, batch_number, processed);

-- Similar tables for: establishment, denomination, address, contact, activity, branch
-- Each with their respective columns matching final table schemas
```

**Cleanup Strategy:** Delete all staging table data after job completes (per-job cleanup).

---

### Backend Implementation

#### New Library: `lib/import/batched-update.ts`

**Core Functions:**

```typescript
// Step 1: Preparation phase
export async function prepareImport(
  zipBuffer: Buffer,
  workerType: WorkerType
): Promise<PrepareImportResult> {
  // 1. Parse metadata
  // 2. Create import_jobs record
  // 3. Extract CSVs and parse
  // 4. Insert into import_staging (with batch_number assigned)
  // 5. Create batch records in import_job_batches
  // 6. Return job_id, total_batches, batch_count_by_table
}

// Step 2: Batch processing
export async function processBatch(
  jobId: string,
  tableName?: string,
  batchNumber?: number
): Promise<ProcessBatchResult> {
  // 1. Get next pending batch (or specified batch)
  // 2. Fetch staging data for batch
  // 3. Execute DELETE or INSERT for that batch
  // 4. Mark batch as completed
  // 5. Return progress info (completed/total, next_batch)
}

// Step 3: Progress tracking
export async function getImportProgress(
  jobId: string
): Promise<ImportProgress> {
  // 1. Query batch statuses by table
  // 2. Calculate overall percentage
  // 3. Identify next pending batch
  // 4. Return structured progress data
}

// Step 4: Finalization
export async function finalizeImport(
  jobId: string
): Promise<FinalizeResult> {
  // 1. Verify all batches completed
  // 2. Resolve primary names (existing logic)
  // 3. Update import_jobs status to 'completed'
  // 4. Clean up staging data
}
```

**Batch Size Configuration:**
```typescript
const BATCH_SIZES = {
  activities: 500,      // Large table, conservative batch size
  addresses: 1000,
  contacts: 1000,
  denominations: 1000,
  enterprises: 2000,    // Smaller table, larger batches ok
  establishments: 2000,
  branches: 1000,
}

// For small files (< 5000 records total), use 1 batch
```

---

#### API Endpoints

**1. POST `/api/import/daily-update/prepare`**
- Body: `{ url: string }` or `{ filename: string }`
- Downloads ZIP
- Calls `prepareImport()`
- Returns: `{ job_id, total_batches, batches_by_table }`
- Max duration: 90 seconds

**2. POST `/api/import/daily-update/process-batch`**
- Body: `{ job_id: string, table_name?: string, batch_number?: number }`
- Processes next pending batch (or specified batch)
- Returns: `{ batch_completed: true, progress: { completed, total, percentage }, next_batch }`
- Max duration: 30 seconds

**3. GET `/api/import/daily-update/status/:job_id`**
- Returns full progress breakdown
- Structure:
  ```json
  {
    "job_id": "xxx",
    "status": "processing",
    "overall_progress": {
      "completed_batches": 23,
      "total_batches": 50,
      "percentage": 46
    },
    "tables": {
      "activities": { "completed": 10, "total": 10, "status": "completed" },
      "addresses": { "completed": 12, "total": 15, "status": "processing" },
      "contacts": { "completed": 0, "total": 5, "status": "pending" }
    },
    "current_batch": { "table": "addresses", "batch": 13, "operation": "delete" },
    "next_batch": { "table": "addresses", "batch": 13, "operation": "delete" }
  }
  ```
- Max duration: 5 seconds

**4. POST `/api/import/daily-update/finalize`**
- Body: `{ job_id: string }`
- Calls `finalizeImport()`
- Returns: `{ success: true, names_resolved: 150 }`
- Max duration: 30 seconds

**5. POST `/api/import/daily-update/retry-batch`** (Error handling)
- Body: `{ job_id: string, table_name: string, batch_number: number }`
- Resets batch status to 'pending'
- Returns: `{ success: true }`
- Max duration: 5 seconds

---

### Frontend Implementation

#### New Page: `/app/admin/imports/[job_id]/progress`

**Features:**
- Real-time progress bar (overall)
- Table-by-table breakdown
- Auto-polling every 2 seconds
- Automatic batch processing (calls process-batch in loop)
- Pause/resume functionality
- Error display with retry buttons

**UI Mockup:**
```
┌─────────────────────────────────────────────────────────┐
│ Daily Update Import - Extract 167                       │
│ Started: 2025-10-31 10:30:45                           │
└─────────────────────────────────────────────────────────┘

Overall Progress
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 46% (23/50 batches)

Tables:
┌─ activities      ━━━━━━━━━━ 100% (10/10)  ✓ Completed
├─ addresses       ━━━━━━━░░░  80% (12/15)  ⏳ Processing batch 13/15
├─ contacts        ░░░░░░░░░░   0% (0/5)    ⏸ Pending
├─ denominations   ░░░░░░░░░░   0% (0/8)    ⏸ Pending
├─ enterprises     ░░░░░░░░░░   0% (0/3)    ⏸ Pending
└─ establishments  ░░░░░░░░░░   0% (0/9)    ⏸ Pending

Current Operation:
  Marking 957 addresses as historical (batch 13/15)

Estimated Completion: ~2 minutes

[⏸ Pause Import]  [🔄 Refresh]
```

**Processing Loop (React):**
```typescript
useEffect(() => {
  if (status === 'processing' && !paused) {
    const interval = setInterval(async () => {
      // 1. Fetch current status
      const progress = await fetch(`/api/import/daily-update/status/${jobId}`)
      setProgress(progress)

      // 2. If not all completed, process next batch
      if (progress.next_batch) {
        await fetch('/api/import/daily-update/process-batch', {
          method: 'POST',
          body: JSON.stringify({ job_id: jobId })
        })
      } else if (progress.overall_progress.percentage === 100) {
        // 3. All batches done, finalize
        await fetch('/api/import/daily-update/finalize', {
          method: 'POST',
          body: JSON.stringify({ job_id: jobId })
        })
        setStatus('completed')
      }
    }, 2000) // Poll every 2 seconds

    return () => clearInterval(interval)
  }
}, [status, paused, jobId])
```

#### Updated: `/app/admin/imports/page.tsx`

- "Import Daily Update" button triggers `/prepare` endpoint
- Redirects to `/imports/[job_id]/progress` immediately
- Shows list of recent imports with status badges

---

## Implementation Steps

### Phase 1: Database Schema (30 minutes)
1. Create migration script: `scripts/create-batched-import-schema.ts`
2. Add `import_job_batches` table
3. Add `import_staging` table
4. Test schema creation

### Phase 2: Core Library (3-4 hours)
1. Create `lib/import/batched-update.ts`
2. Implement `prepareImport()` function
   - Parse ZIP and metadata
   - Assign batch numbers
   - Insert to staging table
   - Create batch records
3. Implement `processBatch()` function
   - Build batch-specific SQL
   - Execute deletes/inserts
   - Update batch status
4. Implement `getImportProgress()` function
5. Implement `finalizeImport()` function
6. Write unit tests for batch logic

### Phase 3: API Endpoints (2-3 hours)
1. Create `/prepare` endpoint
2. Create `/process-batch` endpoint
3. Create `/status` endpoint
4. Create `/finalize` endpoint
5. Create `/retry-batch` endpoint
6. Test each endpoint with Postman/curl

### Phase 4: Frontend UI (2-3 hours)
1. Create progress page component
2. Implement polling logic
3. Build progress visualization components
4. Add pause/resume functionality
5. Add error handling UI
6. Test in browser

### Phase 5: Integration Testing (2 hours)
1. Test with small sample file (< 100 records)
2. Test with full daily update (15k+ records)
3. Test error scenarios (timeout, network failure)
4. Test pause/resume
5. Test retry failed batches

### Phase 6: Deployment & Monitoring
1. Update README with new import flow
2. Add monitoring for batch processing times
3. Deploy to Vercel
4. Monitor first real import

---

## Batch Processing Logic Details

### Delete Operation (Mark Historical)

```sql
-- Example for enterprises
UPDATE enterprise
SET _is_current = false,
    _deleted_at_extract = ${extractNumber}
WHERE entity_number IN (
  SELECT entity_number
  FROM import_staging_enterprise
  WHERE job_id = '${jobId}'
    AND operation = 'delete'
    AND batch_number = ${batchNumber}
    AND processed = false
)
AND _is_current = true;

-- Mark staging records as processed
UPDATE import_staging_enterprise
SET processed = true
WHERE job_id = '${jobId}'
  AND operation = 'delete'
  AND batch_number = ${batchNumber};
```

### Insert Operation

**Example 1: Enterprise (simple table with natural PK)**

```sql
INSERT INTO enterprise (
  entity_number, status, juridical_situation, type_of_enterprise,
  juridical_form, juridical_form_cac, start_date,
  _snapshot_date, _extract_number, _is_current
)
SELECT
  entity_number,
  status,
  juridical_situation,
  type_of_enterprise,
  juridical_form,
  juridical_form_cac,
  start_date,
  '${snapshotDate}'::DATE,
  ${extractNumber},
  true
FROM import_staging_enterprise
WHERE job_id = '${jobId}'
  AND operation = 'insert'
  AND batch_number = ${batchNumber}
  AND processed = false;

-- Mark as processed
UPDATE import_staging_enterprise SET processed = true
WHERE job_id = '${jobId}'
  AND operation = 'insert'
  AND batch_number = ${batchNumber};
```

**Example 2: Activity (table with computed composite ID)**

```sql
INSERT INTO activity (
  id, entity_number, activity_group, nace_version, nace_code, classification,
  _snapshot_date, _extract_number, _is_current
)
SELECT
  -- Compute composite ID
  entity_number || '_' || activity_group || '_' || nace_version || '_' || nace_code,

  entity_number,
  activity_group,
  nace_version,
  nace_code,
  classification,
  '${snapshotDate}'::DATE,
  ${extractNumber},
  true
FROM import_staging_activity
WHERE job_id = '${jobId}'
  AND operation = 'insert'
  AND batch_number = ${batchNumber}
  AND processed = false;

-- Mark as processed
UPDATE import_staging_activity SET processed = true
WHERE job_id = '${jobId}'
  AND operation = 'insert'
  AND batch_number = ${batchNumber};
```

---

## Error Handling

### Batch Failure Scenarios

1. **SQL Error:** Log error to batch record, mark as 'failed', continue with other batches
2. **Timeout:** Batch remains 'processing', can be retried
3. **Network Error:** Frontend retries automatically
4. **Vercel Cold Start:** First batch may be slower, subsequent batches fast

### Recovery Strategies

- **Retry Single Batch:** UI shows "Retry" button for failed batches
- **Retry All Failed:** Bulk retry endpoint
- **Rollback Entire Job:** Reuse existing rollback scripts (adapt for job_id)
- **Manual Intervention:** Staging data persists for debugging

---

## Performance Considerations

### Batch Size Tuning

Start conservative (500-1000 records), monitor actual processing times:
- Target: 10-20 seconds per batch
- If consistently < 5 seconds: increase batch size
- If > 25 seconds: decrease batch size

### Staging Table Cleanup

**Strategy:** Delete all staging data after job completion (called during `finalizeImport()`)

```sql
-- Clean up all staging tables for completed job
DELETE FROM import_staging_enterprise WHERE job_id = '${jobId}';
DELETE FROM import_staging_establishment WHERE job_id = '${jobId}';
DELETE FROM import_staging_denomination WHERE job_id = '${jobId}';
DELETE FROM import_staging_address WHERE job_id = '${jobId}';
DELETE FROM import_staging_contact WHERE job_id = '${jobId}';
DELETE FROM import_staging_activity WHERE job_id = '${jobId}';
DELETE FROM import_staging_branch WHERE job_id = '${jobId}';

-- For failed jobs, staging data remains for debugging until manual cleanup
```

### MotherDuck Cost Optimization

- Each batch = ~5-10 seconds of compute
- 50 batches = ~5 minutes total compute time
- Same cost as current approach, but no timeout issues

---

## Migration Path

**Approach:** Direct replacement of existing import system

1. Deploy new batched import system
2. Update `/api/import/daily-update` to use new flow (`/prepare` → progress page)
3. Monitor first production import closely
4. Keep rollback scripts ready for emergency recovery
5. Maintain existing rollback scripts (adapt for `job_id` if needed)

---

## Benefits Summary

✅ **No Timeouts:** Each batch < 30s, well under Vercel limits
✅ **Real-time Progress:** Users see exactly what's happening
✅ **Handles Variability:** Works with any data volume and variable MotherDuck performance
✅ **Resumable:** Can pause/resume or retry failed batches
✅ **Better UX:** Professional progress tracking instead of "please wait..."
✅ **Better Debugging:** Can inspect staging data, retry specific batches
✅ **Cost Efficient:** Same total compute time, just split into chunks

---

## Design Decisions (Confirmed)

1. **Batch assignment strategy:** ✅ Pre-assigned based on input data size
   - Batches calculated during `prepareImport()` phase
   - Batch sizes: 500-1000 records per batch depending on table

2. **Parallel batch processing:** ✅ Sequential processing
   - Process one batch at a time
   - Simpler, easier to debug and monitor

3. **Staging data format:** ✅ Typed staging tables per entity type
   - Separate tables: `import_staging_enterprise`, `import_staging_denomination`, etc.
   - Proper column types matching final schemas
   - Better performance and type safety vs JSON

4. **Progress polling:** ✅ Client-side polling
   - Frontend polls every 2 seconds
   - Simpler, more compatible with Vercel

5. **Cleanup strategy:** ✅ Per-job cleanup after completion
   - Delete all staging data when job status = 'completed'
   - No time-based cleanup needed

6. **Deployment approach:** ✅ Direct replacement (Option B)
   - Replace existing `/api/import/daily-update` endpoint
   - No parallel systems
   - Keep rollback scripts ready for safety

---

## References

- Current import logic: `lib/import/daily-update.ts`
- Rollback scripts: `scripts/rollback-extract-167.ts`, `scripts/diagnose-extract-167.ts`
- Database schema: `lib/sql/schema/`
- Import jobs table: `lib/sql/schema/09_import_jobs.sql`

---

## Next Session Action Items

1. Review this plan
2. Confirm batch sizes and strategies
3. Create database migration
4. Start implementing `batched-update.ts`

**Estimated Timeline:** Can be implemented over 2-3 coding sessions (4-6 hours each).
