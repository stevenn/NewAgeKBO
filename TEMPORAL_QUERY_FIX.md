# Temporal Query Fix - Incremental Updates Issue

## Problem Statement

The current temporal query logic is **fundamentally broken** for historical snapshots when using incremental updates.

### Current (Wrong) Logic

```typescript
// In app/api/enterprises/[number]/route.ts:88
temporalFilter = `AND _snapshot_date = '${snapshotDate}' AND _extract_number = ${extractNumber}`
```

This queries **ONLY records inserted in Extract N**, not the complete state as of Extract N.

### Why This Is Wrong

Given:
- **Extract 140** (full dump): Contains all enterprise data
- **Extract 150** (incremental): Only contains CHANGES (new, updated, deleted records)
- **Extract 157** (incremental): Only contains CHANGES

Current query for Extract 150:
```sql
SELECT * FROM denominations
WHERE entity_number = '0878.689.049'
  AND _extract_number = 150  -- ❌ WRONG! Only returns records changed in Extract 150
```

**Result**: Empty for enterprise 0878.689.049 because its denominations weren't updated in Extract 150.

**Correct query** should return: The denomination from Extract 140 (still valid at Extract 150).

## Complete Fix Strategy

### Phase 1: Add Deletion Tracking ✅ DONE

**Files Modified**:
- `scripts/add-deletion-tracking.ts` - Migration script
- `scripts/apply-daily-update.ts` - Updated deletion logic

**Changes**:
```typescript
// OLD: Just mark as not current
UPDATE ${dbTableName}
SET _is_current = false
WHERE ${dbPkColumn} IN (${entityNumbers})

// NEW: Track when deleted
UPDATE ${dbTableName}
SET _is_current = false,
    _deleted_at_extract = ${extractNumber}
WHERE ${dbPkColumn} IN (${entityNumbers})
```

**To Apply**:
```bash
# Step 1: Add the column to all tables
npx tsx scripts/add-deletion-tracking.ts

# Step 2: Refresh views to include new column
npx tsx scripts/create-views.ts
```

**Important**: After adding the column, database views must be recreated to reflect the schema change. Views use `SELECT *` and cache the column list, so they need explicit refresh.

### Phase 2: Fix Temporal Query Logic ⚠️ IN PROGRESS

**Core Logic Required**:

For point-in-time query at Extract N:
```sql
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY _extract_number DESC, _snapshot_date DESC
    ) as rn
  FROM denominations
  WHERE entity_number = '0878.689.049'
    -- Records created/updated before or at Extract N
    AND _extract_number <= 150
    -- Not deleted, or deleted after Extract N
    AND (_deleted_at_extract IS NULL OR _deleted_at_extract > 150)
) sub
WHERE rn = 1  -- Take latest version of each record
```

**What This Does**:
1. Finds ALL versions of each record up to Extract 150
2. Excludes records deleted before Extract 150
3. Takes the most recent version of each unique record
4. **Result**: Complete state as it existed at Extract 150

### Phase 3: Update API Implementation

**Files to Update**:
1. `lib/motherduck/temporal-query.ts` ✅ Created helper functions
2. `lib/motherduck/enterprise-detail.ts` ⚠️ Partially updated
3. `app/api/enterprises/[number]/route.ts` ⚠️ Needs update
4. `app/api/enterprises/[number]/snapshots/route.ts` ⚠️ Needs update

**API Route Changes**:
```typescript
// OLD
const temporalFilter = snapshotDate && extractNumber
  ? `AND _snapshot_date = '${snapshotDate}' AND _extract_number = ${extractNumber}`
  : 'AND _is_current = true'

const detail = await fetchEnterpriseDetail(db, number, temporalFilter)

// NEW
const filter = extractNumber
  ? {
      type: 'point-in-time' as const,
      extractNumber: parseInt(extractNumber),
      snapshotDate: snapshotDate || undefined,
    }
  : { type: 'current' as const }

const detail = await fetchEnterpriseDetail(db, number, filter)
```

## Implementation Status

### ✅ COMPLETED - All Phases Done!

1. **Schema Enhancement**
   - Added `_deleted_at_extract` column to all temporal tables
   - Created migration script: `scripts/add-deletion-tracking.ts`
   - ✅ Migration executed successfully
   - ✅ Database views refreshed to include new column

2. **Deletion Tracking**
   - Updated `scripts/apply-daily-update.ts` to track deletion extract number
   - Future deletions will be properly tracked

3. **Temporal Query Helpers**
   - Created `lib/motherduck/temporal-query.ts` with:
     - `buildTemporalFilter()` - Builds WHERE clause for point-in-time queries
     - `buildPointInTimeQuery()` - Wraps queries with ROW_NUMBER() window function
     - `buildChildTableQuery()` - Simplified helper for child tables

4. **Enterprise Detail Fetcher**
   - `lib/motherduck/enterprise-detail.ts` - Fully refactored
   - All child table queries updated (denominations, addresses, activities, contacts, establishments)
   - Enterprises query fixed to work with window function wrapping

5. **API Route Updates**
   - `app/api/enterprises/[number]/route.ts` - Updated to use TemporalFilter object ✅
   - `app/api/enterprises/[number]/snapshots/route.ts` - Updated to use TemporalFilter object ✅

6. **Testing & Validation**
   - Created test script: `scripts/test-temporal-query.ts`
   - ✅ PASS: Enterprise 0878.689.049 at Extract 150 correctly returns data from Extract 140
   - Point-in-time reconstruction working as expected

### 📝 Optional Follow-up

7. **Frontend Compatibility**
   - Currently the inverted comparison UI works around the (now fixed) backend bug
   - May need to adjust UI comparison logic for optimal user experience
   - This is non-critical as the workaround is functional

## Workaround Strategy (Current Behavior)

Until the full fix is complete, the **inverted comparison** approach in the frontend provides a functional workaround:

### How It Works Now

When viewing Extract 150:
1. Backend returns EMPTY data (because of the bug)
2. Frontend also fetches Extract 140 (previous snapshot)
3. Frontend displays Extract 140 data as baseline
4. Frontend shows what was "removed" in Extract 150

### Why This Works

- Users can still see historical data (from previous snapshot)
- Change indicators show what disappeared
- Prevents confusing empty views

### Limitations

- Only works when there IS a previous snapshot
- Can't distinguish between "never existed" and "deleted"
- Performance: Fetches 2 snapshots instead of reconstructing 1

## Next Full Dump Strategy

**Timeline**: Next full dump arrives weekly

**Impact on Deletion Tracking**:
- Full dump will reset all records to `_is_current = true`
- All `_deleted_at_extract` values will be set to NULL
- Deletion tracking starts fresh from the full dump
- Works perfectly for the next month of incremental updates

**Recommendation**:
1. Complete the temporal query fix before the next full dump
2. Test with current incremental data (Extracts 140, 150, 157)
3. When full dump arrives, deletion tracking will work properly going forward

## Testing Plan

### Test Case 1: Enterprise 0878.689.049

**Snapshots**:
- Extract 140: Has denomination "A.S.B.L. Villers 2000"
- Extract 150: No denomination records (not updated)
- Extract 157: No denomination records (not updated)

**Expected Results**:
- Query for Extract 140: Returns denomination ✅
- Query for Extract 150: **Should return** denomination from Extract 140 (currently returns empty ❌)
- Query for Extract 157 (current): Returns empty ✅ (never had data)

### Test Case 2: Companies with Multiple Updates

Test with companies that have changes across all three extracts:
- 0670.994.431
- 0733.584.769
- 0739.743.576

**Verify**:
1. Point-in-time queries return complete state
2. Changes are correctly tracked
3. Deletions are properly handled (after next full dump)

## Files Reference

### Created
- `scripts/add-deletion-tracking.ts` - Migration script
- `lib/motherduck/temporal-query.ts` - Query helpers
- `TEMPORAL_QUERY_FIX.md` - This document
- `TEMPORAL_VERSIONING_ANALYSIS.md` - Problem analysis

### Modified
- `scripts/apply-daily-update.ts` - Deletion tracking
- `lib/motherduck/enterprise-detail.ts` - Partial refactor
- `app/admin/browse/[number]/page.tsx` - Inverted comparison UI (workaround)

### Need Updates
- `app/api/enterprises/[number]/route.ts`
- `app/api/enterprises/[number]/snapshots/route.ts`
- Complete refactor of `lib/motherduck/enterprise-detail.ts`

## Conclusion

The temporal query bug is a **critical issue** that affects all historical data queries. The workaround (inverted comparison UI) provides functional access to historical data, but the proper fix requires:

1. ✅ Deletion tracking (completed)
2. ⚠️ Temporal query refactor (in progress)
3. ❌ API integration (not started)

**Recommendation**: Complete the fix before processing the next full dump to ensure deletion tracking works properly for future incremental updates.
