# Schema Verification - November 2025

**Date**: 2025-11-02
**Purpose**: Verify database schema before processing November full dump

## Verification Results

### Temporal Tables (7)

Tables with full temporal tracking (`_snapshot_date`, `_extract_number`, `_is_current`, `_deleted_at_extract`):

| Table | Primary Key Column | Notes |
|-------|-------------------|-------|
| `enterprises` | `enterprise_number` | Entity identifier |
| `establishments` | `establishment_number` | Entity identifier |
| `denominations` | `id` | Composite hash of entity_number + type + language + hash(name) |
| `addresses` | `id` | Composite hash of entity_number + type + hash(address) |
| `activities` | `id` | Composite hash of entity_number + group + version + code + classification |
| `contacts` | `id` | Composite hash of entity_number + contact_type + hash(value) |
| `branches` | `id` | Composite hash (branch identifier) |

**All 7 tables have `_deleted_at_extract` column** ✅

### Static Tables (2)

Tables WITHOUT temporal tracking (static lookup tables):

| Table | Primary Key | Notes |
|-------|------------|-------|
| `codes` | `(category, code, language)` | Multilingual code descriptions (~21K rows) |
| `nace_codes` | `(nace_version, nace_code)` | NACE economic activity codes (~7K rows) |

**These tables are loaded once per snapshot and never have historical versions.**

## Critical Findings

### 1. Primary Key Columns

For migration queries that compute `_deleted_at_extract`, the correct column to match on is:

- ✅ **enterprises**: `enterprise_number`
- ✅ **establishments**: `establishment_number`
- ❌ **denominations**: `id` (NOT `entity_number`)
- ❌ **addresses**: `id` (NOT `entity_number`)
- ❌ **activities**: `id` (NOT `entity_number`)
- ❌ **contacts**: `id` (NOT `entity_number`)
- ❌ **branches**: `id` (NOT `enterprise_number`)

The `entity_number` column exists in these tables but is NOT the primary key. The primary key is the composite `id` column.

### 2. Static Tables Must Be Excluded

Tables like `codes` and `nace_codes` have NO temporal columns and MUST NOT be included in:
- Temporal migration scripts
- Snapshot comparison scripts
- Historical data backfill operations
- Any operation querying `_is_current`, `_snapshot_date`, `_extract_number`, or `_deleted_at_extract`

## Bugs Fixed

### Script: `migrate-deleted-at-extract.ts`

**Bug 1**: Included `codes` and `nace_codes` in TABLES array
- These tables don't have temporal columns
- Would cause SQL errors when querying `_deleted_at_extract`

**Bug 2**: Wrong ENTITY_ID_COLUMNS mapping
- denominations: Changed `entity_number` → `id`
- addresses: Changed `entity_number` → `id`
- activities: Changed `entity_number` → `id`
- contacts: Changed `entity_number` → `id`
- branches: Changed `enterprise_number` → `id`

**Fix**: Updated TABLES array to only include 7 temporal tables, fixed PK column mapping

### Script: `database-state-snapshot.ts`

**Bug**: Included `codes` and `nace_codes` in TABLES array
- Would try to query temporal columns that don't exist
- Would cause errors when checking `_is_current`, `_extract_number`

**Fix**: Updated TABLES array to only include 7 temporal tables

### Script: `reset-from-full-dump.ts`

**Bug**: Included `codes` and `nace_codes` in TABLES array
- Would try to truncate/backup static lookup tables
- These tables should be preserved, not reset

**Fix**: Updated TABLES array to only include 7 temporal tables

### Code: `lib/import/duckdb-processor.ts`

**Status**: ✅ Already correct!
- `markAllCurrentAsHistorical()` function already has correct 7-table list
- Correctly excludes codes and nace_codes

## Verification Query

```sql
-- Check which tables have temporal tracking
SELECT
  table_name,
  COUNT(CASE WHEN column_name = '_snapshot_date' THEN 1 END) > 0 as has_snapshot_date,
  COUNT(CASE WHEN column_name = '_extract_number' THEN 1 END) > 0 as has_extract_number,
  COUNT(CASE WHEN column_name = '_is_current' THEN 1 END) > 0 as has_is_current,
  COUNT(CASE WHEN column_name = '_deleted_at_extract' THEN 1 END) > 0 as has_deleted_at_extract
FROM information_schema.columns
WHERE table_name IN (
  'enterprises', 'establishments', 'denominations', 'addresses',
  'activities', 'contacts', 'branches', 'codes', 'nace_codes'
)
GROUP BY table_name
ORDER BY table_name;
```

## Migration Query Pattern

For tables where PK is `id`:

```sql
UPDATE ${tableName}
SET _deleted_at_extract = (
  SELECT MIN(_extract_number)
  FROM ${tableName} t2
  WHERE t2.id = ${tableName}.id  -- Match on id, not entity_number!
    AND t2._extract_number > ${tableName}._extract_number
)
WHERE _is_current = false
  AND _deleted_at_extract IS NULL
  AND EXISTS (
    SELECT 1
    FROM ${tableName} t3
    WHERE t3.id = ${tableName}.id
      AND t3._extract_number > ${tableName}._extract_number
  )
```

For tables where PK is entity identifier (enterprises, establishments):

```sql
UPDATE ${tableName}
SET _deleted_at_extract = (
  SELECT MIN(_extract_number)
  FROM ${tableName} t2
  WHERE t2.${entity_id_column} = ${tableName}.${entity_id_column}
    AND t2._extract_number > ${tableName}._extract_number
)
WHERE _is_current = false
  AND _deleted_at_extract IS NULL
  AND EXISTS (
    SELECT 1
    FROM ${tableName} t3
    WHERE t3.${entity_id_column} = ${tableName}.${entity_id_column}
      AND t3._extract_number > ${tableName}._extract_number
  )
```

## Recommendations

1. **Always verify schema** before major operations
2. **Never assume** column names match across tables
3. **Test queries** on sample data before full migration
4. **Document** static vs temporal tables clearly
5. **Update** CLAUDE.md with schema patterns

## Next Steps

With these fixes applied:

1. ✅ Scripts now correctly handle only temporal tables
2. ✅ Primary key columns correctly mapped
3. ✅ Ready for November full dump processing
4. ⏳ Test migration script on dry-run before execute
5. ⏳ Run validation script when November dump available

## References

- Schema DDL files: `lib/sql/schema/*.sql`
- Verification script: `scripts/_verify-schema.ts` (temporary, deleted after use)
- Migration script: `scripts/migrate-deleted-at-extract.ts`
- Process documentation: `docs/MONTHLY_FULL_DUMP_PROCESS.md`
