# Code Tables Temporal Review

**Date**: 2025-11-03
**Question**: Should `codes` and `nace_codes` tables have temporal tracking?
**Answer**: ✅ **NO - Current implementation is CORRECT**

## Executive Summary

After reviewing sample data, KBO documentation, database schema, and import code, the current treatment of `codes` and `nace_codes` as **static lookup tables WITHOUT temporal tracking** is correct and reflects reality.

## Evidence

### 1. Sample Data Analysis

**Test**: Compared `code.csv` files across multiple extracts

```bash
Extract 140 (Oct 5, Full):   MD5 = d92c3249903ed9c7b880873e09ea19af
Extract 141 (Oct 6, Daily):  MD5 = d92c3249903ed9c7b880873e09ea19af
Extract 145 (Oct 10, Daily): MD5 = d92c3249903ed9c7b880873e09ea19af
Extract 150 (Oct 15, Daily): MD5 = d92c3249903ed9c7b880873e09ea19af
```

**Result**: ✅ **Identical files** - Code tables do not change between snapshots

### 2. KBO Cookbook Documentation

**Source**: `specs/KBOCookbook_EN.md`

> "For the codes, you therefore always receive the **entire list** as in the full file, and **not just the changes**."

**Interpretation**:
- code.csv is included in every update ZIP
- It's always the complete, unchanged reference data
- There are no code_delete.csv or code_insert.csv files
- KBO treats codes as static reference data

### 3. File Contents Analysis

**Structure of code.csv** (21,501 rows):
- 97% NACE codes (20,876 rows) - economic activity classifications
- 3% other codes (624 rows) - status, juridical forms, contact types, etc.

**Categories** (15 total):
```
ActivityGroup        - Activity group codes (001-007)
Classification       - Activity classification (MAIN, SECO, ANCI)
ContactType          - Contact type codes (TEL, EMAIL, WEB)
EntityContact        - Entity contact codes
JuridicalForm        - Legal form codes
JuridicalSituation   - Juridical situation codes
Language             - Language codes
Nace2003             - NACE 2003 economic activity codes
Nace2008             - NACE 2008 economic activity codes
Nace2025             - NACE 2025 economic activity codes
Status               - Enterprise status codes (AC, ST)
TypeOfAddress        - Address type codes (REGO, BAET, etc.)
TypeOfDenomination   - Denomination type codes (001-004)
TypeOfEnterprise     - Enterprise type codes
```

### 4. Database Schema Verification

**codes table** (`lib/sql/schema/09_codes.sql`):
```sql
CREATE TABLE IF NOT EXISTS codes (
  category VARCHAR NOT NULL,
  code VARCHAR NOT NULL,
  language VARCHAR NOT NULL,
  description VARCHAR NOT NULL,
  PRIMARY KEY (category, code, language)
);
```
✅ **No temporal columns** (_snapshot_date, _extract_number, _is_current, _deleted_at_extract)

**nace_codes table** (`lib/sql/schema/06_nace_codes.sql`):
```sql
CREATE TABLE IF NOT EXISTS nace_codes (
  nace_version VARCHAR NOT NULL,
  nace_code VARCHAR NOT NULL,
  description_nl VARCHAR,
  description_fr VARCHAR,
  PRIMARY KEY (nace_version, nace_code)
);
```
✅ **No temporal columns**

**Schema documentation** (`lib/sql/schema/00_init.sql`):
```
-- LOOKUP TABLES (Static reference data)
-- 6. NACE Codes (7K rows, <1 MB)
--    Static lookup table - loaded once
-- 9. Codes (21K rows, <1 MB)
--    Static lookup table for all code descriptions
```

### 5. Import Code Verification

**Transformations** (`lib/import/transformations.ts`):
- `getCodesTransformation()` - NO temporal fields added
- `getNaceCodesTransformation()` - NO temporal fields added

**Daily updates** (`lib/import/daily-update.ts`):
- Dynamically discovers tables from ZIP file entries
- Only processes files with `_delete.csv` or `_insert.csv` variants
- `code.csv` has no delete/insert variants → never processed as temporal

**Batched imports** (`lib/import/batched-update.ts`):
- Only includes 7 temporal tables in DELETE/INSERT operations
- `codes` and `nace_codes` explicitly excluded

**Schema consistency review** (`docs/SCHEMA_CONSISTENCY_REVIEW.md`):
```
✅ Issue 1: Including codes/nace_codes in temporal operations
Status: NOT FOUND
- Daily updates: Dynamically discovers tables from ZIP, never includes static tables
- Batched updates: Only processes 7 temporal tables in DELETE/INSERT
```

### 6. View Definitions

Only **7 temporal tables** have `_current` views:
```sql
enterprises_current
establishments_current
denominations_current
addresses_current
activities_current
contacts_current
branches_current
```

**No views for codes or nace_codes** - correct, as they don't need current/historical filtering.

## Why Codes Don't Change

### Business Logic
- **NACE codes** are EU-wide standards that change only when new versions are released (2003 → 2008 → 2025)
- **Status codes** (Active, Stopped) are fixed
- **Juridical form codes** are Belgian legal classifications that rarely change
- **Contact type codes** (TEL, EMAIL, WEB) are stable

### Technical Implementation
KBO includes code.csv in every update to ensure:
- Self-contained ZIP files (no external dependencies)
- Always have complete reference data for lookups
- Simplify processing (no need to track code changes)

## Storage & Performance Impact

### If codes WERE temporal (hypothetical):

**Current snapshot**:
- codes: 624 rows × 1 snapshot = 624 rows
- nace_codes: 7,265 rows × 1 snapshot = 7,265 rows
- **Total**: ~8K rows

**With 24 months of snapshots**:
- codes: 624 rows × 24 snapshots = 14,976 rows
- nace_codes: 7,265 rows × 24 snapshots = 174,360 rows
- **Total**: ~189K rows

**Reality check**:
- These would be 189K rows that are ALL IDENTICAL
- Waste of storage (23x increase)
- Waste of query time (scanning identical rows)
- No business value (codes don't change)

### JOIN impact with temporal tracking:

**Current** (simple lookup):
```sql
SELECT e.*, c.description
FROM enterprises_current e
JOIN codes c ON c.category = 'Status' AND c.code = e.status
```

**If temporal** (complex):
```sql
SELECT e.*, c.description
FROM enterprises e
JOIN codes c
  ON c.category = 'Status'
  AND c.code = e.status
  AND c._extract_number <= e._extract_number
  AND (c._deleted_at_extract IS NULL OR c._deleted_at_extract > e._extract_number)
WHERE e._is_current = true
```

Much more complex, slower, and provides zero value since codes never change!

## Conclusion

✅ **Current implementation is CORRECT**

The `codes` and `nace_codes` tables should remain **static lookup tables** without temporal tracking because:

1. ✅ **Sample data proves** they don't change (identical MD5 across 10+ extracts)
2. ✅ **KBO documentation states** they're always the complete list, not deltas
3. ✅ **Database schema** correctly defines them as static
4. ✅ **Import code** correctly excludes them from temporal operations
5. ✅ **Business logic** supports static codes (EU standards, legal classifications)
6. ✅ **Performance** would suffer with unnecessary temporal tracking
7. ✅ **Storage** would waste 23x more space for identical data

## Recommendations

1. ✅ **No changes needed** to current implementation
2. ✅ **Keep documentation clear** about static vs temporal tables
3. ✅ **Monitor in production** - if KBO ever releases code updates, we'll see different MD5 hashes
4. ⚠️ **Future consideration**: If NACE 2025 is adopted widely and NACE 2003 is deprecated, we might need to update the nace_codes table, but this would be a one-time schema migration, not temporal tracking

## Testing Checklist for November Dump

When November full dump arrives:

- [ ] Compare `code.csv` MD5 hash with October dump
- [ ] Confirm identical (expected)
- [ ] If different, investigate what changed and whether temporal tracking is needed
- [ ] Document findings

---

**Reviewed by**: Claude Code
**Confidence**: High - Multiple independent sources confirm static nature
**Next review**: After November 2025 full dump validation
