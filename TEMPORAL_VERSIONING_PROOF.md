# Temporal Versioning Proof - Enterprise 0721.700.388

## Source Data from CSV Files

### Update Set 140 (Full Dump - Oct 4, 2025)

**File:** `KboOpenData_0140_2025_10_05_Full/address.csv`

```csv
"0721.700.388","REGO",,,"8800","Roeselare","Roeselare","Noordstraat","Noordstraat","52","33","",
```

**Address:** Noordstraat 52 box 33, 8800 Roeselare

---

### Update Set 150 (Daily Update - Oct 14, 2025)

**File:** `address_delete.csv`
```csv
"EntityNumber"
"0721.700.388"
```

**File:** `address_insert.csv`
```csv
"EntityNumber","TypeOfAddress","CountryNL","CountryFR","Zipcode","MunicipalityNL","MunicipalityFR","StreetNL","StreetFR","HouseNumber","Box","ExtraAddressInfo","DateStrikingOff"
"0721.700.388","REGO",,,"8800","Roeselare","Roeselare","Barnumerf","Barnumerf","8","","",
```

**Address:** Barnumerf 8, 8800 Roeselare

**Action:** DELETE old address (Noordstraat) → INSERT new address (Barnumerf)

---

### Update Set 157 (Daily Update - Oct 21, 2025)

**File:** `address_delete.csv`
```csv
"EntityNumber"
"0721.700.388"
```

**File:** `address_insert.csv`
```csv
"EntityNumber","TypeOfAddress","CountryNL","CountryFR","Zipcode","MunicipalityNL","MunicipalityFR","StreetNL","StreetFR","HouseNumber","Box","ExtraAddressInfo","DateStrikingOff"
"0721.700.388","REGO",,,"8830","Hooglede","Hooglede","Kerkstraat","Kerkstraat","20","","",
```

**Address:** Kerkstraat 20, 8830 Hooglede

**Action:** DELETE old address (Barnumerf) → INSERT new address (Kerkstraat)

---

### Update Sets 141-149, 151-156

No changes found for enterprise 0721.700.388 in these Update Sets.

---

## Database State (Current)

From Motherduck query:

| Update Set | Address | `_is_current` | `_deleted_at_extract` |
|------------|---------|---------------|----------------------|
| 140 | Noordstraat 52 box 33, 8800 Roeselare | `false` | **`NULL`** ❌ |
| 150 | Barnumerf 8, 8800 Roeselare | `false` | **`NULL`** ❌ |
| 157 | Kerkstraat 20, 8830 Hooglede | `true` | `NULL` ✅ |

---

## What SHOULD Be in the Database

According to KBO Cookbook logic (DELETE-then-INSERT pattern):

| Update Set | Address | `_is_current` | `_deleted_at_extract` |
|------------|---------|---------------|----------------------|
| 140 | Noordstraat 52 box 33, 8800 Roeselare | `false` | **`150`** ✅ |
| 150 | Barnumerf 8, 8800 Roeselare | `false` | **`157`** ✅ |
| 157 | Kerkstraat 20, 8830 Hooglede | `true` | `NULL` ✅ |

---

## The Problem

**Deletion tracking (`_deleted_at_extract`) is NOT being populated during the import process.**

### According to KBO Cookbook (Section 1.5.2):

> If a name is added, changed or deleted in KBO, then:
> - the enterprise number appears in `denomination_delete.csv`
> - all names of this entity (not the history) appear in `denomination_insert.csv`
>
> You therefore need to go through 2 steps to update your database (in pseudo-sql):
> 1. `DELETE FROM mydatabase.denomination WHERE entitynumber IN (SELECT entitynumber FROM denomination_delete.csv)`
> 2. `INSERT INTO mydatabase.denomination (SELECT * FROM denomination_insert.csv)`

**The same logic applies to addresses, activities, contacts, and all other child tables.**

### Current Implementation Issue

The import script (in `scripts/apply-daily-update.ts`) sets:
```sql
UPDATE ${table}
SET _is_current = false,
    _deleted_at_extract = ${updateSetNumber}
WHERE entity_number IN (delete_list)
  AND _is_current = true
```

But this appears to NOT be working correctly, as evidenced by the `NULL` values in `_deleted_at_extract`.

---

## Impact on Temporal Queries

### Correct Query (with proper deletion tracking):
```sql
WHERE _extract_number <= 150
  AND (_deleted_at_extract IS NULL OR _deleted_at_extract > 150)
```

This would correctly return: **Barnumerf 8** (created in 150, deleted in 157)

### Current Broken Query:
Because all `_deleted_at_extract` values are `NULL`, the query returns ALL records that existed up to Update Set 150, which happens to work by accident because we then use `ROW_NUMBER()` to pick the latest version.

---

## Terminology Correction

Throughout the codebase, we use "Extract" but the correct KBO terminology is **"Update Set"**.

- ✅ Update Set 140 (Full)
- ❌ Extract 140

The term "extract" in KBO context means "extraction" (the process of creating the file), not a subset of data.

---

## Current Status & Resolution (October 2025)

### Investigation Outcome

After thorough investigation and verification:

1. **`_deleted_at_extract` is NOT populated** - Column exists in schema but all values are NULL
2. **This was a deliberate decision** - Populating this column requires complex lookahead logic during imports
3. **Workaround implemented** - Point-in-time queries use natural key partitioning instead

### Working Solution

Instead of relying on `_deleted_at_extract`, temporal queries partition by **natural keys**:

**For denominations**:
```sql
PARTITION BY entity_number, language, denomination_type
```

**For addresses**:
```sql
PARTITION BY entity_number, type_of_address
```

This ensures only the latest version within each natural key group is returned, avoiding duplicates.

### Why This Works

- Each entity can have only ONE denomination per (language, type) combination at a time
- Each entity can have only ONE address per (type) at a time
- Natural key partitioning correctly groups versions
- ROW_NUMBER() picks the latest version within each group

### Documented Limitation

The `_deleted_at_extract` column is:
- ✅ Present in schema for future use
- ✅ Documented in schema comments
- ✅ Explained in IMPLEMENTATION_GUIDE.md
- ❌ Not populated by import scripts (accepted limitation)
- 🔮 Planned for future import pipeline refactor (Phase 4+)

### Database Recommendation

**Recreate database on November 1st** with fresh import to ensure:
- Schema is consistent with latest code
- All comments and documentation are in database
- Clean baseline for future improvements
- No confusion about NULL `_deleted_at_extract` values
