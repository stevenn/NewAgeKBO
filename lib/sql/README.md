# KBO Database Schema

This directory contains the complete database schema for the KBO for the New Age project.

## Overview

The schema is designed for **Motherduck** (cloud-hosted DuckDB) with the following optimizations:

- **Storage**: ~2.5 GB for 2 years of monthly snapshots
- **Per snapshot**: ~100 MB (Parquet with ZSTD compression)
- **Temporal tracking**: Current + monthly snapshots approach
- **No indexes**: Motherduck doesn't use them for query acceleration

## Schema Files

Execute in this order:

### Core Tables

1. **01_enterprises.sql** (1.9M rows, 5 MB)
   - Core enterprise entity
   - Denormalized primary name (NL/FR/DE)
   - Code-only storage for descriptions

2. **02_establishments.sql** (1.7M rows, 4 MB)
   - Establishment units (physical locations)
   - Linked to enterprises

3. **03_denominations.sql** (3.3M rows, 8 MB)
   - ALL business names (link table)
   - Multi-language support
   - Multiple names per entity

4. **04_addresses.sql** (2.8M rows, 11 MB)
   - Address data (link table)
   - 40% of enterprises have no address
   - Multi-language components

5. **05_activities.sql** (36M rows, 71 MB) ⚠️ **CRITICAL**
   - Activity links (link table)
   - **Saves 90% storage** vs denormalized
   - JOIN to nace_codes for descriptions

6. **07_contacts.sql** (0.7M rows, 1 MB)
   - Contact details (phone, email, web)
   - Link table

7. **08_branches.sql** (7K rows, <1 MB)
   - Foreign entity branch offices
   - Address denormalized (small dataset)

### Lookup Tables (Static)

6. **06_nace_codes.sql** (7K rows, <1 MB)
   - NACE code descriptions
   - Loaded once from code.csv
   - Multi-language (NL/FR/DE/EN)

9. **09_codes.sql** (21K rows, <1 MB)
   - All code category descriptions
   - Multi-language (NL/FR/DE only)
   - Runtime JOIN for human-readable text

### Metadata

10. **10_import_jobs.sql**
    - Import operation tracking
    - Statistics and status

## Initialization

### Master Script

**00_init.sql** - Master initialization script with:
- All table comments
- Convenience views (enterprises_current, etc.)
- Documentation and notes

### Usage

```typescript
import { loadAllSchemas, loadInitScript } from '@/lib/sql'

// Load all schema files
const schemas = await loadAllSchemas()

// Or load master init script
const initSql = await loadInitScript()
```

## Design Principles

### 1. Link Table Approach

**Activities** (36M rows) uses link table:
- Stores only: entity_number, activity_group, nace_code, classification
- NACE descriptions stored ONCE in nace_codes table
- **Savings**: 1.5 GB CSV → 71 MB Parquet (21x compression)

### 2. Temporal Tracking

All data tables include:
- `_snapshot_date` - Date of data snapshot
- `_extract_number` - Extract number from KBO
- `_is_current` - TRUE for current, FALSE for historical

**Strategy**: Current + Monthly snapshots only
- Daily updates modify current snapshot in-place
- Monthly full imports create new historical snapshot
- Saves ~90% vs daily granularity

### 3. Code-Only Storage

Core tables store ONLY codes, not descriptions:

```sql
-- Enterprises table
enterprises.juridical_form = "030"  -- code

-- Runtime JOIN for description
SELECT e.*, c.description AS juridical_form_desc
FROM enterprises e
LEFT JOIN codes c
  ON c.category = 'JuridicalForm'
  AND c.code = e.juridical_form
  AND c.language = 'NL'  -- or FR, DE based on user preference
```

**Benefits**:
- Storage efficient
- Flexible (user can switch language)
- Complete (all 3-4 languages supported)

### 4. Multi-Language Support

**Language Codes** (in denomination.csv):
- 0 = Unknown (25%)
- 1 = FR/French (28%)
- 2 = NL/Dutch (45% - most common)
- 3 = DE/German (0.5%)
- 4 = EN/English (0.6%)

**Code Descriptions** (in code.csv):
- Available in: NL, FR, DE
- English NOT available in code.csv

**Addresses**:
- Separate columns per language
- `street_nl`, `street_fr`, `municipality_nl`, `municipality_fr`

### 5. No Indexes

Per Motherduck documentation:
> "While the syntax is supported, indexes are not currently utilized for query acceleration in MotherDuck. Indexes can significantly slow down INSERT operations without any corresponding advantages."

**Query performance comes from**:
- Column pruning (columnar storage)
- Predicate pushdown
- Partition pruning (_is_current filter)
- Parquet file statistics

## Convenience Views

Current snapshot views (filter `_is_current = true`):

- `enterprises_current`
- `establishments_current`
- `denominations_current`
- `addresses_current`
- `activities_current`
- `contacts_current`
- `branches_current`

Usage:
```sql
-- Instead of:
SELECT * FROM enterprises WHERE _is_current = true

-- Use:
SELECT * FROM enterprises_current
```

## Common Query Patterns

### 1. Get Enterprise with Details

```sql
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  e.status,
  c_status.description AS status_desc,
  e.juridical_form,
  c_jur.description AS juridical_form_desc,
  -- Address (may be NULL)
  a.zipcode,
  a.municipality_nl,
  a.street_nl,
  -- Main activity (may be NULL)
  act.nace_code,
  n.description_nl as activity_desc
FROM enterprises_current e
LEFT JOIN codes c_status
  ON c_status.category = 'Status'
  AND c_status.code = e.status
  AND c_status.language = 'NL'
LEFT JOIN codes c_jur
  ON c_jur.category = 'JuridicalForm'
  AND c_jur.code = e.juridical_form
  AND c_jur.language = 'NL'
LEFT JOIN addresses a
  ON e.enterprise_number = a.entity_number
  AND a.type_of_address = 'REGO'
  AND a._is_current = true
LEFT JOIN activities act
  ON e.enterprise_number = act.entity_number
  AND act.classification = 'MAIN'
  AND act.activity_group = '003'
  AND act.nace_version = '2025'
  AND act._is_current = true
LEFT JOIN nace_codes n
  ON act.nace_code = n.nace_code
  AND act.nace_version = n.nace_version
WHERE e.enterprise_number = '0200.065.765';
```

### 2. Search by NACE Code

```sql
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  a.zipcode,
  a.municipality_nl,
  n.description_nl as activity
FROM enterprises_current e
JOIN activities act
  ON e.enterprise_number = act.entity_number
  AND act._is_current = true
JOIN nace_codes n
  ON act.nace_code = n.nace_code
  AND act.nace_version = n.nace_version
LEFT JOIN addresses a
  ON e.enterprise_number = a.entity_number
  AND a.type_of_address = 'REGO'
  AND a._is_current = true
WHERE act.nace_code = '84130'
  AND act.nace_version = '2025'
  AND e.status = 'AC'
LIMIT 100;
```

### 3. Historical Query (Time-series)

```sql
-- Show enterprise changes over time
SELECT
  _snapshot_date,
  primary_name_nl,
  status,
  juridical_form
FROM enterprises
WHERE enterprise_number = '0200.065.765'
ORDER BY _snapshot_date DESC;
```

## Migrations

Future schema changes will be tracked in `migrations/` directory with timestamp-based naming:

```
migrations/
  2025_10_15_001_add_column_foo.sql
  2025_10_15_002_create_index_bar.sql
```

## Related Documentation

- Implementation guide: `/docs/IMPLEMENTATION_GUIDE.md`
- Progress tracking: `/docs/PROGRESS.md`
- TypeScript types: `/lib/types/`
