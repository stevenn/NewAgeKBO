# KBO Open Data - Revised Schema Design

**Date**: 2025-10-13
**Based on**: Data analysis findings + Parquet compression tests

---

## Summary of Key Decisions

1. **Storage format**: Parquet with ZSTD compression (21x compression)
2. **Retention strategy**: Current + Monthly snapshots only
3. **Activity storage**: Link table approach (10x storage reduction)
4. **Address storage**: Link table (40% of enterprises have no address)
5. **Denomination**: Denormalize primary name only
6. **Monthly processing**: Local DuckDB → Parquet → Motherduck
7. **Daily processing**: Vercel → Motherduck (small updates)

---

## Compression Test Results

**activity.csv** (1.5GB CSV → 36M rows):

| Compression | Size | Ratio | Write Time | Read Time | Recommendation |
|-------------|------|-------|------------|-----------|----------------|
| ZSTD | 71MB | 21.6x | 1.65s | 0.001s | **Use for storage** |
| GZIP | 73MB | 21.2x | 2.40s | 0.002s | Slower, similar compression |
| Snappy | 114MB | 13.6x | 1.62s | 0.001s | Use for temp files |
| Uncompressed | 536MB | 2.9x | 1.56s | 0.001s | Not recommended |

**Key insight**: ZSTD compression reduces 1.5GB → 71MB (95% savings) with negligible performance impact.

**Extrapolation to full dataset**:
- CSV total: ~2.1GB (all files)
- Parquet (ZSTD): ~100MB per snapshot
- 24 monthly snapshots: 2.4GB (instead of 50GB CSV)

---

## Data Scope & Limitations

### What's Included in KBO Open Data

The schema below covers **all 9 CSV files** available in the KBO Open Data dataset:

✅ **enterprise.csv** → Core enterprise information
✅ **establishment.csv** → Establishment units
✅ **denomination.csv** → Business names (all languages & types)
✅ **address.csv** → Addresses (legal persons + establishments)
✅ **activity.csv** → Economic activities (NACE codes)
✅ **contact.csv** → Contact details (phone, email, web)
✅ **branch.csv** → Branch offices of foreign entities
✅ **code.csv** → Lookup tables (multilingual descriptions)
✅ **meta.csv** → Extract metadata (snapshot date, version)

### What's NOT in KBO Open Data

Several data types that third-party services (like kbodata.app API) provide are **sourced from external systems** and are NOT available in the KBO Open Data:

❌ **Financial data** - NOT in KBO Open Data
- Paid-in capital amounts
- Fiscal year start/end dates
- Annual assembly dates
- **Source**: Belgian National Bank, Official Gazette publications

❌ **NSSO/RSZ data** - NOT in KBO Open Data
- Employer numbers (RSZ-nummer, ONSS number)
- Employee count ranges
- Sector codes
- **Source**: NSSO (National Social Security Office) database - separate system

❌ **Board members & roles** - NOT in KBO Open Data
- Directors, managers, mandates
- Function titles and dates
- Appointment/resignation dates
- **Source**: Official Gazette (Moniteur Belge/Belgisch Staatsblad), notarial acts

❌ **Real-time VAT validation** - NOT in KBO Open Data
- EU VIES VAT number validation
- **Source**: European Commission VIES service

❌ **Historical financial statements** - NOT in KBO Open Data
- Annual accounts, balance sheets
- **Source**: National Bank of Belgium

### Schema Design Philosophy

Our schema is designed to:
1. **Fully utilize** all data available in KBO Open Data (9 CSV files)
2. **Support temporal tracking** of changes over time (monthly granularity)
3. **Optimize storage** via link tables and Parquet compression
4. **Enable extensibility** for future external data integrations

---

## Multi-Language Strategy

### Language Code Mapping (CRITICAL)

**KBO Open Data uses these language codes:**

| Code | Language | CSV Field Value |
|------|----------|-----------------|
| 0 | Unknown | `"0"` |
| 1 | French  | `"1"` |
| 2 | Dutch   | `"2"` |
| 3 | German  | `"3"` |
| 4 | English | `"4"` |

**⚠️ Common Mistake:** Do NOT assume 0=FR, 1=DE. The actual mapping is 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN.

### Multi-Language Data Patterns

**1. Denominations (Enterprise/Establishment Names)**
- Stored in separate rows per language
- Language code in `Language` field (0-4)
- **Strategy**: Store ALL in `denominations` table, denormalize primary name (NL/FR/DE) into enterprises table

**2. Addresses**
- Multi-language fields within same row: `CountryNL`, `CountryFR`, `MunicipalityNL`, `MunicipalityFR`, etc.
- **Strategy**: Store all language variants in columns

**3. Code Descriptions**
- Stored in `code.csv` with one row per (Category, Code, Language) combination
- All descriptions available in NL, FR, DE, EN (sometimes only NL/FR)
- **Strategy**: Store in `codes` table, JOIN at query time based on user's language preference

### Implementation Patterns

**Pattern 1: Store Codes, JOIN for Descriptions**
```sql
-- Enterprises table stores ONLY codes
enterprises.juridical_form = "030"  -- code

-- codes table has descriptions
codes WHERE category='JuridicalForm' AND code='030' AND language='NL'
  → "Buitenlandse entiteit"
codes WHERE category='JuridicalForm' AND code='030' AND language='FR'
  → "Entité étrangère"

-- Runtime JOIN based on user language
SELECT e.*, c.description AS juridical_form_desc
FROM enterprises e
LEFT JOIN codes c
  ON c.category = 'JuridicalForm'
  AND c.code = e.juridical_form
  AND c.language = :user_language
```

**Benefits:**
- Flexible: User can switch language without data changes
- Storage efficient: No duplicate descriptions
- Complete: All 4 languages supported (NL/FR/DE/EN)

**Pattern 2: Denormalize Primary Name Only**
```sql
-- Enterprises table stores top 3 languages
enterprises.primary_name_nl = "Intergemeentelijke Vereniging Veneco"
enterprises.primary_name_fr = NULL  -- if not available
enterprises.primary_name_de = NULL  -- for German-speaking regions

-- Fallback logic in queries
COALESCE(primary_name_nl, primary_name_fr, primary_name_de) AS name
```

**Pattern 3: Address Multi-Language Columns**
```sql
-- Addresses table has separate columns per language
addresses.street_nl = "Panhuisstraat"
addresses.street_fr = "Panhuisstraat"  -- often same for Flemish cities
addresses.municipality_nl = "Destelbergen"
addresses.municipality_fr = "Destelbergen"
```

### Language Fallback Strategy

**Recommended logic for displaying enterprise information:**

```typescript
function getEnterpriseName(enterprise: Enterprise, userLang: 'NL' | 'FR' | 'DE' | 'EN'): string {
  // Priority 1: User's preferred language
  if (userLang === 'NL' && enterprise.primary_name_nl) return enterprise.primary_name_nl;
  if (userLang === 'FR' && enterprise.primary_name_fr) return enterprise.primary_name_fr;
  if (userLang === 'DE' && enterprise.primary_name_de) return enterprise.primary_name_de;

  // Priority 2: Fallback to Dutch (most common)
  if (enterprise.primary_name_nl) return enterprise.primary_name_nl;

  // Priority 3: Fallback to French
  if (enterprise.primary_name_fr) return enterprise.primary_name_fr;

  // Priority 4: Any available
  return enterprise.primary_name_de || '[No name available]';
}
```

### Code Descriptions Coverage

**All code categories support 4 languages:**
- JuridicalForm: NL ✓ FR ✓ DE ✓ EN ✗ (usually only NL/FR/DE)
- JuridicalSituation: NL ✓ FR ✓ DE ✓ EN ✗
- ActivityGroup: NL ✓ FR ✓ DE ✗ EN ✗ (usually only NL/FR)
- TypeOfAddress: NL ✓ FR ✓ DE ✗ EN ✗
- NACE codes: NL ✓ FR ✓ DE ✓ EN ✓ (fully multilingual)

**Fallback**: If user's language not available, fall back to NL → FR → first available.

---

## Final Schema Design

### Core Tables

#### 1. Enterprises (Core - Codes Only)
```sql
CREATE TABLE enterprises (
  enterprise_number VARCHAR PRIMARY KEY,

  -- Basic info (codes only - descriptions via JOIN to codes table)
  status VARCHAR,  -- AC (active) or ST (stopped)
  juridical_situation VARCHAR,  -- code e.g., "000"
  type_of_enterprise VARCHAR,   -- 1=natural person, 2=legal person
  juridical_form VARCHAR,        -- code e.g., "030"
  juridical_form_cac VARCHAR,    -- code
  start_date DATE,

  -- Primary denomination (denormalized - always exists, 100% coverage)
  primary_name_nl VARCHAR NOT NULL,
  primary_name_fr VARCHAR,
  primary_name_de VARCHAR,       -- Added for German-speaking regions
  primary_name_type VARCHAR,     -- 001, 002, 003, 004

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN  -- true for current snapshot, false for historical
);
```

**Rationale**:
- **Store codes only**, not descriptions (flexible, saves space)
- **JOIN to codes table** at query time for user's preferred language
- Denormalize primary name for fast search (100% coverage)
- Keep basic fields for filtering (status, juridical form codes)
- Support 3 languages in primary name (NL/FR/DE) for diverse regions

#### 2. Establishments (Denormalized Core)
```sql
CREATE TABLE establishments (
  establishment_number VARCHAR PRIMARY KEY,
  enterprise_number VARCHAR NOT NULL,
  start_date DATE,

  -- Primary name (if different from enterprise)
  commercial_name VARCHAR,

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN
);
```

#### 3. Denominations (All Names - Link Table)
```sql
CREATE TABLE denominations (
  id UUID PRIMARY KEY,
  entity_number VARCHAR NOT NULL,  -- enterprise or establishment number
  entity_type VARCHAR NOT NULL,  -- 'enterprise' or 'establishment'
  denomination_type VARCHAR NOT NULL,  -- 001, 002, 003, 004
  language VARCHAR NOT NULL,  -- 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN
  denomination VARCHAR NOT NULL,

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN
);
```

**Rationale**:
- Store ALL denominations (not just primary)
- Enable full-text search across all names
- Support multi-language display

#### 4. Addresses (Link Table)
```sql
CREATE TABLE addresses (
  id UUID PRIMARY KEY,
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,  -- 'enterprise' or 'establishment'
  type_of_address VARCHAR NOT NULL,  -- REGO, BAET, ABBR, OBAD

  -- Address components (multi-language)
  country_nl VARCHAR,
  country_fr VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  extra_address_info VARCHAR,
  date_striking_off DATE,

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN
);
```

**Rationale**:
- 40% of enterprises have no address (natural persons)
- Establishments have their own addresses (BAET type)
- Separate table avoids massive NULLs

#### 5. NACE Codes (Lookup Table - Static)
```sql
CREATE TABLE nace_codes (
  nace_version VARCHAR NOT NULL,  -- 2003, 2008, 2025
  nace_code VARCHAR NOT NULL,
  description_nl VARCHAR,
  description_fr VARCHAR,
  description_de VARCHAR,
  description_en VARCHAR,
  PRIMARY KEY (nace_version, nace_code)
);

-- Load once from code.csv, never changes per snapshot
```

**Rationale**:
- 7,265 unique NACE codes across 3 versions
- Descriptions are 150-200 chars each
- Storing separately avoids repeating 36M times

#### 6. Activities (Link Table - CRITICAL)
```sql
CREATE TABLE activities (
  id UUID PRIMARY KEY,
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,  -- 'enterprise' or 'establishment'
  activity_group VARCHAR NOT NULL,  -- 001-007
  nace_version VARCHAR NOT NULL,
  nace_code VARCHAR NOT NULL,
  classification VARCHAR NOT NULL,  -- MAIN, SECO, ANCI

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN,

  FOREIGN KEY (nace_version, nace_code) REFERENCES nace_codes(nace_version, nace_code)
);
```

**Rationale**:
- **36M activity records** per snapshot
- **Storage reduction**: 1.5GB CSV → 71MB Parquet (with ZSTD)
- NACE descriptions stored once (not 36M times)
- Fast JOIN on (nace_version, nace_code)

**Storage comparison**:
- Denormalized: 36M × 500 bytes = 18GB per snapshot
- Link table: 36M × 50 bytes = 1.8GB per snapshot + 1.5MB codes
- **Savings**: 90% storage reduction

#### 7. Contacts (Link Table)
```sql
CREATE TABLE contacts (
  id UUID PRIMARY KEY,
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,  -- 'enterprise' or 'establishment'
  contact_type VARCHAR NOT NULL,  -- TEL, EMAIL, WEB, etc.
  contact_value VARCHAR NOT NULL,

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN
);
```

#### 8. Branches (Foreign Entities)
```sql
CREATE TABLE branches (
  id VARCHAR PRIMARY KEY,
  enterprise_number VARCHAR,
  start_date DATE,
  branch_name VARCHAR,

  -- Address (denormalized - branches are rare, only 7k)
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN
);
```

**Rationale**: Only 7,326 branches total, so denormalization is fine.

#### 9. Code Lookup Table (Static)
```sql
CREATE TABLE codes (
  category VARCHAR NOT NULL,  -- JuridicalForm, JuridicalSituation, etc.
  code VARCHAR NOT NULL,
  language VARCHAR NOT NULL,  -- NL, FR, DE, EN
  description VARCHAR NOT NULL,
  PRIMARY KEY (category, code, language)
);

-- Load once from code.csv, 21,501 rows
```

#### 10. Import Jobs (Metadata)
```sql
CREATE TABLE import_jobs (
  id UUID PRIMARY KEY,
  extract_number INTEGER UNIQUE,
  extract_type VARCHAR,  -- 'full' or 'update'
  snapshot_date DATE,
  extract_timestamp TIMESTAMP,
  status VARCHAR,  -- 'pending', 'running', 'completed', 'failed'
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message TEXT,
  records_processed BIGINT,
  records_inserted BIGINT,
  records_updated BIGINT,
  records_deleted BIGINT,
  worker_type VARCHAR  -- 'local' or 'vercel'
);
```

---

## Temporal Tracking Strategy

### Approach: Current + Monthly Snapshots

**Current snapshot** (`_is_current = true`):
- Always represents the latest state
- Updated daily with incremental changes
- Queries default to `WHERE _is_current = true`
- Size: ~100MB (Parquet, ZSTD)

**Monthly snapshots** (`_is_current = false`):
- Created on first Sunday of each month
- Immutable historical records
- Marked with `_snapshot_date` and `_extract_number`
- Size per month: ~100MB
- Retention: 24 months = 2.4GB

**Total storage (2 years)**: ~2.5GB

### Schema Views

```sql
-- Current data (default for queries)
CREATE VIEW enterprises_current AS
SELECT * FROM enterprises WHERE _is_current = true;

CREATE VIEW activities_current AS
SELECT * FROM activities WHERE _is_current = true;

-- Historical snapshot at specific date
CREATE VIEW enterprises_snapshot(snapshot_date DATE) AS
SELECT * FROM enterprises
WHERE _snapshot_date = snapshot_date;

-- Time-series query (show enterprise changes over time)
SELECT
  enterprise_number,
  _snapshot_date,
  primary_name_nl,
  status
FROM enterprises
WHERE enterprise_number = '0200.065.765'
ORDER BY _snapshot_date;
```

---

## Data Processing Pipelines

### Pipeline 1: Monthly Full Import (Local)

**Trigger**: Manual or cron (first Sunday of month)
**Environment**: Local machine with DuckDB
**Duration**: ~5 minutes for full dataset

```bash
#!/bin/bash
# monthly-import.sh

EXTRACT_DIR="$1"  # e.g., KboOpenData_0140_2025_10_05_Full

# Step 1: Load CSVs into DuckDB
duckdb kbo.duckdb <<EOF
-- Mark all current records as historical
UPDATE enterprises SET _is_current = false;
UPDATE activities SET _is_current = false;
-- ... (all tables)

-- Load new data
COPY enterprises FROM '$EXTRACT_DIR/enterprise.csv' (FORMAT CSV, HEADER TRUE);
COPY activities FROM '$EXTRACT_DIR/activity.csv' (FORMAT CSV, HEADER TRUE);
-- ... (all files)

-- Apply transformations (denormalization, primary selection)
-- See section below

-- Mark new records as current
UPDATE enterprises SET _is_current = true WHERE _extract_number = 140;
-- ... (all tables)
EOF

# Step 2: Export to Parquet with ZSTD compression
duckdb kbo.duckdb <<EOF
COPY enterprises TO 'output/enterprises_140.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
COPY activities TO 'output/activities_140.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
-- ... (all tables)
EOF

# Step 3: Upload to Motherduck
# (Implementation depends on Motherduck API/CLI)
```

### Pipeline 2: Daily Incremental Update (Vercel)

**Trigger**: Vercel cron (daily, 8am)
**Environment**: Vercel function → Motherduck
**Duration**: <1 minute

```typescript
// app/api/import-daily/route.ts
export async function POST(req: Request) {
  const { extractNumber } = await req.json();

  // Download update ZIP from KBO API
  const updateFiles = await downloadUpdateZip(extractNumber);

  // Connect to Motherduck
  const db = await connectMotherduck();

  // Process deletes
  for (const entityNumber of updateFiles.enterprise_delete) {
    await db.exec(`
      UPDATE enterprises
      SET _is_current = false, _snapshot_date = CURRENT_DATE
      WHERE enterprise_number = ? AND _is_current = true
    `, [entityNumber]);

    // Cascade to related tables
    await db.exec(`
      UPDATE activities
      SET _is_current = false, _snapshot_date = CURRENT_DATE
      WHERE entity_number = ? AND _is_current = true
    `, [entityNumber]);
    // ... (addresses, denominations, contacts)
  }

  // Process inserts
  for (const enterprise of updateFiles.enterprise_insert) {
    await db.exec(`
      INSERT INTO enterprises (..., _is_current, _extract_number)
      VALUES (..., true, ?)
    `, [extractNumber]);
  }

  // Log job status
  await db.exec(`
    INSERT INTO import_jobs (id, extract_number, status, ...)
    VALUES (?, ?, 'completed', ...)
  `, [generateUUID(), extractNumber]);

  return Response.json({ success: true });
}
```

---

## Transformation Logic

### Primary Denomination Selection

```typescript
interface Denomination {
  EntityNumber: string;
  Language: string;  // 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN
  TypeOfDenomination: string;  // 001, 002, 003, 004
  Denomination: string;
}

function selectPrimaryDenomination(denominations: Denomination[]): {
  primary_name_nl: string | null;
  primary_name_fr: string | null;
  primary_name_type: string;
} {
  // Priority order
  const priorities = [
    { type: '001', lang: '2' },  // Legal name, Dutch
    { type: '001', lang: '1' },  // Legal name, French
    { type: '003', lang: '2' },  // Commercial name, Dutch
    { type: '003', lang: '1' },  // Commercial name, French
  ];

  let primaryNL = null;
  let primaryFR = null;
  let primaryType = null;

  for (const priority of priorities) {
    const match = denominations.find(
      d => d.TypeOfDenomination === priority.type && d.Language === priority.lang
    );
    if (match) {
      if (match.Language === '2') primaryNL = match.Denomination;
      if (match.Language === '0') primaryFR = match.Denomination;
      primaryType = match.TypeOfDenomination;
      break;
    }
  }

  // Fallback: use any denomination
  if (!primaryNL && !primaryFR) {
    const fallback = denominations[0];
    if (fallback) {
      primaryNL = fallback.Denomination;
      primaryType = fallback.TypeOfDenomination;
    }
  }

  return { primary_name_nl: primaryNL, primary_name_fr: primaryFR, primary_name_type: primaryType };
}
```

### DuckDB Implementation (SQL)

```sql
-- Create temp table with ranked denominations
CREATE TEMP TABLE ranked_denominations AS
SELECT
  EntityNumber,
  Language,
  TypeOfDenomination,
  Denomination,
  ROW_NUMBER() OVER (
    PARTITION BY EntityNumber
    ORDER BY
      CASE TypeOfDenomination
        WHEN '001' THEN 1  -- Legal name
        WHEN '003' THEN 2  -- Commercial name
        ELSE 3
      END,
      CASE Language
        WHEN '2' THEN 1  -- Dutch
        WHEN '1' THEN 2  -- French
        ELSE 3
      END
  ) as priority_rank
FROM read_csv('denomination.csv', AUTO_DETECT=TRUE);

-- Insert into enterprises with primary denomination
INSERT INTO enterprises (
  enterprise_number,
  primary_name_nl,
  primary_name_fr,
  primary_name_type,
  ...
)
SELECT
  e.EnterpriseNumber,
  MAX(CASE WHEN d.Language = '2' THEN d.Denomination END) as primary_name_nl,
  MAX(CASE WHEN d.Language = '1' THEN d.Denomination END) as primary_name_fr,
  MAX(d.TypeOfDenomination) as primary_name_type,
  ...
FROM read_csv('enterprise.csv', AUTO_DETECT=TRUE) e
LEFT JOIN ranked_denominations d
  ON e.EnterpriseNumber = d.EntityNumber
  AND d.priority_rank = 1
GROUP BY e.EnterpriseNumber, ...;
```

---

## Query Patterns

### Common Queries

#### 1. Search enterprises by name (current) - With Code Descriptions
```sql
-- User prefers Dutch (NL)
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  e.primary_name_fr,
  e.status,
  c_status.description AS status_desc,
  e.juridical_form,
  c_jur.description AS juridical_form_desc
FROM enterprises e
LEFT JOIN codes c_status
  ON c_status.category = 'Status'
  AND c_status.code = e.status
  AND c_status.language = 'NL'
LEFT JOIN codes c_jur
  ON c_jur.category = 'JuridicalForm'
  AND c_jur.code = e.juridical_form
  AND c_jur.language = 'NL'
WHERE e._is_current = true
  AND (e.primary_name_nl ILIKE '%veneco%' OR e.primary_name_fr ILIKE '%veneco%')
LIMIT 100;

-- Or use a user language parameter
SELECT
  e.enterprise_number,
  COALESCE(e.primary_name_nl, e.primary_name_fr, e.primary_name_de) AS name,
  e.status,
  c_jur.description AS juridical_form_desc
FROM enterprises e
LEFT JOIN codes c_jur
  ON c_jur.category = 'JuridicalForm'
  AND c_jur.code = e.juridical_form
  AND c_jur.language = :user_language  -- 'NL', 'FR', 'DE', or 'EN'
WHERE e._is_current = true
LIMIT 100;
```

#### 2. Get enterprise with address and main activity
```sql
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  e.status,
  -- Address (may be NULL)
  a.zipcode,
  a.municipality_nl,
  a.street_nl,
  a.house_number,
  -- Main activity (may be NULL)
  act.nace_code,
  n.description_nl as activity_desc
FROM enterprises_current e
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

#### 3. Search by NACE code
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

#### 4. Search by location
```sql
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  a.street_nl,
  a.house_number,
  e.juridical_form_desc_nl
FROM enterprises_current e
JOIN addresses a
  ON e.enterprise_number = a.entity_number
  AND a._is_current = true
WHERE a.zipcode = '9000'
  AND a.municipality_nl = 'Gent'
  AND e.status = 'AC'
ORDER BY e.primary_name_nl
LIMIT 100;
```

#### 5. Time-series query (historical)
```sql
-- Show enterprise changes over time
SELECT
  _snapshot_date,
  primary_name_nl,
  status,
  juridical_form_desc_nl
FROM enterprises
WHERE enterprise_number = '0200.065.765'
ORDER BY _snapshot_date DESC;
```

#### 6. Get all activities for enterprise (not just main)
```sql
SELECT
  act.classification,
  act.activity_group,
  act.nace_version,
  act.nace_code,
  n.description_nl
FROM activities act
JOIN nace_codes n
  ON act.nace_code = n.nace_code
  AND act.nace_version = n.nace_version
WHERE act.entity_number = '0200.065.765'
  AND act._is_current = true
ORDER BY
  CASE act.classification WHEN 'MAIN' THEN 1 WHEN 'SECO' THEN 2 ELSE 3 END,
  act.nace_version DESC;
```

---

## Query Performance Strategy

### Why No Indexes?

**Critical Understanding: Motherduck does NOT use indexes for query acceleration.**

From Motherduck documentation:
> "While the syntax is supported, indexes are not currently utilized for query acceleration in MotherDuck. Indexes can significantly slow down INSERT operations without any corresponding advantages."

**Therefore: DO NOT create indexes in this schema.**

### Architecture Optimization

**Local DuckDB (Monthly ETL)**:
- One-pass CSV → transformation → Parquet pipeline
- No complex queries during processing
- **Indexes not needed** - would only slow down inserts

**Motherduck (Production)**:
- Columnar Parquet storage already optimized for analytical queries
- Query performance comes from:
  - **Column pruning** (only read needed columns)
  - **Predicate pushdown** (filter during scan)
  - **Partition pruning** (filter by `_is_current`, `_snapshot_date`)
- **Indexes not used** - would waste storage and slow down daily updates

### Efficient Query Patterns

Write queries that work well with columnar storage:

```sql
-- ✅ GOOD: Equality filters (very fast with columnar storage)
SELECT * FROM enterprises
WHERE enterprise_number = '0200.065.765'
  AND _is_current = true;

-- ✅ GOOD: Prefix search (columnar min/max statistics help)
SELECT enterprise_number, primary_name_nl
FROM enterprises
WHERE primary_name_nl LIKE 'ABC%'
  AND _is_current = true
LIMIT 100;

-- ✅ GOOD: Filter on indexed columns first
SELECT e.*, a.zipcode
FROM enterprises e
JOIN addresses a ON e.enterprise_number = a.entity_number
WHERE e._is_current = true  -- Filter small dataset first
  AND a.zipcode = '9000'
  AND a._is_current = true;

-- ⚠️ SLOWER: Case-insensitive substring search (full scan)
SELECT * FROM enterprises
WHERE lower(primary_name_nl) LIKE '%veneco%'
  AND _is_current = true;

-- ⚠️ SLOWER: Multi-table JOINs with substring search
-- Consider external search service for this use case
```

### Search Strategies by Use Case

**1. Exact Lookups (Enterprise Number, Zipcode, NACE Code)**
- ✅ Use direct SQL queries to Motherduck
- Very fast with columnar storage (equality filters)
- No additional infrastructure needed

**2. Prefix Search (Name starts with "ABC")**
- ✅ Use SQL with `LIKE 'ABC%'`
- Acceptable performance with columnar min/max statistics
- Good for dropdown autocomplete with known prefix

**3. Full-Text Search (Any word in name)**
- ❌ Don't use SQL ILIKE '%keyword%' in production (slow)
- ✅ Use external search service:
  - **Elasticsearch / OpenSearch** - Industry standard, full-featured
  - **Typesense** - Open source, typo-tolerant, fast
  - **MeiliSearch** - Lightweight, great developer experience
  - Sync data from Motherduck via nightly job
  - Return enterprise numbers, then JOIN in Motherduck for full details

**4. Fuzzy Search (Typo tolerance)**
- ❌ Can't do efficiently in SQL
- ✅ Use client-side library for small datasets:
  - **Fuse.js** - Fuzzy search in browser
  - **MiniSearch** - Lightweight full-text in browser
  - Load current enterprise names (~2M rows × 50 bytes = 100MB) into app memory
  - Good for internal tools with infrequent data refresh

**5. Faceted Search (Filter by location + activity + status)**
- ✅ Use SQL with multiple WHERE clauses
- Columnar storage handles multi-column filters well
- Example:
```sql
SELECT e.enterprise_number, e.primary_name_nl
FROM enterprises e
JOIN addresses a ON e.enterprise_number = a.entity_number
JOIN activities act ON e.enterprise_number = act.entity_number
WHERE e.status = 'AC'
  AND a.zipcode = '9000'
  AND act.nace_code = '84130'
  AND e._is_current = true
  AND a._is_current = true
  AND act._is_current = true;
```

### Recommended Architecture

**For production deployment:**

```
┌─────────────────────────────────────────────────────────────┐
│                        User Request                          │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
         ┌─────────────────────────────┐
         │   Next.js API Routes        │
         │   (Vercel Edge Functions)   │
         └─────────────┬───────────────┘
                       │
         ┌─────────────┴────────────────┐
         │                              │
         ▼                              ▼
┌─────────────────┐           ┌──────────────────┐
│  Motherduck     │           │  Typesense       │
│  (Structured    │           │  (Full-Text      │
│   Queries)      │           │   Search)        │
└─────────────────┘           └──────────────────┘
         │                              │
         │    Nightly Sync Job          │
         └──────────────►───────────────┘
```

**Query routing logic:**
```typescript
// Exact lookup → Motherduck
if (query.enterpriseNumber) {
  return motherduck.query(`SELECT * FROM enterprises WHERE enterprise_number = ?`, [query.enterpriseNumber]);
}

// Full-text search → Typesense → Motherduck
if (query.searchText && query.searchText.length > 0) {
  const results = await typesense.search({ q: query.searchText, query_by: 'primary_name_nl,primary_name_fr' });
  const enterpriseNumbers = results.hits.map(h => h.document.enterprise_number);
  return motherduck.query(`SELECT * FROM enterprises WHERE enterprise_number IN (?)`, [enterpriseNumbers]);
}

// Faceted filters → Motherduck
if (query.zipcode || query.naceCode || query.status) {
  return motherduck.query(`SELECT ... WHERE zipcode = ? AND status = ?`, [query.zipcode, query.status]);
}
```

### Performance Tuning

**Monitor and optimize:**
1. **Query execution time** - Add logging to Motherduck queries
2. **Column selection** - Only SELECT columns you need
3. **Filter order** - Apply most selective filters first
4. **LIMIT clauses** - Always limit result sets for user-facing queries
5. **Caching** - Cache frequent queries (enterprise lookups, code tables)

**Example optimization:**
```sql
-- ❌ SLOW: Select all columns, filter last
SELECT *
FROM enterprises e
JOIN activities act ON e.enterprise_number = act.entity_number
WHERE lower(e.primary_name_nl) LIKE '%test%';

-- ✅ FAST: Select needed columns, filter first, limit results
SELECT e.enterprise_number, e.primary_name_nl, e.status
FROM enterprises e
WHERE e._is_current = true
  AND e.primary_name_nl LIKE 'Test%'  -- Prefix search
LIMIT 100;
```

### Future Optimizations

**If query performance becomes an issue:**

1. **Materialized views** for common queries:
```sql
CREATE MATERIALIZED VIEW enterprises_with_activity AS
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  e.status,
  act.nace_code,
  n.description_nl
FROM enterprises e
LEFT JOIN activities act ON e.enterprise_number = act.entity_number
LEFT JOIN nace_codes n ON act.nace_code = n.nace_code
WHERE e._is_current = true AND act._is_current = true;
```

2. **Partitioning** by snapshot date (for historical queries):
```sql
CREATE TABLE enterprises (
  ...
) PARTITION BY YEAR(_snapshot_date);
```

3. **Pre-aggregated statistics** table:
```sql
CREATE TABLE enterprise_stats AS
SELECT
  zipcode,
  nace_code,
  COUNT(*) as enterprise_count
FROM enterprises e
JOIN addresses a ON e.enterprise_number = a.entity_number
JOIN activities act ON e.enterprise_number = act.entity_number
WHERE e._is_current = true
GROUP BY zipcode, nace_code;
```

---

## Storage Estimation (Final)

### Per Snapshot (Parquet, ZSTD)
```
enterprises:    1.9M rows × 50 bytes = 95 MB   → Parquet: 5 MB
denominations:  3.3M rows × 50 bytes = 165 MB  → Parquet: 8 MB
addresses:      2.8M rows × 80 bytes = 224 MB  → Parquet: 11 MB
activities:    36.3M rows × 50 bytes = 1.8 GB  → Parquet: 71 MB (measured)
establishments: 1.7M rows × 50 bytes = 85 MB   → Parquet: 4 MB
contacts:       0.7M rows × 40 bytes = 28 MB   → Parquet: 1 MB
branches:       7K rows × 100 bytes = 0.7 MB   → Parquet: <1 MB
nace_codes:     7K rows × 200 bytes = 1.4 MB   → Parquet: <1 MB (static)
codes:         21K rows × 100 bytes = 2.1 MB   → Parquet: <1 MB (static)

TOTAL per snapshot: ~100 MB (Parquet, ZSTD)
```

### 2-Year Retention
```
Current snapshot:      100 MB
Monthly snapshots:     100 MB × 24 = 2.4 GB
Static tables:         2 MB (codes, nace_codes)

TOTAL (2 years):       2.5 GB
```

### Cost Estimation (Motherduck)
Assuming $0.02/GB/month storage:
- 2.5 GB × $0.02 = **$0.05/month** for storage
- Query costs depend on usage (Motherduck uses DuckDB's efficient columnar format)

**Conclusion**: Storage is negligible. Parquet + link tables = massive savings.

---

## Future Integrations (Beyond KBO Open Data)

The following data sources could enhance the platform but are **NOT included in KBO Open Data**:

### 1. Financial Data
- **Sources**: Belgian National Bank (NBB), Official Gazette
- **Data**: Capital amounts, fiscal year dates, annual assembly dates, balance sheets
- **Access**: Requires separate API subscription or data sharing agreement

### 2. NSSO/RSZ Employer Data
- **Source**: NSSO (National Social Security Office)
- **Data**: Employer numbers, employee count ranges, sector codes
- **Access**: Requires formal data sharing agreement, privacy-sensitive

### 3. Board Members & Corporate Roles
- **Sources**: Official Gazette, Notarial acts
- **Data**: Directors, managers, appointment/resignation dates
- **Access**: Web scraping or API, privacy considerations for natural persons

### 4. Real-time VAT Validation
- **Source**: EU VIES (VAT Information Exchange System)
- **Data**: Real-time VAT number validation for cross-border transactions
- **Access**: Public API, rate-limited

### 5. Geographic Enrichment
- **Sources**: StatBel, OpenStreetMap, BeST Address Registry
- **Data**: Geolocation coordinates, statistical sectors, NIS codes
- **Access**: Public APIs, one-time geocoding batch job

### 6. Credit Ratings & Risk Data
- **Sources**: Graydon, Creditsafe, other commercial providers
- **Data**: Credit scores, payment behavior, bankruptcy predictions
- **Access**: Paid subscriptions, legal restrictions on redistribution

**Note**: These integrations would require separate implementation phases, data agreements, and schema extensions. The current platform focuses exclusively on KBO Open Data (9 CSV files).

---

## Next Steps

1. ✅ Schema design complete
2. ⏳ Implement CSV → DuckDB loader with transformations
3. ⏳ Test full import pipeline locally
4. ⏳ Set up Motherduck account and test upload
5. ⏳ Build Next.js API routes for querying
6. ⏳ Build admin UI for job monitoring
7. ⏳ Deploy to Vercel

---

**See ANALYSIS.md for detailed findings and DATA_FINDINGS.md for raw data patterns.**
# KBO Open Data - Data Analysis Findings

**Date**: 2025-10-13
**Dataset Analyzed**: Extract 140 (full) + Extract 147 (update)
**Analysis Tool**: DuckDB

---

## Executive Summary

**CRITICAL FINDINGS**:
1. **40% of enterprises have NO address** (776k out of 1.9M)
2. **35% of enterprises have NO MAIN activity** (685k out of 1.9M)
3. **ALL enterprises have at least one denomination** ✓
4. **Daily update rate**: ~156 changes/day (1,091 changes over 7 days)
5. **Activity storage opportunity**: Link table can reduce storage by ~10x

---

## 1. Denomination Patterns

### TypeOfDenomination Codes
| Code | Description (NL) | Description (FR) | Count (NL) | Count (FR) |
|------|------------------|------------------|------------|------------|
| 001  | Naam             | Dénomination | 694,839 | 820,989 |
| 002  | Afkorting        | Abréviation | 47,777 | 5,464 |
| 003  | Commerciële naam | Dénomination commerciale | 750,835 | 12,983 |
| 004  | Naam van het bijkantoor | Dénomination de la succursale | 87 | 10 |

### Key Insights
- **Type 001 (Legal Name)** is most common: 1.9M rows across all languages
- **Type 003 (Commercial Name)** also very common: 1.3M rows
- **Type 002 (Abbreviation)** less common: 98k rows
- **Type 004 (Branch Name)** rare: only 201 rows

### Language Distribution
- **French (0)**: 839,446 denominations
- **German (1)**: 942,487 denominations
- **Dutch (2)**: 1,493,538 denominations (most common)
- **English (3)**: 15,213 denominations
- **Unknown (4)**: 19,223 denominations

### Multiple Denominations
- Maximum denominations per enterprise: **10** (enterprise 0833.917.314)
- Most enterprises have **multiple denominations** (different types and/or languages)

### **DECISION FOR "PRIMARY" DENOMINATION**:
```
Priority order:
1. Type 001 (Legal Name) in Dutch (Language=2)
2. If no Dutch, use Type 001 in French (Language=0)
3. If no Type 001, use Type 003 (Commercial Name) in Dutch
4. If no Type 003, use Type 003 in French
5. Fallback: ANY denomination available
```

---

## 2. Address Patterns

### TypeOfAddress Codes
| Code | Description (NL) | Description (FR) | Count |
|------|------------------|------------------|-------|
| BAET | Vestigingseenheid | Unité d'établissement | 1,672,490 |
| REGO | Zetel | Siège | 1,161,940 |
| ABBR | Bijkantoor | Succursale | 7,325 |
| OBAD | Oudste actieve vestigingseenheid | Première unité d'établissement active | 1 (code exists but rare) |

### **CRITICAL FINDING**: Not All Enterprises Have Addresses
- **Total enterprises**: 1,938,238
- **Enterprises with address**: 1,161,940 (60%)
- **Enterprises WITHOUT address**: 776,298 (40%)

### Interpretation
- **REGO (Zetel/Siège)** = Registered office address (enterprise level)
- **BAET (Vestigingseenheid)** = Establishment unit address (establishment level)
- **ABBR (Bijkantoor)** = Branch office address (foreign entities)

From specs: *"Addresses (legal persons: seat + optional branch; natural persons: establishment addresses only)"*

This explains the 40% gap:
- Legal persons (TypeOfEnterprise=2) have REGO addresses → appear in address.csv with enterprise number
- Natural persons (TypeOfEnterprise=1) have NO enterprise-level address → only establishment addresses (with establishment number)

### **DECISION FOR "PRIMARY" ADDRESS**:
```
For ENTERPRISES:
1. REGO (Zetel) address if exists
2. NULL if no REGO (natural person)

For ESTABLISHMENTS:
1. BAET (Vestigingseenheid) address if exists
2. ABBR (Bijkantoor) if BAET not available
```

### Schema Implication
**DO NOT denormalize address into enterprises table** - 40% would be NULL. Instead:
- Keep separate `enterprise_addresses` link table
- Filter by TypeOfAddress='REGO' for "primary" address
- Join only when address is needed

---

## 3. Activity Patterns

### Classification Distribution
| Classification | Description | Count | Percentage |
|----------------|-------------|-------|------------|
| MAIN | Main activity | 29,221,690 | 80.4% |
| SECO | Secondary activity | 7,079,688 | 19.5% |
| ANCI | Auxiliary activity | 4,990 | 0.01% |

### NACE Version Distribution
| Version | Count | Percentage |
|---------|-------|------------|
| 2025 | 17,328,420 | 47.7% |
| 2008 | 16,647,716 | 45.8% |
| 2003 | 2,330,232 | 6.4% |

### ActivityGroup Distribution
| ActivityGroup | Description (NL) | Count | Percentage |
|---------------|------------------|-------|------------|
| 003 | Activiteiten | 28,628,062 | 78.8% |
| 001 | BTW-activiteiten | 6,440,792 | 17.7% |
| 006 | RSZ-activiteiten | 1,202,740 | 3.3% |
| 007 | Gesubsideerd onderwijs | 25,360 | 0.07% |
| 005 | RSZPPO-activiteiten | 6,925 | 0.02% |
| 004 | Federaal openbaar ambt | 2,487 | 0.007% |
| 002 | EDRL-activiteiten | 2 | 0.0% |

### **CRITICAL FINDING**: Not All Enterprises Have MAIN Activity
- **Total enterprises**: 1,938,238
- **Enterprises with MAIN activity**: 1,253,298 (65%)
- **Enterprises WITHOUT MAIN activity**: 684,940 (35%)

### Multiple MAIN Activities Per Enterprise
- **Average activities per enterprise**: 12.4
- **Median activities**: 6
- **Maximum**: 957 activities (enterprise 2.175.653.085)
- **Minimum**: 1 activity

### Why Multiple MAIN Activities?
Looking at sample enterprise 0200.065.765:
```
ActivityGroup | NaceVersion | NaceCode | Classification
001           | 2003        | 70111    | MAIN
006           | 2008        | 84130    | MAIN
001           | 2008        | 41101    | MAIN
006           | 2025        | 84130    | MAIN
001           | 2025        | 68121    | MAIN
```

**Explanation**: Same enterprise has MAIN activities across:
- Different **ActivityGroups** (001=BTW, 006=RSZ)
- Different **NACE versions** (2003, 2008, 2025)

So "MAIN" doesn't mean "single main activity" - it means "main activity per group per version".

### **DECISION FOR "PRIMARY" MAIN ACTIVITY**:
```
Priority order:
1. ActivityGroup=003 (general activities)
2. NaceVersion=2025 (newest)
3. Classification=MAIN
4. If multiple still exist, pick first by NaceCode (alphabetically)

Fallback: NULL if no MAIN activity exists
```

### Storage Optimization Opportunity
- **Total activity rows**: 36,306,369
- **Unique NACE codes**: 2,228 (v2025), 2,326 (v2008), 2,711 (v2003) = ~7,265 total
- **NACE descriptions**: Stored in code.csv (3,838 codes × avg 150 bytes = 575KB)

**Current plan** (denormalized):
- 36M rows × (code + desc_nl + desc_fr) = ~500 bytes/row = **18GB per snapshot**

**Link table approach**:
- 36M links × 50 bytes = 1.8GB
- 7,265 NACE codes × 200 bytes = 1.45MB (static, loaded once)
- **Total**: ~1.8GB per snapshot = **10x reduction**

---

## 4. Code Table Analysis

### Categories
| Category | Unique Codes | Total Rows | Use |
|----------|--------------|------------|-----|
| Nace2003 | 3,838 | 7,676 | Activity descriptions |
| Nace2008 | 3,324 | 6,648 | Activity descriptions |
| Nace2025 | 3,276 | 6,552 | Activity descriptions |
| JuridicalForm | 146 | 438 | Legal form codes |
| JuridicalSituation | 40 | 120 | Status codes |
| ActivityGroup | 7 | 14 | Activity category |
| Language | 5 | 10 | Language codes |
| TypeOfAddress | 4 | 8 | Address type |
| TypeOfDenomination | 4 | 8 | Name type |
| ContactType | 3 | 6 | Contact type |
| EntityContact | 3 | 6 | Contact entity |
| TypeOfEnterprise | 3 | 6 | Enterprise type |
| Classification | 3 | 6 | Activity classification |
| Status | 1 | 2 | Active/inactive |

### Total Code Table Size
- **21,501 rows** (including all languages)
- **~5,000 unique codes** across 14 categories
- **~1.9MB** in CSV format

**Recommendation**: Load entire code.csv into Motherduck `codes` table at startup. Use for JOIN operations.

---

## 5. Relationship Validation

### Summary
| Relationship | Coverage | Missing |
|--------------|----------|---------|
| Enterprise → Denomination | 100% ✓ | 0 |
| Enterprise → Address (REGO) | 60% | 40% (natural persons) |
| Enterprise → MAIN Activity | 65% | 35% |

### Implications
1. **Denomination is mandatory** - safe to denormalize ONE primary name
2. **Address is optional** - DO NOT denormalize, use link table
3. **MAIN activity is optional** - DO NOT denormalize, use link table

---

## 6. Daily Update Analysis (Extract 140 → 147)

### Time Period
- **Full snapshot**: October 4, 2025 (extract 140)
- **Update snapshot**: October 11, 2025 (extract 147)
- **Days between**: 7 days

### Changes by Entity Type
| Entity Type | Deletes | Inserts | Net Change | % of Full Dataset |
|-------------|---------|---------|------------|-------------------|
| Enterprise | 161 | 27 | -134 | -0.007% |
| Activity | 41 | 424 | +383 | +0.0011% |
| Address | 52 | 67 | +15 | +0.0005% |
| Denomination | 171 | 52 | -119 | -0.0036% |
| Establishment | 13 | 21 | +8 | +0.0005% |
| Contact | 23 | 39 | +16 | +0.0023% |
| **TOTAL** | **461** | **630** | **+169** | **+0.0009%** |

### Daily Rate Estimation
- **Total changes**: 1,091 over 7 days
- **Average per day**: **~156 changes/day**
- **Change rate**: 0.0008% of full dataset per day

### Annual Growth Projection
- **Daily changes**: 156 rows/day
- **Annual changes**: 156 × 365 = **56,940 rows/year** (negligible)
- **Monthly full snapshots**: 46M rows/month × 12 = **552M rows/year**

**Conclusion**: Data growth is driven by MONTHLY snapshots, not daily updates.

---

## 7. Storage Estimation

### Current Full Dataset (Extract 140)
```
enterprises:    1,938,238 rows × 200 bytes = 388 MB
denominations:  3,309,908 rows × 100 bytes = 331 MB
addresses:      2,841,756 rows × 150 bytes = 426 MB
activities:    36,306,369 rows × 100 bytes = 3,631 MB
establishments: 1,672,491 rows × 150 bytes = 251 MB
contacts:         691,158 rows × 100 bytes = 69 MB
branches:           7,326 rows × 150 bytes = 1 MB
codes:             21,501 rows × 100 bytes = 2 MB

TOTAL: ~5.1 GB per snapshot (uncompressed, estimated)
```

### With Temporal Tracking (Daily Granularity)
- **Current month**: 5.1 GB × 30 days = 153 GB
- **Previous month**: 5.1 GB × 30 days = 153 GB
- **Total for 60 days**: **~306 GB**

**Problem**: This is HUGE and expensive in Motherduck.

### With Tiered Retention Strategy

**Option A: Daily for 60 days + Monthly forever**
```
Daily (60 days):       5.1 GB × 60 = 306 GB
Monthly (24 months):   5.1 GB × 24 = 122 GB
Total (2 years):       428 GB
```

**Option B: Daily for current month + Monthly forever**
```
Daily (30 days):       5.1 GB × 30 = 153 GB
Monthly (24 months):   5.1 GB × 24 = 122 GB
Total (2 years):       275 GB
```

**Option C: Current + Monthly only (RECOMMENDED)**
```
Current (live):        5.1 GB × 1 = 5.1 GB
Monthly (24 months):   5.1 GB × 24 = 122 GB
Total (2 years):       127 GB
```

### With Link Table Optimization (Activities)

**Before** (denormalized activities):
- Activities: 36M rows × 500 bytes = 18 GB per snapshot
- Total per snapshot: ~20 GB

**After** (link table):
- Activity links: 36M rows × 50 bytes = 1.8 GB
- NACE codes table: 7,265 codes × 200 bytes = 1.45 MB (static, loaded once)
- Total per snapshot: ~7 GB

**Savings**: 13 GB per snapshot

**With 24 monthly snapshots**:
- Before: 20 GB × 24 = 480 GB
- After: 7 GB × 24 = 168 GB
- **Savings**: 312 GB (65% reduction)

---

## 8. Data Quality Observations

### Complete Data (100% coverage)
✓ All enterprises have at least one denomination

### Partial Data
⚠ 40% of enterprises have no address (natural persons)
⚠ 35% of enterprises have no MAIN activity
⚠ Some enterprises have up to 957 activities (outliers)

### Data Anomalies
- Language code "4" exists (19,223 rows) but not documented in specs
- Some enterprises have 10+ denominations (complexity)
- ActivityGroup distribution is very skewed (79% in group "003")

---

## 9. Recommendations

### Schema Design
1. **DO denormalize**:
   - Primary denomination (Type 001, Dutch preferred)
   - Basic enterprise fields (status, juridical form, start date)

2. **DO NOT denormalize**:
   - Addresses (40% NULL, use link table)
   - Activities (huge, use link table)
   - Additional denominations (use link table)

### Retention Strategy
**RECOMMENDED**: Option C (Current + Monthly)
- Keep ONLY latest snapshot as "current" (5.1 GB)
- Take monthly snapshot on first Sunday (5.1 GB per month)
- Daily updates modify "current" in-place (not creating new snapshots)
- After 2 years: 127 GB total

**Benefits**:
- Minimal storage cost
- Still supports "point-in-time" queries (monthly granularity)
- Daily updates are fast (modify current snapshot)

**Trade-off**: Cannot query exact state on arbitrary day (only month granularity)

### Activity Storage
**MUST USE link table approach**:
```sql
CREATE TABLE enterprise_activities (
  id UUID PRIMARY KEY,
  enterprise_number VARCHAR,
  activity_group VARCHAR,
  nace_version VARCHAR,
  nace_code VARCHAR,
  classification VARCHAR,
  _valid_from DATE,
  _valid_to DATE
);

CREATE TABLE nace_codes (
  nace_version VARCHAR,
  nace_code VARCHAR,
  description_nl VARCHAR,
  description_fr VARCHAR,
  description_de VARCHAR,
  description_en VARCHAR,
  PRIMARY KEY (nace_version, nace_code)
);
```

**Savings**: 65% storage reduction (312 GB over 2 years)

### Primary Selection Rules
**Documented for implementation**:

**Denomination**:
1. Type=001, Language=2 (Legal Name, Dutch)
2. Type=001, Language=0 (Legal Name, French)
3. Type=003, Language=2 (Commercial Name, Dutch)
4. Type=003, Language=0 (Commercial Name, French)
5. ANY denomination (fallback)

**Address (for enterprises)**:
1. TypeOfAddress=REGO (Registered Office)
2. NULL (natural persons have no enterprise-level address)

**Main Activity**:
1. ActivityGroup=003, NaceVersion=2025, Classification=MAIN
2. ANY ActivityGroup, NaceVersion=2025, Classification=MAIN
3. ANY ActivityGroup, NaceVersion=2008, Classification=MAIN
4. NULL (35% of enterprises have no MAIN activity)

---

## 10. Next Steps

1. ✅ Data analysis complete
2. ⏳ Design final schema with link tables
3. ⏳ Test Parquet compression (expect ~10x compression)
4. ⏳ Test Motherduck upload (measure time and cost)
5. ⏳ Implement CSV → DuckDB → Parquet pipeline
6. ⏳ Build transformation logic with primary selection rules
7. ⏳ Test with full dataset locally
8. ⏳ Deploy to Motherduck

---

## Appendix: Sample Data Snippets

### Enterprise 0200.065.765
**Denominations**:
- (NL, Type 001): "Intergemeentelijke Vereniging Veneco"
- (NL, Type 002): "Veneco"

**Activities** (5 MAIN activities across different groups/versions):
- ActivityGroup 001, NACE 2003, Code 70111
- ActivityGroup 006, NACE 2008, Code 84130
- ActivityGroup 001, NACE 2008, Code 41101
- ActivityGroup 006, NACE 2025, Code 84130
- ActivityGroup 001, NACE 2025, Code 68121

This confirms: ONE enterprise can have MULTIPLE "MAIN" activities.

---

**Analysis complete. See ANALYSIS.md for implementation strategy.**
