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

## Final Schema Design

### Core Tables

#### 1. Enterprises (Denormalized Core)
```sql
CREATE TABLE enterprises (
  enterprise_number VARCHAR PRIMARY KEY,

  -- Basic info
  status VARCHAR,  -- AC (active) or ST (stopped)
  juridical_situation VARCHAR,
  juridical_situation_desc_nl VARCHAR,
  juridical_situation_desc_fr VARCHAR,
  type_of_enterprise VARCHAR,  -- 1=natural person, 2=legal person
  juridical_form VARCHAR,
  juridical_form_desc_nl VARCHAR,
  juridical_form_desc_fr VARCHAR,
  juridical_form_cac VARCHAR,
  start_date DATE,

  -- Primary denomination (denormalized - always exists)
  primary_name_nl VARCHAR NOT NULL,
  primary_name_fr VARCHAR,
  primary_name_type VARCHAR,  -- 001, 002, 003, 004

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN  -- true for current snapshot, false for historical
);

CREATE INDEX idx_ent_number ON enterprises(enterprise_number, _is_current);
CREATE INDEX idx_ent_name_nl ON enterprises(primary_name_nl) WHERE _is_current = true;
CREATE INDEX idx_ent_snapshot ON enterprises(_snapshot_date);
```

**Rationale**:
- All enterprises have at least one denomination (100% coverage)
- Denormalize primary name for fast search/display
- Keep basic fields for filtering (status, juridical form, etc.)
- Do NOT denormalize address (40% NULL) or activities (huge)

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

CREATE INDEX idx_est_enterprise ON establishments(enterprise_number, _is_current);
CREATE INDEX idx_est_number ON establishments(establishment_number, _is_current);
```

#### 3. Denominations (All Names - Link Table)
```sql
CREATE TABLE denominations (
  id UUID PRIMARY KEY,
  entity_number VARCHAR NOT NULL,  -- enterprise or establishment number
  entity_type VARCHAR NOT NULL,  -- 'enterprise' or 'establishment'
  denomination_type VARCHAR NOT NULL,  -- 001, 002, 003, 004
  language VARCHAR NOT NULL,  -- 0=FR, 1=DE, 2=NL, 3=EN, 4=unknown
  denomination VARCHAR NOT NULL,

  -- Temporal tracking
  _snapshot_date DATE,
  _extract_number INTEGER,
  _is_current BOOLEAN
);

CREATE INDEX idx_denom_entity ON denominations(entity_number, _is_current);
CREATE INDEX idx_denom_text ON denominations(denomination) WHERE _is_current = true;
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

CREATE INDEX idx_addr_entity ON addresses(entity_number, _is_current);
CREATE INDEX idx_addr_zipcode ON addresses(zipcode) WHERE _is_current = true;
CREATE INDEX idx_addr_municipality ON addresses(municipality_nl) WHERE _is_current = true;
CREATE INDEX idx_addr_type ON addresses(type_of_address, _is_current);
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
  PRIMARY KEY (nace_version, nace_code)
);

-- Load once from code.csv, never changes per snapshot
-- Note: KBO only provides NL and FR descriptions for NACE codes
```

**Rationale**:
- 7,265 unique NACE codes across 3 versions
- Descriptions are 150-200 chars each (NL and FR only)
- KBO does not provide DE or EN translations for NACE codes
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

CREATE INDEX idx_act_entity ON activities(entity_number, _is_current);
CREATE INDEX idx_act_code ON activities(nace_code, _is_current);
CREATE INDEX idx_act_classification ON activities(classification, _is_current);
CREATE INDEX idx_act_composite ON activities(entity_number, classification, nace_version, _is_current);
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

CREATE INDEX idx_contact_entity ON contacts(entity_number, _is_current);
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
  Language: string;  // 0=FR, 1=DE, 2=NL, 3=EN, 4=unknown
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
    { type: '001', lang: '0' },  // Legal name, French
    { type: '003', lang: '2' },  // Commercial name, Dutch
    { type: '003', lang: '0' },  // Commercial name, French
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
        WHEN '0' THEN 2  -- French
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
  MAX(CASE WHEN d.Language = '0' THEN d.Denomination END) as primary_name_fr,
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

#### 1. Search enterprises by name (current)
```sql
SELECT
  e.enterprise_number,
  e.primary_name_nl,
  e.primary_name_fr,
  e.status,
  e.juridical_form_desc_nl
FROM enterprises_current e
WHERE e.primary_name_nl ILIKE '%veneco%'
  OR e.primary_name_fr ILIKE '%veneco%'
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

## Performance Considerations

### Indexes
- Primary keys on all tables
- Index on `_is_current` for fast current data queries
- Composite indexes on frequently joined columns
- Text indexes on name fields for search

### Partitioning (Future)
If data grows beyond 2 years:
```sql
-- Partition by year
CREATE TABLE enterprises (
  ...
) PARTITION BY YEAR(_snapshot_date);
```

### Materialized Views (Future)
For expensive common queries:
```sql
CREATE MATERIALIZED VIEW enterprises_with_primary_activity AS
SELECT
  e.*,
  act.nace_code,
  n.description_nl as activity_desc
FROM enterprises_current e
LEFT JOIN activities act
  ON e.enterprise_number = act.entity_number
  AND act.classification = 'MAIN'
  AND act.activity_group = '003'
  AND act.nace_version = '2025'
  AND act._is_current = true
LEFT JOIN nace_codes n
  ON act.nace_code = n.nace_code
  AND act.nace_version = n.nace_version;
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
