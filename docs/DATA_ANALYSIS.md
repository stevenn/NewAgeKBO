# Critical Analysis of Implementation Plan

**Date**: 2025-10-13
**Status**: Working Document

## Executive Summary

The implementation plan is 60% complete with good domain understanding but lacks concrete implementation details. Key gaps: temporal data accumulation strategy, Motherduck-specific implementation, Vercel limitations handling, and primary selection rules.

**Revised Strategy**: Process monthly full datasets locally with DuckDB, upload to Motherduck. Handle daily updates via Vercel + Motherduck. Implement tiered retention (daily → monthly → archive).

---

## Critical Issues Identified

### 1. SCHEMA DESIGN - Temporal Data Accumulation

**Issue**: Plan proposes temporal tracking but doesn't address storage growth.

**Reality**:
- KBO Open Data contains NO historical data - only current state snapshots
- YOU build history by keeping each snapshot
- Storage grows linearly: 1.9M enterprises × 365 days = 693.5M enterprise records/year
- With 36M activity records per snapshot × 365 days = 13.14 BILLION activity records/year

**Missing**:
- Storage cost estimation
- Retention policy (how long to keep daily granularity?)
- Archive strategy for old data

**Proposed Solution**: Tiered retention strategy (see below)

---

### 2. DATA VOLUME REALITY

**Actual sizes from sample data** (extract 140):
```
activity.csv:      36,306,369 rows (1.5GB)  - 19 activities/enterprise avg
address.csv:        2,841,756 rows (286MB)  - 1.5 addresses/enterprise avg
denomination.csv:   3,309,908 rows (146MB)  - 1.7 names/enterprise avg
enterprise.csv:     1,938,239 rows (86MB)   - base entities
establishment.csv:  1,672,491 rows (67MB)   - 0.86 establishments/enterprise avg
contact.csv:          691,158 rows (32MB)   - 0.36 contacts/enterprise avg
branch.csv:             7,326 rows (304KB)  - foreign entities
code.csv:              21,501 rows (1.9MB)  - lookup table

Total: 46,788,754 rows per snapshot
```

**Daily update sizes** (extract 147, 7 days after full):
```
enterprise: 163 deletes, 29 inserts  (192 changes)
activity:    43 deletes, 426 inserts (469 changes)
address:     54 deletes, 69 inserts  (123 changes)
denomination: 173 deletes, 54 inserts (227 changes)
establishment: 15 deletes, 23 inserts (38 changes)
contact:     25 deletes, 41 inserts  (66 changes)

Total: ~1,115 changes over 7 days = ~159 changes/day
```

**Implications**:
- Daily updates are TINY (0.008% of full dataset)
- Monthly full imports: 46M rows
- Daily incremental: ~159 rows
- Storage growth: ~159 rows/day × 365 = 58,035 new rows/year from daily updates (negligible)
- REAL growth comes from keeping FULL snapshots monthly (46M rows/month)

---

### 3. DENORMALIZATION STRATEGY - Missing Rules

**Problem**: Plan proposes denormalizing "primary" denomination, address, activity but doesn't define "primary".

**Questions to answer**:
1. Multiple denominations per entity - which is primary?
   - TypeOfDenomination codes: "001", "002", "003"?
   - Are these priority-ordered?
2. Multiple addresses - which is primary?
   - TypeOfAddress: "REGO", "BAET", etc.
   - "REGO" appears to be registered office?
3. Multiple activities per classification - which is "main"?
   - Classification: "MAIN", "SECO", "ANCI"
   - Multiple MAIN activities exist with different NaceVersion (2003, 2008, 2025)
4. Language handling - entity has names in multiple languages
   - Language codes: 0=FR, 1=DE, 2=NL, 3=EN
   - Not all entities have all languages

**Action Required**: Data analysis phase to determine patterns (see Phase 1.5 below)

---

### 4. CODE LOOKUP TABLE - Implementation Details

**code.csv structure**:
```csv
"Category","Code","Language","Description"
"ActivityGroup","001","FR","Activités TVA"
"ActivityGroup","001","NL","BTW-activiteiten"
```

21,501 rows = ~5,400 unique codes × 4 languages

**Missing decisions**:
1. Load into Motherduck table or in-memory map?
2. If Motherduck: JOIN during import or during query?
3. If in-memory: Structure as `Map<Category, Map<Code, Map<Language, Description>>>`?

**Recommendation**:
- Load code.csv into Motherduck `codes` table (permanent reference data)
- Use SQL JOIN during denormalization for reliability
- Cache in application memory for UI display

---

### 5. UPDATE FILE CASCADE LOGIC

**Delete files contain ONLY entity numbers**:
```csv
"EnterpriseNumber"
"0502.503.253"
```

**Problem**: Must cascade to related tables:
- Enterprise deleted → close enterprise record
- Enterprise deleted → close ALL denominations for that enterprise?
- Enterprise deleted → close ALL activities?
- Enterprise deleted → close ALL establishments?

**Similarly for establishment/denomination deletes**:
- Denomination deleted → which one? (multiple per entity)
- Establishment deleted → cascade to establishment addresses/activities?

**Missing**: Explicit cascade rules documentation

**Spec clarification needed**: When denomination_delete.csv has an enterprise number, does it mean:
- Delete ALL denominations for that enterprise (then insert new ones), OR
- Delete specific denomination (but which one - no denomination ID in delete file)?

**Assumption from spec**: Delete-then-insert = delete ALL denominations for listed entities, then insert complete new set from insert file.

---

### 6. MOTHERDUCK IMPLEMENTATION - Completely Missing

**Critical missing details**:
1. Authentication: Token-based or OAuth?
2. Connection method: `@duckdb/node-api` or HTTP API?
3. Data location: Motherduck cloud or hybrid (local + cloud)?
4. Upload method: `COPY FROM 's3://...'` or `INSERT INTO ... SELECT`?
5. Landing zone: S3/GCS bucket or Motherduck staging?
6. Cost model: Storage ($/GB/month) + compute ($/query)?

**Must research**:
- Motherduck pricing for ~46M rows/month
- Best practice for bulk uploads (Parquet via S3 vs direct COPY)
- Query performance on temporal data (billions of rows)

---

### 7. CSV PARSING - Edge Cases

**Observed in sample data**:
```csv
"0200.245.711","AC","012","2","116",,01-01-1922
                                   ^^ empty field (no JuridicalFormCAC)
```

**Edge cases to handle**:
1. Empty fields: consecutive commas
2. Date format: `dd-mm-yyyy` → need parser
3. Number format: `0200.245.711` (dots in enterprise number - keep as string)
4. Quoted strings: proper escape handling
5. BOM at file start
6. Encoding: UTF-8 (verify)
7. Line endings: CRLF vs LF

**Recommendation**: Use battle-tested library (`csv-parse` from csv-parser package) rather than custom parser.

---

### 8. PARQUET CONVERSION - Cost/Benefit Analysis

**To evaluate**:
- CSV: 1.5GB (activity.csv)
- Parquet: Estimated ~100-200MB (10x compression typical)
- Conversion time: TBD (benchmark needed)

**Trade-offs**:
| Approach | Pros | Cons |
|----------|------|------|
| CSV direct | Simple, no conversion | Large transfer, slower queries |
| Local convert to Parquet | Better compression, faster Motherduck queries | Adds conversion step, requires disk space |
| DuckDB native | Can convert CSV→Parquet in single command | Need local DuckDB setup |

**Recommendation**: Use DuckDB locally to convert CSV → Parquet → upload to Motherduck.

```sql
-- DuckDB command
COPY (SELECT * FROM read_csv('activity.csv'))
TO 'activity.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

---

### 9. VERCEL LIMITATIONS - Architecture Redesign

**Vercel constraints**:
- Function timeout: 60s (Pro), 300s (Enterprise)
- Memory: 1GB default, 3GB max
- No persistent filesystem
- Stateless functions

**Plan assumption**: Import runs in single function call (WRONG)

**Revised architecture**:

**For monthly FULL imports** (46M rows, 3-4 hours):
1. Run LOCALLY with DuckDB
2. Process all CSVs → Parquet
3. Upload Parquet files to Motherduck
4. Run SQL on Motherduck to build denormalized tables

**For daily UPDATE imports** (~159 rows, <1 minute):
1. Vercel cron triggers at 8am
2. Download update ZIP from KBO API (small, <1MB)
3. Parse CSVs in-memory
4. Execute DELETE + INSERT SQL on Motherduck
5. Return success/failure

**Job tracking**: Use Motherduck table for status, no need for external queue.

---

### 10. TESTING STRATEGY - Concrete Approach

**Phase 1: Data Validation**
- Load sample data into local DuckDB
- Run analysis queries (see Phase 1.5 below)
- Verify assumptions about primary keys, relationships

**Phase 2: Import Pipeline**
- Test with 10K row subset
- Test with full activity.csv (36M rows)
- Measure: time, memory, CPU

**Phase 3: Temporal Logic**
- Test with extract 140 (full) + extract 147 (update)
- Verify delete-then-insert works correctly
- Check `_valid_from` / `_valid_to` logic

**Phase 4: Integration**
- Test Motherduck upload with Parquet
- Test daily update from Vercel function
- Test UI queries against temporal data

**No mocking**: Use real data and real Motherduck instance (accept cost).

---

### 11. MULTI-LANGUAGE SUPPORT - Concrete Strategy

**Observed patterns**:
```csv
"EntityNumber","Language","TypeOfDenomination","Denomination"
"0200.065.765","2","001","Intergemeentelijke Vereniging Veneco"  (NL)
"0200.065.765","2","002","Veneco"  (abbreviation, NL)
```

Language codes: 0=FR, 1=DE, 2=NL, 3=EN

**Decision for denormalization**:
1. Store ALL languages in separate columns: `name_nl`, `name_fr`, `name_de`, `name_en`
2. If language missing for entity, set to NULL
3. For addresses: `street_nl`, `street_fr` (often identical, but not always)
4. For code descriptions: JOIN on user's preferred language at query time

**Fallback logic**:
- If `name_nl` is NULL, use `name_fr` or `name_en` (first available)
- Document this in code

---

### 12. ERROR HANDLING - Concrete Strategy

**Logging approach**:
```typescript
// Create error log table in Motherduck
CREATE TABLE import_errors (
  id UUID PRIMARY KEY,
  job_id UUID REFERENCES import_jobs(id),
  timestamp TIMESTAMP,
  file_name VARCHAR,
  row_number INTEGER,
  entity_number VARCHAR,
  error_type VARCHAR,  -- 'parse_error', 'validation_error', 'lookup_failed'
  error_message TEXT,
  raw_data TEXT  -- store problematic CSV line
);
```

**Error policy**:
1. Parse errors: Log and skip row (don't fail entire import)
2. Validation errors: Log but continue
3. Lookup failures (missing code): Log with warning, use code as-is
4. SQL errors: FAIL immediately and rollback transaction

**Alerting**:
- If error rate > 1%, send alert
- If any SQL error, send immediate alert
- Daily summary email with error count

**Recovery**:
- Keep original ZIP files for 90 days
- Can re-run import from any extract number
- Use `import_jobs` table to track what was imported

---

## Revised Implementation Strategy

### Hybrid Approach: Local + Cloud

**Monthly FULL imports**:
1. Download ZIP locally (automated script)
2. Process with DuckDB locally:
   - Extract CSVs
   - Load into DuckDB
   - Run denormalization queries
   - Export to Parquet
3. Upload Parquet to Motherduck (via S3 or direct)
4. Update `_valid_to` on old records
5. Log in `import_jobs` table

**Daily UPDATE imports**:
1. Vercel cron (8am) triggers API route
2. Download update ZIP (small, ~1MB)
3. Parse CSVs in-memory (Node.js)
4. Execute SQL on Motherduck:
   ```sql
   -- For each entity in delete file
   UPDATE enterprises SET _valid_to = :snapshot_date - 1
   WHERE enterprise_number = :num AND _valid_to IS NULL;

   -- For each entity in insert file
   INSERT INTO enterprises (...) VALUES (...);
   ```
5. Update `import_jobs` table

**Benefits**:
- No Vercel timeout issues for large imports
- Use local compute for heavy processing
- Motherduck for storage and querying
- Daily updates fast enough for Vercel

---

## Tiered Retention Strategy

**Goal**: Retain daily granularity for current month, monthly snapshots for history, archive old data.

**Schema design**:

```sql
-- Current data (daily updates)
CREATE TABLE enterprises_current AS
SELECT * FROM enterprises WHERE _valid_to IS NULL;

-- Daily snapshots for current month
CREATE TABLE enterprises_daily (
  ... same schema as enterprises ...
  _extract_number INTEGER,
  _snapshot_date DATE
) PARTITION BY DATE_TRUNC('month', _snapshot_date);

-- Monthly snapshots (retained forever)
CREATE TABLE enterprises_monthly (
  ... same schema ...
  _snapshot_month DATE  -- first day of month
);

-- Archive (cold storage, compressed)
CREATE TABLE enterprises_archive (
  ... same schema ...
) PARTITION BY YEAR(_snapshot_date);
```

**Retention policy**:
- **Current**: Always keep latest (WHERE `_valid_to IS NULL`)
- **Daily**: Keep for current month + 1 previous month (rolling 60 days)
- **Monthly**: Keep first snapshot of each month forever
- **Archive**: After 2 years, move to cold storage (cheaper tier)

**Cron jobs**:
1. **Daily** (9am): Import daily updates → `enterprises_daily`
2. **Monthly** (first Sunday):
   - Import full dataset → `enterprises_daily`
   - Create monthly snapshot → `enterprises_monthly` (copy first day of month)
   - Delete daily data older than 60 days
3. **Yearly** (January 1st): Move data older than 2 years → `enterprises_archive`

**Storage calculation**:
- Current: 46M rows (latest snapshot)
- Daily: 46M rows × 2 months = 92M rows
- Monthly: 46M rows × 24 months = 1.1B rows (2 years)
- Archive: 46M rows × N years (compressed)

**Total for 2 years**: ~1.2B rows (manageable with Motherduck)

---

## Activity Storage Optimization

**Problem**: 36M activity rows per snapshot = 19 activities per enterprise average.

**Current plan** (denormalized):
```sql
CREATE TABLE enterprises (
  enterprise_number VARCHAR,
  main_nace_code VARCHAR,
  main_nace_desc_nl VARCHAR,
  main_nace_desc_fr VARCHAR,
  ...
);
```

Only stores ONE main activity, loses 18 others.

**Proposed: Link Table Approach**

```sql
-- Enterprises table (denormalized, NO activities)
CREATE TABLE enterprises (
  enterprise_number VARCHAR PRIMARY KEY,
  status VARCHAR,
  name_nl VARCHAR,
  name_fr VARCHAR,
  address_nl VARCHAR,  -- denormalized primary address
  ...
  _valid_from DATE,
  _valid_to DATE
);

-- Activities lookup table (normalized, reusable)
CREATE TABLE nace_codes (
  nace_version VARCHAR,
  nace_code VARCHAR,
  description_nl VARCHAR,
  description_fr VARCHAR,
  PRIMARY KEY (nace_version, nace_code)
);
-- Note: KBO only provides NL and FR descriptions for NACE codes

-- Link table (many-to-many with temporal tracking)
CREATE TABLE enterprise_activities (
  id UUID PRIMARY KEY,
  enterprise_number VARCHAR,
  activity_group VARCHAR,
  nace_version VARCHAR,
  nace_code VARCHAR,
  classification VARCHAR,  -- 'MAIN', 'SECO', 'ANCI'
  _valid_from DATE,
  _valid_to DATE,
  _extract_number INTEGER,
  FOREIGN KEY (nace_version, nace_code) REFERENCES nace_codes
);

-- Indexes for performance
CREATE INDEX idx_ent_act_number ON enterprise_activities(enterprise_number, _valid_to);
CREATE INDEX idx_ent_act_code ON enterprise_activities(nace_code, _valid_to);
```

**Benefits**:
1. **Storage reduction**: NACE descriptions stored once, not 36M times
2. **Flexibility**: Can query all activities, not just main one
3. **Temporal tracking**: Activities change over time, tracked in link table
4. **Query efficiency**: Join only when needed

**Common queries**:
```sql
-- Get enterprise with main activity (denormalized style)
SELECT e.*,
       ea.nace_code,
       n.description_nl as activity_desc
FROM enterprises e
LEFT JOIN enterprise_activities ea
  ON e.enterprise_number = ea.enterprise_number
  AND ea.classification = 'MAIN'
  AND ea._valid_to IS NULL
LEFT JOIN nace_codes n
  ON ea.nace_version = n.nace_version
  AND ea.nace_code = n.nace_code
WHERE e._valid_to IS NULL;

-- Get all activities for enterprise
SELECT e.name_nl, ea.classification, n.description_nl
FROM enterprises e
JOIN enterprise_activities ea ON e.enterprise_number = ea.enterprise_number
JOIN nace_codes n ON ea.nace_code = n.nace_code
WHERE e.enterprise_number = '0200.065.765'
  AND e._valid_to IS NULL
  AND ea._valid_to IS NULL;
```

**Storage estimate**:
- Old approach: 36M activities × (code + desc_nl + desc_fr) = ~500 bytes/row = 18GB per snapshot
- New approach: 36M links × 50 bytes + 10K codes × 200 bytes = 1.8GB per snapshot + 2MB codes
- **Savings**: 10x reduction

**Same approach for denominations, addresses, contacts** (but less critical as they're smaller tables).

---

## Phase 1.5: Data Analysis with DuckDB

**Before implementing**, run these queries on sample data to validate assumptions:

### Query 1: Denomination patterns
```sql
-- How many denominations per enterprise? What are the types?
SELECT TypeOfDenomination, Language, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv')
GROUP BY TypeOfDenomination, Language
ORDER BY TypeOfDenomination, Language;

-- Enterprises with multiple denominations
SELECT EntityNumber, COUNT(*) as denomination_count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv')
GROUP BY EntityNumber
ORDER BY denomination_count DESC
LIMIT 10;
```

### Query 2: Address patterns
```sql
-- What address types exist?
SELECT TypeOfAddress, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/address.csv')
GROUP BY TypeOfAddress;

-- Enterprises with multiple addresses
SELECT EntityNumber, COUNT(*) as address_count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/address.csv')
GROUP BY EntityNumber
HAVING COUNT(*) > 1
LIMIT 10;
```

### Query 3: Activity patterns
```sql
-- Activity classification distribution
SELECT Classification, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv')
GROUP BY Classification;

-- Enterprises with multiple MAIN activities
SELECT EntityNumber, COUNT(*) as main_activity_count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv')
WHERE Classification = 'MAIN'
GROUP BY EntityNumber
HAVING COUNT(*) > 1
LIMIT 10;

-- NACE version distribution
SELECT NaceVersion, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv')
GROUP BY NaceVersion;
```

### Query 4: Code table analysis
```sql
-- What categories exist in code table?
SELECT Category, COUNT(DISTINCT Code) as unique_codes,
       COUNT(*) as total_rows
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv')
GROUP BY Category;

-- Sample TypeOfDenomination codes
SELECT Code, Language, Description
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv')
WHERE Category = 'TypeOfDenomination'
ORDER BY Code, Language;

-- Sample TypeOfAddress codes
SELECT Code, Language, Description
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv')
WHERE Category = 'TypeOfAddress'
ORDER BY Code, Language;
```

### Query 5: Relationship validation
```sql
-- Do all enterprises have at least one address?
SELECT COUNT(DISTINCT e.EnterpriseNumber) as enterprises_total,
       COUNT(DISTINCT a.EntityNumber) as enterprises_with_address
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/enterprise.csv') e
LEFT JOIN read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/address.csv') a
  ON e.EnterpriseNumber = a.EntityNumber;

-- Do all enterprises have at least one denomination?
SELECT COUNT(DISTINCT e.EnterpriseNumber) as enterprises_total,
       COUNT(DISTINCT d.EntityNumber) as enterprises_with_name
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/enterprise.csv') e
LEFT JOIN read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv') d
  ON e.EnterpriseNumber = d.EntityNumber;

-- Do all enterprises have at least one MAIN activity?
SELECT COUNT(DISTINCT e.EnterpriseNumber) as enterprises_total,
       COUNT(DISTINCT a.EntityNumber) as enterprises_with_main_activity
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/enterprise.csv') e
LEFT JOIN read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv') a
  ON e.EnterpriseNumber = a.EntityNumber
  AND a.Classification = 'MAIN';
```

### Query 6: Daily update analysis
```sql
-- What changed between extract 140 and 147?
SELECT
  'enterprise' as entity_type,
  COUNT(*) as delete_count,
  (SELECT COUNT(*) FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/enterprise_insert.csv')) as insert_count
FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/enterprise_delete.csv')
UNION ALL
SELECT 'activity', COUNT(*),
  (SELECT COUNT(*) FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/activity_insert.csv'))
FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/activity_delete.csv')
UNION ALL
SELECT 'address', COUNT(*),
  (SELECT COUNT(*) FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/address_insert.csv'))
FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/address_delete.csv');
```

**Document findings**: Create `/docs/data-patterns.md` with results.

---

## Parquet Compression Test

**Test script**:
```bash
#!/bin/bash
# Test Parquet compression on activity.csv

duckdb :memory: <<EOF
.timer on

-- Load CSV
CREATE TABLE activity AS
SELECT * FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv');

-- Check size
SELECT COUNT(*) as rows,
       pg_size_pretty(pg_total_relation_size('activity')) as size
FROM activity;

-- Export to Parquet with different compression
COPY activity TO 'activity_uncompressed.parquet' (FORMAT PARQUET, COMPRESSION NONE);
COPY activity TO 'activity_snappy.parquet' (FORMAT PARQUET, COMPRESSION SNAPPY);
COPY activity TO 'activity_zstd.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
COPY activity TO 'activity_gzip.parquet' (FORMAT PARQUET, COMPRESSION GZIP);

.quit
EOF

# Compare sizes
echo "Original CSV:"
ls -lh sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv
echo -e "\nParquet files:"
ls -lh activity_*.parquet

# Cleanup
rm activity_*.parquet
```

**Expected results**:
- CSV: ~1.5GB
- Parquet (ZSTD): ~100-200MB (10x compression)
- Parquet (Snappy): ~300-400MB (faster but less compression)

**Recommendation**: Use ZSTD for storage, Snappy for temporary files.

---

## Motherduck Upload Test

**Prerequisites**:
1. Create Motherduck account
2. Get authentication token
3. Install `@duckdb/node-api`

**Test script (Node.js)**:
```typescript
import { Database } from '@duckdb/node-api';

async function testMotherduckUpload() {
  // Connect to Motherduck
  const db = await Database.create(`md:newagekbo?motherduck_token=${process.env.MOTHERDUCK_TOKEN}`);

  // Create table
  await db.exec(`
    CREATE TABLE IF NOT EXISTS test_activity (
      EntityNumber VARCHAR,
      ActivityGroup VARCHAR,
      NaceVersion VARCHAR,
      NaceCode VARCHAR,
      Classification VARCHAR
    );
  `);

  // Test 1: Upload from local Parquet
  console.time('Upload Parquet');
  await db.exec(`
    INSERT INTO test_activity
    SELECT * FROM read_parquet('activity_zstd.parquet');
  `);
  console.timeEnd('Upload Parquet');

  // Test 2: Upload from local CSV (for comparison)
  console.time('Upload CSV');
  await db.exec(`
    INSERT INTO test_activity
    SELECT * FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv');
  `);
  console.timeEnd('Upload CSV');

  // Verify
  const result = await db.all('SELECT COUNT(*) as count FROM test_activity');
  console.log('Rows inserted:', result[0].count);

  // Cleanup
  await db.exec('DROP TABLE test_activity');
  await db.close();
}

testMotherduckUpload().catch(console.error);
```

**Measure**:
- Upload time for 36M rows
- Network bandwidth used
- Motherduck storage used
- Cost estimate

---

## Next Steps

1. **Save this document** ✓
2. **Run data analysis queries** (Phase 1.5)
3. **Test Parquet compression**
4. **Test Motherduck upload**
5. **Design final schema** based on findings
6. **Update IMPLEMENTATION_PLAN.md** with concrete details
7. **Start Phase 1 implementation**

---

## Open Questions

1. What are the exact meanings of TypeOfDenomination codes (001, 002, 003)?
2. What is the priority order for selecting "primary" denomination?
3. Does REGO always mean registered office address?
4. When denomination_delete.csv lists an enterprise, does it delete ALL denominations or specific one?
5. What is Motherduck pricing for our estimated data volume?
6. Should we use Motherduck hybrid mode (local + cloud) or cloud-only?
7. How do we handle enterprises with NO denomination (if any exist)?
8. Should we support point-in-time queries ("show me enterprise X as it was on date Y")?

**Action**: Research KBO documentation and Motherduck docs to answer these.

---

## Revised Timeline

**Week 1: Foundation & Analysis**
- Day 1-2: Data analysis with DuckDB (Phase 1.5)
- Day 3-4: Parquet compression and Motherduck upload tests
- Day 5: Finalize schema design based on findings

**Week 2-3: Local Import Pipeline**
- Build CSV parser and transformation logic
- Implement link table approach for activities
- Test with full dataset locally

**Week 4-5: Motherduck Integration**
- Build upload pipeline (local → Motherduck)
- Implement temporal logic
- Test with extract 140 + 147

**Week 6-7: Vercel Daily Updates**
- Build API route for daily updates
- Implement Vercel cron job
- Test end-to-end update flow

**Week 8-9: Admin UI**
- Job monitoring dashboard
- Data browser with temporal navigation

**Week 10-11: API & Documentation**
- REST API endpoints
- OpenAPI docs
- TypeScript SDK

**Week 12-13: Testing & Polish**
- Performance optimization
- Error handling refinement
- User acceptance testing

**Week 14: Launch**
- Deploy to production
- Monitor first monthly import
- Monitor daily updates

**Total: 14 weeks** (unchanged but now realistic)
