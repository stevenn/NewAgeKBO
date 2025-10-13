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
