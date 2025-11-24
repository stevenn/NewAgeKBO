# Activity Group Analysis for Peppol Filtering

**Analysis Date:** 2024-10-05 Full Dataset
**Purpose:** Determine which activity groups to include for Peppol participant ID filtering

## Executive Summary

The recommended filter for entities requiring Peppol participant IDs is:

```sql
WHERE activity_group IN ('001', '004', '007')
```

This captures **1,231,969 entities**:
- **1,221,516** VAT-liable entities (Group 001)
- **897** Federal/government entities (Group 004)
- **9,556** Educational institutions (Group 007)

## Activity Group Definitions

| Group | Description | Entity Count | Source |
|-------|-------------|--------------|--------|
| 001 | VAT activities (BTW-activiteiten / Activités TVA) | 1,221,516 | ActivityGroup code in activity.csv |
| 002 | EDRL activities | 2 | Negligible |
| 003 | General activities | 1,647,622 | Largest group, mostly non-VAT |
| 004 | Federal public service activities | 897 | Government entities |
| 005 | ONSSAPL activities | 6,484 | Social sector |
| 006 | ONSS activities (Employers) | 582,605 | Any entity with employees |
| 007 | Subsidized education activities | 9,556 | Schools, universities |

## Cross-Tabulation with VAT (Group 001)

Complete overlap analysis of all groups vs Group 001:

| Group | Total Entities | VAT Overlap | % Overlap | Overlap Type |
|-------|----------------|-------------|-----------|--------------|
| 001 | 1,221,516 | - | - | Reference group |
| 002 | 2 | 0 | 0.00% | **Zero overlap** |
| 003 | 1,647,622 | 3,988 | 0.24% | Minimal overlap |
| 004 | 897 | 0 | 0.00% | **Zero overlap** |
| 005 | 6,484 | 0 | 0.00% | **Zero overlap** |
| 006 | 582,605 | 219,684 | 37.71% | High overlap |
| 007 | 9,556 | 0 | 0.00% | **Zero overlap** |

### Key Findings

**Groups with Zero Overlap (Mutually Exclusive with VAT):**
- Group 002 (EDRL)
- Group 004 (Federal services)
- Group 005 (ONSSAPL)
- Group 007 (Education)

**Group 006 Overlap:**
- 37.71% of ONSS employers also have VAT activities
- These 219,684 entities are already captured by the VAT filter
- The remaining 362,921 ONSS-only entities are non-VAT employers

**Group 003 Overlap:**
- Only 0.24% overlap (3,988 entities)
- Essentially separate from VAT

## VAT Entity Breakdown

Total VAT entities: **1,221,516**

Distribution by group combinations:
- **998,156 (81.71%)** - VAT ONLY (no other activity groups)
- **219,372 (17.96%)** - VAT + Group 006 (ONSS employers)
- **3,676 (0.30%)** - VAT + Group 003 (General activities)
- **312 (0.03%)** - VAT + Groups 003 + 006

## Rationale for Recommended Filter

### Include: Groups 001, 004, 007

**Group 001 (VAT activities) - INCLUDE**
- ✅ VAT-liable entities are required to use e-invoicing for B2B/B2C
- ✅ Primary use case for Peppol participant IDs
- ✅ 1.22M entities

**Group 004 (Federal services) - INCLUDE**
- ✅ Government entities need Peppol for B2G invoicing
- ✅ Zero overlap with VAT (clean addition)
- ✅ 897 entities

**Group 007 (Education) - INCLUDE**
- ✅ Educational institutions need Peppol for B2G invoicing
- ✅ Subsidized schools interact with government
- ✅ Zero overlap with VAT (clean addition)
- ✅ 9,556 entities

### Exclude: Groups 002, 003, 005, 006

**Group 002 (EDRL) - EXCLUDE**
- ❌ Only 2 entities (negligible)
- ❌ No clear Peppol requirement

**Group 003 (General activities) - EXCLUDE**
- ❌ 1.6M entities, only 0.24% have VAT
- ❌ 99.76% would be over-inclusion
- ❌ Mostly non-VAT entities without Peppol needs

**Group 005 (ONSSAPL) - EXCLUDE**
- ❌ Zero overlap with VAT
- ❌ Specialized social sector
- ❌ No clear Peppol requirement for most
- ❌ 6,484 entities

**Group 006 (ONSS employers) - EXCLUDE**
- ❌ Too broad (582K entities)
- ❌ 37.71% overlap already captured by VAT filter
- ❌ Remaining 62.29% (362,921) are non-VAT employers
- ❌ Not all employers need Peppol IDs
- ❌ Would increase scope by 29%

## Top NACE Codes by Activity Group

### Group 001 (VAT Activities)
1. **70200** - Management consultancy (142K entities)
2. **82990** - Business support services (120K entities)
3. **62200** - Computer consultancy (56K entities)
4. **85599** - Other education (55K entities)
5. **73300** - Advertising (47K entities)

### Group 004 (Federal Services)
1. **84111** - General public administration (332 entities)
2. **84231** - Justice and law enforcement (250 entities)
3. **84220** - Defense activities (89 entities)
4. **84130** - Business regulation (52 entities)

### Group 007 (Education)
1. **85203/85204** - Primary education (2,143/2,096 entities)
2. **85314** - Secondary education (1,512 entities)
3. **85311** - General secondary education (622 entities)

### Group 006 (ONSS - Not Included)
1. **56111/56112** - Restaurants (23K/22K entities)
2. **41001** - Construction (12.7K entities)
3. **47110** - Supermarkets/retail (12.4K entities)
4. **70200** - Management consultancy (12K entities)
5. **49410** - Freight transport (10.8K entities)

## Multi-Group Entity Patterns

Common combinations (excluding VAT):
1. **267,283 entities** - Groups 003 + 006 (General + Employers)
2. **7,163 entities** - Groups 006 + 007 (Employers + Education)
3. **6,376 entities** - Groups 005 + 006 (ONSSAPL + Employers)
4. **732 entities** - Groups 004 + 006 (Federal + Employers)

**Observation:** Group 006 (ONSS) combines with almost all other groups, confirming it represents a cross-cutting attribute (being an employer) rather than a specific business type.

## Filter Impact Scenarios

| Filter Scenario | Entity Count | Increase from Baseline |
|-----------------|--------------|------------------------|
| VAT only (001) | 1,221,516 | Baseline |
| VAT + B2G (001,004,007) | **1,231,969** | **+10,453 (+0.86%)** |
| + ONSSAPL (001,004,007,005) | ~1,238,453 | +16,937 (+1.39%) |
| + ONSS (001,004,007,005,006) | 1,586,785 | +365,269 (+29.90%) |

## Implementation Notes

### For Denomination Export Filtering

When creating a variant of `export-current-denominations.ts` to filter by activity group:

```sql
-- Add JOIN to activities table
FROM denominations d
INNER JOIN (
  SELECT DISTINCT entity_number
  FROM activities
  WHERE activity_group IN ('001', '004', '007')
) a ON d.entity_number = a.entity_number
WHERE d._is_current = true
  AND ...
```

### Important Caveats

1. **Activity groups are at the activity level, not entity level**
   - A single entity can have multiple activities with different groups
   - Filter returns entities with AT LEAST ONE activity in the specified groups

2. **This is an indicator-based approach**
   - ActivityGroup '001' indicates VAT-related activities, not official VAT registration
   - The authoritative source is XML Authorization code '00001' (requires SFTP access)
   - See `docs/XML_VAT_IMPLEMENTATION_PLAN.md` for details

3. **NACE versions**
   - Most activities use NACE 2025 (current) or NACE 2008
   - Some legacy entities still have NACE 2003 codes
   - ActivityGroup classification is consistent across NACE versions

## Related Documentation

- `/specs/KBOCookbook_EN.md` - KBO Open Data specifications
- `/docs/XML_VAT_IMPLEMENTATION_PLAN.md` - Authoritative VAT status from XML
- `kbo-vat-extractor/` - Tool for extracting VAT authorization from XML files

## Investigation Scripts

The following temporary investigation scripts were used for this analysis:
- `scripts/investigate-vat-activities.ts` - VAT activity analysis
- `scripts/investigate-b2g-entities.ts` - B2G entity analysis
- `scripts/investigate-onss-entities.ts` - ONSS entity analysis
- `scripts/investigate-crosstab-vat.ts` - Cross-tabulation analysis

These scripts should be deleted after investigation is complete (as per project guidelines).
