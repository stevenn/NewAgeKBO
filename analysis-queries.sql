-- KBO Data Analysis Queries
-- Run with: duckdb :memory: < analysis-queries.sql

.mode markdown
.timer on

-- ==============================================================================
-- QUERY 1: Denomination Patterns
-- ==============================================================================
.print '=== DENOMINATION PATTERNS ==='
.print ''
.print '--- Denomination types by language ---'
SELECT TypeOfDenomination, Language, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv', AUTO_DETECT=TRUE)
GROUP BY TypeOfDenomination, Language
ORDER BY TypeOfDenomination, Language;

.print ''
.print '--- Top 10 enterprises with most denominations ---'
SELECT EntityNumber, COUNT(*) as denomination_count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv', AUTO_DETECT=TRUE)
GROUP BY EntityNumber
ORDER BY denomination_count DESC
LIMIT 10;

.print ''
.print '--- Sample denominations for one enterprise ---'
SELECT *
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv', AUTO_DETECT=TRUE)
WHERE EntityNumber = '0200.065.765';

-- ==============================================================================
-- QUERY 2: Address Patterns
-- ==============================================================================
.print ''
.print '=== ADDRESS PATTERNS ==='
.print ''
.print '--- Address types distribution ---'
SELECT TypeOfAddress, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/address.csv', AUTO_DETECT=TRUE)
GROUP BY TypeOfAddress
ORDER BY count DESC;

.print ''
.print '--- Top 10 enterprises with most addresses ---'
SELECT EntityNumber, COUNT(*) as address_count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/address.csv', AUTO_DETECT=TRUE)
GROUP BY EntityNumber
ORDER BY address_count DESC
LIMIT 10;

-- ==============================================================================
-- QUERY 3: Activity Patterns
-- ==============================================================================
.print ''
.print '=== ACTIVITY PATTERNS ==='
.print ''
.print '--- Activity classification distribution ---'
SELECT Classification, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
GROUP BY Classification
ORDER BY count DESC;

.print ''
.print '--- NACE version distribution ---'
SELECT NaceVersion, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
GROUP BY NaceVersion
ORDER BY count DESC;

.print ''
.print '--- ActivityGroup distribution ---'
SELECT ActivityGroup, COUNT(*) as count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
GROUP BY ActivityGroup
ORDER BY count DESC;

.print ''
.print '--- Top 10 enterprises with most MAIN activities ---'
SELECT EntityNumber, COUNT(*) as main_activity_count
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
WHERE Classification = 'MAIN'
GROUP BY EntityNumber
ORDER BY main_activity_count DESC
LIMIT 10;

.print ''
.print '--- Sample activities for one enterprise ---'
SELECT *
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
WHERE EntityNumber = '0200.065.765'
ORDER BY Classification, NaceVersion;

-- ==============================================================================
-- QUERY 4: Code Table Analysis
-- ==============================================================================
.print ''
.print '=== CODE TABLE ANALYSIS ==='
.print ''
.print '--- Categories in code table ---'
SELECT Category, COUNT(DISTINCT Code) as unique_codes, COUNT(*) as total_rows
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv', AUTO_DETECT=TRUE)
GROUP BY Category
ORDER BY total_rows DESC;

.print ''
.print '--- TypeOfDenomination codes ---'
SELECT Code, Language, Description
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv', AUTO_DETECT=TRUE)
WHERE Category = 'TypeOfDenomination'
ORDER BY Code, Language;

.print ''
.print '--- TypeOfAddress codes ---'
SELECT Code, Language, Description
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv', AUTO_DETECT=TRUE)
WHERE Category = 'TypeOfAddress'
ORDER BY Code, Language;

.print ''
.print '--- ActivityGroup codes (sample) ---'
SELECT Code, Language, Description
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/code.csv', AUTO_DETECT=TRUE)
WHERE Category = 'ActivityGroup'
ORDER BY Code, Language
LIMIT 20;

-- ==============================================================================
-- QUERY 5: Relationship Validation
-- ==============================================================================
.print ''
.print '=== RELATIONSHIP VALIDATION ==='
.print ''
.print '--- Do all enterprises have at least one address? ---'
WITH enterprises AS (
  SELECT EnterpriseNumber FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/enterprise.csv', AUTO_DETECT=TRUE)
),
addresses AS (
  SELECT DISTINCT EntityNumber FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/address.csv', AUTO_DETECT=TRUE)
)
SELECT
  COUNT(DISTINCT e.EnterpriseNumber) as enterprises_total,
  COUNT(DISTINCT a.EntityNumber) as enterprises_with_address,
  COUNT(DISTINCT e.EnterpriseNumber) - COUNT(DISTINCT a.EntityNumber) as enterprises_without_address
FROM enterprises e
LEFT JOIN addresses a ON e.EnterpriseNumber = a.EntityNumber;

.print ''
.print '--- Do all enterprises have at least one denomination? ---'
WITH enterprises AS (
  SELECT EnterpriseNumber FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/enterprise.csv', AUTO_DETECT=TRUE)
),
denominations AS (
  SELECT DISTINCT EntityNumber FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/denomination.csv', AUTO_DETECT=TRUE)
)
SELECT
  COUNT(DISTINCT e.EnterpriseNumber) as enterprises_total,
  COUNT(DISTINCT d.EntityNumber) as enterprises_with_name,
  COUNT(DISTINCT e.EnterpriseNumber) - COUNT(DISTINCT d.EntityNumber) as enterprises_without_name
FROM enterprises e
LEFT JOIN denominations d ON e.EnterpriseNumber = d.EntityNumber;

.print ''
.print '--- Do all enterprises have at least one MAIN activity? ---'
WITH enterprises AS (
  SELECT EnterpriseNumber FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/enterprise.csv', AUTO_DETECT=TRUE)
),
main_activities AS (
  SELECT DISTINCT EntityNumber
  FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
  WHERE Classification = 'MAIN'
)
SELECT
  COUNT(DISTINCT e.EnterpriseNumber) as enterprises_total,
  COUNT(DISTINCT a.EntityNumber) as enterprises_with_main_activity,
  COUNT(DISTINCT e.EnterpriseNumber) - COUNT(DISTINCT a.EntityNumber) as enterprises_without_main_activity
FROM enterprises e
LEFT JOIN main_activities a ON e.EnterpriseNumber = a.EntityNumber;

-- ==============================================================================
-- QUERY 6: Daily Update Analysis
-- ==============================================================================
.print ''
.print '=== DAILY UPDATE ANALYSIS (extract 140 → 147) ==='
.print ''
.print '--- Changes by entity type ---'
SELECT
  'enterprise' as entity_type,
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/enterprise_delete.csv')) as delete_count,
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/enterprise_insert.csv')) as insert_count
UNION ALL
SELECT
  'activity',
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/activity_delete.csv')),
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/activity_insert.csv'))
UNION ALL
SELECT
  'address',
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/address_delete.csv')),
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/address_insert.csv'))
UNION ALL
SELECT
  'denomination',
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/denomination_delete.csv')),
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/denomination_insert.csv'))
UNION ALL
SELECT
  'establishment',
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/establishment_delete.csv')),
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/establishment_insert.csv'))
UNION ALL
SELECT
  'contact',
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/contact_delete.csv')),
  (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/contact_insert.csv'));

.print ''
.print '--- Total changes ---'
SELECT
  SUM(delete_count) + SUM(insert_count) as total_changes,
  SUM(delete_count) as total_deletes,
  SUM(insert_count) as total_inserts
FROM (
  SELECT (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/enterprise_delete.csv')) as delete_count,
         (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/enterprise_insert.csv')) as insert_count
  UNION ALL
  SELECT (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/activity_delete.csv')),
         (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/activity_insert.csv'))
  UNION ALL
  SELECT (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/address_delete.csv')),
         (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/address_insert.csv'))
  UNION ALL
  SELECT (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/denomination_delete.csv')),
         (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/denomination_insert.csv'))
  UNION ALL
  SELECT (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/establishment_delete.csv')),
         (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/establishment_insert.csv'))
  UNION ALL
  SELECT (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/contact_delete.csv')),
         (SELECT COUNT(*)-1 FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/contact_insert.csv'))
);

.print ''
.print '--- Days between extracts ---'
WITH meta_full AS (
  SELECT Value as SnapshotDate
  FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/meta.csv', AUTO_DETECT=TRUE)
  WHERE Variable = 'SnapshotDate'
),
meta_update AS (
  SELECT Value as SnapshotDate
  FROM read_csv('sampledata/KboOpenData_0147_2025_10_12_Update/meta.csv', AUTO_DETECT=TRUE)
  WHERE Variable = 'SnapshotDate'
)
SELECT
  f.SnapshotDate as full_snapshot,
  u.SnapshotDate as update_snapshot,
  DATE_DIFF('day', STRPTIME(f.SnapshotDate, '%d-%m-%Y'), STRPTIME(u.SnapshotDate, '%d-%m-%Y')) as days_between
FROM meta_full f, meta_update u;

-- ==============================================================================
-- QUERY 7: Storage Estimation
-- ==============================================================================
.print ''
.print '=== STORAGE ESTIMATION ==='
.print ''
.print '--- Average activities per enterprise ---'
WITH activity_counts AS (
  SELECT EntityNumber, COUNT(*) as activity_count
  FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
  GROUP BY EntityNumber
)
SELECT
  AVG(activity_count) as avg_activities_per_enterprise,
  MIN(activity_count) as min_activities,
  MAX(activity_count) as max_activities,
  MEDIAN(activity_count) as median_activities
FROM activity_counts;

.print ''
.print '--- Unique NACE codes ---'
SELECT
  NaceVersion,
  COUNT(DISTINCT NaceCode) as unique_nace_codes
FROM read_csv('sampledata/KboOpenData_0140_2025_10_05_Full/activity.csv', AUTO_DETECT=TRUE)
GROUP BY NaceVersion
ORDER BY NaceVersion;

.print ''
.print 'Analysis complete!'
