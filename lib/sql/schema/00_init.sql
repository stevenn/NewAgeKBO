-- KBO for the New Age - Database Schema Initialization
-- This script creates all tables in the correct order
-- Execute this file to set up a fresh Motherduck database
--
-- Storage estimate: ~2.5 GB for 2 years of monthly snapshots
-- Per snapshot: ~100 MB (Parquet with ZSTD compression)
--
-- Design principles:
-- 1. Temporal tracking (current + monthly snapshots)
-- 2. Link tables for activities and addresses (storage optimization)
-- 3. Code-only storage with runtime JOIN for descriptions
-- 4. Multi-language support (NL/FR/DE/EN)
-- 5. No indexes (Motherduck doesn't use them for acceleration)

-- =============================================================================
-- CORE TABLES (Data entities)
-- =============================================================================

-- 1. Enterprises (1.9M rows, 5 MB compressed)
-- \i 01_enterprises.sql

-- 2. Establishments (1.7M rows, 4 MB compressed)
-- \i 02_establishments.sql

-- 3. Denominations (3.3M rows, 8 MB compressed)
-- Link table for all business names
-- \i 03_denominations.sql

-- 4. Addresses (2.8M rows, 11 MB compressed)
-- Link table - 40% of enterprises have no address
-- \i 04_addresses.sql

-- 5. Activities (36M rows, 71 MB compressed)
-- CRITICAL link table - saves 90% storage
-- \i 05_activities.sql

-- 6. Contacts (0.7M rows, 1 MB compressed)
-- Link table for contact details
-- \i 07_contacts.sql

-- 8. Branches (7K rows, <1 MB compressed)
-- Foreign entity branch offices
-- \i 08_branches.sql

-- =============================================================================
-- LOOKUP TABLES (Static reference data)
-- =============================================================================

-- 6. NACE Codes (7K rows, <1 MB)
-- Static lookup table - loaded once
-- \i 06_nace_codes.sql

-- 9. Codes (21K rows, <1 MB)
-- Static lookup table for all code descriptions
-- \i 09_codes.sql

-- =============================================================================
-- METADATA TABLES
-- =============================================================================

-- 10. Import Jobs (grows over time)
-- Tracks all import operations
-- \i 10_import_jobs.sql

-- =============================================================================
-- VIEWS (Optional - for convenience)
-- =============================================================================

-- Current snapshot views (filter _is_current = true)
CREATE OR REPLACE VIEW enterprises_current AS
SELECT * FROM enterprises WHERE _is_current = true;

CREATE OR REPLACE VIEW establishments_current AS
SELECT * FROM establishments WHERE _is_current = true;

CREATE OR REPLACE VIEW denominations_current AS
SELECT * FROM denominations WHERE _is_current = true;

CREATE OR REPLACE VIEW addresses_current AS
SELECT * FROM addresses WHERE _is_current = true;

CREATE OR REPLACE VIEW activities_current AS
SELECT * FROM activities WHERE _is_current = true;

CREATE OR REPLACE VIEW contacts_current AS
SELECT * FROM contacts WHERE _is_current = true;

CREATE OR REPLACE VIEW branches_current AS
SELECT * FROM branches WHERE _is_current = true;

-- =============================================================================
-- NOTES
-- =============================================================================

-- Indexes are NOT created because Motherduck doesn't use them for query acceleration.
-- Query performance comes from:
-- - Column pruning (only read needed columns)
-- - Predicate pushdown (filter during scan)
-- - Partition pruning (filter by _is_current, _snapshot_date)
-- - Columnar Parquet storage (already optimized)

-- For full-text search on enterprise names, consider external search service:
-- - Typesense, MeiliSearch, or Elasticsearch
-- - Sync data from Motherduck via nightly job
-- - Return enterprise numbers, then JOIN in Motherduck

-- Foreign key constraints are not enforced in DuckDB/Motherduck
-- Data integrity maintained at application level during import
