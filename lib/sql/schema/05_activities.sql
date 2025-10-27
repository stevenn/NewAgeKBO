-- Activities table (link table - CRITICAL for storage optimization)
-- 36M activity records per snapshot
-- Storage reduction: 1.5GB CSV → 71MB Parquet (with ZSTD)
-- NACE descriptions stored once in nace_codes table (not 36M times)
-- Fast JOIN on (nace_version, nace_code)

CREATE TABLE IF NOT EXISTS activities (
  -- Primary key (composite to support temporal tracking)
  id VARCHAR NOT NULL,                        -- Concatenated: entity_number_group_version_code_classification
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Entity reference
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'

  -- Activity classification
  activity_group VARCHAR NOT NULL,            -- 001-007
  nace_version VARCHAR NOT NULL,              -- 2003, 2008, 2025
  nace_code VARCHAR NOT NULL,
  classification VARCHAR NOT NULL,            -- MAIN, SECO, ANCI

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical
  _deleted_at_extract INTEGER,                -- Update Set number when this record was deleted (NULL if current)

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (id, _snapshot_date, _extract_number),

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment')),
  CHECK (classification IN ('MAIN', 'SECO', 'ANCI')),
  CHECK (nace_version IN ('2003', '2008', '2025'))
);

-- Comments for documentation
COMMENT ON TABLE activities IS 'Economic activities (~36M) - join nace_codes for descriptions. Supports temporal versioning for historical queries.';
COMMENT ON COLUMN activities.id IS 'Composite key: entity_number_group_version_code_classification_hash. Ensures uniqueness across versions.';
COMMENT ON COLUMN activities._snapshot_date IS 'Date of KBO data snapshot (part of composite PK for temporal tracking)';
COMMENT ON COLUMN activities._extract_number IS 'Monotonic extract number for version ordering (part of composite PK)';
COMMENT ON COLUMN activities.entity_number IS 'Enterprise or establishment number (FK to either table)';
COMMENT ON COLUMN activities.classification IS 'MAIN=Main activity, SECO=Secondary, ANCI=Ancillary';
COMMENT ON COLUMN activities.nace_version IS '2003, 2008, or 2025';
COMMENT ON COLUMN activities._is_current IS 'TRUE for current version (highest extract number), FALSE for historical versions';
COMMENT ON COLUMN activities._deleted_at_extract IS 'Extract number when this record was superseded. NULL if current or never superseded. NOTE: Not populated by import scripts (added 2025-10-26), documented limitation.';
COMMENT ON COLUMN activities.activity_group IS '001=VAT, 002=EDRL, 003=General, 004=Federal public, 005=RSZPPO, 006=RSZ, 007=Subsidized education';
COMMENT ON COLUMN activities.classification IS 'MAIN=Main activity, SECO=Secondary, ANCI=Auxiliary';
COMMENT ON COLUMN activities.nace_version IS '2003, 2008, or 2025';
