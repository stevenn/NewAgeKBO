-- Activities table (link table - CRITICAL for storage optimization)
-- 36M activity records per snapshot
-- Storage reduction: 1.5GB CSV → 71MB Parquet (with ZSTD)
-- NACE descriptions stored once in nace_codes table (not 36M times)
-- Fast JOIN on (nace_version, nace_code)

CREATE TABLE IF NOT EXISTS activities (
  -- Primary key
  id VARCHAR PRIMARY KEY,                     -- UUID generated during import

  -- Entity reference
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'

  -- Activity classification
  activity_group VARCHAR NOT NULL,            -- 001-007
  nace_version VARCHAR NOT NULL,              -- 2003, 2008, 2025
  nace_code VARCHAR NOT NULL,
  classification VARCHAR NOT NULL,            -- MAIN, SECO, ANCI

  -- Temporal tracking
  _snapshot_date DATE NOT NULL,
  _extract_number INTEGER NOT NULL,
  _is_current BOOLEAN NOT NULL,

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment')),
  CHECK (classification IN ('MAIN', 'SECO', 'ANCI')),
  CHECK (nace_version IN ('2003', '2008', '2025'))
);

-- Comments for documentation
COMMENT ON TABLE activities IS 'Economic activities (~36M) - join nace_codes for descriptions';
COMMENT ON COLUMN activities.activity_group IS '001=VAT, 002=EDRL, 003=General, 004=Federal public, 005=RSZPPO, 006=RSZ, 007=Subsidized education';
COMMENT ON COLUMN activities.classification IS 'MAIN=Main activity, SECO=Secondary, ANCI=Auxiliary';
COMMENT ON COLUMN activities.nace_version IS '2003, 2008, or 2025';
