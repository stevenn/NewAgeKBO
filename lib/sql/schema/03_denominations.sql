-- Denominations table (link table)
-- Stores ALL business names for enterprises and establishments
-- Supports multiple languages and types per entity
-- Primary name is denormalized into enterprises table for performance

CREATE TABLE IF NOT EXISTS denominations (
  -- Primary key
  id VARCHAR PRIMARY KEY,                     -- UUID generated during import

  -- Entity reference (can be enterprise OR establishment)
  entity_number VARCHAR NOT NULL,             -- Enterprise or establishment number
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'

  -- Denomination details
  denomination_type VARCHAR NOT NULL,         -- 001, 002, 003, 004
  language VARCHAR NOT NULL,                  -- 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN
  denomination VARCHAR NOT NULL,              -- The actual name text

  -- Temporal tracking
  _snapshot_date DATE NOT NULL,
  _extract_number INTEGER NOT NULL,
  _is_current BOOLEAN NOT NULL,

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment')),
  CHECK (language IN ('0', '1', '2', '3', '4'))
);

-- Comments for documentation
COMMENT ON TABLE denominations IS 'Business names (~3.3M) - legal, commercial, abbreviations';
COMMENT ON COLUMN denominations.entity_type IS 'enterprise or establishment';
COMMENT ON COLUMN denominations.language IS '0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN';
COMMENT ON COLUMN denominations.denomination_type IS '001=Legal name, 002=Abbreviation, 003=Commercial name, 004=Branch name';
