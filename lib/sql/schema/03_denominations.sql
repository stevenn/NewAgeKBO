-- Denominations table (link table)
-- Stores ALL business names for enterprises and establishments
-- Supports multiple languages and types per entity
-- Primary name is denormalized into enterprises table for performance

CREATE TABLE IF NOT EXISTS denominations (
  -- Primary key (composite to support temporal tracking)
  id VARCHAR NOT NULL,                        -- Concatenated: entity_number_type_language_hash(denomination)
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Entity reference (can be enterprise OR establishment)
  entity_number VARCHAR NOT NULL,             -- Enterprise or establishment number
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'

  -- Denomination details
  denomination_type VARCHAR NOT NULL,         -- 001, 002, 003, 004
  language VARCHAR NOT NULL,                  -- 0=unknown, 1=FR, 2=NL, 3=DE, 4=EN
  denomination VARCHAR NOT NULL,              -- The actual name text

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical
  _deleted_at_extract INTEGER,                -- Update Set number when this record was deleted (NULL if current)

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (id, _snapshot_date, _extract_number),

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment')),
  CHECK (language IN ('0', '1', '2', '3', '4'))
);

-- Comments for documentation
COMMENT ON TABLE denominations IS 'Business names (~3.3M) - legal, commercial, abbreviations. Supports temporal versioning for historical queries.';
COMMENT ON COLUMN denominations.id IS 'Composite key: entity_number_type_language_hash(denomination). Ensures uniqueness across versions.';
COMMENT ON COLUMN denominations._snapshot_date IS 'Date of KBO data snapshot (part of composite PK for temporal tracking)';
COMMENT ON COLUMN denominations._extract_number IS 'Monotonic extract number for version ordering (part of composite PK)';
COMMENT ON COLUMN denominations.entity_number IS 'Enterprise or establishment number (FK to either table)';
COMMENT ON COLUMN denominations.entity_type IS 'enterprise or establishment';
COMMENT ON COLUMN denominations.language IS '0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN';
COMMENT ON COLUMN denominations.denomination_type IS '001=Legal name, 002=Abbreviation, 003=Commercial name, 004=Branch name';
COMMENT ON COLUMN denominations._is_current IS 'TRUE for current version (highest extract number), FALSE for historical versions';
COMMENT ON COLUMN denominations._deleted_at_extract IS 'Extract number when this record was superseded. NULL if current or never superseded. NOTE: Not populated by import scripts (added 2025-10-26), documented limitation. Point-in-time queries use partition key (entity_number, language, denomination_type) as workaround.';
