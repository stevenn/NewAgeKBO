-- Establishments table
-- Stores establishment units (physical locations) linked to enterprises
-- Most fields stored as codes (JOIN to codes table for descriptions)

CREATE TABLE IF NOT EXISTS establishments (
  -- Primary key (composite to support temporal tracking)
  establishment_number VARCHAR NOT NULL,
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Foreign key to enterprise
  enterprise_number VARCHAR NOT NULL,

  -- Basic info
  start_date DATE,

  -- Commercial name (if different from enterprise)
  commercial_name VARCHAR,                    -- May be NULL (Type 003 denomination)
  commercial_name_language VARCHAR,           -- Language code: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical
  _deleted_at_extract INTEGER,                -- Update Set number when this record was deleted (NULL if current)

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (establishment_number, _snapshot_date, _extract_number)
);

-- Comments for documentation
COMMENT ON TABLE establishments IS 'Physical locations (~1.7M) linked to enterprises. Supports temporal versioning for historical queries.';
COMMENT ON COLUMN establishments.establishment_number IS 'Unique establishment identifier (format: 9.999.999.999)';
COMMENT ON COLUMN establishments._snapshot_date IS 'Date of KBO data snapshot (part of composite PK for temporal tracking)';
COMMENT ON COLUMN establishments._extract_number IS 'Monotonic extract number for version ordering (part of composite PK)';
COMMENT ON COLUMN establishments.enterprise_number IS 'Parent enterprise (FK)';
COMMENT ON COLUMN establishments.commercial_name IS 'Commercial name in any language (optional, Type 003)';
COMMENT ON COLUMN establishments.commercial_name_language IS 'Language of commercial_name: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN';
COMMENT ON COLUMN establishments._is_current IS 'TRUE for current version (highest extract number), FALSE for historical versions';
COMMENT ON COLUMN establishments._deleted_at_extract IS 'Extract number when this record was superseded. NULL if current or never superseded. NOTE: Not populated by import scripts (added 2025-10-26), documented limitation.';
