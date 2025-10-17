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

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (establishment_number, _snapshot_date, _extract_number)
);

-- Comments for documentation
COMMENT ON TABLE establishments IS 'Physical locations (~1.7M) linked to enterprises';
COMMENT ON COLUMN establishments.enterprise_number IS 'Parent enterprise (FK)';
COMMENT ON COLUMN establishments.commercial_name IS 'Commercial name in any language (optional, Type 003)';
COMMENT ON COLUMN establishments.commercial_name_language IS 'Language of commercial_name: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN';
