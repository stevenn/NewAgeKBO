-- Enterprises table (core entity)
-- Stores enterprise-level data with denormalized primary name for performance
-- Uses code-only storage for descriptions (JOIN to codes table at query time)

CREATE TABLE IF NOT EXISTS enterprises (
  -- Primary key (composite to support temporal tracking)
  enterprise_number VARCHAR NOT NULL,
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Basic info (codes only - descriptions via JOIN to codes table)
  status VARCHAR NOT NULL,                    -- AC (active) or ST (stopped)
  juridical_situation VARCHAR,                -- code e.g., "000"
  type_of_enterprise VARCHAR,                 -- 1=natural person, 2=legal person
  juridical_form VARCHAR,                     -- code e.g., "030"
  juridical_form_cac VARCHAR,                 -- code
  start_date DATE,

  -- Primary denomination (denormalized - always exists, 100% coverage)
  -- Stored for fast search without JOINing to denominations table
  -- Note: All enterprises have a legal name (Type 001), so no need to store type
  primary_name VARCHAR NOT NULL,              -- Primary name (any language, never NULL)
  primary_name_language VARCHAR,              -- Language code: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN
  primary_name_nl VARCHAR,                    -- Dutch version (NULL if not available)
  primary_name_fr VARCHAR,                    -- French version (NULL if not available)
  primary_name_de VARCHAR,                    -- German version (NULL if not available)

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical
  _deleted_at_extract INTEGER,                -- Update Set number when this record was deleted (NULL if current)

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (enterprise_number, _snapshot_date, _extract_number)
);

-- Comments for documentation
COMMENT ON TABLE enterprises IS 'Belgian enterprises (~2M) with primary names. Supports temporal versioning for historical queries.';
COMMENT ON COLUMN enterprises.enterprise_number IS 'Unique enterprise identifier (format: 9999.999.999)';
COMMENT ON COLUMN enterprises._snapshot_date IS 'Date of KBO data snapshot (part of composite PK for temporal tracking)';
COMMENT ON COLUMN enterprises._extract_number IS 'Monotonic extract number for version ordering (part of composite PK)';
COMMENT ON COLUMN enterprises.status IS 'AC=Active, ST=Stopped';
COMMENT ON COLUMN enterprises.primary_name IS 'Primary name in any language - never NULL, for display';
COMMENT ON COLUMN enterprises.primary_name_language IS 'Language of primary_name: 0=Unknown, 1=FR, 2=NL, 3=DE, 4=EN';
COMMENT ON COLUMN enterprises.primary_name_nl IS 'Dutch version if available, else NULL';
COMMENT ON COLUMN enterprises.primary_name_fr IS 'French version if available, else NULL';
COMMENT ON COLUMN enterprises.primary_name_de IS 'German version if available, else NULL';
COMMENT ON COLUMN enterprises._is_current IS 'TRUE for current version (highest extract number), FALSE for historical versions';
COMMENT ON COLUMN enterprises._deleted_at_extract IS 'Extract number when this record was superseded. NULL if current or never superseded. NOTE: Not populated by import scripts (added 2025-10-26), documented limitation.';
