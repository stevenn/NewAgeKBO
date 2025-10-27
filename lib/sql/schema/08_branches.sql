-- Branches table
-- Branch offices of foreign entities in Belgium
-- Note: KBO dataset only provides minimal information (id, start_date, enterprise_number)

CREATE TABLE IF NOT EXISTS branches (
  -- Primary key (composite to support temporal tracking)
  id VARCHAR NOT NULL,                        -- Branch identifier
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Foreign entity reference
  enterprise_number VARCHAR,                  -- May be NULL

  -- Basic info
  start_date DATE,

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical
  _deleted_at_extract INTEGER,                -- Update Set number when this record was deleted (NULL if current)

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (id, _snapshot_date, _extract_number)
);

-- Comments for documentation
COMMENT ON TABLE branches IS 'Foreign entity branches (~7K) - minimal data in KBO. Supports temporal versioning for historical queries.';
COMMENT ON COLUMN branches.id IS 'Composite key: enterprise_number_denomination_hash. Ensures uniqueness across versions.';
COMMENT ON COLUMN branches._snapshot_date IS 'Date of KBO data snapshot (part of composite PK for temporal tracking)';
COMMENT ON COLUMN branches._extract_number IS 'Monotonic extract number for version ordering (part of composite PK)';
COMMENT ON COLUMN branches.enterprise_number IS 'Parent enterprise (nullable)';
COMMENT ON COLUMN branches._is_current IS 'TRUE for current version (highest extract number), FALSE for historical versions';
COMMENT ON COLUMN branches._deleted_at_extract IS 'Extract number when this record was superseded. NULL if current or never superseded. NOTE: Not populated by import scripts (added 2025-10-26), documented limitation.';
