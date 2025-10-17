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

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (id, _snapshot_date, _extract_number)
);

-- Comments for documentation
COMMENT ON TABLE branches IS 'Foreign entity branches (~7K) - minimal data in KBO';
COMMENT ON COLUMN branches.enterprise_number IS 'Parent enterprise (nullable)';
