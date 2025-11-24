-- Export Jobs Table
-- Tracks export operations for VAT-liable entities and other exports
-- Uses MotherDuck tables for storage instead of files

CREATE TABLE IF NOT EXISTS export_jobs (
  -- Primary key
  id VARCHAR PRIMARY KEY,

  -- Export configuration
  export_type VARCHAR NOT NULL,               -- 'vat_entities' (extensible for future types)
  filter_config JSON,                         -- Stores filter parameters (e.g., {"activity_groups": ["001", "004", "007"]})

  -- Job status
  status VARCHAR NOT NULL,                    -- 'pending', 'running', 'completed', 'failed'
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message VARCHAR,

  -- Export results
  records_exported BIGINT DEFAULT 0,
  table_name VARCHAR,                         -- MotherDuck table name (e.g., 'export_vat_entities_20251124_143022')

  -- Expiration and cleanup
  expires_at TIMESTAMP,                       -- When the export table should be dropped (24 hours from creation)

  -- Worker info
  worker_type VARCHAR NOT NULL,               -- 'vercel', 'web_manual', 'cli'
  created_by VARCHAR,                         -- User ID from Clerk

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  -- Constraints
  CHECK (export_type IN ('vat_entities')),    -- Extend as needed
  CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  CHECK (worker_type IN ('vercel', 'web_manual', 'cli'))
);

-- Index for listing exports by user
CREATE INDEX IF NOT EXISTS idx_export_jobs_created_by ON export_jobs(created_by);

-- Index for finding expired exports to clean up
CREATE INDEX IF NOT EXISTS idx_export_jobs_expires_at ON export_jobs(expires_at);

-- Index for listing recent exports
CREATE INDEX IF NOT EXISTS idx_export_jobs_created_at ON export_jobs(created_at DESC);

COMMENT ON TABLE export_jobs IS 'Export job metadata and MotherDuck table tracking';
COMMENT ON COLUMN export_jobs.table_name IS 'Name of the MotherDuck table containing export results';
COMMENT ON COLUMN export_jobs.expires_at IS 'Timestamp when the export table should be dropped (cleanup after 24 hours)';
