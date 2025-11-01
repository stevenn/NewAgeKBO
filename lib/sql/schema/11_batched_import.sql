-- Batched Import System Tables
-- Supports micro-batch processing of large KBO imports to avoid Vercel timeouts
-- Related: import_jobs table (10_import_jobs.sql)

-- =============================================================================
-- BATCH TRACKING TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS import_job_batches (
  -- Composite primary key
  job_id VARCHAR NOT NULL,
  table_name VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,              -- 'delete' or 'insert'

  -- Batch status
  status VARCHAR NOT NULL,                  -- 'pending', 'processing', 'completed', 'failed'
  records_count INTEGER,
  started_at TIMESTAMP,
  completed_at TIMESTAMP,
  error_message TEXT,

  PRIMARY KEY (job_id, table_name, batch_number, operation),

  -- Constraints
  CHECK (operation IN ('delete', 'insert')),
  CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
);

-- Indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_job_batches_status ON import_job_batches(job_id, status);
CREATE INDEX IF NOT EXISTS idx_job_batches_table ON import_job_batches(job_id, table_name);

-- Comments
COMMENT ON TABLE import_job_batches IS 'Tracks individual batch processing status for micro-batch imports';
COMMENT ON COLUMN import_job_batches.job_id IS 'Foreign key to import_jobs.id';
COMMENT ON COLUMN import_job_batches.table_name IS 'enterprises, establishments, denominations, addresses, contacts, activities, or branches';
COMMENT ON COLUMN import_job_batches.operation IS 'delete (mark historical) or insert (add new records)';
COMMENT ON COLUMN import_job_batches.status IS 'pending, processing, completed, failed';


-- =============================================================================
-- STAGING TABLES (Typed per entity type)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Enterprise Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_enterprises (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,               -- 'delete' or 'insert'
  processed BOOLEAN DEFAULT false,

  -- Enterprise columns (matching enterprises table schema)
  enterprise_number VARCHAR NOT NULL,
  status VARCHAR,
  juridical_situation VARCHAR,
  type_of_enterprise VARCHAR,
  juridical_form VARCHAR,
  juridical_form_cac VARCHAR,
  start_date DATE,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  -- Constraints
  CHECK (operation IN ('delete', 'insert'))
);

CREATE INDEX IF NOT EXISTS idx_staging_enterprises_batch
  ON import_staging_enterprises(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_enterprises IS 'Temporary storage for enterprise data during batched import';


-- -----------------------------------------------------------------------------
-- Establishment Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_establishments (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,
  processed BOOLEAN DEFAULT false,

  -- Establishment columns (matching establishments table schema)
  establishment_number VARCHAR NOT NULL,
  enterprise_number VARCHAR NOT NULL,
  start_date DATE,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CHECK (operation IN ('delete', 'insert'))
);

CREATE INDEX IF NOT EXISTS idx_staging_establishments_batch
  ON import_staging_establishments(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_establishments IS 'Temporary storage for establishment data during batched import';


-- -----------------------------------------------------------------------------
-- Denomination Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_denominations (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,
  processed BOOLEAN DEFAULT false,

  -- Denomination columns (from CSV: EntityNumber, Language, TypeOfDenomination, Denomination)
  -- Note: entity_type NOT in CSV - computed during INSERT based on EntityNumber format
  entity_number VARCHAR NOT NULL,
  language VARCHAR NOT NULL,
  denomination_type VARCHAR NOT NULL,
  denomination VARCHAR NOT NULL,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CHECK (operation IN ('delete', 'insert')),
  CHECK (language IN ('0', '1', '2', '3', '4'))
);

CREATE INDEX IF NOT EXISTS idx_staging_denominations_batch
  ON import_staging_denominations(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_denominations IS 'Temporary storage for denomination data during batched import';


-- -----------------------------------------------------------------------------
-- Address Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_addresses (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,
  processed BOOLEAN DEFAULT false,

  -- Address columns (from CSV: EntityNumber, TypeOfAddress, CountryNL, CountryFR, ...)
  -- Note: entity_type NOT in CSV - computed during INSERT based on EntityNumber format
  entity_number VARCHAR NOT NULL,
  type_of_address VARCHAR NOT NULL,
  country_nl VARCHAR,
  country_fr VARCHAR,
  zipcode VARCHAR,
  municipality_nl VARCHAR,
  municipality_fr VARCHAR,
  street_nl VARCHAR,
  street_fr VARCHAR,
  house_number VARCHAR,
  box VARCHAR,
  extra_address_info VARCHAR,
  date_striking_off DATE,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CHECK (operation IN ('delete', 'insert')),
  CHECK (type_of_address IN ('REGO', 'BAET', 'ABBR', 'OBAD'))
);

CREATE INDEX IF NOT EXISTS idx_staging_addresses_batch
  ON import_staging_addresses(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_addresses IS 'Temporary storage for address data during batched import';


-- -----------------------------------------------------------------------------
-- Contact Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_contacts (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,
  processed BOOLEAN DEFAULT false,

  -- Contact columns (from CSV: EntityNumber, EntityContact, ContactType, Value)
  -- Note: entity_type NOT in CSV - computed during INSERT based on EntityNumber format
  entity_number VARCHAR NOT NULL,
  entity_contact VARCHAR NOT NULL,
  contact_type VARCHAR NOT NULL,
  contact_value VARCHAR NOT NULL,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CHECK (operation IN ('delete', 'insert'))
);

CREATE INDEX IF NOT EXISTS idx_staging_contacts_batch
  ON import_staging_contacts(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_contacts IS 'Temporary storage for contact data during batched import';


-- -----------------------------------------------------------------------------
-- Activity Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_activities (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,
  processed BOOLEAN DEFAULT false,

  -- Activity columns (from CSV: EntityNumber, ActivityGroup, NaceVersion, NaceCode, Classification)
  -- Note: entity_type NOT in CSV - computed during INSERT based on EntityNumber format
  entity_number VARCHAR NOT NULL,
  activity_group VARCHAR NOT NULL,
  nace_version VARCHAR NOT NULL,
  nace_code VARCHAR NOT NULL,
  classification VARCHAR NOT NULL,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CHECK (operation IN ('delete', 'insert')),
  CHECK (classification IN ('MAIN', 'SECO', 'ANCI')),
  CHECK (nace_version IN ('2003', '2008', '2025'))
);

CREATE INDEX IF NOT EXISTS idx_staging_activities_batch
  ON import_staging_activities(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_activities IS 'Temporary storage for activity data during batched import';


-- -----------------------------------------------------------------------------
-- Branch Staging
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS import_staging_branches (
  -- Batch tracking
  job_id VARCHAR NOT NULL,
  batch_number INTEGER NOT NULL,
  operation VARCHAR NOT NULL,
  processed BOOLEAN DEFAULT false,

  -- Branch columns (matching branches table schema)
  id VARCHAR NOT NULL,
  enterprise_number VARCHAR,
  start_date DATE,

  -- Metadata
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  CHECK (operation IN ('delete', 'insert'))
);

CREATE INDEX IF NOT EXISTS idx_staging_branches_batch
  ON import_staging_branches(job_id, batch_number, processed);

COMMENT ON TABLE import_staging_branches IS 'Temporary storage for branch data during batched import';
