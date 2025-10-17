-- Contacts table (link table)
-- Stores contact details (phone, email, web) for enterprises and establishments

CREATE TABLE IF NOT EXISTS contacts (
  -- Primary key (composite to support temporal tracking)
  id VARCHAR NOT NULL,                        -- Concatenated: entity_number_entity_contact_contact_type_value
  _snapshot_date DATE NOT NULL,               -- Part of PK for temporal tracking
  _extract_number INTEGER NOT NULL,           -- Part of PK for temporal tracking

  -- Entity reference
  entity_number VARCHAR NOT NULL,
  entity_type VARCHAR NOT NULL,               -- 'enterprise' or 'establishment'
  entity_contact VARCHAR NOT NULL,            -- ENT=Enterprise, ESTB=Establishment, BRANCH=Branch

  -- Contact details
  contact_type VARCHAR NOT NULL,              -- TEL, EMAIL, WEB, etc.
  contact_value VARCHAR NOT NULL,             -- The actual contact information

  -- Temporal tracking
  _is_current BOOLEAN NOT NULL,               -- TRUE for current, FALSE for historical

  -- Composite primary key to support temporal tracking
  PRIMARY KEY (id, _snapshot_date, _extract_number),

  -- Constraints
  CHECK (entity_type IN ('enterprise', 'establishment'))
);

-- Comments for documentation
COMMENT ON TABLE contacts IS 'Contact info (~690K) - phone, email, website';
COMMENT ON COLUMN contacts.entity_contact IS 'ENT, ESTB, or BRANCH';
COMMENT ON COLUMN contacts.contact_type IS 'TEL, EMAIL, WEB';
COMMENT ON COLUMN contacts.contact_value IS 'Phone, email, or URL';
