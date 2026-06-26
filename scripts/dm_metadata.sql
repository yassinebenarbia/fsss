CREATE TABLE IF NOT EXISTS dm_metadata (
  id UUID DEFAULT uuid_generate_v4() NOT NULL UNIQUE,
  -- reply UUID REFERENCES dm(id), FIXME
  PRIMARY KEY (id) 
);
