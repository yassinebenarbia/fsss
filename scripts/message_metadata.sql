CREATE TABLE IF NOT EXISTS message_metadata (
  id UUID DEFAULT uuid_generate_v4() NOT NULL UNIQUE,
  reply UUID REFERENCES replies(id), -- FIXME: remove this NOW
  -- instead do reply UUID REFERENCES messages(id),
  PRIMARY KEY (id) 
);
