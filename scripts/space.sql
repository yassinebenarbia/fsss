CREATE TABLE space (
  id UUID DEFAULT uuid_generate_v4() UNIQUE,
  server_id UUID REFERENCES servers(id) NOT NULL,
  name TEXT NOT NULL,
  creator UUID REFERENCES users(id) NOT NULL,
  topic TEXT,
  -- removed messages column so that each message will have a space_id
  creation_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  bucket_id UUID NOT NULL,
  metadata UUID REFERENCES space_metadata(id),
  PRIMARY KEY (id, server_id),
  CONSTRAINT space_origin_and_name UNIQUE (server_id, name) -- make sure only one space that have similar name in server
);
