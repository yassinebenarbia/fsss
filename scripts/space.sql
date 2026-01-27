CREATE TABLE space (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  name TEXT NOT NULL,
  creator BIGSERIAL REFERENCES users(id) NOT NULL,
  server_id UUID REFERENCES servers(id) NOT NULL,
  topic TEXT,
  messages UUID[],
  creation_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  bucket_id UUID NOT NULL,
  metadata UUID REFERENCES space_metadata(id),
  CONSTRAINT space_origin_and_name UNIQUE (server_id, name) -- make sure only one space that have similar name in server
);
