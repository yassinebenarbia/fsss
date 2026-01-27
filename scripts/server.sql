CREATE TABLE servers (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  metadata UUID REFERENCES server_metadata(id),
  name TEXT NOT NULL,
  creation_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  creator BIGSERIAL REFERENCES users(id),
  spaces UUID[],
  CONSTRAINT name_and_creator UNIQUE (name, creator)
);
