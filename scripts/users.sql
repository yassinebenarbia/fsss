CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL,
  -- description TEXT,
  -- display_name TEXT,
  reg_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  password_hash TEXT NOT NULL
  joined_serves UUID[] -- This is so that we don't have to scan through the whole list of serves to get what servers this user is in
);
