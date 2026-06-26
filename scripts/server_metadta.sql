CREATE TABLE IF NOT EXISTS server_metadata (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  image TEXT
  server_bans_id UUID, 
);
