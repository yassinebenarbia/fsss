CREATE TABLE IF NOT EXISTS user_metadata (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  nickname TEXT,
  bio TEXT,
  user_ban_id UUID, -- REMOVE
  pfp UUID
);
