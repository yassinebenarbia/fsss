CREATE TABLE IF NOT EXISTS server_ban(
  id UUID NOT NULL references server_metadata(server_bans_id), -- REMOVE
  banner_server_id UUID REFERENCES servers(id) NOT NULL,
  banned_user_id UUID REFERENCES users(id) NOT NULL,
  ban_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(banner_server_id, banned_user_id)  
);
