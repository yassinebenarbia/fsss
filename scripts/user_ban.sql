CREATE TABLE IF NOT EXISTS user_ban(
  id UUID NOT NULL references user_metadata(user_ban_id), -- REMOVE
  banner_user_id UUID REFERENCES users(id) NOT NULL,
  banned_user_id UUID REFERENCES users(id) NOT NULL,
  ban_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(banner_user_id, banned_user_id)  
);
