CREATE TABLE IF NOT EXISTS friend_tuple (
  rhs UUID REFERENCES users(id),
  lhs UUID REFERENCES users(id),
  friends_since TIMESTAMPTZ NOT NULL DEFAULT now() -- FIXME: add this 
);
-- to query friends of a user, you select friend_tuple where the id occures either in rhs or lhs
