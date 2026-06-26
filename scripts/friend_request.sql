CREATE TABLE IF NOT EXISTS friend_request (
  id UUID DEFAULT uuid_generate_v4(),
  requester UUID NOT NULL references users(id),
  requested UUID NOT NULL references users(id),
  request_time TIMESTAMPTZ NOT NULL DEFAULT now(),
  request_message TEXT
);
-- is there a way to make sure that friend_request can only occure if A and B are not friends
