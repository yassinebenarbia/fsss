CREATE TABLE dm (
  id UUID DEFAULT uuid_generate_v4() NOT NULL UNIQUE,
  message_kind MESSAGE_KIND NOT NULL,
  sender_id UUID REFERENCES users(id) NOT NULL,
  receiver_id UUID REFERENCES users(id) NOT NULL,
  -- TODO: add optional name
  sent_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  content TEXT, -- file name or text content
  metadata UUID REFERENCES dm_metadata(id),
  PRIMARY KEY (id, receiver_id, sender_id)
);

