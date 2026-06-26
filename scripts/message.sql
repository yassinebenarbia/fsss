CREATE TABLE messages (
  id UUID DEFAULT uuid_generate_v4() NOT NULL UNIQUE,
  message_kind MESSAGE_KIND NOT NULL,
  space_id UUID NOT NULL,
  sender_id UUID REFERENCES users(id) NOT NULL,
  -- TODO: add optional name
  sent_time  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata UUID REFERENCES message_metadata(id),
  content TEXT, -- file name or text content
  PRIMARY KEY (id, space_id)
);
