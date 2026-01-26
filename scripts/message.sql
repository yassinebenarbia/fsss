CREATE TABLE message (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  message_type TEXT NOT NULL,
  content TEXT NOT NULL
);
