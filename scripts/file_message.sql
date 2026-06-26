CREATE TABLE file_message (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  key TEXT
);
-- spaces messages -> messages -> file message
-- to know which bucket this file is in, you need to know the original space
