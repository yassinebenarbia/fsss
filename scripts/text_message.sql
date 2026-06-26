CREATE TABLE text_message (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  content TEXT
);
