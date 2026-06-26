CREATE TABLE IF NOT EXISTS replies (
  id UUID DEFAULT uuid_generate_v4() NOT NULL, -- current reply id
  replying_to UUID REFERENCES messages(id), -- message being replied to
  PRIMARY KEY (id) 
);
