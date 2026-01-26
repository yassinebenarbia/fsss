CREATE TABLE user_server (
  server_id UUID REFERENCES servers(id),
  user_id BIGSERIAL REFERENCES users(id),
  role ROLE NOT NULL,
  PRIMARY KEY (server_id, user_id),
  CONSTRAINT server_id_and_user_id UNIQUE (server_id, user_id)
)
