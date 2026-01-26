create table if not exist admin_previlages (
  id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
  admin_id BIGSERIAL REFERENCES users(id) NOT NULL,
)
