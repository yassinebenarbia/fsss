-- TODO: manage this thing
CREATE TABLE permissions (
  id UUID DEFAULT uuid_generate_v4() UNIQUE,
  moder UUID refers users(id),
  create_space boolean,
  delete_space boolean,
  delete_server boolean,
  ban_users boolean, -- can ban users that are not joined
  mod_members boolean, -- only can assign default mod permissions unless specified otherwiese by his own permission
                       -- by default, only the creator of the server can do that
  tag_everyone boolean,
  tag_mods boolean,
  change_his_permissions boolean,
  change_others_permissions boolean,
);
