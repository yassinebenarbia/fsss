// TODO: inspect when the user already have that server created
// TODO: rename 'user_server' table to 'members'
//
// NOTE:
// we need to re-design the banning table to only contain the banner and banned id
// - to check the users banned by a certain server we select by banner_id = id
// - to check the servers that banned a user we select by banned_id = id
// for the user we do the same, we store the tuple of banner and banned id
// - to check the users banned by another user we select by banner_id = id
// - to check the users who banned a specific user banned_id = id
use anyhow::anyhow;
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{DateTime, NaiveDateTime, Utc};
use postgres::NoTls;
use postgres::types::private::BytesMut;
use postgres::types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::Client as PostgresClient;
use uuid::Uuid;

use crate::api::{
    DirectMessage, Member, Server, ServerId, Space, SpaceId, SpaceMessage, Token, User, UserId,
    UserMetadata,
};
use crate::config;
use crate::messages::search_user::SearchField;
use crate::messages::write_dm::WriteDM;
use crate::messages::write_message::WriteMessageRequest;

pub struct InternalUser {
    pub id: UserId,
    pub name: String,
    pub password_hash: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Role {
    Admin,
    Member,
    Guest,
    VIP,
}

impl<'a> FromSql<'a> for Role {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = <&str as FromSql>::from_sql(ty, raw)?;

        match s {
            "admin" => Ok(Role::Admin),
            "member" => Ok(Role::Member),
            "guest" => Ok(Role::Guest),
            "vip" => Ok(Role::VIP),
            _ => Err(format!("invalid Role value: {}", s).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "role")
    }
}

impl ToSql for Role {
    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "role")
    }

    tokio_postgres::types::to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        let s = match self {
            Role::Admin => "admin",
            Role::Member => "member",
            Role::Guest => "guest",
            Role::VIP => "vip",
        };

        s.to_sql(ty, out)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum MessageKind {
    TEXT,
    FILE,
}

impl From<&crate::websocket::MessageKind> for MessageKind {
    fn from(value: &crate::api::MessageKind) -> Self {
        match value {
            crate::api::MessageKind::Text | crate::api::MessageKind::Markdown => Self::TEXT,
        }
    }
}

impl<'a> FromSql<'a> for MessageKind {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let s = <&str as FromSql>::from_sql(ty, raw)?;

        match s {
            "text" | "TEXT" | "Text" => Ok(MessageKind::TEXT),
            "file" | "FILE" | "File" => Ok(MessageKind::FILE),
            _ => Err(format!("invalid MessageKind value: {}", s).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "message_kind")
    }
}

impl ToSql for MessageKind {
    fn accepts(ty: &Type) -> bool {
        matches!(ty.name(), "message_kind")
    }

    tokio_postgres::types::to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        let s = match self {
            MessageKind::TEXT => "TEXT",
            MessageKind::FILE => "FILE",
        };

        s.to_sql(ty, out)
    }
}

#[derive(Serialize)]
pub struct FriendRequest {
    pub request_id: Uuid,
    pub requester_id: Uuid,
    pub requested_id: Uuid,
    pub request_time: NaiveDateTime,
    pub message: Option<String>,
}

impl FriendRequest {
    pub fn new(
        request_id: Uuid,
        requester_id: Uuid,
        requested_id: Uuid,
        request_time: NaiveDateTime,
        message: Option<String>,
    ) -> Self {
        Self {
            request_id,
            requester_id,
            requested_id,
            request_time,
            message,
        }
    }
}

// NOTE: This abstraction exist to implement some frequetly used
// methods over the postgres db
pub struct CustomPostgresClient {
    postgres_client: Arc<PostgresClient>,
}

pub struct ServerMetadata {
    server_ban_id: Uuid,
    image: Option<String>,
    metadata_id: Uuid,
}

impl ServerMetadata {
    pub fn new(server_ban_id: Uuid, metadata_id: Uuid, image: Option<String>) -> Self {
        Self {
            server_ban_id,
            metadata_id,
            image,
        }
    }
}

impl CustomPostgresClient {
    pub async fn new(config: &config::Postgres) -> anyhow::Result<Self> {
        let mut postgres_config = tokio_postgres::Config::new();
        let (postgres_client, postgres_connection) = postgres_config
            .host(&config.host)
            .port(config.port)
            .user(&config.username)
            .password(&config.password)
            .dbname(&config.dbname)
            .connect(NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = postgres_connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self {
            postgres_client: Arc::new(postgres_client),
        })
    }

    pub async fn invalidate_token(&self, token: &str) -> anyhow::Result<()> {
        self.postgres_client
            .execute("DELETE FROM tokens WHERE token = $1;", &[&token])
            .await?;
        Ok(())
    }

    pub async fn is_joined(&self, server_id: &ServerId, user_id: &UserId) -> anyhow::Result<bool> {
        Ok(self
            .postgres_client
            .query_one(
                "SELECT role FROM user_server WHERE user_id = $1 AND server_id = $2",
                &[&user_id.inner(), &server_id.inner()],
            )
            .await
            .map_err(|_| anyhow!("Unable to locate user/server or user did not join the server"))?
            .len()
            .ne(&0))
    }

    pub async fn search_users(
        &self,
        query: &str,
        field: &SearchField,
        limit: &Option<i64>,
    ) -> anyhow::Result<Vec<User>> {
        let limit = limit.unwrap_or(20);

        let sfield = match field {
            // SearchField::ID => format!("user_id = $1"),
            SearchField::Name => format!("name Like $1"),
            SearchField::Nickname => format!("nickname Like $1"),
            SearchField::Bio => format!("bio Like $1"),
        };

        let squery = format!(
            "SELECT users.name, users.id, user_metadata.nickname, user_metadata.bio, user_metadata.pfp, user_metadata.user_ban_id FROM users LEFT JOIN user_metadata ON user_metadata.id = users.id WHERE {} LIMIT $2;",
            sfield
        );
        println!("{}", squery);

        let rows = self
            .postgres_client
            .query(
                &squery,
                &[
                    &if query.is_empty() {
                        query.to_string()
                    } else {
                        format!("%{}%", query)
                    },
                    &limit,
                ],
            )
            .await?;
        println!("fuh");

        let mut users = vec![];

        for row in rows {
            users.push(User::try_from(row)?);
        }

        Ok(users)
    }

    // TODO: make this take the banner and banned usre id's so we can know if X can ban y instead
    // of if X can ban in the server Z
    pub async fn can_ban(&self, server_id: &ServerId, user_id: &UserId) -> anyhow::Result<bool> {
        // TODO: add more fine tuning for roles?
        Ok(self.is_admin(&server_id, user_id).await?)
    }

    pub async fn either_is_banned(
        &self,
        banner_user_id: &UserId,
        banned_user_id: &UserId,
    ) -> anyhow::Result<bool> {
        println!(
            "{}",
            format!(
                "SELECT * FROM user_ban WHERE (banned_user_id = '{}' AND banner_user_id = '{}') OR (banner_user_id = '{}' AND banned_user_id = '{}')",
                &banned_user_id, &banner_user_id, &banned_user_id, &banner_user_id
            )
        );

        let res = self
            .postgres_client
            .query(
                "SELECT * FROM user_ban WHERE (banned_user_id = $1 AND banner_user_id = $2) OR (banner_user_id = $3 AND banned_user_id = $4)",
                &[&banned_user_id.inner(), &banner_user_id.inner(), &banned_user_id.inner(), &banner_user_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("Unable to locate user ban, {}", e))?
            .len();
        println!("ROWS: {res}");
        Ok(res.eq(&1))
    }

    #[allow(unused)]
    pub async fn is_banned_by_user(
        &self,
        banner_user_id: &UserId,
        banned_user_id: &UserId,
    ) -> anyhow::Result<bool> {
        println!(
            "{}",
            format!(
                "SELECT * FROM user_ban WHERE banned_user_id = '{}' AND banner_user_id = '{}'",
                &banned_user_id, &banner_user_id
            )
        );

        let res = self
            .postgres_client
            .query(
                "SELECT * FROM user_ban WHERE banned_user_id = $1 AND banner_user_id = $2",
                &[&banned_user_id.inner(), &banner_user_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("Unable to locate user ban, {}", e))?
            .len();
        Ok(res.eq(&1))
    }

    pub async fn is_banned_from_server(
        &self,
        server_id: &ServerId,
        user_id: &UserId,
    ) -> anyhow::Result<bool> {
        println!(
            "{}",
            format!(
                "SELECT id FROM server_ban WHERE banned_user_id = '{}' AND banner_server_id = '{}'",
                &user_id, &server_id
            )
        );
        Ok(self
            .postgres_client
            .query(
                "SELECT id FROM server_ban WHERE banned_user_id = $1 AND banner_server_id = $2",
                &[&user_id.inner(), &server_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("Unable to locate user/server, {}", e))?
            .len()
            .eq(&1))
    }

    pub async fn is_admin(&self, server_id: &ServerId, user_id: &UserId) -> anyhow::Result<bool> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT role FROM user_server WHERE user_id = $1 AND server_id = $2",
                &[&user_id.inner(), &server_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("Unable to locate user/server, {}", e))?;

        let role = row.get::<usize, Role>(0);

        Ok(matches!(role, Role::Admin))
    }

    pub async fn create_empty_server(&self, name: &str, creator: &UserId) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO servers(name, creator) VALUES ($1, $2);",
                &[&name, &creator.inner()],
            )
            .await
            .map_err(|_| anyhow!("Unable to create server '{}'", name))?;

        Ok(())
    }

    pub async fn join_server(
        &self,
        server_id: &str,
        user_id: &UserId,
        role: &Role,
    ) -> anyhow::Result<Vec<Space>> {
        self.postgres_client
            .execute(
                "INSERT INTO user_server(server_id, user_id, role) VALUES ($1, $2, $3);",
                &[&Uuid::from_str(server_id)?, &user_id.inner(), &role],
            )
            .await
            .map_err(|e| anyhow!("{e}: Unable to join '{}'", server_id))?;

        self.get_server_spaces(server_id).await
    }

    /// Creates an empty server and joins as an admin
    pub async fn create_empty_server_and_join(
        &self,
        server_name: &str,
        creator: &UserId,
    ) -> anyhow::Result<Uuid> {
        self.create_empty_server(server_name, creator).await?;
        println!("SERVER CREATED");
        let server_id = self.get_server_id(server_name, creator).await?;
        println!("SERVER ID: {server_id}");
        self.join_server(&server_id.to_string(), creator, &Role::Admin)
            .await?;
        println!("SERVER {server_id} JOINED SUCCESSFULLY!");
        Ok(server_id)
    }

    pub async fn create_empty_space(
        &self,
        name: &str,
        server_id: &str,
        creator_id: i64,
        bucket_id: &Uuid,
    ) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO space(creator, name, server_id, bucket_id) VALUES ($1, $2, $3, $4);",
                &[&creator_id, &name, &Uuid::from_str(server_id)?, bucket_id],
            )
            .await
            .map_err(|e| anyhow!("{e}: unable to create space {name}!"))?;
        Ok(())
    }

    pub async fn create_empty_space_with_uuid(
        &self,
        space_name: &str,
        server_id: &str,
        creator_id: &Uuid,
        bucket_id: &Uuid,
        space_id: &Uuid,
    ) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO space(creator, name, server_id, bucket_id, id) VALUES ($1, $2, $3, $4, $5);",
                &[&creator_id, &space_name, &Uuid::from_str(server_id)?, bucket_id, &space_id],
            )
            .await
            .map_err(|e| anyhow!("{e}: unable to create space {space_name}!"))?;
        Ok(())
    }

    pub async fn delete_space(&self, space_id: &str, server_id: &str) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "DELETE FROM space WHERE name = $1 AND server_id = $2;",
                &[&Uuid::from_str(space_id)?, &Uuid::from_str(server_id)?],
            )
            .await
            .map_err(|e| anyhow!("{e}: coudn't delete space '{space_id}'"))?;
        Ok(())
    }

    pub async fn get_members(&self, server_id: &str) -> anyhow::Result<Vec<Member>> {
        // TODO: check if banned
        let server_id = ServerId::try_from(server_id)?;

        let rows = self.postgres_client.query(
            "SELECT users.id, users.name, user_server.role from user_server INNER JOIN users ON user_server.server_id = $1 AND users.id = user_server.user_id;",
            &[&server_id.inner()],
        ).await?;

        let mut members = vec![];
        for row in rows {
            let name = row.get::<&str, String>("name");
            let user_id = UserId::from(row.get::<&str, Uuid>("id"));

            let is_banned = self.is_banned_from_server(&server_id, &user_id).await?;
            let role = row.get::<&str, Role>("role");
            members.push(Member {
                name,
                id: user_id.inner_clone(),
                role,
                banned: is_banned,
            });
        }
        Ok(members)
    }

    pub async fn delete_server(&self, server_id: &str) -> anyhow::Result<()> {
        for space in self.get_spaces(&server_id).await? {
            self.delete_space(&space.id.to_string(), server_id).await?;
        }

        self.postgres_client
            .execute(
                "DELETE FROM servers WHERE id = $1",
                &[&Uuid::from_str(server_id)?],
            )
            .await
            .map_err(|e| anyhow!("{e}: coudn't delete server '{server_id}'"))?;
        Ok(())
    }

    pub async fn delete_space_by_id(&self, id: &str) -> anyhow::Result<()> {
        self.postgres_client
            .execute("DELETE FROM space WHERE id = $1;", &[&Uuid::from_str(id)?])
            .await
            .map_err(|e| anyhow!("{e}: coudn't delete space '{id}'"))?;
        Ok(())
    }

    pub async fn leave_server(&self, server: &str, user_id: &UserId) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "DELETE FROM user_server WHERE user_id = $1 AND server_id = $2;",
                &[&user_id.inner(), &Uuid::from_str(server)?],
            )
            .await
            .map_err(|e| anyhow!("{e}: coudn't leave server '{server}'"))?;
        Ok(())
    }

    async fn check_user_exist(&self, username: &str) -> anyhow::Result<bool> {
        let locked_postgres = self.postgres_client.clone();
        Ok(locked_postgres
            .execute("SELECT name from users WHERE name=$1;", &[&username])
            .await?
            .ne(&0))
    }

    async fn message_exist(&self, id: &Uuid) -> anyhow::Result<bool> {
        let locked_postgres = self.postgres_client.clone();
        Ok(locked_postgres
            .execute("SELECT name FROM messages WHERE id=$1;", &[&id])
            .await?
            .ne(&0))
    }

    async fn space_contain_message(
        &self,
        space_id: &Uuid,
        message_id: &Uuid,
    ) -> anyhow::Result<bool> {
        let locked_postgres = self.postgres_client.clone();
        Ok(locked_postgres
            .execute(
                "SELECT * FROM messages WHERE id=$1 AND space_id=$2;",
                &[&message_id, &space_id],
            )
            .await
            .map_err(|_| anyhow!("Error: Space {space_id} doesn't contain message {message_id}"))?
            .ne(&0))
    }

    pub async fn get_user_by_username(&self, username: &str) -> anyhow::Result<InternalUser> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT id, name, password_hash from users where name = $1",
                &[&username],
            )
            .await?;

        let id: Uuid = row.get::<usize, Uuid>(0);
        let id = UserId::from(id);
        let name: String = row.get::<usize, String>(1);
        let password_hash: String = row.get::<usize, String>(2);
        Ok(InternalUser {
            id,
            name,
            password_hash,
        })
    }

    pub async fn get_user_by_id(&self, id: &Uuid) -> anyhow::Result<User> {
        let row = self
            .postgres_client
            .query_one("SELECT id, name from users where id = $1", &[&id])
            .await?;

        let id: Uuid = row.get::<usize, Uuid>(0);
        let name: String = row.get::<usize, String>(1);
        Ok(User::new(name, id, None)) // FIXME: add metadata
    }

    pub async fn get_spaces(&self, server_id: &str) -> anyhow::Result<Vec<Space>> {
        let locked_postgres = self.postgres_client.clone();
        let rows = locked_postgres
            .query(
                "SELECT id, name, topic, creator, creation_time, bucket_id from space where server_id = $1",
                &[&uuid::Uuid::from_str(&server_id).unwrap()],
            )
            .await?;

        let mut spaces = vec![];
        for row in rows {
            let space_id: Uuid = row.get::<usize, Uuid>(0);
            let space_name: String = row.get::<usize, String>(1);
            let space_topic: Option<String> = row.get::<usize, Option<String>>(2);
            let space_creator: Uuid = row.get::<usize, Uuid>(3);
            let creation_time: NaiveDateTime = row.get::<usize, DateTime<Utc>>(4).naive_utc();
            let bucket: Uuid = row.get::<usize, Uuid>(5);
            spaces.push(Space::new(
                space_id,
                space_name,
                space_topic,
                space_creator,
                creation_time,
                bucket,
            ))
        }

        Ok(spaces)
    }

    pub async fn register_user(
        &self,
        username: &str,
        password: &str,
        id: &Uuid,
    ) -> anyhow::Result<Token> {
        if self.check_user_exist(username).await? {
            eprintln!("Username {} already exist", username);
            return Err(anyhow!("Username already exist"));
        }

        let password = hash(password, DEFAULT_COST)?;
        self.postgres_client
            .execute(
                "INSERT INTO users(id, name, password_hash) VALUES($1, $2, $3);",
                &[&id, &username, &password],
            )
            .await?;

        let id = self.get_user_by_username(username).await?.id;
        println!("ID: {id} USER: {username}");

        self.register_token_for_user(&id).await
    }

    pub async fn update_server_metadata(
        &self,
        server_id: &ServerId,
        metadata: &ServerMetadata,
    ) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO server_metadata(id, image, server_bans_id) VALUES($1, $2, $3) 
                on conflict (id) do update set
                id = excluded.id,
                image = excluded.image,
                server_bans_id = excluded.server_bans_id;",
                &[
                    &metadata.metadata_id,
                    &metadata.image,
                    &metadata.server_ban_id,
                ],
            )
            .await?;

        self.postgres_client
            .execute(
                "UPDATE servers SET metadata = $1 WHERE id = $2;",
                &[&metadata.metadata_id, &server_id.inner()],
            )
            .await?;

        println!("fkfkfk");
        Ok(())
    }

    // create new table entry of user_metadata
    // update users with that table entry
    pub async fn update_user_metadata(&self, metadata: &UserMetadata) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO user_metadata(id, nickname, bio, pfp, user_ban_id) VALUES($1, $2, $3, $4, $5) on conflict (id) do update set id = excluded.id, nickname = excluded.nickname, bio = excluded.bio, pfp = excluded.pfp, user_ban_id = excluded.user_ban_id;",
                &[&metadata.metadata_id, &metadata.nickname, &metadata.bio, &metadata.pfp, &metadata.user_ban_id],
            )
            .await?;

        Ok(())
    }

    pub async fn destroy_token(&self, token: &Token) -> anyhow::Result<()> {
        todo!()
    }

    pub async fn remove_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        todo!()
    }

    pub async fn logout_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        todo!()
    }

    pub async fn get_dms(
        &self,
        part1: &UserId,
        part2: &UserId,
        limit: &Option<i64>,
    ) -> anyhow::Result<Vec<DirectMessage>> {
        let limit = limit.unwrap_or(20);
        let rows = self
            .postgres_client
            .query(
                "SELECT
  dm.id           AS dm_id,
  dm.content,
  dm.sent_time,
  dm.sender_id,
  dm.receiver_id,
  dm.message_kind,
  sender.name         AS sender_name,
  receiver.name         AS receiver_name
FROM dm
JOIN users AS sender ON dm.sender_id = sender.id
JOIN users AS receiver ON dm.receiver_id = receiver.id
WHERE (dm.sender_id = $1 AND dm.receiver_id = $2) OR (dm.sender_id = $2 AND dm.receiver_id = $1)
ORDER BY dm.sent_time DESC LIMIT $3;",
                &[part1.inner(), part2.inner(), &limit],
            )
            .await?;

        let mut messages: Vec<DirectMessage> = vec![];

        for row in rows {
            let dm_id = row.get::<&str, Uuid>("dm_id");
            let content = row.get::<&str, String>("content");
            let sender_id = row.get::<&str, Uuid>("sender_id");
            let receiver_id = row.get::<&str, Uuid>("receiver_id");
            let sender_name = row.get::<&str, Option<String>>("sender_name");
            let receiver_name = row.get::<&str, Option<String>>("receiver_name");
            let kind = row.get::<&str, MessageKind>("message_kind");
            let sent_time = row.get::<&str, DateTime<Utc>>("sent_time").naive_utc();

            messages.push(DirectMessage::new(
                dm_id,
                sender_id,
                receiver_id,
                sender_name,
                receiver_name,
                content,
                kind,
                sent_time,
                None,
            ));
        }

        Ok(messages)
    }

    // TODO: update message to contain info replie
    pub async fn get_space_messages(
        &self,
        space_id: &Uuid,
        limit: &Option<i64>,
    ) -> anyhow::Result<Vec<SpaceMessage>> {
        let limit = limit.unwrap_or(20);
        let rows = self
            .postgres_client
            .query(
                "SELECT messages.id, messages.space_id, messages.content, messages.sender_id, messages.message_kind, messages.sent_time, users.name, replies.replying_to FROM messages 
                INNER JOIN users ON users.id = messages.sender_id AND messages.space_id = $1 
                LEFT JOIN message_metadata ON messages.metadata = message_metadata.id 
                LEFT JOIN replies ON message_metadata.reply = replies.id 
                ORDER BY messages.sent_time DESC LIMIT $2;",
                &[&space_id, &limit],
            )
            .await?;

        let mut messages = vec![];

        for row in rows {
            let id = row.get::<&str, Uuid>("id");
            let space_id = row.get::<&str, Uuid>("space_id");
            let content = row.get::<&str, String>("content");
            let sender_id = row.get::<&str, Uuid>("sender_id");
            let sender_name = row.get::<&str, String>("name");
            let kind = row.get::<&str, MessageKind>("message_kind");
            let sent_time = row.get::<&str, DateTime<Utc>>("sent_time").naive_utc();
            let replying_to = row.get::<&str, Option<Uuid>>("replying_to");
            messages.push(SpaceMessage::new(
                id,
                space_id,
                content,
                sender_id,
                Some(sender_name),
                kind.into(),
                sent_time,
                replying_to,
            ));
        }

        Ok(messages)
    }

    pub async fn login_user(&self, username: &str, password: &str) -> anyhow::Result<Token> {
        let rows = self
            .postgres_client
            .query(
                "SELECT id, password_hash FROM users WHERE name = $1;",
                &[&username],
            )
            // TODO: map the message error?
            .await?;

        for row in rows {
            let id = UserId::from(&row.get::<usize, Uuid>(0));
            let hashed_password: String = row.get::<usize, String>(1);
            println!("Hashed password: {hashed_password}");
            if verify(password, &hashed_password).unwrap_or(false) {
                return self.register_token_for_user(&id).await;
            }
        }

        Err(anyhow!("Wrong username or password"))
    }

    pub async fn get_or_create_token(&self, id: i64) -> anyhow::Result<Token> {
        todo!()
    }

    #[allow(unused)]
    pub async fn get_token_by_id(&self, id: i64) -> anyhow::Result<Token> {
        let rows = self
            .postgres_client
            .query(
                "SELECT token, creation_time, expiration_time FROM tokens WHERE id = $1;",
                &[&id],
            )
            .await?;

        for row in rows {
            let token: String = row.get::<usize, String>(0);
            let creation_time: chrono::DateTime<Utc> = row.get::<usize, chrono::DateTime<Utc>>(1);
            let expiration_time: chrono::DateTime<Utc> = row.get::<usize, chrono::DateTime<Utc>>(2);
            return Ok(Token {
                creation_time: creation_time.naive_utc(),
                expiration_time: expiration_time.naive_utc(),
                token,
            });
        }

        return Err(anyhow!("things went wrong"));
    }

    #[allow(unused)]
    pub async fn get_token_by_name(&self, username: &str) -> anyhow::Result<Token> {
        let rows = self.postgres_client
            .query("SELECT tokens.token, tokens.creation_time, tokens.expiration_time FROM tokens INNER JOIN users ON tokens.id = users.id AND users.name = $1;",
                &[&username],
            )
            .await?;

        for row in rows {
            let token: String = row.get::<usize, String>(0);
            let creation_time: chrono::DateTime<Utc> = row.get::<usize, chrono::DateTime<Utc>>(1);
            let expiration_time: chrono::DateTime<Utc> = row.get::<usize, chrono::DateTime<Utc>>(2);
            let now = Utc::now();
            match expiration_time.cmp(&now) {
                std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
                    return Ok(Token {
                        expiration_time: expiration_time.naive_utc(),
                        creation_time: creation_time.naive_utc(),
                        token,
                    });
                }
                std::cmp::Ordering::Less => return Err(anyhow!("Token expired")),
            }
        }
        Err(anyhow!("Account doesn't exist"))
    }

    pub async fn register_token_for_user(&self, id: &UserId) -> anyhow::Result<Token> {
        let token = Token::default();
        self.postgres_client
            .execute(
                "INSERT INTO tokens(id, token) Values($1, $2) on conflict (id) do update set
                token = excluded.token,
                creation_time = excluded.creation_time,
                expiration_time = excluded.expiration_time;",
                &[&id.inner(), &token.token],
            )
            .await?;
        Ok(token)
    }

    pub async fn friend_request_exist(&self, id: &Uuid) -> anyhow::Result<bool> {
        Ok(self
            .postgres_client
            .query_opt("SELECT id FROM friend_request WHERE id = $1", &[id])
            .await?
            .is_some())
    }

    pub async fn delete_friend_request(&self, id: &Uuid) -> anyhow::Result<()> {
        println!(
            "{}",
            format!("DELETE FROM friend_request WHERE id = '{}'", id)
        );
        self.postgres_client
            .execute("DELETE FROM friend_request WHERE id = $1", &[id])
            .await?;
        Ok(())
    }

    // if on it was this easy
    pub async fn make_friends(&self, lhs: &Uuid, rhs: &Uuid) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO friend_tuple (rhs, lhs) VALUES ($1, $2)",
                &[rhs, lhs],
            )
            .await?;
        Ok(())
    }

    pub async fn accept_friend_request(&self, request: &FriendRequest) -> anyhow::Result<()> {
        self.delete_friend_request(&request.request_id).await.unwrap();
        self.make_friends(&request.requester_id, &request.requested_id)
            .await.unwrap();
        Ok(())
    }

    pub async fn reject_friend_request(&self, request: &FriendRequest) -> anyhow::Result<()> {
        self.delete_friend_request(&request.request_id).await?;
        Ok(())
    }

    pub async fn cancle_friend_request(&self, request: &FriendRequest) -> anyhow::Result<()> {
        self.delete_friend_request(&request.request_id).await?;
        Ok(())
    }

    pub async fn get_incoming_friend_requests(
        &self,
        user_id: &Uuid,
        limit: &i64,
    ) -> anyhow::Result<Vec<FriendRequest>> {
        println!(
            "{}",
            format!(
                "SELECT id, requester, requested, request_time, request_message FROM friend_request WHERE requested = '{}' LIMIT {}",
                user_id, limit
            )
        );
        let rows = self
            .postgres_client
            .query(
                "SELECT id, requester, requested, request_time, request_message FROM friend_request WHERE requested = $1 LIMIT $2",
                &[&user_id, &limit],
            )
            .await?;

        let mut requests = vec![];
        for row in rows {
            let id = row.get::<&str, Uuid>("id");
            let requested_id = row.get::<&str, Uuid>("requested");
            let requester_id = row.get::<&str, Uuid>("requester");
            let request_time = row
                .try_get::<&str, DateTime<Utc>>("request_time")?
                .naive_utc();
            let request_message = row.get::<&str, Option<String>>("request_message");

            requests.push(FriendRequest::new(
                id,
                requester_id,
                requested_id,
                request_time,
                request_message,
            ));
        }

        Ok(requests)
    }

    pub async fn get_friend_request(&self, id: &Uuid) -> anyhow::Result<FriendRequest> {
        println!(
            "{}",
            format!(
                "SELECT id, requester, requested, request_time, request_message FROM friend_request WHERE id = '{}'",
                id
            )
        );

        let row = self
            .postgres_client
            .query_opt(
                "SELECT id, requester, requested, request_time, request_message FROM friend_request WHERE id = $1",
                &[id],
            )
            .await.unwrap();

        match row {
            Some(row) => {
                let id = row.get::<&str, Uuid>("id");
                let requested_id = row.get::<&str, Uuid>("requested");
                println!("requested: {:?}", requested_id);
                let requester_id = row.get::<&str, Uuid>("requester");
                println!("requester: {:?}", requester_id);
                let request_time = row
                    .try_get::<&str, DateTime<Utc>>("request_time")?
                    .naive_utc();
                println!("{:?}", request_time);
                let request_message = row.get::<&str, Option<String>>("request_message");
                println!("{:?}", request_message);

                Ok(FriendRequest::new(
                    id,
                    requester_id,
                    requested_id,
                    request_time,
                    request_message,
                ))
            }
            None => {
                return Err(anyhow::anyhow!("No friend request found"));
            }
        }
    }

    pub async fn get_user_id_from_token(&self, token: &str) -> anyhow::Result<UserId> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT id FROM tokens WHERE token = $1 and expiration_time >= NOW()",
                &[&token],
            )
            .await?;

        if row.is_empty() {
            return Err(anyhow!("Token expired or don't exist"));
        } else {
            return Ok(UserId::from(row.get::<usize, Uuid>(0)));
        }
    }

    pub async fn get_user_by_token(&self, token: &str) -> anyhow::Result<User> {
        let row = self.postgres_client
            .query_one(
                "SELECT users.name, users.id FROM users JOIN tokens ON tokens.token = $1 AND tokens.expiration_time >= NOW() AND users.id = tokens.id;",
                &[&token],
            )
            .await?;

        if row.is_empty() {
            return Err(anyhow!("Token expired or don't exist"));
        } else {
            let name = row.get::<usize, String>(0);
            let id = row.get::<usize, Uuid>(1);
            return Ok(User::new(name, id, None)); // FIXME: add metadata
        }
    }

    pub async fn get_server_id_from_space(&self, space_id: &Uuid) -> anyhow::Result<Uuid> {
        let row = self
            .postgres_client
            .query_one("SELECT server_id FROM space WHERE id = $1;", &[&space_id])
            .await?;

        if row.is_empty() {
            return Err(anyhow!("space {space_id} doesn't exist!"));
        } else {
            return Ok(row.get::<usize, Uuid>(0));
        }
    }

    // TODO: pass the whole request here instead of destructed fields
    pub async fn write_file_message(
        &self,
        server_id: &ServerId,
        space_id: &SpaceId,
        user_id: &UserId,
        message_kind: &MessageKind,
        key: &str,
    ) -> anyhow::Result<()> {
        if !self.contains_space(&server_id, &space_id).await? {
            return Err(anyhow!(
                "Server {server_id} does not have a space with id {space_id}"
            ));
        }

        self.postgres_client
            .execute(
                "INSERT INTO messages(space_id, message_kind, sender_id, content) Values($1, $2, $3, $4)",
                &[&space_id.inner(), &message_kind, &user_id.inner(), &key],
            )
            .await?;

        Ok(())
    }

    pub async fn write_dm(&self, sender: &UserId, dm: &WriteDM) -> anyhow::Result<Uuid> {
        let receiver = UserId::from(&dm.receiver);
        let message_id = Uuid::new_v4();
        self.user_exist(&sender).await?;
        self.user_exist(&receiver).await?;

        let kind: &MessageKind = &dm.message_kind.clone().into();
        let request = format!(
            "INSERT INTO dm(id, message_kind, sender_id, receiver_id, content) Values('{:?}', '{:?}', '{:?}', '{:?}', '{:?}')",
            &message_id,
            &kind,
            sender.inner(),
            receiver.inner(),
            &dm.message_content
        );
        println!("{request}");

        self.postgres_client
            .execute(
                "INSERT INTO dm(id, message_kind, sender_id, receiver_id, content) Values($1, $2, $3, $4, $5)",
                &[&message_id, &kind, sender.inner(), receiver.inner(), &dm.message_content],
            )
            .await?;
        Ok(message_id)
    }

    // TODO: update db with message_metadata and reply tables and alter the messages table to
    // reference metadata
    // TODO: this method is too complex
    pub async fn write_server_text_message(
        &self,
        user_id: &UserId,
        message: &WriteMessageRequest,
    ) -> anyhow::Result<Uuid> {
        let server_id = ServerId::try_from(&message.server_id)?;
        let space_id = SpaceId::try_from(&message.space_id)?;

        if !self.contains_space(&server_id, &space_id).await? {
            return Err(anyhow!(
                "Server {} does not have a space with id {}",
                message.server_id,
                message.space_id
            ));
        }

        if !self.is_joined(&server_id, &user_id).await? {
            return Err(anyhow!(
                "User {user_id} is not a member of {}",
                message.server_id,
            ));
        }

        let message_id = Uuid::new_v4();
        let kind: &MessageKind = &message.message_kind.clone().into();

        // if have a reply, write to message metadata then to replies table
        if let Some(replied_message_id) = &message.reply {
            if !self
                .space_contain_message(&Uuid::from_str(&message.space_id)?, &replied_message_id)
                .await?
            {
                return Err(anyhow!(
                    "Server {} does not have a space with id {}",
                    message.server_id,
                    message.space_id
                ));
            }

            let metadata_id = Uuid::new_v4();
            let reply_id = Uuid::new_v4();

            // TODO: remove the sql raw commands elswhere
            self.postgres_client
                .execute(
                    "INSERT INTO replies(id, replying_to) Values($1, $2)",
                    &[&reply_id, &replied_message_id],
                )
                .await?;

            // so the metadata refers to the reply which contains the original message and the
            // replied user
            self.postgres_client
                .execute(
                    "INSERT INTO message_metadata(id, reply) Values($1, $2)",
                    &[&metadata_id, &reply_id],
                )
                .await?;

            self.postgres_client
            .execute(
                "INSERT INTO messages(id, space_id, message_kind, sender_id, content, metadata) Values($1, $2, $3, $4, $5, $6);",
                &[&message_id, &Uuid::from_str(&message.space_id)?, &kind , &user_id.inner(), &message.message_content, &metadata_id],
            )
            .await?;
        } else {
            self.postgres_client
            .execute(
                "INSERT INTO messages(id, space_id, message_kind, sender_id, content) Values($1, $2, $3, $4, $5);",
                &[&message_id, &Uuid::from_str(&message.space_id)?, &kind , &user_id.inner(), &message.message_content],
            )
            .await?;
        }

        Ok(message_id)
    }

    pub async fn renew_token(&self, token: &str) -> anyhow::Result<Token> {
        if !self.token_exist_and_not_expired(&token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = UserId::from(uuid::Uuid::from_str(&token)?);
        self.register_token_for_user(&user_id).await
    }

    pub async fn token_exist_and_not_expired(&self, token: &str) -> anyhow::Result<bool> {
        let rows = self
            .postgres_client
            .query(
                "SELECT * FROM tokens WHERE token = $1 and expiration_time >= NOW()",
                &[&token],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    pub async fn get_joined_servers(&self, user_id: &UserId) -> anyhow::Result<Vec<Server>> {
        let rows = self
            .postgres_client
            .query(
                "SELECT server_id FROM user_server WHERE user_id = $1",
                &[&user_id.inner()],
            )
            .await
            .map_err(|_| anyhow!("Unable to locate user/server"))?;

        let mut servers = vec![];

        for row in rows {
            let server_id = row.get::<usize, Uuid>(0);
            servers.push(self.get_server(&server_id).await?);
        }

        return Ok(servers);
    }

    // TODO: sor by member count
    pub async fn get_available_servers(&self, limit: u64) -> anyhow::Result<Vec<Server>> {
        let limit = limit as i64;
        let rows = self
            .postgres_client
            .query(
                "SELECT id, name, creation_time, creator FROM servers LIMIT $1;",
                &[&limit],
            )
            .await
            .map_err(|_| anyhow!("Unable get a list of servers"))?;

        let mut servers = vec![];

        for row in rows {
            servers.push(Server::try_from(row)?);
        }

        return Ok(servers);
    }

    pub async fn get_created_servers(&self, token: &str) -> anyhow::Result<Vec<Server>> {
        let id = self.get_user_id_from_token(token).await?;

        let rows = self
            .postgres_client
            .query("SELECT * FROM servers WHERE creator = $1", &[&id.inner()])
            .await?;

        let mut servers = vec![];

        for row in rows {
            servers.push(Server::try_from(row)?)
        }

        return Ok(servers);
    }

    pub async fn user_exist(&self, user_id: &UserId) -> anyhow::Result<bool> {
        Ok(self
            .postgres_client
            .query("SELECT id FROM users WHERE id = $1", &[user_id.inner()])
            .await?
            .len()
            .eq(&1))
    }

    pub async fn server_exist(&self, server_id: &ServerId) -> anyhow::Result<bool> {
        Ok(self
            .postgres_client
            .query(
                "SELECT id FROM space WHERE server_id = $1",
                &[&server_id.inner()],
            )
            .await?
            .len()
            .eq(&1))
    }

    pub async fn contains_space(
        &self,
        server_id: &ServerId,
        space_id: &SpaceId,
    ) -> anyhow::Result<bool> {
        Ok(!self
            .postgres_client
            .query(
                "SELECT id FROM space WHERE id = $1 AND server_id = $2;",
                &[&space_id.inner(), &server_id.inner()],
            )
            .await?
            .is_empty())
    }

    pub async fn get_server_spaces(&self, server_id: &str) -> anyhow::Result<Vec<Space>> {
        let rows = self
            .postgres_client
            .query(
                "SELECT id, name, topic, creator, creation_time FROM space WHERE server_id = $1",
                &[&uuid::Uuid::from_str(server_id).unwrap()],
            )
            .await?;

        let mut servers = vec![];

        for row in rows {
            let space_id = row.get::<usize, Uuid>(0);
            let space_name = row.get::<usize, String>(1);
            let space_topic = row.get::<usize, Option<String>>(2);
            let creator = row.get::<usize, Uuid>(3);
            let creation_time = row.get::<usize, DateTime<Utc>>(4).naive_utc();
            let bucket_id = row.get::<usize, Uuid>(0);
            servers.push(Space::new(
                space_id,
                space_name,
                space_topic,
                creator,
                creation_time,
                bucket_id,
            ))
        }

        return Ok(servers);
    }

    async fn get_server_id(&self, server_name: &str, creator: &UserId) -> anyhow::Result<Uuid> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT id FROM servers WHERE name = $1 AND creator = $2;",
                &[&server_name, &creator.inner()],
            )
            .await
            .map_err(|e| anyhow!("{e}: Server '{}' not found!", server_name))?;
        let id = row.get::<usize, Uuid>(0);
        Ok(id)
    }

    pub async fn get_space_bucket_id(&self, server_id: &SpaceId) -> anyhow::Result<Uuid> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT bucket_id FROM space WHERE id = $1;",
                &[&server_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("{e}: Server '{}' not found!", server_id))?;
        let id = row.get::<usize, Uuid>(0);
        Ok(id)
    }

    async fn get_server(&self, server_id: &Uuid) -> anyhow::Result<Server> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT id, name, creation_time, creator FROM servers WHERE id = $1",
                &[&server_id],
            )
            .await
            .map_err(|_| anyhow!("Unable to locate server with id {server_id}"))?;

        Ok(Server::try_from(row)?)
    }

    pub async fn register_friend_request(
        &self,
        requester: &UserId,
        requested: &UserId,
        text: &Option<String>,
    ) -> anyhow::Result<Uuid> {
        let request_id = Uuid::new_v4();
        self.postgres_client
            .execute(
                "INSERT INTO friend_request(requester, requested, request_message) VALUES ($1, $2, $3);",
                &[requester.inner(), requested.inner(), text],
            )
            .await
            .map_err(|_| anyhow!("Unable to send friend request to'{}'", requested))?;
        Ok(request_id)
    }

    pub async fn are_friends(&self, rhs: &UserId, lhs: &UserId) -> anyhow::Result<bool> {
        println!(
            "{}",
            format!(
                "SELECT id FROM friend_tuple WHERE rhs = '{}' AND lhs = '{}' OR rhs = '{}' AND lhs = '{}';",
                rhs.inner(),
                lhs.inner(),
                lhs.inner(),
                rhs.inner()
            )
        );

        Ok(!self
            .postgres_client
            .query(
                "SELECT id FROM friend_tuple WHERE rhs = $1 AND lhs = $2 OR rhs = $2 AND lhs = $1;",
                &[rhs.inner(), lhs.inner()],
            )
            .await?
            .is_empty())
    }

    pub async fn get_friends(&self, user_id: &Uuid, limit: &i64) -> anyhow::Result<Vec<User>> {
        println!(
            "{}",
            format!(
                "SELECT * FROM friend_tuple WHERE rhs='{}' OR lhs='{}' LIMIT {}",
                user_id, user_id, limit
            )
        );
        let rows = self
            .postgres_client
            .query(
                "SELECT * FROM friend_tuple WHERE rhs=$1 OR lhs=$1 LIMIT $2",
                &[&user_id, limit],
            )
            .await?;

        let mut friends = vec![];
        for row in rows {
            let rhs = row.get::<&str, Uuid>("rhs");
            let lhs = row.get::<&str, Uuid>("lhs");
            let user = if &rhs == user_id {
                self.get_user_by_id(&lhs).await?
            } else {
                self.get_user_by_id(&rhs).await?
            };
            friends.push(user);
        }
        return Ok(friends);
    }

    pub async fn get_or_create_server_ban_id(&self, server_id: &ServerId) -> anyhow::Result<Uuid> {
        let row = self.postgres_client.query_opt(
            "select server_bans_id from server_metadata join servers on servers.metadata = servers.id AND = $1;",
            &[server_id.inner()],
        ).await?;

        match row {
            Some(row) => return Ok(row.get::<&str, Uuid>("banned_user_id")),
            None => {
                let metadata = &ServerMetadata::new(Uuid::new_v4(), Uuid::new_v4(), None);
                self.update_server_metadata(server_id, metadata).await?;
                return Ok(metadata.server_ban_id.clone());
            }
        }
    }

    pub async fn get_or_create_user_ban_id(&self, user_id: &UserId) -> anyhow::Result<Uuid> {
        let row = self.postgres_client.query_opt(
            "select user_ban_id from server_metadata join users on users.id = user_metadata.id AND = $1;",
            &[user_id.inner()],
        ).await?;

        match row {
            Some(row) => return Ok(row.get::<&str, Uuid>("user_ban_id")),
            None => {
                let ban_id = Uuid::new_v4();
                let metadata =
                    &UserMetadata::new(user_id.inner_clone(), None, None, None, Some(ban_id));
                self.update_user_metadata(metadata).await?;
                return Ok(ban_id.clone());
            }
        }
    }

    #[allow(unused)]
    pub async fn get_server_ban_id(&self, server_id: &Uuid) -> anyhow::Result<Option<Uuid>> {
        let row = self.postgres_client.query_opt(
            "select server_bans_id from server_metadata join servers on servers.metadata = servers.id AND = $1;",
            &[server_id],
        ).await?;

        match row {
            Some(row) => return Ok(row.get::<&str, Option<Uuid>>("banned_user_id")),
            None => return Ok(None),
        }
    }

    pub async fn banned_users(&self, server_id: &Uuid) -> anyhow::Result<Vec<User>> {
        let rows = self
            .postgres_client
            .query(
                "SELECT banned_user_id FROM server_ban WHERE banner_server_id=$1",
                &[&server_id],
            )
            .await?;
        let mut users = vec![];
        for row in rows {
            let banned_user_id = row.get::<&str, Uuid>("banned_user_id");
            let user = self.get_user_by_id(&banned_user_id).await?;
            users.push(user);
        }
        return Ok(users);
    }

    // Banning from server goes into two steps:
    // 1) getting the generic ban id of the server, each server have a unique ban id associated
    //    with it, if not found, one should be created
    // 2) inserting into server_ban the tuple (id, server_ban_id, user_id)
    pub async fn ban_user_from_server(
        &self,
        banned_id: &UserId,
        server_id: &ServerId,
    ) -> anyhow::Result<()> {
        let ban_id = self.get_or_create_server_ban_id(server_id).await?;

        println!(
            "{}",
            format!(
                "INSERT INTO server_ban(id, banner_server_id, banned_user_id) VALUES ('{}', '{}', '{}');",
                &ban_id, &server_id, &banned_id
            )
        );
        self.postgres_client
            .execute(
                "INSERT INTO server_ban(id, banner_server_id, banned_user_id) VALUES ($1, $2, $3);",
                &[&ban_id, &server_id.inner(), &banned_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("Unable to ban '{}' from '{}', {}", banned_id, server_id, e))?;

        Ok(())
    }

    pub async fn unban_user_from_server(
        &self,
        user_id: &UserId,
        server_id: &ServerId,
    ) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "DELETE FROM server_ban WHERE banner_server_id = $1 AND banned_user_id = $2;",
                &[&server_id.inner(), &user_id.inner()],
            )
            .await?;
        Ok(())
    }

    pub async fn ban_user_from_friends(
        &self,
        banner_id: &UserId,
        banned_id: &UserId,
    ) -> anyhow::Result<()> {
        let ban_id = self.get_or_create_user_ban_id(banner_id).await?;

        self.postgres_client
            .execute(
                "INSERT INTO user_ban(id, banner_user_id, banned_user_id) VALUES ($1, $2, $3);",
                &[&ban_id, &banner_id.inner(), &banned_id.inner()],
            )
            .await
            .map_err(|e| anyhow!("Unable to ban '{}' from '{}', {}", banned_id, banner_id, e))?;

        Ok(())
    }
}
