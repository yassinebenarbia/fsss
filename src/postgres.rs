// TODO: inspect when the user already have that server created
// login
//      with username + password
//      with token
//      any login process should return a token
// sign up
// log out
use anyhow::anyhow;
use argon2::password_hash::{PasswordHash, SaltString};
use argon2::{Argon2, PasswordHasher};
use bcrypt::{DEFAULT_COST, hash, verify};
use chrono::{DateTime, NaiveDateTime, Utc};
use futures_util::TryFutureExt;
use postgres::Error;
use postgres::types::private::BytesMut;
use postgres::types::{FromSql, IsNull, ToSql, Type};
use rand::distr::{Alphanumeric, SampleString};
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::Client as PostgresClient;
use uuid::Uuid;

use crate::api::{Server, Space, Token};

pub struct User {
    pub name: String,
    pub id: i64,
    pub password_hash: String,
}

#[derive(Deserialize, Serialize, Debug)]
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

// NOTE: This abstraction exist to implement some frequetly used
// methods over the postgres db
pub struct CustomPostgresClient {
    postgres_client: Arc<PostgresClient>,
}

impl CustomPostgresClient {
    pub fn new(client: PostgresClient) -> Self {
        Self {
            postgres_client: Arc::new(client),
        }
    }

    pub async fn is_admin(&self, user_id: i64, server_id: &str) -> anyhow::Result<bool> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT role FROM user_server WHERE user_id = $1 AND server_id = $2",
                &[&user_id, &Uuid::from_str(server_id)?],
            )
            .await
            .map_err(|_| anyhow!("Unable to locate user/server"))?;

        let role = row.get::<usize, Role>(0);

        Ok(matches!(role, Role::Admin))
    }

    pub async fn create_empty_server(&self, name: &str, creator: i64) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO servers(name, creator) VALUES ($1, $2);",
                &[&name, &creator],
            )
            .await
            .map_err(|_| anyhow!("Unable to create server '{}'", name))?;

        Ok(())
    }

    pub async fn join_server(
        &self,
        server_id: &str,
        user_id: i64,
        role: &Role,
    ) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO user_server(server_id, user_id, role) VALUES ($1, $2, $3);",
                &[&Uuid::from_str(server_id)?, &user_id, &role],
            )
            .await
            .map_err(|_| anyhow!("Unable to join '{}'", server_id))?;

        Ok(())
    }

    /// Creates an empty server and joins as an admin
    pub async fn create_empty_server_and_join(
        &self,
        server_name: &str,
        creator: i64,
    ) -> anyhow::Result<Uuid> {
        self.create_empty_server(server_name, creator).await?;
        let server_id = self.get_server_id(server_name, creator).await?;
        self.join_server(&server_id.to_string(), creator, &Role::Admin)
            .await?;
        Ok(server_id)
    }

    pub async fn create_empty_space(
        &self,
        name: &str,
        server: &str,
        creator: i64,
        bucket_id: &Uuid,
    ) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "INSERT INTO space(creator, name, server_id, bucket_id) VALUES ($1, $2, $3, $4);",
                &[&creator, &name, &Uuid::from_str(server)?, bucket_id],
            )
            .await
            .map_err(|e| anyhow!("{e}: space already exist!"))?;
        Ok(())
    }

    pub async fn delete_space(&self, name: &str, server: &str) -> anyhow::Result<()> {
        self.postgres_client
            .execute(
                "DELETE FROM space WHERE name = $1 AND server_id = $2;",
                &[&name, &Uuid::from_str(server)?],
            )
            .await
            .map_err(|e| anyhow!("{e}: coudn't delete space '{name}'"))?;
        Ok(())
    }

    pub async fn delete_space_by_id(&self, id: &str) -> anyhow::Result<()> {
        self.postgres_client
            .execute("DELETE FROM space WHERE id = $1;", &[&Uuid::from_str(id)?])
            .await
            .map_err(|e| anyhow!("{e}: coudn't delete space '{id}'"))?;
        Ok(())
    }

    pub async fn leave_server(&self, server: &str, user_id: i64) -> anyhow::Result<()> {
        let locked_postgres = self.postgres_client.clone();

        locked_postgres
            .execute(
                "DELETE FROM user_server WHERE user_id = $1 AND server_id = $2;",
                &[&user_id, &Uuid::from_str(server)?],
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

    pub async fn get_user(&self, username: &str) -> anyhow::Result<User> {
        let locked_postgres = self.postgres_client.clone();
        let row = locked_postgres
            .query_one(
                "SELECT id, name, password_hash from users where name = $1",
                &[&username],
            )
            .await?;

        let id: i64 = row.get::<usize, i64>(0);
        let name: String = row.get::<usize, String>(1);
        let password_hash: String = row.get::<usize, String>(2);
        Ok(User {
            id,
            name,
            password_hash,
        })
    }

    pub async fn register_user(&self, username: &str, password: &str) -> anyhow::Result<()> {
        if self.check_user_exist(username).await? {
            eprintln!("Username {} already exist", username);
            return Err(anyhow!("Username already exist"));
        }

        let password = hash(password, DEFAULT_COST)?;
        let locked_postgres = self.postgres_client.clone();
        locked_postgres
            .execute(
                "INSERT INTO users(name, password_hash) VALUES($1, $2);",
                &[&username, &password],
            )
            .await?;

        let id = self.get_user(username).await?.id;
        println!("ID: {id} USER: {username}");

        self.register_token_for_user(id).await?;

        println!("user registered sucessfully");
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

    pub async fn login_user(&self, username: &str, password: &str) -> anyhow::Result<Token> {
        let locked_conn = self.postgres_client.clone();
        let rows = locked_conn
            .query(
                "SELECT id, password_hash FROM users WHERE name = $1;",
                &[&username],
            )
            .await?;

        for row in rows {
            let id: i64 = row.get::<usize, i64>(0);
            let hashed_password: String = row.get::<usize, String>(1);
            println!("Hashed password: {hashed_password}");
            if verify(password, &hashed_password).unwrap_or(false) {
                return self.register_token_for_user(id).await;
            }
        }

        Err(anyhow!("Wrong username or password"))
    }

    pub async fn get_or_create_token(&self, id: i64) -> anyhow::Result<Token> {
        todo!()
    }

    pub async fn get_token_by_id(&self, id: i64) -> anyhow::Result<Token> {
        let locked_conn = self.postgres_client.clone();
        let rows = locked_conn
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

    pub async fn get_token_by_name(&self, username: &str) -> anyhow::Result<Token> {
        let locked_conn = self.postgres_client.clone();
        let rows = locked_conn
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

    pub async fn register_token_for_user(&self, id: i64) -> anyhow::Result<Token> {
        let token = Token::default();
        let locked_conn = self.postgres_client.clone();
        locked_conn
            .execute(
                "INSERT INTO tokens(id, token) Values($1, $2) on conflict (id) do update set
                token = excluded.token,
                creation_time = excluded.creation_time,
                expiration_time = excluded.expiration_time;",
                &[&id, &token.token],
            )
            .await?;
        Ok(token)
    }

    pub async fn get_user_id_from_token(&self, token: &str) -> anyhow::Result<i64> {
        let locked_conn = self.postgres_client.clone();
        let row = locked_conn
            .query_one(
                "SELECT id FROM tokens WHERE token = $1 and expiration_time >= NOW()",
                &[&token],
            )
            .await?;

        if row.is_empty() {
            return Err(anyhow!("Token expired or don't exist"));
        } else {
            return Ok(row.get::<usize, i64>(0));
        }
    }

    pub async fn token_exist_and_not_expired(&self, token: &str) -> anyhow::Result<bool> {
        let locked_conn = self.postgres_client.clone();
        let rows = locked_conn
            .query(
                "SELECT * FROM tokens WHERE token = $1 and expiration_time >= NOW()",
                &[&token],
            )
            .await?;
        Ok(!rows.is_empty())
    }

    fn create_token(&self, username: &str) -> anyhow::Result<Token> {
        // let locked_conn = self.postgres_client.clone();
        // let rows = locked_conn
        //     .query("SELECT tokens.token, tokens.creation_time, tokens.expiration_time FROM tokens INNER JOIN users ON tokens.id = users.id AND users.name = $1;",
        //         &[&username],
        //     )
        //     .await?;
        // Token::default();

        // for row in rows {
        //     let token: String = row.get::<usize, String>(0);
        //     let creation_time: chrono::DateTime<Utc> = row.get::<usize, chrono::DateTime<Utc>>(1);
        //     let expiration_time: chrono::DateTime<Utc> = row.get::<usize, chrono::DateTime<Utc>>(2);
        //     let now = Utc::now();
        //     match expiration_time.cmp(&now) {
        //         std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
        //             return Ok(Token {
        //                 expiration_time: expiration_time.naive_utc(),
        //                 creation_time: creation_time.naive_utc(),
        //                 token,
        //             });
        //         }
        //         std::cmp::Ordering::Less => return Err(anyhow!("Token expired")),
        //     }
        // }
        // Err(anyhow!("Account doesn't exist"))
        todo!()
    }

    pub async fn get_created_servers(&self, token: &str) -> anyhow::Result<Vec<Server>> {
        let id = self.get_user_id_from_token(token).await?;
        let rows = self
            .postgres_client
            .query("SELECT * FROM servers WHERE creator = $1", &[&id])
            .await?;

        let mut servers = vec![];

        for row in rows {
            let server_id = row.get::<usize, Uuid>(0);
            let server_name = row.get::<usize, String>(1);
            let creation_time = row.get::<usize, chrono::DateTime<Utc>>(2).naive_utc();
            let cerator = id;
            servers.push(Server::new(server_id, server_name, creation_time, cerator))
        }

        return Ok(servers);
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
            let creator = row.get::<usize, i64>(3);
            let creation_time = row.get::<usize, chrono::DateTime<Utc>>(4).naive_utc();
            servers.push(Space::new(
                space_id,
                space_name,
                space_topic,
                creator,
                creation_time,
            ))
        }

        return Ok(servers);
    }

    async fn get_server_id(&self, server_name: &str, creator: i64) -> anyhow::Result<Uuid> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT id FROM servers WHERE name = $1 AND creator = $2;",
                &[&server_name, &creator],
            )
            .await
            .map_err(|e| anyhow!("{e}: Server '{}' not found!", server_name))?;
        let id = row.get::<usize, Uuid>(0);
        Ok(id)
    }

    pub async fn get_bucket_id(&self, server_id: &str) -> anyhow::Result<Uuid> {
        let row = self
            .postgres_client
            .query_one(
                "SELECT bucket_id FROM servers WHERE id = $1;",
                &[&Uuid::from_str(server_id).unwrap()],
            )
            .await
            .map_err(|e| anyhow!("{e}: Server '{}' not found!", server_id))?;
        let id = row.get::<usize, Uuid>(0);
        Ok(id)
    }
}
