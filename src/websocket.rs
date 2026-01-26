// TODO: add update datetime in sql tables
// TODO: rename register to signup
// TODO: remove the word request
use std::sync::Arc;

use anyhow::{Error, anyhow};
use chrono::{NaiveDateTime, Utc};
use futures_util::StreamExt;
use futures_util::{SinkExt, TryFutureExt, stream::SplitSink};
use minio::s3::Client as MinioClient;
use minio::s3::Client;
use postgres::types::Timestamp;
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio_postgres::Client as PostgresClient;
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use uuid::Uuid;

use crate::s3::CustomS3client;
use crate::{
    config::Config,
    postgres::{CustomPostgresClient, Role},
};

#[derive(Deserialize, Serialize)]
pub struct Token {
    pub expiration_time: NaiveDateTime,
    pub creation_time: NaiveDateTime,
    pub token: String,
}

impl Default for Token {
    fn default() -> Self {
        Token::new(chrono::Duration::days(1))
    }
}

impl Token {
    pub fn new(expiration_period: chrono::Duration) -> Self {
        let token = Alphanumeric.sample_string(&mut rand::rng(), 64);
        let creation_time = Utc::now().naive_utc();
        let expiration_time = creation_time
            .checked_add_signed(expiration_period)
            .unwrap_or(creation_time);
        Self {
            token,
            creation_time,
            expiration_time,
        }
    }
}

#[derive(Serialize)]
pub struct Server {
    id: uuid::Uuid,
    name: String,
    creator: i64,
    creation_time: NaiveDateTime,
}

impl Server {
    pub(crate) fn new(
        server_id: Uuid,
        server_name: String,
        creation_time: NaiveDateTime,
        creator: i64,
    ) -> Self {
        Self {
            id: server_id,
            name: server_name,
            creator,
            creation_time: creation_time,
        }
    }
}

#[derive(Serialize)]
pub struct Space {
    id: uuid::Uuid,
    name: String,
    topic: Option<String>,
    creator: i64,
    creation_time: NaiveDateTime,
}

impl Space {
    pub(crate) fn new(
        space_id: Uuid,
        space_name: String,
        space_topic: Option<String>,
        creator: i64,
        creation_time: NaiveDateTime,
    ) -> Self {
        Self {
            id: space_id,
            name: space_name,
            topic: space_topic,
            creator,
            creation_time,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Authenticate {}

#[derive(Deserialize, Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateServerRequest {
    name: String,
}

impl CreateServerRequest {
    /// Crates a server for the token owner.
    /// Failes if token is expired, or don't exist, or via internal error
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;
        let bucket_id = &uuid::Uuid::new_v4();
        s3_client.create_bucket(&bucket_id.to_string()).await?;

        println!("ID: {id}");

        match postgres_client
            .create_empty_server_and_join(&self.name, id, bucket_id)
            .await
        {
            Ok(v) => Ok(Response::ServerId(v)),
            Err(e) => {
                s3_client.remove_bucket(&bucket_id.to_string()).await?;
                Err(e)
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct RenewToken {}
impl RenewToken {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }
        let id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .register_token_for_user(id)
            .await
            .map(|v| Response::Token(v))
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateSpaceRequest {
    name: String,
    server: String,
}

impl CreateSpaceRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = postgres_client.get_user_id_from_token(token).await?;

        if !postgres_client.is_admin(user_id, &self.server).await? {
            return Err(anyhow!("Unseficcient previlages"));
        }

        let bucket_id = postgres_client
            .create_empty_space(&self.name, &self.server, user_id)
            .await
            .and(postgres_client.get_bucket_id(&self.server).await)?;

        match s3_client
            .create_folder(&self.name, &bucket_id.to_string())
            .await
        {
            Ok(_) => {}
            Err(e) => {
                postgres_client
                    .delete_space(&self.name, &self.server)
                    .await?;
                return Err(anyhow!("{e}"));
            }
        }

        Ok(Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DeleteSpaceRequest {
    name: String,
    server: String,
}

impl DeleteSpaceRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        if !postgres_client.is_admin(id, &self.server).await? {
            return Err(anyhow!("Unseficcient previlages"));
        }

        postgres_client
            .delete_space(&self.name, &self.server)
            .await?;

        Ok(Response::Ok())
    }
}

#[derive(Deserialize, Serialize)]
pub struct CreateBucketRequest {}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListCreatedServersRequest {}

impl ListCreatedServersRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        postgres_client
            .get_created_servers(&token)
            .await
            .map(|v| Response::ServersList(v))
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListServerSpacesRequest {
    id: String,
}

impl ListServerSpacesRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        postgres_client
            .get_server_spaces(&self.id)
            .await
            .map(|v| Response::SpacesList(v))
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct JoinServerRequest {
    id: String,
}

impl JoinServerRequest {
    /// Joins the server as a `member`
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }
        let user_id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .join_server(&self.id, user_id, &Role::Member)
            .await?;
        Ok(Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LeaveServerRequest {
    id: String,
}

impl LeaveServerRequest {
    /// Leaves the server as a `member`
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }
        let user_id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client.leave_server(&self.id, user_id).await?;
        Ok(Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
enum MessageKind {
    Text(String),
    Markdown(String),
    File { name: String },
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WriteMessageRequest {
    server_id: Uuid,
    space_id: Uuid,
    message_kind: MessageKind,
}

impl WriteMessageRequest {
    /// Leaves the server as a `member`
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        token: &str,
    ) -> anyhow::Result<Response> {
        todo!()
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub enum AfterToken {
    #[serde(untagged)]
    CreateServer(CreateServerRequest),
    #[serde(untagged)]
    CreateSpace {
        token: String,
        message: CreateSpaceRequest,
    },
    #[serde(untagged)]
    ListServers(),
}

#[derive(Deserialize, Serialize)]
pub enum TokanizedMessage {
    #[serde(untagged)]
    CreateServer {
        token: String,
        #[serde(flatten)]
        message: CreateServerRequest,
    },
    #[serde(untagged)]
    CreateSpace {
        token: String,
        #[serde(flatten)]
        message: CreateSpaceRequest,
    },
    #[serde(untagged)]
    ListCreatedServers(),
}

#[derive(Deserialize, Serialize)]
pub enum MessageType {
    RegisterRequest(RegisterRequest),
    LoginRequest(LoginRequest),
    RenewToken {
        token: String,
        #[serde(flatten)]
        message: RenewToken,
    },
    CreateServer {
        token: String,
        #[serde(flatten)]
        message: CreateServerRequest,
    },
    CreateSpace {
        token: String,
        #[serde(flatten)]
        message: CreateSpaceRequest,
    },
    DeleteSpace {
        token: String,
        #[serde(flatten)]
        message: DeleteSpaceRequest,
    },
    /// List servers of a given user
    ListCreatedServers {
        token: String,
        #[serde(flatten)]
        message: ListCreatedServersRequest,
    },
    ListServerSpaces {
        token: String,
        #[serde(flatten)]
        message: ListServerSpacesRequest,
    },
    JoinServer {
        token: String,
        #[serde(flatten)]
        message: JoinServerRequest,
    },
    LeaveServer {
        token: String,
        #[serde(flatten)]
        message: LeaveServerRequest,
    },
    WriteMessage {
        token: String,
        #[serde(flatten)]
        message: WriteMessageRequest,
    },
}

impl MessageType {
    pub async fn login_user(
        &self,
        username: &str,
        password: &str,
        postgres_client: Arc<CustomPostgresClient>,
    ) -> anyhow::Result<Token> {
        postgres_client.login_user(username, password).await
    }

    pub async fn register_user(
        &self,
        username: &str,
        password: &str,
        postgres_client: Arc<CustomPostgresClient>,
    ) -> anyhow::Result<()> {
        postgres_client.register_user(username, password).await
    }

    // RwLock
    pub async fn process_v1(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
    ) -> Response {
        match self {
            // NOTE: called "process" methods should return Response only upon sucess, otherwise,
            // it should return an Err with error description.
            MessageType::RegisterRequest(RegisterRequest { username, password }) => {
                // NOTE: can we pass the postgress and minio client over self instead?
                match self
                    .register_user(username, password, postgres_client)
                    .await
                {
                    Ok(_) => Response::Ok(),
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::LoginRequest(LoginRequest { username, password }) => {
                match self.login_user(username, password, postgres_client).await {
                    Ok(v) => Response::Token(v),
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::RenewToken { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::CreateServer { token, message } => {
                match message.process(postgres_client, s3_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::CreateSpace { token, message } => {
                match message.process(postgres_client, s3_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::DeleteSpace { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::ListCreatedServers { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::ListServerSpaces { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::JoinServer { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::LeaveServer { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::WriteMessage { token, message } => {
                match message.process(postgres_client, token).await {
                    Ok(v) => v,
                    Err(e) => Response::Error(e.to_string()),
                }
            }
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct VersionedMessage {
    pub version: f32,
    pub payload: MessageType,
}

impl VersionedMessage {
    fn randomize() -> Self {
        VersionedMessage {
            version: 1.0,
            payload: MessageType::LoginRequest(LoginRequest {
                username: String::from("username"),
                password: String::from("password"),
            }),
        }
    }
}

#[derive(Serialize)]
pub enum ResponseType {}

// #[serde(rename = "type")]
// _type: ResponseType,
// message: String,

#[derive(Serialize)]
pub enum Response {
    Ok(),
    Close(),
    ServerId(Uuid),
    Token(Token),
    Error(String),
    ServersList(Vec<Server>),
    SpacesList(Vec<Space>),
}

impl From<anyhow::Error> for Response {
    fn from(value: anyhow::Error) -> Self {
        Self::Error(value.to_string())
    }
}

impl Response {
    pub async fn send(
        &self,
        write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
    ) -> anyhow::Result<()> {
        if matches!(self, Self::Close()) {
            write.close().await?;
        } else if matches!(self, Self::Error(_)) {
            write
                .send(Message::text(&serde_json::to_string_pretty(self)?))
                .await?;
        } else {
            write
                .send(Message::text(&serde_json::to_string_pretty(self)?))
                .await?;
        }
        Ok(())
    }

    pub fn close() -> Self {
        Self::Close()
    }
}

mod tests {
    use crate::api::{Response, VersionedMessage};

    #[test]
    fn test_response() {
        let s = serde_json::to_string_pretty(&Response::Ok()).unwrap();
        println!("{s}");

        let s = serde_json::to_string_pretty(&Response::Error("Hello world".to_string())).unwrap();
        println!("{s}");
    }

    #[test]
    fn test_versioned() {
        let s = serde_json::to_string_pretty(&VersionedMessage::randomize()).unwrap();
        println!("{s}");
    }

    #[test]
    fn test_creeate_server() {
        let versioned_message = VersionedMessage {
            version: 1.0,
            payload: super::MessageType::CreateServer {
                token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
                    .to_string(),
                message: super::CreateServerRequest {
                    name: String::from("TheSpace"),
                },
            },
        };
        let s = serde_json::to_string_pretty(&versioned_message).unwrap();
        println!("{s}");
    }

    #[test]
    fn test_creeate_space() {
        let versioned_message = VersionedMessage {
            version: 1.0,
            payload: super::MessageType::CreateSpace {
                token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
                    .to_string(),
                message: super::CreateSpaceRequest {
                    name: String::from("TheSpace"),
                    server: String::from("31dcedb4-9968-4b1d-b5aa-571c2677bc9e"),
                },
            },
        };

        let s = serde_json::to_string_pretty(&versioned_message).unwrap();
        println!("{s}");
    }

    #[test]
    fn test_list_created_servers() {
        let versioned_message = VersionedMessage {
            version: 1.0,
            payload: super::MessageType::ListCreatedServers {
                token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
                    .to_string(),
                message: super::ListCreatedServersRequest {},
            },
        };

        let s = serde_json::to_string_pretty(&versioned_message).unwrap();
        println!("{s}");
    }
}

pub async fn spawn_ws_connection(
    config: &Config,
    s3_client: Arc<CustomS3client>,
    postgres_client: Arc<CustomPostgresClient>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&(config.websocket.host.clone(), config.websocket.port))
        .await
        .unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(handle_ws_connection(
            stream,
            s3_client.clone(),
            postgres_client.clone(),
        ));
    }

    Ok(())
}

async fn handle_ws_connection(
    mut stream: TcpStream,
    s3_client: Arc<CustomS3client>,
    postgres_client: Arc<CustomPostgresClient>,
) -> anyhow::Result<()> {
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");
    let (mut write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match message? {
            Message::Text(sbuf) => match serde_json::from_str(&sbuf) {
                Ok(VersionedMessage { version, payload }) => {
                    if version == 1.0 {
                        payload
                            .process_v1(postgres_client.clone(), s3_client.clone())
                            .await
                            .send(&mut write)
                            .await?
                    } else {
                        Response::Error(String::from("Wrong version"))
                            .send(&mut write)
                            .await?;
                    }
                }
                Err(e) => Response::from(anyhow::anyhow!(e)).send(&mut write).await?,
            },
            Message::Close(_) => Response::close().send(&mut write).await?,
            _ => break,
        }
    }

    Response::close().send(&mut write).await?;
    Ok(())
}
