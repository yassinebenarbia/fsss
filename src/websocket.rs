// TODO: add update datetime in sql tables
// TODO: add previlages to returned joined server list
// TODO: rename register to signup
// TODO: remove the word request
use std::sync::Arc;

use anyhow::{Error, anyhow};
use chrono::{NaiveDateTime, Utc};
use futures_util::StreamExt;
use futures_util::{SinkExt, TryFutureExt, stream::SplitSink};
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, tungstenite};
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
    #[serde(skip_serializing)]
    bucket: Uuid,
}

impl Space {
    pub(crate) fn new(
        space_id: Uuid,
        space_name: String,
        space_topic: Option<String>,
        creator: i64,
        creation_time: NaiveDateTime,
        bucket: Uuid,
    ) -> Self {
        Self {
            id: space_id,
            name: space_name,
            topic: space_topic,
            creator,
            bucket,
            creation_time,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Message {
    id: Uuid,
    space_id: Uuid,
    content: String,
    sender: i64,
    kind: crate::postgres::MessageKind,
    sent_time: NaiveDateTime,
}

impl Message {
    pub fn new(
        id: Uuid,
        space_id: Uuid,
        content: String,
        sender: i64,
        kind: crate::postgres::MessageKind,
        sent_time: NaiveDateTime,
    ) -> Self {
        Self {
            id,
            space_id,
            content,
            sender,
            kind,
            sent_time,
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

impl Process for RegisterRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        postgres_client
            .register_user(&self.username, &self.password)
            .await
            .map(|_| Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateServerRequest {
    name: String,
}

impl Process for CreateServerRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        println!("ID: {id}");

        postgres_client
            .create_empty_server_and_join(&self.name, id)
            .await
            .map(Response::ServerId)
    }
}

#[derive(Deserialize, Serialize)]
pub struct RenewToken {}

impl Process for RenewToken {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
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

impl Process for CreateSpaceRequest {
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

        if !postgres_client.is_admin(&self.server, user_id).await? {
            return Err(anyhow!("Unseficcient previlages"));
        }

        s3_client
            .create_bucket_with_id()
            .and_then(|bucket_id| async move {
                let id = uuid::Uuid::new_v4();
                postgres_client
                    .create_empty_space_with_uuid(
                        &self.name,
                        &self.server,
                        user_id,
                        &bucket_id,
                        &id,
                    )
                    .await?;
                return Ok(id);
            })
            .await
            .map(Response::SpaceId)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct DeleteSpaceRequest {
    name: String,
    server: String,
}

impl Process for DeleteSpaceRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        if !postgres_client.is_admin(&self.server, id).await? {
            return Err(anyhow!("Unseficcient previlages"));
        }

        postgres_client
            .delete_space(&self.name, &self.server)
            .await
            .map(|_| Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListServerSpaces {
    server: String,
}

impl Process for ListServerSpaces {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        if !postgres_client.is_joined(&self.server, id).await? {
            return Err(anyhow!("You need to join to access server spaces"));
        }

        postgres_client
            .get_spaces(&self.server)
            .await
            .map(Response::SpacesList)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListCreatedServersRequest {}

impl Process for ListCreatedServersRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        postgres_client
            .get_created_servers(&token)
            .await
            .map(Response::ServersList)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ListJoinedServersRequest {}

impl Process for ListJoinedServersRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .get_joined_servers(&id)
            .await
            .map(Response::ServersList)
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct JoinServerRequest {
    id: String,
}

impl Process for JoinServerRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .join_server(&self.id, user_id, &Role::Member)
            .await
            .map(|_| Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct LeaveServerRequest {
    id: String,
}

impl Process for LeaveServerRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .leave_server(&self.id, user_id)
            .await
            .map(|_| Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum MessageKind {
    Text,
    Markdown,
}

impl Into<crate::postgres::MessageKind> for MessageKind {
    fn into(self) -> crate::postgres::MessageKind {
        match self {
            MessageKind::Text | MessageKind::Markdown => crate::postgres::MessageKind::TEXT,
        }
    }
}

// NOTE:
// hi catch this message // this will be sent over ws
// ..sending a text file // this will be sent over http
// did you get the message // this will be sent over ws
// HOW DO WE KNOW THE ORDER OF THE MESSAGE WITHOUT LETTING THE USER
// EXPLICITLY SPECIFY THE ORDER?
// SOLUTIONS:
// 1) have a list of messages UUIDs and MESSAGES table where it can specify the
// message type and metadta, then when we receieve a file over http, we just
// construct new message table entry and append the uuid to the space UUIDs
//
// NOTE: A USER NEED TO BE A MEMBER OF THE SERVER TO BE ABLE TO SEND MESSAGES
//
// NOTE: ADD STATUS LIKE MUTED AND BANNED PER SERVER/SPACE AND SPACE STATUS
// (slow_mode, text only, media only, etc.)
//
// NOTE: ONLY TEXT MESSAGES ARE ALLOWED TO BE SENT OVER WS
// ALL MEDIA MESSAGES (files) SHOULD BE FORWARDED THROUGH HTTP
// THROUGH THE /upload PATH
#[derive(Deserialize, Serialize, Debug)]
pub struct WriteMessageRequest {
    server_id: Uuid,
    space_id: Uuid,
    message_kind: MessageKind,
    message_content: String,
}

impl Process for WriteMessageRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .write_text_message(
                &self.server_id,
                &self.space_id,
                id,
                &self.message_kind.clone().into(),
                &self.message_content,
            )
            .await
            .map(|_| Response::Ok())
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct GetSpaceMessagesRequest {
    space_id: Uuid,
    limit: Option<u64>,
}

impl Process for GetSpaceMessagesRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = postgres_client.get_user_id_from_token(&token).await?;
        let server_id = postgres_client
            .get_server_id_from_space(&self.space_id)
            .await?;

        if !postgres_client
            .is_joined(&server_id.to_string(), user_id)
            .await?
        {
            return Err(anyhow!("user is not a server member!"));
        }

        postgres_client
            .get_space_messages(&self.space_id, &self.limit.map(|v| v as i64))
            .await
            .map(Response::MessageList)
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
pub enum MessageType {
    RegisterRequest {
        token: String,
        #[serde(flatten)]
        message: RegisterRequest,
    },
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
    ListJoinedServers {
        token: String,
        #[serde(flatten)]
        message: ListJoinedServersRequest,
    },
    ListServerSpaces {
        token: String,
        #[serde(flatten)]
        message: ListServerSpaces,
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
    SendMessage {
        token: String,
        #[serde(flatten)]
        message: WriteMessageRequest,
    },
    // TODO: add GetSerevrMessages
    GetSpaceMessages {
        token: String,
        #[serde(flatten)]
        message: GetSpaceMessagesRequest,
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

    // RwLock
    pub async fn process_v1(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
    ) -> Response {
        match self {
            // NOTE: called "process" methods should return Response only upon sucess, otherwise,
            // it should return an Err with error description.
            MessageType::RegisterRequest { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::LoginRequest(LoginRequest { username, password }) => {
                match self.login_user(username, password, postgres_client).await {
                    Ok(v) => Response::Token(v),
                    Err(e) => Response::Error(e.to_string()),
                }
            }
            MessageType::RenewToken { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::CreateServer { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::CreateSpace { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::DeleteSpace { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::ListServerSpaces { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::ListCreatedServers { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::ListJoinedServers { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::JoinServer { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::LeaveServer { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::SendMessage { token, message } => {
                process(message, token, postgres_client, s3_client).await
            }
            MessageType::GetSpaceMessages { token, message } => {
                process(message, token, postgres_client, s3_client).await
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
    SpaceId(Uuid),
    Token(Token),
    Error(String),
    ServersList(Vec<Server>),
    MessageList(Vec<Message>),
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
        write: &mut SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>,
    ) -> anyhow::Result<()> {
        if matches!(self, Self::Close()) {
            write.close().await?;
        } else if matches!(self, Self::Error(_)) {
            write
                .send(tungstenite::Message::text(&serde_json::to_string_pretty(
                    self,
                )?))
                .await?;
        } else {
            write
                .send(tungstenite::Message::text(&serde_json::to_string_pretty(
                    self,
                )?))
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

pub trait Process {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        token: &str,
    ) -> anyhow::Result<Response> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }
        Ok(Response::Ok())
    }
}

async fn process<T>(
    message: &T,
    token: &str,
    postgres_client: Arc<CustomPostgresClient>,
    s3_client: Arc<CustomS3client>,
) -> Response
where
    T: Process,
{
    match message.process(postgres_client, s3_client, token).await {
        Ok(v) => v,
        Err(e) => Response::Error(e.to_string()),
    }
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
            tungstenite::Message::Text(sbuf) => match serde_json::from_str(&sbuf) {
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
            tungstenite::Message::Close(_) => Response::close().send(&mut write).await?,
            _ => break,
        }
    }

    Response::close().send(&mut write).await?;
    Ok(())
}
