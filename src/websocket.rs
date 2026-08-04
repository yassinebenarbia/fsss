use std::fmt::Display;
use std::str::FromStr;
// TODO: add a subscription to redis space that contains user ID so that users can receive DM
// notifications via subscribing to that space
// TODO: add tests to check request consisten response format
// TODO: return user some details (name, id) per message, so you don't have to make many requests per message
// TODO: add update datetime in sql tables
// TODO: add previlages to returned joined server list
// TODO: rename register to signup
// TODO: remove the word request
use std::sync::Arc;

use anyhow::anyhow;
use chrono::{DateTime, NaiveDateTime, Utc};
use futures_util::StreamExt;
use futures_util::{SinkExt, stream::SplitSink};
use postgres::Row;
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, tungstenite};
use uuid::Uuid;

use crate::messages::create_server::CreateServerRequest;
use crate::messages::create_space::CreateSpaceRequest;
use crate::messages::login_request::LoginRequest;
use crate::postgres::FriendRequest;
use crate::redis::{CustomRedisClient, CustomRedisPubSink};
use crate::s3::CustomS3client;
use crate::{
    config::Config,
    messages::MessageType,
    postgres::{CustomPostgresClient, Role},
};

#[derive(Deserialize, Serialize, Clone)]
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
    creator: Uuid,
    creation_time: NaiveDateTime,
    // banned_users_list: Vec<Uuid>
}

impl TryFrom<Row> for Server {
    type Error = anyhow::Error;

    /// Expected Row Format
    /// [server_id, server_name, creation_time, creator]
    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let server_id = row.try_get::<usize, Uuid>(0)?;
        let server_name = row.try_get::<usize, String>(1)?;
        let creation_time = row.try_get::<usize, DateTime<Utc>>(2)?.naive_utc();
        let creator = row.try_get::<usize, Uuid>(3)?;

        Ok(Self::new(server_id, server_name, creation_time, creator))
    }
}

impl Server {
    pub(crate) fn new(
        server_id: Uuid,
        server_name: String,
        creation_time: NaiveDateTime,
        creator: Uuid,
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
    pub id: uuid::Uuid,
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    creator: Uuid,
    creation_time: NaiveDateTime,
    #[serde(skip_serializing)]
    bucket: Uuid,
}

impl Space {
    pub(crate) fn new(
        space_id: Uuid,
        space_name: String,
        space_topic: Option<String>,
        creator: Uuid,
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
pub struct DirectMessage {
    id: Uuid,
    sender: Uuid,
    receiver: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receiver_name: Option<String>,
    content: String,
    kind: crate::postgres::MessageKind,
    sent_time: NaiveDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    replying_to: Option<Uuid>,
}

impl DirectMessage {
    pub fn new(
        id: Uuid,
        sender: Uuid,
        receiver: Uuid,
        sender_name: Option<String>,
        receiver_name: Option<String>,
        content: String,
        kind: crate::postgres::MessageKind,
        sent_time: NaiveDateTime,
        replying_to: Option<Uuid>,
    ) -> Self {
        Self {
            id,
            sender,
            receiver,
            sender_name,
            receiver_name,
            content,
            kind,
            sent_time,
            replying_to,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct SpaceMessage {
    id: Uuid,
    #[serde(skip_serializing_if = "Option::is_none")]
    sender_name: Option<String>,
    space_id: Uuid,
    content: String,
    sender: Uuid,
    kind: crate::postgres::MessageKind,
    sent_time: NaiveDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    replying_to: Option<Uuid>,
}

impl SpaceMessage {
    pub fn new(
        id: Uuid,
        space_id: Uuid,
        content: String,
        sender: Uuid,
        name: Option<String>,
        kind: crate::postgres::MessageKind,
        sent_time: NaiveDateTime,
        replying_to: Option<Uuid>,
    ) -> Self {
        Self {
            id,
            space_id,
            sender_name: name,
            content,
            sender,
            kind,
            sent_time,
            replying_to,
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Authenticate {}

#[derive(Deserialize, Serialize)]
pub struct SpaceId(Uuid);

impl Display for SpaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&Uuid> for SpaceId {
    fn from(value: &Uuid) -> Self {
        Self(value.clone())
    }
}

impl From<Uuid> for SpaceId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl TryFrom<&str> for SpaceId {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self::from(Uuid::from_str(value)?))
    }
}

impl TryFrom<&String> for SpaceId {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Ok(Self::from(Uuid::from_str(value)?))
    }
}

impl SpaceId {
    pub fn inner(&self) -> &Uuid {
        &self.0
    }

    pub fn inner_clone(&self) -> Uuid {
        self.0.to_owned()
    }

    pub fn inner_move(self) -> Uuid {
        self.0
    }
}

// TODO: impl ToSql
#[derive(Deserialize, Serialize)]
pub struct ServerId(Uuid);

impl TryFrom<&str> for ServerId {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self::from(Uuid::from_str(value)?))
    }
}

impl TryFrom<&String> for ServerId {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Ok(Self::from(Uuid::from_str(value)?))
    }
}

impl Display for ServerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner())
    }
}

impl ServerId {
    pub fn inner(&self) -> &Uuid {
        &self.0
    }

    pub fn inner_clone(&self) -> Uuid {
        self.0.to_owned()
    }

    pub fn inner_move(self) -> Uuid {
        self.0
    }
}

impl From<&Uuid> for ServerId {
    fn from(value: &Uuid) -> Self {
        Self(value.clone())
    }
}

impl From<Uuid> for ServerId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl From<&UserId> for UserId {
    fn from(value: &UserId) -> Self {
        UserId(value.inner_clone())
    }
}

#[derive(Deserialize, Serialize, Debug)]
pub struct UserId(Uuid);

impl Display for UserId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner())
    }
}

impl UserId {
    pub fn inner_clone(&self) -> Uuid {
        self.0.to_owned()
    }

    pub fn inner_owned(self) -> Uuid {
        self.0
    }

    pub fn inner(&self) -> &Uuid {
        &self.0
    }
}

impl From<&Uuid> for UserId {
    fn from(value: &Uuid) -> Self {
        Self(value.clone())
    }
}

impl From<Uuid> for UserId {
    fn from(value: Uuid) -> Self {
        Self(value)
    }
}

impl TryFrom<&str> for UserId {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self::from(Uuid::from_str(value)?))
    }
}

impl TryFrom<&String> for UserId {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Ok(Self::from(Uuid::from_str(value)?))
    }
}

#[derive(Deserialize, Serialize)]
pub struct RestoreSessionRequest {
    token: String,
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
pub enum OriginRequestType {
    RegisterRequest,
    LoginRequest,
    RenewToken,
    CreateServer,
    CreateSpace,
    DeleteSpace,
    ListCreatedServers,
    ListJoinedServers,
    ListServerSpaces,
    JoinServer,
    LeaveServer,
    SendMServerMessage,
    SendDM,
    GetSpaceMessages,
    JoinSpace,
    ListAvailableServers,
    SubscribeToServer,
    LogoutRequest,
    ListServerMembers,
    SendFriendRequest,
    SearchUser,
    GetFriendRequests,
    GetFriendsList,
    JudgeFriendRequest,
    CancleFriendRequest,
    GetServerBanList,
    BanUserFromServer,
    UnbanUserFromServer,
    BanUserFromFriends,
}

#[derive(Deserialize, Serialize)]
pub struct VersionedMessage {
    pub version: f32,
    pub id: Option<String>,
    pub payload: MessageType,
}

impl VersionedMessage {
    fn randomize() -> Self {
        VersionedMessage {
            version: 1.0,
            id: None,
            payload: MessageType::LoginRequest {
                message: LoginRequest {
                    username: String::from("username"),
                    password: String::from("password"),
                },
            },
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct UserMetadata {
    pub metadata_id: Uuid,
    /// User Nickname
    pub nickname: Option<String>,
    /// Breef biography
    pub bio: Option<String>,
    /// UUID points to the pfp in the pool
    pub pfp: Option<Uuid>,
    pub user_ban_id: Option<Uuid>,
}

impl UserMetadata {
    pub fn new(
        user_id: Uuid,
        nickname: Option<String>,
        bio: Option<String>,
        pfp: Option<Uuid>,
        user_ban_id: Option<Uuid>,
    ) -> Self {
        Self {
            metadata_id: user_id,
            nickname,
            bio,
            pfp,
            user_ban_id,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct User {
    pub name: String,
    pub id: Uuid,
    metadata: Option<UserMetadata>, // FIXME: add status: ONLINE, OFFLINE, DND
}

impl TryFrom<postgres::Row> for User {
    type Error = anyhow::Error;

    fn try_from(value: Row) -> Result<Self, Self::Error> {
        let name = value.get::<&str, String>("name");
        let user_id = value.get::<&str, Uuid>("id");
        let nickname = value.get::<&str, Option<String>>("nickname");
        let bio = value.get::<&str, Option<String>>("bio");
        let pfp = value.get::<&str, Option<Uuid>>("pfp");
        let user_ban_id = value.get::<&str, Option<Uuid>>("user_ban_id");
        if nickname.is_none() && bio.is_none() {
            Ok(User::new(name, user_id, None))
        } else {
            Ok(User::new(
                name,
                user_id.clone(),
                Some(UserMetadata::new(user_id, nickname, bio, pfp, user_ban_id)),
            ))
        }
    }
}

impl User {
    pub fn new(name: String, id: Uuid, metadata: Option<UserMetadata>) -> Self {
        Self { name, id, metadata }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Member {
    pub name: String,
    pub id: Uuid,
    pub role: Role,
    pub banned: bool,
}

#[derive(Serialize)]
pub enum ResponseType {
    Notificaiton {
        #[serde(flatten)]
        notification: Notification,
    },
    MessageSent {
        original_request_type: OriginRequestType,
        message_id: Uuid,
    },
    Ok {
        original_request_type: OriginRequestType,
    },
    ServerJoined {
        original_request_type: OriginRequestType,
        spaces: Vec<Space>,
    },
    Close(),
    ServerId {
        original_request_type: OriginRequestType,
        server_id: Uuid,
    },
    SpaceId(Uuid),
    Token {
        original_request_type: OriginRequestType,
        associated_user_id: Uuid,
        token: Token,
    },
    Error(String),
    ServersList {
        original_request_type: OriginRequestType,
        servers: Vec<Server>,
    },
    SpaceMessageList {
        original_request_type: OriginRequestType,
        messages: Vec<SpaceMessage>,
    },
    DirectMessageList {
        original_request_type: OriginRequestType,
        messages: Vec<DirectMessage>,
    },
    MembersList {
        original_request_type: OriginRequestType,
        server_id: String,
        members: Vec<Member>,
    },
    SpacesList {
        original_request_type: OriginRequestType,
        spaces: Vec<Space>,
    },
    User {
        original_request_type: OriginRequestType,
        user: User,
    },
    Users {
        original_request_type: OriginRequestType,
        users: Vec<User>,
    },
    FriendRequestSent {
        original_request_type: OriginRequestType,
        request_id: Uuid,
        requested_id: Uuid,
    },
    FriendRequests {
        original_request_type: OriginRequestType,
        requests: Vec<FriendRequest>,
    },
    FriendsList {
        original_request_type: OriginRequestType,
        friends: Vec<User>,
    },
}

impl ResponseType {
    fn is_notification(&self) -> bool {
        matches!(self, ResponseType::Notificaiton { .. })
    }
}

#[derive(Serialize)]
pub struct Response<'a> {
    // FIXME: add flatten to this serde
    #[serde(borrow)]
    content: &'a ResponseType,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    is_notification: bool,
}

impl Response<'_> {
    fn print(self) -> Self {
        println!("{}", serde_json::to_string_pretty(&self).unwrap());
        self
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub enum Notification {
    ServerMessage(ServerMessageNotification),
    DM(DMNotification),
    ServerJoin(ServerJoinedNotification),
    SpaceCreated(SpaceCreatedNotification),
    Logout(LogoutNotification),
    FriendRequest(FriendRequestNotification),
    FriendRequestJudgement(FriendRequestJudgementNotification),
    FriendRequestCancled(FriendRequestCancledNotification),
}

pub trait AsNotification {
    fn as_notification(self) -> Notification;
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FriendRequestNotification {
    id: Uuid,
    requester: Uuid,
    requested: Uuid,
    request_time: NaiveDateTime,
    message: Option<String>,
}

impl FriendRequestNotification {
    pub fn new(
        id: Uuid,
        requester: Uuid,
        requested: Uuid,
        request_time: NaiveDateTime,
        message: Option<String>,
    ) -> Self {
        Self {
            id,
            requester,
            requested,
            request_time,
            message,
        }
    }
}

impl AsNotification for FriendRequestNotification {
    fn as_notification(self) -> Notification {
        Notification::FriendRequest(self)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct FriendRequestCancledNotification {
    id: Uuid,
    cancler: Uuid,
}

impl FriendRequestCancledNotification {
    pub fn new(id: Uuid, cancler: Uuid) -> Self {
        Self { id, cancler }
    }
}

impl AsNotification for FriendRequestCancledNotification {
    fn as_notification(self) -> Notification {
        Notification::FriendRequestCancled(self)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: move notifications to a directory
pub struct FriendRequestJudgementNotification {
    id: Uuid,
    accepted: bool,
}

impl FriendRequestJudgementNotification {
    pub fn new(id: Uuid, accepted: bool) -> Self {
        Self { id, accepted }
    }
}

impl AsNotification for FriendRequestJudgementNotification {
    fn as_notification(self) -> Notification {
        Notification::FriendRequestJudgement(self)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: make these a & instead of a moved values
pub struct DMNotification {
    content: String,
    sender: User,
    kind: crate::postgres::MessageKind,
    sent_time: NaiveDateTime,
}

impl DMNotification {
    pub fn new(
        content: String,
        sender: User,
        kind: crate::postgres::MessageKind,
        sent_time: NaiveDateTime,
    ) -> Self {
        Self {
            content,
            sender,
            kind,
            sent_time,
        }
    }
}

impl AsNotification for DMNotification {
    fn as_notification(self) -> Notification {
        Notification::DM(self)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: make these a & instead of a moved values
pub struct ServerMessageNotification {
    space_id: Uuid,
    server_id: Uuid,
    content: String,
    sender: User,
    kind: crate::postgres::MessageKind,
    sent_time: NaiveDateTime,
}

impl AsNotification for ServerMessageNotification {
    fn as_notification(self) -> Notification {
        Notification::ServerMessage(self)
    }
}

impl ServerMessageNotification {
    pub fn new(
        space_id: Uuid,
        server_id: Uuid,
        content: String,
        sender: User,
        kind: crate::postgres::MessageKind,
        sent_time: NaiveDateTime,
    ) -> Self {
        Self {
            space_id,
            server_id,
            content,
            sender,
            kind,
            sent_time,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: make these a & instead of a moved values
pub struct ServerJoinedNotification {
    server_id: Uuid,
    visitor: User,
    join_time: NaiveDateTime,
}

impl AsNotification for ServerJoinedNotification {
    fn as_notification(self) -> Notification {
        Notification::ServerJoin(self)
    }
}

impl ServerJoinedNotification {
    fn new(server_id: Uuid, visitor: User, joined_time: NaiveDateTime) -> Self {
        Self {
            server_id,
            visitor,
            join_time: joined_time,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: make these a & instead of a moved values
pub struct SpaceCreatedNotification {
    server_id: Uuid,
    space_id: Uuid,
    creation_time: NaiveDateTime,
    creator: User,
}

impl AsNotification for SpaceCreatedNotification {
    fn as_notification(self) -> Notification {
        Notification::SpaceCreated(self)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
// TODO: make these a & instead of a moved values
pub struct LogoutNotification {
    logout_time: NaiveDateTime,
    token: String,
    user: User,
}

impl LogoutNotification {
    pub fn new(logout_time: NaiveDateTime, token: String, user: User) -> Self {
        Self {
            logout_time,
            token,
            user,
        }
    }
}

impl AsNotification for LogoutNotification {
    fn as_notification(self) -> Notification {
        Notification::Logout(self)
    }
}

impl SpaceCreatedNotification {
    pub fn new(
        server_id: Uuid,
        space_id: Uuid,
        creation_time: NaiveDateTime,
        creator: User,
    ) -> Self {
        Self {
            server_id,
            space_id,
            creation_time,
            creator,
        }
    }
}

impl<'a> Response<'a> {
    async fn send(
        &self,
        write: &mut SplitSink<WebSocketStream<TcpStream>, tungstenite::Message>,
    ) -> anyhow::Result<()> {
        if matches!(self.content, ResponseType::Close()) {
            write.close().await?;
        } else if matches!(self.content, ResponseType::Error(_)) {
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

    fn with_id(mut self, id: Option<String>) -> Self {
        if id.is_some() {
            self.id = id
        };
        self
    }
}

impl From<anyhow::Error> for ResponseType {
    fn from(value: anyhow::Error) -> Self {
        Self::Error(value.to_string())
    }
}

impl ResponseType {
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

    fn as_response<'a>(&'a self) -> Response<'a> {
        Response {
            id: None,
            content: &self,
            is_notification: self.is_notification(),
        }
    }
}

mod tests {
    use crate::{
        api::{ResponseType, VersionedMessage},
        messages::list_created_servers::ListCreatedServersRequest,
    };

    #[test]
    fn test_response() {
        let s = serde_json::to_string_pretty(&ResponseType::Ok {
            original_request_type: crate::websocket::OriginRequestType::SendMServerMessage,
        })
        .unwrap();
        println!("{s}");

        let s =
            serde_json::to_string_pretty(&ResponseType::Error("Hello world".to_string())).unwrap();
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
            id: None,
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
            id: None,
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
            id: None,
            payload: super::MessageType::ListCreatedServers {
                token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
                    .to_string(),
                message: ListCreatedServersRequest {},
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
    redis_client: Arc<CustomRedisClient>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&(config.websocket.host.clone(), config.websocket.port))
        .await
        .unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        let s3_client_clone = s3_client.clone();
        let postgres_client_clone = postgres_client.clone();
        let redis_client_clone = redis_client.clone();

        tokio::spawn(async move {
            match handle_ws_connection(
                stream,
                s3_client_clone,
                postgres_client_clone,
                redis_client_clone,
            )
            .await
            {
                Ok(result) => {
                    println!("Connection handled successfully: {:?}", result);
                }
                Err(err) => {
                    eprintln!("Error handling connection: {:?}", err);
                }
            }
        });
    }

    Ok(())
}

pub trait Process {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        Ok(ResponseType::Ok {
            original_request_type: self.original_type(),
        })
    }

    fn original_type(&self) -> OriginRequestType;
}

async fn handle_ws_connection(
    stream: TcpStream,
    s3_client: Arc<CustomS3client>,
    postgres_client: Arc<CustomPostgresClient>,
    redis_client: Arc<CustomRedisClient>,
) -> anyhow::Result<()> {
    let ws_stream = tokio_tungstenite::accept_async(stream)
        .await
        .expect("Error during the websocket handshake occurred");
    let (mut write, mut read) = ws_stream.split();

    let pubsub = redis_client.client.get_async_pubsub().await?;
    let mut new_client = redis_client.clone();
    let (publish, mut subscribe) = pubsub.split();
    let mut pub_sink = CustomRedisPubSink::new(publish);

    // Notification life cycle:
    // a notification is published to the redis channel
    // the listener only listen to the channels he's interested in
    // if the listened channel and the published message meet, things happen :p
    loop {
        tokio::select! {
            // Event Type B: Global Alerts (Public)
            msg = subscribe.next() => {
                if let Some(redi_message) = msg {
                    println!("NOTIFICATION");
                    let payload: String = redi_message.get_payload().unwrap();
                    let notification: Notification = serde_json::from_str(&payload)?;
                    ResponseType::Notificaiton {notification: notification}
                        .as_response()
                            .print()
                            .send(&mut write)
                            .await?;
                }
            }
            Some(ws_message) = read.next() => {
                // TODO: make this ws only fn
                match ws_message? {
                    tungstenite::Message::Text(sbuf) => match serde_json::from_str(&sbuf) {
                        Ok(VersionedMessage {
                            version,
                            id,
                            payload,
                        }) => {
                            if version == 1.0 {
                                payload
                                    .process_v1(postgres_client.clone(), s3_client.clone(), &mut pub_sink, &mut new_client)
                                    .await
                                    .as_response()
                                    .with_id(id)
                                    .send(&mut write)
                                    .await?
                            } else {
                                ResponseType::Error(String::from("Wrong version"))
                                    .as_response()
                                    .with_id(id)
                                    .send(&mut write)
                                    .await?;
                            }
                        }
                        Err(e) => {
                            ResponseType::from(anyhow::anyhow!(e))
                                .as_response()
                                .send(&mut write)
                                .await?;
                            break;
                        }
                    },
                    tungstenite::Message::Close(_) => ResponseType::close().send(&mut write).await?,
                    _ => break,
                }
            },
        }
    }

    ResponseType::close().send(&mut write).await?;
    Ok(())
}
