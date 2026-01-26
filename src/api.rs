// TODO: add update datetime in sql tables
// TODO: rename register to signup
// TODO: remove the word request

pub use super::http::*;
pub use super::websocket::*;
// use std::sync::Arc;
//
// use anyhow::{Error, anyhow};
// use chrono::{NaiveDateTime, Utc};
// use futures_util::{SinkExt, TryFutureExt, stream::SplitSink};
// use minio::s3::Client as MinioClient;
// use minio::s3::Client;
// use postgres::types::Timestamp;
// use rand::distr::{Alphanumeric, SampleString};
// use serde::{Deserialize, Serialize};
// use time::Duration;
// use tokio::{
//     net::TcpStream,
//     sync::{Mutex, RwLock},
// };
// use tokio_postgres::Client as PostgresClient;
// use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
// use uuid::Uuid;
//
// use crate::postgres::{CustomPostgresClient, Role};
//
// #[derive(Deserialize, Serialize)]
// pub struct Token {
//     pub expiration_time: NaiveDateTime,
//     pub creation_time: NaiveDateTime,
//     pub token: String,
// }
//
// impl Default for Token {
//     fn default() -> Self {
//         Token::new(chrono::Duration::days(1))
//     }
// }
//
// impl Token {
//     pub fn new(expiration_period: chrono::Duration) -> Self {
//         let token = Alphanumeric.sample_string(&mut rand::rng(), 64);
//         let creation_time = Utc::now().naive_utc();
//         let expiration_time = creation_time
//             .checked_add_signed(expiration_period)
//             .unwrap_or(creation_time);
//         Self {
//             token,
//             creation_time,
//             expiration_time,
//         }
//     }
// }
//
// #[derive(Serialize)]
// pub struct Server {
//     id: uuid::Uuid,
//     name: String,
//     creator: i64,
//     creation_time: NaiveDateTime,
// }
//
// impl Server {
//     pub(crate) fn new(
//         server_id: Uuid,
//         server_name: String,
//         creation_time: NaiveDateTime,
//         creator: i64,
//     ) -> Self {
//         Self {
//             id: server_id,
//             name: server_name,
//             creator,
//             creation_time: creation_time,
//         }
//     }
// }
//
// #[derive(Serialize)]
// pub struct Space {
//     id: uuid::Uuid,
//     name: String,
//     topic: Option<String>,
//     creator: i64,
//     creation_time: NaiveDateTime,
// }
//
// impl Space {
//     pub(crate) fn new(
//         space_id: Uuid,
//         space_name: String,
//         space_topic: Option<String>,
//         creator: i64,
//         creation_time: NaiveDateTime,
//     ) -> Self {
//         Self {
//             id: space_id,
//             name: space_name,
//             topic: space_topic,
//             creator,
//             creation_time,
//         }
//     }
// }
//
// #[derive(Deserialize, Serialize)]
// pub struct Authenticate {}
//
// #[derive(Deserialize, Serialize)]
// pub struct LoginRequest {
//     pub username: String,
//     pub password: String,
// }
//
// #[derive(Deserialize, Serialize)]
// pub struct RegisterRequest {
//     pub username: String,
//     pub password: String,
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct CreateServerRequest {
//     name: String,
// }
//
// impl CreateServerRequest {
//     /// Crates a server for the token owner.
//     /// Failes if token is expired, or don't exist, or via internal error
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//
//         let id = postgres_client.get_user_id_from_token(token).await?;
//
//         println!("ID: {id}");
//
//         Ok(Response::ServerId(
//             postgres_client
//                 .create_empty_server_and_join(&self.name, id)
//                 .await?,
//         ))
//     }
// }
//
// #[derive(Deserialize, Serialize)]
// pub struct RenewToken {}
// impl RenewToken {
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//         let id = postgres_client.get_user_id_from_token(token).await?;
//
//         postgres_client
//             .register_token_for_user(id)
//             .await
//             .map(|v| Response::Token(v))
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct CreateSpaceRequest {
//     name: String,
//     server: String,
// }
//
// impl CreateSpaceRequest {
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//
//         let id = postgres_client.get_user_id_from_token(token).await?;
//
//         if !postgres_client.is_admin(id, &self.server).await? {
//             return Err(anyhow!("Unseficcient previlages"));
//         }
//
//         postgres_client
//             .create_empty_space(&self.name, &self.server, id)
//             .await?;
//         Ok(Response::Ok())
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct DeleteSpaceRequest {
//     name: String,
//     server: String,
// }
//
// impl DeleteSpaceRequest {
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//
//         let id = postgres_client.get_user_id_from_token(token).await?;
//
//         if !postgres_client.is_admin(id, &self.server).await? {
//             return Err(anyhow!("Unseficcient previlages"));
//         }
//
//         postgres_client
//             .delete_space(&self.name, &self.server)
//             .await?;
//
//         Ok(Response::Ok())
//     }
// }
//
// #[derive(Deserialize, Serialize)]
// pub struct CreateBucketRequest {}
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct ListCreatedServersRequest {}
//
// impl ListCreatedServersRequest {
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//
//         postgres_client
//             .get_created_servers(&token)
//             .await
//             .map(|v| Response::ServersList(v))
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct ListServerSpacesRequest {
//     id: String,
// }
//
// impl ListServerSpacesRequest {
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//
//         postgres_client
//             .get_server_spaces(&self.id)
//             .await
//             .map(|v| Response::SpacesList(v))
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct JoinServerRequest {
//     id: String,
// }
//
// impl JoinServerRequest {
//     /// Joins the server as a `member`
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//         let user_id = postgres_client.get_user_id_from_token(token).await?;
//
//         postgres_client
//             .join_server(&self.id, user_id, &Role::Member)
//             .await?;
//         Ok(Response::Ok())
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct LeaveServerRequest {
//     id: String,
// }
//
// impl LeaveServerRequest {
//     /// Leaves the server as a `member`
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         if !postgres_client.token_exist_and_not_expired(token).await? {
//             return Err(anyhow!("Token does not exist or expired!"));
//         }
//         let user_id = postgres_client.get_user_id_from_token(token).await?;
//
//         postgres_client.leave_server(&self.id, user_id).await?;
//         Ok(Response::Ok())
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// enum MessageKind {
//     Text(String),
//     Markdown(String),
//     File { name: String },
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub struct WriteMessageRequest {
//     server_id: Uuid,
//     space_id: Uuid,
//     message_kind: MessageKind,
// }
//
// impl WriteMessageRequest {
//     /// Leaves the server as a `member`
//     async fn process(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//         token: &str,
//     ) -> anyhow::Result<Response> {
//         todo!()
//     }
// }
//
// #[derive(Deserialize, Serialize, Debug)]
// pub enum AfterToken {
//     #[serde(untagged)]
//     CreateServer(CreateServerRequest),
//     #[serde(untagged)]
//     CreateSpace {
//         token: String,
//         message: CreateSpaceRequest,
//     },
//     #[serde(untagged)]
//     ListServers(),
// }
//
// #[derive(Deserialize, Serialize)]
// pub enum TokanizedMessage {
//     #[serde(untagged)]
//     CreateServer {
//         token: String,
//         #[serde(flatten)]
//         message: CreateServerRequest,
//     },
//     #[serde(untagged)]
//     CreateSpace {
//         token: String,
//         #[serde(flatten)]
//         message: CreateSpaceRequest,
//     },
//     #[serde(untagged)]
//     ListCreatedServers(),
// }
//
// // #[derive(Deserialize, Serialize)]
// // pub struct TokanizedMessage {
// //     pub token: String,
// //     #[serde(flatten)]
// //     pub message: AfterToken,
// // }
// impl TokanizedMessage {
//     pub async fn process_tokenized(
//         &self,
//         postgres_client: Arc<CustomPostgresClient>,
//     ) -> anyhow::Result<Response> {
//         match &self {
//             // AfterToken::CreateServer({token, create_server_request}) => {
//             //     if !postgres_client
//             //         .token_exist_and_not_expired(&self.token.to_string())
//             //         .await?
//             //     {
//             //         return Err(anyhow!("Token does not exist or expired!"));
//             //     }
//             //     let id = postgres_client
//             //         .get_id_from_token(&self.token.to_string())
//             //         .await?;
//             //     println!("ID: {id}");
//             //
//             //     postgres_client
//             //         .create_empty_server(&create_server_request.name, id)
//             //         .await?;
//             //     Ok(Response::Ok())
//             // }
//             // AfterToken::CreateSpace(create_space_request) => {
//             //     let id = postgres_client
//             //         .get_id_from_token(&self.token.to_string())
//             //         .await?;
//             //     println!("hiofsdf");
//             //
//             //     postgres_client
//             //         .create_empty_space(
//             //             &create_space_request.name,
//             //             &create_space_request.server,
//             //             id,
//             //         )
//             //         .await?;
//             //     Ok(Response::Ok())
//             // }
//             // AfterToken::ListServers() => todo!(),
//             TokanizedMessage::CreateServer { token, message } => {
//                 if !postgres_client.token_exist_and_not_expired(token).await? {
//                     return Err(anyhow!("Token does not exist or expired!"));
//                 }
//
//                 let id = postgres_client.get_user_id_from_token(token).await?;
//
//                 println!("ID: {id}");
//
//                 postgres_client
//                     .create_empty_server_and_join(&message.name, id)
//                     .await?;
//                 Ok(Response::Ok())
//             }
//             TokanizedMessage::CreateSpace { token, message } => {
//                 let id = postgres_client.get_user_id_from_token(token).await?;
//                 println!("hiofsdf");
//
//                 postgres_client
//                     .create_empty_space(&message.name, &message.server, id)
//                     .await?;
//                 Ok(Response::Ok())
//             }
//             TokanizedMessage::ListCreatedServers() => todo!(),
//         }
//     }
// }
//
// #[derive(Deserialize, Serialize)]
// pub enum MessageType {
//     RegisterRequest(RegisterRequest),
//     LoginRequest(LoginRequest),
//     RenewToken {
//         token: String,
//         #[serde(flatten)]
//         message: RenewToken,
//     },
//     CreateServer {
//         token: String,
//         #[serde(flatten)]
//         message: CreateServerRequest,
//     },
//     CreateSpace {
//         token: String,
//         #[serde(flatten)]
//         message: CreateSpaceRequest,
//     },
//     DeleteSpace {
//         token: String,
//         #[serde(flatten)]
//         message: DeleteSpaceRequest,
//     },
//     /// List servers of a given user
//     ListCreatedServers {
//         token: String,
//         #[serde(flatten)]
//         message: ListCreatedServersRequest,
//     },
//     ListServerSpaces {
//         token: String,
//         #[serde(flatten)]
//         message: ListServerSpacesRequest,
//     },
//     JoinServer {
//         token: String,
//         #[serde(flatten)]
//         message: JoinServerRequest,
//     },
//     LeaveServer {
//         token: String,
//         #[serde(flatten)]
//         message: LeaveServerRequest,
//     },
//     WriteMessage {
//         token: String,
//         #[serde(flatten)]
//         message: WriteMessageRequest,
//     },
// }
//
// impl MessageType {
//     pub async fn process_tokenized(
//         &self,
//         tokenized: &TokanizedMessage,
//         postgres_client: Arc<CustomPostgresClient>,
//     ) -> anyhow::Result<Response> {
//         todo!()
//         // match &self {
//         //     TokanizedMessage::CreateServer { token, message } => {
//         //         if !postgres_client.token_exist_and_not_expired(token).await? {
//         //             return Err(anyhow!("Token does not exist or expired!"));
//         //         }
//         //
//         //         let id = postgres_client.get_id_from_token(token).await?;
//         //
//         //         println!("ID: {id}");
//         //
//         //         postgres_client
//         //             .create_empty_server(&message.name, id)
//         //             .await?;
//         //         Ok(Response::Ok())
//         //     }
//         //     TokanizedMessage::CreateSpace { token, message } => {
//         //         let id = postgres_client.get_id_from_token(token).await?;
//         //         println!("hiofsdf");
//         //
//         //         postgres_client
//         //             .create_empty_space(&message.name, &message.server, id)
//         //             .await?;
//         //         Ok(Response::Ok())
//         //     }
//         //     TokanizedMessage::ListServers() => todo!(),
//         // }
//     }
//
//     pub async fn login_user(
//         &self,
//         username: &str,
//         password: &str,
//         postgres_client: Arc<CustomPostgresClient>,
//     ) -> anyhow::Result<Token> {
//         postgres_client.login_user(username, password).await
//     }
//
//     pub async fn register_user(
//         &self,
//         username: &str,
//         password: &str,
//         postgres_client: Arc<CustomPostgresClient>,
//     ) -> anyhow::Result<()> {
//         postgres_client.register_user(username, password).await
//     }
//
//     // RwLock
//     pub async fn process_v1(&self, postgres_client: Arc<CustomPostgresClient>) -> Response {
//         match self {
//             // NOTE: called "process" methods should return Response only upon sucess, otherwise,
//             // it should return an Err with error description.
//             MessageType::RegisterRequest(RegisterRequest { username, password }) => {
//                 // NOTE: can we pass the postgress and minio client over self instead?
//                 match self
//                     .register_user(username, password, postgres_client)
//                     .await
//                 {
//                     Ok(_) => Response::Ok(),
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::LoginRequest(LoginRequest { username, password }) => {
//                 match self.login_user(username, password, postgres_client).await {
//                     Ok(v) => Response::Token(v),
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::RenewToken { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::CreateServer { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::CreateSpace { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::DeleteSpace { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::ListCreatedServers { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::ListServerSpaces { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::JoinServer { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::LeaveServer { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//             MessageType::WriteMessage { token, message } => {
//                 match message.process(postgres_client, token).await {
//                     Ok(v) => v,
//                     Err(e) => Response::Error(e.to_string()),
//                 }
//             }
//         }
//     }
// }
//
// #[derive(Deserialize, Serialize)]
// pub struct VersionedMessage {
//     pub version: f32,
//     pub payload: MessageType,
// }
//
// impl VersionedMessage {
//     fn randomize() -> Self {
//         VersionedMessage {
//             version: 1.0,
//             payload: MessageType::LoginRequest(LoginRequest {
//                 username: String::from("username"),
//                 password: String::from("password"),
//             }),
//         }
//     }
// }
//
// #[derive(Serialize)]
// pub enum ResponseType {}
//
// // #[serde(rename = "type")]
// // _type: ResponseType,
// // message: String,
//
// #[derive(Serialize)]
// pub enum Response {
//     Ok(),
//     Close(),
//     ServerId(Uuid),
//     Token(Token),
//     Error(String),
//     ServersList(Vec<Server>),
//     SpacesList(Vec<Space>),
// }
//
// impl From<anyhow::Error> for Response {
//     fn from(value: anyhow::Error) -> Self {
//         Self::Error(value.to_string())
//     }
// }
//
// // impl<T: Serialize> From<anyhow::Result<T>> for Response {
// //     fn from(value: anyhow::Result<T>) -> Self {
// //         match value {
// //             Ok(v) => Self {
// //                 _type: ResponseType::Ok,
// //                 message: serde_json::to_string(&v).unwrap_or("done".to_string()),
// //             },
// //             Err(e) => Self {
// //                 _type: ResponseType::Error,
// //                 message: e.to_string(),
// //             },
// //         }
// //     }
// // }
//
// // impl<T> From<anyhow::Result<T>> for Response {
// //     fn from(value: anyhow::Result<T>) -> Self {
// //         match value {
// //             Ok(_) => Self {
// //                 _type: ResponseType::Ok,
// //                 message: String::from("done"),
// //             },
// //             Err(e) => Self {
// //                 _type: ResponseType::Error,
// //                 message: e.to_string(),
// //             },
// //         }
// //     }
// // }
//
// impl Response {
//     pub async fn send(
//         &self,
//         write: &mut SplitSink<WebSocketStream<TcpStream>, Message>,
//     ) -> anyhow::Result<()> {
//         if matches!(self, Self::Close()) {
//             write.close().await?;
//         } else if matches!(self, Self::Error(_)) {
//             write
//                 .send(Message::text(&serde_json::to_string_pretty(self)?))
//                 .await?;
//         } else {
//             write
//                 .send(Message::text(&serde_json::to_string_pretty(self)?))
//                 .await?;
//         }
//         Ok(())
//     }
//
//     pub fn close() -> Self {
//         Self::Close()
//     }
// }
//
// mod tests {
//     use crate::api::{Response, VersionedMessage};
//
//     #[test]
//     fn test_response() {
//         let s = serde_json::to_string_pretty(&Response::Ok()).unwrap();
//         println!("{s}");
//
//         let s = serde_json::to_string_pretty(&Response::Error("Hello world".to_string())).unwrap();
//         println!("{s}");
//     }
//
//     #[test]
//     fn test_versioned() {
//         let s = serde_json::to_string_pretty(&VersionedMessage::randomize()).unwrap();
//         println!("{s}");
//     }
//
//     #[test]
//     fn test_creeate_server() {
//         let versioned_message = VersionedMessage {
//             version: 1.0,
//             payload: super::MessageType::CreateServer {
//                 token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
//                     .to_string(),
//                 message: super::CreateServerRequest {
//                     name: String::from("TheSpace"),
//                 },
//             },
//         };
//         let s = serde_json::to_string_pretty(&versioned_message).unwrap();
//         println!("{s}");
//     }
//
//     #[test]
//     fn test_creeate_space() {
//         let versioned_message = VersionedMessage {
//             version: 1.0,
//             payload: super::MessageType::CreateSpace {
//                 token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
//                     .to_string(),
//                 message: super::CreateSpaceRequest {
//                     name: String::from("TheSpace"),
//                     server: String::from("31dcedb4-9968-4b1d-b5aa-571c2677bc9e"),
//                 },
//             },
//         };
//
//         let s = serde_json::to_string_pretty(&versioned_message).unwrap();
//         println!("{s}");
//     }
//
//     #[test]
//     fn test_list_created_servers() {
//         let versioned_message = VersionedMessage {
//             version: 1.0,
//             payload: super::MessageType::ListCreatedServers {
//                 token: "yDotF0SJAonAnZDNn49zwO6A2ocmPHMr7oCXZ9GpQFwl17KuCtpT4QruxTSiKOoB"
//                     .to_string(),
//                 message: super::ListCreatedServersRequest {},
//             },
//         };
//
//         let s = serde_json::to_string_pretty(&versioned_message).unwrap();
//         println!("{s}");
//     }
// }
