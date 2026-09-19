pub mod ban_user_from_friends;
pub mod ban_user_from_server;
pub mod cancle_friend_request;
pub mod create_server;
pub mod create_space;
pub mod delete_server;
pub mod delete_space;
pub mod get_dms;
pub mod get_friend_list;
pub mod get_friend_requests;
pub mod get_server_ban_list;
pub mod get_space_messages;
pub mod get_user_details;
pub mod join_server;
pub mod join_space;
pub mod judge_friend_request;
pub mod leave_server;
pub mod leave_space;
pub mod list_available_servers;
pub mod list_created_servers;
pub mod list_joined_servers;
pub mod list_server_members;
pub mod list_server_spaces;
pub mod login_request;
pub mod logout_request;
pub mod register_request;
pub mod renew_token;
pub mod search_user;
pub mod send_friend_request;
pub mod subscribe_to_server;
pub mod unban_user_from_server;
pub mod write_dm;
pub mod write_message;

use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{
    api::{Process, ResponseType, ServerId, UserId},
    messages::{
        ban_user_from_friends::BanUserFromFriends, ban_user_from_server::BanUserFromServer,
        cancle_friend_request::CancleFriendRequest, create_server::CreateServerRequest,
        create_space::CreateSpaceRequest, delete_server::DeleteServerRequest,
        delete_space::DeleteSpaceRequest, get_dms::GetDMs, get_friend_list::GetFriendList,
        get_friend_requests::GetFriendRequests, get_server_ban_list::GetServerBanList,
        get_space_messages::GetSpaceMessagesRequest, get_user_details::GetUserDetails,
        join_server::JoinServerRequest, join_space::JoinSpaceRequest,
        judge_friend_request::JudgeFriendRequest, leave_server::LeaveServerRequest,
        leave_space::LeaveSpaceRequest, list_available_servers::ListAvailableServersRequest,
        list_created_servers::ListCreatedServersRequest,
        list_joined_servers::ListJoinedServersRequest, list_server_members::ListServerMembers,
        list_server_spaces::ListServerSpaces, login_request::LoginRequest,
        logout_request::LogoutRequest, register_request::RegisterRequest, renew_token::RenewToken,
        search_user::SearchUser, send_friend_request::SendFriendRequest,
        subscribe_to_server::SubscribeToServerRequest, unban_user_from_server::UnbanUserFromServer,
        write_dm::WriteDM, write_message::WriteMessageRequest,
    },
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    result::error::ServerError::{self},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize)]
pub enum MessageType {
    RegisterRequest {
        #[serde(flatten)]
        message: RegisterRequest,
    },
    LoginRequest {
        #[serde(flatten)]
        message: LoginRequest,
    },
    LogoutRequest {
        token: String,
        #[serde(flatten)]
        message: LogoutRequest,
    },
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
    DeleteServer {
        token: String,
        #[serde(flatten)]
        message: DeleteServerRequest,
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
    ListAvailableServers {
        token: String,
        #[serde(flatten)]
        message: ListAvailableServersRequest,
    },
    ListServerSpaces {
        token: String,
        #[serde(flatten)]
        message: ListServerSpaces,
    },
    ListServerMembers {
        token: String,
        #[serde(flatten)]
        message: ListServerMembers,
    },
    JoinServer {
        token: String,
        #[serde(flatten)]
        message: JoinServerRequest,
    },
    JoinSpace {
        token: String,
        #[serde(flatten)]
        message: JoinSpaceRequest,
    },
    LeaveServer {
        token: String,
        #[serde(flatten)]
        message: LeaveServerRequest,
    },
    LeaveSpace {
        token: String,
        #[serde(flatten)]
        message: LeaveSpaceRequest,
    },
    Logout {
        token: String,
        #[serde(flatten)]
        message: LogoutRequest,
    },
    SendMessage {
        token: String,
        #[serde(flatten)]
        message: WriteMessageRequest,
    },
    SendDM {
        token: String,
        #[serde(flatten)]
        message: WriteDM,
    },
    // TODO: add GetSerevrMessages
    GetSpaceMessages {
        token: String,
        #[serde(flatten)]
        message: GetSpaceMessagesRequest,
    },
    GetDMs {
        token: String,
        #[serde(flatten)]
        message: GetDMs,
    },
    GetUserDetails {
        token: String,
        #[serde(flatten)]
        message: GetUserDetails,
    },
    SubscribeToServer {
        token: String,
        #[serde(flatten)]
        message: SubscribeToServerRequest,
    },
    SendFriendRequest {
        token: String,
        #[serde(flatten)]
        message: SendFriendRequest,
    },
    JudgeFriendRequest {
        token: String,
        #[serde(flatten)]
        message: JudgeFriendRequest,
    },
    CancleFriendRequest {
        token: String,
        #[serde(flatten)]
        message: CancleFriendRequest,
    },
    GetFriendRequests {
        token: String,
        #[serde(flatten)]
        message: GetFriendRequests,
    },
    // Get the friend list associated with your account
    GetFriendList {
        token: String,
        #[serde(flatten)]
        message: GetFriendList,
    },
    SearchUser {
        token: String,
        #[serde(flatten)]
        message: SearchUser,
    },
    BanUserFromServer {
        token: String,
        #[serde(flatten)]
        message: BanUserFromServer,
    },
    UnbanUserFromServer {
        token: String,
        #[serde(flatten)]
        message: UnbanUserFromServer,
    },
    BanUserFromFriends {
        token: String,
        #[serde(flatten)]
        message: BanUserFromFriends,
    },
    GetServerBanList {
        token: String,
        #[serde(flatten)]
        message: GetServerBanList,
    },
}

impl MessageType {
    pub async fn process_v1(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
        pub_sink: &mut CustomRedisPubSink,
        redis_client: &mut Arc<CustomRedisClient>,
    ) -> ResponseType {
        if self.requires_user_ban_check() {
            match self.user_banned_the_other(&postgres_client).await.unwrap() {
                Some((banner_id, banned_id)) => {
                    return ResponseType::error(ServerError::BannedByUser {
                        banner_id,
                        banned_id,
                    });
                }
                None => {
                    return self
                        .process_request(postgres_client, s3_client, pub_sink, redis_client)
                        .await;
                }
            }
        } else if self.requires_server_ban_check() {
            match self.is_banned_by_server(&postgres_client).await.unwrap() {
                Some((banned_id, banner_id)) => {
                    return ResponseType::error(ServerError::BannedByServer {
                        banner_id,
                        banned_id,
                    });
                }
                None => {
                    return self
                        .process_request(postgres_client, s3_client, pub_sink, redis_client)
                        .await;
                }
            }
        } else {
            return self
                .process_request(postgres_client, s3_client, pub_sink, redis_client)
                .await;
        }
    }

    async fn process_request(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
        pub_sink: &mut CustomRedisPubSink,
        redis_client: &mut Arc<CustomRedisClient>,
    ) -> ResponseType {
        #[allow(unused)]
        match self {
            MessageType::RegisterRequest { message } => {
                process(
                    message,
                    "",
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::LoginRequest { message } => {
                process(
                    message,
                    "",
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::LogoutRequest { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::RenewToken { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::CreateServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::CreateSpace { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::DeleteSpace { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::DeleteServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::ListServerSpaces { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::ListCreatedServers { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::ListServerMembers { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::ListJoinedServers { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::ListAvailableServers { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::JoinServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::JoinSpace { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::LeaveServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::SendMessage { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::GetDMs { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::GetSpaceMessages { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::SubscribeToServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::SendFriendRequest { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::JudgeFriendRequest { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            // TODO: add example in ./examples/requests
            MessageType::CancleFriendRequest { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::GetFriendRequests { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::GetFriendList { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            // TODO: add example in ./examples/requests
            MessageType::Logout { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::SearchUser { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::BanUserFromServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::UnbanUserFromServer { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::GetServerBanList { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::SendDM { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::BanUserFromFriends { token, message } => {
                process(
                    message,
                    token,
                    postgres_client,
                    s3_client,
                    pub_sink,
                    redis_client,
                )
                .await
            }
            MessageType::LeaveSpace { token, message } => todo!(), // is this necessary?
            MessageType::GetUserDetails { token, message } => todo!(), // friends = deep details
                                                                    // !friends = simple details
        }
    }

    pub fn requires_user_ban_check(&self) -> bool {
        return matches!(
            self,
            MessageType::GetUserDetails {
                token: _,
                message: _
            } | MessageType::SendFriendRequest {
                token: _,
                message: _
            } | MessageType::SendDM {
                token: _,
                message: _
            }
        );
    }

    pub fn requires_server_ban_check(&self) -> bool {
        return matches!(
            self,
            MessageType::CreateSpace {
                message: _,
                token: _
            } | MessageType::DeleteSpace {
                message: _,
                token: _
            } | MessageType::DeleteServer {
                message: _,
                token: _
            } | MessageType::ListServerSpaces {
                message: _,
                token: _
            } | MessageType::ListServerMembers {
                message: _,
                token: _
            } | MessageType::JoinServer {
                message: _,
                token: _
            } | MessageType::JoinSpace {
                message: _,
                token: _
            } | MessageType::LeaveServer {
                message: _,
                token: _
            } | MessageType::LeaveSpace {
                message: _,
                token: _
            } | MessageType::SendMessage {
                message: _,
                token: _
            } | MessageType::GetSpaceMessages {
                message: _,
                token: _
            } | MessageType::SubscribeToServer {
                message: _,
                token: _
            } | MessageType::BanUserFromServer {
                message: _,
                token: _
            } | MessageType::UnbanUserFromServer {
                message: _,
                token: _
            } | MessageType::GetServerBanList {
                message: _,
                token: _
            }
        );
    }

    async fn check_user_ban<T: UserToUser>(
        postgres_client: &Arc<CustomPostgresClient>,
        token: &str,
        request: &T,
    ) -> anyhow::Result<Option<(UserId, UserId)>> {
        // TODO: return ban direction
        let banned_id = request.sender_id(token, postgres_client).await?;
        let banner_id = request.receiver_id()?;
        if postgres_client
            .either_is_banned(&banned_id, &banner_id)
            .await?
        {
            Ok(Some((banned_id, banner_id)))
        } else {
            return Ok(None);
        }
    }

    async fn user_banned_the_other(
        &self,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<Option<(UserId, UserId)>> {
        match self {
            MessageType::GetUserDetails { token, message } => {
                return Self::check_user_ban(postgres_client, token, message).await;
            }
            MessageType::SendFriendRequest { token, message } => {
                return Self::check_user_ban(postgres_client, token, message).await;
            }
            MessageType::SendDM { token, message } => {
                return Self::check_user_ban(postgres_client, token, message).await;
            }
            _ => unimplemented!(),
        }
    }

    async fn check_server_ban<T: UserToServer>(
        postgres_client: &Arc<CustomPostgresClient>,
        token: &str,
        request: &T,
    ) -> anyhow::Result<Option<(UserId, ServerId)>> {
        let requester_id = request.requester(token, postgres_client).await?;
        let server_id = request.server_id()?;
        if postgres_client
            .is_banned_from_server(&server_id, &requester_id)
            .await?
        {
            return Ok(Some((requester_id, server_id)));
        } else {
            return Ok(None);
        }
    }

    async fn is_banned_by_server(
        &self,
        postgres_client: &Arc<CustomPostgresClient>,
        // ) -> anyhow::Result<bool> {
    ) -> anyhow::Result<Option<(UserId, ServerId)>> {
        match self {
            MessageType::CreateSpace { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::DeleteSpace { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::DeleteServer { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::ListServerSpaces { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::ListServerMembers { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::JoinServer { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::JoinSpace { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::LeaveServer { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::LeaveSpace { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::SendMessage { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::GetSpaceMessages { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::SubscribeToServer { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::BanUserFromServer { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::UnbanUserFromServer { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            MessageType::GetServerBanList { token, message } => {
                return Self::check_server_ban(postgres_client, token, message).await;
            }
            _ => Err(anyhow!("Not a server related request!")),
        }
    }
}

async fn process<T>(
    message: &T,
    token: &str,
    postgres_client: Arc<CustomPostgresClient>,
    s3_client: Arc<CustomS3client>,
    pub_sink: &mut CustomRedisPubSink,
    redis_client: &mut Arc<CustomRedisClient>,
) -> ResponseType
where
    T: Process,
{
    match message
        .process(postgres_client, s3_client, pub_sink, redis_client, token)
        .await
    {
        Ok(v) => v,
        Err(e) => ResponseType::error(e.kind),
    }
}
// should we check for bans while doing the request handling or just before it
// meaning should we unify all ban checks in a single check
// so it goes like this
// ban check -> do the thing
// or
// ban check while doing the thing
// is there higherarchal structure between requests?
//
// some requests need to check if user is in server and not banned from server
//
// what gain would I have if manage to split requests into categories?
//
// I would have to add only a sellective amount of checks for a wide variety of
// requests, like a singular check for a server ban for opperations that are meant
// to be preformed on a server

pub trait UserToServer {
    async fn requester(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(UserId::from(&user.id))
    }

    fn server_id(&self) -> anyhow::Result<ServerId>;
}

pub trait UserToUser {
    async fn sender_id(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(UserId::from(&user.id))
    }

    fn receiver_id(&self) -> anyhow::Result<UserId>;
}
