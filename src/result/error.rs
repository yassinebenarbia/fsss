use std::fmt::Debug;

use serde::Serialize;
use uuid::Uuid;

use crate::api::ServerId;
use crate::api::SpaceId;
use crate::api::UserId;

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "kind", content = "metadata")]
#[allow(unused)]
pub enum ServerError {
    FriendRequestDoesNotExist {
        request_id: Uuid,
    },
    BannedByUser {
        banner_id: UserId,
        banned_id: UserId,
    },
    BannedByServer {
        banner_id: ServerId,
        banned_id: UserId,
    },
    ServerDoestNotExist {
        server_id: ServerId,
    },
    SpaceDoestNotExist {
        space_id: SpaceId,
        server_id: ServerId,
    },
    AlreadyFriends {
        rhs: UserId,
        lhs: UserId,
    },
    NotAMember {
        server_id: ServerId,
        user_id: UserId,
    },
    TokenDoNotExistOrExpired,
    UnkwonUser {
        user_id: UserId,
    },
    Unauthorized,
    VersionError,
    InternalError,
    ConnectionError(&'static str),
}

impl ServerError {
    pub fn message(&self) -> String {
        match self {
            ServerError::BannedByUser {
                banner_id,
                banned_id,
            } => format!("Bann exist between {banned_id} and {banner_id}"),
            ServerError::BannedByServer {
                banner_id,
                banned_id,
            } => format!("User {banned_id} is banned by {banner_id}"),
            ServerError::ServerDoestNotExist { server_id } => {
                format!("Server {server_id} does not exist")
            }
            ServerError::SpaceDoestNotExist {
                space_id,
                server_id,
            } => format!("Space {space_id} doest not exist in server {server_id}"),
            ServerError::AlreadyFriends { rhs, lhs } => {
                format!("{rhs} and {lhs} are already friends")
            }
            ServerError::NotAMember { user_id, server_id } => {
                format!("User {user_id} is not a member in server {server_id}")
            }
            ServerError::TokenDoNotExistOrExpired => "Token Does Not Exist".to_string(),
            ServerError::UnkwonUser { user_id } => format!("Unkown user {user_id}"),
            ServerError::Unauthorized => format!("Unauthorized accesss"),
            ServerError::VersionError => format!("Wrong protocol version"),
            ServerError::InternalError => format!("Internal error"),
            ServerError::ConnectionError(msg) => format!("Connection error: {msg}"),
            ServerError::FriendRequestDoesNotExist { request_id } => {
                format!("Friend request {request_id} doest not exist")
            }
        }
    }
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // NOTE: In case we add more details to error message, match and format accordingly
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for ServerError {}

// unkown token
#[macro_export]
macro_rules! unknown_token {
    () => {
        return Err(ErrorResponse::unknown_token());
    };
}
