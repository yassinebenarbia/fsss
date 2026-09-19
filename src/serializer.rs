use serde::{Serialize, ser::SerializeStruct};
use serde_json::json;

use crate::{api::ResponseType, result::error::ServerError};

// desired format
// event: event type
// data: enum content (besides Notification)
impl Serialize for ResponseType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut sv = serializer.serialize_struct("Data", 6)?;
        match self {
            ResponseType::Notificaiton { notification } => {
                match notification {
                    crate::api::Notification::ServerMessage(server_message_notification) => {
                        // let mut sv = serializer.serialize_struct_variant("data", 0, "S", 3)?;
                        sv.serialize_field("event", "ServerMessage")?;
                        sv.serialize_field("data", &server_message_notification)?;
                    }
                    crate::api::Notification::DM(dmnotification) => {
                        sv.serialize_field("event", "DirectMessage")?;
                        sv.serialize_field("data", &dmnotification)?;
                    }
                    crate::api::Notification::ServerJoin(server_joined_notification) => {
                        sv.serialize_field("event", "ServerJoin")?;
                        sv.serialize_field("data", &server_joined_notification)?;
                    }
                    crate::api::Notification::SpaceCreated(space_created_notification) => {
                        sv.serialize_field("event", "SpaceCreated")?;
                        sv.serialize_field("data", &space_created_notification)?;
                    }
                    crate::api::Notification::Logout(logout_notification) => {
                        sv.serialize_field("event", "Logout")?;
                        sv.serialize_field("data", &logout_notification)?;
                    }
                    crate::api::Notification::FriendRequest(friend_request_notification) => {
                        sv.serialize_field("event", "FriendRequest")?;
                        sv.serialize_field("data", &friend_request_notification)?;
                    }
                    crate::api::Notification::FriendRequestJudgement(
                        friend_request_judgement_notification,
                    ) => {
                        sv.serialize_field("event", "FriendRequestJudgement")?;
                        sv.serialize_field("data", &friend_request_judgement_notification)?;
                    }
                    crate::api::Notification::FriendRequestCancled(
                        friend_request_cancled_notification,
                    ) => {
                        sv.serialize_field("event", "FriendRequestCancled")?;
                        sv.serialize_field("data", &friend_request_cancled_notification)?;
                    }
                }
            }
            ResponseType::MessageSent {
                original_request_type,
                message_id,
            } => {
                sv.serialize_field("event", "MessageSent")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "message_id": &message_id,
                    }),
                )?;
            }
            ResponseType::Ok {
                original_request_type,
            } => {
                sv.serialize_field("event", "Ok")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    }),
                )?;
            }
            ResponseType::ServerJoined {
                original_request_type,
                spaces,
            } => {
                sv.serialize_field("event", "MessageSent")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "spaces": &spaces,
                    }),
                )?;
            }
            ResponseType::Close() => todo!(),
            ResponseType::ServerId {
                original_request_type,
                server_id,
            } => {
                sv.serialize_field("event", "ServerId")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "server_id": &server_id,
                    }),
                )?;
            }
            ResponseType::SpaceId(uuid) => {
                sv.serialize_field("event", "SpaceId")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "id": &uuid,
                    }),
                )?;
            }
            ResponseType::Token {
                original_request_type,
                associated_user_id,
                token,
            } => {
                sv.serialize_field("event", "Token")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "token": &token,
                    "associated_user_id": &associated_user_id,
                    }),
                )?;
            }
            // FIXME: this looks ugly
            ResponseType::Error { message, kind } => {
                #[derive(Serialize)]
                struct ErroWrapper {
                    message: String,
                    #[serde(flatten)]
                    kind: ServerError,
                }
                sv.serialize_field(
                    "data",
                    &ErroWrapper {
                        message: message.clone(),
                        kind: kind.clone(),
                    },
                )?;
            }
            ResponseType::ServersList {
                original_request_type,
                servers,
            } => {
                sv.serialize_field("event", "ServersList")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "servers": &servers,
                    }),
                )?;
            }
            ResponseType::SpaceMessageList {
                original_request_type,
                messages,
            } => {
                sv.serialize_field("event", "SpaceMessageList")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "messages": &messages,
                    }),
                )?;
            }
            ResponseType::DirectMessageList {
                original_request_type,
                messages,
            } => {
                sv.serialize_field("event", "DirectMessageList")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "messages": &messages,
                    }),
                )?;
            }
            ResponseType::MembersList {
                original_request_type,
                server_id,
                members,
            } => {
                sv.serialize_field("event", "MembersList")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "members": &members,
                    "server_id": &server_id,
                    }),
                )?;
            }
            ResponseType::SpacesList {
                original_request_type,
                spaces,
            } => {
                sv.serialize_field("event", "SpacesList")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "spaces": &spaces,
                    }),
                )?;
            }
            ResponseType::User {
                original_request_type,
                user,
            } => {
                sv.serialize_field("event", "User")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "user": &user,
                    }),
                )?;
            }
            ResponseType::Users {
                original_request_type,
                users,
            } => {
                sv.serialize_field("event", "Users")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "users": &users,
                    }),
                )?;
            }
            ResponseType::FriendRequestSent {
                original_request_type,
                request_id,
                requested_id,
            } => {
                sv.serialize_field("event", "FriendRequestSent")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "request_id": &request_id,
                    "requested_id": &requested_id,
                    }),
                )?;
            }
            ResponseType::FriendRequests {
                original_request_type,
                requests,
            } => {
                sv.serialize_field("event", "FriendRequests")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "requests": &requests,
                    }),
                )?;
            }
            ResponseType::FriendsList {
                original_request_type,
                friends,
            } => {
                sv.serialize_field("event", "FriendsList")?;
                sv.serialize_field(
                    "data",
                    &json!({
                    "original_request_type": &original_request_type,
                    "friends": &friends,
                    }),
                )?;
            }
        }
        sv.end()
    }
}

#[allow(unused)]
mod test {
    use crate::api::{ResponseType, UserId};

    #[test]
    fn test_error_one() {
        let response = ResponseType::Error {
            message: format!("Error Message"),
            kind: crate::result::error::ServerError::UnkwonUser {
                user_id: UserId::new(uuid::Uuid::new_v4()),
            },
        };

        let response = serde_json::to_string_pretty(&response).unwrap();

        println!("Response: {response}");
    }

    #[test]
    fn test_error_two() {
        let response = ResponseType::Error {
            message: format!("Error Message"),
            kind: crate::result::error::ServerError::Unauthorized,
        };

        let response = serde_json::to_string_pretty(&response).unwrap();

        println!("Response: {response}");
    }

    #[test]
    fn test_errors() {
        test_error_one();
        test_error_two();
    }
}
