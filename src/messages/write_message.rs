use std::{str::FromStr, sync::Arc};

use anyhow::anyhow;
use chrono::Utc;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{
        AsNotification, MessageKind, ServerMessageNotification, OriginRequestType, Process, ResponseType,
        ServerId, UserId,
    },
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

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
#[derive(Deserialize, Debug, Serialize)]
pub struct WriteMessageRequest {
    pub server_id: String,
    pub space_id: String,
    pub message_kind: MessageKind,
    pub message_content: String,
    pub reply: Option<Uuid>,
}

impl Process for WriteMessageRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        redis_conn: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user = postgres_client.get_user_by_token(token).await?;
        let space_id = Uuid::from_str(&self.space_id)?;
        let server_id = Uuid::from_str(&self.server_id)?;

        let notification = ServerMessageNotification::new(
            space_id.clone(),
            server_id.clone(),
            self.message_content.clone(),
            user.clone(),
            self.message_kind.clone().into(),
            Utc::now().naive_utc(),
        );

        println!("Writing message to DB");
        let user_id = UserId::from(&user.id);

        postgres_client
            .write_server_text_message(&user_id, &self)
            .then(|result| async {
                redis_conn
                    .publish_notification(&notification.as_notification(), ChannelPath::from(self))
                    .await?;
                result
            })
            .await
            .map(|message_id| ResponseType::MessageSent {
                original_request: self.original_type(),
                message_id,
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::SendMServerMessage
    }
}

impl UserToServer for WriteMessageRequest {
    fn server_id(&self) -> anyhow::Result<ServerId> {
        ServerId::try_from(&self.server_id)
    }

    async fn requester(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(UserId::from(&user.id))
    }
}
