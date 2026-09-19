use std::{str::FromStr, sync::Arc};


use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{
        AsNotification, ErrorResponse, FriendRequestNotification, OriginRequestType, Process,
        ResponseType, UserId,
    },
    messages::UserToUser,
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct SendFriendRequest {
    requested_id: String,
    message: Option<String>,
}

impl Process for SendFriendRequest {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::SendFriendRequest
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        redis_client: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            unknown_token!();
        }

        let requester_id = postgres_client.get_user_id_from_token(token).await?;
        let requested_id = UserId::from(Uuid::from_str(&self.requested_id)?);

        if postgres_client
            .are_friends(&requested_id, &requester_id)
            .await?
        {
            return Err(ErrorResponse::already_friends(&requester_id, &requested_id));
        }

        let request_id = postgres_client
            .register_friend_request(&requester_id, &requested_id, &self.message)
            .await?;

        let notification = FriendRequestNotification::new(
            request_id,
            requester_id.inner_clone(),
            requested_id.inner_clone(),
            Utc::now().naive_utc(),
            self.message.clone(),
        );

        redis_client
            .publish_notification(
                &notification.as_notification(),
                ChannelPath::new_dm_path(&requested_id.inner()),
            )
            .await?;

        Ok(ResponseType::FriendRequestSent {
            original_request_type: self.original_type(),
            requested_id: requested_id.inner_clone(),
            request_id,
        })
    }
}

impl UserToUser for SendFriendRequest {
    fn receiver_id(&self) -> anyhow::Result<UserId> {
        Ok(UserId::try_from(&self.requested_id).unwrap())
    }

    async fn sender_id(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await.unwrap();
        Ok(UserId::from(&user.id))
    }
}
