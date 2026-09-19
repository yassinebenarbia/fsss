use std::{str::FromStr, sync::Arc};


use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{AsNotification, ErrorResponse, FriendRequestCancledNotification, OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct CancleFriendRequest {
    canceled_request_id: String,
}

impl Process for CancleFriendRequest {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::CancleFriendRequest
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

        let user = postgres_client.get_user_by_token(token).await?;

        let cancled_request_id = Uuid::from_str(&self.canceled_request_id)?;

        let request = postgres_client
            .get_friend_request(&cancled_request_id)
            .await?;

        if request.requester_id != user.id {
            return Err(ErrorResponse::unauthorized());
        }

        postgres_client.cancle_friend_request(&request).await?;

        let notification =
            FriendRequestCancledNotification::new(request.request_id, request.requester_id);
        redis_client
            .publish_notification(
                &notification.as_notification(),
                ChannelPath::new_dm_path(&request.requested_id),
            )
            .await?;

        Ok(ResponseType::Ok {
            original_request_type: self.original_type(),
        })
    }
}
