use std::{str::FromStr, sync::Arc};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{
        AsNotification, FriendRequestJudgementNotification, OriginRequestType, Process,
        ResponseType, UserId,
    },
    messages::UserToUser,
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct JudgeFriendRequest {
    judged_request_id: String,
    accept: bool,
}

impl Process for JudgeFriendRequest {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::JudgeFriendRequest
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        redis_client: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let judged_request_id = Uuid::from_str(&self.judged_request_id)?;
        if !postgres_client
            .friend_request_exist(&judged_request_id)
            .await?
        {
            return Err(anyhow!(
                "request with id {judged_request_id} does not exist!"
            ));
        }

        let request = postgres_client
            .get_friend_request(&judged_request_id)
            .await?;

        if self.accept {
            postgres_client.accept_friend_request(&request).await?;
            println!("firend request with id {judged_request_id} accepted");
        } else {
            postgres_client.reject_friend_request(&request).await?;
            println!("firend request with id {judged_request_id} rejected");
        }

        let notification = FriendRequestJudgementNotification::new(judged_request_id, self.accept);
        redis_client
            .publish_notification(
                &notification.as_notification(),
                ChannelPath::new_dm_path(&request.requester_id),
            )
            .await?;

        println!("Notification published");

        Ok(ResponseType::Ok {
            original_request_type: self.original_type(),
        })
    }
}
