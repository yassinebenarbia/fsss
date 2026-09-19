use std::sync::Arc;


use serde::{Deserialize, Serialize};

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GetFriendRequests {
    limit: Option<i64>,
}

impl Process for GetFriendRequests {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::GetFriendRequests
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            unknown_token!();
        }

        let user = postgres_client.get_user_by_token(token).await?;

        let limit = self.limit.unwrap_or(20);

        let requests = postgres_client
            .get_incoming_friend_requests(&user.id, &limit)
            .await?;

        Ok(ResponseType::FriendRequests {
            original_request_type: self.original_type(),
            requests,
        })
    }
}
