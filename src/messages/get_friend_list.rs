use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GetFriendList {
    limit: Option<i64>,
}

impl Process for GetFriendList {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::GetFriendsList
    }

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

        let user = postgres_client.get_user_by_token(token).await?;

        let limit = self.limit.unwrap_or(20);

        let friends = postgres_client.get_friends(&user.id, &limit).await?;

        Ok(ResponseType::FriendsList {
            original_request_type: self.original_type(),
            friends,
        })
    }
}
