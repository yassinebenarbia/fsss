use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType, UserId},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
    unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GetDMs {
    user_id: Uuid,
    limit: Option<u64>,
}

impl Process for GetDMs {
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

        let user_id = postgres_client.get_user_id_from_token(&token).await?;

        let part1 = UserId::from(&user_id);
        let part2 = UserId::from(&self.user_id);

        println!("Getting messages between {part1} and {part2}");

        Ok(postgres_client
            .get_dms(&part1, &part2, &self.limit.map(|v| v as i64))
            .await
            .map(|messages| ResponseType::DirectMessageList {
                messages,
                original_request_type: self.original_type(),
            })?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::GetSpaceMessages
    }
}
