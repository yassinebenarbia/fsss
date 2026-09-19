use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType, UserId},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
    unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct ListJoinedServersRequest {}

impl Process for ListJoinedServersRequest {
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

        let id = postgres_client.get_user_id_from_token(token).await?;

        let user_id = UserId::from(&id);

        Ok(postgres_client
            .get_joined_servers(&user_id)
            .await
            .map(|servers| ResponseType::ServersList {
                servers,
                original_request_type: self.original_type(),
            })?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::ListJoinedServers
    }
}
