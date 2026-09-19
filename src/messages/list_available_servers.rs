use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct ListAvailableServersRequest {
    limit: Option<u64>,
}

impl Process for ListAvailableServersRequest {
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

        let limit = self.limit.unwrap_or(20);

        Ok(postgres_client
            .get_available_servers(limit)
            .await
            .map(|servers| ResponseType::ServersList {
                servers,
                original_request_type: self.original_type(),
            })?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::ListAvailableServers
    }
}
