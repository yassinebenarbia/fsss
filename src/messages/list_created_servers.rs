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
pub struct ListCreatedServersRequest {}

impl Process for ListCreatedServersRequest {
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

        postgres_client
            .get_created_servers(&token)
            .await
            .map(|servers| ResponseType::ServersList {
                servers,
                original_request_type: self.original_type(),
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::ListCreatedServers
    }
}
