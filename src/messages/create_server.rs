use std::sync::Arc;


use serde::{Deserialize, Serialize};

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateServerRequest {
    pub name: String,
}

impl Process for CreateServerRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        // TODO: check if server exist
        if !postgres_client.token_exist_and_not_expired(token).await? {
            unknown_token!();
        }

        let id = postgres_client.get_user_id_from_token(token).await?;

        Ok(postgres_client
            .create_empty_server_and_join(&self.name, &id)
            .await
            .map(|id| ResponseType::ServerId {
                server_id: id,
                original_request_type: self.original_type(),
            })?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::ListCreatedServers
    }
}
