use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct DeleteSpaceRequest {
    space_id: String,
    server_id: String,
}

impl Process for DeleteSpaceRequest {
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

        let user_id = postgres_client.get_user_id_from_token(token).await?;
        let server_id = ServerId::try_from(&self.server_id)?;

        if !postgres_client.is_admin(&server_id, &user_id).await? {
            return Err(anyhow!("Unseficcient previlages"));
        }

        postgres_client
            .delete_space(&self.space_id, &self.server_id)
            .await
            .map(|_| ResponseType::Ok {
                original_request: self.original_type(),
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::DeleteSpace
    }
}

impl UserToServer for DeleteSpaceRequest {
    fn server_id(&self) -> anyhow::Result<ServerId> {
        ServerId::try_from(&self.server_id)
    }

    async fn requester(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(UserId::from(&user.id))
    }
}
