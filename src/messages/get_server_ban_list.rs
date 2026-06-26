use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GetServerBanList {
    server_id: Uuid,
}

impl Process for GetServerBanList {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::GetServerBanList
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

        // is joined vs is admin?

        let searcher_id = postgres_client.get_user_id_from_token(token).await?;
        let server_id = ServerId::from(&self.server_id);

        if !postgres_client.is_joined(&server_id, &searcher_id).await? {
            return Err(anyhow!("You are not a member of this server!"));
        }

        let users = postgres_client.banned_users(&self.server_id).await?;

        Ok(ResponseType::Users {
            original_request: self.original_type(),
            users,
        })
    }
}

impl UserToServer for GetServerBanList {
    fn server_id(&self) -> anyhow::Result<ServerId> {
        Ok(ServerId::from(&self.server_id))
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
