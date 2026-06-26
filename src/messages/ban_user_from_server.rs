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
pub struct BanUserFromServer {
    server_id: Uuid,
    banned_id: Uuid,
}

impl Process for BanUserFromServer {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::BanUserFromServer
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(&token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let server_id = ServerId::from(self.server_id);

        if !postgres_client.server_exist(&server_id).await? {
            return Err(anyhow!("Server `{}` does not exist", self.server_id));
        }

        let user_id = postgres_client.get_user_id_from_token(&token).await?;
        let banned_id = UserId::from(&self.banned_id);
        let server_id = ServerId::from(&self.server_id);

        if !postgres_client.is_joined(&server_id, &user_id).await? {
            return Err(anyhow!("You are not a member of that server"));
        }

        if !postgres_client.can_ban(&server_id, &user_id).await? {
            return Err(anyhow!("Unseficcient permissions"));
        }

        if !postgres_client.is_joined(&server_id, &banned_id).await? {
            return Err(anyhow!(
                "User `{}` is not a member of that server",
                self.banned_id
            ));
        }

        postgres_client
            .ban_user_from_server(&banned_id, &server_id)
            .await?;

        Ok(ResponseType::Ok {
            original_request: self.original_type(),
        })
    }
}

impl UserToServer for BanUserFromServer {
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
