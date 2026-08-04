use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

// TODO: add unsub request
#[derive(Deserialize, Serialize, Debug)]
pub struct SubscribeToServerRequest {
    server_id: String,
}

impl Process for SubscribeToServerRequest {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::SubscribeToServer
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        pubsub_client: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = postgres_client.get_user_id_from_token(token).await?;
        let user_id = UserId::from(&user_id);
        let server_id = ServerId::try_from(&self.server_id)?;

        if !postgres_client.is_joined(&server_id, &user_id).await? {
            return Err(anyhow!(
                "User with id {} is not joined to {}",
                user_id,
                self.server_id
            ));
        }

        pubsub_client
            .subscribe(ChannelPath::new_server_path(
                &self.server_id,
                None::<&str>,
                None::<&str>,
            ))
            .await?;

        Ok(ResponseType::Ok {
            original_request_type: self.original_type(),
        })
    }
}

impl UserToServer for SubscribeToServerRequest {
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
