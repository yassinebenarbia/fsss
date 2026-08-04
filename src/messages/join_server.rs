use std::sync::Arc;

use anyhow::anyhow;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::UserToServer,
    postgres::{CustomPostgresClient, Role},
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct JoinServerRequest {
    server_id: String,
}

impl Process for JoinServerRequest {
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

        let user = postgres_client.get_user_by_token(token).await?;
        let user_id = UserId::from(&user.id);

        postgres_client
            .join_server(&self.server_id, &user_id, &Role::Member)
            .then(|spaces| async move {
                pubsub_client
                    .subscribe(ChannelPath::new_server_path(
                        &self.server_id,
                        None::<&str>,
                        None::<&str>,
                    ))
                    .await?;
                spaces
            })
            .await
            .map(|spaces| ResponseType::ServerJoined {
                original_request_type: self.original_type(),
                spaces,
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::JoinServer
    }
}

impl UserToServer for JoinServerRequest {
    fn server_id(&self) -> anyhow::Result<crate::api::ServerId> {
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
