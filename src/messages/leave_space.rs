use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType, ServerId, SpaceId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct LeaveSpaceRequest {
    space_id: String,
    server_id: String,
}

impl Process for LeaveSpaceRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        pub_sub_sink: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let user_id = postgres_client.get_user_id_from_token(token).await?;
        let server_id = ServerId::try_from(&self.server_id)?;
        let space_id = SpaceId::try_from(&self.space_id)?;

        if !postgres_client
            .contains_space(&server_id, &space_id)
            .await?
        {
            return Err(anyhow!(format!(
                "Space with id {} is not a part of Server {}",
                self.space_id, self.server_id
            )));
        }

        pub_sub_sink
            .unsubscribe(user_id, &self.server_id, &self.space_id)
            .await
            .map(|_| ResponseType::Ok {
                original_request: self.original_type(),
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::LeaveServer
    }
}

impl UserToServer for LeaveSpaceRequest {
    fn server_id(&self) -> anyhow::Result<ServerId> {
        ServerId::try_from(&self.server_id)
    }

    async fn requester(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<crate::api::UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(crate::api::UserId::from(&user.id))
    }
}
