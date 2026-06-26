use std::{str::FromStr, sync::Arc};

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
pub struct ListServerMembers {
    server: String,
}

impl Process for ListServerMembers {
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

        let id = postgres_client.get_user_id_from_token(token).await?;

        let user_id = UserId::from(&id);
        let server_id = ServerId::from(&Uuid::from_str(&self.server)?);

        if !postgres_client.is_joined(&server_id, &user_id).await? {
            return Err(anyhow!("You need to join to access server Members"));
        }

        postgres_client
            .get_members(&self.server)
            .await
            .map(|members| ResponseType::MembersList {
                members,
                server_id: self.server.clone(),
                original_request_type: self.original_type(),
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::ListServerMembers
    }
}

impl UserToServer for ListServerMembers {
    async fn requester(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(UserId::from(&user.id))
    }

    fn server_id(&self) -> anyhow::Result<ServerId> {
        ServerId::try_from(&self.server)
    }
}
