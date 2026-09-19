use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType, UserId},
    messages::UserToUser,
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
    unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GetUserDetails {
    user_id: Uuid,
}

impl Process for GetUserDetails {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::GetUserDetails
    }

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

        let user = postgres_client.get_user_by_id(&self.user_id).await?;

        Ok(ResponseType::User {
            original_request_type: self.original_type(),
            user: user,
        })
    }
}

impl UserToUser for GetUserDetails {
    fn receiver_id(&self) -> anyhow::Result<UserId> {
        Ok(UserId::from(self.user_id))
    }

    async fn sender_id(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(UserId::from(&user.id))
    }
}
