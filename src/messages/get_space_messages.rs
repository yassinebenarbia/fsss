use std::sync::Arc;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
    unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct GetSpaceMessagesRequest {
    space_id: Uuid,
    server_id: Uuid,
    limit: Option<u64>,
}

impl Process for GetSpaceMessagesRequest {
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

        let user_id = postgres_client.get_user_id_from_token(&token).await?;
        let server_id = postgres_client
            .get_server_id_from_space(&self.space_id)
            .await?;

        let user_id = UserId::from(&user_id);
        let server_id = ServerId::from(&server_id);

        if !postgres_client.is_joined(&server_id, &user_id).await? {
            return Err(ErrorResponse::not_a_member(&server_id, &user_id));
        }

        println!("Getting spaces messages for SERVER ID: {server_id}");

        Ok(postgres_client
            .get_space_messages(&self.space_id, &self.limit.map(|v| v as i64))
            .await
            .map(|messages| ResponseType::SpaceMessageList {
                messages,
                original_request_type: self.original_type(),
            })?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::GetSpaceMessages
    }
}

impl UserToServer for GetSpaceMessagesRequest {
    fn server_id(&self) -> anyhow::Result<ServerId> {
        Ok(ServerId::from(self.server_id))
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
