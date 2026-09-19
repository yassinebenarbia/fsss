use std::sync::Arc;


use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct UnbanUserFromServer {
    server_id: Uuid,
    banned_id: Uuid,
}

impl Process for UnbanUserFromServer {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::UnbanUserFromServer
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        // what do we need to unban a user from server?
        // do we check if user is not banned? or is it the return of the unbn method
        if !postgres_client.token_exist_and_not_expired(&token).await? {
            unknown_token!();
        }

        let server_id = ServerId::from(&self.server_id);
        let unbanner_user_id = postgres_client.get_user_id_from_token(&token).await?;
        let unbanned_user_id = &UserId::from(&self.banned_id);

        if !postgres_client.server_exist(&server_id).await? {
            return Err(ErrorResponse::server_does_not_exist(&server_id));
        }

        if !postgres_client
            .is_joined(&server_id, &unbanner_user_id)
            .await?
        {
            return Err(ErrorResponse::not_a_member(&server_id, unbanned_user_id));
        }

        if !postgres_client
            .can_ban(&server_id, &unbanner_user_id)
            .await?
        {
            return Err(ErrorResponse::unauthorized());
        }

        if !postgres_client
            .is_banned_from_server(&server_id, unbanned_user_id)
            .await?
        {
            return Err(ErrorResponse::not_a_member(&server_id, &unbanner_user_id));
        }

        postgres_client
            .unban_user_from_server(&unbanned_user_id, &server_id)
            .await?;

        Ok(ResponseType::Ok {
            original_request_type: self.original_type(),
        })
    }
}

impl UserToServer for UnbanUserFromServer {
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
