use std::sync::Arc;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{
    api::{ErrorResponse, OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

impl Process for LoginRequest {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::LoginRequest
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        redis_client: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        _: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        postgres_client
            .login_user(&self.username, &self.password)
            .then(|token| async {
                let token = token?;
                let user_id = postgres_client.get_user_id_from_token(&token.token).await?;
                redis_client
                    .subscribe(ChannelPath::new_dm_path(&user_id))
                    .await?;
                Ok((token, user_id))
            })
            .await
            .map(|(token, user_id)| ResponseType::Token {
                original_request_type: self.original_type(),
                associated_user_id: *user_id.inner(),
                token,
            })
    }
}
