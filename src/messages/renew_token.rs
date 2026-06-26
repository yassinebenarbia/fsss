use std::sync::Arc;

use anyhow::anyhow;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize)]
pub struct RenewToken {}

impl Process for RenewToken {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        redis_client: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }
        let id = postgres_client.get_user_id_from_token(token).await?;

        postgres_client
            .register_token_for_user(&id)
            .then(|token| async {
                if let Ok(_) = &token {
                    redis_client
                        .subscribe(ChannelPath::new_dm_path(&id))
                        .await?;
                }
                token
            })
            .await
            .map(|v| ResponseType::Token {
                token: v,
                associated_user_id: id.inner_owned(),
                original_request_type: self.original_type(),
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::RenewToken
    }
}
