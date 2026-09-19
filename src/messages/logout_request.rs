use std::sync::Arc;

use anyhow::anyhow;
use chrono::Utc;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{
    api::{
        AsNotification, ErrorResponse, LogoutNotification, OriginRequestType, Process, ResponseType,
    },
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct LogoutRequest {}

impl Process for LogoutRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        pub_sink: &mut CustomRedisPubSink,
        redis_client: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            unknown_token!();
        }

        let user = postgres_client
            .get_user_by_token(token)
            .await
            .map_err(|e| anyhow!("User does not exist! {e}"))?;

        let notification =
            LogoutNotification::new(Utc::now().naive_utc(), token.to_string(), user.clone());

        redis_client
            .publish_notification(
                &notification.as_notification(),
                ChannelPath::new_dm_path(&user.id),
            )
            .await?;

        Ok(pub_sink
            .unsubscribe_all()
            .then(|result| async {
                result?;
                postgres_client.invalidate_token(token).await
            })
            .await
            .map(|_| ResponseType::Ok {
                original_request_type: self.original_type(),
            })?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::LogoutRequest
    }
}
