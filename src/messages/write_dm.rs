use std::sync::Arc;


use chrono::Utc;
use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{
        AsNotification, DMNotification, ErrorResponse, OriginRequestType, Process, ResponseType,
        TextMessageKind, UserId,
    },
    messages::UserToUser,
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client, unknown_token,
};

#[derive(Deserialize, Serialize)]
pub struct WriteDM {
    pub receiver: Uuid,
    pub message_kind: TextMessageKind,
    pub message_content: String,
    pub reply: Option<Uuid>,
}

impl Process for WriteDM {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::SendDM
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        redis_conn: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            unknown_token!();
        }

        let user = postgres_client.get_user_by_token(token).await?;

        let notification = DMNotification::new(
            self.message_content.clone(),
            user,
            self.message_kind.clone().into(),
            Utc::now().naive_utc(),
        );

        let sender_id = postgres_client.get_user_id_from_token(token).await?;

        // write message
        Ok(postgres_client
            .write_dm(&sender_id, &self)
            .then(|result| async {
                redis_conn
                    .publish_notification(&notification.as_notification(), ChannelPath::from(self))
                    .await?;
                result
            })
            .await
            .map(|message_id| ResponseType::MessageSent {
                original_request_type: self.original_type(),
                message_id,
            })?)
    }
}
impl UserToUser for WriteDM {
    fn receiver_id(&self) -> anyhow::Result<UserId> {
        Ok(UserId::from(self.receiver))
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
