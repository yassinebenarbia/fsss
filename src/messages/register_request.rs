use std::sync::Arc;

use futures_util::FutureExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{OriginRequestType, Process, ResponseType, UserMetadata},
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize)]
pub struct RegisterRequest {
    pub username: String,
    pub password: String,
    nickname: Option<String>,
    bio: Option<String>,
    // FIXME: length + format
    pronounce: Option<String>,
}

impl Process for RegisterRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        redis_client: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        _: &str,
    ) -> anyhow::Result<ResponseType> {
        let user_id = Uuid::new_v4();

        postgres_client
            .register_user(&self.username, &self.password, &user_id)
            .then(|token| async {
                if let Ok(_) = &token {
                    postgres_client
                        .update_user_metadata(&UserMetadata::new(
                            user_id,
                            self.nickname.clone(),
                            self.bio.clone(),
                            None,
                            user_id,
                        ))
                        .await?;
                }
                token
            })
            .then(|token| async {
                if let Ok(_) = &token {
                    redis_client
                        .subscribe(ChannelPath::new_dm_path(user_id))
                        .await?;
                }
                token
            })
            .await
            .map(|token| ResponseType::Token {
                original_request_type: self.original_type(),
                associated_user_id: user_id,
                token,
            })
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::RegisterRequest
    }
}
