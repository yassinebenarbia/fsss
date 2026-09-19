use std::{str::FromStr, sync::Arc};

use chrono::Utc;
use futures_util::TryFutureExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{
        AsNotification, ErrorResponse, OriginRequestType, Process, ResponseType, ServerId,
        SpaceCreatedNotification, UserId,
    },
    messages::UserToServer,
    postgres::CustomPostgresClient,
    redis::{ChannelPath, CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
    unknown_token,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct CreateSpaceRequest {
    pub name: String,
    pub server: String,
}

impl Process for CreateSpaceRequest {
    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        s3_client: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        redis_conn: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType, ErrorResponse> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            unknown_token!();
        }

        let user = postgres_client.get_user_by_token(token).await?;
        let user_id = UserId::from(&user.id);
        let server_id = ServerId::try_from(&self.server)?;
        println!("Got user {}", user.name);

        if !postgres_client.is_admin(&server_id, &user_id).await? {
            return Err(ErrorResponse::unauthorized());
        }

        let server_id = Uuid::from_str(&self.server).unwrap();
        let space_id = uuid::Uuid::new_v4();

        let notification = SpaceCreatedNotification::new(
            server_id.clone(),
            space_id.clone(),
            Utc::now().naive_utc(),
            user.clone(),
        );

        println!("Creating space {}", self.name);

        Ok(s3_client
            .create_bucket_with_id()
            .and_then(|bucket_id| async move {
                postgres_client
                    .create_empty_space_with_uuid(
                        &self.name,
                        &self.server,
                        &user.id,
                        &bucket_id,
                        &space_id,
                    )
                    .await?;
                println!("created space {}", space_id);
                return Ok(server_id);
            })
            .and_then(|response| async move {
                redis_conn
                    .publish_notification(
                        &notification.as_notification(),
                        ChannelPath::new_server_path(
                            &server_id.to_string(),
                            Some(space_id.to_string()),
                            None::<u64>,
                        ),
                    )
                    .await?;

                println!("Notification pushed to {}!", server_id);
                return Ok(response);
            })
            .await
            .map(ResponseType::SpaceId)?)
    }

    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::CreateSpace
    }
}

impl UserToServer for CreateSpaceRequest {
    fn server_id(&self) -> anyhow::Result<ServerId> {
        ServerId::try_from(&self.server)
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
