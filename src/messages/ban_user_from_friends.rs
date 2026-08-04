use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    api::{OriginRequestType, Process, ResponseType, ServerId, UserId},
    messages::{UserToServer, UserToUser},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct BanUserFromFriends {
    banned_id: Uuid,
}

impl Process for BanUserFromFriends {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::BanUserFromFriends
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(&token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let banner_id = postgres_client.get_user_id_from_token(&token).await?;
        let banned_id = UserId::from(&self.banned_id);

        postgres_client
            .ban_user_from_friends(&banner_id, &banned_id)
            .await?;

        Ok(ResponseType::Ok {
            original_request_type: self.original_type(),
        })
    }
}

impl UserToUser for BanUserFromFriends {
    fn receiver_id(&self) -> anyhow::Result<UserId> {
        todo!()
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
