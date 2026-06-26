use std::sync::Arc;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::{
    api::{OriginRequestType, Process, ResponseType},
    postgres::CustomPostgresClient,
    redis::{CustomRedisClient, CustomRedisPubSink},
    s3::CustomS3client,
};

#[derive(Deserialize, Serialize, Debug)]
pub enum SearchField {
    // ID,
    Name,
    Nickname,
    Bio,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SearchUser {
    limit: Option<i64>,
    query: String,
    field: SearchField,
}

impl Process for SearchUser {
    fn original_type(&self) -> OriginRequestType {
        OriginRequestType::SearchUser
    }

    async fn process(
        &self,
        postgres_client: Arc<CustomPostgresClient>,
        _: Arc<CustomS3client>,
        _: &mut CustomRedisPubSink,
        _: &mut Arc<CustomRedisClient>,
        token: &str,
    ) -> anyhow::Result<ResponseType> {
        if !postgres_client.token_exist_and_not_expired(token).await? {
            return Err(anyhow!("Token does not exist or expired!"));
        }

        let users = postgres_client
            .search_users(&self.query, &self.field, &self.limit)
            .await?;

        Ok(ResponseType::Users {
            original_request: self.original_type(),
            users,
        })
    }
}
