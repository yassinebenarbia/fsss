use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    api::{Process, ServerId},
    messages::UserToServer,
    postgres::CustomPostgresClient,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct JoinSpaceRequest {
    server_id: String,
    space_id: String,
}

impl Process for JoinSpaceRequest {
    fn original_type(&self) -> crate::api::OriginRequestType {
        todo!()
    }
}

impl JoinSpaceRequest {
    pub async fn register_user_to_space(&self) -> anyhow::Result<()> {
        todo!()
    }
}

impl UserToServer for JoinSpaceRequest {
    fn server_id(&self) -> anyhow::Result<crate::api::ServerId> {
        ServerId::try_from(&self.server_id)
    }

    async fn requester(
        &self,
        token: &str,
        postgres_client: &Arc<CustomPostgresClient>,
    ) -> anyhow::Result<crate::api::UserId> {
        let user = postgres_client.get_user_by_token(token).await?;
        Ok(crate::api::UserId::from(&user.id))
    }
}
