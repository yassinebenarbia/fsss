use std::sync::Arc;

use crate::config::S3;

use aws_config::{SdkConfig, meta::region::RegionProviderChain};
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};

#[derive(Clone, Debug)]
pub struct CustomS3client {
    pub client: Arc<aws_sdk_s3::Client>,
}

impl CustomS3client {
    pub fn new(client: aws_sdk_s3::Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }

    pub async fn create_s3_client(config: &S3) -> anyhow::Result<Self> {
        let creds = Credentials::builder()
            .access_key_id(config.username.clone())
            .secret_access_key(config.password.clone())
            .provider_name("minio")
            .build();

        let shared_creds = SharedCredentialsProvider::new(creds);

        let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");

        let s3_config_builder = SdkConfig::builder();
        let s3_config = s3_config_builder
            .region(region_provider.region().await.unwrap())
            .endpoint_url(&config.url.to_string())
            .credentials_provider(shared_creds)
            .build();

        let s3_client = aws_sdk_s3::Client::new(&s3_config);
        Ok(Self::new(s3_client))
    }

    pub async fn create_bucket(&self, name: &str) -> anyhow::Result<()> {
        self.client
            .create_bucket()
            .set_bucket(Some(name.to_string()))
            .send()
            .await?;

        Ok(())
    }

    pub async fn create_folder(&self, folder_name: &str, bucket_name: &str) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(bucket_name)
            .key(folder_name)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{e}: couldn't create bucket folder"))?;

        Ok(())
    }

    pub async fn remove_bucket(&self, name: &str) -> anyhow::Result<()> {
        self.client
            .delete_bucket()
            .set_bucket(Some(name.to_string()))
            .send()
            .await?;

        Ok(())
    }
}
