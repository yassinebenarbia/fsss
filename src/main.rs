use clap::Parser;
use std::sync::Arc;

use crate::{
    api::{spawn_http_connection, spawn_ws_connection},
    config::Config,
    postgres::CustomPostgresClient,
    redis::CustomRedisClient,
    s3::CustomS3client,
};

mod api;
mod args;
mod config;
mod http;
mod messages;
mod postgres;
mod redis;
mod s3;
mod websocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = args::Args::parse();

    let config: Config =
        serde_json::from_str(&std::fs::read_to_string(args.config).unwrap()).unwrap();

    let custom_postgres_client = Arc::new(CustomPostgresClient::new(&config.postgres).await?);
    let s3_client: Arc<CustomS3client> =
        Arc::new(CustomS3client::create_s3_client(&config.minio).await?);

    let redis_client: Arc<CustomRedisClient> =
        Arc::new(CustomRedisClient::new(&config.redis).await?);

    tokio::select! {
        _ = spawn_ws_connection(&config,
            s3_client.clone(),
            custom_postgres_client.clone(),
            redis_client.clone(),
        ) => {},
        _ = spawn_http_connection(&config,
            s3_client.clone(),
            custom_postgres_client.clone(),
            redis_client.clone(),
        ) => {}
        _ = tokio::signal::ctrl_c() => {
            println!("shutdown signal received");
        }
    }

    println!("Goodbye!");
    Ok(())
}
