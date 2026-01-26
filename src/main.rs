use ::postgres::NoTls;
use clap::Parser;
use std::sync::Arc;

use crate::{
    api::{spawn_http_connection, spawn_ws_connection},
    config::Config,
    postgres::CustomPostgresClient,
    s3::CustomS3client,
};

mod api;
mod args;
mod config;
mod http;
mod postgres;
mod s3;
mod websocket;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = args::Args::parse();

    let config: Config =
        serde_json::from_str(&std::fs::read_to_string(args.config).unwrap()).unwrap();

    // postgres
    let mut postgres_config = tokio_postgres::Config::new();

    let (postgres_client, postgres_connection) = postgres_config
        .host(&config.postgres.host)
        .port(config.postgres.port)
        .user(&config.postgres.username)
        .password(&config.postgres.password)
        .dbname("mydb")
        .connect(NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = postgres_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let custom_postgres_client = Arc::new(CustomPostgresClient::new(postgres_client));

    let s3_client = Arc::new(CustomS3client::create_s3_client(&config.minio).await?);

    tokio::select! {
        _ = spawn_ws_connection(&config,
            s3_client.clone(),
            custom_postgres_client.clone(),
        ) => {},
        _ = spawn_http_connection(&config,
            s3_client.clone(),
            custom_postgres_client.clone(),
        ) => {}
        _ = tokio::signal::ctrl_c() => {
            println!("shutdown signal received");
        }
    }

    println!("Goodbye!");
    Ok(())
}
