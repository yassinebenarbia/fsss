use std::{net::IpAddr, str::FromStr, sync::Arc};

use aws_config::{SdkConfig, meta::region::RegionProviderChain};
use aws_sdk_s3::{
    config::{Credentials, SharedCredentialsProvider},
    primitives::ByteStream,
};

use axum::{
    Router,
    body::Body,
    extract::{Multipart, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    routing::post,
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use futures_util::{TryFutureExt, TryStreamExt};
use minio::s3::Client as MinioClient;
use std::net::SocketAddr;
use tokio::net::TcpListener;

use crate::{
    config::{Config, S3},
    postgres::CustomPostgresClient,
    s3::CustomS3client,
};

pub async fn spawn_http_connection(
    config: &Config,
    s3_client: Arc<CustomS3client>,
    postgres_client: Arc<CustomPostgresClient>,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/upload",
            post(upload).with_state((s3_client, postgres_client.clone())),
        )
        .route_layer(middleware::from_fn_with_state(
            postgres_client.clone(),
            auth,
        ))
        .with_state(postgres_client);

    let http_listener = TcpListener::bind((config.http.host.clone(), config.http.port)).await?;
    axum::serve(http_listener, app).await?;
    Ok(())
}

// #[axum::debug_handler]
async fn upload<'a>(
    State((s3_client, postgres_client)): State<(Arc<CustomS3client>, Arc<CustomPostgresClient>)>,
    mut multipart: Multipart,
) -> String {
    let mut server_id = String::new();

    while let Some(field) = multipart.next_field().await.unwrap() {
        let filename = field
            .file_name()
            .map(sanitize_filename)
            .unwrap_or("upload.bin".to_string());

        println!("name: {:?}", field.name());
        match field.name() {
            Some(s) => match s {
                "server_id" => server_id.push_str(&field.text().await.unwrap()),
                "file" => {
                    println!("{:?}", server_id);
                    if !server_id.is_empty() {
                        println!("server_id: {server_id}");
                        let bucket_id = postgres_client.get_bucket_id(&server_id).await.unwrap();
                        println!("bucket-id: {}", bucket_id.to_string());
                        match s3_client
                            .client
                            .put_object()
                            .bucket(&bucket_id.to_string())
                            .key(filename)
                            .body(ByteStream::from(field.bytes().await.unwrap()))
                            .send()
                            .await
                        {
                            Ok(_) => return String::from("upload complete"),
                            Err(e) => return e.to_string(),
                        }
                    }
                }
                _ => {}
            },
            None => {}
        }
        println!("O {server_id}");
    }

    String::new()
}

// for http
fn sanitize_filename(name: &str) -> String {
    name.replace('/', "_").replace('\\', "_")
}

// for http
async fn auth(
    State(state): State<Arc<CustomPostgresClient>>,
    TypedHeader(auth): TypedHeader<Authorization<Bearer>>,
    req: Request<Body>,
    next: Next,
) -> Result<axum::response::Response, StatusCode> {
    let token = auth.token();
    println!("Auth token: {}", token);
    if state.token_exist_and_not_expired(token).await.unwrap() {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
