use std::{net::IpAddr, str::FromStr, sync::Arc};

use aws_config::{SdkConfig, meta::region::RegionProviderChain};
use aws_sdk_s3::{
    config::{Credentials, SharedCredentialsProvider},
    primitives::ByteStream,
};

use axum::{
    Extension, Router,
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
use uuid::Uuid;

use crate::{
    config::{Config, S3},
    postgres::CustomPostgresClient,
    s3::CustomS3client,
};

#[derive(serde::Serialize, serde::Deserialize, Clone)]
struct User {
    token: uuid::Uuid,
}

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
    Extension(token): Extension<String>,
    State((s3_client, postgres_client)): State<(Arc<CustomS3client>, Arc<CustomPostgresClient>)>,
    mut multipart: Multipart,
) -> String {
    let mut server_id = String::new();
    let mut space_id = String::new();

    while let Some(field) = multipart.next_field().await.unwrap() {
        let filename = field
            .file_name()
            .map(sanitize_filename)
            .unwrap_or("upload.bin".to_string());

        match field.name() {
            Some(s) => match s {
                "server_id" => server_id.push_str(&field.text().await.unwrap()),
                "space_id" => space_id.push_str(&field.text().await.unwrap()),
                "file" => {
                    // TODO: check if server exist
                    // TODO: check if user is joined in
                    // TODO: check if user have suffice previlages
                    // TODO: check if space exist
                    // TODO: upload the file
                    // TODO: create MESSAGE entry in db
                    // TODO: update space MESSAGE UUIDs with MESSAGE UUID
                    if !(server_id.is_empty() && space_id.is_empty()) {
                        // TODO: handle inner fields with a function to avoid nesting
                        let user_id = postgres_client
                            .get_user_id_from_token(&token)
                            .await
                            .unwrap();

                        if !postgres_client
                            .is_joined(&server_id, user_id)
                            .await
                            .unwrap()
                        {
                            return String::from("Not joined");
                        }

                        if !postgres_client
                            .contains_space(&server_id, &space_id)
                            .await
                            .unwrap()
                        {
                            return String::from("Space does not exist in server!");
                        }

                        let bucket_id = postgres_client
                            .get_space_bucket_id(&space_id)
                            .await
                            .unwrap();

                        match s3_client
                            .client
                            .put_object()
                            .bucket(&bucket_id.to_string())
                            .key(&filename)
                            .body(ByteStream::from(field.bytes().await.unwrap()))
                            .send()
                            .await
                        {
                            Ok(_) => {
                                postgres_client
                                    .write_file_message(
                                        &Uuid::from_str(&server_id).unwrap(),
                                        &Uuid::from_str(&space_id).unwrap(),
                                        user_id,
                                        &crate::postgres::MessageKind::FILE,
                                        &filename,
                                    )
                                    .await
                                    .unwrap();

                                return String::from("upload complete");
                            }
                            Err(e) => return e.to_string(),
                        }
                    } else {
                        return String::from("File header field be put last!");
                    }
                }
                _ => {}
            },
            None => {}
        }
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
    mut req: Request<Body>,
    next: Next,
) -> Result<axum::response::Response, StatusCode> {
    let token = auth.token();
    if state.token_exist_and_not_expired(&token).await.unwrap() {
        req.extensions_mut().insert(token.to_string());
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
