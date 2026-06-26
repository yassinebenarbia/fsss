use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct S3 {
    pub url: url::Url,
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Postgres {
    pub username: String,
    pub password: String,
    pub host: String,
    pub dbname: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Redis {
    pub host: String,
    pub port: u16,
    // pub listener_timeout: u64,
    // pub max_open_clients: u64,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Websocket {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct Http {
    pub host: String,
    pub port: u16,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Config {
    pub minio: S3,
    pub postgres: Postgres,
    pub websocket: Websocket,
    pub http: Http,
    // TODO: remove Option
    pub redis: Redis,
}
