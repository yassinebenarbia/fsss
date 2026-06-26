use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use anyhow::anyhow;
use axum::{extract::ws, middleware::map_request_with_state};
use futures_util::{SinkExt, StreamExt, TryFutureExt};
use rand::{
    Rng,
    distr::{Alphabetic, Alphanumeric, SampleString},
};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, Utf8Bytes, protocol::CloseFrame},
};
use uuid::Uuid;

struct User {
    username: String,
    password: String,
    user_id: Uuid,
    token: String,
}

impl User {
    /// Save account details to a file
    fn save(&self, file_path: &Option<PathBuf>) -> anyhow::Result<()> {
        todo!()
    }

    async fn ban(&self, user: &User) -> anyhow::Result<()> {
        let request = json!(
        {
          "version": 1.0,
          "payload": {
            "BanUserFromFriends": {
              "token": self.token,
              "banned_id": user.user_id
            }
          }
        }
                );

        let mut conn = Connection::create().await?;
        let response = conn.send(&request).await?;
        println!("Ban Response: {:?}", response);
        response.get("content").unwrap().get("Ok").unwrap();
        Ok(())
    }

    async fn get_dms(&self, user: &User) -> anyhow::Result<Value> {
        let request = json!(
        {
            "version": 1.0,
            "payload": {
                "GetDMs": {
                    "token": self.token,
                    "user_id": user.user_id,
                }
            }
        }
                    );

        let mut conn = Connection::create().await?;
        println!("Connection created");
        let response = conn.send(&request).await?;
        println!("DMs: {:?}", response);
        return Ok(response);
    }

    async fn dm(&self, user: &User, text: Option<String>) -> anyhow::Result<Value> {
        let message_content = text.unwrap_or("Am I banned?".into());
        let request = json!(
        {
            "version": 1.0,
            "payload": {
                "SendDM": {
                    "token": self.token,
                    "receiver": user.user_id,
                    "message_kind": "Text",
                    "message_content": message_content
                }
            }
        }
                    );

        let mut conn = Connection::create().await?;
        println!("Connection created");
        let response = conn.send(&request).await?;
        println!("DM Response: {:?}", response);
        return Ok(response);
    }

    async fn register() -> anyhow::Result<Self> {
        let mut rng = rand::rng();
        let username: String = (0..7).map(|_| rng.sample(Alphabetic) as char).collect();
        let password = Alphanumeric.sample_string(&mut rand::thread_rng(), 10);
        let request = json!(
        {
          "version": 1.0,
          "payload": {
            "RegisterRequest": {
              "username": username,
              "password": password
            }
          }
        }
                        );

        println!("Registration Request:\n {:?}", request.to_string());

        let mut conn = Connection::create().await?;
        let response = conn.send(&request).await?;
        let user_id = Uuid::from_str(
            response
                .get("content")
                .unwrap()
                .get("Token")
                .unwrap()
                .get("associated_user_id")
                .unwrap()
                .as_str()
                .unwrap(),
        )
        .unwrap();

        let token = response
            .get("content")
            .unwrap()
            .get("Token")
            .unwrap()
            .get("token")
            .unwrap()
            .get("token")
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned();

        Ok(Self {
            username,
            password,
            user_id,
            token,
        })
    }

    async fn login() -> anyhow::Result<Self> {
        let username = "";
        let password = "";
        let request = json!(
        {
          "version": 1.0,
          "payload": {
            "LoginRequest": {
                "username": username,
                "password": password
            }
          }
        }
                );
        let mut conn = Connection::create().await?;
        conn.send(&request).await?;
        todo!()
    }
}

struct Connection {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    latest_response: Option<Value>,
}

impl Connection {
    fn new(
        inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
        latest_response: Option<Value>,
    ) -> Self {
        Self {
            inner,
            latest_response,
        }
    }

    async fn create() -> anyhow::Result<Self> {
        let url = "ws://127.0.0.1:8081";
        let (ws_stream, response) = connect_async(url).await.expect("Failed to connect");

        println!("Connected with status: {}", response.status());
        Ok(Self::new(ws_stream, None))
    }

    async fn send(&mut self, value: &Value) -> anyhow::Result<Value> {
        self.inner
            .send(Message::Text(serde_json::to_string(value)?.into()))
            .await?;

        while let Some(result) = self.inner.next().await {
            match result {
                Ok(Message::Text(text)) => {
                    println!("Server says: {}", text);
                    self.inner.close(None).await.ok();
                    return Ok(Value::from_str(text.as_str())?);
                }
                Ok(Message::Close(frame)) => {
                    println!("Server closed: {:?}", frame);
                    self.inner.close(None).await.ok();
                    return Ok(Value::Null);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return Err(anyhow!(e));
                }
                _ => return Err(anyhow!("Unexpected response")),
            }
        }
        // reachable?
        return Err(anyhow!("Unexpected response"));
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.inner
            .close(Some(CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: Utf8Bytes::from_static("okak"),
            }))
            .await?;
        todo!()
    }
}

mod tests {
    use serde_json::Value;

    use crate::User;

    fn check_dm_failure(value: &Value, message: Option<String>) -> bool {
        match message {
            Some(m) => value
                .get("content")
                .unwrap()
                .get("Error")
                .unwrap()
                .as_str()
                .unwrap()
                .eq(&m),
            None => value.get("content").unwrap().get("Error").is_some(),
        }
    }

    fn check_dm_success(value: &Value) -> bool {
        value.get("content").unwrap().get("OK").is_some()
    }

    fn check_dms_list(value: &Value) -> bool {
        value
            .get("content")
            .unwrap()
            .get("DirectMessageList")
            .is_some()
    }

    #[tokio::test]
    async fn get_dms_flow() -> anyhow::Result<()> {
        let user1 = User::register().await?;
        let user2 = User::register().await?;

        check_dm_success(&user1.dm(&user2, Some("Hi!".into())).await?);
        check_dm_success(&user2.dm(&user1, Some("Hello".into())).await?);

        check_dms_list(&user1.get_dms(&user2).await?);

        Ok(())
    }

    #[tokio::test]
    async fn dm_flow() -> anyhow::Result<()> {
        let user1 = User::register().await?;
        let user2 = User::register().await?;

        check_dm_success(&user1.dm(&user2, None).await?);
        check_dm_success(&user2.dm(&user1, None).await?);
        Ok(())
    }

    #[tokio::test]
    async fn ban_dm_flow() -> anyhow::Result<()> {
        let user1 = User::register().await?;
        let user2 = User::register().await?;

        user1.ban(&user2).await?;
        check_dm_failure(&user1.dm(&user2, None).await?, None);
        check_dm_failure(&user2.dm(&user1, None).await?, None);
        Ok(())
    }
}
