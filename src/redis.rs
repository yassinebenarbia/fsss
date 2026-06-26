// TODO: creating a server doesn't forcefully join you to the server, fix it
// So basically, I need a way to say, hey, I'm interested in this server notifications, can you
// please send them whenever there is any?
// proposition:
// - make a SubscribeToServer request that allows user to subscribe to their joined serves
// - a created server is subscribed to by default
use std::sync::Arc;

use redis::{
    self, Commands,
    aio::{ConnectionManager, PubSubSink},
};

use crate::{
    api::Notification,
    config,
    messages::{write_dm::WriteDM, write_message::WriteMessageRequest},
};

pub struct CustomRedisClient {
    pub client: redis::Client,
}

impl CustomRedisClient {
    pub async fn new(config: &config::Redis) -> anyhow::Result<Self> {
        let client = redis::Client::open(format!(
            "redis://{}:{}/?protocol=3",
            config.host, config.port
        ))?;
        Ok(Self { client })
    }

    /// * notification: Websocket notification
    /// * channel_path: ChannelPath
    pub async fn publish_notification<T1: ToString, T2: ToString, T3: ToString>(
        &self,
        notification: &Notification,
        channel_path: ChannelPath<T1, T2, T3>,
    ) -> anyhow::Result<()> {
        let mut client = self.client.clone();
        let str_notification = serde_json::to_string(&notification)?;
        let publish_channel = channel_path.to_string();
        println!("publish channel: {publish_channel}");
        let _: () = client.publish(publish_channel, str_notification)?;
        Ok(())
    }

    pub async fn conn_manager(&self) -> anyhow::Result<Arc<ConnectionManager>> {
        Ok(Arc::new(self.client.get_connection_manager().await?))
    }
}

pub struct CustomRedisPubSink {
    pub sink: PubSubSink,
}

impl CustomRedisPubSink {
    pub fn new(sink: PubSubSink) -> Self {
        Self { sink }
    }

    pub async fn unsubscribe_all(&mut self) -> anyhow::Result<()> {
        self.sink.unsubscribe("").await?;
        Ok(())
    }

    pub async fn unsubscribe(
        &mut self,
        user_id: impl ToString,
        server_id: impl ToString,
        space_id: impl ToString,
    ) -> anyhow::Result<()> {
        self.sink
            .unsubscribe(format!(
                "{}/{}/{}",
                server_id.to_string(),
                space_id.to_string(),
                user_id.to_string(),
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn subscribe<T1: ToString, T2: ToString, T3: ToString>(
        &mut self,
        channel_path: ChannelPath<T1, T2, T3>,
    ) -> anyhow::Result<()> {
        // FIXME: add user_id and make it work
        let subscribe_channel = channel_path.to_string();
        println!("subscribe channel: {subscribe_channel}");
        self.sink.psubscribe(&subscribe_channel).await?;
        Ok(())
    }
}

pub enum ChannelPath<T1: ToString, T2: ToString, T3: ToString> {
    ServerPath {
        server_id: T1,
        space_id: Option<T2>,
        user_id: Option<T3>,
    },
    DmPath {
        user_id: T1,
    },
}

impl From<&WriteDM> for ChannelPath<String, String, String> {
    fn from(value: &WriteDM) -> Self {
        match &value.reply {
            Some(replied_id) => ChannelPath::new_dm_path(value.receiver.to_string()),
            None => ChannelPath::new_dm_path(value.receiver.to_string()),
        }
    }
}

impl From<&WriteMessageRequest> for ChannelPath<String, String, String> {
    fn from(value: &WriteMessageRequest) -> Self {
        match &value.reply {
            Some(replied_id) => ChannelPath::new_server_path(
                value.server_id.to_string(),
                Some(value.space_id.to_string()),
                Some(replied_id.to_string()),
            ),
            None => ChannelPath::new_server_path(
                value.server_id.to_string(),
                Some(value.space_id.to_string()),
                None,
            ),
        }
    }
}

impl<T1: ToString, T2: ToString, T3: ToString> ChannelPath<T1, T2, T3> {
    pub fn new_server_path(server_id: T1, space_id: Option<T2>, user_id: Option<T3>) -> Self {
        Self::ServerPath {
            user_id,
            server_id,
            space_id,
        }
    }
}

impl<T1: ToString> ChannelPath<T1, T1, T1> {
    pub fn new_dm_path(user_id: T1) -> Self
    where
        T1: ToString,
    {
        Self::DmPath { user_id }
    }
}

impl<T1: ToString, T2: ToString, T3: ToString> ToString for ChannelPath<T1, T2, T3> {
    fn to_string(&self) -> String {
        match self {
            ChannelPath::ServerPath {
                server_id,
                space_id,
                user_id,
            } => {
                let mut channel_path = (*server_id).to_string();
                match space_id {
                    Some(space_id) => channel_path.push_str(&format!("/{}", space_id.to_string())),
                    None => {}
                }
                match user_id {
                    Some(user_id) => channel_path.push_str(&format!("/{}", user_id.to_string())),
                    None => {}
                }
                channel_path.push_str("/*");
                return channel_path;
            }
            ChannelPath::DmPath { user_id } => return format!("{}/*", user_id.to_string()),
        }
    }
}
