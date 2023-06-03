use crate::{
    message::{Body, BodyKind, Message, Payload},
    server::{HasInner, Serve, ServerInner},
};
use async_trait::async_trait;
use core::panic;
use serde_json::json;
use std::collections::HashMap;

use super::storage::Storage;

#[derive(Debug, Default)]
pub struct KafkaServer {
    inner: ServerInner,
    storage: Option<Storage>,
}

impl KafkaServer {
    /**
     * This message requests that a "msg" value be appended to a log identified by "key"
     */
    async fn send(&mut self, msg: &Message) -> Message {
        let key = msg.body.payload.get_str("key");
        let content = msg.body.payload.get_usize("msg");

        let offset = self.storage_mut().append(key, content).await;

        let body = Body {
            kind: BodyKind::SendOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::init("offset", json!(offset)),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }

    fn storage_mut(&mut self) -> &mut Storage {
        match &mut self.storage {
            Some(ref mut s) => s,
            None => panic!("storage error"),
        }
    }

    fn storage(&self) -> &Storage {
        self.storage.as_ref().unwrap()
    }

    /**
     * This message requests that a node return messages from a set of logs
     * starting from the given offset in each log
     */
    pub async fn poll(&mut self, msg: &Message) -> Message {
        let offsets = msg.body.payload.get_raw("offsets").clone();
        let offsets: HashMap<String, usize> = serde_json::from_value(offsets).unwrap();

        let msgs = self.storage().read_from(&offsets).await;
        let body = Body {
            kind: BodyKind::PollOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::init(
                "msgs",
                serde_json::to_value(msgs).expect("failed to serialize msgs in poll"),
            ),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }
}

#[async_trait]
impl Serve for KafkaServer {
    async fn reply(&mut self, msg: &Message) -> Option<Message> {
        match msg.body.kind {
            BodyKind::Init => {
                let reply_msg = self.inner.init(msg);
                self.storage = Some(Storage::new(self.inner.node_id()).await);
                reply_msg
            }
            BodyKind::Send => Some(self.send(msg).await),
            BodyKind::Poll => Some(self.poll(msg).await),
            _ => panic!("{}", format!("cannot handle msg: {:?}", msg)),
        }
    }
}

impl HasInner for KafkaServer {
    fn as_inner(&mut self) -> &mut ServerInner {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, fs};

    use crate::{
        kafka::storage::tests::clean_disk_data,
        message::{BodyKind, MessageBuilder},
        server::Serve,
        utils::tests::generate_random_node_id,
    };
    use serde_json::json;

    use crate::kafka::KafkaServer;

    #[tokio::test]
    async fn test_send() {
        let node_id = generate_random_node_id();
        let mut server = KafkaServer::default();
        let msg = MessageBuilder::new()
            .insert("node_id", json!(&node_id))
            .build();
        server.reply(&msg).await;

        let msg = MessageBuilder::new()
            .bodykind(BodyKind::Send)
            .msg_id(200)
            .insert("key", json!("k1"))
            .insert("msg", json!(123))
            .build();
        let reply_msg = server.send(&msg).await;

        assert_eq!(BodyKind::SendOk, reply_msg.body.kind);
        assert_eq!(0, server.storage().offsets()["k1"]);
        let buffer = fs::read_to_string(format!("log/{}", server.storage().log_name())).unwrap();
        let buffer = buffer.trim_end();
        assert_eq!("0:k1:123", buffer);

        clean_disk_data(&node_id).await;
    }

    #[tokio::test]
    async fn test_poll() {
        let node_id = generate_random_node_id();
        let mut server = KafkaServer::default();
        let msg = MessageBuilder::new()
            .insert("node_id", json!(&node_id))
            .build();
        server.reply(&msg).await;

        let builder = MessageBuilder::new().bodykind(BodyKind::Send);

        let msg = builder
            .clone()
            .insert("key", json!("k1"))
            .insert("msg", json!(123))
            .build();
        server.send(&msg).await;
        let msg = builder
            .clone()
            .insert("key", json!("k1"))
            .insert("msg", json!(123))
            .build();
        server.send(&msg).await;
        let msg = builder
            .clone()
            .insert("key", json!("k2"))
            .insert("msg", json!(122))
            .build();
        server.send(&msg).await;

        let msg = MessageBuilder::new()
            .bodykind(BodyKind::Poll)
            .insert(
                "offsets",
                serde_json::to_value(HashMap::from([
                    ("k1".to_string(), 1),
                    ("k2".to_string(), 0),
                ]))
                .unwrap(),
            )
            .build();
        let reply_msg = server.reply(&msg).await.unwrap();
        let msgs = reply_msg.body.payload.get_raw("msgs");
        let msgs: HashMap<String, Vec<[usize; 2]>> = serde_json::from_value(msgs.clone()).unwrap();
        assert_eq!([1_usize, 123_usize], msgs["k1"][0]);
        assert_eq!([0_usize, 122_usize], msgs["k2"][0]);

        clean_disk_data(&node_id).await;
    }
}
