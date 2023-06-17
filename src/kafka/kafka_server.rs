use crate::{
    message::{Body, BodyKind, Message, Payload},
    server::{HasInner, Serve, ServerInner},
};
use async_trait::async_trait;
use core::panic;
use serde_json::json;
use std::collections::{HashMap, HashSet};

use super::storage::Storage;

#[derive(Debug, Default)]
pub struct KafkaServer {
    inner: ServerInner,
    storage: Option<Storage>,
    commit_offsets: HashMap<String, usize>,
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

    /**
     * This message informs the node that messages have been successfully
     * processed up to and including the given offset.
     * Your node will receive a request message body that looks like this:
     * {
     *      "type": "commit_offsets",
     *       "offsets": {
     *          "k1": 1000,
     *          "k2": 2000
     *      }
     * }
     */
    pub async fn commit_offsets(&mut self, msg: &Message) -> Message {
        let offsets = msg.body.payload.get_raw("offsets").clone();
        let offsets: HashMap<String, usize> = serde_json::from_value(offsets).unwrap();
        self.merge_offsets(&offsets);

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body: Body { 
                kind: BodyKind::CommitOffsetsOk,
                msg_id: self.inner.next_msg_id(),
                reply_to: Some(msg.body.msg_id),
                payload: Payload::default(),
            },
        }
    }

    fn merge_offsets(&mut self, offsets: &HashMap<String, usize>) {
        for (key, offset) in offsets.iter() {
            self.commit_offsets.insert(key.to_string(), *offset);
        }
    }

    /**
     * This message returns a map of committed offsets for a given set of logs.
     * Clients use this to figure out where to start consuming from in a given log.
     */
    pub async fn list_committed_offsets(&mut self, msg: &Message) -> Message {
        let keys = msg.body.payload.get_raw("keys").clone();
        let keys: Vec<String> = serde_json::from_value(keys).unwrap();
        let keys: HashSet<String> = HashSet::from_iter(keys);

        let filtered: HashMap<&String, &usize> = self.commit_offsets
            .iter()
            .filter(|&(k,_)| keys.contains(k))
            .collect();

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body: Body { 
                kind: BodyKind::ListCommittedOffsetsOk,
                msg_id: self.inner.next_msg_id(),
                reply_to: Some(msg.body.msg_id),
                payload: Payload::init("offsets", json!(filtered)),
            },
        }
    }
}

#[async_trait]
impl Serve for KafkaServer {
    async fn reply(&mut self, msg: &Message) -> Option<Message> {
        match msg.body.kind {
            BodyKind::Init => {
                let reply_msg = self.inner.init(msg);
                self.storage = Some(Storage::new().await);
                reply_msg
            }
            BodyKind::Send => Some(self.send(msg).await),
            BodyKind::Poll => Some(self.poll(msg).await),
            BodyKind::CommitOffsets => Some(self.commit_offsets(msg).await),
            BodyKind::ListCommittedOffsets => Some(self.list_committed_offsets(msg).await),
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
        let reply_msg = server.reply(&msg).await.unwrap();

        assert_eq!(BodyKind::SendOk, reply_msg.body.kind);
        assert_eq!(0, server.storage().offsets()["k1"]);
        let buffer = fs::read_to_string(format!("log/{}", server.storage().log_name())).unwrap();
        let buffer = buffer.trim_end();
        assert_eq!("0:k1:123", buffer);

        drop(server);
        clean_disk_data().await;
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
        server.reply(&msg).await;
        let msg = builder
            .clone()
            .insert("key", json!("k1"))
            .insert("msg", json!(123))
            .build();
        server.reply(&msg).await;
        let msg = builder
            .clone()
            .insert("key", json!("k2"))
            .insert("msg", json!(122))
            .build();
        server.reply(&msg).await;

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

        drop(server);
        clean_disk_data().await;
    }

    #[tokio::test]
    async fn test_commit_offsets() {
        let node_id = generate_random_node_id();
        let mut server = KafkaServer::default();
        let msg = MessageBuilder::new()
            .insert("node_id", json!(&node_id))
            .build();
        server.reply(&msg).await;

        let builder = MessageBuilder::new().bodykind(BodyKind::CommitOffsets);

        let msg = builder
            .clone()
            .insert("offsets", json!(HashMap::from([("k1", 1000), ("k2", 2000)])))
            .build();
        let reply_msg = server.reply(&msg).await.unwrap();
        assert_eq!(BodyKind::CommitOffsetsOk, reply_msg.body.kind);

        drop(server);
        clean_disk_data().await;
    }

    #[tokio::test]
    async fn test_list_committed_offsets() {
        let node_id = generate_random_node_id();
        let mut server = KafkaServer::default();
        let msg = MessageBuilder::new()
            .insert("node_id", json!(&node_id))
            .build();
        server.reply(&msg).await;

        let builder = MessageBuilder::new().bodykind(BodyKind::CommitOffsets);
        let msg = builder
            .clone()
            .insert("offsets", json!(HashMap::from([("k1", 1000), ("k2", 2000)])))
            .build();
        server.reply(&msg).await;

        let msg = builder
            .clone()
            .insert("offsets", json!(HashMap::from([("k3", 2000), ("k2", 2500)])))
            .build();
        server.reply(&msg).await;

        let msg = MessageBuilder::new()
            .bodykind(BodyKind::ListCommittedOffsets)
            .insert("keys", json!(vec!["k1", "k2"]))
            .build();
        let reply_msg = server.reply(&msg).await.unwrap();
        assert_eq!(BodyKind::ListCommittedOffsetsOk, reply_msg.body.kind);

        let ret_offsets = reply_msg.body.payload.get_raw("offsets");
        let ret_offsets: HashMap<String, usize> = serde_json::from_value(ret_offsets.clone()).unwrap();
        assert_eq!(1000, ret_offsets["k1"]);
        assert_eq!(2500, ret_offsets["k2"]);

        drop(server);
        clean_disk_data().await;
    }
}
