use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Message {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body,
}

#[derive(Debug, Default, Clone)]
pub struct MessageBuilder {
    pub src: String,
    pub dst: String,
    pub bodykind: BodyKind,
    pub msg_id: usize,
    pub reply_to: Option<usize>,
    pub payload: HashMap<String, Value>,
}

impl MessageBuilder {
    pub fn new() -> Self {
        Self {
            src: "src".to_string(),
            dst: "dst".to_string(),
            ..Default::default()
        }
    }

    pub fn bodykind(mut self, bodykind: BodyKind) -> Self {
        self.bodykind = bodykind;
        self
    }

    pub fn msg_id(mut self, msg_id: usize) -> Self {
        self.msg_id = msg_id;
        self
    }

    pub fn reply_to(mut self, reply_to: usize) -> Self {
        self.reply_to = Some(reply_to);
        self
    }

    pub fn insert(mut self, key: &str, value: Value) -> Self {
        self.payload.insert(key.to_string(), value);
        self
    }

    pub fn build(self) -> Message {
        Message {
            src: self.src,
            dst: self.dst,
            body: Body {
                kind: self.bodykind,
                msg_id: self.msg_id,
                reply_to: self.reply_to,
                payload: Payload(self.payload),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Body {
    #[serde(rename = "type")]
    pub kind: BodyKind,
    pub msg_id: usize,
    #[serde(rename = "in_reply_to", skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BodyKind {
    #[default]
    Init,
    InitOk,
    Echo,
    EchoOk,
    Generate,
    GenerateOk,
    Broadcast,
    BroadcastOk,
    Read,
    ReadOk,
    Topology,
    TopologyOk,
    Send,
    SendOk,
    Poll,
    PollOk,
    CommitOffsets,
    CommitOffsetsOk,
    ListCommittedOffsets,
    ListCommittedOffsetsOk,
}

// impl Default for BodyKind {
//     fn default() -> Self {
//         BodyKind::Init
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Payload(HashMap<String, Value>);

impl Payload {
    pub fn init(key: &str, value: Value) -> Self {
        let inner = HashMap::from([(key.to_string(), value)]);
        Self(inner)
    }

    pub fn get_str(&self, key: &str) -> &str {
        self.0[key].as_str().unwrap()
    }

    pub fn get_raw(&self, key: &str) -> &Value {
        &self.0[key]
    }

    pub fn get_usize(&self, key: &str) -> usize {
        self.0[key].as_u64().unwrap() as usize
    }

    pub fn put(&mut self, key: &str, value: Value) {
        self.0.insert(key.to_string(), value);
    }
}

#[cfg(test)]
mod tests {
    use std::assert_eq;

    use crate::message::{BodyKind, Payload};

    use super::{Body, MessageBuilder};

    #[test]
    fn test_flatten() {
        let body = Body {
            kind: BodyKind::Generate,
            msg_id: 0,
            reply_to: None,
            payload: Payload::default(),
        };

        let serialized_body = serde_json::to_string(&body).unwrap();
        let body: Body = serde_json::from_str(&serialized_body).unwrap();
        assert_eq!(BodyKind::Generate, body.kind);
    }

    #[test]
    fn test_message_builder() {
        let msg = MessageBuilder::new()
            .bodykind(BodyKind::BroadcastOk)
            .msg_id(10)
            .build();

        assert_eq!(BodyKind::BroadcastOk, msg.body.kind);
        assert_eq!(10, msg.body.msg_id);
    }
}
