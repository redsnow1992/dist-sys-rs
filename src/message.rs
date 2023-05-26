use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body {
    #[serde(rename = "type")]
    pub kind: BodyKind,
    pub msg_id: usize,
    #[serde(rename = "in_reply_to", skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BodyKind {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Payload(HashMap<String, Value>);

impl Payload {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn init(key: &str, value: Value) -> Self {
        let inner = HashMap::from([(key.to_string(), value)]);
        Self(inner)
    }

    pub fn get_str(&self, key: &str) -> &str {
        self.0[key].as_str().unwrap()
    }

    pub fn get(&self, key: &str) -> &Value {
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

    use super::Body;

    #[test]
    fn test_flatten() {
        let body = Body {
            kind: BodyKind::Generate,
            msg_id: 0,
            reply_to: None,
            payload: Payload::new(),
        };

        let serialized_body = serde_json::to_string(&body).unwrap();
        let body: Body = serde_json::from_str(&serialized_body).unwrap();
        assert_eq!(BodyKind::Generate, body.kind);
    }
}
