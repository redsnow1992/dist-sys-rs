use std::collections::HashMap;

use anyhow::Result;

use dist_sys_rs::{
    message::{Body, BodyKind, Message, Payload},
    server::{HasInner, Serve, ServerInner},
};
use serde_json::json;

#[derive(Debug, Default)]
pub struct BroadcastServer {
    inner: ServerInner,
    pub messages: Vec<usize>,
    pub topology: HashMap<String, Vec<String>>,
}

impl BroadcastServer {
    pub fn broadcast(&mut self, msg: &Message) -> Message {
        self.messages.push(msg.body.payload.get_usize("message"));

        let body = Body {
            kind: BodyKind::BroadcastOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::new(),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.clone(),
            body,
        }
    }

    pub fn read(&mut self, msg: &Message) -> Message {
        let body = Body {
            kind: BodyKind::ReadOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::init("messages", json!(self.messages)),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }

    pub fn topology(&mut self, msg: &Message) -> Message {
        self.topology = HashMap::new();
        let topology_value = msg.body.payload.get_raw("topology");
        for (k, v) in topology_value.as_object().unwrap().iter() {
            let dsts: Vec<String> = v
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect();
            self.topology.insert(k.to_string(), dsts);
        }

        let body = Body {
            kind: BodyKind::TopologyOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::new(),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }
}

impl Serve for BroadcastServer {
    fn reply(&mut self, msg: &Message) -> Message {
        match &msg.body.kind {
            BodyKind::Init => self.inner.init(msg),
            BodyKind::Broadcast => self.broadcast(msg),
            BodyKind::Read => self.read(msg),
            BodyKind::Topology => self.topology(msg),
            _ => panic!("receive ${:?}", msg),
        }
    }
}

impl HasInner for BroadcastServer {
    fn into_inner(&mut self) -> &mut ServerInner {
        &mut self.inner
    }
}

fn main() -> Result<()> {
    let mut server = BroadcastServer::default();
    server.serve()
}
