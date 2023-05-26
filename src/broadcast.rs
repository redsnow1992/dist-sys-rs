use std::collections::HashMap;

use anyhow::Result;

use dist_sys_rs::{server::Server, message::{Message, BodyKind, Body, Payload}};
use serde_json::{json, Value};

#[derive(Debug, Default)]
pub struct BroadcastServer {
    pub node_id: String,
    next_msg_id: usize,
    pub messages: Vec<usize>,
    pub topology: HashMap<String, Vec<String>>,
}

impl BroadcastServer {
    pub fn broadcast(&mut self, dst: &String, reply_msg_id: usize, msg: usize) -> Message {
        self.next_msg_id += 1;
        self.messages.push(msg);

        let body = Body {
            kind: BodyKind::BroadcastOk,
            msg_id: self.next_msg_id,
            reply_to: Some(reply_msg_id),
            payload: Payload::new(),
        };

        Message {
            src: self.node_id.clone(),
            dst: dst.clone(),
            body,
        }
    }

    pub fn read(&mut self, dst: &String, reply_msg_id: usize) -> Message {
        self.next_msg_id += 1;

        let body = Body {
            kind: BodyKind::ReadOk,
            msg_id: self.next_msg_id,
            reply_to: Some(reply_msg_id),
            payload: Payload::init("messages", json!(self.messages)),
        };

        Message {
            src: self.node_id.clone(),
            dst: dst.clone(),
            body,
        }
    }

    pub fn topology(&mut self, dst: &String, reply_msg_id: usize, topology: &Value) -> Message {
        self.next_msg_id += 1;
        self.topology = HashMap::new();
        for (k, v) in topology.as_object().unwrap().iter() {
            let dsts: Vec<String> = v.as_array().unwrap()
                .iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect();
            self.topology.insert(k.to_string(), dsts);
        };

        let body = Body {
            kind: BodyKind::TopologyOk,
            msg_id: self.next_msg_id,
            reply_to: Some(reply_msg_id),
            payload: Payload::new(),
        };

        Message {
            src: self.node_id.clone(),
            dst: dst.clone(),
            body,
        }
    }
}

impl Server for BroadcastServer {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn set_node_id(&mut self, node_id: &str) {
        self.node_id = node_id.to_string();
    }

    fn reply(&mut self, msg: &Message) -> Message {
        match &msg.body.kind {
            BodyKind::Init => self.init(&msg.src, msg.body.payload.get_str("node_id")),
            BodyKind::Broadcast => self.broadcast(&msg.src, msg.body.msg_id, msg.body.payload.get_usize("message")),
            BodyKind::Read => self.read(&msg.src, msg.body.msg_id),
            BodyKind::Topology => self.topology(&msg.src, msg.body.msg_id, msg.body.payload.get("topology")),
            _ => panic!("receive ${:?}", msg),
        }
    }
}

fn main() -> Result<()>{
    let mut server = BroadcastServer::default();
    server.eventloop()
}