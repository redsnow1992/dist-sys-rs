use anyhow::Result;
use core::panic;
use dist_sys_rs::{
    message::{Body, BodyKind, Message, Payload},
    server::Server,
};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[derive(Debug, Default)]
pub struct UniqueIdServer {
    pub node_id: String,
    /// last-generated time
    pub last_ts: u64,
    /// a counter for IDs generated at that timestamp
    pub count: usize,
    pub next_msg_id: usize,
}

impl UniqueIdServer {
    fn compose_id(&self) -> String {
        format!("{}{}{}", self.node_id(), self.last_ts, self.count)
    }

    fn generate_id(&mut self) -> String {
        let ts = std::cmp::max(current_time_millis(), self.last_ts);
        if ts == self.last_ts {
            self.count += 1;
        }
        self.last_ts = ts;

        self.compose_id()
    }

    pub fn generate(&mut self, dst: &String, reply_msg_id: usize) -> Message {
        self.next_msg_id += 1;

        let body = Body {
            kind: BodyKind::GenerateOk,
            msg_id: self.next_msg_id,
            reply_to: Some(reply_msg_id),
            payload: Payload::init("id", json!(self.generate_id())),
        };

        Message {
            src: self.node_id.clone(),
            dst: dst.clone(),
            body,
        }
    }
}

impl Server for UniqueIdServer {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn set_node_id(&mut self, node_id: &str) {
        self.node_id = node_id.to_string();
    }

    fn reply(&mut self, msg: &Message) -> Message {
        match &msg.body.kind {
            BodyKind::Init => self.init(&msg.src, msg.body.payload.get_str("node_id")),
            BodyKind::Generate => self.generate(&msg.src, msg.body.msg_id),
            _ => panic!("receive ${:?}", msg),
        }
    }
}

fn main() -> Result<()> {
    let mut server = UniqueIdServer::default();
    server.eventloop()
}
