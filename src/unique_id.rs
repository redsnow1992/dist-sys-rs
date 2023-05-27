use anyhow::Result;
use core::panic;
use dist_sys_rs::{
    message::{Body, BodyKind, Message, Payload},
    server::{HasInner, Serve, ServerInner},
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
    inner: ServerInner,
    /// last-generated time
    pub last_ts: u64,
    /// a counter for IDs generated at that timestamp
    pub count: usize,
}

impl UniqueIdServer {
    fn compose_id(&self) -> String {
        format!("{}{}{}", self.inner.node_id(), self.last_ts, self.count)
    }

    fn generate_id(&mut self) -> String {
        let ts = std::cmp::max(current_time_millis(), self.last_ts);
        if ts == self.last_ts {
            self.count += 1;
        }
        self.last_ts = ts;

        self.compose_id()
    }

    pub fn generate(&mut self, msg: &Message) -> Message {
        let body = Body {
            kind: BodyKind::GenerateOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::init("id", json!(self.generate_id())),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }
}

impl Serve for UniqueIdServer {
    fn reply(&mut self, msg: &Message) -> Message {
        match &msg.body.kind {
            BodyKind::Init => self.inner.init(&msg),
            BodyKind::Generate => self.generate(&msg),
            _ => panic!("receive ${:?}", msg),
        }
    }
}

impl HasInner for UniqueIdServer {
    fn into_inner(&mut self) -> &mut ServerInner {
        &mut self.inner
    }
}

fn main() -> Result<()> {
    let mut server = UniqueIdServer::default();
    server.serve()
}
