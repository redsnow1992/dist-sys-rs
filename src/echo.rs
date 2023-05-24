use anyhow::Result;
use core::panic;
use dist_sys_rs::{
    message::{Body, BodyKind, Message, Payload},
    server::Server,
};
use serde_json::json;

#[derive(Debug, Default)]
pub struct EchoServer {
    node_id: String,
    next_msg_id: usize,
}

impl EchoServer {
    fn echo(&mut self, dst: &String, reply_msg_id: usize, echo: &str) -> Message {
        self.next_msg_id += 1;

        let body = Body {
            kind: BodyKind::EchoOk,
            msg_id: self.next_msg_id,
            reply_to: Some(reply_msg_id),
            payload: Payload::init("echo", json!(echo)),
        };

        Message {
            src: self.node_id.clone(),
            dst: dst.clone(),
            body,
        }
    }
}

impl Server for EchoServer {
    fn set_node_id(&mut self, node_id: &str) {
        self.node_id = node_id.to_string();
    }

    fn reply(&mut self, msg: &Message) -> Message {
        match &msg.body.kind {
            BodyKind::Init => self.init(&msg.src, msg.body.payload.get_str("node_id")),
            BodyKind::Echo => {
                self.echo(&msg.src, msg.body.msg_id, msg.body.payload.get_str("echo"))
            }
            _ => panic!("receive ${:?}", msg),
        }
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }
}

fn main() -> Result<()> {
    let mut server = EchoServer::default();
    server.eventloop()
}
