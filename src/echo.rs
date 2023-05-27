use anyhow::Result;
use core::panic;
use dist_sys_rs::{
    message::{Body, BodyKind, Message, Payload},
    server::{HasInner, Serve, ServerInner},
};
use serde_json::json;

#[derive(Debug, Default)]
pub struct EchoServer {
    inner: ServerInner,
}

impl EchoServer {
    fn echo(&mut self, msg: &Message) -> Message {
        let body = Body {
            kind: BodyKind::EchoOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::init("echo", json!(msg.body.payload.get_str("echo"))),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }
}

impl Serve for EchoServer {
    fn reply(&mut self, msg: &Message) -> Option<Message> {
        match msg.body.kind {
            BodyKind::Init => self.inner.init(msg),
            BodyKind::Echo => Some(self.echo(msg)),
            _ => panic!("{}", format!("cannot handle msg: {:?}", msg)),
        }
    }
}

impl HasInner for EchoServer {
    fn as_inner(&mut self) -> &mut ServerInner {
        &mut self.inner
    }
}

fn main() -> Result<()> {
    let mut server = EchoServer::default();
    server.serve()
}
