use anyhow::Result;
use async_trait::async_trait;
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

#[async_trait]
impl Serve for EchoServer {
    async fn reply(&mut self, msg: &Message) -> Option<Message> {
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

#[tokio::main]
async fn main() -> Result<()> {
    let mut server = EchoServer::default();
    server.serve().await
}

#[cfg(test)]
mod tests {
    use dist_sys_rs::message::{Body, BodyKind, Message, Payload};
    use serde_json::json;

    use crate::EchoServer;

    #[test]
    fn test_echo() {
        let mut server = EchoServer::default();
        let msg = Message {
            src: "n1".to_string(),
            dst: "n2".to_string(),
            body: Body {
                kind: BodyKind::Echo,
                reply_to: None,
                payload: Payload::init("echo", json!("hhh")),
                msg_id: 100,
            },
        };
        let reply_msg = server.echo(&msg);
        assert_eq!(BodyKind::EchoOk, reply_msg.body.kind);
        assert_eq!(100, reply_msg.body.reply_to.unwrap());
        assert_eq!("hhh", reply_msg.body.payload.get_str("echo"));
    }
}
