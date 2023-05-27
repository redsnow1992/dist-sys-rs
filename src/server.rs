use std::io::{StdoutLock, Write};

use anyhow::{Context, Result};

use crate::message::{Body, BodyKind, Message, Payload};

#[derive(Debug, Default)]
pub struct ServerInner {
    node_id: String,
    next_msg_id: usize,
}

impl ServerInner {
    fn advance(&mut self) {
        self.next_msg_id += 1;
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn next_msg_id(&self) -> usize {
        self.next_msg_id
    }

    pub fn init(&mut self, msg: &Message) -> Message {
        self.node_id = msg.body.payload.get_str("node_id").to_string();
        self.next_msg_id = 0;

        let body = Body {
            kind: BodyKind::InitOk,
            reply_to: Some(1),
            msg_id: 1,
            payload: Payload::new(),
        };

        Message {
            src: self.node_id.clone(),
            dst: msg.src.to_string(),
            body,
        }
    }
}

pub trait HasInner {
    fn into_inner(&mut self) -> &mut ServerInner;
}

pub trait Serve: HasInner {
    fn reply(&mut self, msg: &Message) -> Message;

    fn reply_inner(&mut self, msg: &Message) -> Message {
        self.into_inner().advance();
        self.reply(msg)
    }

    /// eventloop to process msg
    fn serve(&mut self) -> Result<()> {
        let stdin = std::io::stdin().lock();
        let mut stdout = std::io::stdout().lock();
        let input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for msg in input {
            let msg = msg.context("cannot deserialize from stdin")?;
            let reply_msg = self.reply(&msg);
            Self::write_to_stdout(&mut stdout, &reply_msg);
        }

        Ok(())
    }

    fn write_to_stdout(output: &mut StdoutLock, msg: &Message) {
        serde_json::to_writer(&mut *output, msg).unwrap();
        output.write_all(b"\n").unwrap();
    }
}
