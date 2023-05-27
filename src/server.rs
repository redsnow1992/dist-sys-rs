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

    pub fn init(&mut self, msg: &Message) -> Option<Message> {
        self.node_id = msg.body.payload.get_str("node_id").to_string();
        self.next_msg_id = 0;

        let body = Body {
            kind: BodyKind::InitOk,
            reply_to: Some(1),
            msg_id: 1,
            payload: Payload::default(),
        };

        Some(Message {
            src: self.node_id.clone(),
            dst: msg.src.to_string(),
            body,
        })
    }
}

pub trait HasInner {
    fn as_inner(&mut self) -> &mut ServerInner;
}

pub trait Serve: HasInner {
    fn send(&mut self) -> Option<Vec<Message>> {
        None
    }

    fn reply(&mut self, msg: &Message) -> Option<Message>;

    fn reply_inner(&mut self, msg: &Message) -> Option<Message> {
        self.as_inner().advance();
        self.reply(msg)
    }

    /// eventloop to process msg
    fn serve(&mut self) -> Result<()> {
        let stdin = std::io::stdin().lock();
        let mut stdout = std::io::stdout().lock();
        let input = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();
        for msg in input {
            let msg = msg.context("cannot deserialize from stdin")?;
            if let Some(reply_msg) = self.reply(&msg) {
                Self::write_to_stdout(&mut stdout, &reply_msg);
            }

            if let Some(to_send) = self.send() {
                for to_send_msg in to_send.iter() {
                    self.as_inner().advance();
                    Self::write_to_stdout(&mut stdout, to_send_msg);
                }
            }
        }

        Ok(())
    }

    fn write_to_stdout(output: &mut StdoutLock, msg: &Message) {
        serde_json::to_writer(&mut *output, msg).unwrap();
        output.write_all(b"\n").unwrap();
    }
}
