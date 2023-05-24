use std::io::{StdoutLock, Write};

use anyhow::{Context, Result};

use crate::message::{Body, BodyKind, Message, Payload};

pub trait Server {
    /// return node_id of server
    fn node_id(&self) -> &str;

    /// set node_id by init msg
    fn set_node_id(&mut self, node_id: &str);

    /// detail with init msg
    fn init(&mut self, dst: &String, node_id: &str) -> Message {
        self.set_node_id(node_id);

        let body = Body {
            kind: BodyKind::InitOk,
            reply_to: Some(1),
            msg_id: 1,
            payload: Payload::new(),
        };

        Message {
            src: self.node_id().to_string(),
            dst: dst.clone(),
            body,
        }
    }

    /// reply to each msg
    fn reply(&mut self, msg: &Message) -> Message;

    /// eventloop to process msg
    fn eventloop(&mut self) -> Result<()> {
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
