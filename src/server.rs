use anyhow::Result;
use async_trait::async_trait;
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, Stdout};

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

#[async_trait]
pub trait Serve: HasInner {
    async fn send(&mut self) -> Option<Vec<Message>> {
        None
    }

    async fn reply(&mut self, msg: &Message) -> Option<Message>;

    async fn reply_inner(&mut self, msg: &Message) -> Option<Message> {
        self.as_inner().advance();
        self.reply(msg).await
    }

    /// eventloop to process msg
    async fn serve(&mut self) -> Result<()> {
        let stdin = tokio::io::stdin();
        let mut stdout = tokio::io::stdout();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            let msg: Message = serde_json::from_str(&line).unwrap();
            if let Some(reply_msg) = self.reply(&msg).await {
                Self::write_to_stdout(&mut stdout, &reply_msg).await;
            }

            if let Some(to_send) = self.send().await {
                for to_send_msg in to_send.iter() {
                    self.as_inner().advance();
                    Self::write_to_stdout(&mut stdout, to_send_msg).await;
                }
            }
        }

        Ok(())
    }

    async fn write_to_stdout(output: &mut Stdout, msg: &Message) {
        let serialized = serde_json::to_string(&msg).unwrap();
        output.write_all(serialized.as_bytes()).await.unwrap();
        output.write_all(b"\n").await.unwrap();

        // serde_json::to_writer(&mut *output, msg).unwrap();
        // output.write_all(b"\n").unwrap();
    }
}
