use anyhow::Result;
use std::collections::HashMap;

use dist_sys_rs::{
    message::{Body, BodyKind, Message, Payload},
    server::{HasInner, Serve, ServerInner},
};
use serde_json::json;

#[derive(Debug, Default)]
pub struct BroadcastServer {
    inner: ServerInner,
    pub messages: Vec<usize>,
    /**
     * latest idx of messages not sent to node, start from 0.
     * each node has an idx
     */
    sent_idx_map: HashMap<String, usize>,
    pub topology: HashMap<String, Vec<String>>,
}

impl BroadcastServer {
    /**
     * This message requests that a value be broadcast out to all nodes in the cluster.
     * The value is always an integer and it is unique for each message from Maelstrom.
     */
    pub fn broadcast(&mut self, msg: &Message) -> Message {
        self.messages.push(msg.body.payload.get_usize("message"));

        let body = Body {
            kind: BodyKind::BroadcastOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::default(),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.clone(),
            body,
        }
    }

    /**
     * broadcast back to other nodes in this cluster
     */
    pub fn broadcast_back(&mut self) -> Option<Vec<Message>> {
        let src = self.inner.node_id();

        self.topology.get(src).map(|others| {
            let mut ret = Vec::with_capacity(5 * others.len()); // TODO
            for node_id in others.iter() {
                let sent_idx = *self.sent_idx_map.get(node_id).unwrap_or(&0);
                let msg_len = self.messages.len();
                let msg_range = sent_idx..msg_len;

                for msg_id in msg_range {
                    let body = Body {
                        kind: BodyKind::Broadcast,
                        msg_id: self.inner.next_msg_id(),
                        reply_to: None,
                        payload: Payload::init("message", json!(msg_id)),
                    };
                    let message = Message {
                        src: src.to_string(),
                        dst: node_id.to_string(),
                        body,
                    };
                    ret.push(message);
                }
                // update idx to next
                self.sent_idx_map.insert(node_id.to_string(), msg_len);
            }
            ret
        })
    }

    /**
     * This message requests that a node return all values that it has seen.
     */
    pub fn read(&mut self, msg: &Message) -> Message {
        let body = Body {
            kind: BodyKind::ReadOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::init("messages", json!(self.messages)),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }

    /**
    * This message informs the node of who its neighboring nodes are.
    * example:
    *  "topology": {
    *      "n1": ["n2", "n3"],
    *      "n2": ["n1"],
    *      "n3": ["n1"]
       }
    */
    pub fn topology(&mut self, msg: &Message) -> Message {
        self.topology = HashMap::new();
        let topology_value = msg.body.payload.get_raw("topology");
        for (k, v) in topology_value.as_object().unwrap().iter() {
            let dsts: Vec<String> = v
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item.as_str().unwrap().to_string())
                .collect();
            self.topology.insert(k.to_string(), dsts);
        }

        let body = Body {
            kind: BodyKind::TopologyOk,
            msg_id: self.inner.next_msg_id(),
            reply_to: Some(msg.body.msg_id),
            payload: Payload::default(),
        };

        Message {
            src: self.inner.node_id().to_string(),
            dst: msg.src.to_string(),
            body,
        }
    }
}

impl Serve for BroadcastServer {
    fn reply(&mut self, msg: &Message) -> Option<Message> {
        match &msg.body.kind {
            BodyKind::Init => self.inner.init(msg),
            BodyKind::Broadcast => Some(self.broadcast(msg)),
            BodyKind::Read => Some(self.read(msg)),
            BodyKind::Topology => Some(self.topology(msg)),
            // receive broadcast_ok msg from other nodes which reply to self's broadcast msg
            BodyKind::BroadcastOk => None,
            _ => panic!("receive ${:?}", msg),
        }
    }

    fn send(&mut self) -> Option<Vec<Message>> {
        self.broadcast_back()
    }
}

impl HasInner for BroadcastServer {
    fn as_inner(&mut self) -> &mut ServerInner {
        &mut self.inner
    }
}

fn main() -> Result<()> {
    let mut server = BroadcastServer::default();
    server.serve()
}

#[cfg(test)]
mod tests {
    use dist_sys_rs::message::{Body, BodyKind, Message, Payload};
    use serde_json::json;

    use crate::BroadcastServer;

    #[test]
    fn test_broadcast() {
        let mut server = BroadcastServer::default();
        let msg = Message {
            src: "n1".to_string(),
            dst: "n2".to_string(),
            body: Body {
                kind: BodyKind::Broadcast,
                reply_to: None,
                payload: Payload::init("message", json!(10)),
                msg_id: 100,
            },
        };
        let reply_msg = server.broadcast(&msg);
        assert_eq!(BodyKind::BroadcastOk, reply_msg.body.kind);
        assert_eq!(100, reply_msg.body.reply_to.unwrap());
    }
}
