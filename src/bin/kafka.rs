use anyhow::Result;
use dist_sys_rs::{kafka::KafkaServer, server::Serve};

#[tokio::main]
async fn main() -> Result<()> {
    let mut server = KafkaServer::default();
    server.serve().await
}
