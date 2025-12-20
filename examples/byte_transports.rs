//! Example demonstrating Layer 1 (FrameTransport) and Layer 2 (Channel) usage.

use serde::{Deserialize, Serialize};
use xrpc::{
    ChannelConfig, ChannelFrameTransport, FrameTransport, SerdeChannel, TcpConfig,
    TcpFrameTransport, TcpFrameTransportListener, TypedChannel,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Request {
    id: u64,
    data: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Response {
    id: u64,
    result: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    channel_frame_transport_example().await?;
    tcp_frame_transport_example().await?;
    serde_channel_example().await?;
    typed_channel_example().await?;
    Ok(())
}

async fn channel_frame_transport_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- ChannelFrameTransport (in-process, Layer 1) ---");

    let config = ChannelConfig::default();
    let (t1, t2) = ChannelFrameTransport::create_pair("example", config)?;

    t1.send_frame(b"Hello from t1").await?;
    let received = t2.recv_frame().await?;
    println!("t2 received: {:?}", String::from_utf8_lossy(&received));

    t2.send_frame(b"Hello back from t2").await?;
    let received = t1.recv_frame().await?;
    println!("t1 received: {:?}", String::from_utf8_lossy(&received));

    let stats = t1.stats().unwrap();
    println!(
        "t1 stats: sent={}, received={}",
        stats.messages_sent, stats.messages_received
    );

    println!();
    Ok(())
}

async fn tcp_frame_transport_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TcpFrameTransport (network, Layer 1) ---");

    let config = TcpConfig::default();
    let listener = TcpFrameTransportListener::bind("127.0.0.1:0".parse()?, config.clone()).await?;
    let addr = listener.local_addr()?;
    println!("Server listening on {}", addr);

    let client_handle = tokio::spawn(async move {
        let client = TcpFrameTransport::connect(addr, TcpConfig::default())
            .await
            .unwrap();
        client.send_frame(b"Hello from TCP client").await.unwrap();

        let response = client.recv_frame().await.unwrap();
        println!(
            "[Client] Received: {:?}",
            String::from_utf8_lossy(&response)
        );
    });

    let server = listener.accept().await?;
    let received = server.recv_frame().await?;
    println!(
        "[Server] Received: {:?}",
        String::from_utf8_lossy(&received)
    );

    server.send_frame(b"Hello from TCP server").await?;

    client_handle.await?;
    println!();
    Ok(())
}

async fn serde_channel_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- SerdeChannel (Layer 2, flexible types) ---");

    let config = ChannelConfig::default();
    let (t1, t2) = ChannelFrameTransport::create_pair("serde", config)?;

    let ch1 = SerdeChannel::new(t1);
    let ch2 = SerdeChannel::new(t2);

    let request = Request {
        id: 1,
        data: "test data".to_string(),
    };
    ch1.send(&request).await?;

    let received: Request = ch2.recv().await?;
    println!("Received: {:?}", received);
    assert_eq!(received, request);

    let response = Response {
        id: 1,
        result: "success".to_string(),
    };
    ch2.send(&response).await?;

    let received: Response = ch1.recv().await?;
    println!("Received: {:?}", received);

    let stats = ch1.stats();
    println!(
        "Stats: sent={}, received={}",
        stats.messages_sent, stats.messages_received
    );

    println!();
    Ok(())
}

async fn typed_channel_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TypedChannel (Layer 2, fixed types) ---");

    let config = ChannelConfig::default();
    let (t1, t2) = ChannelFrameTransport::create_pair("typed", config)?;

    let client: TypedChannel<Request, Response, _> = TypedChannel::new(t1);
    let server: TypedChannel<Response, Request, _> = TypedChannel::new(t2);

    let request = Request {
        id: 42,
        data: "typed request".to_string(),
    };
    client.send(&request).await?;

    let received: Request = server.recv().await?;
    println!("Server received: {:?}", received);

    let response = Response {
        id: received.id,
        result: format!("processed: {}", received.data),
    };
    server.send(&response).await?;

    let received: Response = client.recv().await?;
    println!("Client received: {:?}", received);

    println!();
    Ok(())
}
