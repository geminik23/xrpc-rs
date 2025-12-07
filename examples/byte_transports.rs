use serde::{Deserialize, Serialize};
use xrpc::{
    ChannelConfig, ChannelTransport, RawTransport, TcpConfig, TcpTransport, TcpTransportListener,
    Transport, TypedChannel,
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
    channel_transport_example().await?;
    tcp_transport_example().await?;
    raw_transport_example().await?;
    typed_channel_example().await?;
    Ok(())
}

async fn channel_transport_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- ChannelTransport (in-process) ---");

    let config = ChannelConfig::default();
    let (t1, t2) = ChannelTransport::create_pair("example", config)?;

    t1.send(b"Hello from t1").await?;
    let received = t2.recv().await?;
    println!("t2 received: {:?}", String::from_utf8_lossy(&received));

    t2.send(b"Hello back from t2").await?;
    let received = t1.recv().await?;
    println!("t1 received: {:?}", String::from_utf8_lossy(&received));

    let stats = t1.stats().unwrap();
    println!(
        "t1 stats: sent={}, received={}",
        stats.messages_sent, stats.messages_received
    );

    println!();
    Ok(())
}

async fn tcp_transport_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TcpTransport (network) ---");

    let config = TcpConfig::default();
    let listener = TcpTransportListener::bind("127.0.0.1:0".parse()?, config.clone()).await?;
    let addr = listener.local_addr()?;
    println!("Server listening on {}", addr);

    let client_handle = tokio::spawn(async move {
        let client = TcpTransport::connect(addr, TcpConfig::default())
            .await
            .unwrap();
        client.send(b"Hello from TCP client").await.unwrap();

        let response = client.recv().await.unwrap();
        println!(
            "[Client] Received: {:?}",
            String::from_utf8_lossy(&response)
        );
    });

    let server = listener.accept().await?;
    let received = server.recv().await?;
    println!(
        "[Server] Received: {:?}",
        String::from_utf8_lossy(&received)
    );

    server.send(b"Hello from TCP server").await?;

    client_handle.await?;
    println!();
    Ok(())
}

async fn raw_transport_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- RawTransport (direct serialization) ---");

    let config = ChannelConfig::default();
    let (t1, t2) = ChannelTransport::create_pair("raw", config)?;

    let raw1 = RawTransport::new(t1);
    let raw2 = RawTransport::new(t2);

    let request = Request {
        id: 1,
        data: "test data".to_string(),
    };
    raw1.send_direct(&request).await?;

    let received: Request = raw2.recv_direct().await?;
    println!("Received: {:?}", received);
    assert_eq!(received, request);

    let response = Response {
        id: 1,
        result: "success".to_string(),
    };
    raw2.send_direct(&response).await?;

    let received: Response = raw1.recv_direct().await?;
    println!("Received: {:?}", received);

    let stats = raw1.stats();
    println!(
        "Stats: sent={}, received={}",
        stats.messages_sent, stats.messages_received
    );

    println!();
    Ok(())
}

async fn typed_channel_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- TypedChannel (type-safe wrapper) ---");

    let config = ChannelConfig::default();
    let (t1, t2) = ChannelTransport::create_pair("typed", config)?;

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
