use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use xrpc::{
    Message, MessageTransport, MessageTransportAdapter, RpcClient, RpcServer, SharedMemoryConfig,
    SharedMemoryTransport,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddRequest {
    a: i32,
    b: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AddResponse {
    result: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoRequest {
    message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EchoResponse {
    message: String,
    length: usize,
}

const SERVICE_NAME: &str = "rpc_example_service";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("server");

    match mode {
        "server" => run_server().await?,
        "client" => run_client().await?,
        _ => {
            eprintln!("Usage: cargo run --example rpc_client_server -- [server|client]");
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Server] Starting RPC server");

    let config = SharedMemoryConfig::default();
    let transport = SharedMemoryTransport::create_server(SERVICE_NAME, config)?;
    let msg_transport = Arc::new(MessageTransportAdapter::new(transport));

    let server = RpcServer::new();

    // Register typed handler for "add" method
    server.register_typed("add", |req: AddRequest| async move {
        println!("[Server] add({}, {})", req.a, req.b);
        Ok(AddResponse {
            result: req.a + req.b,
        })
    });

    // Register typed handler for "echo" method
    server.register_typed("echo", |req: EchoRequest| async move {
        println!("[Server] echo(\"{}\")", req.message);
        let len = req.message.len();
        Ok(EchoResponse {
            message: req.message,
            length: len,
        })
    });

    // Register function handler for "shutdown"
    server.register_fn("shutdown", |msg: Message| async move {
        println!("[Server] Shutdown requested");
        Message::reply(msg.id, "bye")
    });

    println!("[Server] Registered {} handlers", server.handler_count());
    println!("[Server] Waiting for requests\n");

    // Run server loop
    loop {
        let message = msg_transport.recv().await?;
        println!("[Server] Received: method={}", message.method);

        if message.method == "shutdown" {
            if let Some(response) = server.handle_message(message).await {
                msg_transport.send(&response).await?;
            }
            break;
        }

        if let Some(response) = server.handle_message(message).await {
            msg_transport.send(&response).await?;
        }
    }

    println!("[Server] Shutting down");
    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Client] Connecting to RPC server");

    let transport = SharedMemoryTransport::connect_client(SERVICE_NAME)?;
    let msg_transport = MessageTransportAdapter::new(transport);

    let client = RpcClient::new(msg_transport);
    let _handle = client.start();

    println!("[Client] Connected!\n");

    // Call "add" method
    println!("[Client] Calling add(10, 32)");
    let resp: AddResponse = client.call("add", &AddRequest { a: 10, b: 32 }).await?;
    println!("[Client] Result: {}\n", resp.result);

    // Call "add" again
    println!("[Client] Calling add(100, 200)");
    let resp: AddResponse = client.call("add", &AddRequest { a: 100, b: 200 }).await?;
    println!("[Client] Result: {}\n", resp.result);

    // Call "echo" method
    println!("[Client] Calling echo(\"Hello, RPC!\")");
    let resp: EchoResponse = client
        .call(
            "echo",
            &EchoRequest {
                message: "Hello, RPC!".to_string(),
            },
        )
        .await?;
    println!(
        "[Client] Result: message=\"{}\", length={}\n",
        resp.message, resp.length
    );

    // Call unknown method (expect error)
    println!("[Client] Calling unknown method");
    let result: Result<(), _> = client.call("unknown", &()).await;
    match result {
        Ok(_) => println!("[Client] Unexpected success"),
        Err(e) => println!("[Client] Got expected error: {}\n", e),
    }

    // Send notification (fire-and-forget)
    println!("[Client] Sending notification");
    client.notify("log", &"Client is done").await?;

    // Shutdown server
    println!("[Client] Sending shutdown");
    let resp: String = client.call("shutdown", &()).await?;
    println!("[Client] Server response: {}", resp);

    client.close().await?;
    println!("[Client] Done!");

    Ok(())
}
