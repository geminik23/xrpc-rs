use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use xrpc::{
    MessageTransportAdapter, RpcClient, RpcServer, SharedMemoryConfig, SharedMemoryTransport,
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

    server.register_typed("add", |req: AddRequest| async move {
        println!("[Server] add({}, {})", req.a, req.b);
        Ok(AddResponse {
            result: req.a + req.b,
        })
    });

    server.register_typed("echo", |req: EchoRequest| async move {
        println!("[Server] echo(\"{}\")", req.message);
        let len = req.message.len();
        Ok(EchoResponse {
            message: req.message,
            length: len,
        })
    });

    println!("[Server] Registered {} handlers", server.handler_count());
    println!("[Server] Waiting for requests\n");

    server.serve(msg_transport).await?;

    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Client] Connecting to RPC server");

    let transport = SharedMemoryTransport::connect_client(SERVICE_NAME)?;
    let msg_transport = MessageTransportAdapter::new(transport);

    let client = RpcClient::new(msg_transport);
    let _handle = client.start();

    println!("[Client] Connected!\n");

    println!("[Client] Calling add(10, 32)");
    let resp: AddResponse = client.call("add", &AddRequest { a: 10, b: 32 }).await?;
    println!("[Client] Result: {}\n", resp.result);

    println!("[Client] Calling add(100, 200)");
    let resp: AddResponse = client.call("add", &AddRequest { a: 100, b: 200 }).await?;
    println!("[Client] Result: {}\n", resp.result);

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

    println!("[Client] Calling unknown method");
    let result: Result<(), _> = client.call("unknown", &()).await;
    match result {
        Ok(_) => println!("[Client] Unexpected success"),
        Err(e) => println!("[Client] Got expected error: {}\n", e),
    }

    client.close().await?;
    println!("[Client] Done!");

    Ok(())
}
