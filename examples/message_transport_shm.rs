use serde::{Deserialize, Serialize};
use std::env;
use xrpc::{
    Message, MessageTransport, MessageTransportAdapter, SharedMemoryConfig, SharedMemoryTransport,
    message::types::{CompressionType, MessageType},
};

#[derive(Debug, Serialize, Deserialize)]
struct AddRequest {
    a: i32,
    b: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct AddResponse {
    result: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogEvent {
    level: String,
    message: String,
}

const SERVICE_NAME: &str = "test_message_tranport";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("server");

    match mode {
        "server" => run_server().await?,
        "client" => run_client().await?,
        _ => {
            eprintln!("Usage: cargo run --example message_transport_shm -- [server|client]");
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn run_server() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Server] Creating shared memory transport");

    let config = SharedMemoryConfig::default();
    let transport = SharedMemoryTransport::create_server(SERVICE_NAME, config)?;
    let msg_transport = MessageTransportAdapter::new(transport);

    println!("[Server] Waiting for messages");

    // Wait for messages
    loop {
        let message = msg_transport.recv().await?;

        match message.msg_type {
            MessageType::Call => {
                println!(
                    "[Server] Received Call: method={}, id={}",
                    message.method, message.id
                );

                match message.method.as_str() {
                    "add" => {
                        let req: AddRequest = message.deserialize_payload()?;
                        println!("[Server] AddRequest: {} + {}", req.a, req.b);

                        let resp = AddResponse {
                            result: req.a + req.b,
                        };
                        let reply = Message::reply(message.id, resp)?;
                        msg_transport.send(&reply).await?;
                        println!("[Server] Sent reply");
                    }
                    "divide" => {
                        let req: AddRequest = message.deserialize_payload()?;
                        if req.b == 0 {
                            let error = Message::error(message.id, "Division by zero");
                            msg_transport.send(&error).await?;
                            println!("[Server] Sent error: division by zero");
                        } else {
                            let resp = AddResponse {
                                result: req.a / req.b,
                            };
                            let reply = Message::reply(message.id, resp)?;
                            msg_transport.send(&reply).await?;
                        }
                    }
                    "shutdown" => {
                        println!("[Server] Shutdown requested");
                        let reply = Message::reply(message.id, "ok")?;
                        msg_transport.send(&reply).await?;
                        break;
                    }
                    _ => {
                        let error = Message::error(message.id, "Unknown method");
                        msg_transport.send(&error).await?;
                    }
                }
            }
            MessageType::Notification => {
                println!("[Server] Received Notification: method={}", message.method);
                if message.method == "log" {
                    let event: LogEvent = message.deserialize_payload()?;
                    println!("[Server] Log [{}]: {}", event.level, event.message);
                }
            }
            _ => {
                println!("[Server] unkown message type: {:?}", message.msg_type);
            }
        }
    }

    println!("[Server] Shutting down");
    Ok(())
}

async fn run_client() -> Result<(), Box<dyn std::error::Error>> {
    println!("[Client] Connecting to shared memory transport");

    let transport = SharedMemoryTransport::connect_client(SERVICE_NAME)?;
    let msg_transport = MessageTransportAdapter::new(transport);

    println!("[Client] Connected!");

    // Call: add (no compression)
    println!("\n[Client] Calling add (10 + 32)");
    let call = Message::call("add", AddRequest { a: 10, b: 32 })?;
    msg_transport.send(&call).await?;

    let reply = msg_transport.recv().await?;
    let resp: AddResponse = reply.deserialize_payload()?;
    println!("[Client] Result: {}", resp.result);

    // Call: add with LZ4 compression
    println!("\n[Client] Calling add with LZ4 compression (100 + 200)");
    let mut call = Message::call("add", AddRequest { a: 100, b: 200 })?;
    call.metadata.compression = CompressionType::Lz4;
    msg_transport.send(&call).await?;

    let reply = msg_transport.recv().await?;
    let resp: AddResponse = reply.deserialize_payload()?;
    println!(
        "[Client] Result: {} (compression: {:?})",
        resp.result, reply.metadata.compression
    );

    // Call: divide by zero (expect error)
    println!("\n[Client] Calling divide (10 / 0)");
    let call = Message::call("divide", AddRequest { a: 10, b: 0 })?;
    msg_transport.send(&call).await?;

    let reply = msg_transport.recv().await?;
    if reply.msg_type == MessageType::Error {
        let error_msg: String = reply.deserialize_payload()?;
        println!("[Client] Got error: {}", error_msg);
    }

    // Notification (fire-and-forget)
    println!("\n[Client] Sending notification log");
    let notification = Message::notification(
        "log",
        LogEvent {
            level: "INFO".to_string(),
            message: "Client started successfully".to_string(),
        },
    )?;
    msg_transport.send(&notification).await?;
    println!("[Client] Notification sent (no response expected)");

    // Shutdown
    println!("\n[Client] Sending shutdown...");
    let call = Message::call("shutdown", ())?;
    msg_transport.send(&call).await?;
    let _ = msg_transport.recv().await?;
    println!("[Client] Done!");

    Ok(())
}
