use std::env;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let bind_address = if args.len() == 1 {
        "127.0.0.1:0"
    } else if args.len() == 2 {
        &args[1]
    } else {
        eprintln!("Usage: {} <optional bind address>", args[0]);
        return;
    };

    let listener = TcpListener::bind(bind_address).await.unwrap();
    let addr = listener.local_addr().unwrap();

    println!("listening started on {}, ready to accept", addr);
    while let Ok((mut stream, _)) = listener.accept().await {
        tokio::task::spawn(async move {
            let mut buf = vec![0; 1024];
            loop {
                let n = stream
                    .read(&mut buf)
                    .await
                    .expect("failed to read data from stream");

                if n == 0 {
                    return;
                }

                stream
                    .write_all(&buf[0..n])
                    .await
                    .expect("failed to write data to stream");
            }
        });
    }
}
