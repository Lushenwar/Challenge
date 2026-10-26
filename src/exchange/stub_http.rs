//! Test helper: a one-shot HTTP server for discovery tests.

/// Serves `pages[i]` (status, body) to the i-th request and records each request line.
pub fn serve(pages: Vec<(u16, String)>) -> (String, std::sync::mpsc::Receiver<String>) {
    use std::io::{BufRead, BufReader, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for ((status, body), stream) in pages.into_iter().zip(listener.incoming()) {
            let mut stream = stream.unwrap();
            let mut line = String::new();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            reader.read_line(&mut line).unwrap();
            loop {
                let mut h = String::new();
                reader.read_line(&mut h).unwrap();
                if h == "\r\n" || h.is_empty() {
                    break;
                }
            }
            let _ = tx.send(line); // the test may not keep the receiver
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (url, rx)
}
