//! Hard rule 8 at startup: SIGINT / SIGTERM that arrive while initial discovery is still
//! running must end the process cleanly (exit 0), not kill it or be ignored.

use std::{
    io::Read,
    net::TcpListener,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

/// A REST endpoint that accepts connections and never answers: discovery hangs.
fn black_hole() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            held.push(stream); // keep it open, say nothing
        }
    });
    format!("http://{addr}")
}

fn key_file(dir: &std::path::Path) -> std::path::PathBuf {
    use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
    let pem = SigningKey::from_bytes(&[3; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let path = dir.join("key.pem");
    std::fs::write(&path, pem.as_bytes()).unwrap();
    path
}

fn signal_during_startup(sig: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
        .args(["--key-id", "test", "--rest-url", &black_hole(), "--output"])
        .arg(dir.path().join("out.sbe"))
        .arg("--key-path")
        .arg(key_file(dir.path()))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(800)); // well inside initial discovery
    let killed = Command::new("kill")
        .args([sig, &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let start = Instant::now();
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "collector ignored {sig} during startup"
        );
        std::thread::sleep(Duration::from_millis(50));
    };
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .unwrap()
        .read_to_string(&mut stderr)
        .unwrap();
    assert!(status.success(), "{sig}: {status:?}\n{stderr}");
    assert!(stderr.contains("signal during startup"), "{stderr}");
    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_end(&mut stdout)
        .unwrap();
    assert!(stdout.is_empty(), "nothing may reach stdout");
}

#[test]
fn sigint_during_startup_exits_cleanly() {
    signal_during_startup("-INT");
}

#[test]
fn sigterm_during_startup_exits_cleanly() {
    signal_during_startup("-TERM");
}
