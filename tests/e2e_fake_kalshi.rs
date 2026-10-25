//! End to end: the real collector binary against a local fake Kalshi that knows the true
//! book of every market. Each capture is checked against that ground truth:
//! - a market's first message is a snapshot;
//! - every snapshot equals the true book at its exchange timestamp;
//! - every update is a real level change (same side, price, absolute quantity), written
//!   once, and leaves the rebuilt book equal to the true book at that moment;
//! - after events stop, each live market's last state is its final true book.
//!
//! Scenarios: steady state, one socket lost, both lost (a deliberate gap), markets added
//! and removed, a silent subscription, a seq gap, a slow socket, and (ignored) scale.

mod fake_kalshi;

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use engineering_challenge::util::{
    AlignedSbeReader, OrderbookSchema,
    dtos::challenge_orderbook::{AnyMessage, PriceLevelUpdateDomain, SnapshotDomain},
};
use fake_kalshi::{Event, Fake, Fault};
use serde_json::Value;

type Book = BTreeMap<(u8, i64), i64>;

struct Run {
    _dir: tempfile::TempDir,
    cap: PathBuf,
    log: PathBuf,
    report: Value,
    history: Vec<Event>,
    fake_stats: Value,
}

fn key_file(dir: &Path) -> PathBuf {
    use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
    let pem = SigningKey::from_bytes(&[9; 32])
        .to_pkcs8_pem(Default::default())
        .unwrap();
    let path = dir.join("key.pem");
    std::fs::write(&path, pem.as_bytes()).unwrap();
    path
}

fn start(fake: &Fake, dir: &Path, markets_per_socket: usize) -> Child {
    let log = std::fs::File::create(dir.join("collector.log")).unwrap();
    Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
        .args([
            "--key-id",
            "e2e",
            "--rest-url",
            &fake.rest_url,
            "--ws-url",
            &fake.ws_url,
        ])
        .args(["--discovery-secs", "1", "--replicas", "2"])
        .args(["--markets-per-socket", &markets_per_socket.to_string()])
        .arg("--key-path")
        .arg(key_file(dir))
        .arg("--output")
        .arg(dir.join("cap.sbe"))
        .arg("--report")
        .arg(dir.join("report.json"))
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap()
}

/// Run the collector against `fake`, apply `faults` at their offsets, stop events,
/// let the last frames drain, then SIGINT and collect everything.
async fn run(
    fake: Fake,
    markets_per_socket: usize,
    total: Duration,
    faults: Vec<(Duration, Fault)>,
) -> Run {
    let dir = tempfile::tempdir().unwrap();
    let mut child = start(&fake, dir.path(), markets_per_socket);
    let t0 = tokio::time::Instant::now();
    for (at, f) in faults {
        tokio::time::sleep_until(t0 + at).await;
        fake.fault(f);
    }
    tokio::time::sleep_until(t0 + total).await;
    fake.fault(Fault::Pause(true));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let killed = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(killed.success());
    let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
    let log = dir.path().join("collector.log");
    assert!(
        status.success(),
        "collector exit {status:?}\n{}",
        tail(&log)
    );
    let report =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("report.json")).unwrap())
            .unwrap();
    Run {
        cap: dir.path().join("cap.sbe"),
        log,
        report,
        history: fake.history().await,
        fake_stats: fake.stats().await,
        _dir: dir,
    }
}

fn tail(p: &Path) -> String {
    let s = std::fs::read_to_string(p).unwrap_or_default();
    s.lines()
        .rev()
        .take(30)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join("\n")
}

/// The true book of `market` once every event up to `ts_ms` happened.
fn truth(events: &[Event], ts_ms: i64) -> Book {
    let mut b = Book::new();
    for e in events.iter().take_while(|e| e.ts_ms <= ts_ms) {
        let k = (e.side as u8, e.price);
        if e.qty == 0 {
            b.remove(&k);
        } else {
            b.insert(k, e.qty);
        }
    }
    b
}

#[derive(Debug, Default)]
struct Checked {
    snapshots: u64,
    updates: u64,
    markets: usize,
    /// Markets whose last message is an empty (terminal) snapshot.
    ended: Vec<String>,
    errors: Vec<String>,
}

/// Compare the capture with the fake's history (see the module doc).
fn check(run: &Run) -> Checked {
    let mut by_market: HashMap<&str, Vec<Event>> = HashMap::new();
    for e in &run.history {
        by_market
            .entry(e.market.as_str())
            .or_default()
            .push(e.clone());
    }
    struct St {
        book: Option<Book>,
        last_ts: i64,
        ended: bool,
    }
    let mut st: BTreeMap<String, St> = BTreeMap::new();
    let mut c = Checked::default();
    let err = |c: &mut Checked, s: String| {
        if c.errors.len() < 20 {
            c.errors.push(s);
        }
    };
    let mut r = AlignedSbeReader::<OrderbookSchema>::open(&run.cap).unwrap();
    while let Some(msg) = r.read_msg().unwrap() {
        match msg {
            AnyMessage::Snapshot(s) => {
                let s = SnapshotDomain::try_from_decoder(s).unwrap();
                c.snapshots += 1;
                let ts = s.exchange_timestamp.timestamp_micros() / 1000;
                let mut book = Book::new();
                book.extend(s.bids.iter().map(|l| ((0, l.price_level), l.quantity)));
                book.extend(s.asks.iter().map(|l| ((1, l.price_level), l.quantity)));
                let events = by_market
                    .get(s.symbol.as_str())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                let e = st.entry(s.symbol.clone()).or_insert(St {
                    book: None,
                    last_ts: 0,
                    ended: false,
                });
                e.ended = book.is_empty() && !truth(events, ts).is_empty();
                if !e.ended && book != truth(events, ts) {
                    err(
                        &mut c,
                        format!("{}: snapshot at {ts} differs from the true book", s.symbol),
                    );
                }
                e.last_ts = ts;
                e.book = Some(book);
            }
            AnyMessage::PriceLevelUpdate(u) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(u).unwrap();
                c.updates += 1;
                let ts = u.exchange_timestamp.timestamp_micros() / 1000;
                let events = by_market
                    .get(u.symbol.as_str())
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                let Some(e) = st.get_mut(&u.symbol).filter(|e| e.book.is_some()) else {
                    err(
                        &mut c,
                        format!("{}: update before its first snapshot", u.symbol),
                    );
                    continue;
                };
                let real = events.iter().any(|e| {
                    e.ts_ms == ts
                        && e.side == u.side
                        && e.price == u.price_level
                        && e.qty == u.quantity
                });
                if !real {
                    err(
                        &mut c,
                        format!("{}: update at {ts} is not a real level change", u.symbol),
                    );
                }
                if ts <= e.last_ts {
                    err(
                        &mut c,
                        format!("{}: update at {ts} written twice or out of order", u.symbol),
                    );
                }
                let book = e.book.as_mut().unwrap();
                let k = (u.side as u8, u.price_level);
                if u.quantity == 0 {
                    book.remove(&k);
                } else {
                    book.insert(k, u.quantity);
                }
                if *book != truth(events, ts) {
                    err(
                        &mut c,
                        format!(
                            "{}: book after update at {ts} differs from the true book",
                            u.symbol
                        ),
                    );
                }
                e.last_ts = ts;
            }
            _ => panic!("unknown message"),
        }
    }
    c.markets = st.len();
    for (m, e) in &st {
        if e.ended {
            c.ended.push(m.clone());
            continue;
        }
        let events = by_market.get(m.as_str()).map(Vec::as_slice).unwrap_or(&[]);
        if e.book.as_ref() != Some(&truth(events, i64::MAX)) {
            err(
                &mut c,
                format!("{m}: final state differs from the final true book"),
            );
        }
    }
    c
}

fn verify(run: &Run, events: bool) -> (i32, Value) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_verify"));
    cmd.arg(&run.cap);
    if events {
        cmd.arg("--events").arg(&run.log);
    }
    let out = cmd.output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        serde_json::from_slice(&out.stdout).unwrap_or(Value::Null),
    )
}

fn assert_true_to_the_exchange(run: &Run, markets: usize) -> Checked {
    let c = check(run);
    assert!(c.errors.is_empty(), "{:#?}\n{}", c.errors, tail(&run.log));
    assert!(
        c.markets >= markets,
        "only {} of {markets} markets in the capture",
        c.markets
    );
    assert!(c.updates > 0 && c.snapshots >= markets as u64, "{c:?}");
    let (code, summary) = verify(run, true);
    assert_eq!(code, 0, "verify: {summary}");
    c
}

fn arbiter(run: &Run, field: &str) -> u64 {
    run.report["arbiter"][field].as_u64().unwrap()
}

const S: fn(u64) -> Duration = Duration::from_secs;

#[tokio::test(flavor = "multi_thread")]
async fn steady_state() {
    let run = run(Fake::start(20, 300, 1).await, 5000, S(8), vec![]).await;
    let c = assert_true_to_the_exchange(&run, 20);
    assert!(c.updates > 1000, "{c:?}");
    assert_eq!(arbiter(&run, "gaps"), 0);
    // The checker must catch a capture that disagrees with the exchange: alter one
    // mid-run event of the true history and check the same capture again.
    let mut wrong = Run {
        history: run.history.clone(),
        report: Value::Null,
        fake_stats: Value::Null,
        ..run
    };
    let i = wrong.history.len() / 2;
    wrong.history[i].qty += 1;
    assert!(
        !check(&wrong).errors.is_empty(),
        "an altered event must be detected"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn one_socket_lost_backup_takes_over_without_a_gap() {
    let run = run(
        Fake::start(20, 300, 2).await,
        5000,
        S(10),
        vec![(S(4), Fault::Drop(0))],
    )
    .await;
    assert_true_to_the_exchange(&run, 20);
    assert!(arbiter(&run, "failovers") >= 1, "{}", run.report);
    assert_eq!(arbiter(&run, "gaps"), 0, "the replica covered every market");
    assert!(
        run.fake_stats["accepted"].as_u64().unwrap() >= 3,
        "the lost socket reconnected"
    );
}

/// Deliberate failure: both replicas down at once. The data written is still true, but
/// the collector reports gaps and `verify` without the event log flags the jump.
#[tokio::test(flavor = "multi_thread")]
async fn both_sockets_lost_is_a_reported_gap() {
    let faults = vec![(S(4), Fault::Refuse(S(3))), (S(4), Fault::DropAll)];
    let run = run(Fake::start(20, 300, 3).await, 5000, S(14), faults).await;
    let c = check(&run);
    assert!(c.errors.is_empty(), "{:#?}", c.errors);
    assert!(arbiter(&run, "gaps") >= 1, "{}", run.report);
    assert!(
        std::fs::read_to_string(&run.log)
            .unwrap()
            .contains("gap market=")
    );
    let (code, summary) = verify(&run, false);
    assert_ne!(
        code, 0,
        "verify must flag the books that jumped across the gap: {summary}"
    );
    assert!(summary["unexplained_mismatches"].as_u64().unwrap() >= 1);
    assert_eq!(
        verify(&run, true).0,
        0,
        "with the event log every jump is explained"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn markets_added_and_removed() {
    let fake = Fake::start(20, 300, 4).await;
    let gone = fake.markets[0].clone();
    let faults = vec![
        (S(3), Fault::Add("FAKE-NEW".into())),
        (S(3), Fault::Remove(gone.clone())),
    ];
    let run = run(fake, 5000, S(9), faults).await;
    let c = assert_true_to_the_exchange(&run, 21);
    assert!(
        c.ended.contains(&gone),
        "removed market ends with an empty snapshot: {c:?}"
    );
    assert!(run.history.iter().any(|e| e.market == "FAKE-NEW"));
    assert!(
        run.report["control"]["added"].as_u64().unwrap() >= 1,
        "{}",
        run.report
    );
    assert!(
        run.report["control"]["removed_lifecycle"].as_u64().unwrap() >= 1,
        "{}",
        run.report
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn silent_subscription_fails_over_to_the_replica() {
    let fake = Fake::start(20, 300, 5).await;
    let quiet = fake.markets[1].clone();
    let run = run(fake, 5000, S(10), vec![(S(3), Fault::Silence(0, quiet))]).await;
    assert_true_to_the_exchange(&run, 20);
    assert!(arbiter(&run, "failovers_stale") >= 1, "{}", run.report);
}

#[tokio::test(flavor = "multi_thread")]
async fn seq_gap_resyncs_with_get_snapshot() {
    let run = run(
        Fake::start(20, 300, 6).await,
        5000,
        S(8),
        vec![(S(3), Fault::SkipSeq(0))],
    )
    .await;
    assert_true_to_the_exchange(&run, 20);
    let gaps: u64 = run.report["connections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["seq_gaps"].as_u64().unwrap())
        .sum();
    assert!(gaps >= 1, "{}", run.report);
    assert!(
        run.fake_stats["commands"]["update_subscription:get_snapshot"]
            .as_u64()
            .unwrap_or(0)
            >= 1,
        "{}",
        run.fake_stats
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn slow_socket_fails_over_to_the_fresher_replica() {
    let run = run(
        Fake::start(20, 300, 7).await,
        5000,
        S(10),
        vec![(S(3), Fault::Delay(0, S(3)))],
    )
    .await;
    assert_true_to_the_exchange(&run, 20);
    assert!(arbiter(&run, "failovers_stale") >= 1, "{}", run.report);
}

/// Thousands of markets over many sockets, with one socket lost mid-run.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "scale: ~1 min; run with --ignored"]
async fn scale_thousands_of_markets_many_sockets() {
    let run = run(
        Fake::start(5000, 3000, 8).await,
        500,
        S(40),
        vec![(S(20), Fault::Drop(3))],
    )
    .await;
    assert_true_to_the_exchange(&run, 5000);
    assert!(
        run.report["control"]["sockets"].as_u64().unwrap() >= 20,
        "{}",
        run.report
    );
    assert_eq!(arbiter(&run, "gaps"), 0);
}
