# Requirement → evidence

Every requirement of the [challenge](https://voluntas.io/engineering-challenge), with the
tests that check it (`cargo test --all-targets`; `--ignored` for the scale test) and the
live runs that showed it on the real exchange. Live-run summaries are committed under
[`evidence/`](evidence/) and [`latency/`](latency/); the README's *Evidence* table lists
the rest. "e2e" = [`tests/e2e_fake_kalshi.rs`](../tests/e2e_fake_kalshi.rs): the real
binary against a local fake Kalshi that knows the true book, with every capture checked
against it.

## Base task

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 1 | Subscribe to all live regular markets, excluding MVEs | `keeps_live_binary_markets_and_drops_mve` (discovery.rs); e2e `steady_state`, `scale_thousands_of_markets_many_sockets` | 1 h on all 162,896 live non-MVE markets (README); 6 h soak, 175,165 markets seen ([soak](evidence/soak-6h-2026-10-25.json)) |
| 2 | An order book per symbol | `snapshot_then_deltas`, `never_negative`, `negative_result_is_an_error_and_leaves_book_unchanged` (book.rs); `replays_committed_fixtures` (32 recorded snapshots rebuilt exactly from earlier recorded deltas); e2e: every snapshot and update equals the true book | soak: 52.9M periodic snapshots matched the rebuilt books, 0 unexplained |
| 3 | Pick up new symbols, drop ones that stop trading | e2e `markets_added_and_removed`; `removal_writes_one_terminal_snapshot_across_replicas`; Polymarket `keyset_discovery_*`, `live_markets_runs_the_complete_listing_in_the_background` | soak: 175,165 added, 24,307 removed by lifecycle, 1,320 by discovery; Polymarket: 26 removed after two complete listings, 24/24 checked confirmed closed ([evidence](evidence/polymarket-removals-2026-10-25.json)) |
| 4 | Complete and time-accurate | `gap_invalidates_and_requests_snapshot`, `pending_until_subscription_snapshot_then_streams` (conn_kalshi.rs); `dropped_delta_fails_verify` (verify catches a missing delta); `output_tracks_the_true_book_through_any_interleaving` (arbiter property test); e2e `seq_gap_resyncs_with_get_snapshot` | soak: 736M deltas, 0 seq gaps; chaos: 152 injected frame drops = 152 seq gaps detected, all resynced ([chaos](evidence/chaos-2h-2026-10-25.json)) |

## Output format

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 5 | Deltas to stdout | CI: `--help` writes nothing to stdout, clippy denies `print_stdout`; `sigint_during_startup_exits_cleanly` (empty stdout) | every capture above was `stdout > file` |
| 6 | A full snapshot every minute | `verify` fails any live symbol whose snapshots are > 61 s apart; `clean_capture_verifies` | soak: max interval 60.05 s, 0 violations over 6 h |
| 7 | The repo's helpers for all output | CI: `src/util`, `sbe_schemas`, `src/side.rs` byte-identical to tag `starter`; `starter_roundtrip`; `byte_identical_to_the_starter_writer` (our writer vs `AlignedSbeWriter`, proptest); every capture read back with the unmodified `AlignedSbeReader` | — |

## Timing

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 8 | Accurate receive times | receive time taken before decoding (`conn.rs`); `failover_to_a_quiet_backup_never_goes_back_in_time`; property test: output time never goes backwards | soak: 5,154 sub-millisecond regressions in 411M messages (minute-boundary race, README *Ordering and timestamps*); latency percentiles per run in [`latency/collector-*.json`](latency/) |

## Websockets

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 9 | Spread symbols over many sockets | `replicas_on_distinct_sockets_within_capacity`, `backups_are_mixed`, `invariants_hold_under_random_adds_and_removes` (planner); e2e scale: 5,000 markets on ≥ 20 sockets | soak: 63 sockets; 20,000 markets in one subscribe accepted (README *Kalshi wire facts*) |
| 10 | API key | `ed25519_signs_timestamp_method_path`, `rsa_pss_signs_from_pkcs8_and_pkcs1`, `rejects_garbage` (auth.rs) | every Kalshi run used the signed handshake |

## Shutdown

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 11 | On SIGINT close the websockets and the file | `sigint_during_startup_exits_cleanly`, `sigterm_during_startup_exits_cleanly`; every e2e scenario ends with SIGINT, exit 0, a complete report and a readable capture | soak and chaos: exit 0 |

## Extension 1: redundant websockets

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 12 | Redundant sockets against silent disconnects, quiet subscriptions, delays, crashes | `disconnect_fails_over_with_a_snapshot_and_no_gap`, `no_valid_replica_means_a_gap_until_any_source_returns`, `replica_failover_and_double_fault_verify`; e2e `one_socket_lost_backup_takes_over_without_a_gap`, `silent_subscription_fails_over_to_the_replica`, `slow_socket_fails_over_to_the_fresher_replica`, `both_sockets_lost_is_a_reported_gap` (deliberate failure: verify flags it) | socket killed mid-run on all markets: 4,543 failovers, 0 gaps (README); chaos: drop/delay/silence faults, verify exit 0 |
| 13 | Snapshot on every source switch | `forwards_only_the_active_source`, `disconnect_fails_over_with_a_snapshot_and_no_gap`, `a_consistently_faster_replica_becomes_active`; property test | soak: 3.4M failovers; verify pairs every non-periodic snapshot with its logged cause (0 unexplained) |

## Extension 2: subscription monitoring

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 14 | Per-symbol update rate | `slow_baseline_needs_time_for_five_messages`, `quiet_markets_and_new_subscriptions_are_exempt` (health.rs) | — |
| 15 | Resubscribe when the rate collapses | `rate_collapsed_market_is_resubscribed`, `rate_collapse_with_a_valid_replica_spares_the_active_source`, `rate_rule_skips_minutes_a_source_was_not_valid_throughout`, `collapse_after_baseline_resubscribes_then_cools_down`, `resubscribe_deletes_resubscribes_and_waits_for_a_fresh_snapshot`; lag rule: `silenced_active_source_fails_over_within_2s_of_replica_moving_ahead`, `socket_wide_lag_fails_over_without_per_market_resubscribes` | soak: 1.77M rate resubscribes, 0.77M lag resubscribes |
| 16 | Periodic full reconnects | `recycle_is_scheduled_then_spaced`, `recycle_waits_until_every_market_has_another_valid_replica` | soak: 470 recycles |

## Extension 3: generic encode/decode

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 17 | Only encode/decode changes per exchange | CI: no venue name outside `src/exchange/`; Polymarket adapter tests (`book_is_a_snapshot_without_zero_levels`, `price_change_is_an_absolute_size_per_asset`, `subscribe_then_operation_commands`) | 30 min live Polymarket, verify exit 0 (README); manual CI job `live-polymarket` |

## Extension 4: latency

| # | Requirement | Tests | Live evidence |
|---|---|---|---|
| 18 | Experiments: racing, source port, … | `racing_opens_several_and_keeps_one`, `sets_nodelay_and_pins_the_source_port`, `a_taken_source_port_falls_back_to_any_port` (net.rs) | E1–E5 with `record` (E4 racing: no gain on average; E5 source ports: path partly port-dependent, rejected with data), C1–C3 through the collector ([`latency.md`](latency.md), raw results in [`latency/`](latency/)) |
| 19 | Findings in a markdown file | — | [`latency.md`](latency.md) |
