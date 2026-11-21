# contributing

thanks for poking around the codebase bhai. grab some chai and read these quick rules before opening a PR.

## ground rules

- keep code fast, readable, and easy to test offline without spending real SOL.
- never commit wallet `keypair.json` files, private keys, or paid RPC API keys (seriously yaar, git history never forgets).
- stick to public endpoints (`http://127.0.0.1:8899`, `https://api.devnet.solana.com`, `https://api.mainnet-beta.solana.com`) and `Keypair::new()` in tests.
- keep serialized packets at or under `1232` bytes (`MAX_SOLANA_PACKET_BYTES`) or QUIC TPU validators will drop them on sight.
- do not touch the X.509 DER byte array in `src/tpu_quic.rs` unless you enjoy debugging TLS handshakes at 4am.

## local dev checks

- check all workspace targets and examples:

```bash
cargo check --workspace --all-targets
cargo check --workspace --examples
```

- run the full unit and integration test suite:

```bash
cargo test --workspace
```

- test the local A/B benchmark in dry fixture mode:

```bash
cargo run -- bench --local --samples 10
```

## opening a PR

- branch off `main` (`feat/your-thing` or `fix/your-bug`).
- add a unit test if you change fee math, slot-4 hold thresholds, or wire encoding.
- make sure `cargo test --workspace` is green before pushing.
