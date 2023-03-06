# Queue Benches

Done for the [windmill](https://github.com/windmill-labs/windmill) project, (c) Windmill Labs, 2022, MIT License

Includes simple async implementations for local, postgres & redis of queues. Simply run using `cargo criterion` (first `cargo install cargo-criterion`) or `cargo bench`. See [criterion](https://github.com/bheisler/criterion.rs) for more information on how this works.

To run redis benchmarks, a local redis instance is required on port 6389 with no password. Namespace `rsmq` in DB 0 is used.

To run postgres benchmarks, a postgres DB has to be available at `postgresql://postgres:changeme@localhost:5432/test-db`
