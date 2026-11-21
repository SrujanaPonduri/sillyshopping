# Shopping Sessions

An implementation of `rules.md` for the Shopping Sessions benchmark. It reads `/data` and writes `/output/user_daily/` and `/output/campaign_daily/` as Parquet. The image runs with no arguments.

The production program is a native Rust binary in [rust/](rust/). The earlier DuckDB/Polars version in [pipeline/pipeline.py](pipeline/pipeline.py) is kept as the readable reference that the tests compare against.

## Status

| Check | Result |
| --- | --- |
| 36 fixtures, Rust binary, local | **36/36 pass** |
| 36 fixtures, Rust binary, in the `linux/amd64` container with scorer flags | **36/36 pass** |
| Dev set (`generate --seed 1 --events 1000000 --clicks 250000`) vs `dev-answer/`, Rust, local and in container | **MATCH** |
| 5M and 20M benchmark sets, Rust vs the Python reference's output | **identical** (both tables, full sorted rows and schema) |
| Randomized differential test vs `tests/reference.py`, 12 seeds, Rust | **all match** |
| Same test with tiny parse ranges (boundary handling), Rust | **all match** |
| Determinism: 20M set at 1, 4, and 8 threads, and with tiny ranges | **identical** every time |
| 60M benchmark set: self-checks (sums, assists rule) | **pass** |

The 60M benchmark data comes from `tests/perf_gen.py`. It has realistic shape but is **not** the challenge generator, so it isn't a substitute for the official dev-set check.

## Running

```sh
make build            # Rust image for linux/amd64 (the Dockerfile compiles the binary)
make fixtures         # 36 fixtures in the container, scorer flags
make dev && make run-dev && make check-dev    # needs the generator on PATH and dev-answer/ unzipped here

# Native, without Docker
cd rust && cargo build --release
DATA_DIR=/path/to/data OUTPUT_DIR=/path/to/out NCPU=8 ./rust/target/release/shopping-sessions
```

Environment: `DATA_DIR` (default `/data`), `OUTPUT_DIR` (default `/output`), `NCPU` (thread count, default 8). The Rust program uses only memory. It writes nothing except the two output folders and logs to stderr. `SS_RANGE_BYTES` is a test-only override of the parse range size.

## Architecture (Rust)

1. **Input.** Each CSV is memory-mapped and split into newline-aligned byte ranges of about 16 MB. Every line starts in exactly one range. Ranges are parsed in parallel, and the header is skipped only in the first range of each file.
2. **CSV dialect.** A line is split on commas and must have exactly the right number of fields. A field is valid if it has no quote, or is one quoted run with no quote inside. Quotes are removed, `""` is null, and `\r` stays as a byte of its field. Every value is a slice into the mapped file, so nothing is copied.
3. **Timestamps and prices.** The three timestamp forms are parsed by hand, with calendar checks (leap years, hour 24, second 60) and offset checks. Prices are digits only, from 1 to 10,000,000.
4. **Events.** Dedup on `event_id` is sharded by hash across 64 shards, and each shard keeps the copy with the smallest `ingest_ts`. Validation runs on the kept copy only.
5. **Identity.** Every user ID is interned into a sharded hash table, giving a dense integer ID. Union-find over the linked pairs (iterative, with path halving) gives components. Each person is named by the byte-wise smallest ID in its component, computed once per ID.
6. **Sessions.** Events are sorted by one packed key, `person << 32 | (ts - window start)`, and scanned once. A new session starts when the gap between distinct timestamps exceeds 1,800,000 ms.
7. **Attribution.** For each person, the credit rule is a merge walk over sorted sessions and clicks. Assists use merged click intervals `[ts, ts + 7 days]`, and each interval is matched to a session range by binary search, per day. The amount of work is proportional to the number of clicks, not to sessions times campaigns.
8. **Aggregation.** Work is split into chunks at person boundaries and run in parallel. The outputs are per-(person, day) rows and dense (campaign, day) arrays, which are summed across chunks.
9. **Output.** Both tables are written with the Arrow Parquet writer. `day` is `DATE32`, and all counts are `INT64`.

## Performance

Measured natively on this Mac (11 cores, 18 GB, currently swapping). The inputs are the benchmark sets from `tests/perf_gen.py`. Times are wall time for the whole process, including reading the input.

| Input | Python reference (Polars + DuckDB, natively) | Rust, final |
| --- | --- | --- |
| 20M events | 14.9 s | 2.2 s to 3.1 s (3 runs) |
| 60M events | 52 s | 22.7 s to 125.7 s across three runs; median 47 s |

The 60M spread comes from this Mac swapping: 4.5 GB of swap was in use, and the first run had to read the inputs back from disk. Peak RSS was 8.8 GB, against 24 GB on the scorer, which has no swap. These numbers are **not** the scorer's timing. Quit Docker Desktop before a native benchmark, or measure on the scorer's machine. The container's own speed on this Mac is not representative, since it runs amd64 under emulation.

Stage times at 60M (fastest run): parse 1.5 s for events, 0.6 s for clicks and links; events dedup and validation 8.3 s; intern 3.8 s; events sort 3.0 s; clicks sort 0.7 s; sessions, credit, and assists 3.9 s; write 0.5 s. Dedup and interning are the largest costs, and both can likely be trimmed.

## Files

- `rust/src/main.rs`, `rust/src/parse.rs`: the pipeline. `rust/Cargo.lock` pins the dependencies. Build with `--locked`.
- `Dockerfile`: builds the Rust binary in a Rust image, then copies it into a slim runtime image that runs as uid 1000.
- `pipeline/pipeline.py`: the Python reference, kept for tests.
- `tests/reference.py`: naive reference implementation.
- `tests/random_check.py`: randomized differential test. Use `--impl` to test a binary, for example `--impl ./rust/target/release/shopping-sessions`.
- `tests/perf_gen.py`: benchmark input generator (not the challenge generator).
- `fixtures/`, `scripts/`, `Makefile`, `requirements.txt`, `TEAM.md`: unchanged from the starter, except `requirements.txt`, which the Python reference still uses.
