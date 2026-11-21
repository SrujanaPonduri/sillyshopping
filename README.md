# Shopping Sessions: DuckDB pipeline

A complete implementation of `rules.md` for the Shopping Sessions benchmark. It reads `/data`, writes `/output/user_daily/` and `/output/campaign_daily/` as Parquet, and runs with no arguments.

## Status

| Check | Status |
| --- | --- |
| All 36 edge-case fixtures (`fixtures/`), run locally with Python | **Passes, 36/36** |
| Randomized differential test against a naive reference (`tests/`), 8 seeds of 20k events plus 8 small seeds | **All match** |
| 2M-event benchmark data, pipeline output vs the naive reference | **Exact match**, both tables |
| 20M and 60M benchmark data, self-checks (sums, types) | **Pass** (no reference run at this size) |
| Dev set: `generate --seed 1 --events 1000000 --clicks 250000`, pipeline run locally, `scripts/compare.py` against `dev-answer/` | **MATCH.** 117,038 user rows and 27,400 campaign rows, no missing or extra rows. Run with local Python, not in Docker |
| Docker image build (`linux/amd64`) | **Builds**, and the image is `amd64` |
| 36 fixtures in the container (`make fixtures`, scorer flags) | **Passes, 36/36** |
| Dev set in the container (`make run-dev` then `make check-dev`, scorer flags) | **MATCH**, same counts as above. The Docker VM has 8 GB of memory, so the 24 GB limit is not enforced here |
| Hidden practice submission | **Not made** |

Before any submission, the one check still open is a 60M-event run in the container. That needs about 8 GB of memory, and the Docker VM here has only 8 GB, so raise Docker Desktop's memory first. The Dockerfile is unchanged from the starter; `requirements.txt` changed (see below).

**Polars version.** The pipeline was first pinned to `polars==2.1.0`. In the `linux/amd64` image under emulation on this Mac, that build segfaults on the first DataFrame operation, even though the CPU reports AVX2. The image now uses `polars-lts-cpu==1.33.1`, a build for older CPUs with the same API surface the pipeline uses. The fixtures, the random tests and the 2M benchmark were all rerun on 1.33.1 before the image was rebuilt. The scorer's Xeon CPUs have AVX-512 and may not need this change, but the pin works on both.

## Running

```sh
# Local, no Docker: all 36 fixtures plus randomized checks against the reference
make test-local

# Docker, as the scorer runs it (needs Docker with buildx)
make fixtures          # 36 fixtures in the container
make dev && make run-dev && make check-dev   # needs generator on PATH and dev-answer/ unzipped here
```

`pipeline/pipeline.py` reads `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU` and `MEM_GB`, with the same defaults as the starter. Logs go to stderr. Each run uses its own folder under `SCRATCH_DIR`, which is removed at exit.

## Architecture

1. **CSV parsing** (`parse_file`, `parse_batch`). Each file is read as raw bytes in 64 MiB blocks and split on `\n` only. DuckDB and Polars both strip a `\r` that precedes `\n`, and the rules keep it, so the parser does not use a native CSV reader. Each line is split on `,`. A field is valid when it has no quote, or when it is one quoted run with no quote inside. A line with the wrong number of fields, or any invalid field, is dropped. Files are parsed in parallel worker processes, each limited to one Polars thread. Parsed batches go to Parquet under `SCRATCH_DIR`.
2. **Identity** (`connected_components`). Linked IDs become integer nodes, and one SciPy connected-components pass gives each component. Each person is named by the byte-wise smallest of its IDs. An ID that is not linked keeps its own name.
3. **Events** (SQL). Dedup on `event_id` keeps the copy with the smallest `ingest_ts`. Validation runs on that kept copy only. Late events are dropped when `ingest_ts - ts > 24 h`.
4. **Sessions** (SQL). Distinct `(person, ts)` pairs are ordered. A new session starts when the gap to the previous distinct timestamp is over 1,800,000 ms. Every event takes the start of its session, and the session's day is the UTC date of that start.
5. **Clicks and acquisition credit** (SQL). Clicks are deduped on `click_id` and validated. Clicks that share a `(person, ts)` are reduced to the smallest `click_id`. For each session an ASOF join finds the latest click at or before the session start. The click credits the session when the gap is at most 7 days, and otherwise the session is `organic`.
6. **Assists** (SQL). For each `(person, campaign)`, click times are merged into intervals `[ts, ts + 7 days]` (closed at both ends). A session is assisted by a campaign exactly when its start falls in one of that campaign's intervals. The count per campaign and day comes from rank differences over each person's sorted session starts, using ASOF lookups. This avoids materializing one row per (session, campaign) pair, which would be very large for the whale users.
7. **Outputs**. `user_daily` aggregates sessions by person and day, and `top_category` breaks ties by the byte-wise smallest category. `campaign_daily` combines credited sessions with assisted counts and keeps rows that have at least one credited session or one assist. All numeric outputs are cast to `BIGINT`, and `day` is `DATE`.

## Performance (measured here, not on the scorer)

Machine: a Mac with 11 cores and 18 GB of RAM. Runs used `NCPU=8` and `MEM_GB=12` (DuckDB memory limit 9.6 GB). The data came from `tests/perf_gen.py`, a benchmark generator with realistic shape but **not** the challenge generator. It uses one user per 30 events (about 667k users at 20M events and 2M at 60M). Each figure is one run.

| Input | Wall time | Peak RSS |
| --- | --- | --- |
| 2M events | ~3 s | 1.3 GB (serial parse, before parallel parse) |
| 20M events | 17.3 s | 6.0 GB |
| 60M events | 61.4 s | 8.0 GB |

Stage times for the 60M run: parse 15 s, events and dedup 15 s, sessions 8 s, clicks and credit 3 s, assists 17 s, write 1.5 s.

The 60M run is above the 60-second practice target. The largest stage is assists. Ideas not yet tried: sorting sessions once and reusing that order for both ASOF joins, and splitting the parse stage for large files. Neither is implemented, and neither has been measured.

## Verification notes

- `tests/reference.py` is a deliberately naive implementation of `rules.md`, written separately from the pipeline and used only for testing.
- `tests/random_check.py` generates dirty random data with duplicates, malformed lines, all three timestamp forms, bad timestamps, `\r` line endings, linked IDs, and boundary clicks. It runs both implementations and compares the tables exactly.
- Random-test generator fixes: duplicate copies of a click are byte-identical, and duplicate events carry a later whole-second `ingest_ts`. The spec requires both properties. Earlier generator runs broke them, and the resulting mismatches were in the test data, not in the pipeline.
- `tests/perf_gen.py` writes benchmark inputs. Its output is not the official data and is not compared with `dev-answer/`.

## Files

- `pipeline/pipeline.py`: the pipeline.
- `tests/reference.py`: naive reference implementation for testing.
- `tests/random_check.py`: randomized differential test.
- `tests/perf_gen.py`: benchmark input generator, not the challenge generator.
- `fixtures/`: the 36 edge-case fixtures, unchanged from the starter.
- `scripts/`: fixture runner, scorer-style Docker runner, and dev comparison, unchanged from the starter.
- `Dockerfile`, `Makefile`, `run.sh`, `requirements.txt`: packaging. `requirements.txt` now pins `polars`, `numpy` and `scipy` as well as `duckdb`.
