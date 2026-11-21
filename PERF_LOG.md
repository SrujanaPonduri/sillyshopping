# Performance log

One line per experiment. Labels: **scorer** = score the team saw on the practice run (user-reported, 20M events,
noisy up to ~20% per draw); **sandbox** = measured by the assistant on a 2-core x86 sandbox (wall times noisy
about +-0.5 s, so instruction counts from callgrind on the 1M dev set are used for A/B); **estimate** = not measured.
Scorer numbers are not used to pick commits; same-session A/B is.

## Scorer history (practice, ~20M events)

| Commit | Change | Scorer |
|---|---|---|
| (earlier) | first native Rust pipeline | 49.91 s |
| (earlier) | faster interning, dedup, splitter | 7 s, then 5 s, then 3 s |
| f4c19d3 | glibc huge pages via GLIBC_TUNABLES | 2.81 s / 2.83 s |
| b97e7e0 | parallel identity + merge | 2.70 s |
| 9d04f4c | stage 3+4: compact records, per-partition pipeline, no interning | 2.52 s |
| 33a0e2f | date cache in timestamp parser, buffer reuse in sessions step | not yet run (merged to main) |

## Sandbox experiments

| Change | Result | Label |
|---|---|---|
| stage 3+4 vs b97e7e0 (LTO, 20M, 2 cores, back-to-back) | ~6.0 s wall / 10 CPU-s vs ~8.1 s / 12 CPU-s | sandbox |
| link-table prefetch (batch of 32 lines) + staged scatter | parse phase 5.6 s -> 3.2 s wall (non-LTO, 20M) | sandbox |
| date cache + reused buffers (33a0e2f) | 2.746G -> 2.550G instructions (-7.1%), 1M dev set | sandbox, callgrind |

| per-partition grouping: counting pass by person + short per-person sorts instead of a comparison sort over the partition | 1M dev set 2.550G -> 2.463G instructions (-3.4%); 20M NCPU=1 user time median of 5: 8.85 s -> 8.49 s (-4%), non-LTO, noisy | sandbox |

## Where the time goes (sandbox, NCPU=1, 20M events, 9d04f4c, LTO)

links 0.5 CPU-s, parse into partitions 7.7, dedup 1.0, sessions + user_daily write 2.5; total 11.9 CPU-s.
Instruction profile (1M dev set): split_fields ~15%, parse_ts ~12%, rest flat.
