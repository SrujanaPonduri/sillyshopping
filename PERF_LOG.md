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
| 33a0e2f | date cache in timestamp parser, buffer reuse in sessions step | 2.30 s |
| fc5f899 | counting-pass grouping by person + fused AVX2 line/field scanner | 1.82 s |
| 7432ab1 | first-field id compares, biggest-first output groups | 1.69 s |

## Sandbox experiments

| Change | Result | Label |
|---|---|---|
| stage 3+4 vs b97e7e0 (LTO, 20M, 2 cores, back-to-back) | ~6.0 s wall / 10 CPU-s vs ~8.1 s / 12 CPU-s | sandbox |
| link-table prefetch (batch of 32 lines) + staged scatter | parse phase 5.6 s -> 3.2 s wall (non-LTO, 20M) | sandbox |
| date cache + reused buffers (33a0e2f) | 2.746G -> 2.550G instructions (-7.1%), 1M dev set | sandbox, callgrind |

| per-partition grouping: counting pass by person + short per-person sorts instead of a comparison sort over the partition | 1M dev set 2.550G -> 2.463G instructions (-3.4%); 20M NCPU=1 user time median of 5: 8.85 s -> 8.49 s (-4%), non-LTO, noisy | sandbox |
| fused AVX2 line/comma/quote scanner (runtime-detected, old splitter as fallback); fuzz vs old path 0 mismatches in 400k buffers | 1M dev set 2.463G -> 2.271G instructions (-7.8%); 20M NCPU=1 user time median of 5 vs the version before the counting pass: 9.40 s -> 8.99 s (-4.4%, includes the counting-pass change), non-LTO, noisy | sandbox |
| id comparisons (event-id dedup, click-id ties) read only the first field of the line instead of splitting the whole line | 1M dev set 2.271G -> 2.192G instructions (-3.5%) | sandbox, callgrind |
| user-partition groups handed out biggest first from a shared queue (tail latency for whale users) | no sandbox signal on 2 cores; expected to matter only at 8 threads | estimate |
| tried and dropped: insertion sort for short per-person runs (+0.3% instr) | | sandbox, callgrind |
| scanner field count as a const generic (unrolled field loop) | 1M dev set 2.1887G -> 2.1312G instructions (-2.6%) | sandbox, callgrind |
| quote-free line fast path in the scanner (unchecked slices, no per-field quote handling) | -> 2.1195G (-0.6%) | sandbox, callgrind |
| inline 20-byte ISO timestamp path (fixed-size array, masked separator checks), general path out of line | -> 2.0833G (-1.7%) | sandbox, callgrind |
| numeric timestamps eight digits at a time (SWAR, overlapping tail load); unit test vs naive, 200k cases | -> 2.0562G (-1.3%) | sandbox, callgrind |
| the four parser changes together | 2.1887G -> 2.0562G instructions (-6.1%); 20M, 2 cores, wall median of 5-6: ~3.7 s -> ~3.6 s (-2%, noisy) | sandbox |
| gates for the parser changes: 36 fixtures, 60 random seeds vs reference, dev1/s5/s20 identical to the previous engine, files byte-identical at NCPU 1/3/8, SS_RANGE_BYTES=4096 identical, scanner fuzz 400k buffers 0 mismatches, new-vs-old parse_ts fuzz 6M strings 0 mismatches | all pass | sandbox |
| per-worker reusable buffers in the per-partition step (no fresh allocation per partition) | 20M, 2 cores, user+sys CPU time median of 5: 6.64 s -> 6.34 s (-4.5%, noisy); instructions +0.5% (1M dev set); page faults unchanged; outputs identical to the previous engine on dev1/s5/s20; 36 fixtures, 40 seeds pass | sandbox |
| tried and dropped: parse the ingest time from the event time when they share the first 17 bytes (exact, fuzzed 8M cases 0 mismatches) | 2.0562G -> 2.0695G instructions (+0.6%): the inline ISO path with the date cache is already cheap | sandbox, callgrind |
| tried and dropped: prefetch the first line of persons a few ahead before reading their names | no change in CPU time (median of 6: 7.25 s vs 7.32 s); skipping the name read entirely saved only ~2.7% (upper bound, noisy) | sandbox |
| tried and dropped: MADV_POPULATE_READ on each input range (no measurable change, 5 runs each); mallopt (mmap/trim thresholds, top pad) (no measurable change); SWAR numeric parse alone earlier (-0.7%) is now part of the bundle above | | sandbox |

## Where the time goes (sandbox, NCPU=1, 20M events, 9d04f4c, LTO)

links 0.5 CPU-s, parse into partitions 7.7, dedup 1.0, sessions + user_daily write 2.5; total 11.9 CPU-s.
Instruction profile (1M dev set): split_fields ~15%, parse_ts ~12%, rest flat.

## Short-ID hash, scratch image, and ideas measured and dropped (2026-11-22)

| change | measured |
| --- | --- |
| scorer: static binary in an empty `FROM scratch` image (b1d22b8) | 1.68 s (same code in a Debian image: 1.71, 1.72; glibc hugetlb on/off made no difference there) |
| `hash_id`: one folded 128-bit multiply on the first and last 8 bytes for IDs up to 16 bytes, ahash above that | dev-set instructions 2.0505G -> 2.0305G (-1.0%); s20 sandbox wall median 4.38 -> 4.26 s, CPU 8.38 -> 8.12 s (2 cores, 10 interleaved runs, noisy) |
| a first version mixed the length into the low bits and made `e1` and `e10` collide; the random differential check against the reference caught it (0/60 seeds), the fixtures did not. Fixed by mixing the length times an odd constant. | 0 collisions over 4.2M IDs of length 1-16 in a model test |
| tried and dropped: exact 64-bit packed ID keys (6-bit codes, bijective mix, hashed fallback) | correct, but +6% instructions (2.18G) and no clear time gain |
| tried and dropped: bulk-mask line scanner | -0.6% instructions |
| tried and dropped: half-hour-bin ordering instead of the per-person sort | +0.3% instructions (the sort is about 3% for events, the two counting passes cost as much) |
| profile note: callgrind's inclusive column counts recursive calls repeatedly (the sort looked like 7% and is about 3%) | |

## Smaller dedup tag (2026-11-22)

| change | measured |
| --- | --- |
| scorer, same session: hugetlb on a static scratch image (6803a20) 1.61 s; same code without it (fast-hash, a2bf877) 1.68 s | the same code in a Debian image scored 1.64 and 1.71/1.72 in different sessions, so session noise is about 0.08 s |
| dedup `Tag` 24 -> 16 bytes: the ingest time is no longer stored, it is read back from the line (`ingest_of`) for ids that occur more than once | dev-set instructions 2.0305G -> 2.0791G (+2.4%, the re-reads); s20 sandbox wall median 4.39 -> 4.31 s, CPU unchanged. Fixtures 36/36, 60/60 reference seeds, output identical to fast-hash on dev1/s5/s20/odd-ID set at 1, 3, 8 CPUs and with SS_RANGE_BYTES=4096. The saved memory traffic may help on the 8-core scorer; not measurable in the 2-core sandbox. |
