#!/usr/bin/env python3
"""
Shopping Sessions pipeline: Python/Polars CSV dialect parsing, SciPy identity
components, DuckDB for everything relational.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)

Stages
  1. Parse every events/clicks/links CSV line by line from raw bytes with the
     dialect of rules.md (\r is data; quotes are syntax; field counts are exact).
     Bytes go through Python rather than a native CSV reader, because DuckDB and
     Polars both strip a \r that precedes \n, and rules.md keeps it.
  2. Identity: one SciPy connected-components pass over the linked IDs. Each
     person is named by the byte-wise smallest of its IDs.
  3. DuckDB SQL for events (dedup, validation, late drop, sessions), clicks
     (dedup, validation, credit by ASOF join) and assists (merged 7-day click
     intervals, counted with rank differences rather than per-pair rows).
"""
import datetime as dt
import glob
import json
import multiprocessing
import os
import shutil
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor

import duckdb
import numpy as np
import polars as pl
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

os.environ.setdefault("POLARS_MAX_THREADS", "1")  # set before polars starts threads; parse workers are processes
DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
GAP_MS = 1_800_000
LATE_MS = DAY_MS
MAX_PRICE = 10_000_000
CHUNK_BYTES = 1 << 26  # 64 MiB of raw input per parse batch

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


def sql_quote(path):
    return path.replace("'", "''")


# ---------------------------------------------------------------- stage 1: CSV
EVENT_COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
CLICK_COLS = ["click_id", "user_id", "campaign_id", "ts"]
LINK_COLS = ["user_id_a", "user_id_b"]
KEYS = {"events": "event_id", "clicks": "click_id", "links": None}
COLS = {"events": EVENT_COLS, "clicks": CLICK_COLS, "links": LINK_COLS}


def iter_line_batches(path):
    """Yield lists of raw lines (bytes, no newline), header excluded, in file order.

    Only b'\\n' ends a line. Lines are split from raw bytes, so every \\r stays
    in the field it belongs to.
    """
    skipped = False
    rest = b""
    with open(path, "rb") as f:
        while True:
            block = f.read(CHUNK_BYTES)
            if not block:
                break
            data = rest + block
            cut = data.rfind(b"\n")
            if cut < 0:
                rest = data
                continue
            rest = data[cut + 1:]
            lines = data[:cut].split(b"\n")
            if not skipped:
                lines = lines[1:]
                skipped = True
            if lines:
                yield lines
    if rest and skipped:  # final line without a trailing newline
        yield [rest]


def parse_batch(lines, names):
    """Apply the dialect to one batch. Returns a DataFrame with one string column per name.

    A field is valid when it has no quote, or is one quoted run with no quote inside.
    A quoted field's value is its inside; an empty value is null. A line is kept when
    it has exactly len(names) valid fields.
    """
    ncols = len(names)
    series = pl.Series("line", lines, dtype=pl.Binary).cast(pl.String)
    fields = (
        pl.DataFrame({"line": series})
        .select(pl.col("line").str.split(",").alias("f"))
        .with_columns(
            pl.col("f").list.eval(pl.element().str.replace(r'^"([^"]*)"$', "${1}")).alias("c")
        )
        .filter(
            (pl.col("c").list.len() == ncols)
            & pl.col("c").list.eval(~pl.element().str.contains('"', literal=True)).list.all()
        )
    )
    exprs = []
    for i, name in enumerate(names):
        value = pl.col("c").list.get(i)
        exprs.append(pl.when(value.str.len_bytes() > 0).then(value).otherwise(None).alias(name))
    return fields.select(exprs)


def parse_file(task):
    """Parse one CSV file into Parquet batches. Runs in a worker process."""
    kind, path, file_no, out_dir = task
    cols = COLS[kind]
    rows = 0
    batches = 0
    for batch_no, lines in enumerate(iter_line_batches(path)):
        df = parse_batch(lines, cols)
        if kind == "events":
            df = df.filter(pl.col("event_id").is_not_null())
        elif kind == "clicks":
            df = df.filter(pl.col("click_id").is_not_null())
        else:
            df = df.filter(pl.col("user_id_a").is_not_null() & pl.col("user_id_b").is_not_null())
        if df.height == 0:
            continue
        df.write_parquet(os.path.join(out_dir, f"f{file_no:05d}-b{batch_no:05d}.parquet"))
        rows += df.height
        batches += 1
    return rows, batches


def parse_kind(kind, out_dir, pool):
    """Parse all CSVs of one kind into Parquet batches under out_dir, files in parallel."""
    os.makedirs(out_dir, exist_ok=True)
    files = sorted(glob.glob(os.path.join(DATA, kind, "*.csv")))
    tasks = [(kind, path, i, out_dir) for i, path in enumerate(files)]
    results = list(pool.map(parse_file, tasks)) if tasks else []
    rows = sum(r for r, _ in results)
    batches = sum(b for _, b in results)
    if batches == 0:
        pl.DataFrame(schema={c: pl.String for c in COLS[kind]}).write_parquet(
            os.path.join(out_dir, "empty.parquet")
        )
    log(f"parsed {kind}: {len(files)} files, {rows} valid lines, {batches} batches")


# ---------------------------------------------------------------- SQL helpers
def install_macros(con):
    con.execute(
        r"""
CREATE OR REPLACE MACRO parse_ts(s) AS (
  CASE
    WHEN regexp_full_match(s, '[0-9]+') THEN
      CASE WHEN ltrim(s, '0') = '' THEN 0 ELSE TRY_CAST(ltrim(s, '0') AS BIGINT) END
    WHEN regexp_full_match(s, '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})') THEN
      CASE
        WHEN try_strptime(left(s, 19), '%Y-%m-%dT%H:%M:%S') IS NULL THEN NULL
        WHEN length(s) = 20 THEN epoch_ms(try_strptime(left(s, 19), '%Y-%m-%dT%H:%M:%S'))
        WHEN TRY_CAST(substr(s, 21, 2) AS INTEGER) > 23 THEN NULL
        WHEN TRY_CAST(substr(s, 24, 2) AS INTEGER) > 59 THEN NULL
        ELSE epoch_ms(try_strptime(left(s, 19), '%Y-%m-%dT%H:%M:%S'))
             - (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
               * (TRY_CAST(substr(s, 21, 2) AS BIGINT) * 60 + TRY_CAST(substr(s, 24, 2) AS BIGINT)) * 60000
      END
    ELSE NULL
  END)
"""
    )
    con.execute(
        f"""
CREATE OR REPLACE MACRO valid_price(p) AS COALESCE(
  regexp_full_match(p, '[0-9]+') AND ltrim(p, '0') <> '' AND length(ltrim(p, '0')) <= 8
  AND TRY_CAST(ltrim(p, '0') AS BIGINT) <= {MAX_PRICE}, false)
"""
    )


def parse_manifest():
    with open(os.path.join(DATA, "manifest.json")) as f:
        manifest = json.load(f)

    def ms(text):
        when = dt.datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
        return int(when.timestamp() * 1000)

    return ms(manifest["window_start"]), ms(manifest["window_end"])


# ---------------------------------------------------------------- main
def main():
    # A private run folder under SCRATCH, so nothing from an earlier run is read.
    os.makedirs(SCRATCH, exist_ok=True)
    run_dir = tempfile.mkdtemp(prefix="run-", dir=SCRATCH)
    try:
        run(run_dir)
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def run(run_dir):
    ws, we = parse_manifest()
    log(f"window {ws}..{we} ms")

    scratch_parsed = os.path.join(run_dir, "parsed")
    # Worker processes parse files in parallel; polars is limited to one thread each.
    ctx = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(max_workers=NCPU, mp_context=ctx) as pool:
        for kind in ("events", "clicks", "links"):
            parse_kind(kind, os.path.join(scratch_parsed, kind), pool)

    con = duckdb.connect()
    con.execute(f"SET threads = {NCPU}")
    con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
    con.execute(f"SET temp_directory = '{sql_quote(os.path.join(run_dir, 'duckdb_tmp'))}'")
    con.execute("SET preserve_insertion_order = false")
    install_macros(con)

    ev_glob = sql_quote(os.path.join(scratch_parsed, "events", "*.parquet"))
    ck_glob = sql_quote(os.path.join(scratch_parsed, "clicks", "*.parquet"))
    lk_glob = sql_quote(os.path.join(scratch_parsed, "links", "*.parquet"))
    products_path = sql_quote(os.path.join(DATA, "products.csv"))
    campaigns_path = sql_quote(os.path.join(DATA, "campaigns.csv"))

    # ---- identity: links -> components -> idmap(id, name)
    con.execute(f"CREATE TABLE lk AS SELECT user_id_a AS a, user_id_b AS b FROM read_parquet('{lk_glob}')")
    con.execute(
        "CREATE TABLE nodes AS SELECT id, CAST(row_number() OVER () - 1 AS BIGINT) AS i FROM "
        "(SELECT a AS id FROM lk UNION SELECT b FROM lk)"
    )
    n_nodes = con.sql("SELECT count(*) FROM nodes").fetchone()[0]
    if n_nodes:
        edges = con.sql(
            "SELECT na.i AS i, nb.i AS j FROM lk JOIN nodes na ON na.id = lk.a JOIN nodes nb ON nb.id = lk.b"
        ).fetchnumpy()
        ei = np.asarray(edges["i"], dtype=np.int64)
        ej = np.asarray(edges["j"], dtype=np.int64)
        graph = coo_matrix((np.ones(len(ei), dtype=np.int8), (ei, ej)), shape=(n_nodes, n_nodes)).tocsr()
        _, labels = connected_components(graph, directed=False)
        lab_path = os.path.join(run_dir, "labels.parquet")
        pl.DataFrame({"i": np.arange(n_nodes, dtype=np.int64), "lab": labels.astype(np.int64)}).write_parquet(lab_path)
        con.execute(f"CREATE TABLE labs AS SELECT * FROM read_parquet('{sql_quote(lab_path)}')")
        con.execute(
            "CREATE TABLE idmap AS "
            "WITH comp AS (SELECT l.lab, min(n.id) AS name FROM labs l JOIN nodes n USING (i) GROUP BY l.lab) "
            "SELECT n.id AS id, c.name AS name FROM nodes n JOIN labs l USING (i) JOIN comp c USING (lab)"
        )
    else:
        con.execute("CREATE TABLE idmap (id VARCHAR, name VARCHAR)")
    log(f"identity: {n_nodes} linked IDs")

    # ---- reference tables
    con.execute(
        f"CREATE TABLE products AS SELECT product_id, category FROM read_csv('{products_path}', header=true, all_varchar=true)"
    )
    con.execute(
        f"CREATE TABLE campaigns AS SELECT campaign_id, channel FROM read_csv('{campaigns_path}', header=true, all_varchar=true)"
    )

    # ---- events
    con.execute(
        f"""
CREATE TABLE ev0 AS
SELECT event_id, user_id, parse_ts(ts) AS ts_ms, parse_ts(ingest_ts) AS ing_ms,
       event_type, product_id, price_cents
FROM read_parquet('{ev_glob}')
"""
    )
    con.execute(
        """
CREATE TABLE ev1 AS
SELECT * FROM ev0 QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ing_ms ASC NULLS LAST) = 1
"""
    )
    con.execute("DROP TABLE ev0")
    con.execute(
        f"""
CREATE TABLE ev2 AS
SELECT COALESCE(m.name, e.user_id) AS person, e.ts_ms, e.event_type, e.product_id,
       CASE WHEN e.event_type = 'purchase' THEN TRY_CAST(ltrim(e.price_cents, '0') AS BIGINT) END AS price
FROM ev1 e LEFT JOIN idmap m ON m.id = e.user_id
WHERE e.user_id IS NOT NULL
  AND e.ts_ms IS NOT NULL AND e.ts_ms >= {ws} AND e.ts_ms < {we}
  AND e.ing_ms IS NOT NULL AND e.ing_ms - e.ts_ms <= {LATE_MS}
  AND e.event_type IN ('view', 'cart', 'purchase')
  AND e.product_id IS NOT NULL
  AND (e.event_type <> 'purchase' OR valid_price(e.price_cents))
"""
    )
    con.execute("DROP TABLE ev1")
    log("events parsed, deduped, validated")

    # ---- sessions: distinct (person, ts) -> session start
    con.execute(
        f"""
CREATE TABLE tsmap AS
WITH d AS (SELECT DISTINCT person, ts_ms FROM ev2),
     l AS (SELECT person, ts_ms, lag(ts_ms) OVER (PARTITION BY person ORDER BY ts_ms) AS prev FROM d),
     s AS (SELECT person, ts_ms,
                  CASE WHEN prev IS NULL OR ts_ms - prev > {GAP_MS} THEN ts_ms END AS newstart
           FROM l)
SELECT person, ts_ms,
       max(newstart) OVER (PARTITION BY person ORDER BY ts_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sess_start
FROM s
"""
    )
    con.execute(
        """
CREATE TABLE ev3 AS
SELECT e.person, t.sess_start, e.event_type, e.product_id, e.price
FROM ev2 e JOIN tsmap t ON t.person = e.person AND t.ts_ms = e.ts_ms
"""
    )
    con.execute("DROP TABLE ev2")
    con.execute("DROP TABLE tsmap")
    con.execute(
        """
CREATE TABLE sess AS
SELECT person, sess_start,
       count(*) AS events,
       count_if(event_type = 'purchase') AS purchases,
       coalesce(sum(price), 0) AS revenue
FROM ev3 GROUP BY person, sess_start
"""
    )
    con.execute(
        """
CREATE TABLE pcat AS
SELECT e.person, e.sess_start, COALESCE(p.category, 'unknown') AS cat, e.price
FROM ev3 e LEFT JOIN products p ON p.product_id = e.product_id
WHERE e.event_type = 'purchase'
"""
    )
    con.execute("DROP TABLE ev3")
    log("sessions built")

    # ---- clicks
    con.execute(
        f"""
CREATE TABLE clk0 AS
SELECT DISTINCT ON (click_id) click_id, user_id, campaign_id, parse_ts(ts) AS ts_ms
FROM read_parquet('{ck_glob}')
ORDER BY click_id
"""
    )
    con.execute(
        f"""
CREATE TABLE clk1 AS
SELECT COALESCE(m.name, c.user_id) AS person, c.campaign_id, c.ts_ms, c.click_id
FROM clk0 c
JOIN campaigns cp ON cp.campaign_id = c.campaign_id
LEFT JOIN idmap m ON m.id = c.user_id
WHERE c.user_id IS NOT NULL
  AND c.ts_ms IS NOT NULL AND c.ts_ms >= {ws - WEEK_MS} AND c.ts_ms < {we}
"""
    )
    con.execute("DROP TABLE clk0")
    log("clicks validated")

    # Credit: latest click per (person, ts); equal ts -> smallest click_id.
    con.execute(
        """
CREATE TABLE cagg AS
SELECT person, ts_ms, min(click_id) AS cid, arg_min(campaign_id, click_id) AS camp
FROM clk1 GROUP BY person, ts_ms
"""
    )
    con.execute(
        f"""
CREATE TABLE sess_c AS
SELECT s.person, s.sess_start, s.events, s.purchases, s.revenue,
       CASE WHEN c.ts_ms IS NOT NULL AND s.sess_start - c.ts_ms <= {WEEK_MS} THEN c.camp ELSE 'organic' END AS camp
FROM sess s
ASOF LEFT JOIN cagg c ON s.person = c.person AND s.sess_start >= c.ts_ms
"""
    )
    con.execute("DROP TABLE cagg")
    log("sessions credited")

    # ---- assists: per (person, campaign) merged intervals [ts, ts + 7d]
    con.execute(
        """
CREATE TABLE cd AS SELECT DISTINCT person, campaign_id AS camp, ts_ms FROM clk1
"""
    )
    con.execute(
        f"""
CREATE TABLE isl AS
WITH l AS (
  SELECT person, camp, ts_ms, lag(ts_ms) OVER (PARTITION BY person, camp ORDER BY ts_ms) AS prev FROM cd
),
g AS (
  SELECT person, camp, ts_ms,
         sum(CASE WHEN prev IS NULL OR ts_ms - prev > {WEEK_MS} THEN 1 ELSE 0 END)
           OVER (PARTITION BY person, camp ORDER BY ts_ms ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
  FROM l
)
SELECT person, camp, min(ts_ms) AS lo, max(ts_ms) + {WEEK_MS} AS hi
FROM g GROUP BY person, camp, grp
"""
    )
    con.execute("DROP TABLE cd")
    day_lo = ws // DAY_MS
    day_hi = (we - 1) // DAY_MS
    con.execute(
        f"""
CREATE TABLE slc AS
SELECT i.person, i.camp, d.di,
       greatest(i.lo, d.di * {DAY_MS}) AS a,
       least(i.hi, d.di * {DAY_MS} + {DAY_MS - 1}) AS b
FROM isl i
CROSS JOIN LATERAL (
  SELECT unnest(generate_series(greatest(i.lo // {DAY_MS}, {day_lo}), least(i.hi // {DAY_MS}, {day_hi}))) AS di
) d
"""
    )
    con.execute("DROP TABLE isl")
    con.execute(
        """
CREATE TABLE sr AS
SELECT person, sess_start, CAST(row_number() OVER (PARTITION BY person ORDER BY sess_start) AS BIGINT) AS rk
FROM sess
"""
    )
    # Sessions of a person in [a, b] = rank(<= b) - rank(<= a - 1), ranks from ASOF lookups.
    con.execute(
        """
CREATE TABLE slc_b AS
SELECT x.person, x.camp, x.di, x.a, x.b, x.a - 1 AS am1, coalesce(r.rk, 0) AS rb
FROM slc x ASOF LEFT JOIN sr r ON x.person = r.person AND x.b >= r.sess_start
"""
    )
    con.execute("DROP TABLE slc")
    con.execute(
        """
CREATE TABLE slc_c AS
SELECT x.camp, x.di, x.rb - coalesce(r.rk, 0) AS cnt
FROM slc_b x ASOF LEFT JOIN sr r ON x.person = r.person AND x.am1 >= r.sess_start
"""
    )
    con.execute("DROP TABLE slc_b")
    con.execute("DROP TABLE sr")
    log("assists counted")

    # ---- outputs
    os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
    con.execute(
        f"""
COPY (
  WITH base AS (
    SELECT person, sess_start // {DAY_MS} AS dd, count(*) AS sessions,
           sum(events) AS events, sum(revenue) AS revenue
    FROM sess GROUP BY person, sess_start // {DAY_MS}
  ),
  cat AS (
    SELECT person, sess_start // {DAY_MS} AS dd, cat, sum(price) AS amt
    FROM pcat GROUP BY person, sess_start // {DAY_MS}, cat
  ),
  top AS (
    SELECT person, dd, cat FROM cat
    QUALIFY row_number() OVER (PARTITION BY person, dd ORDER BY amt DESC, cat ASC) = 1
  )
  SELECT b.person AS user_id,
         CAST(epoch_ms(b.dd * {DAY_MS}) AS DATE) AS day,
         CAST(b.sessions AS BIGINT) AS sessions,
         CAST(b.events AS BIGINT) AS events,
         CAST(coalesce(b.revenue, 0) AS BIGINT) AS revenue_cents,
         t.cat AS top_category
  FROM base b LEFT JOIN top t ON t.person = b.person AND t.dd = b.dd
) TO '{sql_quote(os.path.join(OUT, "user_daily", "part-0.parquet"))}' (FORMAT parquet)
"""
    )
    con.execute(
        f"""
COPY (
  WITH credited AS (
    SELECT camp, sess_start // {DAY_MS} AS dd, count(*) AS sessions,
           count_if(purchases > 0) AS purchasing_sessions, sum(revenue) AS revenue
    FROM sess_c GROUP BY camp, sess_start // {DAY_MS}
  ),
  assisted AS (
    SELECT camp, di AS dd, sum(cnt) AS assisted FROM slc_c GROUP BY camp, di
  ),
  joined AS (
    SELECT coalesce(c.camp, a.camp) AS camp, coalesce(c.dd, a.dd) AS dd,
           coalesce(c.sessions, 0) AS sessions,
           coalesce(c.purchasing_sessions, 0) AS purchasing_sessions,
           coalesce(c.revenue, 0) AS revenue,
           coalesce(a.assisted, 0) AS assisted
    FROM credited c FULL OUTER JOIN assisted a ON c.camp = a.camp AND c.dd = a.dd
  )
  SELECT j.camp AS campaign_id,
         coalesce(cp.channel, 'organic') AS channel,
         CAST(epoch_ms(j.dd * {DAY_MS}) AS DATE) AS day,
         CAST(j.sessions AS BIGINT) AS sessions,
         CAST(j.purchasing_sessions AS BIGINT) AS purchasing_sessions,
         CAST(j.revenue AS BIGINT) AS revenue_cents,
         CAST(j.assisted AS BIGINT) AS assisted_sessions
  FROM joined j LEFT JOIN campaigns cp ON cp.campaign_id = j.camp
  WHERE j.sessions > 0 OR j.assisted > 0
) TO '{sql_quote(os.path.join(OUT, "campaign_daily", "part-0.parquet"))}' (FORMAT parquet)
"""
    )
    log("wrote user_daily and campaign_daily")


if __name__ == "__main__":
    main()
