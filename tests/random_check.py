#!/usr/bin/env python3
"""
Randomized differential test: a small dirty dataset is generated for each seed,
the pipeline and tests/reference.py both run on it, and their two tables are
compared exactly (order-insensitive).

Usage: random_check.py [SEEDS] [--events N] [--clicks N]
Each run writes its data under a temporary folder and removes it unless it fails.
"""
import argparse
import datetime as dt
import json
import os
import random
import shutil
import subprocess
import sys
import tempfile

import polars as pl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PY = sys.executable
WEEK = 7 * 86_400_000
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def iso_utc(ms):
    return (EPOCH + dt.timedelta(milliseconds=ms)).strftime("%Y-%m-%dT%H:%M:%SZ")


def fmt_ts(ms, rng):
    """Render a timestamp in one of the valid forms, or a bad one."""
    roll = rng.random()
    if roll < 0.55:
        return iso_utc(ms)
    if roll < 0.75:
        offset = rng.choice([-600, -330, 0, 60, 330, 720, 840, -1439])
        local = EPOCH + dt.timedelta(milliseconds=ms - offset * 60_000)
        sign = "+" if offset >= 0 else "-"
        hh, mm = divmod(abs(offset), 60)
        return local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{hh:02d}:{mm:02d}"
    if roll < 0.95:
        return str(ms)
    return rng.choice(["", "2026-10-03T10:00:00.5Z", "2026-10-03 10:00:00Z", "2026-02-30T10:00:00Z",
                       "2026-10-03T24:00:00Z", "2026-10-03T10:00:00+24:00", "2026-10-03T10:00:00+0200",
                       "1791071890000.0", "2026-10-03T10:00:60Z"])


def quote_maybe(value, rng):
    if value is None:
        return ""
    if value != "" and rng.random() < 0.2:
        return f'"{value}"'
    return value


def make_dataset(folder, seed, n_events, n_clicks):
    rng = random.Random(seed)
    start_day = dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc) + dt.timedelta(days=rng.randint(0, 60))
    ws = int(start_day.timestamp() * 1000)
    we = ws + 14 * 86_400_000
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "manifest.json"), "w") as f:
        json.dump({"window_start": iso_utc(ws), "window_end": iso_utc(we)}, f)

    users = [f"u{i}" for i in range(1, 120)] + ["U7", " u9", "u9 ", "u200"]
    weights = [1.0 / (i + 1) ** 1.1 for i in range(len(users))]
    products = [f"p{i}" for i in range(1, 15)]
    categories = ["shoes", "books", "toys", "food"]
    campaigns = {f"c_{i}": rng.choice(["search", "social", "email"]) for i in range(1, 9)}

    with open(os.path.join(folder, "products.csv"), "w") as f:
        f.write("product_id,category\n")
        for i, p in enumerate(products):
            f.write(f"{p},{categories[i % len(categories)]}\n")
    with open(os.path.join(folder, "campaigns.csv"), "w") as f:
        f.write("campaign_id,channel\n")
        for c, ch in campaigns.items():
            f.write(f"{c},{ch}\n")

    # links: a random forest over the user pool, plus dirt
    links = []
    for i in range(1, len(users)):
        if rng.random() < 0.35:
            other = users[rng.randrange(0, i)]
            links.append((users[i], other))
    links += [("U7", "u1"), ("u1", "u1"), ("", "u2"), ("u3", "u3")]
    rng.shuffle(links)

    # events: sessions clustered around random anchors, with boundary gaps
    events = []
    for n in range(n_events):
        user = rng.choices(users, weights)[0]
        base = ws - 86_400_000 + rng.randrange(0, 16 * 86_400_000)
        if rng.random() < 0.3:
            base = ws + rng.choice([0, 1, 1_800_000, 1_800_001, 3_600_000]) + rng.randrange(0, 3)
        eid = f"e{n}"
        etype = rng.choices(["view", "cart", "purchase", "Purchase", "view "], [5, 2, 3, 0.1, 0.1])[0]
        pid = rng.choice(products + ["p99", "", " p1"])
        price = rng.choice([str(rng.randint(1, 20_000)), "0250", "19.99", "-5", "", "0", "10000001", "00000000001"])
        # ingest_ts is whole seconds (F1). Copies that tie on ingest_ts must be identical
        # (rules.md), so a differing copy is always ingested at least one second later.
        ingest = (base + rng.choice([0, 0, 1000, 60_000, 90_000_000, 86_400_000, 86_400_001])) // 1000 * 1000
        events.append((eid, user, base, ingest, etype, pid, price))
        if rng.random() < 0.05:  # duplicate copies, possibly with a different user and ts
            other_user = rng.choice(users)
            events.append((eid, other_user, base + rng.choice([0, 0, 5000]), ingest + rng.randint(1, 600) * 1000,
                           etype, pid, price))

    clicks = []
    for n in range(n_clicks):
        user = rng.choices(users, weights)[0]
        ts = ws - WEEK - 5_000_000 + rng.randrange(0, 15 * 86_400_000 + WEEK)
        if rng.random() < 0.3:
            ts = ws + rng.choice([0, 1, 2]) * 1000
        camp = rng.choice(list(campaigns) + ["c_99", "C_1"])
        cid = f"k{n}"
        clicks.append((cid, user, camp, ts, rng.random() < 0.05))

    def render_event(row):
        eid, user, ts, ing, etype, pid, price = row
        bad_user = rng.random() < 0.01
        user_text = "" if bad_user else user
        return ",".join([
            quote_maybe(eid, rng), quote_maybe(user_text, rng), quote_maybe(fmt_ts(ts, rng), rng),
            quote_maybe(iso_utc(ing), rng), quote_maybe(etype, rng), quote_maybe(pid, rng),
            quote_maybe(price, rng),
        ])

    def render_click(row):
        cid, user, camp, ts = row
        return ",".join([quote_maybe(cid, rng), quote_maybe(user, rng), quote_maybe(camp, rng),
                         quote_maybe(fmt_ts(ts, rng), rng)])

    def dirty(line):
        roll = rng.random()
        if roll < 0.01:
            return line + ",extra"
        if roll < 0.02:
            return ""
        if roll < 0.025:
            return line.replace(",", ',"', 1)
        if roll < 0.04:
            return line + "\r"
        return line

    def write_split(sub, header, lines, files):
        target = os.path.join(folder, sub)
        os.makedirs(target, exist_ok=True)
        rng.shuffle(lines)
        chunks = [lines[i::files] for i in range(files)]
        for i, chunk in enumerate(chunks):
            with open(os.path.join(target, f"part-{i:05d}.csv"), "w", newline="") as f:
                f.write(header + "\n")
                for line in chunk:
                    f.write(line + "\n")
        with open(os.path.join(target, "part-99999.csv"), "w", newline="") as f:
            f.write(header + "\n")

    # Copies of a click are identical text (rules.md), so each copy is rendered once.
    click_lines = []
    for cid, user, camp, ts, dup in clicks:
        line = dirty(render_click((cid, user, camp, ts)))
        click_lines.append(line)
        if dup:
            click_lines.append(line)
    write_split("events", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
                [dirty(render_event(r)) for r in events], 4)
    write_split("clicks", "click_id,user_id,campaign_id,ts", click_lines, 3)
    write_split("links", "user_id_a,user_id_b",
                [",".join([quote_maybe(a, rng), quote_maybe(b, rng)]) for a, b in links], 2)


def frame(path, schema):
    if not os.path.isdir(path):
        return pl.DataFrame(schema=schema)
    files = sorted(p for p in os.listdir(path) if p.endswith(".parquet"))
    if not files:
        return pl.DataFrame(schema=schema)
    return pl.concat([pl.read_parquet(os.path.join(path, p)) for p in files])


def canonical(df, cols):
    df = df.select(cols).with_columns(
        [pl.col(c).cast(pl.String).fill_null("<null>") for c in cols]
    )
    return df.sort(cols)


def run_seed(seed, n_events, n_clicks, keep):
    tmp = tempfile.mkdtemp(prefix=f"sscheck-{seed}-")
    data = os.path.join(tmp, "data")
    make_dataset(data, seed, n_events, n_clicks)
    out_pipe = os.path.join(tmp, "out_pipeline")
    out_ref = os.path.join(tmp, "out_reference")
    scratch = os.path.join(tmp, "scratch")
    os.makedirs(scratch)
    os.makedirs(out_pipe)
    env = dict(os.environ, DATA_DIR=data, OUTPUT_DIR=out_pipe, SCRATCH_DIR=scratch, NCPU="4", MEM_GB="4")
    p = subprocess.run([PY, os.path.join(ROOT, "pipeline", "pipeline.py")], env=env, capture_output=True, text=True)
    if p.returncode != 0:
        print(f"seed {seed}: pipeline failed\n{p.stderr[-3000:]}")
        return False
    subprocess.run([PY, os.path.join(HERE, "reference.py"), data, out_ref], check=True)

    ok = True
    users = pl.read_csv(os.path.join(out_ref, "user_daily.csv"), has_header=False,
                        new_columns=["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
                        schema_overrides={"user_id": pl.String, "day": pl.String, "top_category": pl.String},
                        infer_schema_length=0)
    camps = pl.read_csv(os.path.join(out_ref, "campaign_daily.csv"), has_header=False,
                        new_columns=["campaign_id", "channel", "day", "sessions", "purchasing_sessions",
                                     "revenue_cents", "assisted_sessions"],
                        infer_schema_length=0)
    got_users = frame(os.path.join(out_pipe, "user_daily"), None).with_columns(pl.col("day").cast(pl.String))
    got_camps = frame(os.path.join(out_pipe, "campaign_daily"), None).with_columns(pl.col("day").cast(pl.String))
    for name, want, got in (
        ("user_daily", users, got_users),
        ("campaign_daily", camps, got_camps),
    ):
        cols = want.columns
        w = canonical(want.with_columns(pl.col("top_category").replace("", None) if "top_category" in cols else pl.col(cols[0])), cols)
        g = canonical(got, cols)
        if not w.equals(g):
            ok = False
            print(f"seed {seed}: {name} differs: reference {w.height} rows, pipeline {g.height} rows")
            print("only reference:", w.join(g, on=cols, how="anti").head(5))
            print("only pipeline:", g.join(w, on=cols, how="anti").head(5))
    if ok:
        print(f"seed {seed}: match ({got_users.height} user rows, {got_camps.height} campaign rows)")
    if ok and not keep:
        shutil.rmtree(tmp, ignore_errors=True)
    else:
        print(f"  kept {tmp}")
    return ok


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("seeds", nargs="?", type=int, default=5)
    parser.add_argument("--events", type=int, default=1500)
    parser.add_argument("--clicks", type=int, default=600)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    results = [run_seed(s, args.events, args.clicks, args.keep) for s in range(1, args.seeds + 1)]
    print(f"{sum(results)}/{len(results)} seeds match")
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
