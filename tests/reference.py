#!/usr/bin/env python3
"""
A deliberately naive, independent implementation of rules.md, used only to check
the pipeline on random data. It favours obviousness over speed: quadratic loops,
no shared helpers with pipeline/pipeline.py, plain Python dicts and sets.

Usage: reference.py DATA_DIR OUTPUT_DIR   (writes user_daily.csv, campaign_daily.csv)
"""
import csv
import datetime as dt
import glob
import json
import os
import re
import sys

WEEK = 7 * 86_400_000
DAY = 86_400_000
GAP = 1_800_000
LATE = 86_400_000


def parse_fields(line, n):
    """Return the n cleaned values of a line, or None when the line is malformed."""
    if line == "":
        return None
    parts = line.split(",")
    if len(parts) != n:
        return None
    out = []
    for part in parts:
        m = re.fullmatch(r'"([^"]*)"', part)
        value = m.group(1) if m else part
        if '"' in value:
            return None
        out.append(value if value != "" else None)
    return out


def read_rows(folder, n):
    rows = []
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        with open(path, "rb") as f:
            text = f.read().decode("utf-8")
        lines = text.split("\n")
        if lines and lines[-1] == "":
            lines = lines[:-1]
        for line in lines[1:]:  # header row
            fields = parse_fields(line, n)
            if fields is not None:
                rows.append(fields)
    return rows


def parse_ts(s):
    if s is None:
        return None
    if re.fullmatch(r"[0-9]+", s):
        return int(s)
    m = re.fullmatch(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})", s)
    if not m:
        return None
    try:
        base = dt.datetime(*map(int, m.groups()[:6]), tzinfo=dt.timezone.utc)
    except ValueError:
        return None
    zone = m.group(7)
    if zone == "Z":
        offset = 0
    else:
        hh, mm = int(zone[1:3]), int(zone[4:6])
        if hh > 23 or mm > 59:
            return None
        offset = (1 if zone[0] == "+" else -1) * (hh * 60 + mm) * 60_000
    return int(base.timestamp() * 1000) - offset


def is_price(p):
    return p is not None and re.fullmatch(r"[0-9]+", p) is not None and 1 <= int(p) <= 10_000_000


def main():
    data, out = sys.argv[1], sys.argv[2]
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    ws = parse_ts(manifest["window_start"])
    we = parse_ts(manifest["window_end"])

    # identity: union-find over link sides
    parent = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in read_rows(os.path.join(data, "links"), 2):
        if a is None or b is None:
            continue
        for x in (a, b):
            parent.setdefault(x, x)
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)
    members = {}
    for x in parent:
        members.setdefault(find(x), []).append(x)
    name_of_id = {}
    for root, ids in members.items():
        name = min(ids)
        for x in ids:
            name_of_id[x] = name

    def person(uid):
        return name_of_id.get(uid, uid)

    products = {}
    with open(os.path.join(data, "products.csv"), newline="") as f:
        for row in csv.DictReader(f):
            products[row["product_id"]] = row["category"]
    campaigns = {}
    with open(os.path.join(data, "campaigns.csv"), newline="") as f:
        for row in csv.DictReader(f):
            campaigns[row["campaign_id"]] = row["channel"]

    # events
    kept = {}
    for eid, uid, ts, ing, etype, pid, price in read_rows(os.path.join(data, "events"), 7):
        if eid is None:
            continue
        ing_ms = parse_ts(ing)
        key = (ing_ms is None, ing_ms or 0)
        if eid not in kept or key < kept[eid][0]:
            kept[eid] = (key, (uid, ts, ing_ms, etype, pid, price))
    events = []
    for _, (uid, ts, ing_ms, etype, pid, price) in kept.values():
        ts_ms = parse_ts(ts)
        if uid is None or ts_ms is None or not (ws <= ts_ms < we):
            continue
        if ing_ms is None or ing_ms - ts_ms > LATE:
            continue
        if etype not in ("view", "cart", "purchase") or pid is None:
            continue
        if etype == "purchase" and not is_price(price):
            continue
        events.append({
            "person": person(uid), "ts": ts_ms, "type": etype, "pid": pid,
            "price": int(price) if etype == "purchase" else 0,
        })

    # sessions: per person, sorted by ts; gaps between distinct timestamps
    by_person = {}
    for e in events:
        by_person.setdefault(e["person"], []).append(e)
    sessions = {}  # (person, start) -> dict
    for p, evs in by_person.items():
        stamps = sorted({e["ts"] for e in evs})
        start_of = {}
        current = None
        prev = None
        for t in stamps:
            if prev is None or t - prev > GAP:
                current = t
            start_of[t] = current
            prev = t
        for e in evs:
            key = (p, start_of[e["ts"]])
            s = sessions.setdefault(key, {"events": 0, "purchases": 0, "revenue": 0, "cats": {}})
            s["events"] += 1
            if e["type"] == "purchase":
                s["purchases"] += 1
                s["revenue"] += e["price"]
                cat = products.get(e["pid"], "unknown")
                s["cats"][cat] = s["cats"].get(cat, 0) + e["price"]

    # clicks
    seen = {}
    for cid, uid, camp, ts in read_rows(os.path.join(data, "clicks"), 4):
        if cid is None:
            continue
        seen.setdefault(cid, (uid, camp, ts))
    clicks_by_person = {}
    for cid, (uid, camp, ts) in seen.items():
        ts_ms = parse_ts(ts)
        if uid is None or ts_ms is None or not (ws - WEEK <= ts_ms < we):
            continue
        if camp not in campaigns:
            continue
        clicks_by_person.setdefault(person(uid), []).append((ts_ms, cid, camp))

    # attribution per session
    credited = {}  # (camp, day) -> [sessions, purchasing, revenue]
    assisted = {}  # (camp, day) -> count
    user_rows = {}
    for (p, start), s in sessions.items():
        day = start // DAY
        cands = [c for c in clicks_by_person.get(p, []) if start - WEEK <= c[0] <= start]
        if cands:
            best = min(cands, key=lambda c: (-c[0], c[1]))
            camp = best[2]
        else:
            camp = "organic"
        row = credited.setdefault((camp, day), [0, 0, 0])
        row[0] += 1
        row[1] += 1 if s["purchases"] else 0
        row[2] += s["revenue"]
        for camp_a in {c[2] for c in cands}:
            assisted[(camp_a, day)] = assisted.get((camp_a, day), 0) + 1

        u = user_rows.setdefault((p, day), {"sessions": 0, "events": 0, "revenue": 0, "cats": {}})
        u["sessions"] += 1
        u["events"] += s["events"]
        u["revenue"] += s["revenue"]
        for cat, amt in s["cats"].items():
            u["cats"][cat] = u["cats"].get(cat, 0) + amt

    os.makedirs(out, exist_ok=True)
    day_str = lambda d: (dt.date(1970, 1, 1) + dt.timedelta(days=d)).isoformat()  # noqa: E731
    with open(os.path.join(out, "user_daily.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for (p, day), u in sorted(user_rows.items()):
            top = None
            if u["cats"]:
                top = min(u["cats"].items(), key=lambda kv: (-kv[1], kv[0]))[0]
            w.writerow([p, day_str(day), u["sessions"], u["events"], u["revenue"], top or ""])
    keys = set(credited) | set(assisted)
    with open(os.path.join(out, "campaign_daily.csv"), "w", newline="") as f:
        w = csv.writer(f)
        for camp, day in sorted(keys):
            c = credited.get((camp, day), [0, 0, 0])
            a = assisted.get((camp, day), 0)
            if c[0] == 0 and a == 0:
                continue
            channel = "organic" if camp == "organic" else campaigns[camp]
            w.writerow([camp, channel, day_str(day), c[0], c[1], c[2], a])


if __name__ == "__main__":
    main()
