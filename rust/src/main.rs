//! Shopping Sessions, native pipeline. Same outputs as pipeline/pipeline.py (the reference).
//!
//! Stages (each timed on stderr):
//!  1. mmap the inputs; split every CSV into newline-aligned byte ranges; parse in parallel.
//!  2. dedup events on event_id (sharded hash), validate the kept copy.
//!  3. intern user IDs (sharded hash), union-find over the linked IDs, name each person
//!     by its byte-wise smallest ID.
//!  4. sort events and clicks by (person, time); sessionize, credit and assist per person
//!     chunk in parallel; build per-(person, day) user rows and dense campaign arrays.
//!  5. write both tables as Parquet.

mod parse;

use ahash::{AHashMap, RandomState};
use arrow::array::{ArrayRef, Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use memchr::memchr;
use memmap2::Mmap;
use parse::*;
use rayon::prelude::*;
use std::env;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const SHARDS: usize = 64;
/// Target size of one parse task. SS_RANGE_BYTES overrides it for testing boundary handling only.
fn range_bytes() -> usize {
    env::var("SS_RANGE_BYTES").ok().and_then(|v| v.parse().ok()).unwrap_or(16 << 20)
}
const PURCHASE: u8 = 3;

type Bytes = &'static [u8];

/// Raw pointer that may be shared between threads when each thread writes disjoint indexes.
struct SyncPtr<T>(*mut T);
unsafe impl<T> Send for SyncPtr<T> {}
unsafe impl<T> Sync for SyncPtr<T> {}
impl<T> SyncPtr<T> {
    fn get(&self) -> *mut T {
        self.0
    }
}

struct Timer(Instant);
impl Timer {
    fn log(&self, msg: &str) {
        eprintln!("[{:7.2}s] {}", self.0.elapsed().as_secs_f64(), msg);
    }
}

// ----------------------------------------------------------------- input files

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                name.ends_with(".csv") && !name.starts_with('.') && p.is_file()
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    out.sort();
    out
}

/// Memory-map a file for the whole run. Empty files map to an empty slice.
fn map_file(path: &Path) -> Bytes {
    let file = File::open(path).expect("open input");
    if file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    let mmap = unsafe { Mmap::map(&file).expect("mmap input") };
    let leaked: &'static Mmap = Box::leak(Box::new(mmap));
    &leaked[..]
}

fn next_line_start(data: &[u8], pos: usize) -> usize {
    let size = data.len();
    if pos >= size {
        return size;
    }
    if data[pos - 1] == b'\n' {
        return pos;
    }
    match memchr(b'\n', &data[pos..]) {
        Some(i) => pos + i + 1,
        None => size,
    }
}

/// Newline-aligned ranges [start, end) of about RANGE_BYTES. Every line starts in exactly one range.
fn ranges(data: &[u8]) -> Vec<(usize, usize)> {
    let size = data.len();
    if size == 0 {
        return Vec::new();
    }
    let step = range_bytes().max(1);
    let mut cuts = vec![0, size];
    let mut k = 1;
    while k * step < size {
        cuts.push(next_line_start(data, k * step));
        k += 1;
    }
    cuts.sort_unstable();
    cuts.dedup();
    cuts.windows(2).filter(|w| w[0] < w[1]).map(|w| (w[0], w[1])).collect()
}

/// All (file, range) tasks of one kind, in file order then range order.
fn tasks_for(dir: &Path) -> Vec<(Bytes, usize, usize, bool)> {
    let mut tasks = Vec::new();
    for path in list_csv(dir) {
        let data = map_file(&path);
        for (start, end) in ranges(data) {
            tasks.push((data, start, end, start == 0));
        }
    }
    tasks
}

// ----------------------------------------------------------------- reference tables

struct Campaigns {
    index: AHashMap<Bytes, u32>,
    channels: Vec<String>, // channel per campaign index; organic is the last entry
    ids: Vec<String>,
}

struct Products {
    index: AHashMap<Bytes, u8>,
    names: Vec<String>, // category names, byte-wise sorted; "unknown" included
    unknown: u8,
}

fn read_reference(data_dir: &Path) -> (Products, Campaigns) {
    let mut product_rows: Vec<(Bytes, Bytes)> = Vec::new();
    let mut campaign_rows: Vec<(Bytes, Bytes)> = Vec::new();
    for (name, rows) in [("products.csv", &mut product_rows), ("campaigns.csv", &mut campaign_rows)] {
        let data = map_file(&data_dir.join(name));
        let mut fields: [Option<Bytes>; 8] = [None; 8];
        for_each_line(data, true, |line| {
            if split_fields(line, 2, &mut fields) {
                if let (Some(a), Some(b)) = (fields[0], fields[1]) {
                    rows.push((a, b));
                }
            }
        });
    }

    let mut names: Vec<String> = product_rows
        .iter()
        .map(|(_, c)| String::from_utf8_lossy(c).into_owned())
        .collect();
    names.push("unknown".to_string());
    names.sort();
    names.dedup();
    let mut index = AHashMap::new();
    for (pid, cat) in &product_rows {
        let name = String::from_utf8_lossy(cat).into_owned();
        let idx = names.binary_search(&name).unwrap() as u8;
        index.insert(*pid, idx);
    }
    let unknown = names.binary_search(&"unknown".to_string()).unwrap() as u8;
    let products = Products { index, names, unknown };

    let mut cindex = AHashMap::new();
    let mut channels = Vec::new();
    let mut ids = Vec::new();
    for (cid, channel) in &campaign_rows {
        cindex.insert(*cid, channels.len() as u32);
        channels.push(String::from_utf8_lossy(channel).into_owned());
        ids.push(String::from_utf8_lossy(cid).into_owned());
    }
    channels.push("organic".to_string());
    ids.push("organic".to_string());
    (products, Campaigns { index: cindex, channels, ids })
}

// ----------------------------------------------------------------- stage 1: parse

#[derive(Clone, Copy)]
struct EvRow {
    eid: Bytes,
    uid: Bytes, // empty means null
    ts: i64,    // i64::MIN when bad
    ing: i64,   // i64::MIN when bad
    price: u32, // purchases with a valid price only
    typ: u8,    // 0 unknown, 1 view, 2 cart, 3 purchase
    cat: u8,    // purchases only
    pid_ok: bool,
    price_ok: bool,
}

const BAD: i64 = i64::MIN;

fn parse_events(products: &Products, tasks: &[(Bytes, usize, usize, bool)]) -> Vec<EvRow> {
    let parts: Vec<Vec<EvRow>> = tasks
        .par_iter()
        .map(|&(data, start, end, skip)| {
            let mut out = Vec::new();
            let mut fields: [Option<Bytes>; 8] = [None; 8];
            for_each_line(&data[start..end], skip, |line| {
                if !split_fields(line, 7, &mut fields) {
                    return;
                }
                let eid = match fields[0] {
                    Some(e) => e,
                    None => return,
                };
                let typ = match fields[4] {
                    Some(b"view") => 1,
                    Some(b"cart") => 2,
                    Some(b"purchase") => 3,
                    _ => 0,
                };
                let mut row = EvRow {
                    eid,
                    uid: fields[1].unwrap_or(&[]),
                    ts: fields[2].and_then(parse_ts).unwrap_or(BAD),
                    ing: fields[3].and_then(parse_ts).unwrap_or(BAD),
                    price: 0,
                    typ,
                    cat: products.unknown,
                    pid_ok: fields[5].is_some(),
                    price_ok: false,
                };
                if typ == PURCHASE {
                    if let Some(p) = fields[6].and_then(parse_price) {
                        row.price = p as u32;
                        row.price_ok = true;
                    }
                    if let Some(pid) = fields[5] {
                        row.cat = *products.index.get(pid).unwrap_or(&products.unknown);
                    }
                }
                out.push(row);
            });
            out
        })
        .collect();
    let total: usize = parts.iter().map(Vec::len).sum();
    let mut rows = Vec::with_capacity(total);
    for p in parts {
        rows.extend(p);
    }
    rows
}

#[derive(Clone, Copy)]
struct CRow {
    cid: Bytes,
    uid: Bytes,
    ts: i64,
    camp: u32,
}

fn parse_clicks(campaigns: &Campaigns, ws: i64, we: i64, tasks: &[(Bytes, usize, usize, bool)]) -> Vec<CRow> {
    let lo = ws - WEEK;
    let parts: Vec<Vec<CRow>> = tasks
        .par_iter()
        .map(|&(data, start, end, skip)| {
            let mut out = Vec::new();
            let mut fields: [Option<Bytes>; 8] = [None; 8];
            for_each_line(&data[start..end], skip, |line| {
                if !split_fields(line, 4, &mut fields) {
                    return;
                }
                let (Some(cid), Some(uid), Some(camp), Some(ts)) = (fields[0], fields[1], fields[2], fields[3]) else {
                    return;
                };
                let Some(&camp) = campaigns.index.get(camp) else { return };
                let Some(ts) = parse_ts(ts) else { return };
                if ts < lo || ts >= we {
                    return;
                }
                out.push(CRow { cid, uid, ts, camp });
            });
            out
        })
        .collect();
    parts.concat()
}

fn parse_links(tasks: &[(Bytes, usize, usize, bool)]) -> Vec<(Bytes, Bytes)> {
    let parts: Vec<Vec<(Bytes, Bytes)>> = tasks
        .par_iter()
        .map(|&(data, start, end, skip)| {
            let mut out = Vec::new();
            let mut fields: [Option<Bytes>; 8] = [None; 8];
            for_each_line(&data[start..end], skip, |line| {
                if split_fields(line, 2, &mut fields) {
                    if let (Some(a), Some(b)) = (fields[0], fields[1]) {
                        out.push((a, b));
                    }
                }
            });
            out
        })
        .collect();
    parts.concat()
}

// ----------------------------------------------------------------- stage 2: events dedup

#[inline]
fn shard_of(rs: &RandomState, key: &[u8]) -> usize {
    (rs.hash_one(key) >> 58) as usize
}

/// Item indexes grouped by shard, in index order within each shard. Built in parallel:
/// each worker buckets its own slice, and the per-worker buckets are joined in worker order.
fn bucket(shards: &[u8]) -> Vec<Vec<u32>> {
    let workers = rayon::current_num_threads().max(1);
    let per = shards.len().div_ceil(workers).max(1);
    let parts: Vec<Vec<Vec<u32>>> = (0..workers)
        .into_par_iter()
        .map(|w| {
            let lo = (w * per).min(shards.len());
            let hi = ((w + 1) * per).min(shards.len());
            let mut buckets: Vec<Vec<u32>> = vec![Vec::new(); SHARDS];
            for i in lo..hi {
                buckets[shards[i] as usize].push(i as u32);
            }
            buckets
        })
        .collect();
    (0..SHARDS)
        .into_par_iter()
        .map(|s| {
            let total: usize = parts.iter().map(|p| p[s].len()).sum();
            let mut v = Vec::with_capacity(total);
            for p in &parts {
                v.extend_from_slice(&p[s]);
            }
            v
        })
        .collect()
}

/// Dedup on event_id: the copy with the smallest ingest_ts wins (null ingest last).
/// Then validate the kept copy. Returns the indexes of valid kept rows.
fn dedup_and_validate(rows: &[EvRow], rs: &RandomState, ws: i64, we: i64) -> Vec<u32> {
    let shards: Vec<u8> = rows.par_iter().map(|r| shard_of(rs, r.eid) as u8).collect();
    let buckets = bucket(&shards);
    let per_shard: Vec<Vec<u32>> = buckets
        .par_iter()
        .map(|idxs| {
            let mut best: AHashMap<Bytes, u32> = AHashMap::with_capacity(idxs.len());
            for &i in idxs {
                let r = &rows[i as usize];
                match best.get_mut(r.eid) {
                    None => {
                        best.insert(r.eid, i);
                    }
                    Some(cur) => {
                        if ing_key(r.ing) < ing_key(rows[*cur as usize].ing) {
                            *cur = i;
                        }
                    }
                }
            }
            best.values().copied().filter(|&i| valid(&rows[i as usize], ws, we)).collect()
        })
        .collect();
    per_shard.concat()
}

#[inline]
fn ing_key(ing: i64) -> (bool, i64) {
    if ing == BAD {
        (true, 0)
    } else {
        (false, ing)
    }
}

#[inline]
fn valid(r: &EvRow, ws: i64, we: i64) -> bool {
    !r.uid.is_empty()
        && r.ts != BAD
        && r.ts >= ws
        && r.ts < we
        && r.ing != BAD
        && r.ing - r.ts <= LATE
        && r.typ != 0
        && r.pid_ok
        && (r.typ != PURCHASE || r.price_ok)
}

// ----------------------------------------------------------------- stage 3: identity

/// Global user-ID table: one hash map per shard. Local IDs are shard-local; the global ID is base[shard] + local.
struct Interner {
    maps: Vec<AHashMap<Bytes, u32>>,
    rs: RandomState,
}

impl Interner {
    fn new(rs: RandomState) -> Self {
        Interner { maps: (0..SHARDS).map(|_| AHashMap::new()).collect(), rs }
    }

    /// Intern every key. Returns (shard, local) per item, packed as shard << 32 | local.
    fn intern(&mut self, keys: &[Bytes]) -> Vec<u64> {
        let rs = &self.rs;
        let shards: Vec<u8> = keys.par_iter().map(|k| shard_of(rs, k) as u8).collect();
        let buckets = bucket(&shards);
        let results: Vec<Vec<(u32, u64)>> = self
            .maps
            .par_iter_mut()
            .enumerate()
            .map(|(s, map)| {
                let idxs = &buckets[s];
                let mut out = Vec::with_capacity(idxs.len());
                for &i in idxs {
                    let key = keys[i as usize];
                    let next = map.len() as u32;
                    let local = *map.entry(key).or_insert(next);
                    out.push((i, ((s as u64) << 32) | local as u64));
                }
                out
            })
            .collect();
        let mut packed = vec![0u64; keys.len()];
        let packed_ptr = SyncPtr(packed.as_mut_ptr());
        results.par_iter().for_each(|part| {
            for &(i, p) in part {
                // SAFETY: every item index appears in exactly one shard's part, so writes never overlap.
                unsafe { *packed_ptr.get().add(i as usize) = p };
            }
        });
        packed
    }

    fn total(&self) -> usize {
        self.maps.iter().map(|m| m.len()).sum()
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

// ----------------------------------------------------------------- stage 4: sessions

#[derive(Clone, Copy)]
struct EvS {
    key: u64, // person << 32 | (ts - ws)
    price: u32,
    cat: u8,
    typ: u8,
}

#[derive(Clone, Copy)]
struct ClS {
    key: u64, // person << 32 | (ts - (ws - 7 days))
    cid: Bytes,
    ts: i64,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Sess {
    person: u32,
    start: i64,
    day: u32, // day index from the window's first day
    purchases: u32,
    revenue: i64,
}

#[derive(Clone, Copy)]
struct UserRow {
    person: u32,
    day: u32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: i32, // category index, -1 when no purchases
}

/// Smallest user ID of each person, looked up by person ID. Shared read-only by the chunk workers.
struct Names {
    keys: Vec<Bytes>,
    best: Vec<u32>,
}

impl Names {
    fn name(&self, person: u32) -> String {
        String::from_utf8(self.keys[self.best[person as usize] as usize].to_vec()).expect("user id is UTF-8")
    }
}

struct ChunkOut {
    users: Vec<(String, UserRow)>,
    credited: Vec<[i64; 3]>, // (campaign, day) -> sessions, purchasing, revenue; campaign = ncamp is organic
    assisted: Vec<i64>,      // (campaign, day)
}

struct Dims {
    ws: i64,
    wsday: i64,
    nd: usize,
    ncamp: usize, // organic index
    ncat: usize,
}

fn process_chunk(dims: &Dims, names: &Names, evs: &[EvS], cls: &[ClS]) -> ChunkOut {
    // ---- sessions and per-(person, day) user accumulators
    let mut sessions: Vec<Sess> = Vec::new();
    let mut users: Vec<(String, UserRow)> = Vec::new();
    let mut acc: Option<(u32, u32, UserRow, Vec<i64>, bool)> = None; // (person, day, row, cat sums, has purchase)

    let flush = |acc: &mut Option<(u32, u32, UserRow, Vec<i64>, bool)>, users: &mut Vec<(String, UserRow)>| {
        if let Some((_, _, mut row, cats, purch)) = acc.take() {
            row.top = -1;
            if purch {
                let mut best = 0i64;
                for (c, &sum) in cats.iter().enumerate() {
                    if sum > best {
                        best = sum;
                        row.top = c as i32;
                    }
                }
            }
            users.push((names.name(row.person), row));
        }
    };

    let mut last_ts = BAD;
    let mut cur_person = u32::MAX;
    for e in evs {
        let person = (e.key >> 32) as u32;
        let ts = (e.key & 0xffff_ffff) as i64 + dims.ws;
        if person != cur_person {
            flush(&mut acc, &mut users);
            cur_person = person;
            last_ts = BAD;
        }
        let new_session = last_ts == BAD || (ts != last_ts && ts - last_ts > GAP);
        last_ts = ts;
        let day = ((ts / DAY) - dims.wsday) as u32;
        if new_session {
            sessions.push(Sess { person, start: ts, day, purchases: 0, revenue: 0 });
            let same = matches!(&acc, Some((p, d, _, _, _)) if *p == person && *d == day);
            if !same {
                flush(&mut acc, &mut users);
                acc = Some((
                    person,
                    day,
                    UserRow { person, day, sessions: 0, events: 0, revenue: 0, top: -1 },
                    vec![0i64; dims.ncat],
                    false,
                ));
            }
            if let Some((_, _, row, _, _)) = acc.as_mut() {
                row.sessions += 1;
            }
        }
        let s = sessions.last_mut().unwrap();
        let (_, _, row, cats, purch) = acc.as_mut().unwrap();
        row.events += 1;
        if e.typ == PURCHASE {
            s.purchases += 1;
            s.revenue += e.price as i64;
            row.revenue += e.price as i64;
            cats[e.cat as usize] += e.price as i64;
            *purch = true;
        }
    }
    flush(&mut acc, &mut users);

    // ---- attribution per person, on the same chunk
    let mut credited = vec![[0i64; 3]; (dims.ncamp + 1) * dims.nd];
    let mut assisted = vec![0i64; (dims.ncamp + 1) * dims.nd];
    let mut si = 0usize;
    let mut ci = 0usize;
    while si < sessions.len() {
        let person = sessions[si].person;
        let s_end = si + sessions[si..].iter().take_while(|s| s.person == person).count();
        let sp = &sessions[si..s_end];
        while ci < cls.len() && ((cls[ci].key >> 32) as u32) < person {
            ci += 1;
        }
        let c_end = ci + cls[ci..].iter().take_while(|c| (c.key >> 32) as u32 == person).count();
        let cp = &cls[ci..c_end];

        // credit: latest click at or before the session start; ties resolved by sorted cid
        let mut j = 0usize;
        let mut latest: Option<(i64, u32)> = None;
        for s in sp {
            while j < cp.len() && cp[j].ts <= s.start {
                if latest.map_or(true, |(t, _)| cp[j].ts > t) {
                    latest = Some((cp[j].ts, cp[j].camp));
                }
                j += 1;
            }
            let camp = match latest {
                Some((t, c)) if s.start - t <= WEEK => c as usize,
                _ => dims.ncamp,
            };
            let cell = &mut credited[camp * dims.nd + s.day as usize];
            cell[0] += 1;
            if s.purchases > 0 {
                cell[1] += 1;
            }
            cell[2] += s.revenue;
        }

        // assists: per campaign, merged intervals [ts, ts + 7d]; sessions inside each interval
        let mut by_camp: Vec<(u32, i64)> = cp.iter().map(|c| (c.camp, c.ts)).collect();
        by_camp.sort_unstable();
        let mut g = 0usize;
        while g < by_camp.len() {
            let camp = by_camp[g].0;
            let mut h = g;
            while h < by_camp.len() && by_camp[h].0 == camp {
                h += 1;
            }
            let mut lo = by_camp[g].1;
            let mut prev = lo;
            for k in g + 1..h {
                let ts = by_camp[k].1;
                if ts - prev > WEEK {
                    count_island(dims, sp, &mut assisted, camp as usize, lo, prev + WEEK);
                    lo = ts;
                }
                prev = ts;
            }
            count_island(dims, sp, &mut assisted, camp as usize, lo, prev + WEEK);
            g = h;
        }
        si = s_end;
        ci = c_end;
    }
    ChunkOut { users, credited, assisted }
}

/// Add, per day, the number of sessions of this person whose start lies in [lo, hi] (closed).
fn count_island(dims: &Dims, sp: &[Sess], assisted: &mut [i64], camp: usize, lo: i64, hi: i64) {
    let first_day = ((lo / DAY) - dims.wsday).max(0);
    let last_day = ((hi / DAY) - dims.wsday).min(dims.nd as i64 - 1);
    for d in first_day..=last_day {
        let day_start = (dims.wsday + d) * DAY;
        let a = lo.max(day_start);
        let b = hi.min(day_start + DAY - 1);
        if a > b {
            continue;
        }
        let n = sp.partition_point(|s| s.start <= b) - sp.partition_point(|s| s.start < a);
        if n > 0 {
            assisted[camp * dims.nd + d as usize] += n as i64;
        }
    }
}

// ----------------------------------------------------------------- stage 5: write

fn write_tables(
    out: &Path,
    cat_names: &[String],
    user_rows: Vec<(String, u32, i64, i64, i64, i32)>,
    campaigns: &Campaigns,
    dims: &Dims,
    credited: &[[i64; 3]],
    assisted: &[i64],
) -> Result<(), Box<dyn std::error::Error>> {
    // user_daily
    let n = user_rows.len();
    let mut uid = Vec::with_capacity(n);
    let mut day = Vec::with_capacity(n);
    let mut sess = Vec::with_capacity(n);
    let mut ev = Vec::with_capacity(n);
    let mut rev = Vec::with_capacity(n);
    let mut top: Vec<Option<String>> = Vec::with_capacity(n);
    for (name, d, s, e, r, t) in &user_rows {
        uid.push(name.as_str());
        day.push((dims.wsday + *d as i64) as i32);
        sess.push(*s);
        ev.push(*e);
        rev.push(*r);
        top.push(if *t < 0 { None } else { Some(cat_names[*t as usize].clone()) });
    }
    let top_refs: Vec<Option<&str>> = top.iter().map(|o| o.as_deref()).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(uid)) as ArrayRef,
            Arc::new(Date32Array::from(day)),
            Arc::new(Int64Array::from(sess)),
            Arc::new(Int64Array::from(ev)),
            Arc::new(Int64Array::from(rev)),
            Arc::new(StringArray::from(top_refs)),
        ],
    )?;
    write_parquet(&out.join("user_daily").join("part-0.parquet"), schema, batch)?;

    // campaign_daily
    let mut cid = Vec::new();
    let mut chan = Vec::new();
    let mut cday = Vec::new();
    let mut cs = Vec::new();
    let mut cp = Vec::new();
    let mut cr = Vec::new();
    let mut ca = Vec::new();
    for camp in 0..=dims.ncamp {
        for d in 0..dims.nd {
            let c = credited[camp * dims.nd + d];
            let a = assisted[camp * dims.nd + d];
            if c[0] == 0 && a == 0 {
                continue;
            }
            cid.push(campaigns.ids[camp].clone());
            chan.push(campaigns.channels[camp].clone());
            cday.push((dims.wsday + d as i64) as i32);
            cs.push(c[0]);
            cp.push(c[1]);
            cr.push(c[2]);
            ca.push(a);
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let cid_refs: Vec<&str> = cid.iter().map(String::as_str).collect();
    let chan_refs: Vec<&str> = chan.iter().map(String::as_str).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(cid_refs)) as ArrayRef,
            Arc::new(StringArray::from(chan_refs)),
            Arc::new(Date32Array::from(cday)),
            Arc::new(Int64Array::from(cs)),
            Arc::new(Int64Array::from(cp)),
            Arc::new(Int64Array::from(cr)),
            Arc::new(Int64Array::from(ca)),
        ],
    )?;
    write_parquet(&out.join("campaign_daily").join("part-0.parquet"), schema, batch)?;
    Ok(())
}

fn write_parquet(
    path: &Path,
    schema: Arc<Schema>,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

// ----------------------------------------------------------------- main

fn main() {
    let timer = Timer(Instant::now());
    let data_dir = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out_dir = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    rayon::ThreadPoolBuilder::new()
        .num_threads(ncpu.max(1))
        .build_global()
        .expect("thread pool");

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(data_dir.join("manifest.json")).expect("manifest")).expect("json");
    let ws = parse_ts(manifest["window_start"].as_str().unwrap_or("").as_bytes()).expect("window_start");
    let we = parse_ts(manifest["window_end"].as_str().unwrap_or("").as_bytes()).expect("window_end");
    let wsday = ws.div_euclid(DAY);
    let nd = ((we - 1).div_euclid(DAY) - wsday + 1) as usize;

    let (products, campaigns) = read_reference(&data_dir);
    timer.log("reference tables read");

    let ev_tasks = tasks_for(&data_dir.join("events"));
    let ck_tasks = tasks_for(&data_dir.join("clicks"));
    let ln_tasks = tasks_for(&data_dir.join("links"));
    timer.log(&format!(
        "planned {} event, {} click, {} link ranges",
        ev_tasks.len(),
        ck_tasks.len(),
        ln_tasks.len()
    ));

    let rows = parse_events(&products, &ev_tasks);
    timer.log(&format!("parsed {} event lines", rows.len()));
    let clicks = parse_clicks(&campaigns, ws, we, &ck_tasks);
    timer.log(&format!("parsed {} valid clicks", clicks.len()));
    let links = parse_links(&ln_tasks);
    timer.log(&format!("parsed {} valid links", links.len()));

    let rs = RandomState::new();
    let kept = dedup_and_validate(&rows, &rs, ws, we);
    timer.log(&format!("events deduped and validated: {} kept", kept.len()));

    // ---- identity
    let mut interner = Interner::new(rs);
    let ev_keys: Vec<Bytes> = kept.iter().map(|&i| rows[i as usize].uid).collect();
    let ev_packed = interner.intern(&ev_keys);
    let ck_keys: Vec<Bytes> = clicks.iter().map(|c| c.uid).collect();
    let ck_packed = interner.intern(&ck_keys);
    let mut ln_keys: Vec<Bytes> = Vec::with_capacity(links.len() * 2);
    for &(a, b) in &links {
        ln_keys.push(a);
        ln_keys.push(b);
    }
    let ln_packed = interner.intern(&ln_keys);
    timer.log(&format!("interned {} user ids", interner.total()));

    let mut base = vec![0u32; SHARDS];
    let mut acc = 0u32;
    for s in 0..SHARDS {
        base[s] = acc;
        acc += interner.maps[s].len() as u32;
    }
    let n_ids = acc as usize;
    let gid = |p: u64| base[(p >> 32) as usize] + (p & 0xffff_ffff) as u32;
    let mut keys: Vec<Bytes> = vec![&[]; n_ids];
    for s in 0..SHARDS {
        for (&k, &local) in &interner.maps[s] {
            keys[(base[s] + local) as usize] = k;
        }
    }

    let mut parent: Vec<u32> = (0..n_ids as u32).collect();
    let mut size: Vec<u32> = vec![1; n_ids];
    for i in 0..links.len() {
        let a = gid(ln_packed[2 * i]);
        let b = gid(ln_packed[2 * i + 1]);
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            let (big, small) = if size[ra as usize] >= size[rb as usize] { (ra, rb) } else { (rb, ra) };
            parent[small as usize] = big;
            size[big as usize] += size[small as usize];
        }
    }
    let root: Vec<u32> = (0..n_ids as u32).map(|g| find(&mut parent, g)).collect();
    // smallest ID per component, compared byte-wise, once per ID
    let mut best = vec![u32::MAX; n_ids];
    for g in 0..n_ids {
        let r = root[g] as usize;
        if best[r] == u32::MAX || keys[g] < keys[best[r] as usize] {
            best[r] = g as u32;
        }
    }
    let names = Names { keys, best };
    timer.log(&format!("identity: {} ids, union-find done", n_ids));

    // ---- events: person-sorted array
    let mut evs: Vec<EvS> = kept
        .iter()
        .zip(ev_packed.iter())
        .map(|(&i, &p)| {
            let r = &rows[i as usize];
            let person = root[gid(p) as usize];
            EvS { key: ((person as u64) << 32) | (r.ts - ws) as u64, price: r.price, cat: r.cat, typ: r.typ }
        })
        .collect();
    drop(rows);
    evs.par_sort_unstable_by_key(|e| e.key);
    timer.log(&format!("events sorted: {}", evs.len()));

    // ---- clicks: person-sorted array, duplicates removed (identical copies share a key)
    let lo = ws - WEEK;
    let mut cls: Vec<ClS> = clicks
        .iter()
        .zip(ck_packed.iter())
        .map(|(c, &p)| {
            let person = root[gid(p) as usize];
            ClS { key: ((person as u64) << 32) | (c.ts - lo) as u64, cid: c.cid, ts: c.ts, camp: c.camp }
        })
        .collect();
    cls.par_sort_unstable_by(|a, b| a.key.cmp(&b.key).then_with(|| a.cid.cmp(b.cid)));
    cls.dedup_by(|a, b| a.key == b.key && a.cid == b.cid);
    timer.log(&format!("clicks sorted: {}", cls.len()));

    // ---- chunks split at person boundaries
    let dims = Dims { ws, wsday, nd, ncamp: campaigns.ids.len() - 1, ncat: products.names.len() };
    let mut cuts: Vec<usize> = vec![0];
    let chunks_wanted = (ncpu * 32).max(1);
    for k in 1..chunks_wanted {
        let mut i = k * evs.len() / chunks_wanted;
        while i > 0 && i < evs.len() && (evs[i].key >> 32) == (evs[i - 1].key >> 32) {
            i += 1;
        }
        if i > *cuts.last().unwrap() && i < evs.len() {
            cuts.push(i);
        }
    }
    cuts.push(evs.len());
    let chunk_ranges: Vec<(usize, usize)> = cuts.windows(2).map(|w| (w[0], w[1])).filter(|(a, b)| a < b).collect();
    let outs: Vec<ChunkOut> = chunk_ranges
        .par_iter()
        .enumerate()
        .map(|(k, &(a, b))| {
            let person_lo = (evs[a].key >> 32) as u32;
            let c_start = if k == 0 { 0 } else { cls.partition_point(|c| ((c.key >> 32) as u32) < person_lo) };
            let c_end = if k + 1 == chunk_ranges.len() {
                cls.len()
            } else {
                let next = (evs[b].key >> 32) as u32;
                cls.partition_point(|c| ((c.key >> 32) as u32) < next)
            };
            process_chunk(&dims, &names, &evs[a..b], &cls[c_start..c_end.max(c_start)])
        })
        .collect();
    timer.log(&format!("sessions, credit and assists done over {} chunks", outs.len()));

    // ---- merge
    let mut credited = vec![[0i64; 3]; (dims.ncamp + 1) * nd];
    let mut assisted = vec![0i64; (dims.ncamp + 1) * nd];
    let mut user_rows: Vec<(String, u32, i64, i64, i64, i32)> = Vec::new();
    for out in &outs {
        for (dst, src) in credited.iter_mut().zip(&out.credited) {
            for k in 0..3 {
                dst[k] += src[k];
            }
        }
        for (dst, src) in assisted.iter_mut().zip(&out.assisted) {
            *dst += src;
        }
        for (name, u) in &out.users {
            user_rows.push((name.clone(), u.day, u.sessions, u.events, u.revenue, u.top));
        }
    }
    timer.log(&format!("merged {} user rows", user_rows.len()));

    std::fs::create_dir_all(out_dir.join("user_daily")).expect("create output dir");
    std::fs::create_dir_all(out_dir.join("campaign_daily")).expect("create output dir");
    write_tables(
        &out_dir,
        &products.names,
        user_rows,
        &campaigns,
        &dims,
        &credited,
        &assisted,
    )
    .expect("write parquet");
    timer.log("wrote user_daily and campaign_daily");
}
