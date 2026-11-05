// pdb-turso.mjs — bản async của pdb.mjs cho Turso (crawler trên GitHub Actions).
// Cùng API, thêm helper cho auto.mjs. P4K_TURSO_LOCAL=1 -> chạy thử trên SQLite local.
import { tursoClient } from "./turso-http.mjs";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);

export const IDF = /^[A-Z0-9]{10,17}$/;
export const IS_TIMESTAMP = /^\d{8}T\d{4,6}Z?$/i;
export const PHIM_EXT = new Set([
  ".h5", ".hdf5", ".pb", ".arrow", ".model", ".pth", ".safetensors",
  ".pt", ".ckpt", ".weights", ".tflite", ".onnx", ".keras", ".bin", ".pkl",
  ".joblib", ".gguf", ".nemo", ".mar", ".msgpack", ".mlmodel", ".h5mu", ".params", ".param",
  ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".ts", ".flv", ".wmv", ".mpg", ".mpeg", ".3gp",
]);
export const JUNK_EMPTY = parseInt(process.env.P4K_JUNK_EMPTY || "3", 10);
// TTL: repo có phim (has_id=1) 6h; repo không có phim (has_id=0) 24h.
const REPO_TTL_HAS_MS = Math.max(600000, parseInt(process.env.P4K_REPO_TTL || "21600000", 10));
const REPO_TTL_NOID_MS = Math.max(600000, parseInt(process.env.P4K_REPO_TTL_NO_ID || "604800000", 10)); // 7 ngày cho repo không có phim
const JITTER_MS = 3600000;
const h32 = (s) => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; };
const dueAt = (hasId, updated, key) => {
  const now = Date.now();
  const ttl = hasId ? REPO_TTL_HAS_MS : REPO_TTL_NOID_MS;
  return now + ttl - (h32(key) % JITTER_MS);
};
export const isFarm = (u) => { const s = String(u); return /^[a-z]+\d{3,10}$/i.test(s) || /^[a-z]+\d{1,4}[a-z]+\d{0,4}$/i.test(s) || /^[a-z]{12,}$/i.test(s); };
export const isFarmStrict = (u) => { const s = String(u); return /^[a-z]+\d{3,10}$/i.test(s) || /^[a-z]+\d{1,4}[a-z]+\d{0,4}$/i.test(s); };
const LOW_NUM = /^[a-z]{2,}[0-9]{1,7}$/;
export function accountScore(u, a = {}) {
  let s = 0;
  if (a.has_id) s += 100;
  if (LOW_NUM.test(String(u))) s += 50;
  else if (/^[a-z]/.test(String(u))) s += 25;
  if (/^[A-Z]/.test(String(u))) s -= 50;
  if (a.junk) s -= 200;
  if (!a.nrepo) s += 5;
  return s;
}
export function repoScore(id, account, kind, a = {}) {
  const base = String(id).split("/").slice(1).join("/");
  let p;
  if (kind === "bucket") p = base === account ? 80 : 50;
  else if (id === account + "/" + account) p = 70;
  else if (/^[a-z]{3,20}$/.test(base)) p = LOW_NUM.test(String(account)) ? 30 : 0;
  else if (/^[A-Za-z]{3,20}$/.test(base)) p = LOW_NUM.test(String(account)) ? 10 : 0;
  else if (kind === "model") p = -60;
  else p = -40;
  if (LOW_NUM.test(String(account))) p += 20;
  if (a.has_id) p += 40;
  if (/^[A-Z]/.test(String(account)) && !a.has_id) p -= 50;
  return p;
}
export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function maskLog(m) {
  if (typeof m !== "string") return m;
  return m
    .replace(/\b([a-zA-Z0-9_-]{2,})([a-zA-Z0-9_-]{2,})\/([a-zA-Z0-9_-]{2,})([a-zA-Z0-9_-]{2,})\b/g, (match, u1, u2, r1, r2) => `${u1.slice(0, 2)}***${u2.slice(-1)}/${r1.slice(0, 2)}***${r2.slice(-1)}`)
    .replace(/(junk rong |inv thieu |inv err )([a-zA-Z0-9_-]{2,})([a-zA-Z0-9_-]{2,})/g, (match, pfx, u1, u2) => `${pfx}${u1.slice(0, 2)}***${u2.slice(-1)}`);
}
export const log = (m) => process.stderr.write(`[${new Date().toISOString().slice(11, 19)}] ${maskLog(m)}\n`);
export async function retrySync(fn) { return await fn(); }

export function isId(name) {
  const b = String(name || "").split("/").pop();
  const m = b.match(/\.[^.]+$/);
  if (!m || !PHIM_EXT.has(m[0].toLowerCase())) return false;
  const base = b.slice(0, b.length - m[0].length);
  return IDF.test(base) && !IS_TIMESTAMP.test(base);
}
export function idParts(name) {
  const bn = String(name || "").split("/").pop();
  const m = bn.match(/\.[^.]+$/);
  const ext = m ? m[0].toLowerCase() : "(no-ext)";
  const base = m ? bn.slice(0, bn.length - m[0].length) : bn;
  return { ext, base, ok: IDF.test(base) && !IS_TIMESTAMP.test(base) };
}

// ---------- backend: Turso HTTP hoặc SQLite local (test) ----------
export function openDB() {
  if (process.env.P4K_TURSO_LOCAL) {
    const { DatabaseSync } = require("node:sqlite");
    const d = new DatabaseSync(process.env.P4K_DB || "/tmp/p4k-turso-local.db");
    d.exec("PRAGMA busy_timeout=15000");
    return {
      mode: "local",
      exec(sql, args = []) {
        const st = d.prepare(sql);
        if (/^\s*(select|with|pragma)/i.test(sql)) {
          const rows = st.all(...args);
          return { rows, changes: 0 };
        }
        const r = st.run(...args);
        return { rows: [], changes: r.changes };
      },
      async batch(stmts, chunk = 500) { for (let i = 0; i < stmts.length; i += chunk) for (const s of stmts.slice(i, i + chunk)) this.exec(s.sql, s.args || []); },
    };
  }
  const c = tursoClient({ url: process.env.TURSO_DATABASE_URL, token: process.env.TURSO_AUTH_TOKEN });
  return {
    mode: "turso",
    exec: (sql, args = []) => c.exec(sql, args),
    batch: (stmts, chunk = 500) => c.batch(stmts, chunk),
  };
}

const SCHEMA = [
  `CREATE TABLE IF NOT EXISTS accounts(username TEXT PRIMARY KEY, total_bytes INTEGER DEFAULT 0, nrepo INTEGER DEFAULT 0)`,
  `CREATE TABLE IF NOT EXISTS repos(id TEXT, kind TEXT, account TEXT, size INTEGER DEFAULT 0, nfile INTEGER DEFAULT 0, PRIMARY KEY(id, kind))`,
  `CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT, kind TEXT, path TEXT, name TEXT, size INTEGER, off TEXT)`,
  `CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, val TEXT)`,
  `CREATE TABLE IF NOT EXISTS stems(stem TEXT PRIMARY KEY, n INTEGER DEFAULT 0, scanned_at INTEGER DEFAULT 0, added INTEGER DEFAULT 0)`,
  `CREATE TABLE IF NOT EXISTS seen_ext(ext TEXT PRIMARY KEY, n INTEGER DEFAULT 0, farm_n INTEGER DEFAULT 0, sample TEXT)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_account ON repos(account)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS idx_files_rp ON files(repo_id, kind, path)`,
  `CREATE INDEX IF NOT EXISTS idx_files_sizename ON files(name, size)`,
];
// Cột bổ sung (DB import từ SQLite đã có; DB mới thì CREATE ở trên thiếu cột -> thêm mềm).
const ADD_COLS = [
  ["accounts", "verified", "INTEGER DEFAULT 0"], ["accounts", "has_id", "INTEGER DEFAULT 0"],
  ["accounts", "nrepo", "INTEGER DEFAULT 0"], ["accounts", "junk", "INTEGER DEFAULT 0"],
  ["accounts", "scanned_at", "INTEGER DEFAULT 0"], ["accounts", "last_seen", "INTEGER DEFAULT 0"],
  ["repos", "last_modified", "INTEGER DEFAULT 0"], ["repos", "state", "TEXT DEFAULT 'new'"],
  ["repos", "has_id", "INTEGER DEFAULT 0"], ["repos", "indexed_at", "INTEGER DEFAULT 0"], ["repos", "due_at", "INTEGER DEFAULT 0"],
  ["repos", "worker", "TEXT"], ["repos", "entries", "INTEGER DEFAULT 0"], ["repos", "sha", "TEXT"],
  ["repos", "updated", "INTEGER DEFAULT 0"], ["repos", "prio", "INTEGER DEFAULT 0"],
  ["repos", "junk", "INTEGER DEFAULT 0"], ["accounts", "next_scan_at", "INTEGER DEFAULT 0"],
];

// Index + trigger + counters phải tạo SAU khi đã có cột (DB import có thể thiếu cột).
const IDX2 = [
  `CREATE TABLE IF NOT EXISTS counters(k TEXT PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_state ON repos(state)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_prio ON repos(state, prio DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_accounts_next ON accounts(next_scan_at)`,
  `CREATE INDEX IF NOT EXISTS idx_accounts_scan ON accounts(junk, scanned_at)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_claim ON repos(state, junk, prio DESC, last_modified DESC)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_due ON repos(junk, state, has_id DESC, due_at ASC)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_deadfiles ON repos(state, nfile)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_junkfiles ON repos(junk, nfile)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_account_junk ON repos(account, junk)`,
];
// Triggers counter ĐÃ BỎ: chúng ghi +4 dòng counters mỗi lần repo đổi state/junk
// (mỗi repo stale đổi state 2 lần -> 8 dòng), là nguồn write lớn nhất. Thay bằng
// COUNT có index khi cần (đọc rẻ hơn ghi rất nhiều trong quota Turso).
const TRG_NAMES = ["trg_rep_ins", "trg_rep_del", "trg_rep_upd", "trg_acc_ins", "trg_acc_del", "trg_acc_upd"];
const DROP_TRG = TRG_NAMES.map((t) => ({ sql: "DROP TRIGGER IF EXISTS " + t }));
// 2 index dư: idx_files_repo bị idx_files_rp(repo_id,kind,path) phủ; idx_files_name
// bị idx_files_sizename(name,size) phủ. Bỏ để mỗi file chỉ còn 2 index (giảm ~40% writes).
const DROP_IDX = [
  { sql: "DROP INDEX IF EXISTS idx_files_name" },
  { sql: "DROP INDEX IF EXISTS idx_files_repo" },
  { sql: "DROP INDEX IF EXISTS idx_repos_stale" },
  { sql: "DROP INDEX IF EXISTS idx_repos_stale_due" },
];

export async function ensureSchema(db) {
  if (await metaGet(db, "schema_ready_v1")) return;
  await db.batch(SCHEMA.map((sql) => ({ sql })));
  for (const [t, c, d] of ADD_COLS) {
    let have = false;
    try { const info = await db.exec(`PRAGMA table_info(${t})`); have = (info.rows || []).some((r) => r.name === c); } catch { }
    if (!have) { try { await db.exec(`ALTER TABLE ${t} ADD COLUMN ${c} ${d}`); } catch { /* đã có cột */ } }
  }
  await db.batch(IDX2.map((sql) => ({ sql })));
  await db.batch([...DROP_TRG, ...DROP_IDX]);
  await db.batch([
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('prio_idx_v1','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('prio_backfill_v3','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('prio_score_v1','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('acct_hasid_v1','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('acct_nrepo_v1','1')" },
    { sql: "INSERT OR REPLACE INTO meta(key,val) VALUES('schema_ready_v1','1')" },
  ]);
  if (!(await metaGet(db, "counters_v1"))) {
    // Backfill 1 lần: đồng bộ repos.junk theo account junk + hẹn lịch quét.
    await db.exec("UPDATE repos SET junk=1 WHERE junk=0 AND account IN (SELECT username FROM accounts WHERE junk=1)");
    await db.exec("UPDATE accounts SET next_scan_at=scanned_at + CASE WHEN junk=1 THEN " + JUNK_RETRY_MS + " WHEN has_id=1 THEN 18000000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE next_scan_at=0");
    await metaSet(db, "counters_v1", "1");
    log("counters_v1 backfill xong");
  }
  // Quét rác 1 lần: account không có phim (has_id=0) và đã index hết (không còn repo 'new')
  // -> junk vĩnh viễn. Sau bước này inv/index chỉ còn làm account thật.
  if (!(await metaGet(db, "junk_sweep_v1"))) {
    const r = await db.exec("UPDATE accounts SET junk=1, next_scan_at=? + " + JUNK_RETRY_MS + " WHERE junk=0 AND has_id=0 AND username NOT IN (SELECT account FROM repos WHERE state='new')", [Date.now()]);
    await db.exec("UPDATE repos SET junk=1 WHERE junk=0 AND account IN (SELECT username FROM accounts WHERE junk=1)");
    await metaSet(db, "junk_sweep_v1", "1");
    log("junk_sweep_v1 xong, account junk +" + r.changes);
  }
  // Đẩy inv chạy ngay: mở lại hạn quét cho mọi account non-junk (1 lần).
  if (!(await metaGet(db, "inv_nudge_v1"))) {
    const r = await db.exec("UPDATE accounts SET next_scan_at=0 WHERE junk=0 AND next_scan_at>0");
    await metaSet(db, "inv_nudge_v1", "1");
    log("inv_nudge_v1 xong, account pending +" + r.changes);
  }
  // Đồng bộ TTL inventory has_id về 6h: tính lại next_scan_at cho account đã quét (1 lần).
  if (!(await metaGet(db, "acct_ttl5h_v1"))) {
    const r = await db.exec("UPDATE accounts SET next_scan_at=scanned_at + CASE WHEN has_id=1 THEN 18000000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE junk=0 AND scanned_at>0");
    await metaSet(db, "acct_ttl5h_v1", "1");
    log("acct_ttl5h_v1 xong, reschedule " + r.changes);
  }
  // due_at_v1: backfill hạn recheck từ indexed_at + TTL (has_id 1h / no_id 24h).
  if (!(await metaGet(db, "due_at_v1"))) {
    const r = await db.exec(
      "UPDATE repos SET due_at = indexed_at + CASE WHEN has_id=0 THEN ? ELSE ? END - (RANDOM() & 2097151) WHERE state='done' AND (due_at IS NULL OR due_at=0)",
      [REPO_TTL_NOID_MS, REPO_TTL_HAS_MS]
    );
    await metaSet(db, "due_at_v1", "1");
    log("due_at_v1 backfill " + r.changes);
  }
}

export async function metaGet(db, k) { const r = await db.exec("SELECT val FROM meta WHERE key=?", [k]); return r.rows[0] ? r.rows[0].val : null; }
export async function metaSet(db, k, v) { await db.exec("INSERT OR REPLACE INTO meta(key,val) VALUES(?,?)", [k, String(v)]); }

export async function storeTree(db, repoId, kind, files, off, isNew = false) {
  const want = new Map(), extMap = new Map();
  let n = 0, size = 0, hasPhim = false;
  for (const f of files) {
    if (f.type && f.type !== "file") continue;
    const p = String(f.path || f.name || "");
    const bn = p.split("/").pop();
    const ip = idParts(bn);
    if (!ip.ok) continue;
    const e = extMap.get(ip.ext) || [0, bn]; e[0]++; extMap.set(ip.ext, e);
    if (!PHIM_EXT.has(ip.ext)) continue;
    hasPhim = true;
    const sz = f.size || 0;
    const oid = f.oid || f.xetHash || off || null;
    want.set(p, [bn, sz, oid]);
    n++; size += sz;
  }
  // Chỉ ghi phần thay đổi: nếu là repo mới tinh (isNew) thì không cần SELECT đối chiếu.
  const cur = isNew ? { rows: [] } : await db.exec("SELECT path,name,size,off FROM files WHERE repo_id=? AND kind=?", [repoId, kind]);
  const have = new Map();
  for (const r of cur.rows) have.set(r.path, [r.name, r.size, r.off]);
  const existing = have.size;
  const stmts = [];
  let del = 0, ins = 0, upd = 0;
  for (const [p, [bn, sz, oid]] of want) {
    const h = have.get(p);
    if (!h) {
      stmts.push({ sql: "INSERT OR REPLACE INTO files(repo_id,kind,path,name,size,off) VALUES(?,?,?,?,?,?)", args: [repoId, kind, p, bn, sz, oid] });
      ins++;
      continue;
    }
    have.delete(p);
    if (h[0] !== bn || h[1] !== sz || (h[2] ?? null) !== (oid ?? null)) {
      stmts.push({ sql: "UPDATE files SET name=?,size=?,off=? WHERE repo_id=? AND kind=? AND path=?", args: [bn, sz, oid, repoId, kind, p] });
      upd++;
    }
  }
  for (const p of have.keys()) {
    stmts.push({ sql: "DELETE FROM files WHERE repo_id=? AND kind=? AND path=?", args: [repoId, kind, p] });
    del++;
  }
  const changed = del + ins + upd;
  if (changed && changed * 2 > n + existing) {
    // Đổi quá nửa: full rewrite rẻ hơn nhiều câu lẻ.
    const full = isNew ? [] : [{ sql: "DELETE FROM files WHERE repo_id=? AND kind=?", args: [repoId, kind] }];
    for (const [p, [bn, sz, oid]] of want) full.push({ sql: "INSERT OR REPLACE INTO files(repo_id,kind,path,name,size,off) VALUES(?,?,?,?,?,?)", args: [repoId, kind, p, bn, sz, oid] });
    if (full.length) await db.batch(full, 100);
    ins = n; del = existing; upd = 0;
  } else if (stmts.length) {
    await db.batch(stmts, 100);
  }
  await noteExts(db, extMap, hasPhim);
  return { n, size, del, ins, upd };
}

async function noteExts(db, extMap, hasPhim) {
  // seen_ext chỉ dùng cho phân tích offline pipeline.mjs, bỏ qua trong runtime crawler để tiết kiệm Turso writes.
  return;
}

export async function repoCounts(db, id, kind) {
  const r = await db.exec("SELECT COUNT(*) c, COALESCE(SUM(size),0) s FROM files WHERE repo_id=? AND kind=?", [id, kind]);
  const x = r.rows[0] || { c: 0, s: 0 };
  return { n: x.c, size: x.s };
}

// state 'done' = repo stale (claim trong bộ nhớ, không qua 'fetching').
export async function finishRepo(db, id, kind, { n, size, dead, entries, sha, updated }) {
  if (staleSeen.delete(kind + ":" + id)) staleDone.add(kind + ":" + id);
  if (dead) {
    await db.exec("DELETE FROM files WHERE repo_id=? AND kind=?", [id, kind]);
    await db.exec("UPDATE repos SET state='dead', has_id=0, nfile=0, size=0, indexed_at=?, due_at=0 WHERE id=? AND kind=? AND state IN ('fetching','done')", [Date.now(), id, kind]);
  } else {
    const hasId = n > 0 ? 1 : 0;
    const due = dueAt(hasId, updated || 0, kind + ":" + id);
    await db.exec(
      "UPDATE repos SET state='done', has_id=?, nfile=?, size=?, entries=?, indexed_at=?, due_at=?, worker=NULL, sha=COALESCE(?, sha), updated=COALESCE(?, updated) WHERE id=? AND kind=? AND state IN ('fetching','done')",
      [hasId, n, size, entries || 0, Date.now(), due, sha ?? null, updated ?? null, id, kind]);
  }
}

// Chỉ cập nhật mốc recheck khi SHA/updated không đổi — giữ nguyên nfile/size/entries.
// Turso tính write theo dòng + mỗi index bị đổi: repo stale chỉ SET indexed_at/sha/updated
// (không đụng state/worker -> 2 index thay vì 6) và gom batch, flush bằng flushTouches().
const touchBuf = [];
const TOUCH_BATCH = 100;
let flushingTouches = false;
// Hạn recheck thích ứng, tính từ cột has_id/updated của dòng hiện tại + jitter tất định.
// Hạn recheck: has_id=1 1h, has_id=0 24h + jitter tất định.
const DUE_SQL = "? + CASE WHEN has_id=0 THEN ? ELSE ? END - ?";
export async function touchRepo(db, id, kind, sha, updated) {
  const block = Math.floor(Date.now() / 300000) * 300000;
  const jit = h32(kind + ":" + id) % JITTER_MS;
  const dueArgs = [block, block, REPO_TTL_NOID_MS, REPO_TTL_HAS_MS, jit];
  const key = kind + ":" + id;
  if (staleSeen.delete(key)) {
    staleDone.add(key);
    touchBuf.push({ sql: `UPDATE repos SET indexed_at=?, due_at=${DUE_SQL}, sha=COALESCE(?, sha), updated=COALESCE(?, updated) WHERE id=? AND kind=? AND state='done'`, args: [...dueArgs, sha ?? null, updated ?? null, id, kind] });
    if (touchBuf.length >= TOUCH_BATCH) await flushTouches(db);
    return 1;
  }
  const r = await db.exec(`UPDATE repos SET state='done', indexed_at=?, due_at=${DUE_SQL}, worker=NULL, sha=COALESCE(?, sha), updated=COALESCE(?, updated) WHERE id=? AND kind=? AND state='fetching'`, [...dueArgs, sha ?? null, updated ?? null, id, kind]);
  return r.changes;
}

export async function flushTouches(db) {
  if (!touchBuf.length || flushingTouches) return 0;
  flushingTouches = true;
  const st = touchBuf.splice(0);
  try {
    await db.batch(st, 50);
    return st.length;
  } catch (e) {
    // Không unshift lại toàn bộ để tránh đầu độc buffer gây loop timeout; repo nào sót sẽ được touch ở run sau.
    return 0;
  } finally {
    flushingTouches = false;
  }
}

export async function unclaimRepo(db, id, kind) {
  await db.exec("UPDATE repos SET state='new', worker=NULL WHERE id=? AND kind=? AND state='fetching'", [id, kind]);
}

export async function recoverStale(db, ms = 300000) {
  const r = await db.exec("UPDATE repos SET state='new', worker=NULL WHERE state='fetching' AND indexed_at<?", [Date.now() - ms]);
  if (r.changes) log(`recoverStale repos=${r.changes}`);
  return r.changes;
}

let noReturning = false;
export async function claimRepo(db, worker, kind = null) {
  // 1 câu UPDATE...RETURNING thay cho SELECT+UPDATE (bớt 1 read, atomic, hết race).
  const where = kind
    ? "state='new' AND kind=? AND junk=0"
    : "state='new' AND kind IN ('dataset','bucket') AND junk=0";
  const params = kind ? [worker, Date.now(), kind] : [worker, Date.now()];
  if (!noReturning) {
    try {
      const r = await db.exec(
        `UPDATE repos SET state='fetching', worker=?, indexed_at=? WHERE rowid=(SELECT rowid FROM repos WHERE ${where} ORDER BY prio DESC, last_modified DESC, rowid LIMIT 1) AND state='new' RETURNING id, account, kind`,
        params
      );
      return r.rows[0] || null;
    } catch { noReturning = true; }
  }
  // Turso cũ không hỗ trợ RETURNING -> fallback như trước.
  for (let i = 0; i < 50; i++) {
    const sql = kind
      ? "SELECT r.id,r.account,r.kind FROM repos r WHERE r.state='new' AND r.kind=? AND r.junk=0 ORDER BY r.prio DESC, r.last_modified DESC, r.rowid LIMIT 1"
      : "SELECT r.id,r.account,r.kind FROM repos r WHERE r.state='new' AND r.kind IN ('dataset','bucket') AND r.junk=0 ORDER BY r.prio DESC, r.last_modified DESC, r.rowid LIMIT 1";
    const q = await db.exec(sql, kind ? [kind] : []);
    const row = q.rows[0];
    if (!row) return null;
    const u = await db.exec("UPDATE repos SET state='fetching', worker=?, indexed_at=? WHERE id=? AND kind=? AND state='new'", [worker, Date.now(), row.id, row.kind]);
    if (u.changes) return row;
  }
  return null;
}

export const lmSeen = new Map(); // kind:id -> lastModified đã xử lý trong run
export const accCache = new Map();

export async function preloadBucketMeta(db) {
  try {
    const r = await db.exec("SELECT id, last_modified FROM repos WHERE kind='bucket' AND state='done'");
    for (const row of r.rows) {
      if (row.last_modified != null) {
        lmSeen.set("bucket:" + row.id, Number(row.last_modified) || 0);
      }
    }
    return r.rows.length;
  } catch {
    return 0;
  }
}

export async function upsertRepoMeta(db, id, account, kind, lastModified) {
  const lm = lastModified ? Date.parse(lastModified) || 0 : 0;
  // Cùng repo xuất hiện lại trong run (feed trùng, bucket sweep #2, feed chồng
  // bucket) với cùng lastModified thì kết quả không đổi -> bỏ lượt đọc PK.
  const seenKey = kind + ":" + id;
  if (lmSeen.get(seenKey) === lm) return false;
  if (lmSeen.size > 100000) lmSeen.clear();
  lmSeen.set(seenKey, lm);
  if (!accCache.has(account)) {
    const a = await db.exec("SELECT has_id,nrepo,junk FROM accounts WHERE username=?", [account]);
    accCache.set(account, a.rows[0] || {});
    if (accCache.size > 5000) accCache.clear();
  }
  const acc = accCache.get(account) || {};
  const prio = repoScore(id, account, kind, acc);
  const ex = (await db.exec("SELECT last_modified,state,prio FROM repos WHERE id=? AND kind=?", [id, kind])).rows[0];
  if (!ex) {
    await db.exec("INSERT INTO repos(id,kind,account,last_modified,state,junk,prio) VALUES(?,?,?,?,'new',?,?)", [id, kind, account, lm, acc.junk ? 1 : 0, prio]);
    await db.exec("UPDATE accounts SET nrepo=nrepo+1 WHERE username=?", [account]);
    return true;
  }
  if (lm > (ex.last_modified || 0) || ex.state === "dead") {
    // Repo từng bị đánh dead nhưng vẫn xuất hiện trong listing -> sống lại, đưa vào hàng đợi index.
    // Không hạ state 'fetching' về 'new' khi worker đang xử lý (tránh race condition nhiều worker cùng cào).
    const newState = ex.state === "fetching" ? "fetching" : "new";
    await db.exec("UPDATE repos SET last_modified=CASE WHEN ? > COALESCE(last_modified,0) THEN ? ELSE last_modified END, state=?, account=?, junk=?, prio=? WHERE id=? AND kind=?", [lm, lm, newState, account, acc.junk ? 1 : 0, prio, id, kind]);
    return newState === "new";
  }
  if (ex.state === "new" && prio > (ex.prio || 0)) await db.exec("UPDATE repos SET prio=? WHERE id=? AND kind=?", [prio, id, kind]);
  return ex.state === "new";
}

const verifiedAccounts = new WeakMap();
function verifiedSet(db) {
  let s = verifiedAccounts.get(db);
  if (!s) { s = new Set(); verifiedAccounts.set(db, s); }
  return s;
}

export async function reevalAccount(db, u) {
  const seen = verifiedSet(db);
  if (seen.has(u)) return;
  const acc = (await db.exec("SELECT has_id,verified FROM accounts WHERE username=?", [u])).rows[0];
  if (acc && acc.has_id) {
    if (!acc.verified) await db.exec("UPDATE accounts SET verified=1 WHERE username=?", [u]);
    seen.add(u);
    return;
  }
  const hit = await db.exec("SELECT 1 x FROM repos WHERE account=? AND has_id=1 LIMIT 1", [u]);
  if (hit.rows.length) {
    await db.exec("UPDATE accounts SET verified=1, has_id=1, next_scan_at=scanned_at+18000000 WHERE username=?", [u]);
    const a = (await db.exec("SELECT has_id,nrepo,junk FROM accounts WHERE username=?", [u])).rows[0] || {};
    const rs = (await db.exec("SELECT id,kind FROM repos WHERE account=?", [u])).rows;
    const st = rs.map((r) => ({ sql: "UPDATE repos SET prio=? WHERE id=? AND kind=?", args: [repoScore(r.id, u, r.kind, a), r.id, r.kind] }));
    if (st.length) await db.batch(st, 500);
    seen.add(u);
    return;
  }
  // Account rác = không có file id nào (has_id=0) VÀ đã index hết (không còn repo 'new'/'fetching').
  const pendingWork = await db.exec("SELECT 1 x FROM repos WHERE account=? AND state IN ('new','fetching') LIMIT 1", [u]);
  if (!pendingWork.rows.length) await markJunk(db, u);
}

// Đánh dấu account rác: ẩn mọi repo của nó, đặt TTL ~1 năm (thực chất là vĩnh viễn).
export async function markJunk(db, u) {
  const now = Date.now();
  const r = await db.exec("UPDATE accounts SET junk=1, scanned_at=?, next_scan_at=? + " + JUNK_RETRY_MS + " WHERE username=?", [now, now, u]);
  await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=1 WHERE account=? AND junk=0", [u]);
  return r.changes;
}

export async function applyGitDiff(db, repoId, kind, diff) {
  const lines = String(diff).split("\n");
  const ops = []; let cur = null;
  const unq = (p) => { p = String(p); if (p.startsWith('"') && p.endsWith('"')) { try { return JSON.parse(p); } catch { } } return p; };
  const push = () => { if (cur) ops.push(cur); cur = null; };
  for (const ln of lines) {
    if (ln.startsWith("diff --git ")) {
      push();
      const m = ln.slice(11).match(/^a\/(.*?) b\/(.*)$/);
      cur = { status: "mod", a: m ? unq(m[1]) : null, b: m ? unq(m[2]) : null, oid: null, size: null };
      continue;
    }
    if (!cur) continue;
    if (ln.startsWith("new file mode")) { cur.status = "add"; continue; }
    if (ln.startsWith("deleted file mode")) { cur.status = "del"; continue; }
    if (ln.startsWith("rename from ") || ln.startsWith("copy from ")) { cur.status = "ren"; cur.a = unq(ln.slice(12)); continue; }
    if (ln.startsWith("rename to ") || ln.startsWith("copy to ")) { cur.status = "ren"; cur.b = unq(ln.slice(10)); continue; }
    if (ln.startsWith("Binary files ")) { cur.bin = true; continue; }
    if (ln.startsWith("--- ") && ln.slice(4) !== "/dev/null") { if (!cur.a) cur.a = unq(ln.slice(4).replace(/^a\//, "")); continue; }
    if (ln.startsWith("+++ ") && ln.slice(4) !== "/dev/null") { if (!cur.b) cur.b = unq(ln.slice(4).replace(/^b\//, "")); continue; }
    const ch0 = ln[0];
    if (ch0 === "+" || ch0 === "-" || ch0 === " ") {
      const body = ln.slice(1);
      if (body.startsWith("oid sha256:")) cur.oid = body.slice(11).trim();
      else if (body.startsWith("size ")) cur.size = parseInt(body.slice(5), 10);
    }
  }
  push();
  const dels = [], ups = [], extMap = new Map();
  const track = (nm) => { if (!nm) return; const bn = nm.split("/").pop(); const ip = idParts(bn); if (ip.ok) { const e = extMap.get(ip.ext) || [0, bn]; e[0]++; extMap.set(ip.ext, e); } };
  for (const o of ops) {
    const A = o.a, B = o.b || o.a;
    if (!A && !B) return { ok: false, reason: "unparsed" };
    track(A); track(B);
    const idA = A ? isId(A.split("/").pop()) : false;
    const idB = B ? isId(B.split("/").pop()) : false;
    if (o.status === "ren") { if (idA || idB) return { ok: false, reason: "rename" }; continue; }
    if (o.status === "del") { if (idA) dels.push(A); continue; }
    if (!idB) continue;
    if (o.bin || o.size == null) return { ok: false, reason: "no-size:" + B };
    ups.push([B, B.split("/").pop(), o.size, o.oid]);
  }
  const st = [];
  for (const p of dels) st.push({ sql: "DELETE FROM files WHERE repo_id=? AND kind=? AND path=?", args: [repoId, kind, p] });
  for (const [p, bn, sz, oid] of ups) {
    st.push({ sql: "UPDATE files SET name=?,size=?,off=? WHERE repo_id=? AND kind=? AND path=?", args: [bn, sz, oid, repoId, kind, p] });
    st.push({
      sql: "INSERT INTO files(repo_id,kind,path,name,size,off) SELECT ?,?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM files WHERE repo_id=? AND kind=? AND path=?)",
      args: [repoId, kind, p, bn, sz, oid, repoId, kind, p],
    });
  }
  if (st.length) await db.batch(st, 500);
  const cnt = await repoCounts(db, repoId, kind);
  await noteExts(db, extMap, cnt.n > 0 || [...extMap.keys()].some((e) => PHIM_EXT.has(e)));
  return { ok: true, dels: dels.length, ups: ups.length };
}

// ---------- helper cho auto.mjs (không dùng SQL trực tiếp) ----------
export async function getAccount(db, u) {
  return (await db.exec("SELECT has_id,junk,nrepo FROM accounts WHERE username=?", [u])).rows[0] || null;
}
export async function insertAccount(db, u) {
  const r = await db.exec("INSERT OR IGNORE INTO accounts(username,last_seen) VALUES(?,?)", [u, Date.now()]);
  return r.changes;
}
export const JUNK_RETRY_MS = 31536000000; // account junk gan nhu vinh vien (1 nam) -> khong lo inv/index
export const TTL_EXPR = "CASE WHEN junk=1 THEN " + JUNK_RETRY_MS + " WHEN has_id=1 THEN 18000000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END";
export async function setScanned(db, u) {
  const now = Date.now();
  await db.exec("UPDATE accounts SET scanned_at=?, next_scan_at=? + (" + TTL_EXPR + ") WHERE username=?", [now, now, u]);
}
// Adaptive: TTL đã "nướng" sẵn vào next_scan_at lúc quét -> chỉ cần index range (next_scan_at).
export const PENDING_ROW = "SELECT username,has_id,nrepo,junk,scanned_at FROM accounts WHERE ";
export async function pendingAccounts(db, cut, adaptive = false) {
  const sql = adaptive
    ? PENDING_ROW + "next_scan_at<=? ORDER BY next_scan_at LIMIT 1000"
    : PENDING_ROW + "junk=0 AND scanned_at<=? ORDER BY scanned_at LIMIT 1000";
  return (await db.exec(sql, [cut])).rows;
}
export async function countPending(db, cut, adaptive = false) {
  const sql = adaptive
    ? "SELECT COUNT(*) c FROM accounts WHERE next_scan_at<=?"
    : "SELECT COUNT(*) c FROM accounts WHERE junk=0 AND scanned_at<=?";
  return (await db.exec(sql, [cut])).rows[0].c;
}
export async function countStaleHasId(db, now) {
  const r = await db.exec("SELECT COUNT(*) c FROM repos WHERE state='done' AND junk=0 AND has_id=1 AND due_at<=?", [now]);
  return r.rows[0]?.c || 0;
}
// has_id=0 thường chiếm ~85k dòng (không có file) -> COUNT đầy đủ rất tốn read.
// Chỉ cần biết "còn nhiều hay không": cap 10k dòng, quét tối đa 10k thay vì 85k.
export async function countStaleNoId(db, now, cap = 10000) {
  const r = await db.exec("SELECT COUNT(*) c FROM (SELECT 1 FROM repos WHERE state='done' AND junk=0 AND has_id=0 AND due_at<=? LIMIT ?)", [now, cap]);
  return r.rows[0]?.c || 0;
}
export async function countStale(db, cutHasId, cutNoId) {
  return (await countStaleHasId(db, cutHasId)) + (await countStaleNoId(db, cutNoId));
}
// Claim stale trong bộ nhớ (workflow crawl có concurrency group -> chỉ 1 crawler),
// không ghi state='fetching'. Đọc theo lô; flush touch trước để lô mới không lặp repo vừa xử lý.
const staleSeen = new Set();
const staleDone = new Set();
const staleQ = new Map();
const staleFill = new Map();
const staleCursor = new Map();
const staleAtEnd = new Map();
const STALE_BATCH = 200;

export function resetStaleQueue(hasId = null) {
  if (hasId === null) {
    staleCursor.clear();
    staleAtEnd.clear();
    staleQ.clear();
  } else {
    const qk = String(hasId);
    staleCursor.delete(qk);
    staleAtEnd.delete(qk);
    staleQ.delete(qk);
  }
}

export async function claimRepoStale(db, worker, cut, hasId = null) {
  const qk = String(hasId);
  while (true) {
    let q = staleQ.get(qk);
    if (!q || !q.length) {
      if (staleAtEnd.get(qk)) {
        return null;
      }
      if (!staleFill.has(qk)) staleFill.set(qk, (async () => {
        try { await flushTouches(db); } catch { }
        let sql = "SELECT r.id,r.account,r.kind,r.due_at FROM repos r WHERE r.state='done' AND r.junk=0 AND r.due_at<=?";
        const params = [cut];
        if (hasId !== null) {
          sql += " AND r.has_id=?";
          params.push(hasId ? 1 : 0);
        }
        const cursor = staleCursor.get(qk);
        if (cursor) {
          sql += " AND (r.due_at > ? OR (r.due_at = ? AND r.id > ?))";
          params.push(cursor.due, cursor.due, cursor.id);
        }
        sql += " ORDER BY r.due_at ASC, r.id ASC LIMIT " + STALE_BATCH;
        const rows = (await db.exec(sql, params)).rows;
        if (rows.length) {
          const last = rows[rows.length - 1];
          staleCursor.set(qk, { due: last.due_at, id: last.id });
        }
        if (rows.length < STALE_BATCH) {
          staleAtEnd.set(qk, true);
        }
        staleQ.set(qk, rows);
      })().finally(() => staleFill.delete(qk)));
      await staleFill.get(qk);
      q = staleQ.get(qk) || [];
    }

    while (q.length) {
      const row = q.shift();
      const key = row.kind + ":" + row.id;
      if (staleSeen.has(key) || staleDone.has(key)) continue;
      staleSeen.add(key);
      return row;
    }

    if (staleAtEnd.get(qk)) {
      return null;
    }
  }
}
export function releaseStale(db, id, kind) {
  staleSeen.delete(kind + ":" + id);
}
export async function clearJunk(db, u) {
  const r = await db.exec("UPDATE accounts SET junk=0, next_scan_at=scanned_at + CASE WHEN has_id=1 THEN 18000000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE username=?", [u]);
  await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=0 WHERE account=? AND junk=1", [u]);
  return r.changes;
}
export async function getRepoPrev(db, id, kind) {
  return (await db.exec("SELECT sha,updated,nfile FROM repos WHERE id=? AND kind=?", [id, kind])).rows[0] || {};
}
export async function countNew(db) {
  const r = await db.exec("SELECT COUNT(*) c FROM repos WHERE state='new' AND junk=0");
  return r.rows[0] ? r.rows[0].c : 0;
}
export async function stateCounts(db) {
  return (await db.exec("SELECT state, COUNT(*) c FROM repos GROUP BY state")).rows;
}
export async function accountCounts(db) {
  const r = await db.exec("SELECT COUNT(*) c FROM accounts WHERE junk=0");
  return r.rows[0] ? r.rows[0].c : 0;
}

export async function pendingAccountsAdaptive(db, now) { return pendingAccounts(db, now, true); }
export async function countPendingAdaptive(db, now) { return countPending(db, now, true); }
