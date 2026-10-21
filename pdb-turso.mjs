// pdb-turso.mjs — bản async của pdb.mjs cho Turso (crawler trên GitHub Actions).
// Cùng API, thêm helper cho auto.mjs. P4K_TURSO_LOCAL=1 -> chạy thử trên SQLite local.
import { tursoClient } from "./turso-http.mjs";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);

export const IDF = /^[A-Z0-9]{10,17}$/;
export const PHIM_EXT = new Set([
  ".h5", ".hdf5", ".pb", ".arrow", ".model", ".pth", ".safetensors",
  ".pt", ".ckpt", ".weights", ".tflite", ".onnx", ".keras", ".bin", ".pkl",
  ".joblib", ".gguf", ".nemo", ".mar", ".msgpack", ".mlmodel", ".h5mu", ".params", ".param",
  ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".ts", ".flv", ".wmv", ".mpg", ".mpeg", ".3gp",
]);
export const JUNK_EMPTY = parseInt(process.env.P4K_JUNK_EMPTY || "3", 10);
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
export const log = (m) => process.stderr.write(`[${new Date().toISOString().slice(11, 19)}] ${m}\n`);
export async function retrySync(fn) { return await fn(); }

export function isId(name) {
  const b = String(name || "").split("/").pop();
  const m = b.match(/\.[^.]+$/);
  if (!m || !PHIM_EXT.has(m[0].toLowerCase())) return false;
  return IDF.test(b.slice(0, b.length - m[0].length));
}
export function idParts(name) {
  const bn = String(name || "").split("/").pop();
  const m = bn.match(/\.[^.]+$/);
  const ext = m ? m[0].toLowerCase() : "(no-ext)";
  const base = m ? bn.slice(0, bn.length - m[0].length) : bn;
  return { ext, base, ok: IDF.test(base) };
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
  `CREATE INDEX IF NOT EXISTS idx_files_name ON files(name)`,
  `CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id)`,
  `CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, val TEXT)`,
  `CREATE TABLE IF NOT EXISTS stems(stem TEXT PRIMARY KEY, n INTEGER DEFAULT 0, scanned_at INTEGER DEFAULT 0, added INTEGER DEFAULT 0)`,
  `CREATE TABLE IF NOT EXISTS seen_ext(ext TEXT PRIMARY KEY, n INTEGER DEFAULT 0, farm_n INTEGER DEFAULT 0, sample TEXT)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_account ON repos(account)`,
  `CREATE INDEX IF NOT EXISTS idx_files_rp ON files(repo_id, kind, path)`,
  `CREATE INDEX IF NOT EXISTS idx_files_sizename ON files(name, size)`,
];
// Cột bổ sung (DB import từ SQLite đã có; DB mới thì CREATE ở trên thiếu cột -> thêm mềm).
const ADD_COLS = [
  ["accounts", "verified", "INTEGER DEFAULT 0"], ["accounts", "has_id", "INTEGER DEFAULT 0"],
  ["accounts", "nrepo", "INTEGER DEFAULT 0"], ["accounts", "junk", "INTEGER DEFAULT 0"],
  ["accounts", "scanned_at", "INTEGER DEFAULT 0"], ["accounts", "last_seen", "INTEGER DEFAULT 0"],
  ["repos", "last_modified", "INTEGER DEFAULT 0"], ["repos", "state", "TEXT DEFAULT 'new'"],
  ["repos", "has_id", "INTEGER DEFAULT 0"], ["repos", "indexed_at", "INTEGER DEFAULT 0"],
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
  `CREATE INDEX IF NOT EXISTS idx_repos_stale ON repos(junk, state, has_id DESC, indexed_at ASC)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_deadfiles ON repos(state, nfile)`,
  `CREATE INDEX IF NOT EXISTS idx_repos_junkfiles ON repos(junk, nfile)`,
];
const TRG = [
  `CREATE TRIGGER IF NOT EXISTS trg_rep_ins AFTER INSERT ON repos BEGIN
     INSERT INTO counters(k,v) VALUES('rst:'||NEW.state,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
     INSERT INTO counters(k,v) VALUES('rsj:'||NEW.state||':'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
   END`,
  `CREATE TRIGGER IF NOT EXISTS trg_rep_del AFTER DELETE ON repos BEGIN
     INSERT INTO counters(k,v) VALUES('rst:'||OLD.state,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
     INSERT INTO counters(k,v) VALUES('rsj:'||OLD.state||':'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
   END`,
  `CREATE TRIGGER IF NOT EXISTS trg_rep_upd AFTER UPDATE ON repos WHEN NEW.state<>OLD.state OR NEW.junk<>OLD.junk BEGIN
     INSERT INTO counters(k,v) VALUES('rst:'||OLD.state,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
     INSERT INTO counters(k,v) VALUES('rsj:'||OLD.state||':'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
     INSERT INTO counters(k,v) VALUES('rst:'||NEW.state,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
     INSERT INTO counters(k,v) VALUES('rsj:'||NEW.state||':'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
   END`,
  `CREATE TRIGGER IF NOT EXISTS trg_acc_ins AFTER INSERT ON accounts BEGIN
     INSERT INTO counters(k,v) VALUES('acct:t',1) ON CONFLICT(k) DO UPDATE SET v=v+1;
     INSERT INTO counters(k,v) VALUES('acjt:'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
   END`,
  `CREATE TRIGGER IF NOT EXISTS trg_acc_del AFTER DELETE ON accounts BEGIN
     INSERT INTO counters(k,v) VALUES('acct:t',-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
     INSERT INTO counters(k,v) VALUES('acjt:'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
   END`,
  `CREATE TRIGGER IF NOT EXISTS trg_acc_upd AFTER UPDATE ON accounts WHEN NEW.junk<>OLD.junk BEGIN
     INSERT INTO counters(k,v) VALUES('acjt:'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
     INSERT INTO counters(k,v) VALUES('acjt:'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
   END`,
];

export async function ensureSchema(db) {
  await db.batch(SCHEMA.map((sql) => ({ sql })));
  for (const [t, c, d] of ADD_COLS) {
    let have = false;
    try { const info = await db.exec(`PRAGMA table_info(${t})`); have = (info.rows || []).some((r) => r.name === c); } catch { }
    if (!have) { try { await db.exec(`ALTER TABLE ${t} ADD COLUMN ${c} ${d}`); } catch { /* đã có cột */ } }
  }
  await db.batch(IDX2.map((sql) => ({ sql })));
  await db.batch(TRG.map((sql) => ({ sql })));
  await db.batch([
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('prio_idx_v1','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('prio_backfill_v3','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('prio_score_v1','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('acct_hasid_v1','1')" },
    { sql: "INSERT OR IGNORE INTO meta(key,val) VALUES('acct_nrepo_v1','1')" },
  ]);
  if (!(await metaGet(db, "counters_v1"))) {
    // Backfill 1 lần: đồng bộ repos.junk theo account junk, rồi chốt lại counters cho khớp tuyệt đối.
    await db.exec("UPDATE repos SET junk=1 WHERE junk=0 AND account IN (SELECT username FROM accounts WHERE junk=1)");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rst:'||state,COUNT(*) FROM repos GROUP BY state");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rsj:'||state||':'||junk,COUNT(*) FROM repos GROUP BY state,junk");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'acct:t',COUNT(*) FROM accounts");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'acjt:'||junk,COUNT(*) FROM accounts GROUP BY junk");
    await db.exec("UPDATE accounts SET next_scan_at=scanned_at + CASE WHEN junk=1 THEN " + JUNK_RETRY_MS + " WHEN has_id=1 THEN 18000000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE next_scan_at=0");
    await metaSet(db, "counters_v1", "1");
    log("counters_v1 backfill xong");
  }
  // Quét rác 1 lần: account không có phim (has_id=0) và đã index hết (không còn repo 'new')
  // -> junk vĩnh viễn. Sau bước này inv/index chỉ còn làm account thật.
  if (!(await metaGet(db, "junk_sweep_v1"))) {
    // Tắt trigger tạm thời: UPDATE hàng loạt nhanh, rồi dựng lại counters cho khớp.
    await db.batch(["trg_rep_ins", "trg_rep_del", "trg_rep_upd", "trg_acc_ins", "trg_acc_del", "trg_acc_upd"].map((t) => ({ sql: "DROP TRIGGER IF EXISTS " + t })));
    const r = await db.exec("UPDATE accounts SET junk=1, next_scan_at=? + " + JUNK_RETRY_MS + " WHERE junk=0 AND has_id=0 AND username NOT IN (SELECT account FROM repos WHERE state='new')", [Date.now()]);
    await db.exec("UPDATE repos SET junk=1 WHERE junk=0 AND account IN (SELECT username FROM accounts WHERE junk=1)");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'acjt:'||junk, COUNT(*) FROM accounts GROUP BY junk");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) VALUES('acct:t',(SELECT COUNT(*) FROM accounts))");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rsj:'||state||':'||junk, COUNT(*) FROM repos GROUP BY state,junk");
    await db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rst:'||state, COUNT(*) FROM repos GROUP BY state");
    await db.batch(TRG.map((sql) => ({ sql })));
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
}

export async function metaGet(db, k) { const r = await db.exec("SELECT val FROM meta WHERE key=?", [k]); return r.rows[0] ? r.rows[0].val : null; }
export async function metaSet(db, k, v) { await db.exec("INSERT OR REPLACE INTO meta(key,val) VALUES(?,?)", [k, String(v)]); }

export async function storeTree(db, repoId, kind, files, off) {
  const ins = [], extMap = new Map();
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
    ins.push({ sql: "INSERT INTO files(repo_id,kind,path,name,size,off) VALUES(?,?,?,?,?,?)", args: [repoId, kind, p, bn, sz, oid] });
    n++; size += sz;
  }
  const stmts = [{ sql: "DELETE FROM files WHERE repo_id=? AND kind=?", args: [repoId, kind] }, ...ins];
  await db.batch(stmts, 500);
  await noteExts(db, extMap, hasPhim);
  return { n, size };
}

async function noteExts(db, extMap, hasPhim) {
  if (!extMap || extMap.size === 0) return;
  const st = [];
  for (const [ext, [c, sample]] of extMap) st.push({
    sql: "INSERT INTO seen_ext(ext,n,farm_n,sample) VALUES(?,?,?,?) ON CONFLICT(ext) DO UPDATE SET n=n+excluded.n, farm_n=farm_n+excluded.farm_n",
    args: [ext, c, hasPhim ? c : 0, sample],
  });
  await db.batch(st, 500);
}

export async function repoCounts(db, id, kind) {
  const r = await db.exec("SELECT COUNT(*) c, COALESCE(SUM(size),0) s FROM files WHERE repo_id=? AND kind=?", [id, kind]);
  const x = r.rows[0] || { c: 0, s: 0 };
  return { n: x.c, size: x.s };
}

export async function finishRepo(db, id, kind, { n, size, dead, entries, sha, updated }) {
  if (dead) {
    await db.exec("DELETE FROM files WHERE repo_id=? AND kind=?", [id, kind]);
    await db.exec("UPDATE repos SET state='dead', has_id=0, nfile=0, size=0, indexed_at=? WHERE id=? AND kind=? AND state='fetching'", [Date.now(), id, kind]);
  }
  else await db.exec(
    "UPDATE repos SET state='done', has_id=?, nfile=?, size=?, entries=?, indexed_at=?, worker=NULL, sha=COALESCE(?, sha), updated=COALESCE(?, updated) WHERE id=? AND kind=? AND state='fetching'",
    [n > 0 ? 1 : 0, n, size, entries || 0, Date.now(), sha ?? null, updated ?? null, id, kind]);
}

// Chỉ cập nhật mốc recheck khi SHA/updated không đổi — giữ nguyên nfile/size/entries.
export async function touchRepo(db, id, kind, sha, updated) {
  const r = await db.exec("UPDATE repos SET state='done', indexed_at=?, worker=NULL, sha=COALESCE(?, sha), updated=COALESCE(?, updated) WHERE id=? AND kind=? AND state='fetching'", [Date.now(), sha ?? null, updated ?? null, id, kind]);
  return r.changes;
}

export async function unclaimRepo(db, id, kind) {
  await db.exec("UPDATE repos SET state='new', worker=NULL WHERE id=? AND kind=? AND state='fetching'", [id, kind]);
}

export async function recoverStale(db, ms = 300000) {
  const r = await db.exec("UPDATE repos SET state='new', worker=NULL WHERE state='fetching' AND indexed_at<?", [Date.now() - ms]);
  if (r.changes) log(`recoverStale repos=${r.changes}`);
  return r.changes;
}

export async function claimRepo(db, worker, kind = null) {
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

export async function upsertRepoMeta(db, id, account, kind, lastModified) {
  const lm = lastModified ? Date.parse(lastModified) || 0 : 0;
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
    await db.exec("UPDATE repos SET last_modified=CASE WHEN ? > COALESCE(last_modified,0) THEN ? ELSE last_modified END, state='new', account=?, junk=?, prio=? WHERE id=? AND kind=?", [lm, lm, account, acc.junk ? 1 : 0, prio, id, kind]);
    return true;
  }
  if (ex.state === "new" && prio > (ex.prio || 0)) await db.exec("UPDATE repos SET prio=? WHERE id=? AND kind=?", [prio, id, kind]);
  return ex.state === "new";
}
const accCache = new Map();

export async function reevalAccount(db, u) {
  const hit = await db.exec("SELECT 1 x FROM repos WHERE account=? AND has_id=1 LIMIT 1", [u]);
  if (hit.rows.length) {
    await db.exec("UPDATE accounts SET verified=1, has_id=1, next_scan_at=scanned_at+18000000 WHERE username=?", [u]);
    const acc = (await db.exec("SELECT has_id,nrepo,junk FROM accounts WHERE username=?", [u])).rows[0] || {};
    const rs = (await db.exec("SELECT id,kind FROM repos WHERE account=?", [u])).rows;
    const st = rs.map((r) => ({ sql: "UPDATE repos SET prio=? WHERE id=? AND kind=?", args: [repoScore(r.id, u, r.kind, acc), r.id, r.kind] }));
    if (st.length) await db.batch(st, 500);
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
  await db.exec("UPDATE repos SET junk=1 WHERE account=? AND junk=0", [u]);
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
export async function countStale(db, cut) {
  const r = await db.exec("SELECT COUNT(*) c FROM repos WHERE state='done' AND junk=0 AND indexed_at<=?", [cut]);
  return r.rows[0].c;
}
export async function claimRepoStale(db, worker, cut) {
  const row = (await db.exec("SELECT r.id,r.account,r.kind FROM repos r WHERE r.state='done' AND r.junk=0 AND r.indexed_at<=? ORDER BY r.has_id DESC, r.indexed_at ASC LIMIT 1", [cut])).rows[0];
  if (!row) return null;
  const u = await db.exec("UPDATE repos SET state='fetching', worker=?, indexed_at=? WHERE id=? AND kind=? AND state='done' AND indexed_at<=?", [worker, Date.now(), row.id, row.kind, cut]);
  return u.changes ? row : null;
}
export async function clearJunk(db, u) {
  const r = await db.exec("UPDATE accounts SET junk=0, next_scan_at=scanned_at + CASE WHEN has_id=1 THEN 18000000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE username=?", [u]);
  await db.exec("UPDATE repos SET junk=0 WHERE account=? AND junk=1", [u]);
  return r.changes;
}
export async function getRepoPrev(db, id, kind) {
  return (await db.exec("SELECT sha,updated FROM repos WHERE id=? AND kind=?", [id, kind])).rows[0] || {};
}
export async function countNew(db) {
  const r = await db.exec("SELECT v c FROM counters WHERE k='rsj:new:0'");
  return r.rows[0] ? r.rows[0].c : 0;
}
export async function stateCounts(db) {
  return (await db.exec("SELECT substr(k,5) state, v c FROM counters WHERE k LIKE 'rst:%'")).rows;
}
export async function accountCounts(db) {
  const r = await db.exec("SELECT v c FROM counters WHERE k='acjt:0'");
  return r.rows[0] ? r.rows[0].c : 0;
}

export async function pendingAccountsAdaptive(db, now) { return pendingAccounts(db, now, true); }
export async function countPendingAdaptive(db, now) { return countPending(db, now, true); }
