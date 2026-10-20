import { DatabaseSync } from "node:sqlite";
import fs from "fs";

export const DB_PATH = process.env.P4K_DB || "/sdcard/Download/P4k/phim4k.db";
export const SCAN_PATH = process.env.P4K_SCAN || "/tmp/opencode/aw/scan2.json";
export const IDF = /^[A-Z0-9]{10,17}$/;
// Đuôi hợp lệ của file phim (farm giấu video dưới đuôi ML/model hoặc video thật).
// Loại ảnh/tài liệu/audio/data (.png .jpg .npy .json .md .wav .parquet .pdb ...).
export const PHIM_EXT = new Set([
  ".h5", ".hdf5", ".pb", ".arrow", ".model", ".pth", ".safetensors",
  ".pt", ".ckpt", ".weights", ".tflite", ".onnx", ".keras", ".bin", ".pkl",
  ".joblib", ".gguf", ".nemo", ".mar", ".msgpack", ".mlmodel", ".h5mu", ".params", ".param",
  ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".ts", ".flv", ".wmv", ".mpg", ".mpeg", ".3gp",
]);
export const JUNK_EMPTY = parseInt(process.env.P4K_JUNK_EMPTY || "3", 10);
// isFarm (RỘNG): chữ+số (abc123), chữ+số+chữ (hoanglong2k5), chữ-only dài >=12 (nguyenthimaihuong).
export const isFarm = (u) => { const s = String(u); return /^[a-z]+\d{3,10}$/i.test(s) || /^[a-z]+\d{1,4}[a-z]+\d{0,4}$/i.test(s) || /^[a-z]{12,}$/i.test(s); };
// isFarmStrict (cho feed quét toàn HF): BẮT BUỘC có số -> tránh kéo org/tên ML hợp lệ.
export const isFarmStrict = (u) => { const s = String(u); return /^[a-z]+\d{3,10}$/i.test(s) || /^[a-z]+\d{1,4}[a-z]+\d{0,4}$/i.test(s); };

// ---------- chấm điểm ưu tiên (dựa mẫu đo trên DB: bucket/self-repo của account lowercase+digits) ----------
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

// SQLite trao khoá ghi trả SQLITE_BUSY ngay (deadlock-avoid) -> tự thử lại ở tầng JS.
const _wa = new Int32Array(new SharedArrayBuffer(4));
export function sleepSync(ms) { Atomics.wait(_wa, 0, 0, ms); }
export function retrySync(fn, tries = 200) {
  for (let i = 0; i < tries; i++) {
    try { return fn(); }
    catch (e) { const m = String((e && e.message) || e); if (/locked|busy/i.test(m)) { sleepSync(15 + Math.floor(Math.random() * 45)); continue; } throw e; }
  }
  throw new Error("retrySync exhausted (database locked)");
}

export function openDB(path = DB_PATH, opts = {}) {
  const db = new DatabaseSync(path, opts.readOnly ? { readOnly: true } : {});
  retrySync(() => db.exec("PRAGMA busy_timeout=15000"));
  if (!opts.readOnly) {
    retrySync(() => db.exec("PRAGMA journal_mode=WAL"));
    retrySync(() => db.exec("PRAGMA synchronous=NORMAL"));
  }
  return db;
}

function cols(db, t) { return db.prepare(`PRAGMA table_info(${t})`).all().map((r) => r.name); }
function addCol(db, t, c, d) { if (!cols(db, t).includes(c)) db.exec(`ALTER TABLE ${t} ADD COLUMN ${c} ${d}`); }
function tableExists(db, t) { return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(t); }
// Ưu tiên index: bucket + dataset đặt tên trùng account là chữ ký farm.
function prioOf(id, account, kind) {
  if (kind === "bucket") return 2;
  return id === account + "/" + account ? 2 : 1;
}
function reposComposite(db) {
  const pk = db.prepare("PRAGMA table_info(repos)").all().filter((c) => c.pk > 0).map((c) => c.name).sort();
  return pk.length === 2 && pk[0] === "id" && pk[1] === "kind";
}

const SCHEMA_IDX_TRG = `CREATE INDEX IF NOT EXISTS idx_repos_state ON repos(state);
           CREATE INDEX IF NOT EXISTS idx_repos_account ON repos(account);
           CREATE INDEX IF NOT EXISTS idx_files_rp ON files(repo_id, kind, path);
           CREATE TABLE IF NOT EXISTS counters(k TEXT PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0);
           CREATE INDEX IF NOT EXISTS idx_accounts_next ON accounts(next_scan_at);
           CREATE INDEX IF NOT EXISTS idx_accounts_scan ON accounts(junk, scanned_at);
           CREATE INDEX IF NOT EXISTS idx_repos_claim ON repos(state, junk, prio DESC, last_modified DESC);
           CREATE INDEX IF NOT EXISTS idx_repos_stale ON repos(junk, state, has_id DESC, indexed_at ASC);
           CREATE INDEX IF NOT EXISTS idx_repos_deadfiles ON repos(state, nfile);`;
const SCHEMA_TRG = `CREATE TRIGGER IF NOT EXISTS trg_rep_ins AFTER INSERT ON repos BEGIN
             INSERT INTO counters(k,v) VALUES('rst:'||NEW.state,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
             INSERT INTO counters(k,v) VALUES('rsj:'||NEW.state||':'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
           END;
           CREATE TRIGGER IF NOT EXISTS trg_rep_del AFTER DELETE ON repos BEGIN
             INSERT INTO counters(k,v) VALUES('rst:'||OLD.state,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
             INSERT INTO counters(k,v) VALUES('rsj:'||OLD.state||':'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
           END;
           CREATE TRIGGER IF NOT EXISTS trg_rep_upd AFTER UPDATE ON repos WHEN NEW.state<>OLD.state OR NEW.junk<>OLD.junk BEGIN
             INSERT INTO counters(k,v) VALUES('rst:'||OLD.state,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
             INSERT INTO counters(k,v) VALUES('rsj:'||OLD.state||':'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
             INSERT INTO counters(k,v) VALUES('rst:'||NEW.state,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
             INSERT INTO counters(k,v) VALUES('rsj:'||NEW.state||':'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
           END;
           CREATE TRIGGER IF NOT EXISTS trg_acc_ins AFTER INSERT ON accounts BEGIN
             INSERT INTO counters(k,v) VALUES('acct:t',1) ON CONFLICT(k) DO UPDATE SET v=v+1;
             INSERT INTO counters(k,v) VALUES('acjt:'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
           END;
           CREATE TRIGGER IF NOT EXISTS trg_acc_del AFTER DELETE ON accounts BEGIN
             INSERT INTO counters(k,v) VALUES('acct:t',-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
             INSERT INTO counters(k,v) VALUES('acjt:'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
           END;
           CREATE TRIGGER IF NOT EXISTS trg_acc_upd AFTER UPDATE ON accounts WHEN NEW.junk<>OLD.junk BEGIN
             INSERT INTO counters(k,v) VALUES('acjt:'||OLD.junk,-1) ON CONFLICT(k) DO UPDATE SET v=v-1;
             INSERT INTO counters(k,v) VALUES('acjt:'||NEW.junk,1) ON CONFLICT(k) DO UPDATE SET v=v+1;
           END;`;

// Mô hình chuẩn: account -> repo(kind=dataset|bucket) -> file (tra thẳng `files`).
// Khoá repo = (id, kind) vì id bucket TRÙNG id dataset (1.501/2.268).
export function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS accounts(username TEXT PRIMARY KEY, total_bytes INTEGER DEFAULT 0, nrepo INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS repos(id TEXT, kind TEXT, account TEXT, size INTEGER DEFAULT 0, nfile INTEGER DEFAULT 0, PRIMARY KEY(id, kind));
    CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT, kind TEXT, path TEXT, name TEXT, size INTEGER, off TEXT);
    CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);
    CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id);
    CREATE TABLE IF NOT EXISTS fetched(repo_id TEXT PRIMARY KEY, ts INTEGER, n INTEGER);
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, val TEXT);
    CREATE TABLE IF NOT EXISTS stems(stem TEXT PRIMARY KEY, n INTEGER DEFAULT 0, scanned_at INTEGER DEFAULT 0, added INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS seen_ext(ext TEXT PRIMARY KEY, n INTEGER DEFAULT 0, farm_n INTEGER DEFAULT 0, sample TEXT);
  `);
  addCol(db, "accounts", "verified", "INTEGER DEFAULT 0");
  addCol(db, "accounts", "has_id", "INTEGER DEFAULT 0");
  addCol(db, "accounts", "nrepo", "INTEGER DEFAULT 0");
  addCol(db, "accounts", "junk", "INTEGER DEFAULT 0");
  addCol(db, "accounts", "scanned_at", "INTEGER DEFAULT 0");
  addCol(db, "accounts", "last_seen", "INTEGER DEFAULT 0");
  addCol(db, "repos", "last_modified", "INTEGER DEFAULT 0");
  addCol(db, "repos", "state", "TEXT DEFAULT 'new'");
  addCol(db, "repos", "has_id", "INTEGER DEFAULT 0");
  addCol(db, "repos", "indexed_at", "INTEGER DEFAULT 0");
  addCol(db, "repos", "worker", "TEXT");
  addCol(db, "repos", "entries", "INTEGER DEFAULT 0");
  addCol(db, "repos", "sha", "TEXT");
  addCol(db, "repos", "updated", "INTEGER DEFAULT 0");
  addCol(db, "repos", "prio", "INTEGER DEFAULT 0");
  addCol(db, "repos", "junk", "INTEGER DEFAULT 0");
  addCol(db, "accounts", "next_scan_at", "INTEGER DEFAULT 0");
  db.exec(SCHEMA_IDX_TRG);
  if (!metaGet(db, "prio_idx_v1")) {
    db.exec(`DROP INDEX IF EXISTS idx_repos_prio;
             DROP INDEX IF EXISTS idx_repos_prio_d;
             CREATE INDEX IF NOT EXISTS idx_repos_prio ON repos(state, prio DESC);`);
    metaSet(db, "prio_idx_v1", "1");
  }
  if (!metaGet(db, "prio_backfill_v3")) {
    db.exec("UPDATE repos SET prio=CASE WHEN kind='bucket' OR id=account||'/'||account THEN 2 ELSE 1 END");
    metaSet(db, "prio_backfill_v3", "1");
  }
  if (!metaGet(db, "prio_score_v1")) {
    retrySync(() => db.exec("BEGIN IMMEDIATE"));
    try {
      const accs = new Map(db.prepare("SELECT username,has_id,nrepo,junk FROM accounts").all().map((a) => [a.username, a]));
      const upd = db.prepare("UPDATE repos SET prio=? WHERE id=? AND kind=?");
      for (const r of db.prepare("SELECT id,kind,account FROM repos").iterate()) upd.run(repoScore(r.id, r.account, r.kind, accs.get(r.account) || {}), r.id, r.kind);
      metaSet(db, "prio_score_v1", "1");
      db.exec("COMMIT");
    } catch (e) { try { db.exec("ROLLBACK"); } catch {} throw e; }
  }
  if (!metaGet(db, "acct_hasid_v1")) {
    db.exec("UPDATE accounts SET has_id=(SELECT CASE WHEN COUNT(*)>0 THEN 1 ELSE 0 END FROM repos WHERE repos.account=accounts.username AND repos.has_id=1)");
    metaSet(db, "acct_hasid_v1", "1");
  }
  if (!metaGet(db, "acct_nrepo_v1")) {
    db.exec("UPDATE accounts SET nrepo=(SELECT COUNT(*) FROM repos WHERE repos.account=accounts.username)");
    metaSet(db, "acct_nrepo_v1", "1");
  }
  if (!metaGet(db, "counters_v1")) {
    db.exec("UPDATE repos SET junk=1 WHERE junk=0 AND account IN (SELECT username FROM accounts WHERE junk=1)");
    db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rst:'||state,COUNT(*) FROM repos GROUP BY state");
    db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rsj:'||state||':'||junk,COUNT(*) FROM repos GROUP BY state,junk");
    db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'acct:t',COUNT(*) FROM accounts");
    db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'acjt:'||junk,COUNT(*) FROM accounts GROUP BY junk");
    db.exec("UPDATE accounts SET next_scan_at=scanned_at + CASE WHEN junk=1 THEN " + JUNK_RETRY_MS + " WHEN has_id=1 THEN 43200000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE next_scan_at=0");
    metaSet(db, "counters_v1", "1");
    log("counters_v1 backfill xong");
  }
  if (!metaGet(db, "junk_sweep_v1")) {
    retrySync(() => {
      // Tắt trigger tạm thời: UPDATE hàng loạt nhanh, rồi dựng lại counters cho khớp.
      for (const t of ["trg_rep_ins", "trg_rep_del", "trg_rep_upd", "trg_acc_ins", "trg_acc_del", "trg_acc_upd"]) db.exec("DROP TRIGGER IF EXISTS " + t);
      const r = db.prepare("UPDATE accounts SET junk=1, next_scan_at=? + " + JUNK_RETRY_MS + " WHERE junk=0 AND has_id=0 AND username NOT IN (SELECT account FROM repos WHERE state='new')").run(Date.now()).changes;
      db.prepare("UPDATE repos SET junk=1 WHERE junk=0 AND account IN (SELECT username FROM accounts WHERE junk=1)").run();
      db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'acjt:'||junk, COUNT(*) FROM accounts GROUP BY junk");
      db.exec("INSERT OR REPLACE INTO counters(k,v) VALUES('acct:t',(SELECT COUNT(*) FROM accounts))");
      db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rsj:'||state||':'||junk, COUNT(*) FROM repos GROUP BY state,junk");
      db.exec("INSERT OR REPLACE INTO counters(k,v) SELECT 'rst:'||state, COUNT(*) FROM repos GROUP BY state");
      db.exec(SCHEMA_TRG);
      metaSet(db, "junk_sweep_v1", "1");
      log("junk_sweep_v1 xong, account junk +" + r);
    });
  }
  if (!metaGet(db, "inv_nudge_v1")) {
    retrySync(() => {
      const r = db.prepare("UPDATE accounts SET next_scan_at=0 WHERE junk=0 AND next_scan_at>0").run().changes;
      metaSet(db, "inv_nudge_v1", "1");
      log("inv_nudge_v1 xong, account pending +" + r);
    });
  }
}

export function metaGet(db, k) { const r = db.prepare("SELECT val FROM meta WHERE key=?").get(k); return r ? r.val : null; }
export function metaSet(db, k, v) { retrySync(() => db.prepare("INSERT OR REPLACE INTO meta(key,val) VALUES(?,?)").run(k, String(v))); }

export function isId(name) {
  const b = String(name || "").split("/").pop();
  const m = b.match(/\.[^.]+$/);
  if (!m || !PHIM_EXT.has(m[0].toLowerCase())) return false;
  return IDF.test(b.slice(0, b.length - m[0].length));
}
// Tách đuôi + id của basename (ok = id khớp IDF, BẤT KỂ đuôi) — để ghi nhận đuôi lạ.
export function idParts(name) {
  const bn = String(name || "").split("/").pop();
  const m = bn.match(/\.[^.]+$/);
  const ext = m ? m[0].toLowerCase() : "(no-ext)";
  const base = m ? bn.slice(0, bn.length - m[0].length) : bn;
  return { ext, base, ok: IDF.test(base) };
}
// Ghi nhận thống kê đuôi (MỌI đuôi, kể cả chưa whitelist) — để phát hiện đuôi phim MỚI.
// farm_n = số file đuôi này nằm trong repo CÓ đuôi phim hợp lệ (dấu hiệu farm thật).
export function noteExts(db, extMap, hasPhim) {
  if (!extMap || extMap.size === 0) return;
  const ins = db.prepare("INSERT INTO seen_ext(ext,n,farm_n,sample) VALUES(?,?,?,?) ON CONFLICT(ext) DO UPDATE SET n=n+excluded.n, farm_n=farm_n+excluded.farm_n");
  for (const [ext, [c, sample]] of extMap) ins.run(ext, c, hasPhim ? c : 0, sample);
}

// Lưu file id của 1 tree: chỉ giữ file khớp id + đuôi thuộc PHIM_EXT (loại ảnh/doc/audio).
// MERGE (không xoá-ghi-lại): upsert theo (repo_id,kind,path) + chỉ xoá path vắng mặt.
// Toàn bộ nằm trong 1 transaction -> addon không bao giờ thấy repo bị rỗng giữa chừng.
export function storeTree(db, repoId, kind, files, off) {
  return retrySync(() => {
    try {
      db.exec("BEGIN IMMEDIATE");
      db.exec("DROP TABLE IF EXISTS _keep; CREATE TEMP TABLE _keep(path TEXT PRIMARY KEY)");
      const keepIns = db.prepare("INSERT OR IGNORE INTO _keep(path) VALUES(?)");
      const up = db.prepare("UPDATE files SET name=?, size=?, off=? WHERE repo_id=? AND kind=? AND path=?");
      const ins = db.prepare("INSERT INTO files(repo_id,kind,path,name,size,off) VALUES(?,?,?,?,?,?)");
      let n = 0, size = 0, hasPhim = false;
      const extMap = new Map();
      for (const f of files) {
        if (f.type && f.type !== "file") continue;
        const p = String(f.path || f.name || "");
        const bn = p.split("/").pop();
        const ip = idParts(bn);
        if (!ip.ok) continue;
        const e = extMap.get(ip.ext) || [0, bn]; e[0]++; extMap.set(ip.ext, e);
        if (!PHIM_EXT.has(ip.ext)) continue;
        hasPhim = true;
        keepIns.run(p);
        const sz = f.size || 0;
        const oid = f.oid || f.xetHash || off || null;
        const ch = up.run(bn, sz, oid, repoId, kind, p);
        if (!ch.changes) ins.run(repoId, kind, p, bn, sz, oid);
        n++; size += sz;
      }
      noteExts(db, extMap, hasPhim);
      db.prepare("DELETE FROM files WHERE repo_id=? AND kind=? AND path NOT IN (SELECT path FROM _keep)").run(repoId, kind);
      db.exec("COMMIT");
      return { n, size };
    } catch (e) { try { db.exec("ROLLBACK"); } catch {} throw e; }
  });
}

export function repoCounts(db, id, kind) {
  const r = retrySync(() => db.prepare("SELECT COUNT(*) c, COALESCE(SUM(size),0) s FROM files WHERE repo_id=? AND kind=?").get(id, kind));
  return { n: r.c, size: r.s };
}

// Áp git-diff (từ /compare) vào files: chỉ THÊM/SỬA/XOÁ đúng path thay đổi.
// Chỉ xử lý file dạng id + LFS pointer (có oid/size). Ca phức tạp (rename, binary,
// thiếu size) -> trả {ok:false} để caller fallback tải cây đầy đủ.
function unq(p) { p = String(p); if (p.startsWith('"') && p.endsWith('"')) { try { return JSON.parse(p); } catch {} } return p; }
export function applyGitDiff(db, repoId, kind, diff) {
  const lines = String(diff).split("\n");
  const ops = []; let cur = null;
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

  return retrySync(() => {
    try {
      db.exec("BEGIN IMMEDIATE");
      const del = db.prepare("DELETE FROM files WHERE repo_id=? AND kind=? AND path=?");
      for (const p of dels) del.run(repoId, kind, p);
      const up = db.prepare("UPDATE files SET name=?, size=?, off=? WHERE repo_id=? AND kind=? AND path=?");
      const ins = db.prepare("INSERT INTO files(repo_id,kind,path,name,size,off) VALUES(?,?,?,?,?,?)");
      for (const [p, bn, sz, oid] of ups) { const ch = up.run(bn, sz, oid, repoId, kind, p); if (!ch.changes) ins.run(repoId, kind, p, bn, sz, oid); }
      const repoHasPhim = !!(db.prepare("SELECT 1 FROM files WHERE repo_id=? AND kind=? LIMIT 1").get(repoId, kind)) || [...extMap.keys()].some((e) => PHIM_EXT.has(e));
      noteExts(db, extMap, repoHasPhim);
      db.exec("COMMIT");
      return { ok: true, dels: dels.length, ups: ups.length };
    } catch (e) { try { db.exec("ROLLBACK"); } catch {} throw e; }
  });
}

export function finishRepo(db, id, kind, { n, size, dead, entries, sha, updated }) {
  retrySync(() => {
    if (dead) {
      db.prepare("DELETE FROM files WHERE repo_id=? AND kind=?").run(id, kind);
      db.prepare("UPDATE repos SET state='dead', has_id=0, nfile=0, size=0, indexed_at=? WHERE id=? AND kind=?").run(Date.now(), id, kind);
    } else {
      db.prepare("UPDATE repos SET state='done', has_id=?, nfile=?, size=?, entries=?, indexed_at=?, worker=NULL, sha=COALESCE(?, sha), updated=COALESCE(?, updated) WHERE id=? AND kind=?").run(n > 0 ? 1 : 0, n, size, entries || 0, Date.now(), sha ?? null, updated ?? null, id, kind);
    }
  });
}

export function unclaimRepo(db, id, kind) { retrySync(() => db.prepare("UPDATE repos SET state='new', worker=NULL WHERE id=? AND kind=? AND state='fetching'").run(id, kind)); }

// Đếm account: verified nếu có repo chứa id; junk nếu >=JUNK_EMPTY repo rỗng mà chưa verified.
export function reevalAccount(db, u) {
  retrySync(() => {
    const hit = db.prepare("SELECT 1 FROM repos WHERE account=? AND has_id=1 LIMIT 1").get(u);
    if (hit) {
      db.prepare("UPDATE accounts SET verified=1, has_id=1, next_scan_at=scanned_at+43200000 WHERE username=?").run(u);
      const acc = db.prepare("SELECT has_id,nrepo,junk FROM accounts WHERE username=?").get(u) || {};
      const upd = db.prepare("UPDATE repos SET prio=? WHERE id=? AND kind=?");
      for (const r of db.prepare("SELECT id,kind FROM repos WHERE account=?").all(u)) upd.run(repoScore(r.id, u, r.kind, acc), r.id, r.kind);
      return;
    }
    // Account rác = không có file id nào (has_id=0) VÀ đã index hết (không còn repo 'new').
    const pendingNew = db.prepare("SELECT 1 x FROM repos WHERE account=? AND state='new' LIMIT 1").get(u);
    if (!pendingNew) markJunk(db, u);
  });
}

// Đánh dấu account rác: ẩn mọi repo của nó, đặt TTL ~1 năm (thực chất là vĩnh viễn).
export function markJunk(db, u) {
  return retrySync(() => {
    const now = Date.now();
    const r = db.prepare("UPDATE accounts SET junk=1, scanned_at=?, next_scan_at=? + " + JUNK_RETRY_MS + " WHERE username=?").run(now, now, u).changes;
    db.prepare("UPDATE repos SET junk=1 WHERE account=? AND junk=0").run(u);
    return r;
  });
}

export function upsertRepoMeta(db, id, account, kind, lastModified) {
  const lm = lastModified ? Date.parse(lastModified) || 0 : 0;
  return retrySync(() => {
    const acc = db.prepare("SELECT has_id, nrepo, junk FROM accounts WHERE username=?").get(account) || {};
    const prio = repoScore(id, account, kind, acc);
    const ex = db.prepare("SELECT last_modified, state, prio FROM repos WHERE id=? AND kind=?").get(id, kind);
    if (!ex) {
      db.prepare("INSERT INTO repos(id,kind,account,last_modified,state,junk,prio) VALUES(?,?,?,?,'new',?,?)").run(id, kind, account, lm, acc.junk ? 1 : 0, prio);
      db.prepare("UPDATE accounts SET nrepo=nrepo+1 WHERE username=?").run(account);
      return true;
    }
    if (lm > (ex.last_modified || 0)) {
      db.prepare("UPDATE repos SET last_modified=?, state='new', account=?, junk=?, prio=? WHERE id=? AND kind=?").run(lm, account, acc.junk ? 1 : 0, prio, id, kind);
      return true;
    }
    if (ex.state === "new" && prio > (ex.prio || 0)) db.prepare("UPDATE repos SET prio=? WHERE id=? AND kind=?").run(prio, id, kind);
    return ex.state === "new";
  });
}

// Giải phóng claim "fetching" bị treo (worker chết giữa chừng).
export function recoverStale(db, ms = 300000) {
  const cut = Date.now() - ms;
  return retrySync(() => {
    const a = db.prepare("UPDATE repos SET state='new', worker=NULL WHERE state='fetching' AND indexed_at<?").run(cut).changes;
    if (a) log(`recoverStale repos=${a}`);
    return a;
  });
}

export function claimRepo(db, worker, kind = null) {
  return retrySync(() => {
    // Thử nhiều lần: nếu 2 worker tranh cùng 1 row, kẻ thua phải lấy row kế tiếp
    // (không được return null ngay kẻo thoát cả vòng lặp).
    const sql = kind
      ? "SELECT r.id,r.account,r.kind FROM repos r WHERE r.state='new' AND r.kind=? AND r.junk=0 ORDER BY r.prio DESC, r.last_modified DESC, r.rowid LIMIT 1"
      : "SELECT r.id,r.account,r.kind FROM repos r WHERE r.state='new' AND r.kind IN ('dataset','bucket') AND r.junk=0 ORDER BY r.prio DESC, r.last_modified DESC, r.rowid LIMIT 1";
    for (let i = 0; i < 200; i++) {
      const row = (kind ? db.prepare(sql).get(kind) : db.prepare(sql).get());
      if (!row) return null;
      const r = db.prepare("UPDATE repos SET state='fetching', worker=?, indexed_at=? WHERE id=? AND kind=? AND state='new'").run(worker, Date.now(), row.id, row.kind);
      if (r.changes) return row;
    }
    return null;
  });
}

// Gộp bảng buckets (schema cũ) vào repos.kind='bucket'; đổi khoá repos -> (id,kind); bỏ tiền tố BUCKET:.
export function migrateMergeBuckets(db) {
  if (metaGet(db, "merged_buckets_v2")) return { skipped: true };
  let rebuilt = false, merged = 0;
  retrySync(() => {
    db.exec("BEGIN IMMEDIATE");
    if (!reposComposite(db)) {
      db.exec(`CREATE TABLE repos_new(id TEXT, kind TEXT, account TEXT, size INTEGER DEFAULT 0, nfile INTEGER DEFAULT 0, last_modified INTEGER DEFAULT 0, state TEXT DEFAULT 'new', has_id INTEGER DEFAULT 0, entries INTEGER DEFAULT 0, indexed_at INTEGER DEFAULT 0, worker TEXT, prio INTEGER DEFAULT 0, junk INTEGER DEFAULT 0, PRIMARY KEY(id, kind))`);
      db.exec(`INSERT OR IGNORE INTO repos_new(id,kind,account,size,nfile,last_modified,state,has_id,entries,indexed_at,worker,prio,junk)
               SELECT id,kind,account,size,nfile,last_modified,state,has_id,entries,indexed_at,worker,${cols(db, "repos").includes("prio") ? "prio" : "0"},${cols(db, "repos").includes("junk") ? "junk" : "0"} FROM repos`);
      db.exec("DROP TABLE repos");
      db.exec("ALTER TABLE repos_new RENAME TO repos");
      db.exec("CREATE INDEX IF NOT EXISTS idx_repos_state ON repos(state); CREATE INDEX IF NOT EXISTS idx_repos_account ON repos(account);");
      rebuilt = true;
    }
    if (tableExists(db, "buckets")) {
      const hasState = cols(db, "buckets").includes("state");
      db.exec(`INSERT OR IGNORE INTO repos(id,kind,account,size,nfile,state,indexed_at,prio)
               SELECT id,'bucket',account,size,nfile,${hasState ? "state" : "'new'"},${hasState ? "indexed_at" : "0"},2 FROM buckets`);
      // đồng bộ metadata bucket theo files (còn tiền tố BUCKET:)
      db.exec("UPDATE repos SET nfile=(SELECT COUNT(*) FROM files f WHERE f.repo_id='BUCKET:'||repos.id), size=(SELECT COALESCE(SUM(f.size),0) FROM files f WHERE f.repo_id='BUCKET:'||repos.id), has_id=(SELECT CASE WHEN COUNT(*)>0 THEN 1 ELSE 0 END FROM files f WHERE f.repo_id='BUCKET:'||repos.id) WHERE kind='bucket'");
      db.exec("UPDATE repos SET state='done', indexed_at=" + Date.now() + " WHERE kind='bucket' AND id IN (SELECT substr(repo_id,8) FROM fetched WHERE repo_id LIKE 'BUCKET:%')");
      // bỏ tiền tố BUCKET: trong files + dọn fetched bucket
      db.exec("UPDATE files SET repo_id=substr(repo_id,8) WHERE repo_id LIKE 'BUCKET:%'");
      db.exec("DELETE FROM fetched WHERE repo_id LIKE 'BUCKET:%'");
      merged = db.prepare("SELECT COUNT(*) c FROM repos WHERE kind='bucket'").get().c;
      db.exec("DROP TABLE buckets");
    }
    metaSet(db, "merged_buckets_v2", new Date().toISOString());
    db.exec("COMMIT");
  });
  log(`mergeBuckets rebuilt=${rebuilt} bucket_repos=${merged}`);
  return { rebuilt, merged };
}

// Migrate 1 lần từ scan2.json (giữ nguyên state đã index). Chạy SAU migrateMergeBuckets.
export function migrateFromScan(db, p = SCAN_PATH) {
  if (metaGet(db, "migrated_scan2")) return { skipped: true };
  if (!fs.existsSync(p)) return { skipped: true, nofile: true };
  const scan = JSON.parse(fs.readFileSync(p));
  const A = db.prepare("INSERT OR IGNORE INTO accounts(username) VALUES(?)");
  const R = db.prepare("INSERT OR IGNORE INTO repos(id,kind,account,size,nfile,last_modified,state,prio) VALUES(?,?,?,?,?,0,'new',?)");
  db.exec("BEGIN");
  let na = 0, nr = 0, nb = 0;
  for (const u of scan.users || []) { A.run(u); na++; }
  for (const u in scan.repos || {}) {
    const r = scan.repos[u];
    for (const id of r.datasets || []) { const t = scan.trees?.[id] || {}; R.run(id, "dataset", u, t.size || 0, t.count || 0, prioOf(id, u, "dataset")); nr++; }
    for (const id of r.models || []) { const t = scan.trees?.[id] || {}; R.run(id, "model", u, t.size || 0, t.count || 0, prioOf(id, u, "model")); nr++; }
    for (const b of r.buckets || []) { R.run(b.id, "bucket", u, b.size || 0, b.files || 0, prioOf(b.id, u, "bucket")); nb++; }
  }
  db.exec("COMMIT");
  db.prepare("UPDATE repos SET state='done', indexed_at=? WHERE kind IN ('dataset','model') AND id IN (SELECT repo_id FROM fetched)").run(Date.now());
  db.prepare("UPDATE repos SET state='dead' WHERE kind IN ('dataset','model') AND id IN (SELECT repo_id FROM fetched WHERE n=0)").run();
  // NGUỒN CHÂN LÝ = bảng files; đồng bộ nfile/size/has_id theo (id,kind).
  db.exec(`UPDATE repos SET
      nfile=(SELECT COUNT(*) FROM files f WHERE f.repo_id=repos.id AND f.kind=repos.kind),
      size =(SELECT COALESCE(SUM(f.size),0) FROM files f WHERE f.repo_id=repos.id AND f.kind=repos.kind),
      has_id=(SELECT CASE WHEN COUNT(*)>0 THEN 1 ELSE 0 END FROM files f WHERE f.repo_id=repos.id AND f.kind=repos.kind)`);
  db.exec("UPDATE accounts SET verified=1 WHERE username IN (SELECT DISTINCT account FROM repos WHERE has_id=1)");
  if (scan.discover?.cursor) metaSet(db, "feed_cursor", scan.discover.cursor);
  metaSet(db, "migrated_scan2", new Date().toISOString());
  return { na, nr, nb };
}

// ---------- helper backend-agnostic cho auto.mjs ----------
export function getAccount(db, u) { return db.prepare("SELECT has_id,junk,nrepo FROM accounts WHERE username=?").get(u) || null; }
export function insertAccount(db, u) { return db.prepare("INSERT OR IGNORE INTO accounts(username,last_seen) VALUES(?,?)").run(u, Date.now()).changes; }
export const JUNK_RETRY_MS = 31536000000; // account junk gan nhu vinh vien (1 nam) -> khong lo inv/index
export const TTL_EXPR = "CASE WHEN junk=1 THEN " + JUNK_RETRY_MS + " WHEN has_id=1 THEN 43200000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END";
export function setScanned(db, u) { const now = Date.now(); return retrySync(() => db.prepare("UPDATE accounts SET scanned_at=?, next_scan_at=? + (" + TTL_EXPR + ") WHERE username=?").run(now, now, u)).changes; }
export function pendingAccounts(db, cut, adaptive = true) {
  return adaptive
    ? db.prepare("SELECT username,has_id,nrepo,junk,scanned_at FROM accounts WHERE next_scan_at<=? ORDER BY next_scan_at LIMIT 1000").all(cut)
    : db.prepare("SELECT username,has_id,nrepo,junk,scanned_at FROM accounts WHERE junk=0 AND scanned_at<=? ORDER BY scanned_at LIMIT 1000").all(cut);
}
export function countPending(db, cut, adaptive = true) {
  return adaptive
    ? db.prepare("SELECT COUNT(*) c FROM accounts WHERE next_scan_at<=?").get(cut).c
    : db.prepare("SELECT COUNT(*) c FROM accounts WHERE junk=0 AND scanned_at<=?").get(cut).c;
}
export function countNew(db) { const r = db.prepare("SELECT v c FROM counters WHERE k='rsj:new:0'").get(); return r ? r.c : 0; }
export function stateCounts(db) { return db.prepare("SELECT substr(k,5) state, v c FROM counters WHERE k LIKE 'rst:%'").all(); }
export function accountCounts(db) { const r = db.prepare("SELECT v c FROM counters WHERE k='acjt:0'").get(); return r ? r.c : 0; }
export function getRepoPrev(db, id, kind) { return db.prepare("SELECT sha,updated FROM repos WHERE id=? AND kind=?").get(id, kind) || {}; }
export function pendingAccountsAdaptive(db, cut) { return pendingAccounts(db, cut, true); }
export function countPendingAdaptive(db, cut) { return countPending(db, cut, true); }
export function countStale(db, cut) { return db.prepare("SELECT COUNT(*) c FROM repos WHERE state='done' AND junk=0 AND indexed_at<=?").get(cut).c; }
export function claimRepoStale(db, worker, cut) {
  return retrySync(() => {
    const row = db.prepare("SELECT r.id,r.account,r.kind FROM repos r WHERE r.state='done' AND r.junk=0 AND r.indexed_at<=? ORDER BY r.has_id DESC, r.indexed_at ASC LIMIT 1").get(cut);
    if (!row) return null;
    const u = db.prepare("UPDATE repos SET state='fetching', worker=?, indexed_at=? WHERE id=? AND kind=? AND state='done' AND indexed_at<=?").run(worker, Date.now(), row.id, row.kind, cut);
    return u.changes ? row : null;
  });
}
export function clearJunk(db, u) {
  return retrySync(() => {
    const r = db.prepare("UPDATE accounts SET junk=0, next_scan_at=scanned_at + CASE WHEN has_id=1 THEN 43200000 WHEN nrepo>0 THEN 604800000 ELSE 1209600000 END WHERE username=?").run(u).changes;
    db.prepare("UPDATE repos SET junk=0 WHERE account=? AND junk=1").run(u);
    return r;
  });
}
