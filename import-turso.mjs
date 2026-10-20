// import-turso.mjs — nạp phim4k.db (SQLite local) lên Turso theo lô.
// Dùng: TURSO_DATABASE_URL=... TURSO_AUTH_TOKEN=... node import-turso.mjs [--reset] [--offset N]
// An toàn: accounts/repos dùng INSERT OR REPLACE; files resume bằng --offset <rowid đã nạp>.
import { DatabaseSync } from "node:sqlite";
import { tursoClient } from "./turso-http.mjs";

const SRC = process.env.P4K_DB || "/sdcard/Download/P4k/phim4k.db";
const RESET = process.argv.includes("--reset");
const offIdx = process.argv.indexOf("--offset");
const FILE_OFFSET = offIdx > 0 ? parseInt(process.argv[offIdx + 1], 10) : 0;
const BATCH = parseInt(process.env.P4K_IMPORT_BATCH || "500", 10);
const log = (m) => console.error(`[${new Date().toISOString().slice(11, 19)}] ${m}`);

const db = new DatabaseSync(SRC, { readOnly: true });
const c = tursoClient({ url: process.env.TURSO_DATABASE_URL, token: process.env.TURSO_AUTH_TOKEN });

async function execAll(sqls) {
  for (const sql of sqls) await c.exec(sql);
}

async function main() {
  log(`import ${SRC} -> ${c.endpoint} reset=${RESET} offset=${FILE_OFFSET}`);
  await execAll([
    `CREATE TABLE IF NOT EXISTS accounts(username TEXT PRIMARY KEY, total_bytes INTEGER DEFAULT 0, nrepo INTEGER DEFAULT 0, verified INTEGER DEFAULT 0, has_id INTEGER DEFAULT 0, junk INTEGER DEFAULT 0, scanned_at INTEGER DEFAULT 0, last_seen INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS repos(id TEXT, kind TEXT, account TEXT, size INTEGER DEFAULT 0, nfile INTEGER DEFAULT 0, last_modified INTEGER DEFAULT 0, state TEXT DEFAULT 'new', has_id INTEGER DEFAULT 0, indexed_at INTEGER DEFAULT 0, worker TEXT, entries INTEGER DEFAULT 0, sha TEXT, updated INTEGER DEFAULT 0, prio INTEGER DEFAULT 0, PRIMARY KEY(id, kind))`,
    `CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT, kind TEXT, path TEXT, name TEXT, size INTEGER, off TEXT)`,
    `CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, val TEXT)`,
    `CREATE TABLE IF NOT EXISTS stems(stem TEXT PRIMARY KEY, n INTEGER DEFAULT 0, scanned_at INTEGER DEFAULT 0, added INTEGER DEFAULT 0)`,
    `CREATE TABLE IF NOT EXISTS seen_ext(ext TEXT PRIMARY KEY, n INTEGER DEFAULT 0, farm_n INTEGER DEFAULT 0, sample TEXT)`,
  ]);

  if (RESET) { log("reset: xoá files/repos/accounts"); await execAll(["DELETE FROM files", "DELETE FROM repos", "DELETE FROM accounts"]); }

  // accounts
  {
    const rows = db.prepare("SELECT username,total_bytes,nrepo,verified,has_id,junk,scanned_at,last_seen FROM accounts").all();
    log(`accounts=${rows.length}`);
    for (let i = 0; i < rows.length; i += BATCH) {
      await c.batch(rows.slice(i, i + BATCH).map((r) => ({
        sql: "INSERT OR REPLACE INTO accounts(username,total_bytes,nrepo,verified,has_id,junk,scanned_at,last_seen) VALUES(?,?,?,?,?,?,?,?)",
        args: [r.username, r.total_bytes || 0, r.nrepo || 0, r.verified || 0, r.has_id || 0, r.junk || 0, r.scanned_at || 0, r.last_seen || 0],
      })), BATCH);
      if ((i / BATCH) % 20 === 0) log(`accounts ${Math.min(i + BATCH, rows.length)}/${rows.length}`);
    }
  }

  // repos
  {
    const rows = db.prepare("SELECT id,kind,account,size,nfile,last_modified,state,has_id,indexed_at,worker,entries,sha,updated,prio FROM repos").all();
    log(`repos=${rows.length}`);
    for (let i = 0; i < rows.length; i += BATCH) {
      await c.batch(rows.slice(i, i + BATCH).map((r) => ({
        sql: "INSERT OR REPLACE INTO repos(id,kind,account,size,nfile,last_modified,state,has_id,indexed_at,worker,entries,sha,updated,prio) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        args: [r.id, r.kind, r.account, r.size || 0, r.nfile || 0, r.last_modified || 0, r.state || "new", r.has_id || 0, r.indexed_at || 0, r.worker || null, r.entries || 0, r.sha || null, r.updated || 0, r.prio || 0],
      })), BATCH);
      if ((i / BATCH) % 20 === 0) log(`repos ${Math.min(i + BATCH, rows.length)}/${rows.length}`);
    }
  }

  // files (lớn nhất) — resume theo rowid
  {
    const total = db.prepare("SELECT COUNT(*) c FROM files").get().c;
    log(`files=${total} (offset ${FILE_OFFSET})`);
    let last = FILE_OFFSET, done = 0, t0 = Date.now();
    while (true) {
      const rows = db.prepare("SELECT rowid,repo_id,kind,path,name,size,off FROM files WHERE rowid>? ORDER BY rowid LIMIT ?").all(last, BATCH);
      if (!rows.length) break;
      await c.batch(rows.map((r) => ({
        sql: "INSERT OR REPLACE INTO files(id,repo_id,kind,path,name,size,off) VALUES(?,?,?,?,?,?,?)",
        args: [r.rowid, r.repo_id, r.kind, r.path, r.name, r.size || 0, r.off || null],
      })), BATCH);
      last = rows[rows.length - 1].rowid; done += rows.length;
      if (done % 20000 < BATCH) log(`files ${done}/${total - FILE_OFFSET} lastRowid=${last} ${(done / ((Date.now() - t0) / 1000)).toFixed(0)}/s`);
    }
    log(`files xong done=${done} lastRowid=${last}`);
  }

  // stems, seen_ext, meta
  {
    const rows = db.prepare("SELECT stem,n,scanned_at,added FROM stems").all();
    log(`stems=${rows.length}`);
    for (let i = 0; i < rows.length; i += BATCH) await c.batch(rows.slice(i, i + BATCH).map((r) => ({
      sql: "INSERT OR REPLACE INTO stems(stem,n,scanned_at,added) VALUES(?,?,?,?)", args: [r.stem, r.n || 0, r.scanned_at || 0, r.added || 0],
    })), BATCH);
    const exts = db.prepare("SELECT ext,n,farm_n,sample FROM seen_ext").all();
    log(`seen_ext=${exts.length}`);
    for (let i = 0; i < exts.length; i += BATCH) await c.batch(exts.slice(i, i + BATCH).map((r) => ({
      sql: "INSERT OR REPLACE INTO seen_ext(ext,n,farm_n,sample) VALUES(?,?,?,?)", args: [r.ext, r.n || 0, r.farm_n || 0, r.sample || null],
    })), BATCH);
    const meta = db.prepare("SELECT key,val FROM meta").all();
    await c.batch(meta.map((r) => ({ sql: "INSERT OR REPLACE INTO meta(key,val) VALUES(?,?)", args: [r.key, r.val] })), BATCH);
    log(`meta=${meta.length}`);
  }

  // index sau khi nạp (nhanh hơn + ít rows written)
  log("tạo index...");
  await execAll([
    "CREATE INDEX IF NOT EXISTS idx_files_name ON files(name)",
    "CREATE INDEX IF NOT EXISTS idx_files_repo ON files(repo_id)",
    "CREATE INDEX IF NOT EXISTS idx_files_rp ON files(repo_id, kind, path)",
    "CREATE INDEX IF NOT EXISTS idx_repos_state ON repos(state)",
    "CREATE INDEX IF NOT EXISTS idx_repos_account ON repos(account)",
    "CREATE INDEX IF NOT EXISTS idx_repos_prio ON repos(state, prio DESC)",
  ]);
  const q = async (sql) => (await c.exec(sql)).rows[0];
  log(`VERIFY ${JSON.stringify({ accounts: await q("SELECT COUNT(*) c FROM accounts"), repos: await q("SELECT COUNT(*) c FROM repos"), files: await q("SELECT COUNT(*) c FROM files") })}`);
}

main().catch((e) => { log("FATAL " + (e && e.stack ? e.stack : e)); process.exit(1); });
