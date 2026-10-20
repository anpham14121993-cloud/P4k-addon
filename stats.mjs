// stats.mjs — thống kê DB Turso. Chạy: set -a; . ./.env.turso; set +a; node stats.mjs [--save]
import { tursoClient } from "./turso-http.mjs";
const c = tursoClient({ url: process.env.TURSO_DATABASE_URL, token: process.env.TURSO_AUTH_TOKEN });
const SAVE = process.argv.includes("--save");
const one = async (sql, args = []) => (await c.exec(sql, args)).rows[0] || {};
const all = async (sql, args = []) => (await c.exec(sql, args)).rows;
const gb = (n) => ((Number(n) || 0) / 1073741824).toFixed(1) + " GB";
const tb = (n) => ((Number(n) || 0) / 1099511627776).toFixed(2) + " TB";

const t0 = Date.now();
// Đọc từ counters + SUM(nfile) theo repo: tránh mọi truy vấn quét toàn bảng files.
const files = await one("SELECT COALESCE(SUM(nfile),0) n, COALESCE(SUM(size),0) sz FROM repos");
const acct = await one("SELECT (SELECT v FROM counters WHERE k='acct:t') total, (SELECT v FROM counters WHERE k='acjt:1') junk, COALESCE(SUM(has_id=1),0) has_id, COALESCE(SUM(scanned_at=0),0) never FROM accounts");
const repos = await all("SELECT substr(k,5) state, v c FROM counters WHERE k LIKE 'rst:%' ORDER BY v DESC");
const kinds = await all("SELECT kind, COUNT(*) c FROM repos WHERE state='done' GROUP BY kind ORDER BY c DESC");
const fkinds = await all("SELECT kind, COALESCE(SUM(nfile),0) c FROM repos GROUP BY kind ORDER BY c DESC");
const pend = await one("SELECT COUNT(*) c FROM accounts WHERE next_scan_at<=?", [Date.now()]);
const page = await one("PRAGMA page_count");

const dbBytes = (Number(page.page_count) || 0) * 4096;
const cur = await all("SELECT key,val FROM meta WHERE key IN ('feed_cursor','stats_v1')");
const meta = Object.fromEntries(cur.map((r) => [r.key, r.val]));
const prev = meta.stats_v1 ? JSON.parse(meta.stats_v1) : null;
const topRepos = await all("SELECT id,kind,nfile,size FROM repos WHERE state='done' ORDER BY nfile DESC LIMIT 10");
const topAcct = await all("SELECT account,COUNT(*) repos,COALESCE(SUM(nfile),0) files,COALESCE(SUM(size),0) sz FROM repos WHERE state='done' GROUP BY account ORDER BY files DESC LIMIT 10");

const now = { files: Number(files.n), bytes: Number(files.sz), accounts: Number(acct.total), junk: Number(acct.junk), has_id: Number(acct.has_id), repos: repos.reduce((s, r) => s + Number(r.c), 0), dbBytes, at: new Date().toISOString() };
const delta = (k, v, fmt = (x) => x) => prev ? `  (${v - prev[k] >= 0 ? "+" : ""}${fmt(v - prev[k])})` : "";

console.log(`== DB ${process.env.TURSO_DATABASE_URL?.replace(/^libsql:\/\//, "") || ""}  (${((Date.now() - t0) / 1000).toFixed(1)}s)`);
console.log(`files      ${now.files}${delta("files", now.files)}  · logic ${tb(now.bytes)}`);
console.log(`accounts   ${now.accounts} (has_id ${acct.has_id}, junk ${acct.junk}, chưa quét ${acct.never})`);
console.log(`repos      ${now.repos}${delta("repos", now.repos)}`);
console.log(`  trạng thái: ${repos.map((r) => r.state + "=" + r.c).join("  ")}`);
console.log(`  kind done : ${kinds.map((r) => r.kind + "=" + r.c).join("  ")}`);
console.log(`  files kind: ${fkinds.map((r) => r.kind + "=" + r.c).join("  ")}`);
console.log(`pending in : ${pend.c} account (TTL thích ứng)`);
console.log(`dung lượng : ${gb(dbBytes)} (${(dbBytes / 1048576).toFixed(1)} MiB, ${page.page_count} trang × 4096)${prev ? "  (" + (dbBytes - prev.dbBytes >= 0 ? "+" : "") + ((dbBytes - prev.dbBytes) / 1048576).toFixed(1) + " MiB)" : ""}`);
console.log(`feed cursor: ${meta.feed_cursor ? "có (" + String(meta.feed_cursor).length + " ký tự)" : "hết/trống"}`);
console.log(`\nTop repo theo số file:\n` + topRepos.map((r) => `  ${r.kind}:${r.id}  ${r.nfile} file  ${gb(r.size)}`).join("\n"));
console.log(`\nTop account theo số file:\n` + topAcct.map((r) => `  ${r.account}  ${r.repos} repo  ${r.files} file  ${gb(r.sz)}`).join("\n"));
if (SAVE) {
  await c.exec("INSERT OR REPLACE INTO meta(key,val) VALUES('stats_v1',?)", [JSON.stringify(now)]);
  console.log("\n(đã lưu snapshot stats_v1)");
}
