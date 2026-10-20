// verify-turso.mjs — kiểm tra nhanh Turso: counts, index, độ trễ 2 query của addon.
// TURSO_DATABASE_URL=... TURSO_AUTH_TOKEN=... node verify-turso.mjs
import { tursoClient } from "./turso-http.mjs";
const c = tursoClient({ url: process.env.TURSO_DATABASE_URL, token: process.env.TURSO_AUTH_TOKEN });
const t = async (label, sql, args = []) => { const t0 = Date.now(); const r = await c.exec(sql, args); console.log(label, (Date.now() - t0) + "ms", r.rows.length > 5 ? `rows=${r.rows.length}` : JSON.stringify(r.rows)); return r; };
const counts = await t("counts", "SELECT (SELECT COUNT(*) FROM accounts) a,(SELECT COUNT(*) FROM repos) r,(SELECT COUNT(*) FROM files) f");
const sample = (await c.exec("SELECT name FROM files WHERE name >= 'A' ORDER BY name LIMIT 1")).rows[0];
await t("plan_name", "EXPLAIN QUERY PLAN SELECT kind,repo_id,path,size FROM files WHERE name >= ? AND name < ? ORDER BY size DESC LIMIT 1", [sample.name.slice(0, sample.name.lastIndexOf(".")) + ".", sample.name.slice(0, sample.name.lastIndexOf(".")) + "/"]);
const base = sample.name.slice(0, sample.name.lastIndexOf("."));
for (let i = 0; i < 3; i++) await t("row", "SELECT kind,repo_id,path,size FROM files WHERE name >= ? AND name < ? ORDER BY size DESC LIMIT 1", [base + ".", base + "/"]);
await t("vars", "SELECT kind,repo_id,path,size FROM files WHERE name >= ? AND name < ? AND rowid IN (SELECT MIN(rowid) FROM files WHERE name >= ? AND name < ? GROUP BY size) ORDER BY size DESC LIMIT 20", [base + ".", base + "/", base + ".", base + "/"]);
console.log("OK", counts.rows[0]);
