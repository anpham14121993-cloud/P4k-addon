// clean-dead.mjs — xoá file còn sót của repo dead / account junk. Idempotent, chạy được định kỳ.
// Quét theo REPO (index idx_files_repo) nên không bao giờ quét toàn bảng files (tránh đốt quota rows read).
// Turso: TURSO_DATABASE_URL=... TURSO_AUTH_TOKEN=... node clean-dead.mjs
// Local: P4K_TURSO_LOCAL=1 P4K_DB=/path.db node clean-dead.mjs
import { createRequire } from "node:module";
import { tursoClient } from "./turso-http.mjs";
const require = createRequire(import.meta.url);
const LIMIT = parseInt(process.env.P4K_CLEAN_CHUNK || "500", 10);

let exec;
if (process.env.P4K_TURSO_LOCAL) {
  const { DatabaseSync } = require("node:sqlite");
  const d = new DatabaseSync(process.env.P4K_DB || "/tmp/p4k-turso-local.db");
  d.exec("PRAGMA busy_timeout=15000");
  exec = (sql, args = []) => {
    if (/^\s*(select|with|pragma)/i.test(sql)) return { rows: d.prepare(sql).all(...args), changes: 0 };
    return { rows: [], changes: d.prepare(sql).run(...args).changes };
  };
} else {
  const c = tursoClient({ url: process.env.TURSO_DATABASE_URL, token: process.env.TURSO_AUTH_TOKEN });
  exec = (sql, args = []) => c.exec(sql, args);
}

const DEAD = "SELECT id,kind,nfile FROM repos WHERE state='dead' AND nfile>0 LIMIT ?";
const JUNK = "SELECT id,kind,nfile FROM repos WHERE junk=1 AND nfile>0 LIMIT ?";

let dead = 0, junk = 0;
for (const [label, sql] of [["dead", DEAD], ["junk", JUNK]]) {
  let removed = 0, rounds = 0;
  for (; rounds < 20; rounds++) {
    const rows = (await exec(sql, [LIMIT])).rows;
    if (!rows.length) break;
    for (const r of rows) {
      const d = await exec("DELETE FROM files WHERE repo_id=? AND kind=?", [r.id, r.kind]);
      const u = await exec("UPDATE repos SET nfile=0, size=0, has_id=0 WHERE id=? AND kind=? AND nfile>0", [r.id, r.kind]);
      removed += d.changes; void u;
    }
    console.log(`${label} -${rows.length} repo (file -${removed})`);
  }
  if (label === "dead") dead = removed; else junk = removed;
}
console.log(`DONE dead_files=${dead} junk_files=${junk} rounds_ok`);
