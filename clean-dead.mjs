// clean-dead.mjs — xoá file của repo dead (và file của account junk). Chạy định kỳ, idempotent.
// Turso: TURSO_DATABASE_URL=... TURSO_AUTH_TOKEN=... node clean-dead.mjs
// Local: P4K_TURSO_LOCAL=1 P4K_DB=/path.db node clean-dead.mjs
import { createRequire } from "node:module";
import { tursoClient } from "./turso-http.mjs";
const require = createRequire(import.meta.url);
const CHUNK = parseInt(process.env.P4K_CLEAN_CHUNK || "5000", 10);

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

const DEAD = "SELECT f.rowid FROM files f JOIN repos r ON r.id=f.repo_id AND r.kind=f.kind WHERE r.state='dead' LIMIT ?";
const JUNK = "SELECT f.rowid FROM files f JOIN repos r ON r.id=f.repo_id AND r.kind=f.kind JOIN accounts a ON a.username=r.account WHERE a.junk=1 LIMIT ?";

let dead = 0, junk = 0;
for (;;) {
  const r = await exec("DELETE FROM files WHERE rowid IN (" + DEAD + ")", [CHUNK]);
  dead += r.changes;
  console.log(`dead -${r.changes} (tong ${dead})`);
  if (!r.changes) break;
}
for (;;) {
  const r = await exec("DELETE FROM files WHERE rowid IN (" + JUNK + ")", [CHUNK]);
  junk += r.changes;
  console.log(`junk -${r.changes} (tong ${junk})`);
  if (!r.changes) break;
}
const n = (await exec("SELECT COUNT(*) c FROM files")).rows[0].c;
console.log(`DONE dead=${dead} junk=${junk} files=${n}`);
