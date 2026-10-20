// turso-db.mjs — adapter DB cho addon trên Workers: 2 query giống bản SQLite + cache.
import { tursoClient } from "./turso-http.mjs";

const TTL_MS = 10 * 60 * 1000;
const MAX_MEM = 1000;
const baseOf = (id) => (id.includes(".") ? id.slice(0, id.lastIndexOf(".")) : id);

export function tursoDb(env) {
  const c = tursoClient({ url: env.TURSO_DATABASE_URL, token: env.TURSO_AUTH_TOKEN });
  const mem = new Map(); // "<kind>:<id>" -> { at, ... }
  const cache = typeof caches !== "undefined" && caches.default ? caches.default : null;
  const keyOf = (kind, id) => new Request("https://p4k-db.internal/" + kind + "/" + encodeURIComponent(id));

  async function cacheGet(kind, id) {
    const mk = kind + ":" + id;
    if (mem.has(mk)) {
      const v = mem.get(mk);
      if (Date.now() - v.at < TTL_MS) return v;
      mem.delete(mk);
    }
    if (cache) {
      try {
        const r = await cache.match(keyOf(kind, id));
        if (r) {
          const v = await r.json();
          if (v && typeof v.at === "number" && Date.now() - v.at < TTL_MS) { putMem(kind, id, v); return v; }
        }
      } catch { }
    }
    return null;
  }
  function putMem(kind, id, v) {
    const mk = kind + ":" + id;
    if (mem.size >= MAX_MEM) { const k = mem.keys().next().value; mem.delete(k); }
    mem.set(mk, v);
  }
  async function cachePut(kind, id, v) {
    putMem(kind, id, v);
    if (!cache) return;
    try {
      await cache.put(keyOf(kind, id), new Response(JSON.stringify(v), {
        headers: { "content-type": "application/json", "cache-control": "max-age=" + Math.floor(TTL_MS / 1000) },
      }));
    } catch { }
  }

  async function row(id) {
    const hit = await cacheGet("row", id);
    if (hit) return hit.row;
    const b = baseOf(id);
    const r = await c.exec("SELECT kind, repo_id, path, size FROM files WHERE name >= ? AND name < ? ORDER BY size DESC LIMIT 1", [b + ".", b + "/"]);
    const row = r.rows[0] || null;
    await cachePut("row", id, { at: Date.now(), row });
    return row;
  }

  async function variants(id) {
    const hit = await cacheGet("vars", id);
    if (hit && hit.vars) return hit.vars;
    const b = baseOf(id);
    const r = await c.exec(
      "SELECT kind, repo_id, path, size FROM files WHERE name >= ? AND name < ? " +
      "AND rowid IN (SELECT MIN(rowid) FROM files WHERE name >= ? AND name < ? GROUP BY size) ORDER BY size DESC",
      [b + ".", b + "/", b + ".", b + "/"]);
    await cachePut("vars", id, { at: Date.now(), vars: r.rows });
    return r.rows;
  }

  return { row, variants };
}
