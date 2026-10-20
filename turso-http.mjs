// turso-http.mjs — client tối giản cho Turso SQL-over-HTTP (/v2/pipeline), không dependency.
// Dùng được ở Cloudflare Workers, Node 18+, GitHub Actions.
const encArg = (v) =>
  v === null || v === undefined ? { type: "null" }
  : typeof v === "number" ? (Number.isInteger(v) ? { type: "integer", value: String(v) } : { type: "float", value: String(v) })
  : { type: "text", value: String(v) };

const dec = (cell) => {
  if (!cell || cell.type === "null") return null;
  if (cell.type === "integer" || cell.type === "float") return Number(cell.value);
  return cell.value; // text
};

export function tursoClient({ url, token, timeout = 20000 }) {
  if (!url || !token) throw new Error("turso: thiếu TURSO_DATABASE_URL / TURSO_AUTH_TOKEN");
  const endpoint = String(url).replace(/^libsql:\/\//i, "https://").replace(/\/+$/, "") + "/v2/pipeline";

  async function pipeline(requests, attempt = 0) {
    let res;
    try {
      res = await fetch(endpoint, {
        method: "POST",
        headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
        body: JSON.stringify({ requests }),
        signal: AbortSignal.timeout(timeout),
      });
    } catch (e) {
      if (attempt < 4) { await new Promise((r) => setTimeout(r, 300 * (attempt + 1))); return pipeline(requests, attempt + 1); }
      throw new Error("turso fetch: " + (e && e.message ? e.message : e));
    }
    if (res.status === 429 || res.status >= 500) {
      if (attempt < 4) {
        const ra = parseInt(res.headers.get("retry-after") || "0", 10);
        await new Promise((r) => setTimeout(r, ra > 0 ? ra * 1000 : 500 * (attempt + 1)));
        return pipeline(requests, attempt + 1);
      }
    }
    if (!res.ok) throw new Error("turso http " + res.status + " " + (await res.text()).slice(0, 200));
    const j = await res.json();
    if (!Array.isArray(j.results)) throw new Error("turso: response thieu results");
    return j.results;
  }

  function parseResult(r) {
    if (!r || r.type !== "ok") throw new Error("turso: " + (r && r.error && r.error.message ? r.error.message : "query error"));
    const result = r.response && r.response.result;
    const cols = (result && result.cols || []).map((c) => c.name);
    const rows = (result && result.rows || []).map((row) => {
      const o = {};
      row.forEach((cell, i) => { o[cols[i]] = dec(cell); });
      return o;
    });
    return { rows, changes: result && result.affected_row_count || 0, lastInsertRowid: result && result.last_insert_rowid };
  }

  async function exec(sql, args = []) {
    const rs = await pipeline([{ type: "execute", stmt: { sql, args: args.map(encArg) } }, { type: "close" }]);
    return parseResult(rs[0]);
  }

  async function batch(statements, chunk = 100) {
    const out = [];
    for (let i = 0; i < statements.length; i += chunk) {
      const part = statements.slice(i, i + chunk);
      const reqs = part.map((s) => ({ type: "execute", stmt: { sql: s.sql, args: (s.args || []).map(encArg) } }));
      reqs.push({ type: "close" });
      const rs = await pipeline(reqs);
      for (let k = 0; k < part.length; k++) out.push(parseResult(rs[k]));
    }
    return out;
  }

  return { exec, batch, endpoint };
}
