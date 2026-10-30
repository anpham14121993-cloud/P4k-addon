// turso-db.mjs — adapter DB cho addon trên Workers: files, titles, episodes_streams + cache.
import { tursoClient } from "./turso-http.mjs";

const TTL_MS = 30 * 60 * 1000;
const MAX_MEM = 5000;
const baseOf = (id) => (id.includes(".") ? id.slice(0, id.lastIndexOf(".")) : id);

function normalizeText(v) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

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
          if (v && typeof v.at === "number" && Date.now() - v.at < TTL_MS) {
            putMem(kind, id, v);
            return v;
          }
        }
      } catch {}
    }
    return null;
  }

  function putMem(kind, id, v) {
    const mk = kind + ":" + id;
    if (mem.size >= MAX_MEM) {
      const k = mem.keys().next().value;
      mem.delete(k);
    }
    mem.set(mk, v);
  }

  async function cachePut(kind, id, v) {
    putMem(kind, id, v);
    if (!cache) return;
    try {
      await cache.put(
        keyOf(kind, id),
        new Response(JSON.stringify(v), {
          headers: { "content-type": "application/json", "cache-control": "max-age=" + Math.floor(TTL_MS / 1000) },
        })
      );
    } catch {}
  }

  /* ═══════════════ 1. TRUY VẤN BẢNG FILES (HUGGING FACE) ═══════════════ */

  async function row(id) {
    const hit = await cacheGet("row", id);
    if (hit) return hit.row;
    const b = baseOf(id);
    const r = await c.exec(
      "SELECT kind, repo_id, path, size FROM files WHERE name >= ? AND name < ? ORDER BY size DESC LIMIT 1",
      [b + ".", b + "/"]
    );
    const fileRow = r.rows[0] || null;
    await cachePut("row", id, { at: Date.now(), row: fileRow });
    return fileRow;
  }

  async function variants(id) {
    const hit = await cacheGet("vars", id);
    if (hit && hit.vars) return hit.vars;
    const b = baseOf(id);
    const r = await c.exec(
      "SELECT kind, repo_id, path, size FROM files WHERE name >= ? AND name < ? " +
        "AND rowid IN (SELECT MIN(rowid) FROM files WHERE name >= ? AND name < ? GROUP BY size) ORDER BY size DESC LIMIT 50",
      [b + ".", b + "/", b + ".", b + "/"]
    );
    await cachePut("vars", id, { at: Date.now(), vars: r.rows });
    return r.rows;
  }

  /* ═══════════════ 2. TRUY VẤN BẢNG TITLES (PHIM & CATALOG) ═══════════════ */

  async function getCatalog(catalogId, page = 1, limit = 20) {
    const cacheKey = `${catalogId}:${page}:${limit}`;
    const hit = await cacheGet("cat", cacheKey);
    if (hit && hit.rows) return hit.rows;

    const offset = Math.max(0, (page - 1) * limit);
    let sql = "";
    const args = [];
    if (catalogId === "phim4k-otv-movies") {
      sql =
        "SELECT id, type, title, poster_url, thumbnail_url, release_year, imdb_rating, quality, overview FROM titles WHERE type = 'movie' ORDER BY id DESC LIMIT ? OFFSET ?";
      args.push(limit, offset);
    } else if (catalogId === "phim4k-otv-series" || catalogId === "phim4k-otv-series-latest") {
      sql =
        "SELECT id, type, title, poster_url, thumbnail_url, release_year, imdb_rating, quality, overview FROM titles WHERE type = 'series' ORDER BY id DESC LIMIT ? OFFSET ?";
      args.push(limit, offset);
    } else if (catalogId === "phim4k-otv-top") {
      sql =
        "SELECT id, type, title, poster_url, thumbnail_url, release_year, imdb_rating, quality, overview FROM titles ORDER BY imdb_rating DESC, id DESC LIMIT ? OFFSET ?";
      args.push(limit, offset);
    } else {
      return [];
    }
    const r = await c.exec(sql, args);
    const rows = r.rows || [];
    if (rows.length > 0) {
      await cachePut("cat", cacheKey, { at: Date.now(), rows });
    }
    return rows;
  }

  async function search(query, mediaType, limit = 20) {
    const cleanQ = normalizeText(query);
    if (!cleanQ) return [];
    let sql = "";
    const args = [];
    const pattern = "%" + cleanQ + "%";
    if (mediaType === "series" || mediaType === "tvseries") {
      sql =
        "SELECT id, type, title, poster_url, thumbnail_url, release_year, imdb_rating, quality, overview FROM titles WHERE type = 'series' AND title_clean LIKE ? ORDER BY release_year DESC LIMIT ?";
      args.push(pattern, limit);
    } else if (mediaType === "movie") {
      sql =
        "SELECT id, type, title, poster_url, thumbnail_url, release_year, imdb_rating, quality, overview FROM titles WHERE type = 'movie' AND title_clean LIKE ? ORDER BY release_year DESC LIMIT ?";
      args.push(pattern, limit);
    } else {
      sql =
        "SELECT id, type, title, poster_url, thumbnail_url, release_year, imdb_rating, quality, overview FROM titles WHERE title_clean LIKE ? ORDER BY release_year DESC LIMIT ?";
      args.push(pattern, limit);
    }
    const r = await c.exec(sql, args);
    return r.rows || [];
  }

  async function getTitle(id) {
    const numId = parseInt(id, 10);
    if (!numId) return null;
    const hit = await cacheGet("title", String(numId));
    if (hit && hit.row) return hit.row;
    const r = await c.exec("SELECT * FROM titles WHERE id = ? LIMIT 1", [numId]);
    const row = r.rows[0] || null;
    if (row) await cachePut("title", String(numId), { at: Date.now(), row });
    return row;
  }

  async function getTitleByImdb(imdbId) {
    if (!imdbId) return null;
    const cleanImdb = String(imdbId).startsWith("tt") ? String(imdbId) : "tt" + imdbId;
    const hit = await cacheGet("imdb", cleanImdb);
    if (hit && hit.row) return hit.row;
    const r = await c.exec("SELECT * FROM titles WHERE imdb_id = ? LIMIT 1", [cleanImdb]);
    const row = r.rows[0] || null;
    if (row) await cachePut("imdb", cleanImdb, { at: Date.now(), row });
    return row;
  }

  /* ═══════════════ 3. TRUY VẤN BẢNG EPISODES_STREAMS (TẬP & LUỒNG PHÁT) ═══════════════ */

  async function getSeriesEpisodes(titleId) {
    const numId = parseInt(titleId, 10);
    if (!numId) return [];
    const hit = await cacheGet("eps", String(numId));
    if (hit && hit.rows) return hit.rows;
    const r = await c.exec(
      "SELECT season, episode, label FROM episodes_streams WHERE title_id = ? GROUP BY season, episode ORDER BY season ASC, episode ASC",
      [numId]
    );
    const rows = r.rows || [];
    if (rows.length > 0) await cachePut("eps", String(numId), { at: Date.now(), rows });
    return rows;
  }

  async function getStreams(titleId, season = 1, episode = 1) {
    const numId = parseInt(titleId, 10);
    if (!numId) return [];
    const s = parseInt(season, 10) || 1;
    const e = parseInt(episode, 10) || 1;
    const cacheKey = `${numId}:${s}:${e}`;
    const hit = await cacheGet("stream", cacheKey);
    if (hit && hit.rows) return hit.rows;
    const r = await c.exec(
      "SELECT s.id, s.title_id, s.season, s.episode, s.file_id, s.label, s.container, s.quality, s.subtitles, t.title, t.quality as release_quality " +
        "FROM episodes_streams s JOIN titles t ON s.title_id = t.id " +
        "WHERE s.title_id = ? AND s.season = ? AND s.episode = ? ORDER BY s.id ASC",
      [numId, s, e]
    );
    const rows = r.rows || [];
    if (rows.length > 0) await cachePut("stream", cacheKey, { at: Date.now(), rows });
    return rows;
  }

  return { row, variants, getCatalog, search, getTitle, getTitleByImdb, getSeriesEpisodes, getStreams };
}
