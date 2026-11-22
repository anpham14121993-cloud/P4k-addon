// p4k-sync-lib.mjs — Thư viện dùng chung cho sync-realtime.mjs và reconcile-api.mjs (Phim4K -> Turso DB).
import fs from "fs";
import { tursoClient } from "./turso-http.mjs";
import { vnProxy, P4K_HEADERS } from "./vn-proxy.mjs";

export const P4K_API_BASE = "https://api.phim4k.space/rest-api/v130/";
export const P4K_CF_PROXY_BASE = "https://phim4k-addon.notthingp.workers.dev/api-proxy/";

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export function loadTursoClient() {
  let dbUrl = process.env.TURSO_DATABASE_URL || "";
  let dbToken = process.env.TURSO_AUTH_TOKEN || "";

  if (!process.env.GITHUB_ACTIONS && fs.existsSync(".env.turso")) {
    const lines = fs.readFileSync(".env.turso", "utf-8").split("\n");
    for (const line of lines) {
      const eq = line.indexOf("=");
      if (eq > 0) {
        const k = line.slice(0, eq).trim();
        const v = line.slice(eq + 1).trim();
        if (k && v && !k.startsWith("#")) {
          if (k === "TURSO_DATABASE_URL") dbUrl = v;
          if (k === "TURSO_AUTH_TOKEN") dbToken = v;
          process.env[k] = v;
        }
      }
    }
  }

  if (!dbUrl || !dbToken) {
    console.error("Lỗi: Thiếu TURSO_DATABASE_URL hoặc TURSO_AUTH_TOKEN");
    process.exit(1);
  }

  return tursoClient({ url: dbUrl, token: dbToken });
}

export function createCliArgs(argv = process.argv.slice(2)) {
  return {
    get(name, def = null) {
      for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a === "--" + name && i + 1 < argv.length) return argv[i + 1];
        if (a.startsWith("--" + name + "=")) return a.slice(name.length + 3);
      }
      return def;
    },
    has(name) {
      return argv.some((a) => a === "--" + name || a.startsWith("--" + name + "="));
    },
  };
}

export function cleanText(v) {
  return v ? String(v).trim() : null;
}

export function normalizeText(v) {
  return String(v || "")
    .replace(/[đĐ]/g, "d")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function fileNameFrom(url) {
  const s = String(url || "").trim();
  if (!s) return "";
  return s.slice(s.lastIndexOf("/") + 1).split("?")[0].replace(/\.[^.]+$/, "");
}

export function looksLikeYear(n) {
  return n >= 1900 && n <= 2100;
}

export function episodeInfo(seasonEntry, ep) {
  const episodeName = String((ep && (ep.episodes_name || ep.name || ep.title)) || "");
  const file = String((ep && (ep.file_url || ep.url)) || "").split("?")[0].split("/").pop();
  const text = episodeName + " " + file;
  let season = null;
  let episode = null;
  let episodeEnd = null;

  const se = text.match(/(?:^|[^a-z0-9])s\s*0*(\d{1,2})\s*[._ -]?\s*e\s*0*(\d{1,4})(?!\d)/i);
  const batch = episodeName.match(/^\s*0*(\d{1,4})\s*[-–]\s*0*(\d{1,4})(?!\d)/);
  const eNum = episodeName.match(/^\s*e(?:p|pisode)?\s*[._ -]*0*(\d{1,4})(?!\d)/i);
  const tap = text.match(/t[aậ]p\s*[._ -]*0*(\d{1,4})(?!\d)/i);
  const cjk = text.match(/第\s*0*(\d{1,4})\s*[集話]/);
  const lead = episodeName.match(/^\s*0*(\d{1,4})(?!\d)/);

  if (se) {
    season = parseInt(se[1], 10);
    episode = parseInt(se[2], 10);
  } else if (batch) {
    const a = parseInt(batch[1], 10);
    const b = parseInt(batch[2], 10);
    if (!looksLikeYear(a) && !looksLikeYear(b)) {
      episode = a;
      episodeEnd = b > a ? b : null;
    }
  }

  if (episode === null && eNum && !looksLikeYear(parseInt(eNum[1], 10))) episode = parseInt(eNum[1], 10);
  if (episode === null && tap && !looksLikeYear(parseInt(tap[1], 10))) episode = parseInt(tap[1], 10);
  if (episode === null && cjk && !looksLikeYear(parseInt(cjk[1], 10))) episode = parseInt(cjk[1], 10);
  if (episode === null && lead && !looksLikeYear(parseInt(lead[1], 10))) episode = parseInt(lead[1], 10);

  if (ep && typeof ep === "object") {
    if (episode === null) {
      const d = ep.episode ?? ep.episode_number ?? ep.number ?? ep.ep ?? ep.episodes_number;
      if (d != null && /^\d+$/.test(String(d)) && !looksLikeYear(parseInt(String(d), 10))) {
        episode = parseInt(String(d), 10);
      }
    }
    if (season === null) {
      const d = ep.season ?? ep.season_number ?? ep.seasons_number;
      if (d != null && /^\d+$/.test(String(d))) season = parseInt(String(d), 10);
    }
  }

  if (season === null) {
    const label = String((seasonEntry && (seasonEntry.seasons_name || seasonEntry.season_name || seasonEntry.name || seasonEntry.title)) || "");
    const sm =
      label.match(/season\s*0*(\d+)/i) ||
      label.match(/m[uù]a\s*0*(\d+)/i) ||
      label.match(/ph[aầ]n\s*0*(\d+)/i) ||
      label.match(/(?:^|[^a-z0-9])s\s*0*(\d{1,2})(?!\d)/i);
    if (sm) season = parseInt(sm[1], 10);
  }

  if (season === null) season = 1;
  if (episode === null && seasonEntry && Array.isArray(seasonEntry.episodes) && seasonEntry.episodes.length === 1) {
    episode = 1;
  }
  return { season, episode, episodeEnd };
}

export function createP4kApi() {
  let globalCoolUntil = 0;

  async function fetchViaCfProxy(url) {
    try {
      const proxyUrl = url.replace(P4K_API_BASE, P4K_CF_PROXY_BASE);
      const r = await fetch(proxyUrl, { signal: AbortSignal.timeout(12000) });
      if (r.ok) {
        const text = await r.text();
        if (!text.includes("Cloudflare")) {
          const data = JSON.parse(text);
          if (data && (Array.isArray(data) || typeof data === "object")) return data;
        }
      }
    } catch {}
    return null;
  }

  async function fetchWithRetry(url, maxRetries = 3) {
    if (vnProxy.activeProxy) {
      const data = await vnProxy.fetchJson(url, maxRetries);
      if (data) return data;
    }

    for (let i = 0; i < maxRetries; i++) {
      const now = Date.now();
      if (now < globalCoolUntil) {
        await sleep(globalCoolUntil - now);
      }
      try {
        const r = await fetch(url, { headers: P4K_HEADERS, signal: AbortSignal.timeout(10000) });
        if (r.status === 429) {
          const ra = parseInt(r.headers.get("retry-after") || "0", 10);
          const coolMs = ra > 0 ? ra * 1000 : 15000;
          globalCoolUntil = Date.now() + coolMs;
          await sleep(coolMs);
          continue;
        }
        if (r.status === 403) {
          const cfData = await fetchViaCfProxy(url);
          if (cfData) return cfData;
          return vnProxy.fetchJson(url, 2);
        }
        if (!r.ok) {
          if (r.status >= 500) {
            await sleep(1500 * (i + 1));
            continue;
          }
          const cfData = await fetchViaCfProxy(url);
          if (cfData) return cfData;
          return null;
        }
        const txt = await r.text();
        if (txt.includes("rate limited") || txt.includes("Cloudflare")) {
          const cfData = await fetchViaCfProxy(url);
          if (cfData) return cfData;
          globalCoolUntil = Date.now() + 15000;
          await sleep(15000);
          continue;
        }
        return JSON.parse(txt);
      } catch {
        const cfData = await fetchViaCfProxy(url);
        if (cfData) return cfData;
        if (i === maxRetries - 1) {
          return vnProxy.fetchJson(url, 2);
        }
        await sleep(1500 * (i + 1));
      }
    }
    return null;
  }

  async function isTitleAliveOnApi(vId, mediaType) {
    const detailType = mediaType === "series" || mediaType === "tvseries" ? "tvseries" : "movie";
    const d = await fetchWithRetry(`${P4K_API_BASE}single_details?type=${detailType}&id=${vId}`, 2);
    return !!(d && d.videos_id);
  }

  /**
   * Tải toàn bộ danh mục phân trang từ API Phim4K với limit=100.
   * Chỉ đánh dấu complete=true khi đọc sạch tới EOF mà không gãy trang nào.
   */
  async function fetchFullCatalogList(endpoint, { limit = 100, maxPages = 200, delayMs = 60 } = {}) {
    const items = [];
    let pagesFetched = 0;
    let complete = false;

    for (let page = 1; page <= maxPages; page++) {
      const sep = endpoint.includes("?") ? "&" : "?";
      const data = await fetchWithRetry(`${P4K_API_BASE}${endpoint}${sep}limit=${limit}&page=${page}`, 4);
      if (!Array.isArray(data)) {
        return { items, complete: false, errorPage: page, pagesFetched };
      }
      pagesFetched++;
      if (data.length === 0) {
        complete = true;
        break;
      }
      items.push(...data);
      if (data.length < limit) {
        complete = true;
        break;
      }
      if (delayMs > 0) await sleep(delayMs);
    }

    return { items, complete, errorPage: complete ? null : maxPages, pagesFetched };
  }

  return { fetchWithRetry, fetchFullCatalogList, isTitleAliveOnApi };
}

export class BatchCollector {
  constructor(client, batchSize = 50, dryRun = false) {
    this.client = client;
    this.batchSize = batchSize;
    this.dryRun = dryRun;
    this.queue = [];
    this.totalFlushed = 0;
  }

  async add(stmt) {
    if (this.dryRun) return;
    this.queue.push(stmt);
    if (this.queue.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush() {
    if (this.dryRun || this.queue.length === 0) return;
    const batch = [...this.queue];
    this.queue = [];
    try {
      await this.client.batch(batch);
      this.totalFlushed += batch.length;
    } catch (e) {
      console.error(`Lỗi thực thi batch ${batch.length} câu lệnh:`, e.message);
      for (const s of batch) {
        try {
          await this.client.exec(s.sql, s.args || []);
          this.totalFlushed++;
        } catch (singleErr) {
          console.error(`Lỗi câu lệnh đơn: ${s.sql}`, singleErr.message);
        }
      }
    }
  }
}

export const UPSERT_TITLE_SQL = `INSERT INTO titles (id, type, title, title_clean, imdb_id, tmdb_id, poster_url, thumbnail_url, release_year, imdb_rating, quality, genres, overview, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  type = excluded.type,
  title = excluded.title,
  title_clean = excluded.title_clean,
  imdb_id = COALESCE(excluded.imdb_id, titles.imdb_id),
  tmdb_id = COALESCE(excluded.tmdb_id, titles.tmdb_id),
  poster_url = COALESCE(excluded.poster_url, titles.poster_url),
  thumbnail_url = COALESCE(excluded.thumbnail_url, titles.thumbnail_url),
  release_year = COALESCE(excluded.release_year, titles.release_year),
  imdb_rating = COALESCE(excluded.imdb_rating, titles.imdb_rating),
  quality = COALESCE(excluded.quality, titles.quality),
  genres = COALESCE(excluded.genres, titles.genres),
  overview = COALESCE(excluded.overview, titles.overview),
  updated_at = excluded.updated_at`;

export const UPSERT_STREAM_SQL = `INSERT INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at, hf_id, hf_checked_at, hf_status, hf_fails, rail_size)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(title_id, season, episode, file_id) DO UPDATE SET
  label = COALESCE(excluded.label, episodes_streams.label),
  container = COALESCE(excluded.container, episodes_streams.container),
  quality = COALESCE(excluded.quality, episodes_streams.quality),
  subtitles = COALESCE(excluded.subtitles, episodes_streams.subtitles),
  created_at = excluded.created_at,
  hf_id = COALESCE(episodes_streams.hf_id, excluded.hf_id),
  hf_checked_at = COALESCE(episodes_streams.hf_checked_at, excluded.hf_checked_at),
  hf_status = COALESCE(episodes_streams.hf_status, excluded.hf_status),
  hf_fails = COALESCE(episodes_streams.hf_fails, excluded.hf_fails),
  rail_size = COALESCE(episodes_streams.rail_size, excluded.rail_size)`;

function buildStreamStmt({ vId, season, episode, fid, label, container, quality, subtitles, nowMs, preservedHfMap }) {
  const hf = preservedHfMap ? preservedHfMap.get(fid) : null;
  return {
    sql: UPSERT_STREAM_SQL,
    args: [
      vId,
      season,
      episode,
      fid,
      label,
      container,
      quality,
      subtitles,
      nowMs,
      hf ? hf.hf_id ?? null : null,
      hf ? hf.hf_checked_at ?? null : null,
      hf ? hf.hf_status ?? null : null,
      hf ? hf.hf_fails ?? null : null,
      hf ? hf.rail_size ?? null : null,
    ],
  };
}

/**
 * Đồng bộ 1 phim từ dữ liệu single_details vào bảng titles + episodes_streams.
 * Smart Dedup an toàn: chỉ gộp/xóa ID trùng trong DB khi xác nhận ID cũ đó đã chết trên API.
 */
export async function syncTitleRecord({
  client,
  collector,
  item,
  detail,
  type,
  itemUpdatedAt,
  force = false,
  isDeadOldIdFn,
  preservedHfMap = null,
  fallbackImdbId = null,
  stats,
  nowMs = Date.now(),
}) {
  const vId = Number(detail.videos_id);
  const cleanTitle = normalizeText(detail.title);
  const year = parseInt(String(detail.release || "").slice(0, 4), 10) || null;
  let imdbId = detail.imdb_id ? (String(detail.imdb_id).startsWith("tt") ? String(detail.imdb_id) : "tt" + detail.imdb_id) : fallbackImdbId || null;
  const tmdbId = detail.tmdb_id ? parseInt(detail.tmdb_id, 10) || null : null;
  const rating = (detail.imdb_rating || (item && item.imdb_rating)) ? parseFloat(detail.imdb_rating || item.imdb_rating) || null : null;
  const genres = Array.isArray(detail.genre) ? JSON.stringify(detail.genre.map((g) => g.name).filter(Boolean)) : null;

  // 1. Smart Dedup an toàn: giữ tất cả ID còn sống trên API; chỉ xóa bản trùng trong DB nếu đã chết trên API.
  let hadMerge = false;
  if (cleanTitle && typeof isDeadOldIdFn === "function") {
    const dupRes = await client.exec(
      "SELECT id, title, imdb_id FROM titles WHERE title_clean = ? AND release_year IS ? AND type = ? AND id != ? LIMIT 10",
      [cleanTitle, year, type, vId]
    );
    for (const oldRow of dupRes.rows || []) {
      const oldId = Number(oldRow.id);
      if (!(await isDeadOldIdFn(oldId, type, oldRow))) continue;
      if (!imdbId && oldRow.imdb_id) imdbId = oldRow.imdb_id;
      if (preservedHfMap) {
        const hfRows = await client.exec(
          "SELECT file_id, hf_id, hf_checked_at, hf_status, hf_fails, rail_size FROM episodes_streams WHERE title_id = ? AND (hf_id IS NOT NULL OR hf_checked_at IS NOT NULL OR hf_status IS NOT NULL)",
          [oldId]
        );
        for (const hr of hfRows.rows || []) {
          if (hr.file_id && (!preservedHfMap.has(hr.file_id) || (!preservedHfMap.get(hr.file_id).hf_id && hr.hf_id))) {
            preservedHfMap.set(hr.file_id, hr);
          }
        }
      }
      console.log(`  [SMART MERGE] "${detail.title}" -> Xóa ID cũ đã chết trên API (${oldId}), chuyển sang ID sống ${vId}`);
      await collector.add({ sql: "DELETE FROM episodes_streams WHERE title_id = ?", args: [oldId] });
      await collector.add({ sql: "DELETE FROM titles WHERE id = ?", args: [oldId] });
      stats.duplicatesCleaned++;
      if (stats.deletedIds) stats.deletedIds.add(oldId);
      hadMerge = true;
    }
  }

  const existRes = await client.exec(
    "SELECT id, type, quality, poster_url, updated_at FROM titles WHERE id = ?",
    [vId]
  );
  const existingTitle = existRes.rows && existRes.rows.length > 0 ? existRes.rows[0] : null;
  const typeChanged = existingTitle && existingTitle.type && existingTitle.type !== type;

  const titleArgs = [
    vId,
    type,
    cleanText(detail.title),
    cleanTitle,
    imdbId,
    tmdbId,
    cleanText(detail.poster_url || (item && item.poster_url)),
    cleanText(detail.thumbnail_url || (item && item.thumbnail_url)),
    year,
    rating,
    cleanText(detail.video_quality || (item && item.video_quality)),
    genres,
    cleanText(detail.description || (item && item.description)),
    itemUpdatedAt,
  ];

  if (type === "movie") {
    const apiVideos = Array.isArray(detail.videos) ? detail.videos : [];
    const streamRes = typeChanged
      ? { rows: [] }
      : await client.exec("SELECT id, file_id FROM episodes_streams WHERE title_id = ?", [vId]);
    const existingStreams = streamRes.rows || [];
    const existingFids = new Set(existingStreams.map((r) => r.file_id));

    const validFids = new Set();
    const newVideos = [];
    for (const v of apiVideos) {
      const fid = fileNameFrom(v.file_url);
      if (!fid) continue;
      validFids.add(fid);
      if (!existingFids.has(fid)) {
        newVideos.push(v);
        existingFids.add(fid);
      }
    }

    const staleIds =
      !typeChanged && !force && validFids.size > 0
        ? existingStreams.filter((r) => !r.file_id || !validFids.has(r.file_id)).map((r) => r.id)
        : [];

    if (!force && existingTitle && !typeChanged && existingStreams.length > 0 && newVideos.length === 0 && staleIds.length === 0 && !hadMerge) {
      stats.titlesSkipped++;
      return { status: "skipped" };
    }

    await collector.add({ sql: UPSERT_TITLE_SQL, args: titleArgs });
    stats.titlesSynced++;

    if (!existingTitle || hadMerge || typeChanged || force) {
      if (existingTitle) {
        await collector.add({ sql: "DELETE FROM episodes_streams WHERE title_id = ?", args: [vId] });
      }
      for (const v of apiVideos) {
        const fid = fileNameFrom(v.file_url);
        if (!fid) continue;
        await collector.add(
          buildStreamStmt({
            vId,
            season: 1,
            episode: 1,
            fid,
            label: cleanText(v.label),
            container: cleanText(v.file_type),
            quality: cleanText(detail.video_quality),
            subtitles: Array.isArray(v.subtitle) && v.subtitle.length ? JSON.stringify(v.subtitle) : null,
            nowMs,
            preservedHfMap,
          })
        );
        stats.streamsInserted++;
      }
    } else {
      for (const sId of staleIds) {
        await collector.add({ sql: "DELETE FROM episodes_streams WHERE id = ?", args: [sId] });
      }
      for (const v of newVideos) {
        const fid = fileNameFrom(v.file_url);
        if (!fid) continue;
        await collector.add(
          buildStreamStmt({
            vId,
            season: 1,
            episode: 1,
            fid,
            label: cleanText(v.label),
            container: cleanText(v.file_type),
            quality: cleanText(detail.video_quality),
            subtitles: Array.isArray(v.subtitle) && v.subtitle.length ? JSON.stringify(v.subtitle) : null,
            nowMs,
            preservedHfMap,
          })
        );
        stats.streamsInserted++;
      }
    }
    if (hadMerge) await collector.flush();
    return { status: "synced" };
  }

  // type === "series"
  const epRes =
    typeChanged || force
      ? { rows: [] }
      : await client.exec("SELECT id, season, episode, file_id FROM episodes_streams WHERE title_id = ?", [vId]);
  const existingRows = epRes.rows || [];
  const existingEpKeys = new Set(existingRows.map((r) => `${r.season}:${r.episode}:${r.file_id}`));

  const validFids = new Set();
  const validEpKeys = new Set();
  const pendingEpisodes = [];
  if (Array.isArray(detail.season)) {
    for (const s of detail.season) {
      if (!s || !Array.isArray(s.episodes)) continue;
      for (const ep of s.episodes) {
        const fid = fileNameFrom(ep.file_url || ep.url);
        if (!fid) continue;
        validFids.add(fid);

        const info = episodeInfo(s, ep);
        if (info.episode === null) continue;

        const from = info.episode;
        const to = info.episodeEnd ? info.episodeEnd : from;
        const span = Math.min(to, from + 60);

        for (let epNum = from; epNum <= span; epNum++) {
          const epKey = `${info.season}:${epNum}:${fid}`;
          validEpKeys.add(epKey);
          if (existingEpKeys.has(epKey)) continue;

          pendingEpisodes.push({
            season: info.season,
            episode: epNum,
            fid,
            label: cleanText(ep.episodes_name || s.seasons_name),
            file_type: cleanText(ep.file_type),
            subtitles: Array.isArray(ep.subtitle) && ep.subtitle.length ? JSON.stringify(ep.subtitle) : null,
          });
          existingEpKeys.add(epKey);
        }
      }
    }
  }

  const staleIds = [];
  if (!typeChanged && !force) {
    for (const r of existingRows) {
      if (!validFids.has(r.file_id) || !validEpKeys.has(`${r.season}:${r.episode}:${r.file_id}`)) {
        staleIds.push(r.id);
      }
    }
  }

  if (!force && existingTitle && !typeChanged && pendingEpisodes.length === 0 && staleIds.length === 0 && !hadMerge) {
    stats.titlesSkipped++;
    return { status: "skipped" };
  }

  if (typeChanged || force) {
    await collector.add({ sql: "DELETE FROM episodes_streams WHERE title_id = ?", args: [vId] });
  }
  for (const sId of staleIds) {
    await collector.add({ sql: "DELETE FROM episodes_streams WHERE id = ?", args: [sId] });
  }

  await collector.add({ sql: UPSERT_TITLE_SQL, args: titleArgs });
  stats.titlesSynced++;

  for (const pe of pendingEpisodes) {
    await collector.add(
      buildStreamStmt({
        vId,
        season: pe.season,
        episode: pe.episode,
        fid: pe.fid,
        label: pe.label,
        container: pe.file_type,
        quality: cleanText(detail.video_quality),
        subtitles: pe.subtitles,
        nowMs,
        preservedHfMap,
      })
    );
    stats.streamsInserted++;
  }
  if (hadMerge) await collector.flush();
  return { status: "synced" };
}
