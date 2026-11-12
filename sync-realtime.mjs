// sync-realtime.mjs — Đồng bộ thời gian thực từ Phim4K API vào Turso DB.
// Cơ chế: Quét trang 1-2 mới nhất của movies & tvseries, tích hợp Smart Dedup (so khớp Tên + Năm)
// để tự động chuyển đổi ID cũ sang ID mới, dọn sạch trùng lặp, cập nhật poster và link stream mới nhất.

import fs from "fs";
import { tursoClient } from "./turso-http.mjs";
import { vnProxy, IS_GH_ACTIONS } from "./vn-proxy.mjs";

/* ═══════════════════════ CẤU HÌNH & KẾT NỐI DB ═══════════════════════ */

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

const client = tursoClient({ url: dbUrl, token: dbToken });

/* ═══════════════════════ THAM SỐ DÒNG LỆNH ═══════════════════════ */

const args = process.argv.slice(2);
function getArg(name, def = null) {
  const idx = args.indexOf("--" + name);
  if (idx !== -1 && idx + 1 < args.length) return args[idx + 1];
  return def;
}
function hasFlag(name) {
  return args.includes("--" + name);
}

const PAGES = Math.max(1, parseInt(getArg("pages", "2"), 10) || 2);
const DELAY_MS = Math.max(100, parseInt(getArg("delay", "250"), 10) || 250);
const BATCH_SIZE = Math.max(10, parseInt(getArg("batch-size", "50"), 10) || 50);
const DRY_RUN = hasFlag("dry-run");
const FORCE = hasFlag("force");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const startTime = Date.now();

function cleanText(v) { return v ? String(v).trim() : null; }
function normalizeText(v) {
  return String(v || "").replace(/[đĐ]/g, "d").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}
function fileNameFrom(url) {
  const s = String(url || "").trim();
  if (!s) return "";
  return s.slice(s.lastIndexOf("/") + 1).split("?")[0].replace(/\.[^.]+$/, "");
}
function looksLikeYear(n) { return n >= 1900 && n <= 2100; }

function episodeInfo(seasonEntry, ep) {
  const episodeName = String((ep && (ep.episodes_name || ep.name || ep.title)) || "");
  const file = String((ep && (ep.file_url || ep.url)) || "").split("?")[0].split("/").pop();
  const text = episodeName + " " + file;
  let season = null, episode = null, episodeEnd = null;

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
    const a = parseInt(batch[1], 10), b = parseInt(batch[2], 10);
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
    const sm = label.match(/season\s*0*(\d+)/i) || label.match(/m[uù]a\s*0*(\d+)/i) ||
               label.match(/ph[aầ]n\s*0*(\d+)/i) || label.match(/(?:^|[^a-z0-9])s\s*0*(\d{1,2})(?!\d)/i);
    if (sm) season = parseInt(sm[1], 10);
  }

  if (season === null) season = 1;
  if (episode === null && seasonEntry && Array.isArray(seasonEntry.episodes) && seasonEntry.episodes.length === 1) {
    episode = 1;
  }
  return { season, episode, episodeEnd };
}

/* ═══════════════════════ GỌI API PHIM4K CÓ RETRY & PROXY ═══════════════════════ */

const headers = {
  "Accept": "application/json",
  "API-KEY": "bbbb411dea44849",
  "Authorization": "Basic " + Buffer.from("admin:").toString("base64"),
  "User-Agent": "okhttp/4.9.0",
};

async function fetchViaCfProxy(url) {
  try {
    const proxyUrl = url.replace(
      "https://api.phim4k.space/rest-api/v130/",
      "https://phim4k-addon.notthingp.workers.dev/api-proxy/"
    );
    const r = await fetch(proxyUrl, { signal: AbortSignal.timeout(10000) });
    if (r.ok) {
      const data = await r.json();
      if (data && (Array.isArray(data) || data.videos_id || data.title)) return data;
    }
  } catch {}
  return null;
}

let globalCoolUntil = 0;

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
      const r = await fetch(url, { headers, signal: AbortSignal.timeout(8000) });
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
        if (r.status >= 500) { await sleep(1500 * (i + 1)); continue; }
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
    } catch (e) {
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

/* ═══════════════════════ BATCH COLLECTOR ═══════════════════════ */

class BatchCollector {
  constructor(client, batchSize = 50) {
    this.client = client;
    this.batchSize = batchSize;
    this.queue = [];
    this.totalFlushed = 0;
  }

  async add(stmt) {
    if (DRY_RUN) return;
    this.queue.push(stmt);
    if (this.queue.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush() {
    if (DRY_RUN || this.queue.length === 0) return;
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

/* ═══════════════════════ CHƯƠNG TRÌNH ĐỒNG BỘ ═══════════════════════ */

async function main() {
  console.log("=============================================================================");
  console.log(">>> ĐỒNG BỘ THỜI GIAN THỰC PHIM4K -> TURSO DATABASE (SMART DEDUP) <<<");
  console.log(`Chế độ: ${DRY_RUN ? "DRY-RUN (Chỉ đọc)" : "GHI DATABASE"} | Số trang quét: ${PAGES}`);
  console.log("=============================================================================\n");

  const collector = new BatchCollector(client, BATCH_SIZE);

  let totalTitlesSynced = 0;
  let totalTitlesSkipped = 0;
  let totalDuplicatesCleaned = 0;
  let totalStreamsInserted = 0;

  for (const cat of [
    { name: "Phim lẻ (movies)", path: "movies", type: "movie" },
    { name: "Phim bộ (tvseries)", path: "tvseries", type: "series" },
  ]) {
    console.log(`\n--- BẮT ĐẦU ĐỒNG BỘ: ${cat.name.toUpperCase()} ---`);
    for (let page = 1; page <= PAGES; page++) {
      const listUrl = `https://api.phim4k.space/rest-api/v130/${cat.path}?page=${page}`;
      const list = await fetchWithRetry(listUrl);
      if (!Array.isArray(list) || list.length === 0) {
        console.log(`Trang ${page} không có phim nào, kết thúc danh mục ${cat.path}.`);
        break;
      }

      console.log(`[${cat.path}] Trang ${page}: Tìm thấy ${list.length} phim từ Phim4K.`);

      for (let i = 0; i < list.length; i++) {
        const item = list[i];
        const vId = Number(item.videos_id);
        if (!vId) continue;

        await sleep(DELAY_MS);
        const detailType = cat.type === "movie" ? "movie" : "tvseries";
        const detailUrl = `https://api.phim4k.space/rest-api/v130/single_details?type=${detailType}&id=${vId}`;
        const d = await fetchWithRetry(detailUrl);
        if (!d || !d.videos_id) {
          console.warn(`  [${vId}] Không lấy được chi tiết, bỏ qua.`);
          continue;
        }

        const cleanTitle = normalizeText(d.title);
        const year = parseInt(String(d.release || "").slice(0, 4), 10) || null;
        const imdbId = d.imdb_id ? (String(d.imdb_id).startsWith("tt") ? String(d.imdb_id) : "tt" + d.imdb_id) : null;
        const tmdbId = d.tmdb_id ? parseInt(d.tmdb_id, 10) || null : null;
        const rating = (d.imdb_rating || item.imdb_rating) ? parseFloat(d.imdb_rating || item.imdb_rating) || null : null;
        const genres = Array.isArray(d.genre) ? JSON.stringify(d.genre.map((g) => g.name).filter(Boolean)) : null;

        // 1. SMART DEDUP: Tìm kiếm bản ghi cũ trùng Tên + Năm trong DB (khác vId)
        let hadMerge = false;
        if (cleanTitle) {
          const dupRes = await client.exec(
            "SELECT id, title FROM titles WHERE title_clean = ? AND release_year = ? AND id != ? LIMIT 5",
            [cleanTitle, year, vId]
          );
          const oldDups = dupRes.rows || [];
          if (oldDups.length > 0) {
            for (const oldRow of oldDups) {
              const oldId = oldRow.id;
              console.log(`  [SMART MERGE] Gộp phim: "${d.title}" -> Xoá ID cũ ${oldId}, cập nhật sang ID mới ${vId}`);
              await collector.add({ sql: "DELETE FROM episodes_streams WHERE title_id = ?", args: [oldId] });
              await collector.add({ sql: "DELETE FROM titles WHERE id = ?", args: [oldId] });
              totalDuplicatesCleaned++;
            }
            hadMerge = true;
          }
        }

        // Kiểm tra xem title này đã tồn tại trong DB chưa
        const existRes = await client.exec(
          "SELECT id, type, quality, poster_url, updated_at FROM titles WHERE id = ?",
          [vId]
        );
        const existingTitle = existRes.rows && existRes.rows.length > 0 ? existRes.rows[0] : null;
        const typeChanged = existingTitle && existingTitle.type && existingTitle.type !== cat.type;

        // Xử lý theo loại: Phim lẻ hoặc Phim bộ
        if (cat.type === "movie") {
          const apiVideos = Array.isArray(d.videos) ? d.videos : [];

          // Lấy danh sách stream hiện tại trong DB
          const streamRes = typeChanged ? { rows: [] } : await client.exec(
            "SELECT id, file_id FROM episodes_streams WHERE title_id = ?",
            [vId]
          );
          const existingStreams = streamRes.rows || [];
          const existingFids = new Set(existingStreams.map((r) => r.file_id));

          const newVideos = apiVideos.filter((v) => {
            const fid = fileNameFrom(v.file_url);
            return fid && !existingFids.has(fid);
          });

          // SMART SKIP: Nếu phim đã có trong DB, có stream, không có stream mới, không đổi loại, và không có merge trùng lặp
          if (!FORCE && existingTitle && !typeChanged && existingStreams.length > 0 && newVideos.length === 0 && !hadMerge) {
            console.log(`  [Phim lẻ] [${vId}] ${d.title}: Đã có sẵn (${existingStreams.length} stream, không đổi, bỏ qua).`);
            totalTitlesSkipped++;
            continue;
          }

          // Cập nhật/Thêm mới Title
          await collector.add({
            sql: `INSERT INTO titles (id, type, title, title_clean, imdb_id, tmdb_id, poster_url, thumbnail_url, release_year, imdb_rating, quality, genres, overview, updated_at)
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
                    updated_at = excluded.updated_at`,
            args: [
              vId,
              cat.type,
              cleanText(d.title),
              cleanTitle,
              imdbId,
              tmdbId,
              cleanText(d.poster_url || item.poster_url),
              cleanText(d.thumbnail_url || item.thumbnail_url),
              year,
              rating,
              cleanText(d.video_quality || item.video_quality),
              genres,
              cleanText(d.description || item.description),
              Date.now(),
            ],
          });
          totalTitlesSynced++;

          // Nạp stream
          if (!existingTitle || hadMerge || typeChanged || FORCE) {
            // Phim mới hoàn toàn hoặc vừa merge hoặc đổi loại hoặc ép buộc: làm mới toàn bộ
            await collector.add({ sql: "DELETE FROM episodes_streams WHERE title_id = ?", args: [vId] });
            for (const v of apiVideos) {
              const fid = fileNameFrom(v.file_url);
              if (!fid) continue;
              await collector.add({
                sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                args: [
                  vId, 1, 1, fid,
                  cleanText(v.label),
                  cleanText(v.file_type),
                  cleanText(d.video_quality),
                  Array.isArray(v.subtitle) && v.subtitle.length ? JSON.stringify(v.subtitle) : null,
                  Date.now(),
                ],
              });
              totalStreamsInserted++;
            }
            console.log(`  [Phim lẻ] [${vId}] ${d.title}: +${apiVideos.length} stream mới.`);
          } else if (newVideos.length > 0) {
            // Phim cũ có thêm stream mới: chỉ chèn stream mới
            for (const v of newVideos) {
              const fid = fileNameFrom(v.file_url);
              if (!fid) continue;
              await collector.add({
                sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                args: [
                  vId, 1, 1, fid,
                  cleanText(v.label),
                  cleanText(v.file_type),
                  cleanText(d.video_quality),
                  Array.isArray(v.subtitle) && v.subtitle.length ? JSON.stringify(v.subtitle) : null,
                  Date.now(),
                ],
              });
              totalStreamsInserted++;
            }
            console.log(`  [Phim lẻ] [${vId}] ${d.title}: +${newVideos.length} stream mới (đã có ${existingStreams.length} stream).`);
          }

        } else if (cat.type === "series") {
          if (typeChanged) {
            console.log(`  [CHUYỂN LOẠI] [${vId}] ${d.title}: Chuyển từ "${existingTitle.type}" sang "series" -> Xoá sạch stream cũ.`);
            await collector.add({ sql: "DELETE FROM episodes_streams WHERE title_id = ?", args: [vId] });
          }

          // Lấy danh sách stream đã có của series để không nạp đè trùng lặp và phát hiện stream rác
          const epRes = typeChanged ? { rows: [] } : await client.exec(
            "SELECT id, season, episode, file_id FROM episodes_streams WHERE title_id = ?",
            [vId]
          );
          const existingRows = epRes.rows || [];
          const existingEpKeys = new Set(
            existingRows.map((r) => `${r.season}:${r.episode}:${r.file_id}`)
          );

          const validFids = new Set();
          const validEpKeys = new Set();
          const pendingEpisodes = [];
          if (Array.isArray(d.season)) {
            for (const s of d.season) {
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

          // Dọn dẹp stream rác/cũ (stream có trong DB nhưng không còn trong API hoặc phim lẻ cũ còn sót lại)
          const staleIds = [];
          if (!typeChanged) {
            for (const r of existingRows) {
              if (!validFids.has(r.file_id) || !validEpKeys.has(`${r.season}:${r.episode}:${r.file_id}`)) {
                staleIds.push(r.id);
              }
            }
            if (staleIds.length > 0) {
              for (const sId of staleIds) {
                await collector.add({ sql: "DELETE FROM episodes_streams WHERE id = ?", args: [sId] });
              }
              console.log(`  [Phim bộ] [${vId}] ${d.title}: Đã dọn dẹp ${staleIds.length} stream/tập rác không hợp lệ.`);
            }
          }

          // SMART SKIP: Nếu phim bộ đã có trong DB, không đổi loại, không có tập mới, không có stream rác, và không có merge trùng lặp
          if (!FORCE && existingTitle && !typeChanged && pendingEpisodes.length === 0 && staleIds.length === 0 && !hadMerge) {
            console.log(`  [Phim bộ] [${vId}] ${d.title}: Đã có sẵn (${existingRows.length} tập/stream, không có tập mới, bỏ qua).`);
            totalTitlesSkipped++;
            continue;
          }

          // Cập nhật/Thêm mới Title
          await collector.add({
            sql: `INSERT INTO titles (id, type, title, title_clean, imdb_id, tmdb_id, poster_url, thumbnail_url, release_year, imdb_rating, quality, genres, overview, updated_at)
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
                    updated_at = excluded.updated_at`,
            args: [
              vId,
              cat.type,
              cleanText(d.title),
              cleanTitle,
              imdbId,
              tmdbId,
              cleanText(d.poster_url || item.poster_url),
              cleanText(d.thumbnail_url || item.thumbnail_url),
              year,
              rating,
              cleanText(d.video_quality || item.video_quality),
              genres,
              cleanText(d.description || item.description),
              Date.now(),
            ],
          });
          totalTitlesSynced++;

          // Nạp các tập mới
          for (const pe of pendingEpisodes) {
            await collector.add({
              sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              args: [
                vId,
                pe.season,
                pe.episode,
                pe.fid,
                pe.label,
                pe.file_type,
                cleanText(d.video_quality),
                pe.subtitles,
                Date.now(),
              ],
            });
            totalStreamsInserted++;
          }
          console.log(`  [Phim bộ] [${vId}] ${d.title}: +${pendingEpisodes.length} tập/stream mới.`);
        }
      }
    }
  }

  // Cập nhật timestamp lần đồng bộ gần nhất vào bảng meta
  if (!DRY_RUN) {
    await collector.add({
      sql: "INSERT OR REPLACE INTO meta (key, val) VALUES ('sync_realtime_at', ?)",
      args: [String(Date.now())],
    });
  }

  await collector.flush();

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log("\n============================ TỔNG KẾT ĐỒNG BỘ ============================");
  console.log(`Thời gian thực thi: ${elapsed}s`);
  console.log(`Phim đã đồng bộ mới/cập nhật: ${totalTitlesSynced}`);
  console.log(`Phim đã bỏ qua (không đổi): ${totalTitlesSkipped}`);
  console.log(`Bản ghi cũ trùng lặp đã dọn (Smart Merge): ${totalDuplicatesCleaned}`);
  console.log(`Tổng luồng stream/tập đã nạp: ${totalStreamsInserted}`);
  console.log(`Tổng câu lệnh SQL thực thi: ${collector.totalFlushed}`);
  console.log("============================================================================");
}

main().catch((e) => {
  console.error("Lỗi đồng bộ thời gian thực:", e);
  process.exit(1);
});
