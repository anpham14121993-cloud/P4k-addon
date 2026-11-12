// backfill.mjs — Crawl toàn bộ kho phim lẻ và phim bộ từ Phim4K nạp vào Turso DB.
// Hỗ trợ resume (bỏ qua phim đã có), batch insert và cấu hình qua CLI/GitHub Actions.

import fs from "fs";
import { tursoClient } from "./turso-http.mjs";
import { vnProxy, IS_GH_ACTIONS } from "./vn-proxy.mjs";

/* ═══════════════════════ CẤU HÌNH & KẾT NỐI DB ═══════════════════════ */

let dbUrl = process.env.TURSO_DATABASE_URL || "";
let dbToken = process.env.TURSO_AUTH_TOKEN || "";

if ((!dbUrl || !dbToken) && fs.existsSync("/sdcard/Download/P4k/.env.turso")) {
  const envTurso = fs.readFileSync("/sdcard/Download/P4k/.env.turso", "utf8");
  for (const line of envTurso.split("\n")) {
    if (line.startsWith("TURSO_DATABASE_URL=")) dbUrl = line.split("=")[1].trim();
    if (line.startsWith("TURSO_AUTH_TOKEN=")) dbToken = line.split("=")[1].trim();
  }
}

if (!dbUrl || !dbToken) {
  console.error("Lỗi: Thiếu biến môi trường TURSO_DATABASE_URL hoặc TURSO_AUTH_TOKEN!");
  process.exit(1);
}

const client = tursoClient({ url: dbUrl, token: dbToken });

/* ═══════════════════════ THAM SỐ DÒNG LỆNH (CLI) ═══════════════════════ */

const args = process.argv.slice(2);
function getArg(name, def = null) {
  const idx = args.indexOf("--" + name);
  if (idx !== -1 && idx + 1 < args.length) return args[idx + 1];
  return def;
}
function hasFlag(name) {
  return args.includes("--" + name);
}

const TYPE = (getArg("type", "all") || "all").toLowerCase(); // all | movies | series | tvseries
const FROM_PAGE = Math.max(1, parseInt(getArg("from-page", "1"), 10) || 1);
const TO_PAGE = parseInt(getArg("to-page", "0"), 10) || 0; // 0 = cào đến khi hết trang
const FORCE = hasFlag("force"); // Nếu bật: ghi đè cả phim đã có trong DB
const DELAY_MS = Math.max(100, parseInt(getArg("delay", "280"), 10) || 280);
const BATCH_SIZE = Math.max(10, parseInt(getArg("batch-size", "50"), 10) || 50);
const BUDGET_SEC = parseInt(getArg("budget", "0"), 10) || 0;

/* ═══════════════════════ TIỆN ÍCH & PARSER ═══════════════════════ */

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

/* ═══════════════════════ GỌI API PHIM4K CÓ RETRY ═══════════════════════ */

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

async function fetchWithRetry(url, maxRetries = 3) {
  // 1. Nếu đã có activeProxy được xác nhận sống -> dùng tiếp
  if (vnProxy.activeProxy) {
    const data = await vnProxy.fetchJson(url, maxRetries);
    if (data) return data;
  }

  // 2. Thử gọi trực tiếp trước (kể cả trên GitHub Actions — okhttp + auth thường qua được)
  for (let i = 0; i < maxRetries; i++) {
    try {
      const r = await fetch(url, { headers, signal: AbortSignal.timeout(8000) });
      if (r.status === 429) {
        console.warn(`[429 Rate Limit] Đang đợi ${2000 * (i + 1)}ms...`);
        await sleep(2000 * (i + 1));
        continue;
      }
      if (r.status === 403) {
        console.warn(`[HTTP 403] Bị chặn trực tiếp. Thử qua Cloudflare Worker API proxy...`);
        const cfData = await fetchViaCfProxy(url);
        if (cfData) return cfData;
        console.warn(`[CF Proxy] Thất bại, thử xoay Proxy VN...`);
        return vnProxy.fetchJson(url, 2);
      }
      if (!r.ok) {
        if (r.status >= 500) { await sleep(1000 * (i + 1)); continue; }
        const cfData = await fetchViaCfProxy(url);
        if (cfData) return cfData;
        return null;
      }
      const txt = await r.text();
      if (txt.includes("rate limited") || txt.includes("Cloudflare")) {
        console.warn(`[Cloudflare WAF] Bị chặn tạm thời: ${txt.slice(0, 150)}`);
        const cfData = await fetchViaCfProxy(url);
        if (cfData) return cfData;
        await sleep(2000 * (i + 1));
        continue;
      }
      return JSON.parse(txt);
    } catch (e) {
      const cfData = await fetchViaCfProxy(url);
      if (cfData) return cfData;
      if (i === maxRetries - 1) {
        return vnProxy.fetchJson(url, 2);
      }
      await sleep(1000 * (i + 1));
    }
  }
  return null;
}

/* ═══════════════════════ BỘ QUẢN LÝ BATCH TURSO ═══════════════════════ */

class BatchCollector {
  constructor(client, batchSize) {
    this.client = client;
    this.batchSize = batchSize;
    this.queue = [];
    this.totalFlushed = 0;
  }

  add(stmt) {
    this.queue.push(stmt);
  }

  async flush() {
    if (this.queue.length === 0) return;
    const toSend = this.queue;
    this.queue = [];
    await this.client.batch(toSend);
    this.totalFlushed += toSend.length;
  }

  async checkAutoFlush() {
    if (this.queue.length >= this.batchSize) {
      await this.flush();
    }
  }
}

/* ═══════════════════════ TIẾN TRÌNH CRAWL CHÍNH ═══════════════════════ */

async function main() {
  console.log("================================================================================");
  console.log("🚀 KHỞI ĐỘNG TIẾN TRÌNH BACKFILL PHIM4K -> TURSO DATABASE");
  console.log(`Loại: ${TYPE} | Từ trang: ${FROM_PAGE} | Đến trang: ${TO_PAGE || "Hết kho"}`);
  console.log(`Ghi đè (--force): ${FORCE} | Delay: ${DELAY_MS}ms | Batch size: ${BATCH_SIZE}`);
  console.log("================================================================================\n");

  // 1. Tải danh sách ID đã có trong DB để tránh query lại nếu không --force
  console.log("1. Đang kiểm tra các ID đã có sẵn trong Turso DB...");
  const existingRes = await client.exec("SELECT id, type FROM titles");
  const existingTypes = new Map((existingRes.rows || []).map((r) => [Number(r.id), r.type]));
  console.log(`=> Đã có sẵn ${existingTypes.size} bộ phim trong database.\n`);

  const collector = new BatchCollector(client, BATCH_SIZE);

  let totalTitlesInserted = 0;
  let totalStreamsInserted = 0;
  let totalSkipped = 0;

  function isOverBudget() {
    if (BUDGET_SEC <= 0) return false;
    return (Date.now() - startTime) / 1000 >= BUDGET_SEC;
  }

  // Graceful exit handler
  async function gracefulExit(signal) {
    console.log(`\nNhận tín hiệu ${signal}. Đang flush dữ liệu còn tồn đọng vào DB...`);
    await collector.flush();
    printSummary();
    process.exit(0);
  }
  process.on("SIGINT", () => gracefulExit("SIGINT"));
  process.on("SIGTERM", () => gracefulExit("SIGTERM"));

  function printSummary() {
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    console.log("\n============================ TỔNG KẾT TIẾN TRÌNH ============================");
    console.log(`Thời gian chạy: ${elapsed}s`);
    console.log(`Phim nạp mới/cập nhật: ${totalTitlesInserted}`);
    console.log(`Bản stream/tập nạp: ${totalStreamsInserted}`);
    console.log(`Phim bỏ qua (đã có): ${totalSkipped}`);
    console.log(`Tổng câu lệnh SQL thực thi: ${collector.totalFlushed}`);
    console.log("=============================================================================");
  }

  // 2. Hàm cào từng danh mục (movies hoặc tvseries)
  async function crawlCategory(listPath, titleType) {
    const detailType = titleType === "movie" ? "movie" : "tvseries";
    console.log(`\n>>> BẮT ĐẦU CRAWL DANH MỤC: ${listPath.toUpperCase()} (Type: ${titleType}) <<<`);
    let page = FROM_PAGE;

    while (true) {
      if (TO_PAGE > 0 && page > TO_PAGE) {
        console.log(`Đã đạt giới hạn trang chỉ định (trang ${TO_PAGE}). Dừng danh mục ${listPath}.`);
        break;
      }
      if (isOverBudget()) {
        console.log(`Đã hết thời gian cho phép (--budget ${BUDGET_SEC}s). Dừng danh mục ${listPath}.`);
        break;
      }

      const listUrl = `https://api.phim4k.space/rest-api/v130/${listPath}?page=${page}`;
      const list = await fetchWithRetry(listUrl);

      if (!list || !Array.isArray(list) || list.length === 0) {
        console.log(`Trang ${page} không có phim nào hoặc đã tới trang cuối. Hoàn tất danh mục ${listPath}.`);
        break;
      }

      let pageInserted = 0;
      let pageSkipped = 0;
      let pageStreams = 0;

      for (let i = 0; i < list.length; i++) {
        if (isOverBudget()) break;

        const item = list[i];
        const vId = Number(item.videos_id);
        if (!vId) continue;

        // 1. Phim lẻ: Nếu không phải --force, không đổi loại và đã có trong DB -> Bỏ qua luôn 100%
        const prevType = existingTypes.get(vId);
        const typeChanged = prevType && prevType !== titleType;

        if (!FORCE && !typeChanged && titleType === "movie" && existingTypes.has(vId)) {
          pageSkipped++;
          totalSkipped++;
          continue;
        }

        // 2. Phim bộ: Nếu đã có trong DB và không đổi loại, lấy danh sách các tập đã lưu để không ghi đè lại
        let isExistingSeries = false;
        let existingRows = [];
        let existingEpKeys = new Set();
        if (!FORCE && !typeChanged && titleType === "series" && existingTypes.has(vId)) {
          isExistingSeries = true;
          try {
            const epRes = await client.exec(
              "SELECT id, season, episode, file_id FROM episodes_streams WHERE title_id = ?",
              [vId]
            );
            existingRows = epRes.rows || [];
            existingEpKeys = new Set(
              existingRows.map((r) => `${r.season}:${r.episode}:${r.file_id}`)
            );
          } catch (e) {
            console.error(`Lỗi kiểm tra tập cũ của phim ${vId}:`, e.message);
          }
        }

        await sleep(DELAY_MS);

        const detailUrl = `https://api.phim4k.space/rest-api/v130/single_details?type=${detailType}&id=${vId}`;
        let d = null;
        try {
          d = await fetchWithRetry(detailUrl);
        } catch (e) {
          console.warn(`[single_details] Lỗi khi tải chi tiết ID ${vId}: ${e.message}`);
        }
        if (!d || !d.videos_id) {
          console.warn(`  [${listPath}: p${page}] [${i + 1}/${list.length}] [${vId}] Không lấy được chi tiết, bỏ qua ID này.`);
          continue;
        }

        const cleanTitle = normalizeText(d.title);
        const year = parseInt(String(d.release || "").slice(0, 4), 10) || null;
        const imdbId = d.imdb_id ? (String(d.imdb_id).startsWith("tt") ? String(d.imdb_id) : "tt" + d.imdb_id) : null;
        const tmdbId = d.tmdb_id ? parseInt(d.tmdb_id, 10) || null : null;
        const rating = (d.imdb_rating || item.imdb_rating) ? parseFloat(d.imdb_rating || item.imdb_rating) || null : null;
        const genres = Array.isArray(d.genre) ? JSON.stringify(d.genre.map((g) => g.name).filter(Boolean)) : null;

        let itemStreams = 0;
        let staleStreamsCleaned = 0;
        const streamStatements = [];

        // Nếu title đổi loại (ví dụ: từ movie sang series hoặc ngược lại): xoá sạch stream cũ
        if (typeChanged) {
          console.log(`  [CHUYỂN LOẠI] [${vId}] ${d.title}: Chuyển từ "${prevType}" sang "${titleType}" -> Xoá sạch stream cũ.`);
          streamStatements.push({
            sql: "DELETE FROM episodes_streams WHERE title_id = ?",
            args: [vId],
          });
        }

        // Xử lý stream nguồn phát
        if (titleType === "movie") {
          if (Array.isArray(d.videos)) {
            for (const v of d.videos) {
              const fileId = fileNameFrom(v.file_url);
              if (!fileId) continue;

              streamStatements.push({
                sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                args: [
                  vId,
                  1,
                  1,
                  fileId,
                  cleanText(v.label || d.title),
                  cleanText(v.file_type),
                  cleanText(d.video_quality),
                  Array.isArray(v.subtitle) && v.subtitle.length ? JSON.stringify(v.subtitle) : null,
                  Date.now(),
                ],
              });
              itemStreams++;
            }
          }
        } else if (titleType === "series") {
          const validFids = new Set();
          const validEpKeys = new Set();
          if (Array.isArray(d.season)) {
            for (const s of d.season) {
              if (!Array.isArray(s.episodes)) continue;
              for (const ep of s.episodes) {
                const fileId = fileNameFrom(ep.file_url);
                if (!fileId) continue;
                validFids.add(fileId);

                const info = episodeInfo(s, ep);
                if (info.episode === null) continue;

                const from = info.episode;
                const to = info.episodeEnd ? info.episodeEnd : from;
                const span = Math.min(to, from + 60);

                for (let epNum = from; epNum <= span; epNum++) {
                  const epKey = `${info.season}:${epNum}:${fileId}`;
                  validEpKeys.add(epKey);
                  if (existingEpKeys.has(epKey)) {
                    // Tập này ĐÃ CÓ trong DB rồi -> Bỏ qua, không ghi đè lại vào Turso!
                    continue;
                  }

                  streamStatements.push({
                    sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    args: [
                      vId,
                      info.season,
                      epNum,
                      fileId,
                      cleanText(ep.episodes_name || s.seasons_name),
                      cleanText(ep.file_type),
                      cleanText(d.video_quality),
                      Array.isArray(ep.subtitle) && ep.subtitle.length ? JSON.stringify(ep.subtitle) : null,
                      Date.now(),
                    ],
                  });
                  itemStreams++;
                }
              }
            }
          }

          // Dọn dẹp stream rác/cũ không còn trong API
          if (isExistingSeries && !typeChanged) {
            for (const r of existingRows) {
              if (!validFids.has(r.file_id) || !validEpKeys.has(`${r.season}:${r.episode}:${r.file_id}`)) {
                streamStatements.push({
                  sql: "DELETE FROM episodes_streams WHERE id = ?",
                  args: [r.id],
                });
                staleStreamsCleaned++;
              }
            }
            if (staleStreamsCleaned > 0) {
              console.log(`  [Phim bộ] [${vId}] ${d.title}: Dọn dẹp ${staleStreamsCleaned} stream rác không hợp lệ.`);
            }
          }
        }

        // Nếu là phim bộ cũ và KHÔNG có tập mới nào và KHÔNG có stream rác bị dọn -> Bỏ qua hoàn toàn, không tốn ghi Turso!
        if (isExistingSeries && itemStreams === 0 && staleStreamsCleaned === 0 && !typeChanged) {
          pageSkipped++;
          totalSkipped++;
          console.log(`  [${listPath}: p${page}] [${i + 1}/${list.length}] [${vId}] ${d.title} -> Đã đủ tập (bỏ qua)`);
          continue;
        }

        // Nếu là phim mới hoặc phim bộ có thêm tập mới: Ghi thông tin title
        collector.add({
          sql: `INSERT OR REPLACE INTO titles (id, type, title, title_clean, imdb_id, tmdb_id, poster_url, thumbnail_url, release_year, imdb_rating, quality, genres, overview, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            vId,
            titleType,
            cleanText(d.title),
            cleanTitle,
            imdbId,
            tmdbId,
            cleanText(d.poster_url),
            cleanText(d.thumbnail_url),
            year,
            rating,
            cleanText(d.video_quality),
            genres,
            cleanText(d.description),
            Date.now(),
          ],
        });

        // Ghi các stream/tập mới
        for (const st of streamStatements) {
          collector.add(st);
        }

        existingTypes.set(vId, titleType);
        pageInserted++;
        totalTitlesInserted++;
        pageStreams += itemStreams;
        totalStreamsInserted += itemStreams;

        console.log(`  [${listPath}: p${page}] [${i + 1}/${list.length}] [${vId}] ${d.title} -> +${itemStreams} streams mới`);
        await collector.checkAutoFlush();
      }

      // Luôn flush sau mỗi trang để đảm bảo dữ liệu ghi liên tục
      await collector.flush();

      const elapsedSec = Math.max(1, Math.round((Date.now() - startTime) / 1000));
      const speed = (totalTitlesInserted / elapsedSec).toFixed(1);
      console.log(
        `[${listPath}] Trang ${page}: +${pageInserted} phim, +${pageStreams} streams (bỏ qua: ${pageSkipped}) | Tổng nạp: ${totalTitlesInserted} phim, ${totalStreamsInserted} streams | Tốc độ: ${speed} phim/s`
      );

      page++;
    }
  }

  // 3. Thực thi theo cấu hình
  if (TYPE === "all" || TYPE === "movies" || TYPE === "movie") {
    await crawlCategory("movies", "movie");
  }
  if (!isOverBudget() && (TYPE === "all" || TYPE === "tvseries" || TYPE === "series")) {
    await crawlCategory("tvseries", "series");
  }

  await collector.flush();
  printSummary();
}

main().catch((err) => {
  console.error("LỖI KHÔNG MONG MUỐN:", err);
  process.exit(1);
});
