// upgrade-quality.mjs — Tuyến 2: Đồng bộ nâng cấp chất lượng (4K, Remux, Blu-ray...)
// Chạy 2 lần/ngày: 12:00 trưa và 00:00 nửa đêm giờ Việt Nam (05:00 và 17:00 UTC).
// Chỉ thực hiện 1 nhiệm vụ duy nhất: Kiểm tra và nạp bản chất lượng mới (file_id mới)
// cho các phim/season phát hành gần đây (2025–2026 hoặc có cập nhật trong 1 năm qua).
// Tuyệt đối không kiểm tra link chết, không ghi đè file cũ (để Tuyến 3 xử lý).

import fs from "node:fs";
import { tursoClient } from "./turso-http.mjs";

// Đọc env cục bộ nếu chạy dưới Termux/Local
if (!process.env.TURSO_DATABASE_URL && fs.existsSync(".env.turso")) {
  const lines = fs.readFileSync(".env.turso", "utf-8").split("\n");
  for (const line of lines) {
    const eq = line.indexOf("=");
    if (eq > 0) {
      const k = line.slice(0, eq).trim();
      const v = line.slice(eq + 1).trim();
      if (!process.env[k]) process.env[k] = v;
    }
  }
}

const TURSO_URL = process.env.TURSO_DATABASE_URL;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;

if (!TURSO_URL || !TURSO_TOKEN) {
  console.error("Lỗi: Thiếu TURSO_DATABASE_URL hoặc TURSO_AUTH_TOKEN");
  process.exit(1);
}

const args = process.argv.slice(2);
function getArg(name, def = null) {
  const i = args.indexOf("--" + name);
  return i >= 0 && args[i + 1] ? args[i + 1] : def;
}
function hasFlag(name) {
  return args.includes("--" + name);
}

const TYPE = (getArg("type", "all") || "all").toLowerCase(); // all | movie | series
const DELAY_MS = Math.max(150, parseInt(getArg("delay", "350"), 10) || 350);
const BATCH_SIZE = Math.max(5, parseInt(getArg("batch-size", "30"), 10) || 30);
const LIMIT = parseInt(getArg("limit", "0"), 10) || 0; // 0 = toàn bộ
const DRY_RUN = hasFlag("dry-run");

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const startTime = Date.now();

function cleanText(v) { return v ? String(v).trim() : null; }
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

let globalCoolUntil = 0;

async function fetchWithRetry(url, maxRetries = 4) {
  for (let i = 0; i < maxRetries; i++) {
    const now = Date.now();
    if (globalCoolUntil > now) {
      await sleep(globalCoolUntil - now);
    }
    try {
      const r = await fetch(url, { headers, signal: AbortSignal.timeout(10000) });
      if (r.status === 429) {
        const ra = parseInt(r.headers.get("retry-after") || "0", 10);
        const coolMs = ra > 0 ? ra * 1000 : 15000;
        console.warn(`[429 Rate Limit] Phim4K giới hạn nhịp. Tạm nghỉ ${Math.round(coolMs / 1000)}s toàn cục...`);
        globalCoolUntil = Date.now() + coolMs;
        await sleep(coolMs);
        continue;
      }
      if (!r.ok) {
        if (r.status >= 500) { await sleep(1500 * (i + 1)); continue; }
        return null;
      }
      const txt = await r.text();
      if (txt.includes("rate limited") || txt.includes("Cloudflare")) {
        globalCoolUntil = Date.now() + 15000;
        await sleep(15000);
        continue;
      }
      return JSON.parse(txt);
    } catch (e) {
      await sleep(1500 * (i + 1));
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

  async add(stmt) {
    if (DRY_RUN) return;
    this.queue.push(stmt);
    if (this.queue.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush() {
    if (this.queue.length === 0 || DRY_RUN) return;
    const batch = this.queue.splice(0, this.queue.length);
    for (let attempt = 1; attempt <= 4; attempt++) {
      try {
        await this.client.batch(batch);
        this.totalFlushed += batch.length;
        return;
      } catch (e) {
        console.error(`[Turso Flush Lỗi lần ${attempt}] ${e.message}`);
        if (attempt === 4) throw e;
        await sleep(1000 * attempt);
      }
    }
  }
}

/* ═══════════════════════ CHƯƠNG TRÌNH CHÍNH ═══════════════════════ */

async function main() {
  console.log("=============================================================================");
  console.log(">>> TUYẾN 2: ĐỒNG BỘ NÂNG CẤP CHẤT LƯỢNG (4K, REMUX, BLU-RAY) <<<");
  console.log(`Lịch chạy: 12:00 & 00:00 hàng ngày (Giờ VN) | Chế độ: ${DRY_RUN ? "DRY-RUN (chỉ đọc)" : "GHI DATABASE"}`);
  console.log(`Tùy chọn: Type=${TYPE}, Delay=${DELAY_MS}ms, Batch=${BATCH_SIZE}, Limit=${LIMIT || "Không giới hạn"}`);
  console.log("=============================================================================\n");

  const client = tursoClient({ url: TURSO_URL, token: TURSO_TOKEN });
  const collector = new BatchCollector(client, BATCH_SIZE);

  const currentYear = new Date().getFullYear();
  const cutoffYear = currentYear - 1; // 2025 nếu hiện tại là 2026
  const oneYearAgoMs = Date.now() - 365 * 24 * 60 * 60 * 1000;

  let totalChecked = 0;
  let totalUpgradesFound = 0;
  let totalNewStreamsInserted = 0;

  // 1. Đồng bộ nâng cấp cho PHIM LẺ (Movies)
  if (TYPE === "all" || TYPE === "movie" || TYPE === "movies") {
    console.log(`\n--- 1. BẮT ĐẦU KIỂM TRA PHIM LẺ (Năm >= ${cutoffYear}, chưa có 4K/Remux) ---`);
    let movieSql = `SELECT id, title, release_year, quality FROM titles 
                    WHERE type = 'movie' 
                      AND release_year >= ? 
                      AND (quality IS NULL OR (quality NOT LIKE '%4K%' AND quality NOT LIKE '%Remux%')) 
                    ORDER BY id DESC`;
    if (LIMIT > 0) movieSql += ` LIMIT ${LIMIT}`;

    const movieRes = await client.exec(movieSql, [cutoffYear]);
    const movies = movieRes.rows || [];
    console.log(`Tìm thấy ${movies.length} phim lẻ cần đối soát nâng cấp chất lượng.`);

    for (let i = 0; i < movies.length; i++) {
      const m = movies[i];
      totalChecked++;

      // Lấy danh sách file_id đã có trong DB của phim này
      let existingFileIds = new Set();
      try {
        const streamRes = await client.exec(
          "SELECT file_id FROM episodes_streams WHERE title_id = ?",
          [m.id]
        );
        existingFileIds = new Set((streamRes.rows || []).map((r) => r.file_id).filter(Boolean));
      } catch (e) {
        console.error(`Lỗi đọc stream cũ của phim ${m.id}:`, e.message);
      }

      await sleep(DELAY_MS);
      const detailUrl = `https://api.phim4k.space/rest-api/v130/single_details?type=movie&id=${m.id}`;
      const d = await fetchWithRetry(detailUrl);
      if (!d || !Array.isArray(d.videos)) continue;

      let newStreamsForMovie = 0;
      for (const v of d.videos) {
        const fileId = fileNameFrom(v.file_url);
        if (!fileId || existingFileIds.has(fileId)) continue;

        // Phát hiện bản chất lượng mới (file_id chưa có trong DB)!
        await collector.add({
          sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          args: [
            m.id,
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
        existingFileIds.add(fileId);
        newStreamsForMovie++;
        totalNewStreamsInserted++;
      }

      if (newStreamsForMovie > 0) {
        totalUpgradesFound++;
        console.log(`  [Phim lẻ] [${i + 1}/${movies.length}] [${m.id}] ${m.title} (${m.release_year}) -> NÂNG CẤP: +${newStreamsForMovie} stream mới!`);
        // Cập nhật nhãn chất lượng mới của phim nếu có thay đổi
        if (d.video_quality && d.video_quality !== m.quality) {
          await collector.add({
            sql: `UPDATE titles SET quality = ?, updated_at = ? WHERE id = ?`,
            args: [cleanText(d.video_quality), Date.now(), m.id],
          });
        }
      }
    }
  }

  // 2. Đồng bộ nâng cấp cho PHIM BỘ (Series)
  if (TYPE === "all" || TYPE === "series" || TYPE === "tvseries") {
    console.log(`\n--- 2. BẮT ĐẦU KIỂM TRA PHIM BỘ (Năm >= ${cutoffYear}, chưa có 4K/Remux) ---`);
    let seriesSql = `SELECT id, title, release_year, quality, updated_at FROM titles 
                     WHERE type = 'series' 
                       AND release_year >= ? 
                       AND (quality IS NULL OR (quality NOT LIKE '%4K%' AND quality NOT LIKE '%Remux%')) 
                     ORDER BY id DESC`;
    if (LIMIT > 0) seriesSql += ` LIMIT ${LIMIT}`;

    const seriesRes = await client.exec(seriesSql, [cutoffYear]);
    const seriesList = seriesRes.rows || [];
    console.log(`Tìm thấy ${seriesList.length} phim bộ cần đối soát nâng cấp chất lượng.`);

    for (let i = 0; i < seriesList.length; i++) {
      const sItem = seriesList[i];
      totalChecked++;

      // Lấy danh sách stream đã có trong DB: season:episode:file_id
      let existingStreamKeys = new Set();
      try {
        const streamRes = await client.exec(
          "SELECT season, episode, file_id FROM episodes_streams WHERE title_id = ?",
          [sItem.id]
        );
        existingStreamKeys = new Set((streamRes.rows || []).map((r) => `${r.season}:${r.episode}:${r.file_id}`));
      } catch (e) {
        console.error(`Lỗi đọc stream cũ của phim bộ ${sItem.id}:`, e.message);
      }

      await sleep(DELAY_MS);
      const detailUrl = `https://api.phim4k.space/rest-api/v130/single_details?type=tvseries&id=${sItem.id}`;
      const d = await fetchWithRetry(detailUrl);
      if (!d || !Array.isArray(d.season)) continue;

      let newStreamsForSeries = 0;
      for (const s of d.season) {
        if (!Array.isArray(s.episodes)) continue;
        for (const ep of s.episodes) {
          const fileId = fileNameFrom(ep.file_url);
          if (!fileId) continue;
          const info = episodeInfo(s, ep);
          if (info.episode === null) continue;

          const from = info.episode;
          const to = info.episodeEnd ? info.episodeEnd : from;
          const span = Math.min(to, from + 60);

          for (let epNum = from; epNum <= span; epNum++) {
            const key = `${info.season}:${epNum}:${fileId}`;
            if (existingStreamKeys.has(key)) continue;

            // Phát hiện bản chất lượng mới của tập phim!
            await collector.add({
              sql: `INSERT OR REPLACE INTO episodes_streams (title_id, season, episode, file_id, label, container, quality, subtitles, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
              args: [
                sItem.id,
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
            existingStreamKeys.add(key);
            newStreamsForSeries++;
            totalNewStreamsInserted++;
          }
        }
      }

      if (newStreamsForSeries > 0) {
        totalUpgradesFound++;
        console.log(`  [Phim bộ] [${i + 1}/${seriesList.length}] [${sItem.id}] ${sItem.title} (${sItem.release_year}) -> NÂNG CẤP: +${newStreamsForSeries} stream chất lượng mới!`);
        if (d.video_quality && d.video_quality !== sItem.quality) {
          await collector.add({
            sql: `UPDATE titles SET quality = ?, updated_at = ? WHERE id = ?`,
            args: [cleanText(d.video_quality), Date.now(), sItem.id],
          });
        }
      }
    }
  }

  await collector.flush();

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log("\n============================ TỔNG KẾT TUYẾN 2 ============================");
  console.log(`Thời gian thực thi: ${elapsed}s`);
  console.log(`Tổng phim/bộ đã đối soát: ${totalChecked}`);
  console.log(`Số phim/bộ có bản nâng cấp chất lượng: ${totalUpgradesFound}`);
  console.log(`Tổng số stream/bản chất lượng mới nạp vào DB: ${totalNewStreamsInserted}`);
  console.log(`Tổng số câu lệnh SQL thực thi: ${collector.totalFlushed}`);
  console.log("============================================================================");
}

main().catch((err) => {
  console.error("Lỗi nghiêm trọng trong Tuyến 2:", err);
  process.exit(1);
});
