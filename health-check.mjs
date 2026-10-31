// health-check.mjs — Tuyến 3: Bảo trì & Sửa link hỏng định kỳ (Chu kỳ 30 ngày)
// Chạy 1 lần/ngày vào lúc 03:00 sáng giờ Việt Nam (20:00 UTC).
// Cơ chế: Adaptive Quota = Math.ceil(Tổng số phim / 30 ngày).
// Tự động kiểm tra đối soát file_id với API Phim4K để phát hiện và sửa các link đã bị thay thế/re-upload.
// Tự động quay vòng (Wrap-around cursor) khi quét hết kho phim để bước sang chu kỳ tháng mới.

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

const DELAY_MS = Math.max(150, parseInt(getArg("delay", "350"), 10) || 350);
const BATCH_SIZE = Math.max(5, parseInt(getArg("batch-size", "30"), 10) || 30);
const OVERRIDE_LIMIT = parseInt(getArg("limit", "0"), 10) || 0;
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
  console.log(">>> TUYẾN 3: BẢO TRÌ & SỬA LINK HỎNG ĐỊNH KỲ (CHU KỲ 30 NGÀY) <<<");
  console.log(`Lịch chạy: 03:00 sáng hàng ngày (Giờ VN) | Chế độ: ${DRY_RUN ? "DRY-RUN (chỉ đọc)" : "GHI DATABASE"}`);
  console.log("=============================================================================\n");

  const client = tursoClient({ url: TURSO_URL, token: TURSO_TOKEN });
  const collector = new BatchCollector(client, BATCH_SIZE);

  // 1. Tính toán Adaptive Quota = Math.ceil(Tổng số phim / 30)
  const countRes = await client.exec("SELECT COUNT(*) AS total FROM titles");
  const totalTitles = countRes.rows && countRes.rows[0] ? Number(countRes.rows[0].total) : 0;
  const adaptiveQuota = Math.max(10, Math.ceil(totalTitles / 30));
  const dailyQuota = OVERRIDE_LIMIT > 0 ? OVERRIDE_LIMIT : adaptiveQuota;

  console.log(`[Thống kê kho] Tổng số phim hiện có trong DB: ${totalTitles}`);
  console.log(`[Adaptive Quota] Mục tiêu hôm nay: quét ${dailyQuota} phim (1/30 tổng kho để hoàn tất trong 30 ngày)`);

  // 2. Đọc con trỏ tiến độ (Cursor) từ bảng meta
  let lastId = 0;
  let cycle = 1;
  try {
    const metaRes = await client.exec(
      "SELECT key, val FROM meta WHERE key IN ('health_check_last_id', 'health_check_cycle')"
    );
    for (const r of metaRes.rows || []) {
      if (r.key === "health_check_last_id") lastId = Number(r.val) || 0;
      if (r.key === "health_check_cycle") cycle = Number(r.val) || 1;
    }
  } catch (e) {
    console.warn("Chưa đọc được meta con trỏ cũ, bắt đầu từ ID 0:", e.message);
  }

  console.log(`[Chu kỳ hiện tại: Tháng thứ ${cycle}] Bắt đầu quét tiếp từ ID: ${lastId}`);

  // 3. Lấy danh sách phim tiếp theo cần kiểm tra
  let batchTitles = [];
  const queryRes = await client.exec(
    "SELECT id, type, title, release_year FROM titles WHERE id > ? ORDER BY id ASC LIMIT ?",
    [lastId, dailyQuota]
  );
  batchTitles = queryRes.rows || [];

  let nextLastId = lastId;
  let wrapAroundHappened = false;

  // Nếu đã quét đến cuối kho phim -> Tự động quay vòng (Wrap-around) về đầu để hoàn tất chu kỳ
  if (batchTitles.length < dailyQuota) {
    console.log(`Đã quét tới cuối kho phim (lấy được ${batchTitles.length} phim). Tiến hành quay vòng (Wrap-around) về ID 0...`);
    const remainQuota = dailyQuota - batchTitles.length;
    const wrapRes = await client.exec(
      "SELECT id, type, title, release_year FROM titles WHERE id > 0 ORDER BY id ASC LIMIT ?",
      [remainQuota]
    );
    const wrapTitles = wrapRes.rows || [];
    batchTitles.push(...wrapTitles);
    cycle++;
    wrapAroundHappened = true;
    console.log(`➔ Đã sang chu kỳ mới: Tháng thứ ${cycle}! Lấy thêm ${wrapTitles.length} phim từ đầu danh sách.`);
  }

  console.log(`Tổng số phim sẽ kiểm tra trong lượt chạy này: ${batchTitles.length} phim.`);

  let totalStreamsChecked = 0;
  let totalStreamsRepaired = 0;
  let totalTitlesRepaired = 0;

  // 4. Bắt đầu đối soát từng phim với API Phim4K
  for (let i = 0; i < batchTitles.length; i++) {
    const t = batchTitles[i];
    nextLastId = t.id;
    await sleep(DELAY_MS);

    const detailType = t.type === "movie" ? "movie" : "tvseries";
    const detailUrl = `https://api.phim4k.space/rest-api/v130/single_details?type=${detailType}&id=${t.id}`;
    const d = await fetchWithRetry(detailUrl);
    if (!d || !d.videos_id) continue;

    // 4.1 Xử lý PHIM LẺ
    if (t.type === "movie" && Array.isArray(d.videos)) {
      // Lấy danh sách stream trong DB
      const dbStreamsRes = await client.exec(
        "SELECT id, file_id, label FROM episodes_streams WHERE title_id = ? AND season = 1 AND episode = 1",
        [t.id]
      );
      const dbStreams = dbStreamsRes.rows || [];
      totalStreamsChecked += dbStreams.length;

      // Danh sách file_id hiện tại từ API
      const apiFileIds = d.videos.map((v) => fileNameFrom(v.file_url)).filter(Boolean);
      let titleFixed = false;

      for (let sIdx = 0; sIdx < dbStreams.length; sIdx++) {
        const dbS = dbStreams[sIdx];
        // Nếu file_id trong DB không còn tồn tại trên API và API có file_id mới tương ứng
        if (!apiFileIds.includes(dbS.file_id) && apiFileIds[sIdx] && apiFileIds[sIdx] !== dbS.file_id) {
          const newFileId = apiFileIds[sIdx];
          await collector.add({
            sql: "UPDATE episodes_streams SET file_id = ?, created_at = ? WHERE id = ?",
            args: [newFileId, Date.now(), dbS.id],
          });
          console.log(`  [CỨU LINK PHIM LẺ] [${t.id}] ${t.title}: Sửa file_id cũ "${dbS.file_id}" -> mới "${newFileId}"`);
          totalStreamsRepaired++;
          titleFixed = true;
        }
      }
      if (titleFixed) totalTitlesRepaired++;
    }

    // 4.2 Xử lý PHIM BỘ
    if (t.type === "series" && Array.isArray(d.season)) {
      // Lấy danh sách stream trong DB theo season:episode
      const dbStreamsRes = await client.exec(
        "SELECT id, season, episode, file_id, label FROM episodes_streams WHERE title_id = ?",
        [t.id]
      );
      const dbStreams = dbStreamsRes.rows || [];
      totalStreamsChecked += dbStreams.length;

      // Index db streams theo `season:episode`
      const dbStreamMap = new Map();
      for (const row of dbStreams) {
        const k = `${row.season}:${row.episode}`;
        if (!dbStreamMap.has(k)) dbStreamMap.set(k, []);
        dbStreamMap.get(k).push(row);
      }

      let titleFixed = false;

      // Duyệt qua API seasons & episodes
      for (const s of d.season) {
        if (!Array.isArray(s.episodes)) continue;
        for (const ep of s.episodes) {
          const apiFileId = fileNameFrom(ep.file_url);
          if (!apiFileId) continue;
          const info = episodeInfo(s, ep);
          if (info.episode === null) continue;

          const k = `${info.season}:${info.episode}`;
          const existingList = dbStreamMap.get(k);

          if (existingList && existingList.length > 0) {
            // Kiểm tra xem file_id của tập này có bị thay đổi không
            const matchExact = existingList.some((x) => x.file_id === apiFileId);
            if (!matchExact && existingList.length === 1) {
              // Tập này chỉ có 1 stream và file_id đã bị đổi trên API -> Cứu link!
              const target = existingList[0];
              await collector.add({
                sql: "UPDATE episodes_streams SET file_id = ?, created_at = ? WHERE id = ?",
                args: [apiFileId, Date.now(), target.id],
              });
              console.log(`  [CỨU LINK TẬP] [${t.id}] ${t.title} S${info.season}E${info.episode}: "${target.file_id}" -> "${apiFileId}"`);
              totalStreamsRepaired++;
              titleFixed = true;
            }
          }
        }
      }
      if (titleFixed) totalTitlesRepaired++;
    }
  }

  // 5. Cập nhật con trỏ tiến độ vào bảng meta
  if (!DRY_RUN && nextLastId > 0) {
    await collector.add({
      sql: "INSERT OR REPLACE INTO meta (key, val) VALUES ('health_check_last_id', ?)",
      args: [String(nextLastId)],
    });
    await collector.add({
      sql: "INSERT OR REPLACE INTO meta (key, val) VALUES ('health_check_cycle', ?)",
      args: [String(cycle)],
    });
    await collector.add({
      sql: "INSERT OR REPLACE INTO meta (key, val) VALUES ('health_check_last_run', ?)",
      args: [new Date().toISOString()],
    });
  }

  await collector.flush();

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log("\n============================ TỔNG KẾT TUYẾN 3 ============================");
  console.log(`Thời gian thực thi: ${elapsed}s`);
  console.log(`Số phim đã kiểm tra: ${batchTitles.length}`);
  console.log(`Tổng số luồng stream đã đối soát: ${totalStreamsChecked}`);
  console.log(`Số link/tập đã được sửa thành công: ${totalStreamsRepaired}`);
  console.log(`Số phim có link được cứu sống: ${totalTitlesRepaired}`);
  console.log(`Con trỏ dừng lại tại ID: ${nextLastId} (Chu kỳ tháng thứ ${cycle})`);
  console.log(`Tổng câu lệnh SQL thực thi: ${collector.totalFlushed}`);
  console.log("============================================================================");
}

main().catch((err) => {
  console.error("Lỗi nghiêm trọng trong Tuyến 3:", err);
  process.exit(1);
});
