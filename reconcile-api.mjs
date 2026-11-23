// reconcile-api.mjs — Đối soát 2 chiều 100% giữa API Phim4K (movies + tvseries) và Oracle/Turso DB.
// - Nạp toàn bộ phim đang có trên API nhưng thiếu/đổi loại trong DB.
// - Bảo toàn imdb_id và thông tin match HF (theo file_id) từ các bản ghi cũ bị Phim4K đổi ID.
// - Xóa toàn bộ ID không còn tồn tại trong danh mục sống của API để DB khớp 100% với Phim4K.

import {
  P4K_API_BASE,
  BatchCollector,
  createCliArgs,
  createP4kApi,
  loadTursoClient,
  normalizeText,
  sleep,
  syncTitleRecord,
} from "./p4k-sync-lib.mjs";

const client = loadTursoClient();
const cli = createCliArgs();
const api = createP4kApi();

const DELAY_MS = Math.max(50, parseInt(cli.get("delay", "180"), 10) || 180);
const BATCH_SIZE = Math.max(10, parseInt(cli.get("batch-size", "50"), 10) || 50);
const DRY_RUN = cli.has("dry-run");
const ALLOW_LARGE_PRUNE = cli.has("allow-large-prune");
const REFRESH_ALL = cli.has("refresh-all");
const REFRESH_DEAD = cli.has("refresh-dead");

const MIN_EXPECTED_MOVIES = 1000;
const MIN_EXPECTED_SERIES = 1000;
const MAX_PRUNE_RATIO = 0.45;

const startTime = Date.now();

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function fetchLiveCatalogSnapshot() {
  const [lm, m, ls, s] = await Promise.all([
    api.fetchFullCatalogList("latest_movies", { limit: 100, delayMs: 60 }),
    api.fetchFullCatalogList("movies", { limit: 100, delayMs: 60 }),
    api.fetchFullCatalogList("latest_tvseries", { limit: 100, delayMs: 60 }),
    api.fetchFullCatalogList("tvseries", { limit: 100, delayMs: 60 }),
  ]);

  for (const [name, res] of [
    ["latest_movies", lm],
    ["movies", m],
    ["latest_tvseries", ls],
    ["tvseries", s],
  ]) {
    if (!res.complete) {
      throw new Error(`Không tải trọn vẹn danh mục ${name} (dừng ở trang ${res.errorPage}). Hủy đối soát để tránh xóa nhầm DB.`);
    }
    console.log(`[API] ${name}: ${res.items.length} mục (${res.pagesFetched} trang, trọn vẹn).`);
  }

  // Giữ thứ tự từ latest_* làm thứ tự thời gian (pos nhỏ = mới hơn).
  const liveMap = new Map();
  let moviePos = 0;
  for (const item of [...lm.items, ...m.items]) {
    const id = Number(item && item.videos_id);
    if (!id) continue;
    if (!liveMap.has(id)) {
      liveMap.set(id, { id, type: "movie", item, pos: moviePos++ });
    }
  }

  let seriesPos = 0;
  for (const item of [...ls.items, ...s.items]) {
    const id = Number(item && item.videos_id);
    if (!id) continue;
    const prev = liveMap.get(id);
    if (!prev) {
      liveMap.set(id, { id, type: "series", item, pos: seriesPos++ });
    } else if (prev.type !== "series") {
      // Nếu vừa có mặt bên series vừa bên movie, ưu tiên series theo rule detail của Phim4K.
      liveMap.set(id, { id, type: "series", item, pos: seriesPos++ });
    }
  }

  let movieCount = 0;
  let seriesCount = 0;
  for (const v of liveMap.values()) {
    if (v.type === "movie") movieCount++;
    else seriesCount++;
  }

  if (movieCount < MIN_EXPECTED_MOVIES || seriesCount < MIN_EXPECTED_SERIES) {
    throw new Error(`Số lượng phim sống bất thường (movie=${movieCount}, series=${seriesCount}). Hủy đối soát để bảo vệ DB.`);
  }

  return { liveMap, movieCount, seriesCount };
}

async function collectPreservedData(clientRef, deleteIds, dbMap) {
  const preservedHfByFileId = new Map();
  const preservedImdbByNorm = new Map();
  const ambiguousNormKeys = new Set();

  for (const id of deleteIds) {
    const r = dbMap.get(id);
    if (!r || !r.imdb_id) continue;
    const k = `${r.type}|${r.title_clean || normalizeText(r.title)}|${r.release_year || ""}`;
    if (ambiguousNormKeys.has(k)) continue;
    if (preservedImdbByNorm.has(k) && preservedImdbByNorm.get(k) !== r.imdb_id) {
      preservedImdbByNorm.delete(k);
      ambiguousNormKeys.add(k);
    } else {
      preservedImdbByNorm.set(k, r.imdb_id);
    }
  }

  for (const chunk of chunkArray(deleteIds, 180)) {
    const placeholders = chunk.map(() => "?").join(",");
    const res = await clientRef.exec(
      `SELECT file_id, hf_id, hf_checked_at, hf_status, hf_fails, rail_size
       FROM episodes_streams
       WHERE title_id IN (${placeholders})
         AND (hf_id IS NOT NULL OR hf_checked_at IS NOT NULL OR hf_status IS NOT NULL)`,
      chunk
    );
    for (const row of res.rows || []) {
      if (!row.file_id) continue;
      const cur = preservedHfByFileId.get(row.file_id);
      if (!cur || (!cur.hf_id && row.hf_id)) {
        preservedHfByFileId.set(row.file_id, row);
      }
    }
  }

  return { preservedHfByFileId, preservedImdbByNorm };
}

async function main() {
  console.log("=============================================================================");
  console.log(">>> ĐỐI SOÁT 100% DANH MỤC API PHIM4K <-> ORACLE/TURSO DB (LỰA CHỌN A) <<<");
  console.log(`Chế độ: ${DRY_RUN ? "DRY-RUN (Chỉ đọc)" : "GHI DATABASE"} | Delay: ${DELAY_MS}ms`);
  console.log("=============================================================================\n");

  const { liveMap, movieCount, seriesCount } = await fetchLiveCatalogSnapshot();
  console.log(`\n[SNAPSHOT API] Tổng hợp duy nhất: movie=${movieCount}, series=${seriesCount}, total=${liveMap.size}`);

  const dbRes = await client.exec("SELECT id, type, title, title_clean, imdb_id, release_year, updated_at FROM titles");
  const dbMap = new Map();
  for (const r of dbRes.rows || []) {
    dbMap.set(Number(r.id), r);
  }
  console.log(`[SNAPSHOT DB] Hiện có: ${dbMap.size} bản ghi trong titles.`);

  const toDeleteIds = [];
  for (const id of dbMap.keys()) {
    if (!liveMap.has(id)) toDeleteIds.push(id);
  }

  const deadTitleIds = new Set();
  if (REFRESH_DEAD && !REFRESH_ALL) {
    const deadRes = await client.exec("SELECT DISTINCT title_id FROM episodes_streams WHERE hf_status = 'dead'");
    for (const r of deadRes.rows || []) deadTitleIds.add(Number(r.title_id));
    console.log(`[REFRESH-DEAD] Sẽ kiểm tra lại ${deadTitleIds.size} phim đang có luồng dead.`);
  }

  const toSync = [];
  for (const entry of liveMap.values()) {
    const dbRow = dbMap.get(entry.id);
    if (REFRESH_ALL || !dbRow || dbRow.type !== entry.type || (REFRESH_DEAD && deadTitleIds.has(entry.id))) {
      toSync.push(entry);
    }
  }

  const pruneRatio = dbMap.size > 0 ? toDeleteIds.length / dbMap.size : 0;
  console.log(
    `[CHÊNH LỆCH] Cần nạp/cập nhật: ${toSync.length} | Cần xóa (không còn trên API): ${toDeleteIds.length} (${(pruneRatio * 100).toFixed(1)}% DB)`
  );

  if (!ALLOW_LARGE_PRUNE && pruneRatio > MAX_PRUNE_RATIO) {
    throw new Error(
      `Tỷ lệ xóa (${(pruneRatio * 100).toFixed(1)}%) vượt ngưỡng an toàn ${(MAX_PRUNE_RATIO * 100).toFixed(0)}%. Truyền --allow-large-prune nếu chắc chắn.`
    );
  }

  const { preservedHfByFileId, preservedImdbByNorm } = await collectPreservedData(client, toDeleteIds, dbMap);
  console.log(`[BẢO TOÀN] Đã lưu tạm ${preservedHfByFileId.size} file_id HF và ${preservedImdbByNorm.size} imdb_id trước khi dọn.\n`);

  const collector = new BatchCollector(client, BATCH_SIZE, DRY_RUN);
  const stats = {
    titlesSynced: 0,
    titlesSkipped: 0,
    duplicatesCleaned: 0,
    streamsInserted: 0,
    streamsDeleted: 0,
    detailNotFound: 0,
    deletedIds: new Set(),
  };

  const isDeadOldIdFn = async (oldId) => !liveMap.has(Number(oldId));

  for (let i = 0; i < toSync.length; i++) {
    const entry = toSync[i];
    const detailType = entry.type === "series" ? "tvseries" : "movie";
    const d = await api.fetchWithRetry(`${P4K_API_BASE}single_details?type=${detailType}&id=${entry.id}`, 3);
    if (!d || !d.videos_id) {
      stats.detailNotFound++;
      console.warn(`  [${i + 1}/${toSync.length}] [${entry.id}] single_details rỗng/không tồn tại (${d && d.message ? d.message : "null"}).`);
    } else {
      const k = `${entry.type}|${normalizeText(d.title)}|${parseInt(String(d.release || "").slice(0, 4), 10) || ""}`;
      const fallbackImdbId = preservedImdbByNorm.get(k) || null;
      const itemUpdatedAt = startTime - entry.pos * 10000;
      const res = await syncTitleRecord({
        client,
        collector,
        item: entry.item,
        detail: d,
        type: entry.type,
        itemUpdatedAt,
        force: Boolean(dbMap.has(entry.id) && dbMap.get(entry.id).type !== entry.type),
        isDeadOldIdFn,
        preservedHfMap: preservedHfByFileId,
        fallbackImdbId,
        stats,
      });
      if ((i + 1) % 50 === 0 || i === toSync.length - 1 || res.status === "synced") {
        if ((i + 1) % 25 === 0 || i === toSync.length - 1) {
          console.log(`  [${i + 1}/${toSync.length}] Đã xử lý [${entry.type}] [${entry.id}] ${d.title}`);
        }
      }
    }
    if (i < toSync.length - 1 && DELAY_MS > 0) await sleep(DELAY_MS);
  }

  await collector.flush();

  const remainingToDelete = toDeleteIds.filter((id) => !stats.deletedIds.has(id));
  if (!DRY_RUN && remainingToDelete.length > 0) {
    console.log(`\n[DỌN DB] Đang xóa ${remainingToDelete.length} ID không còn trên API Phim4K...`);
    for (const chunk of chunkArray(remainingToDelete, 150)) {
      const placeholders = chunk.map(() => "?").join(",");
      await collector.add({
        sql: `DELETE FROM episodes_streams WHERE title_id IN (${placeholders})`,
        args: chunk,
      });
      await collector.add({
        sql: `DELETE FROM titles WHERE id IN (${placeholders})`,
        args: chunk,
      });
    }
  }

  if (!DRY_RUN) {
    await collector.add({
      sql: "DELETE FROM episodes_streams WHERE NOT EXISTS (SELECT 1 FROM titles t WHERE t.id = episodes_streams.title_id)",
      args: [],
    });
    await collector.add({
      sql: "INSERT OR REPLACE INTO meta (key, val) VALUES ('reconcile_api_at', ?)",
      args: [String(Date.now())],
    });
  }

  await collector.flush();

  let postDbCounts = null;
  if (!DRY_RUN) {
    const postRes = await client.exec("SELECT type, COUNT(*) AS cnt FROM titles GROUP BY type");
    postDbCounts = { movie: 0, series: 0 };
    for (const r of postRes.rows || []) postDbCounts[r.type] = Number(r.cnt);
  }

  const elapsed = Math.round((Date.now() - startTime) / 1000);
  console.log("\n===================== TỔNG KẾT ĐỐI SOÁT (RECONCILE API) =====================");
  console.log(`Thời gian thực thi: ${elapsed}s`);
  console.log(`API Phim4K: movie=${movieCount}, series=${seriesCount}, total=${liveMap.size}`);
  console.log(`Phim đã nạp/cập nhật: ${stats.titlesSynced} (detail rỗng: ${stats.detailNotFound})`);
  console.log(`Phim đã bỏ qua (không đổi): ${stats.titlesSkipped}`);
  console.log(`Smart Merge khi đối soát: ${stats.duplicatesCleaned}`);
  console.log(`Stream mới nạp: ${stats.streamsInserted} | Stream cũ đã dọn: ${stats.streamsDeleted}`);
  console.log(`ID đã xóa khỏi DB: ${DRY_RUN ? `${toDeleteIds.length} (dry-run)` : toDeleteIds.length}`);
  if (postDbCounts) {
    console.log(
      `DB sau đối soát: movie=${postDbCounts.movie}, series=${postDbCounts.series}, total=${postDbCounts.movie + postDbCounts.series}`
    );
  }
  console.log(`Tổng câu lệnh SQL thực thi: ${collector.totalFlushed}`);
  console.log("==============================================================================");
}

main().catch((e) => {
  console.error("Lỗi đối soát API:", e);
  process.exit(1);
});
