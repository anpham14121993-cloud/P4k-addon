// sync-realtime.mjs — Đồng bộ thời gian thực từ Phim4K API vào Turso DB.
// Cơ chế: Quét các trang mới cập nhật (movies/tvseries + trang 1 latest_* để đón ID mới tức thì),
// tích hợp Smart Dedup an toàn (chỉ gộp khi xác nhận ID cũ đã chết trên API) và dọn stream cũ.

import {
  P4K_API_BASE,
  BatchCollector,
  createCliArgs,
  createP4kApi,
  loadTursoClient,
  sleep,
  syncTitleRecord,
} from "./p4k-sync-lib.mjs";

const client = loadTursoClient();
const cli = createCliArgs();
const api = createP4kApi();

const PAGES = Math.max(1, parseInt(cli.get("pages", "5"), 10) || 5);
const DELAY_MS = Math.max(100, parseInt(cli.get("delay", "250"), 10) || 250);
const BATCH_SIZE = Math.max(10, parseInt(cli.get("batch-size", "50"), 10) || 50);
const DRY_RUN = cli.has("dry-run");
const FORCE = cli.has("force");

const startTime = Date.now();

async function main() {
  console.log("=============================================================================");
  console.log(">>> ĐỒNG BỘ THỜI GIAN THỰC PHIM4K -> TURSO DATABASE (SMART DEDUP) <<<");
  console.log(`Chế độ: ${DRY_RUN ? "DRY-RUN (Chỉ đọc)" : "GHI DATABASE"} | Số trang quét: ${PAGES}`);
  console.log("=============================================================================\n");

  const collector = new BatchCollector(client, BATCH_SIZE, DRY_RUN);
  const stats = {
    titlesSynced: 0,
    titlesSkipped: 0,
    duplicatesCleaned: 0,
    streamsInserted: 0,
  };

  for (const cat of [
    { name: "Phim lẻ (movies)", path: "movies?limit=24", latestPath: "latest_movies?limit=24", type: "movie" },
    { name: "Phim bộ (tvseries)", path: "tvseries?limit=24", latestPath: "latest_tvseries?limit=24", type: "series" },
  ]) {
    console.log(`\n--- BẮT ĐẦU ĐỒNG BỘ: ${cat.name.toUpperCase()} ---`);
    const seenIds = new Set();
    const batchLiveIds = new Set();
    const batches = [];

    for (let page = 1; page <= PAGES; page++) {
      const listUrl = `${P4K_API_BASE}${cat.path}&page=${page}`;
      const list = await api.fetchWithRetry(listUrl);
      if (!Array.isArray(list) || list.length === 0) {
        console.log(`Trang ${page} không có phim nào, kết thúc danh mục ${cat.path}.`);
        break;
      }
      console.log(`[${cat.path}] Trang ${page}: Tìm thấy ${list.length} phim từ Phim4K.`);
      for (const x of list) {
        if (x && x.videos_id) batchLiveIds.add(Number(x.videos_id));
      }
      batches.push({ tag: cat.path, page, list, isLatestFallback: false });
    }

    const latestList = await api.fetchWithRetry(`${P4K_API_BASE}${cat.latestPath}&page=1`);
    if (Array.isArray(latestList) && latestList.length > 0) {
      for (const x of latestList) {
        if (x && x.videos_id) batchLiveIds.add(Number(x.videos_id));
      }
      batches.push({ tag: cat.latestPath, page: 1, list: latestList, isLatestFallback: true });
    }

    const isDeadOldIdFn = async (oldId, mediaType) => {
      if (batchLiveIds.has(Number(oldId))) return false;
      return !(await api.isTitleAliveOnApi(oldId, mediaType));
    };

    for (const batch of batches) {
      for (let i = 0; i < batch.list.length; i++) {
        const item = batch.list[i];
        const vId = Number(item.videos_id);
        if (!vId || seenIds.has(vId)) continue;
        seenIds.add(vId);

        const itemUpdatedAt = batch.isLatestFallback
          ? startTime + 1000 - i * 10
          : startTime - ((batch.page - 1) * 50 + i) * 1000;

        await sleep(DELAY_MS);
        const detailType = cat.type === "movie" ? "movie" : "tvseries";
        const detailUrl = `${P4K_API_BASE}single_details?type=${detailType}&id=${vId}`;
        const d = await api.fetchWithRetry(detailUrl);
        if (!d || !d.videos_id) {
          console.warn(`  [${vId}] Không lấy được chi tiết, bỏ qua.`);
          continue;
        }

        const res = await syncTitleRecord({
          client,
          collector,
          item,
          detail: d,
          type: cat.type,
          itemUpdatedAt,
          force: FORCE,
          isDeadOldIdFn,
          stats,
        });
        if (res.status === "synced") {
          console.log(`  [${cat.type}] [${vId}] ${d.title}: đã đồng bộ.`);
        }
      }
    }
  }

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
  console.log(`Phim đã đồng bộ mới/cập nhật: ${stats.titlesSynced}`);
  console.log(`Phim đã bỏ qua (không đổi): ${stats.titlesSkipped}`);
  console.log(`Bản ghi cũ trùng lặp đã dọn (Smart Merge): ${stats.duplicatesCleaned}`);
  console.log(`Tổng luồng stream/tập đã nạp: ${stats.streamsInserted}`);
  console.log(`Tổng câu lệnh SQL thực thi: ${collector.totalFlushed}`);
  console.log("============================================================================");
}

main().catch((e) => {
  console.error("Lỗi đồng bộ thời gian thực:", e);
  process.exit(1);
});
