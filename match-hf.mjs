// match-hf.mjs — Đối chiếu file_id bị đổi tên trên episodes_streams sang file thật trên HF (bảng files)
// bằng dung lượng (size = len + 64 hoặc raw len) và xác minh 32 byte giữa luồng video (XOR DEFAULT_KEY).
// Chạy: node match-hf.mjs [--limit=200] [--budget=1200] [--concurrency=3] [--cooldown-hours=24] [--ids=ID1,ID2] [--dry-run]

import fs from "node:fs";
import { tursoClient } from "./turso-http.mjs";
import { setEnv, configuredResolvers, resolveDirectUrls } from "./core.mjs";

if (!process.env.GITHUB_ACTIONS && fs.existsSync("/sdcard/Download/P4k/.env.turso")) {
  for (const line of fs.readFileSync("/sdcard/Download/P4k/.env.turso", "utf8").split(/\r?\n/)) {
    const eq = line.indexOf("=");
    if (eq <= 0) continue;
    const k = line.slice(0, eq).trim();
    const v = line.slice(eq + 1).trim().replace(/^['"]|['"]$/g, "");
    if (k && v && !k.startsWith("#")) process.env[k] = v;
  }
}
setEnv(process.env);

const DB_URL = process.env.TURSO_DATABASE_URL || "";
const DB_TOKEN = process.env.TURSO_AUTH_TOKEN || "";
if (!DB_URL || !DB_TOKEN) {
  console.error("Thiếu TURSO_DATABASE_URL hoặc TURSO_AUTH_TOKEN");
  process.exit(1);
}

const args = process.argv.slice(2);
function getArg(name, def = null) {
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--" + name && i + 1 < args.length) return args[i + 1];
    if (a.startsWith("--" + name + "=")) return a.slice(name.length + 3);
  }
  return def;
}
const hasFlag = (name) => args.some((a) => a === "--" + name || a.startsWith("--" + name + "="));

const rawLimit = parseInt(getArg("limit", "200"), 10);
const LIMIT = Number.isFinite(rawLimit) ? Math.max(0, rawLimit) : 200;
const BUDGET_MS = Math.max(30, parseInt(getArg("budget", "1200"), 10) || 1200) * 1000;
const CONCURRENCY = Math.max(1, Math.min(6, parseInt(getArg("concurrency", "3"), 10) || 3));
const COOLDOWN_MS = Math.max(1, parseInt(getArg("cooldown-hours", "168"), 10) || 168) * 3600 * 1000;
const DRY_RUN = hasFlag("dry-run");
const NEW_ONLY = hasFlag("new-only");
const HOT_ONLY = hasFlag("hot-only");
const TARGET_TYPE = (() => {
  const t = String(getArg("type", "all") || "all").toLowerCase();
  return t === "movie" || t === "series" ? t : "all";
})();
const SHARD = (() => {
  const raw = String(getArg("shard", "0/1") || "0/1");
  const m = raw.match(/^(\d+)\/(\d+)$/);
  if (!m) return { index: 0, total: 1 };
  const total = Math.max(1, parseInt(m[2], 10));
  const index = Math.max(0, Math.min(total - 1, parseInt(m[1], 10)));
  return { index, total };
})();
const TARGET_IDS = [...new Set(String(getArg("ids", "") || "").split(",").map((s) => s.trim()).filter(Boolean))];
const DIAG_URL = (process.env.P4K_DIAG_URL || "https://phim4k-addon.notthingp.workers.dev/_diag").replace(/\/$/, "");
const ACTIVE_RESOLVERS = configuredResolvers();
const USE_DIRECT_MINT = ACTIVE_RESOLVERS.length > 0;
const ALLOW_DIAG_FALLBACK = !hasFlag("no-diag-fallback");
let directBlockedUntil = 0;
let loggedDirectFallback = false;
const HF_TOKENS = String(process.env.HF_TOKEN || "").split(",").map((s) => s.trim()).filter(Boolean);
const HEADER_SIZE = 64;

// Khóa XOR 256-byte chuẩn dùng để giải mã payload HF từ offset 0x40 (khớp worker.mjs)
const DEFAULT_KEY = Uint8Array.from([
  0x48, 0x21, 0x0c, 0xf2, 0xc1, 0x27, 0x21, 0xf7, 0xf1, 0x1e, 0x12, 0x9b, 0x98, 0x4d, 0xb6, 0xd7,
  0xa9, 0x31, 0xe3, 0xed, 0x2f, 0xd0, 0x3c, 0x46, 0x67, 0x43, 0xc9, 0xbb, 0xc9, 0x78, 0xf6, 0x1b,
  0xfb, 0x28, 0x0b, 0x59, 0x8c, 0x8c, 0x92, 0x55, 0xf4, 0x9d, 0x68, 0xb5, 0xf7, 0x41, 0x3d, 0x33,
  0xfe, 0xc8, 0x2c, 0xd3, 0x08, 0xb4, 0x4e, 0xe5, 0xef, 0xc4, 0xf1, 0x1a, 0x70, 0x29, 0x64, 0x1f,
  0x86, 0x23, 0x0a, 0x9c, 0xf6, 0x2b, 0x92, 0x89, 0x2d, 0x2b, 0x6e, 0xe1, 0x5a, 0x0b, 0x05, 0x87,
  0x95, 0xbd, 0x2b, 0xbc, 0x2f, 0xa9, 0x7e, 0x45, 0xb4, 0x34, 0x81, 0xd9, 0x1a, 0xa4, 0x76, 0x86,
  0xf6, 0x4b, 0x72, 0x0b, 0x30, 0x29, 0x6a, 0x9a, 0x7b, 0xd8, 0x9c, 0xe5, 0x05, 0x41, 0x1c, 0xb4,
  0x3e, 0xfa, 0x33, 0x1f, 0xfa, 0x40, 0xb2, 0xcf, 0xd3, 0xfe, 0xb5, 0x96, 0xba, 0x2a, 0xa7, 0xc2,
  0x3a, 0xce, 0xe1, 0x79, 0xfb, 0xbd, 0x04, 0xd0, 0x82, 0x47, 0x1a, 0xb5, 0xa3, 0xef, 0x73, 0x77,
  0x00, 0xfe, 0xe8, 0xac, 0xf8, 0xc6, 0x98, 0xd5, 0x2b, 0x9d, 0x49, 0xe1, 0xdc, 0x37, 0x1e, 0x0c,
  0x12, 0x71, 0x05, 0xfe, 0x29, 0x95, 0x55, 0x47, 0xa6, 0x2b, 0xd7, 0xdb, 0x7f, 0xf1, 0x2d, 0x96,
  0x1a, 0x21, 0x5f, 0x73, 0x61, 0x0f, 0xfe, 0xfa, 0x0a, 0xb9, 0xdf, 0x51, 0xfa, 0x16, 0xe6, 0x33,
  0x5d, 0x72, 0xf2, 0x5a, 0xc1, 0x01, 0x52, 0x07, 0x87, 0x5d, 0xc3, 0x44, 0xcd, 0xd7, 0x5d, 0x15,
  0xfa, 0xb9, 0xe4, 0xf9, 0x29, 0xf5, 0x15, 0x5b, 0x2c, 0x71, 0xa9, 0x89, 0x0e, 0xe2, 0xdf, 0xce,
  0xb1, 0xa3, 0x3f, 0xa2, 0xf4, 0x7e, 0x3a, 0x29, 0xe0, 0x10, 0x00, 0xcf, 0x55, 0xa6, 0x53, 0xfe,
  0xad, 0x86, 0xe1, 0x40, 0x46, 0xdd, 0x25, 0xe9, 0x47, 0xe9, 0xca, 0x3c, 0x00, 0xe3, 0x16, 0xdb,
]);

const db = tursoClient({ url: DB_URL, token: DB_TOKEN, timeout: 60000, maxConcurrent: 4 });
const UA = { "User-Agent": "Mozilla/5.0 (compatible; p4k-match/1.0)", Accept: "application/json" };
let hfRr = 0;

function nextHfToken() {
  if (!HF_TOKENS.length) return null;
  return HF_TOKENS[hfRr++ % HF_TOKENS.length];
}

function hfRawUrl(row) {
  const repo = String(row.repo_id || "").replace(/^BUCKET:/, "");
  if (row.kind === "bucket") return `https://huggingface.co/buckets/${repo}/resolve/${row.path}`;
  if (row.kind === "dataset") return `https://huggingface.co/datasets/${repo}/resolve/main/${row.path}`;
  return `https://huggingface.co/${repo}/resolve/main/${row.path}`;
}

function baseIdOf(path) {
  return String(path || "").split("/").pop().replace(/\.[^.]+$/, "");
}

async function ensureSchema() {
  const info = await db.exec("PRAGMA table_info(episodes_streams)");
  const cols = new Set((info.rows || []).map((r) => r.name));
  if (!cols.has("hf_id")) await db.exec("ALTER TABLE episodes_streams ADD COLUMN hf_id TEXT");
  if (!cols.has("hf_checked_at")) await db.exec("ALTER TABLE episodes_streams ADD COLUMN hf_checked_at INTEGER");
  if (!cols.has("hf_status")) await db.exec("ALTER TABLE episodes_streams ADD COLUMN hf_status TEXT");
  if (!cols.has("hf_fails")) await db.exec("ALTER TABLE episodes_streams ADD COLUMN hf_fails INTEGER");
  if (!cols.has("rail_size")) await db.exec("ALTER TABLE episodes_streams ADD COLUMN rail_size INTEGER");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_stream_hf ON episodes_streams(hf_id)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_stream_rail_size ON episodes_streams(rail_size)");
  await db.exec("CREATE INDEX IF NOT EXISTS idx_files_size ON files(size)");
}

/** Lấy danh sách URL node Railway từ endpoint /_diag của Worker (dùng khi thiếu HMAC secret cục bộ hoặc bật --diag-fallback). */
async function resolveViaDiag(fid) {
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const r = await fetch(`${DIAG_URL}?file=${encodeURIComponent(fid)}&fast=1`, {
        headers: UA,
        signal: AbortSignal.timeout(12000),
      });
      if (r.ok) {
        const d = await r.json();
        const urls = [];
        const pushJsonUrl = (raw) => {
          try {
            const u = JSON.parse(raw || "{}").url;
            if (u && typeof u === "string" && !/dmm_vo_cc/.test(u) && !urls.includes(u)) urls.push(u);
          } catch {}
        };
        pushJsonUrl(d.direct && d.direct.body);
        for (const m of d.mint || []) pushJsonUrl(m && m.raw);
        if (urls.length) return { urls, errors: [] };
      }
    } catch {}
    if (attempt === 0) await new Promise((res) => setTimeout(res, 1200));
  }
  return { urls: [], errors: ["diag-empty"] };
}

/** Mint vé link dự phòng qua resolve bằng HMAC đã cấu hình (thử direct trước, bị 503 thì relay qua p4k-relay, rồi mới /_diag nếu thiếu secret). */
async function resolveRailwayUrls(fid) {
  let errors = [];
  if (USE_DIRECT_MINT) {
    if (Date.now() >= directBlockedUntil) {
      const res = await resolveDirectUrls(fid, { timeoutMs: 10000, viaRelay: false });
      if (res.urls.length) return { urls: res.urls, errors: [] };
      errors = res.errors;
      if (errors.some((e) => /HTTP503/.test(e))) {
        directBlockedUntil = Date.now() + 120_000;
        if (!loggedDirectFallback) {
          loggedDirectFallback = true;
          console.warn(`[MINT] Direct từ runner bị từ chối (${errors.join(", ")}) -> chuyển sang p4k-relay (${ACTIVE_RESOLVERS.join(",")})`);
        }
      }
    }
    for (let attempt = 0; attempt < 2; attempt++) {
      const relayed = await resolveDirectUrls(fid, { timeoutMs: 10000, viaRelay: true });
      if (relayed.urls.length) return { urls: relayed.urls, errors: [] };
      errors = [...errors, ...relayed.errors];
      if (attempt === 0) await new Promise((r) => setTimeout(r, 1200));
    }
    if (!ALLOW_DIAG_FALLBACK) return { urls: [], errors };
  }
  const fb = await resolveViaDiag(fid);
  return { urls: fb.urls, errors: fb.urls.length ? [] : [...errors, ...fb.errors] };
}

/** Đọc một dải byte từ Railway (tránh follow redirect ngầm) và trích tổng kích thước stream. */
async function fetchRailwayRange(url, start, len) {
  const res = await fetch(url, {
    headers: { "User-Agent": UA["User-Agent"], Range: `bytes=${start}-${start + len - 1}` },
    redirect: "manual",
    signal: AbortSignal.timeout(15000),
  });
  if (res.status !== 206 && res.status !== 200) {
    try { await res.body?.cancel(); } catch {}
    return { ok: false, status: res.status, totalSize: 0, buf: null };
  }
  const buf = Buffer.from(await res.arrayBuffer());
  const cr = res.headers.get("content-range") || "";
  const totalSize = cr.includes("/")
    ? Number(cr.split("/")[1])
    : (res.status === 200 ? Number(res.headers.get("content-length") || 0) : 0);
  return { ok: buf.length > 0, status: res.status, totalSize, buf };
}

/** Đọc dải byte từ file trên Hugging Face (bắt 302 Location rồi tách header Authorization khi qua CDN). */
async function fetchHfRange(hfUrl, start, len) {
  const tok = nextHfToken();
  const hdrs = { Range: `bytes=${start}-${start + len - 1}` };
  if (tok) hdrs.Authorization = "Bearer " + tok;

  let res = await fetch(hfUrl, { headers: hdrs, redirect: "manual", signal: AbortSignal.timeout(15000) });
  if (res.status >= 300 && res.status < 400) {
    const loc = res.headers.get("location");
    try { await res.body?.cancel(); } catch {}
    if (!loc) return null;
    res = await fetch(new URL(loc, hfUrl).toString(), {
      headers: { Range: `bytes=${start}-${start + len - 1}` },
      redirect: "manual",
      signal: AbortSignal.timeout(15000),
    });
  }
  if (res.status !== 206 && res.status !== 200) {
    try { await res.body?.cancel(); } catch {}
    return null;
  }
  return Buffer.from(await res.arrayBuffer());
}

/** Kiểm chứng ứng viên HF khớp 32 byte tại offset giữa luồng video (giải XOR nếu +64 byte header). */
async function verifyCandidate(railUrl, totalSize, cand) {
  const candSize = Number(cand.size);
  const isDisguised = candSize === totalSize + HEADER_SIZE;
  const isRaw = candSize === totalSize;
  if (!isDisguised && !isRaw) return false;

  const mid = totalSize > 4096 ? Math.floor(totalSize / 2) : 0;
  const railMid = await fetchRailwayRange(railUrl, mid, 32);
  if (!railMid.ok || railMid.buf.length < 16) return false;

  const hfStart = isDisguised ? mid + HEADER_SIZE : mid;
  const hfMid = await fetchHfRange(hfRawUrl(cand), hfStart, railMid.buf.length);
  if (!hfMid || hfMid.length < railMid.buf.length) return false;

  if (isDisguised) {
    for (let i = 0; i < railMid.buf.length; i++) {
      hfMid[i] ^= DEFAULT_KEY[(mid + i) & 255];
    }
  }
  return hfMid.subarray(0, railMid.buf.length).equals(railMid.buf);
}

function buildScopeFilters() {
  const whereExtra = [];
  const params = [];
  if (TARGET_TYPE !== "all") {
    whereExtra.push("AND t.type = ?");
    params.push(TARGET_TYPE);
  }
  if (SHARD.total > 1) {
    whereExtra.push("AND ((unicode(substr(e.file_id, 1, 1)) * 31 + unicode(substr(e.file_id, 2, 1)) * 17 + unicode(substr(e.file_id, length(e.file_id), 1))) % ?) = ?");
    params.push(SHARD.total, SHARD.index);
  }
  return { whereExtra, params };
}

/** Ghi trạng thái đối chiếu của một file_id vào episodes_streams (giữ nhất quán backoff & rail_size). */
async function markResult(fid, { status, hfId = null, railSize = null, now }) {
  if (DRY_RUN) return;
  if (status === "ok") {
    await db.exec(
      "UPDATE episodes_streams SET hf_id = ?, hf_status = 'ok', hf_fails = 0, rail_size = NULL, hf_checked_at = ? WHERE file_id = ?",
      [hfId, now, fid]
    );
    return;
  }
  if (status === "no_hf") {
    await db.exec(
      "UPDATE episodes_streams SET hf_status = 'no_hf', hf_fails = 0, rail_size = ?, hf_checked_at = ? WHERE file_id = ?",
      [railSize, now, fid]
    );
    return;
  }
  await db.exec(
    "UPDATE episodes_streams SET hf_status = ?, hf_fails = COALESCE(hf_fails, CASE WHEN hf_checked_at IS NOT NULL THEN 4 ELSE 0 END) + 1, rail_size = COALESCE(?, rail_size), hf_checked_at = ? WHERE file_id = ?",
    [status, railSize, now, fid]
  );
}

/** Resolve Railway link sống và lấy tổng kích thước byte. */
async function resolveLiveRailway(fid) {
  const { urls: railUrls, errors } = await resolveRailwayUrls(fid);
  if (!railUrls.length) return { resolveFailed: true, liveUrl: null, totalSize: 0, lastStatus: 0, forbidden: false, mintErrors: errors };

  let lastStatus = 0;
  let saw403 = false;
  for (const u of railUrls) {
    const probe = await fetchRailwayRange(u, 0, 32);
    lastStatus = probe.status;
    if (probe.status === 403) saw403 = true;
    if (probe.ok && probe.totalSize > 1024) {
      return { resolveFailed: false, liveUrl: u, totalSize: probe.totalSize, lastStatus, forbidden: false };
    }
  }
  return { resolveFailed: false, liveUrl: null, totalSize: 0, lastStatus, forbidden: saw403 };
}

/** Resolve Railway; cập nhật thống kê lỗi và tuyệt đối không ghi dead khi Railway trả HTTP 403. */
async function resolveOrRecordFailure(fid, stats, { prefix = "", shouldLog = false, logLabel = "" } = {}) {
  const rail = await resolveLiveRailway(fid);
  if (rail.resolveFailed) {
    stats.resolveFail++;
    if (stats.resolveFail <= 10) console.warn(`  [${prefix}RESOLVE-FAIL] ${fid}: ${(rail.mintErrors || []).join(", ") || "empty"}`);
    return null;
  }
  if (!rail.liveUrl) {
    if (rail.forbidden || rail.lastStatus === 403) {
      stats.forbidden++;
      if (shouldLog) console.log(`  [${logLabel}] ${fid}: Railway HTTP 403 (bỏ qua, không ghi dead)`);
      return null;
    }
    stats.dead++;
    await markResult(fid, { status: "dead", now: Date.now() });
    if (shouldLog) console.log(`  [${logLabel}] ${fid}: Railway HTTP ${rail.lastStatus} (backoff)`);
    return null;
  }
  return rail;
}

/** Đối chiếu một file Railway đang sống với các ứng viên cùng dung lượng trong bảng files. */
async function matchAgainstHf(fid, liveUrl, totalSize, now, stats, label) {
  const hfRes = await db.exec(
    "SELECT kind, repo_id, path, size FROM files WHERE size IN (?, ?) ORDER BY id DESC LIMIT 12",
    [totalSize + HEADER_SIZE, totalSize]
  );
  const hfRows = hfRes.rows || [];
  if (!hfRows.length) {
    stats.noHfCandidate++;
    await markResult(fid, { status: "no_hf", railSize: totalSize, now });
    console.log(`  [${label}] ${fid}: Railway sống (${totalSize} B) -> lưu rail_size chờ crawl bắt HF`);
    return;
  }

  let matchedRow = null;
  for (const hr of hfRows) {
    if (await verifyCandidate(liveUrl, totalSize, hr)) {
      matchedRow = hr;
      break;
    }
  }

  if (!matchedRow) {
    stats.mismatch++;
    await markResult(fid, { status: "mismatch", railSize: totalSize, now });
    console.log(`  [${label}] ${fid}: Có ${hfRows.length} ứng viên size=${totalSize} nhưng byte giữa file chưa khớp / HF lỗi`);
    return;
  }

  const hfBase = baseIdOf(matchedRow.path);
  stats.matched++;
  await markResult(fid, { status: "ok", hfId: hfBase, now });
  console.log(`  [MATCH ${stats.matched} | ${label}] ${fid} -> ${matchedRow.repo_id}/${matchedRow.path} (hf_id=${hfBase}, size=${matchedRow.size})`);
}

/** Bước 0: Bắt ngay các mã no_hf có rail_size vừa xuất hiện trong files (0 tốn request Railway nếu files chưa có). */
async function checkReadyNoHf(deadline, stats) {
  const { whereExtra, params } = buildScopeFilters();
  const readyRes = await db.exec(
    `SELECT e.file_id, MAX(e.rail_size) AS rail_size
     FROM episodes_streams e
     JOIN titles t ON t.id = e.title_id
     WHERE e.hf_id IS NULL
       AND e.hf_status = 'no_hf'
       AND e.rail_size IS NOT NULL AND e.rail_size > 1024
       AND EXISTS (SELECT 1 FROM files f WHERE f.size IN (e.rail_size, e.rail_size + ${HEADER_SIZE}))
       ${whereExtra.join("\n       ")}
     GROUP BY e.file_id
     LIMIT 200`,
    params
  );
  const rows = readyRes.rows || [];
  if (!rows.length) return;
  console.log(`[NO_HF-READY] Phát hiện ${rows.length} mã no_hf vừa có size tương ứng trong files -> xác minh 32 byte...`);

  let idx = 0;
  async function runReady() {
    while (Date.now() < deadline) {
      const i = idx++;
      if (i >= rows.length) break;
      const fid = rows[i].file_id;
      const savedSize = Number(rows[i].rail_size || 0);
      stats.noHfRechecked++;
      try {
        const rail = await resolveOrRecordFailure(fid, stats, { prefix: "NO_HF " });
        if (!rail) continue;
        await matchAgainstHf(fid, rail.liveUrl, rail.totalSize || savedSize, Date.now(), stats, `NO_HF ${i + 1}/${rows.length}`);
      } catch (e) {
        console.warn(`  [NO_HF ERR] ${fid}: ${e.message || e}`);
      }
    }
  }
  await Promise.all(Array.from({ length: CONCURRENCY }, () => runReady()));
}

async function main() {
  const t0 = Date.now();
  const deadline = t0 + BUDGET_MS;
  const mode = NEW_ONLY ? "new-only" : (HOT_ONLY ? "hot-only" : "full");
  console.log(`=== MATCH-HF START | mode=${mode} type=${TARGET_TYPE} shard=${SHARD.index}/${SHARD.total} dryRun=${DRY_RUN} limit=${LIMIT} conc=${CONCURRENCY} budget=${Math.round(BUDGET_MS / 1000)}s cooldown=${COOLDOWN_MS / 3600000}h ===`);
  console.log(
    USE_DIRECT_MINT
      ? `[MINT] mode=github-direct resolvers=${ACTIVE_RESOLVERS.join(",")} diagFallback=${ALLOW_DIAG_FALLBACK}`
      : `[MINT] mode=cf-worker-diag (thiếu P4K_HMAC_SVPC/P4K_HMAC_SVATV)`
  );

  if (!DRY_RUN) await ensureSchema();

  const stats = { total: 0, noHfRechecked: 0, probed: 0, matched: 0, dead: 0, forbidden: 0, resolveFail: 0, noHfCandidate: 0, mismatch: 0 };
  if (!TARGET_IDS.length) await checkReadyNoHf(deadline, stats);

  let candidates;
  if (TARGET_IDS.length) {
    candidates = TARGET_IDS.map((fid) => ({ file_id: fid }));
  } else {
    const nowMs = Date.now();
    const hMs = 3600 * 1000;
    const c2h = nowMs - Math.min(2 * hMs, COOLDOWN_MS);
    const c6h = nowMs - Math.min(6 * hMs, COOLDOWN_MS);
    const c24h = nowMs - Math.min(24 * hMs, COOLDOWN_MS);
    const cCold = nowMs - COOLDOWN_MS;
    const sqlLimit = LIMIT > 0 ? LIMIT : 50000;
    const scope = buildScopeFilters();
    const params = [];

    let checkedCond;
    if (NEW_ONLY) {
      checkedCond = "e.hf_checked_at IS NULL";
    } else if (HOT_ONLY) {
      checkedCond = `(e.hf_checked_at IS NULL OR (COALESCE(e.hf_status, 'dead') != 'no_hf' AND ((e.hf_fails = 1 AND e.hf_checked_at < ?) OR (e.hf_fails = 2 AND e.hf_checked_at < ?))))`;
      params.push(c2h, c6h);
    } else {
      checkedCond = `(
        e.hf_checked_at IS NULL
        OR (
          COALESCE(e.hf_status, 'dead') != 'no_hf'
          AND (
            (COALESCE(e.hf_fails, 4) <= 1 AND e.hf_checked_at < ?)
            OR (COALESCE(e.hf_fails, 4) = 2 AND e.hf_checked_at < ?)
            OR (COALESCE(e.hf_fails, 4) = 3 AND e.hf_checked_at < ?)
            OR (COALESCE(e.hf_fails, 4) >= 4 AND e.hf_checked_at < ?)
          )
        )
        OR (e.hf_status = 'no_hf' AND e.hf_checked_at < ?)
      )`;
      params.push(c2h, c6h, c24h, cCold, cCold);
    }
    params.push(...scope.params, sqlLimit);

    // Khi quét vét định kỳ (!HOT_ONLY), ưu tiên mã quá hạn kiểm tra lâu nhất trước để fails>=3 / 7d không bị kẹt.
    const orderBy =
      !HOT_ONLY && !NEW_ONLY
        ? `CASE WHEN MAX(e.hf_checked_at) IS NULL THEN 0 ELSE 1 END,
           COALESCE(MIN(e.hf_checked_at), 0) ASC,
           MIN(COALESCE(e.hf_fails, 4)) ASC,
           CASE WHEN length(e.file_id) = 12 THEN 0 ELSE 1 END,
           MIN(CASE WHEN t.type = 'movie' THEN 0 ELSE 1 END),
           MAX(e.title_id) DESC`
        : `CASE WHEN MAX(e.hf_checked_at) IS NULL THEN 0 ELSE 1 END,
           MIN(COALESCE(e.hf_fails, 4)) ASC,
           CASE WHEN length(e.file_id) = 12 THEN 0 ELSE 1 END,
           MIN(CASE WHEN t.type = 'movie' THEN 0 ELSE 1 END),
           MAX(e.created_at) DESC,
           COALESCE(MIN(e.hf_checked_at), 0) ASC,
           MAX(e.title_id) DESC`;

    const res = await db.exec(
      `SELECT e.file_id, MAX(e.title_id) AS title_id
       FROM episodes_streams e
       JOIN titles t ON t.id = e.title_id
       WHERE e.file_id != ''
         AND e.hf_id IS NULL
         AND ${checkedCond}
         AND NOT EXISTS (
           SELECT 1 FROM files f WHERE f.name >= e.file_id || '.' AND f.name < e.file_id || '/'
         )
         ${scope.whereExtra.join("\n         ")}
       GROUP BY e.file_id
       ORDER BY
         ${orderBy}
       LIMIT ?`,
      params
    );
    candidates = res.rows || [];
  }

  stats.total = candidates.length;
  console.log(`Đã nạp ${candidates.length} mã file cần đối chiếu.`);
  let idx = 0;

  async function worker() {
    while (Date.now() < deadline) {
      const myIdx = idx++;
      if (myIdx >= candidates.length) break;
      const fid = candidates[myIdx].file_id;
      const probedNo = ++stats.probed;
      if (probedNo % 100 === 0) {
        const sec = Math.round((Date.now() - t0) / 1000);
        console.log(`  [PROGRESS ${probedNo}/${candidates.length} | ${sec}s] matched=${stats.matched} dead=${stats.dead} forbidden=${stats.forbidden} resolveFail=${stats.resolveFail} noHf=${stats.noHfCandidate} mismatch=${stats.mismatch}`);
      }

      try {
        const rail = await resolveOrRecordFailure(fid, stats, {
          shouldLog: myIdx < 15 || stats.probed % 25 === 0,
          logLabel: `${stats.probed}/${candidates.length}`,
        });
        if (!rail) continue;

        await matchAgainstHf(fid, rail.liveUrl, rail.totalSize, Date.now(), stats, `${stats.probed}/${candidates.length}`);
      } catch (e) {
        console.warn(`  [ERR] ${fid}: ${e.message || e}`);
      }
    }
  }

  await Promise.all(Array.from({ length: CONCURRENCY }, () => worker()));
  const elapsed = Math.round((Date.now() - t0) / 1000);
  console.log("\n=== TỔNG KẾT ĐỐI CHIẾU HF ===");
  console.log(JSON.stringify({ elapsedSec: elapsed, dryRun: DRY_RUN, ...stats }));
}

main().catch((e) => {
  console.error("match-hf fatal:", e);
  process.exit(1);
});
