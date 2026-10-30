/**
 * Phim4K OTV — Stremio/Nuvio addon (Cloudflare Workers, ES Module).
 * PHIÊN BẢN CỨNG HOÁ: giữ nguyên flow gốc, thêm lớp resolver chịu lỗi.
 *
 * ── Thay đổi so với P4K-Stable.txt ───────────────────────────────────────────
 *  1. Ký theo HOST RESOLVER thay vì so khớp chuỗi "cdn.phim4k.lol".
 *     Bản gốc chỉ ký khi URL chứa đúng tên miền đó; nếu API đổi sang host
 *     khác (sv1/sv2/ubicloud) thì stream chết ngay. Nay nhận diện theo host.
 *  2. Phát hiện FALLBACK của resolver: sv1 trả "…/dmm_vo_cc/<file>" (KV miss)
 *     khi chữ ký sai. Bản gốc trả thẳng URL chết cho client. Nay phát hiện
 *     và retry với chữ ký mới.
 *  3. CACHE resolve theo tên file (TTL mặc định 25′). Resolver trả
 *     expires_in=21600 nhưng pool node XOAY VÒNG theo thời gian (đo thực tế:
 *     có lúc toàn bộ pool là *.up.railway.app, có lúc là ubicloud). Cache
 *     ngắn giúp bám pool mới thay vì giữ node đã lỗi.
 *  4. Timeout + retry tách bạch từng tầng, log giới hạn, không rò secret.
 *  5. NODE FALLBACK: resolver trả svvv.gboiz7.workers.dev → đổi sang
 *     sv.p4kzz.workers.dev (cùng pool file). Host dự phòng chết thì giữ host
 *     gốc; sức khoẻ host cache 60s, không probe lại từng file.
 *  6. ĐÃ GỠ cơ chế verify=1/probe đo tốc độ: không còn tải thử 128KB để chấm
 *     điểm node. Node đầu tiên đủ dùng (HTTPS/host ưu tiên) là trả ngay.
 *
 * ── Đã kiểm chứng (2026-10-03) ───────────────────────────────────────────────
 *  - catalog movies/tvseries/search/latest/top_views, meta movie/tvseries: OK
 *  - genVideoUrl(): chữ ký HMAC đúng, resolver trả {"url","expires_in":21600}
 *  - URL đã resolve trả HTTP 206 video/mp4
 *  - Bản gốc VẪN CHẠY ĐÚNG. Các thay đổi trên là phòng thủ, không sửa logic.
 *
 * ── GIỚI HẠN ĐÃ BIẾT (không sửa được từ phía addon) ──────────────────────────
 *  - Resolver CHỈ kiểm chữ ký HMAC, không kiểm email/token/VIP. Thêm tham số
 *    không có tác dụng (đã thử). Vì vậy addon không thể tự vào "nhóm node
 *    Donater" — phải do phía cung cấp API cấp resolver khác.
 *  - Pool node đổi theo thời gian; không kiểm soát được.
 */


/* ═══════════════ p4k-test: KHO SQLITE CỤC BỘ (thay resolver) ═══════════════
 * Luồng stream KHÔNG gọi svpc/svatv nữa. Lấy ID cuối URL cdn.phim4k.lol,
 * tra bảng `files` trong phim4k.db → dựng link HuggingFace trực tiếp.
 * Không fallback: tra không thấy → 404.
 */
const PROCESS_ENV = (typeof process !== "undefined" && process.env) ? process.env : {};
let ENV = PROCESS_ENV;
// Nạp secrets từ Cloudflare Worker env (worker.mjs gọi setEnv(env) ở request đầu).
export function setEnv(e) { ENV = e || {}; applySecrets(ENV); }
const HF_ENDPOINT = String(PROCESS_ENV.HF_ENDPOINT || "https://huggingface.co").replace(/\/$/, "");

// DB adapter inject: { row(id) -> row|null, variants(id) -> rows[] } (async).
let DB = null;
export function setDb(db) { DB = db; }
function _baseOf(id) { return id.includes(".") ? id.slice(0, id.lastIndexOf(".")) : id; }
async function dbRow(id) { if (!DB) throw new Error("db not set"); return DB.row(id); }
async function dbVariants(id) { if (!DB) throw new Error("db not set"); return DB.variants(id); }

function fmtBytes(n) {
  n = Number(n) || 0;
  if (n >= 1e12) return (n / 1e12).toFixed(2) + " TB";
  if (n >= 1e9) return (n / 1e9).toFixed(2) + " GB";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + " MB";
  return Math.max(1, Math.round(n / 1e3)) + " KB";
}
function hfRepo(row) { return String(row.repo_id || "").replace(/^BUCKET:/, ""); }   // bucket.mjs lưu kèm tiền tố
function hfDirectUrl(row) {
  const p = row.path, repo = hfRepo(row);
  if (row.kind === "bucket") return HF_ENDPOINT + "/buckets/" + repo + "/resolve/" + p;
  if (row.kind === "dataset") return HF_ENDPOINT + "/datasets/" + repo + "/resolve/main/" + p;
  return HF_ENDPOINT + "/" + repo + "/resolve/main/" + p;
}

const VERSION = "2.1.0-app-dyn";

/* ═══════════════════════ 1. CẤU HÌNH ═══════════════════════ */

let CONFIG_SECRET = "";

// Config động như app Phim4K 2.8.4: app KHÔNG nhúng host — mọi host (api/cdn)
// đều đến từ config AES-GCM. Thử theo thứ tự; rip trả cdn=svpc (đúng bộ mobile).
const CONFIG_SOURCES = [
  "https://rip.cryboiz.workers.dev/api/add",
  "https://ltv.cryboiz.workers.dev/api/add",
];

// Dự phòng nếu cả worker config đều chết (đã kiểm chứng 2026-10-06, 10-12).
// Giá trị này CHỈ là dự phòng — mọi host thật đến từ config động.
const FALLBACK_CFG = {
  apiBase: "https://api.phim4k.space/rest-api/v130/",
  jsonBase: "https://api.phim4k.space/rest-api/v130/",
  cdnBase: "https://svpc.cryboiz.workers.dev",
  cdnFallback: "",
  apiKey: "",
  tmdbKey: "",
  prefer: [],
  signedHosts: [],
};

// Mirror API (đã kiểm: api.phim4k.space và api.phim4k.online trả y hệt nhau).
// getJson thất bại ở base chính (mạng/timeout/5xx) thì thử từng mirror.
const API_MIRRORS = [
  "https://api.phim4k.space/rest-api/v130/",
  "https://api.phim4k.online/rest-api/v130/",
];

let API_USER = "admin";
let API_PASS = "";

const ADDON_NAME = "Nothing-P • P4K";

// Secret theo từng resolver (kiểm chứng thực nghiệm 2026-10-12):
//   svpc  -> f7a2…  (resolver của mobile 2.8.4, config rip trỏ cdn=svpc)
//   svatv -> 5e8d…  (cộng đồng/cũ, nhúng trong libapi_config.so của 270/271)
let HMAC_SECRETS = {};
let HMAC_SECRET = "";

// Resolver 503 "STREAM_DOMAIN_UNAVAILABLE" theo egress: từ IP VN-FPT (và cả colo
// CF SEA như HKG/TWN) resolver chối 503; egress nước ngoài (US/Google) mint vé OK.
// Node (sv.p4kzz…) không khóa theo IP → vé mint bất kỳ egress nào vẫn play 206.
// EGRESS_MIRRORS: pass-through proxy để đổi IP khi gọi resolver. Đã rà 30/10:
// allorigins/codetabs/allorigins2/webcors chết, whateverorigin 500, corsproxy 401,
// cors.lol 429 — CHỈ r.jina còn mint được. Các mirror chết vừa tốn request vừa
// giữ subrequest slot (gây "stalled response" / giảm tốc), nên bỏ khỏi danh sách.
const EGRESS_MIRRORS = [
  { name: "p4k-relay", wrap: (u) => "https://p4k-relay.vercel.app/api/relay?url=" + encodeURIComponent(u), ms: 6000, headers: { Accept: "application/json" } },
  { name: "r.jina.ai", wrap: (u) => "https://r.jina.ai/" + u, ms: 6000, headers: {} },
];
// Ghi chú (tạm giữ cấu hình cũ nếu cần bật lại một mirror khác):
// { name: "allorigins", wrap: (u) => "https://api.allorigins.win/raw?url=" + encodeURIComponent(u), ms: 6000, headers: { Accept: "application/json" } },
// { name: "codetabs", wrap: (u) => "https://api.codetabs.com/v1/proxy?quest=" + encodeURIComponent(u), ms: 6000, headers: { Accept: "application/json" } },
// { name: "whateverorigin", wrap: (u) => "https://www.whateverorigin.org/get?url=" + encodeURIComponent(u), ms: 6000, headers: { Accept: "application/json" } },
// Egress mint ưu tiên theo resolver TỪ CONFIG (rip chính → ltv dự phòng).
// Hằng số này CHỈ dùng khi config chết hẳn (FALLBACK_CFG cũng rơi về đây):
// svpc (mobile 2.8.4) đã kiểm chứng mint tốt ở egress nước ngoài.
const EGRESS_HOST = "svpc.cryboiz.workers.dev";

let TMDB_API_KEY = "";
const TMDB_API_BASE = "https://api.themoviedb.org/3/";

// Nạp toàn bộ secret từ env (Worker env hoặc process.env khi chạy local).
// Tên biến env: CONFIG_SECRET, P4K_API_KEY, P4K_TMDB_KEY, P4K_API_USER,
// P4K_API_PASS, P4K_HMAC_SVPC, P4K_HMAC_SVATV, TMDB_API_KEY.
function applySecrets(env) {
  env = env || {};
  CONFIG_SECRET = env.CONFIG_SECRET || "";
  API_USER = env.P4K_API_USER || "admin";
  API_PASS = env.P4K_API_PASS || "";
  FALLBACK_CFG.apiKey = env.P4K_API_KEY || "";
  FALLBACK_CFG.tmdbKey = env.P4K_TMDB_KEY || "";
  HMAC_SECRETS = {
    "svpc.cryboiz.workers.dev": env.P4K_HMAC_SVPC || "",
    "svatv.cryboiz.workers.dev": env.P4K_HMAC_SVATV || "",
  };
  HMAC_SECRET = HMAC_SECRETS["svatv.cryboiz.workers.dev"] || HMAC_SECRETS["svpc.cryboiz.workers.dev"] || "";
  TMDB_API_KEY = env.TMDB_API_KEY || "";
}
applySecrets(PROCESS_ENV);


// Dự phòng cho TMDB. api.themoviedb.org hay bị nhà mạng chặn (FPT reset
// connection ở tầng TLS), còn cinemeta.strem.io thì không. Chỉ tra được theo
// imdb id (tt...), nhưng đủ để thay bước "tt -> tmdb_id -> tên" khi TMDB chết.
const CINEMETA_BASE = "https://v3-cinemeta.strem.io/meta/";

// Tham số thời gian
const UPSTREAM_TIMEOUT_MS = 10000;
const RESOLVE_TIMEOUT_MS = 8000;
const HEALTH_TIMEOUT_MS = 6000;   // kiểm host dự phòng có sống không
const RESOLVE_ATTEMPTS = 2;      // số lần thử lấy node tốt
const DIRECT_RESOLVE_BUDGET_MS = 1500; // chờ trực tiếp tối đa trước khi chuyển egress
const RESOLVE_CACHE_TTL_MS = 25 * 60 * 1000;
const DYNAMIC_TTL_MS = 10 * 60 * 1000;

// file_url từ API LUÔN phải đi qua resolver mới có node phát được:
// genVideoUrl() dựng URL từ cfg.cdnBase, không quan tâm host trong file_url.
// Nên mặc định coi mọi URL là cần ký. Muốn giới hạn thì đặt config key
// "signed_hosts" (mảng hostname) — không hardcode tên host vào code.

const enc = new TextEncoder();
const dec = new TextDecoder();

let dynamicCache = { at: 0, value: null };
let resolveCache = new Map();   // filename -> {url, at}
let poolDeadMs = 0;             // epoch ms khi phát hiện resolver toàn-503 → bỏ direct tạm thời
const POOL_DEAD_WINDOW_MS = 60 * 1000; // nhớ "pool chết" trong 60s
const EGRESS_ALWAYS = false;    // bật true = mint egress luôn, không thử resolver trước
// Lưu ý: nếu đặt EGRESS_ALWAYS=true thì bỏ luôn phần chờ thử resolver ~0.5s/request.

/* ═══════════════════════ 2. TIỆN ÍCH ═══════════════════════ */

function toHex(bytes) {
  let s = "";
  for (const b of bytes) s += b.toString(16).padStart(2, "0");
  return s;
}
async function sha256(input) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", input));
}
async function hmacSha256(keyStr, msgStr) {
  const key = await crypto.subtle.importKey(
    "raw", enc.encode(keyStr), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]
  );
  return new Uint8Array(await crypto.subtle.sign("HMAC", key, enc.encode(msgStr)));
}
function xorIntToHex8(ts, mask) {
  const out = [(ts >>> 24) & 0xff, (ts >>> 16) & 0xff, (ts >>> 8) & 0xff, ts & 0xff]
    .map((v, i) => v ^ mask[i]);
  return toHex(out);
}
function safeDecode(s) {
  try { return decodeURIComponent(s); } catch { return s; }
}
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

function safeHostname(u) { try { return new URL(u).hostname; } catch { return "?"; } }

// Trả về giá trị đầu tiên "thật" (non-null) trong tập promise đang chạy song song.
// budgetMs = null thì đợi hết; có ngân sách thì hết hạn trả null (không hủy promise).
function firstNonNull(promises, budgetMs) {
  return new Promise((resolve) => {
    if (!promises.length) { resolve(null); return; }
    let pending = promises.length;
    const timer = budgetMs != null ? setTimeout(() => resolve(null), budgetMs) : null;
    for (const p of promises) {
      Promise.resolve(p).then((v) => {
        if (v) { if (timer) clearTimeout(timer); resolve(v); }
        else if (--pending === 0) { if (timer) clearTimeout(timer); resolve(null); }
      }, () => {
        if (--pending === 0) { if (timer) clearTimeout(timer); resolve(null); }
      });
    }
  });
}

async function fetchWithTimeout(url, init = {}, ms = UPSTREAM_TIMEOUT_MS) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), ms);
  try { return await fetch(url, { ...init, signal: ctrl.signal }); }
  finally { clearTimeout(timer); }
}

// Hủy body của những Response KHÔNG định đọc (503/4xx/5xx, response thua cuộc,
// probe không cần nội dung…). Bỏ qua không hủy → CF giữ kết nối treo, quá số
// lượng in-flight sẽ phải HỦY response cũ ("stalled HTTP response"). cancel()
// là idempotent, an toàn sau khi body đã đọc hết hoặc bị lỗi giữa chừng.
function discardResponse(res) {
  try {
    const body = res && res.body;
    if (body && typeof body.cancel === "function") body.cancel().catch(() => {});
  } catch { }
}

class HttpError extends Error {
  constructor(status, message) { super(message); this.status = status; }
}
function corsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}
function json(body, status = 200, extra = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "public, max-age=60",
      ...corsHeaders(), ...extra,
    },
  });
}

async function withRetry(fn, attempts = 2) {
  let lastErr;
  for (let i = 0; i < attempts; i++) {
    try { return await fn(); }
    catch (e) {
      lastErr = e;
      if (e instanceof HttpError && e.status < 500) throw e;
      if (i < attempts - 1) await sleep(Math.min(400 * (i + 1), 900));
    }
  }
  throw lastErr;
}

/* ═══════════════════════ 3. CONFIG ĐỘNG ═══════════════════════ */

function b64urlToBytes(s) {
  try {
    let b = String(s).replace(/-/g, "+").replace(/_/g, "/");
    while (b.length % 4) b += "=";
    const bin = atob(b);
    const u = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i);
    return u;
  } catch { return null; }
}

// Worker /api/add trả {alg,encoding,iv,ciphertext}; key = SHA256(CONFIG_SECRET).
// Tag 16 byte nằm CUỐI ciphertext (giống WebCrypto subtle.encrypt).
// Lấy SONG SONG hết các nguồn trong CONFIG_SOURCES để giữ ưu tiên:
//   nguồn đầu (rip) = resolver chính (cdnBase),
//   nguồn sau (ltv) = resolver dự phòng (cdnFallback).
// Resolve trực tiếp + egress mint đều thử theo đúng thứ tự đó.
async function fetchDynamicConfig() {
  if (Date.now() - dynamicCache.at < DYNAMIC_TTL_MS) return dynamicCache.value;
  const fallback = { ...FALLBACK_CFG };
  const build = (data, fallback, source) => {
    const url = (v) => (v ? "https://" + v : "");
    return {
      source,
      apiBase: url(data.api) ? url(data.api) + "/rest-api/v130/" : fallback.apiBase,
      jsonBase: url(data.json) ? url(data.json) + "/rest-api/v130/" : fallback.jsonBase,
      cdnBase: url(data.cdn) || fallback.cdnBase,
      cdnFallback: "",
      free1: url(data.free1),
      free2: url(data.free2),
      image: url(data.image),
      introdb: url(data.introdb),
      medata: url(data.medata),
      tmdblogo: url(data.tmdblogo),
      tmdbKey: data.tmdb || fallback.tmdbKey,
      apiKey: data.tokens || fallback.apiKey,
      prefer: Array.isArray(data.prefer) ? data.prefer : fallback.prefer,
      signedHosts: Array.isArray(data.signed_hosts) ? data.signed_hosts : fallback.signedHosts,
    };
  };
  try {
    const key = await crypto.subtle.importKey(
      "raw", await sha256(enc.encode(CONFIG_SECRET)), { name: "AES-GCM" }, false, ["decrypt"]
    );
    const sources = await Promise.allSettled(CONFIG_SOURCES.map(async (src) => {
      try {
        const res = await fetchWithTimeout(src,
          { headers: { Accept: "application/json" } }, 4000);
        if (!res.ok) { discardResponse(res); return null; }
        const j = await res.json();
        const iv = b64urlToBytes(j && j.iv);
        const full = b64urlToBytes(j && j.ciphertext);
        if (!iv || !full || full.length <= 16) return null;
        const plain = await crypto.subtle.decrypt(
          { name: "AES-GCM", iv, tagLength: 128 }, key, full
        );
        return { src, data: (JSON.parse(dec.decode(plain)).data) || {} };
      } catch { return null; }
    }));
    const ok = sources.map((s) => s.status === "fulfilled" ? s.value : null).filter(Boolean);
    if (!ok.length) {
      dynamicCache = { at: Date.now(), value: fallback };
      return fallback;
    }
    const prim = build(ok[0].data, fallback, ok[0].src);
    // Resolver dự phòng: từ nguồn config thứ 2 (ltv), nếu khác resolver chính.
    if (ok[1] && ok[1].src !== ok[0].src) {
      const secCdn = "https://" + (ok[1].data.cdn || "").replace(/\/$/, "");
      if (secCdn !== prim.cdnBase) prim.cdnFallback = secCdn;
    }
    dynamicCache = { at: Date.now(), value: prim };
    return prim;
  } catch {
    dynamicCache = { at: Date.now(), value: fallback };
    return fallback;
  }
}

/* ═══════════════════════ 4. UPSTREAM OTV CLIENT ═══════════════════════ */

function authHeaders(cfg) {
  return {
    "API-KEY": (cfg && cfg.apiKey) || FALLBACK_CFG.apiKey,
    "Authorization": "Basic " + btoa(API_USER + ":" + API_PASS),
    "Accept": "application/json",
  };
}

async function getJson(path, cfg, ms = UPSTREAM_TIMEOUT_MS) {
  const bases = [];
  const b0 = (cfg && cfg.apiBase) || FALLBACK_CFG.apiBase;
  bases.push(b0);
  for (const m of API_MIRRORS) if (m !== b0) bases.push(m);
  let lastErr;
  for (const base of bases) {
    try {
      return await withRetry(async () => {
        const res = await fetchWithTimeout(base + path, { headers: authHeaders(cfg) }, ms);
        if (!res.ok) { discardResponse(res); throw new HttpError(res.status === 404 ? 404 : 502, "upstream HTTP " + res.status); }
        try { return await res.json(); }
        catch { throw new HttpError(502, "upstream invalid json"); }
      });
    } catch (e) {
      lastErr = e;
      if (e instanceof HttpError && e.status === 404) throw e;
      if (!(e instanceof HttpError) || e.status >= 500) continue; // lỗi mạng/5xx → thử mirror kế
      throw e;
    }
  }
  throw lastErr || new HttpError(502, "upstream unreachable");
}

/* ═══════════════════════ 5. RESOLVER (LỚP ĐÃ CỨNG HOÁ) ═══════════════════════ */

async function deriveDailySecret(secret, dateStr) {
  const keyBytes = await sha256(enc.encode(secret));
  const iv = (await sha256(enc.encode("iv:" + secret))).slice(0, 12);
  const key = await crypto.subtle.importKey("raw", keyBytes, { name: "AES-GCM" }, false, ["encrypt"]);
  const ct = new Uint8Array(await crypto.subtle.encrypt(
    { name: "AES-GCM", iv, tagLength: 128 }, key, enc.encode(secret + ":" + dateStr)));
  return secret + ":" + toHex(ct);
}

// genVideoUrl(fileName, host, secret): ký theo HOST + SECRET được truyền vào.
async function genVideoUrl(fileName, host, secret) {
  const dateStr = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  const signingSecret = await deriveDailySecret(secret, dateStr);
  const mask = (await hmacSha256(signingSecret, "otp-ts-mask")).slice(0, 4);
  const now = Math.floor(Date.now() / 1000);
  const fn = fileName.trim();
  const token = toHex(await hmacSha256(signingSecret, fn + ":" + now));
  const ts = xorIntToHex8(now, mask);
  const base = String(host || "").replace(/\/$/, "");
  return base + "/" + fn + "?token=" + token + "&ts=" + ts;
}

// Secret theo host resolver; host có thể có/không có scheme.
function secretForResolver(host) {
  let h = "";
  try { h = new URL(host).hostname; } catch { h = host; }
  return HMAC_SECRETS[h] || HMAC_SECRET;
}

// Danh sách resolver theo thứ tự ưu tiên:
//   chính (cdnBase do rip cấp) → dự phòng (cdnFallback do ltv cấp) → các
//   resolver đã biết còn lại (nạp từ HMAC_SECRETS, không hardcode thêm host).
// egress=true: CHỈ theo config (chính → dự phòng), không trộn thêm resolver lạ.
function resolverList(cfg, egress = false) {
  const list = [];
  const push = (u) => { const v = String(u || "").replace(/\/$/, ""); if (v && list.indexOf(v) < 0) list.push(v); };
  push((cfg && cfg.cdnBase) || FALLBACK_CFG.cdnBase);
  push(cfg && cfg.cdnFallback);
  if (!egress) {
    for (const host of Object.keys(HMAC_SECRETS)) push("https://" + host);
  }
  return list;
}

function fileNameFrom(url) {
  const s = String(url || "").trim();
  if (!s) return "";
  return s.slice(s.lastIndexOf("/") + 1).split("?")[0];
}

// Resolver trả fallback ".../dmm_vo_cc/<file>" khi chữ ký KHÔNG được chấp nhận
// (Worker KV miss). URL đó luôn 404. Phải coi là lỗi và thử lại.
function isResolverFallback(url) {
  return /\/dmm_vo_cc\//.test(String(url || ""));
}

/* ── Node dự phòng ───────────────────────────────────────────────────────────
 * Resolver đang trả host `from`; nếu host này chết/bị chặn thì đổi sang `to`
 * (cùng pool file — đã kiểm: cùng content-range). Đổi host xong mới kiểm nhanh
 * host có sống không: host CÒN TRẢ LỜI (kể cả 404/403 = file chết) tính là
 * sống, chỉ timeout/lỗi mạng mới coi là chết → giữ host gốc. Sức khoẻ host
 * cache 60s để không probe lại cho từng file. Đặt `to: ""` để tắt.
 */
const NODE_FALLBACK = { from: "svvv.gboiz7.workers.dev", to: "sv.p4kzz.workers.dev" };
const HOST_HEALTH_TTL_MS = 60 * 1000;
const hostHealth = new Map();   // hostname -> {ok, at}

function swapNodeHost(url) {
  if (!NODE_FALLBACK.to) return url;
  try {
    const u = new URL(url);
    if (u.hostname !== NODE_FALLBACK.from) return url;
    u.hostname = NODE_FALLBACK.to;
    return u.toString();
  } catch { return url; }
}

async function hostAlive(url) {
  let host;
  try { host = new URL(url).hostname; } catch { return false; }
  const cached = hostHealth.get(host);
  if (cached && Date.now() - cached.at < HOST_HEALTH_TTL_MS) return cached.ok;
  const p = await healthProbe(url);
  const ok = !!(p.ok || typeof p.status === "number");
  hostHealth.set(host, { ok, at: Date.now() });
  return ok;
}

async function applyNodeFallback(url) {
  const swapped = swapNodeHost(url);
  if (swapped === url) return url;
  return (await hostAlive(swapped)) ? swapped : url;
}

/* ── Chọn node ───────────────────────────────────────────────────────────────
 * Resolver xoay vòng pool theo thời gian, và tên host đổi liên tục — trong
 * cùng một ngày đã thấy *.up.railway.app, *.k8s.ubicloud.com rồi
 * sv.p4kzz.workers.dev. Nên KHÔNG hardcode tên host vào đây.
 *
 * Ưu tiên (nếu muốn) lấy từ config động: key "prefer" là mảng tên host.
 * Không có key đó thì mọi node HTTPS đều ngang nhau.
 *
 * Còn lại: ưu tiên HTTPS. Bật/tắt bằng ?prefer=1 hoặc ?prefer=0.
 */
const MAX_CANDIDATES = 4;

// Regex ưu tiên dựng từ cfg.prefer. Không có config → null (không ưu tiên host).
function preferredRegex(cfg) {
  const list = (cfg && cfg.prefer) || [];
  if (!Array.isArray(list) || list.length === 0) return null;
  const parts = list.map((h) => String(h).trim().toLowerCase())
    .filter(Boolean)
    .map((h) => h.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  if (parts.length === 0) return null;
  return new RegExp("(^|\\.)(" + parts.join("|") + ")$", "i");
}

// Chấm điểm node: host ưu tiên (nếu config có) > HTTPS.
function scoreCandidate(c, cfg) {
  let s = 0;
  try {
    const u = new URL(c.url);
    const re = preferredRegex(cfg);
    if (re && re.test(u.hostname)) s += 100;
    if (u.protocol === "https:") s += 40;
    else s -= 15;                       // node chỉ HTTP: dễ bị đánh cắp link
  } catch { return -1e9; }
  return s;
}

// Node đủ dùng để trả luôn mà không cần gom thêm: HTTPS, hoặc nằm trong danh
// sách ưu tiên (nếu có). Không phụ thuộc tên host cụ thể nào.
function isGoodEnough(c, cfg) {
  try {
    const u = new URL(c.url);
    const re = preferredRegex(cfg);
    if (re && re.test(u.hostname)) return true;
    return u.protocol === "https:";
  } catch { return false; }
}

// Kiểm host dự phòng có sống không: tải 1 khoảng nhỏ. Host CÒN TRẢ LỜI
// (kể cả 404/403) tính là sống, chỉ timeout/lỗi mạng mới coi là chết.
async function healthProbe(url) {
  try {
    const res = await fetchWithTimeout(url,
      { headers: { Range: "bytes=0-131071" } }, HEALTH_TIMEOUT_MS);
    if (!res.ok) { discardResponse(res); return { ok: false, status: res.status }; }
    const buf = await res.arrayBuffer();
    return { ok: true, bytes: buf.byteLength };
  } catch { return { ok: false, error: "probe failed" }; }
}

async function resolveOnce(fileName, cfg, opts = {}) {
  const resolvers = resolverList(cfg);
  const attemptDirect = async (host) => {
    try {
      const secret = secretForResolver(host);
      const signed = await genVideoUrl(fileName, host, secret);
      const res = await fetchWithTimeout(signed,
        { headers: { Accept: "application/json" } }, RESOLVE_TIMEOUT_MS);
      if (res.status === 503) {
        // 503 = chữ ký HỢP LỆ nhưng upstream stream-domain đang chết
        // (STREAM_DOMAIN_UNAVAILABLE) → không đứng chờ, để giai đoạn egress lo.
        discardResponse(res);
        return null;
      }
      if (!res.ok) {
        discardResponse(res);
        return null;
      }
      let j;
      try { j = await res.json(); }
      catch { discardResponse(res); return null; }
      if (!j || typeof j.url !== "string" || !j.url) return null;
      if (isResolverFallback(j.url)) return null;
      return { url: j.url, expiresIn: j.expires_in, host };
    } catch { return null; }
  };

  // Trực tiếp: chạy TOÀN BỘ resolver của config CÙNG LÚC, lấy vé hợp lệ đầu tiên.
  // Trước đây tuần tự nên độ trễ = TỔNG timeout các resolver; giờ ≈ 1 request.
  // Ngân sách 2.5s: pool SỐNG thì trả ngay, pool chết thì 503 rơi nhanh,
  // resolver câm/treo thì không kéo dài mà chuyển sang egress luôn.
  const recentlyDead = Date.now() - poolDeadMs < POOL_DEAD_WINDOW_MS;
  // "Đổi IP luôn từ đầu": bật ?emint=1 (hoặc EGRESS_ALWAYS, hoặc vừa dính 503
  // trong cửa sổ gần đây) → bỏ hẳn giai đoạn trực tiếp, mint egress ngay.
  if (opts.emint || EGRESS_ALWAYS || recentlyDead) {
    return resolveViaEgress(fileName, cfg);
  }
  const direct = (resolvers.length ? resolvers : [EGRESS_HOST]).map(attemptDirect);
  const found = await firstNonNull(direct, DIRECT_RESOLVE_BUDGET_MS);
  if (found) { poolDeadMs = 0; return found; }
  poolDeadMs = poolDeadMs || Date.now();

  // Resolver 503 theo egress (VD: IP VN-FPT bị chối, egress quốc tế mint vé OK):
  // sang giai đoạn egress mint qua proxy đổi IP.
  return resolveViaEgress(fileName, cfg);
}

// Ký CHÍNH XÁC rồi đưa qua proxy đổi IP để mint vé. Theo THỨ TỰ ƯU TIÊN như API
// meta: resolver chính (cdnBase = svpc) thử TRƯỚC, cdnFallback (svatv) là DỰ
// PHÒNG — host trước thất bại mới sang host sau. Các mirror TẠI một host chạy
// song song (hiện chỉ r.jina). Config chết hẳn thì rơi về EGRESS_HOST (svpc).
function hostName(h) { try { return new URL(h).hostname; } catch { return String(h || "").replace(/^https?:\/\//, ""); } }
// r.jina bóp IP datacenter (CF colo): đổ quá nhiều mint song song → throttle/abort
// → nhiều file bị skip âm thầm ("lúc được lúc không"). Giới hạn ~3 mint cạnh tranh
// để r.jina xử lý từng đợt, vừa ổn định hơn vừa không tạo burst subrequest cho CF.
const EGRESS_MAX_CONCURRENT = 3;
let egressInFlight = 0;
const egressWaiters = [];
function egressSlot() {
  return new Promise((res) => {
    if (egressInFlight < EGRESS_MAX_CONCURRENT) { egressInFlight++; res(); }
    else egressWaiters.push(res);
  });
}
function egressRelease() {
  egressInFlight = Math.max(0, egressInFlight - 1);
  if (egressWaiters.length) { egressInFlight++; egressWaiters.shift()(); }
}
async function resolveViaEgress(fileName, cfg) {
  await egressSlot();
  try {
    return await resolveViaEgressInner(fileName, cfg);
  } finally { egressRelease(); }
}
async function resolveViaEgressInner(fileName, cfg) {
  let hosts = resolverList(cfg, true);
  if (!hosts.length) hosts = [EGRESS_HOST];
  const reasons = [];
  for (const host of hosts) {
    const secret = secretForResolver(host);
    if (!secret) { reasons.push(hostName(host) + "=no-secret"); continue; }
    const tasks = EGRESS_MIRRORS.map((m) => (async () => {
      const signed = await genVideoUrl(fileName, host, secret);
      try {
        const res = await fetchWithTimeout(m.wrap(signed),
          { headers: { Accept: "application/json", ...(m.headers || {}) } }, m.ms);
        if (!res.ok) { discardResponse(res); return { name: m.name, status: "HTTP" + res.status }; }
        const minted = extractMint(await res.text(), host);
        if (minted) return { url: minted.url, expiresIn: minted.expires_in, host: "egress/" + hostName(host) };
        return { name: m.name, status: "no-mint" };
      } catch (e) { return { name: m.name, status: String(e.message || e.name || e).slice(0, 60) }; }
    })());
    const rs = await Promise.all(tasks);
    const hit = rs.find((r) => r && r.url);
    if (hit) return hit;
    for (const r of rs) reasons.push(hostName(host) + "/" + r.name + "=" + r.status);
  }
  throw new HttpError(502, "cdn resolve egress failed (" + reasons.join("; ").slice(0, 160) + ")");
}

// Lọc vé "mint" thật khỏi mọi định dạng đóng gói của proxy:
//   1) wrapper JSON {"code":200,"data":{"content":"Markdown Content:\n{…}"}} (r.jina + Accept json)
//   2) raw markdown "Markdown Content:\n{…}" (r.jina mặc định)
//   3) {"contents":"{…}"} (whateverorigin)
//   4) JSON trần {url:…} (proxy trong suốt)
// VÉ HỢP LỆ: url https + KHÔNG trỏ về chính resolver đang thử + không fallback.
function extractMint(text, attemptedHost) {
  let rn = "";
  try { rn = new URL(attemptedHost).hostname; } catch { rn = String(attemptedHost || "").replace(/^https?:\/\//, ""); }
  const pick = (cands) => {
    for (const c of cands) {
      let cn = "";
      try { cn = (c && c.url) ? new URL(c.url).hostname : ""; } catch { }
      if (c && typeof c.url === "string" && /^https?:\/\//.test(c.url)
          && cn !== rn
          && !isResolverFallback(c.url)
          && !/dmm_vo_cc/.test(c.url)) return c;
    }
    return null;
  };
  const inContent = (seg) => {
    const a = seg.indexOf("{"), b = seg.lastIndexOf("}");
    if (a >= 0 && b > a) { try { return [JSON.parse(seg.slice(a, b + 1))]; } catch { } }
    return [];
  };
  const cands = [];
  try { const w = JSON.parse(text); if (w && w.data && typeof w.data.content === "string") cands.push(...inContent(w.data.content)); } catch { }
  try { const w2 = JSON.parse(text); if (w2 && typeof w2.contents === "string") cands.push(...inContent(w2.contents)); } catch { }
  const mk = text.lastIndexOf("Markdown Content:");
  if (mk >= 0) cands.push(...inContent(text.slice(mk)));
  if (!cands.length) { try { cands.push(JSON.parse(text)); } catch { } }
  return pick(cands);
}

// Lấy node tốt. Nhiều lần thử vì pool xoay vòng: chữ ký cũ có thể bị từ chối,
// node trả về có thể chỉ-HTTP. Chiến lược:
//   - thử tối ưu: nếu node ĐẦU TIÊN đã đúng ý (host ưu tiên, HTTPS) → trả luôn (1 request)
//   - không thì gom tối đa MAX_CANDIDATES node rồi chọn điểm cao nhất
async function resolveCdnUrl(fileUrl, cfg, opts = {}) {
  const fileName = fileNameFrom(fileUrl);
  if (!fileName) throw new HttpError(502, "bad cdn file url");

  const prefer = opts.prefer !== false;
  const cached = resolveCache.get(fileName);
  if (cached && Date.now() - cached.at < RESOLVE_CACHE_TTL_MS) {
    return cached.url;
  }

  const candidates = [];
  let lastErr;

  for (let attempt = 0; attempt < RESOLVE_ATTEMPTS; attempt++) {
    let got;
    try { got = await resolveOnce(fileName, cfg, opts); }
    catch (e) { lastErr = e; await sleep(200 + attempt * 250); continue; }

    const cand = { url: await applyNodeFallback(got.url), expiresIn: got.expires_in };

    // Node đầu tiên đủ dùng là trả luôn (1 request), không tải thử gì thêm.
    if (!prefer || isGoodEnough(cand, cfg)) {
      resolveCache.set(fileName, { url: cand.url, at: Date.now() });
      return cand.url;
    }
    candidates.push(cand);

    if (candidates.length >= MAX_CANDIDATES) break;
    await sleep(150 + attempt * 200);
  }

  if (candidates.length === 0) {
    if (cached && cached.url) return cached.url;
    throw lastErr || new HttpError(502, "cdn resolve failed");
  }

  candidates.sort((a, b) => scoreCandidate(b, cfg) - scoreCandidate(a, cfg));
  const best = candidates[0];
  resolveCache.set(fileName, { url: best.url, at: Date.now() });
  return best.url;
}

// Nhận diện theo config, không theo danh sách host hardcode.
function isSignedUrl(u, cfg) {
  const list = (cfg && cfg.signedHosts) || [];
  if (Array.isArray(list) && list.length > 0) {
    try { return list.includes(new URL(u).hostname); } catch { return false; }
  }
  return true;
}

async function resolveStreamUrl(fileUrl, cfg, opts = {}) {
  // p4k-test: KHÔNG gọi resolver svpc/svatv. Lấy ID cuối URL cdn → tra DB SQL
  // cục bộ → dựng link HuggingFace trực tiếp. Không fallback.
  const id = fileNameFrom(fileUrl);
  if (!id) throw new HttpError(502, "empty file_url");
  const row = await dbRow(id);
  if (!row) throw new HttpError(404, "id not in db: " + id);
  return hfDirectUrl(row);
}

function attachSubtitles(stream, item) {
  if (Array.isArray(item.subtitles) && item.subtitles.length > 0) {
    const subs = item.subtitles.filter((s) => s && s.url)
      .map((s) => ({ url: s.url, lang: s.srclang || s.language || "vi" }));
    if (subs.length) stream.subtitles = subs;
  }
}

// Chạy fn với tối đa `limit` tác vụ đồng thời; trả [{status,value|reason}] theo thứ tự.
async function mapLimit(arr, limit, fn) {
  const res = new Array(arr.length);
  let i = 0;
  const run = async () => {
    while (i < arr.length) {
      const idx = i++;
      try { res[idx] = { status: "fulfilled", value: await fn(arr[idx], idx) }; }
      catch (e) { res[idx] = { status: "rejected", reason: e }; }
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, arr.length) }, run));
  return res;
}

const FALLBACK_CONCURRENCY = 3;

async function resolveMediaSet(items, cfg, opts = {}) {
  // Ưu tiên CHÍNH lấy từ DB (link HuggingFace trực tiếp). File nào DB KHÔNG có
  // thì chuyển về resolver gốc (svpc/svatv theo config động) làm luồng Dự Phòng.
  const dbUnits = [];   // { item, row, vi, total }
  const fbItems = [];   // item không có trong DB
  for (const it of items) {
    const id = fileNameFrom(it.fileUrl);
    const vars = id ? await dbVariants(id) : [];
    if (!vars.length) { fbItems.push(it); continue; }
    vars.forEach((row, vi) => dbUnits.push({ item: it, row, vi, total: vars.length }));
  }
  if (!dbUnits.length && !fbItems.length) throw new HttpError(502, "empty file set");

  const out = [];
  // 1) Nguồn chính: DB → HuggingFace trực tiếp (giữ tên account-repo như cũ).
  for (const u of dbUnits) {
    let title = u.item.label;
    try {
      if (typeof title === "string" && /💾\s*N\/A/.test(title) && u.row.size) {
        title = title.replace(/💾\s*N\/A/, "💾 " + fmtBytes(u.row.size));
      }
      if (typeof title === "string") title = title + "\n📁 " + hfRepo(u.row);
      if (u.total > 1 && typeof title === "string") title = title + "\n🔀 Bản " + (u.vi + 1) + "/" + u.total;
    } catch {}
    const stream = { name: "⚡ " + ADDON_NAME, url: hfDirectUrl(u.row), title: title || undefined };
    attachSubtitles(stream, u.item);
    out.push(stream);
  }
  // 2) Dự phòng: DB không có → resolve gốc; ghi rõ "Dự Phòng".
  if (fbItems.length) {
    const settled = await mapLimit(fbItems, FALLBACK_CONCURRENCY, (it) => resolveCdnUrl(it.fileUrl, cfg, opts));
    settled.forEach((r, idx) => {
      if (!r || r.status !== "fulfilled" || !r.value) return;
      const it = fbItems[idx];
      let title = it.label;
      try { if (typeof title === "string") title = title + "\n📁 Dự Phòng"; } catch {}
      const stream = { name: "⚡ " + ADDON_NAME, url: r.value, title: title || undefined };
      attachSubtitles(stream, it);
      out.push(stream);
    });
  }
  if (out.length) return out;
  throw new HttpError(404, "không có luồng (DB miss & resolve thất bại)");
}

/* ═══════════════════════ 6. TMDB / ID MAPPING ═══════════════════════ */

const EXTERNAL_ID_MAP = new Map();

function normalizeMovieName(v) {
  return String(v || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}
function yearFromValue(v) {
  const m = String(v || "").match(/(19|20)\d{2}/);
  return m ? m[0] : "";
}
function stripTrailingYear(v) {
  return normalizeMovieName(v).replace(/\s+(19|20)\d{2}$/, "").trim();
}
// Cinemeta: không cần API key, không bị chặn. Trả moviedb_id (= tmdb),
// imdb_id, name, released — đủ để dựng tên khi TMDB không gọi được.
async function cinemetaFetch(type, id, ms = UPSTREAM_TIMEOUT_MS) {
  try {
    const res = await fetchWithTimeout(CINEMETA_BASE + type + "/" + encodeURIComponent(id) + ".json",
      { headers: { Accept: "application/json" } }, ms);
    if (!res.ok) { discardResponse(res); return null; }
    const j = await res.json();
    return (j && j.meta) || null;
  } catch { return null; }
}

// TMDB_API_KEY đến từ config động (key "tmdb"); chỉ fallback về hằng số khi
// worker config chết. Chèn key vào path thay vì để trong base để không phải
// sửa chuỗi query ở mọi nơi.
async function tmdbFetch(path, cfg, ms = UPSTREAM_TIMEOUT_MS) {
  const key = (cfg && cfg.tmdbKey) || TMDB_API_KEY;
  const [base, query = ""] = path.split("?");
  const qs = query ? key + "&" + query : "api_key=" + key;
  try {
    const res = await fetchWithTimeout(TMDB_API_BASE + base + "?" + qs,
      { headers: { Accept: "application/json" } }, ms);
    if (!res.ok) { discardResponse(res); return null; }
    return await res.json();
  } catch { return null; }
}
function unwrapSearchItems(data, apiType) {
  if (Array.isArray(data)) return data;
  if (!data || typeof data !== "object") return [];
  if (Array.isArray(data.data)) return data.data;
  if (Array.isArray(data[apiType])) return data[apiType];
  if (Array.isArray(data.results)) return data.results;
  if (Array.isArray(data.items)) return data.items;
  if (Array.isArray(data.movies)) return data.movies;
  if (Array.isArray(data.tvseries)) return data.tvseries;
  if (data.data && typeof data.data === "object") {
    if (Array.isArray(data.data.items)) return data.data.items;
    if (Array.isArray(data.data.results)) return data.data.results;
    if (Array.isArray(data.data[apiType])) return data.data[apiType];
  }
  return [];
}
async function searchP4K(query, mediaType, cfg, ms) {
  const q = String(query || "").trim().replace(/\+/g, " ");
  if (!q) return [];
  const apiType = mediaType === "series" || mediaType === "tvseries" ? "tvseries" : "movie";
  const path = "search?q=" + encodeURIComponent(q) + "&type=" + apiType;
  try {
    const data = await getJson(path, cfg, ms);
    return unwrapSearchItems(data, apiType).filter((x) => x && (x.videos_id || x.id));
  } catch { return []; }
}
function catalogQuery(catalogId, page) {
  switch (catalogId) {
    case "phim4k-otv-movies": return "movies?page=" + page;
    case "phim4k-otv-series": return "tvseries?page=" + page;
    case "phim4k-otv-series-latest": return "latest_tvseries?limit=20&page=" + page;
    case "phim4k-otv-top": return "top_views?period=all&limit=20&page=" + page;
    default: return null;
  }
}

const CACHE_ORIGIN = "https://p4k-cache.internal";
function cacheReq(path) { return new Request(CACHE_ORIGIN + "/" + path); }
async function cacheGetJSON(path) {
  try {
    if (typeof caches === "undefined") return null;
    const r = await caches.default.match(cacheReq(path));
    return r ? await r.json() : null;
  } catch { return null; }
}
async function cachePutJSON(path, obj, maxAge) {
  try {
    if (typeof caches === "undefined") return;
    await caches.default.put(cacheReq(path), new Response(JSON.stringify(obj), {
      headers: { "content-type": "application/json", "cache-control": "max-age=" + maxAge },
    }));
  } catch {}
}

async function cacheExternal(baseId, tmdbId, imdbId, realId) {
  EXTERNAL_ID_MAP.set(baseId, realId);
  if (tmdbId) {
    EXTERNAL_ID_MAP.set(tmdbId, realId);
    EXTERNAL_ID_MAP.set("tmdb:" + tmdbId, realId);
    EXTERNAL_ID_MAP.set("tmdb_" + tmdbId, realId);
  }
  if (imdbId) {
    EXTERNAL_ID_MAP.set(String(imdbId), realId);
    const c = String(imdbId).startsWith("tt") ? String(imdbId) : "tt" + imdbId;
    EXTERNAL_ID_MAP.set(c, realId);
  }
  await cachePutJSON("ext/" + encodeURIComponent(baseId), { id: realId || null }, realId ? 86400 : 600);
}

async function resolveExternalToVideosId(baseId, mediaType, cfg, ms) {
  if (EXTERNAL_ID_MAP.has(baseId)) return EXTERNAL_ID_MAP.get(baseId);
  const extCached = await cacheGetJSON("ext/" + encodeURIComponent(baseId));
  if (extCached) {
    if (extCached.id) EXTERNAL_ID_MAP.set(baseId, extCached.id);
    return extCached.id || null;
  }
  const tType = mediaType === "series" || mediaType === "tvseries" ? "tv" : "movie";
  let targetTmdbId = null, targetImdbId = null, targetTitle = "", targetOriginalTitle = "", targetYear = "";

  if (baseId.indexOf("tt") === 0) {
    targetImdbId = baseId;
    const find = await tmdbFetch("find/" + encodeURIComponent(baseId) +
      "?external_source=imdb_id", cfg, ms);
    const results = find ? (tType === "tv" ? (find.tv_results || []) : (find.movie_results || [])) : [];
    if (results.length > 0) {
      const hit = results[0];
      targetTmdbId = String(hit.id);
      targetTitle = hit.title || hit.name || "";
      targetOriginalTitle = hit.original_title || hit.original_name || "";
      targetYear = yearFromValue(hit.release_date || hit.first_air_date);
    } else {
      // TMDB không trả lời (bị chặn hoặc key hết hạn) — thử Cinemeta.
      const cmType = tType === "tv" ? "series" : "movie";
      const cm = await cinemetaFetch(cmType, baseId, ms);
      if (cm) {
        const md = cm.moviedb_id || cm.moviedb;
        if (md) targetTmdbId = String(md);
        targetTitle = cm.name || targetTitle;
        targetOriginalTitle = targetOriginalTitle || cm.name || "";
        targetYear = yearFromValue(cm.released || cm.year) || targetYear;
        if (!targetImdbId && cm.imdb_id) targetImdbId = String(cm.imdb_id);
      }
      if (!targetTmdbId && !targetTitle) { await cacheExternal(baseId, null, null, null); return null; }
    }
  } else if (baseId.indexOf("tmdb:") === 0 || baseId.indexOf("tmdb_") === 0) {
    targetTmdbId = baseId.replace(/^tmdb[:_]/, "");
  }
  if (!targetTmdbId && !targetTitle) return null;

  const det = targetTmdbId ? await tmdbFetch(`${tType}/${targetTmdbId}`, cfg, ms) : null;
  if (det) {
    targetTitle = det.title || det.name || targetTitle;
    targetOriginalTitle = det.original_title || det.original_name || targetOriginalTitle;
    targetYear = yearFromValue(det.release_date || det.first_air_date) || targetYear;
    if (!targetImdbId && tType === "movie") targetImdbId = det.imdb_id || null;
  }

  const names = [targetTitle, targetOriginalTitle].map((x) => String(x || "").trim()).filter(Boolean);
  const alt = targetTmdbId
    ? await tmdbFetch(`${tType}/${targetTmdbId}/alternative_titles`, cfg, ms)
    : null;
  const altItems = tType === "tv" ? ((alt && alt.results) || []) : ((alt && alt.titles) || []);
  for (const a of altItems) { const n = a && (a.title || a.name); if (n) names.push(n); }
  const uniqueNames = [...new Set(names)].slice(0, 12);

  if (uniqueNames.length > 0) {
    const byId = new Map();
    // Search song song theo từng tên (cũ: tuần tự 12 tên × ~400ms ≈ 5s).
    const searchRuns = await Promise.all(
      uniqueNames.map((name) => searchP4K(name, mediaType, cfg, ms).catch(() => []))
    );
    for (const items of searchRuns) {
      for (const item of items) {
        const rid = String(item.videos_id || item.id || "");
        if (rid && !byId.has(rid)) byId.set(rid, item);
      }
    }
    const wanted = uniqueNames.map(stripTrailingYear).filter(Boolean);
    const scored = [...byId.values()].map((item) => {
      const candidateName = stripTrailingYear(String(item.title || ""));
      const candidateYear = yearFromValue(item.release || item.title || "");
      let nameScore = 0;
      for (const w of wanted) {
        if (!w) continue;
        if (candidateName === w) nameScore = Math.max(nameScore, 100);
        else if (w.length >= 4 && candidateName.includes(w)) nameScore = Math.max(nameScore, 85);
        else if (candidateName.length >= 4 && w.includes(candidateName)) nameScore = Math.max(nameScore, 70);
      }
      let yearScore = 0;
      if (targetYear && candidateYear) yearScore = candidateYear === targetYear ? 30 : -35;
      return { item, score: nameScore + yearScore, nameScore };
    }).sort((a, b) => b.score - a.score);

    const best = scored.find((c) => c.nameScore >= 70 && c.score >= 65);
    if (best) {
      const realId = String(best.item.videos_id || best.item.id || "");
      if (realId) { await cacheExternal(baseId, targetTmdbId, targetImdbId, realId); return realId; }
    }
  }

  const wantedNames = new Set(uniqueNames.map(normalizeMovieName).filter(Boolean));
  const wantTv = tType === "tv";
  const candidates = [];
  // Fallback: lấy 3 page top-view song song (cũ: tuần tự ~3 × 400ms).
  const pages = [1, 2, 3].map(async (page) => {
    const q = catalogQuery("phim4k-otv-top", page);
    if (!q) return [];
    const data = await getJson(q, cfg, ms).catch(() => []);
    return Array.isArray(data) ? data : [];
  });
  const pageDatas = await Promise.all(pages);
  for (const data of pageDatas) {
    for (const item of data) {
      const vid = String(item.videos_id || item.id || "");
      const isTv = String(item.is_tvseries || "0") === "1";
      if (!vid || isTv !== wantTv) continue;
      candidates.push({ id: "videos_" + vid, name: item.title || "", release: item.release || item.title || "" });
    }
  }
  const fb = candidates.map((c) => {
    const n = normalizeMovieName(c.name), y = yearFromValue(c.release);
    let ns = 0;
    for (const w of wantedNames) {
      if (n === w) ns = Math.max(ns, 100);
      else if (w.length >= 4 && (n.includes(w) || w.includes(n))) ns = Math.max(ns, 70);
    }
    let ys = 0;
    if (targetYear && y) ys = y === targetYear ? 30 : -35;
    return { c, score: ns + ys, nameScore: ns };
  }).sort((a, b) => b.score - a.score).find((x) => x.nameScore >= 70 && x.score >= 65);

  if (fb) {
    const realId = String(fb.c.id).replace(/^videos_/, "");
    if (realId) { await cacheExternal(baseId, targetTmdbId, targetImdbId, realId); return realId; }
  }
  await cacheExternal(baseId, targetTmdbId, targetImdbId, null);
  return null;
}

function parseId(id) {
  const parts = String(id || "").split(":");
  let baseId, season = null, episode = null;
  if (parts[0] === "otv" || parts[0] === "tmdb") {
    if (parts.length < 2 || parts.length > 4) return null;
    baseId = parts[0] + ":" + parts[1];
    season = parts.length >= 3 ? parseInt(parts[2], 10) : null;
    episode = parts.length >= 4 ? parseInt(parts[3], 10) : null;
  } else if (parts[0] === "tt" && parts.length >= 2) {
    // Dạng "tt:1630027" (Nuvio/Stremio hay dùng) lẫn "tt1630027".
    baseId = "tt" + parts[1];
    season = parts.length >= 3 ? parseInt(parts[2], 10) : null;
    episode = parts.length >= 4 ? parseInt(parts[3], 10) : null;
  } else if (/^(videos_|tmdb_|tt)/.test(parts[0])) {
    baseId = parts[0];
    season = parts.length >= 2 ? parseInt(parts[1], 10) : null;
    episode = parts.length >= 3 ? parseInt(parts[2], 10) : null;
  } else return null;
  const internal = baseId.indexOf("otv:") === 0 || baseId.indexOf("videos_") === 0;
  const cleanId = baseId.replace(/^(otv:|videos_|tmdb:|tmdb_|tt)/, "");
  if (!cleanId) return null;
  if (internal && !/^\d+$/.test(cleanId)) return null;
  return { baseId, cleanId, season, episode, internal };
}

/* ═══════════════════════ 7. NHẬN DIỆN TẬP ═══════════════════════ */

function collectEpisodeEntries(d) {
  const out = [], seen = new WeakSet();
  function looksLikeEpisode(x) {
    if (!x || typeof x !== "object" || Array.isArray(x)) return false;
    return x.episodes_name != null || x.episodes_id != null ||
           x.file_url != null || x.stream_key != null || x.subtitle != null;
  }
  function walk(node, seasonHint, indexHint = 0) {
    if (!node || typeof node !== "object") return;
    if (Array.isArray(node)) { for (let i = 0; i < node.length; i++) walk(node[i], seasonHint, i); return; }
    if (seen.has(node)) return;
    seen.add(node);
    let nextSeason = seasonHint;
    if (node.seasons_name != null || node.season_name != null ||
        node.seasonName != null || node.type === "season") nextSeason = node;
    if (looksLikeEpisode(node)) out.push({ ep: node, seasonEntry: nextSeason, epIdx: indexHint });
    for (const [key, value] of Object.entries(node)) {
      if (key === "videos" || key === "cast" || key === "genre" || key === "director") continue;
      walk(value, nextSeason, 0);
    }
  }
  walk(d, null, 0);
  return out;
}

// Năm không bao giờ là số tập. Bản gốc có regex "…<số>…4K" khớp nhầm vào NĂM,
// biến "001-005.S1.2023.4K.mkv" thành tập 2023. Chặn bằng khoảng năm.
function looksLikeYear(n) { return n >= 1900 && n <= 2100; }

// Trả {season, episode, episodeEnd}. episodeEnd > episode nghĩa là file BATCH
// gộp nhiều tập (ví dụ "001-005") → áp dụng cho mọi tập trong khoảng.
function episodeInfo(seasonEntry, ep) {
  const episodeName = String((ep && (ep.episodes_name || ep.name || ep.title)) || "");
  const file = String((ep && (ep.file_url || ep.url)) || "").split("?")[0].split("/").pop();
  const text = episodeName + " " + file;
  let season = null, episode = null, episodeEnd = null;

  // 1) SxxExx — chuẩn nhất, luôn ưu tiên tuyệt đối.
  const se = text.match(/(?:^|[^a-z0-9])s\s*0*(\d{1,2})\s*[._ -]?\s*e\s*0*(\d{1,4})(?!\d)/i);

  // 2) Batch "001-005" / "01-05" ở đầu tên file.
  const batch = episodeName.match(/^\s*0*(\d{1,4})\s*[-–]\s*0*(\d{1,4})(?!\d)/);

  // 3) "E130", "Ep7", "Episode 12" ở đầu tên.
  const eNum = episodeName.match(/^\s*e(?:p|pisode)?\s*[._ -]*0*(\d{1,4})(?!\d)/i);

  // 4) "Tập 12" / "tap 12" (không dấu).
  const tap = text.match(/t[aậ]p\s*[._ -]*0*(\d{1,4})(?!\d)/i);

  // 5) CJK 第12集
  const cjk = text.match(/第\s*0*(\d{1,4})\s*[集話]/);

  // 6) Số ở đầu tên: "01 TVP 4K HQ.mkv" → 1
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

  if (episode === null && eNum && !looksLikeYear(parseInt(eNum[1], 10))) {
    episode = parseInt(eNum[1], 10);
  }
  if (episode === null && tap && !looksLikeYear(parseInt(tap[1], 10))) {
    episode = parseInt(tap[1], 10);
  }
  if (episode === null && cjk && !looksLikeYear(parseInt(cjk[1], 10))) {
    episode = parseInt(cjk[1], 10);
  }
  if (episode === null && lead && !looksLikeYear(parseInt(lead[1], 10))) {
    episode = parseInt(lead[1], 10);
  }

  // 7) Field trực tiếp từ API, chỉ dùng khi tên file không cho biết.
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
    const label = String((seasonEntry &&
      (seasonEntry.seasons_name || seasonEntry.season_name ||
       seasonEntry.seasonName || seasonEntry.name || seasonEntry.title)) || "");
    const sm = label.match(/season\s*0*(\d+)/i) || label.match(/m[uù]a\s*0*(\d+)/i) ||
               label.match(/ph[aầ]n\s*0*(\d+)/i) || label.match(/(?:^|[^a-z0-9])s\s*0*(\d{1,2})(?!\d)/i);
    if (sm) season = parseInt(sm[1], 10);
  }
  if (season === null) season = 1;
  return { season, episode, episodeEnd };
}

function classifyEpisode(seasonEntry, ep, epIdx) {
  const info = episodeInfo(seasonEntry, ep);
  const episode = info.episode === null && Number.isInteger(epIdx) ? epIdx + 1 : info.episode;
  return { season: info.season, episode, episodeEnd: info.episodeEnd };
}

// Tập `want` có nằm trong khoảng file batch không.
function episodeMatches(c, wantSeason, wantEpisode) {
  if (Number(c.season) !== Number(wantSeason)) return false;
  if (c.episode === null) return false;
  const from = Number(c.episode);
  const to = c.episodeEnd ? Number(c.episodeEnd) : from;
  const w = Number(wantEpisode);
  return w >= from && w <= to;
}

function collectEpisodeSources(d, season, episode) {
  const out = [], seen = new Set();
  const entries = collectEpisodeEntries(d);
  const wS = Number(season), wE = Number(episode);
  for (const item of entries) {
    const e = item.ep, s = item.seasonEntry;
    const c = classifyEpisode(s, e, item.epIdx);
    const fileUrl = String((e && (e.file_url || e.url)) || "").trim();
    if (!fileUrl) continue;
    if (!episodeMatches(c, wS, wE)) continue;
    const streamKey = String((e && e.stream_key) || "").trim();
    const sourceId = String((e && (e.episodes_id || e.id)) || "").trim();
    const dedup = fileUrl + "|" + streamKey + "|" + sourceId;
    if (seen.has(dedup)) continue;
    seen.add(dedup);
    const subLangs = Array.isArray(e.subtitle) ? e.subtitle.map((x) => x && (x.srclang || x.language)).filter(Boolean) : [];
    const srcObj = {
      title: d.title, season: wS, episode: wE, source: e.episodes_name, subtitle: e.subtitle,
      container: e.file_type, variant: (s && (s.seasons_name || s.season_name)) || "", subtitleLangs: subLangs,
      releaseInfo: d.video_quality,
    };
    out.push({
      fileUrl,
      label: buildStreamTitle(srcObj),
      subtitles: e.subtitle,
      w: streamTierScore(e.episodes_name, srcObj),
    });
  }
  return out;
}

/* ═══════════════════════ 8. META ═══════════════════════ */

function cleanText(v) { const s = String(v ?? "").trim(); return s || undefined; }

function normalizeMetaDate(value, fallbackYear) {
  const s = String(value ?? "").trim();
  if (s) {
    const dt = new Date(s);
    if (!Number.isNaN(dt.getTime())) return dt.toISOString();
    const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (m) return `${m[1]}-${m[2]}-${m[3]}T00:00:00.000Z`;
    const y = s.match(/^(19|20)\d{2}$/)?.[0];
    if (y) return `${y}-01-01T00:00:00.000Z`;
    const ym = s.match(/\b((?:19|20)\d{2})\b/)?.[1];
    if (ym) return `${ym}-01-01T00:00:00.000Z`;
  }
  const fy = String(fallbackYear ?? "").match(/\b((?:19|20)\d{2})\b/)?.[1];
  return fy ? `${fy}-01-01T00:00:00.000Z` : undefined;
}
function episodeReleasedDate(e, d) {
  const value = e?.released ?? e?.release ?? e?.air_date ?? e?.airDate ??
                e?.date ?? e?.published_at ?? e?.publishedAt ?? d?.release;
  return normalizeMetaDate(value, d?.release);
}

// Bóc tag biến thể tiếng Việt (PĐ/TM/LT/AI/VS) từ tên mùa/tập.
function variantTags(seasonEntry, ep, extra) {
  const s = String((seasonEntry && (seasonEntry.seasons_name || seasonEntry.season_name)) || "") +
    " " + String((ep && (ep.episodes_name || ep.label || ep.name)) || "") +
    " " + String(extra || "");
  const U = s.toUpperCase();
  const out = [];
  if (/(?:PĐ|PHỤ\s?ĐỀ|PHU\s?DE|VIETSUB|VST|SUBTITLE|\bSUB\b)/.test(U)) out.push("PĐ");
  if (/(?:\bTM\b|THUYẾT\s?MINH|THUYET\s?MINH)/.test(U)) out.push("TM");
  if (/(?:\bLT\b|LỒNG\s?TIẾNG|LONG\s?TIENG|DUBBED)/.test(U)) out.push("LT");
  if (/\bAI\b/.test(U)) out.push("AI");
  if (/\bVS\b/.test(U)) out.push("VS");
  return out;
}
// Gộp tag: PĐ/TM/LT nối bằng "-" (vd PĐ-TM), rồi AI · VS.
function tagLine(set) {
  const has = (x) => set.has(x);
  const vn = [has("PĐ") && "PĐ", has("TM") && "TM", has("LT") && "LT"].filter(Boolean).join("-");
  return [vn, has("AI") && "AI", has("VS") && "VS"].filter(Boolean).join(" · ");
}

function detailToMeta(d, type) {
  const meta = {
    id: "otv:" + d.videos_id,
    type,
    name: cleanText(d.title) || "Untitled",
    description: cleanText(d.description),
    poster: cleanText(d.poster_url),
    background: cleanText(d.thumbnail_url),
    releaseInfo: [d.video_quality, d.runtime].filter(Boolean).join(" · ") || undefined,
    imdbRating: d.imdb_rating != null && String(d.imdb_rating).trim() !== ""
      ? String(d.imdb_rating) : undefined,
    genres: (Array.isArray(d.genre) ? d.genre : []).map((g) => cleanText(g && g.name)).filter(Boolean),
    cast: (Array.isArray(d.cast) ? d.cast : []).map((c) => cleanText(c && c.name)).filter(Boolean),
    director: (Array.isArray(d.director) ? d.director : []).map((x) => cleanText(x && x.name)).filter(Boolean),
  };
  if (type === "movie") {
    // Phim lẻ: gộp tag biến thể từ mọi bản (label) + video_quality vào releaseInfo.
    const tagSet = new Set();
    for (const v of (Array.isArray(d.videos) ? d.videos : [])) {
      for (const t of variantTags(null, v, d.video_quality)) tagSet.add(t);
    }
    const tl = tagLine(tagSet);
    if (tl) meta.releaseInfo = [meta.releaseInfo, tl].filter(Boolean).join(" · ");
    return meta;
  }

  meta.videos = [];
  const byEpisode = new Map();
  for (const item of collectEpisodeEntries(d)) {
    const c = classifyEpisode(item.seasonEntry, item.ep || {}, item.epIdx);
    if (c.episode === null) continue;
    const season = Number(c.season);
    if (!Number.isFinite(season)) continue;
    // File batch "001-005" phục vụ MỌI tập trong khoảng → mở rộng thành nhiều video.
    const from = Number(c.episode);
    const to = c.episodeEnd ? Number(c.episodeEnd) : from;
    if (!Number.isFinite(from)) continue;
    const span = Math.min(to, from + 60); // chặn loạt bệnh hoạn dữ liệu
    const tags = variantTags(item.seasonEntry, item.ep, d.video_quality);
    for (let n = from; n <= span; n++) {
      const key = season + ":" + n;
      if (!byEpisode.has(key)) byEpisode.set(key, { season, episode: n, ep: item.ep || {}, tags: new Set() });
      const rec = byEpisode.get(key);
      for (const t of tags) rec.tags.add(t);
    }
  }
  const grouped = [...byEpisode.values()].sort((a, b) => a.season - b.season || a.episode - b.episode);
  for (const item of grouped) {
    const e = item.ep;
    const tl = tagLine(item.tags || new Set());
    const video = {
      id: `otv:${d.videos_id}:${item.season}:${item.episode}`,
      title: `${cleanText(d.title) || "Phim"} · Mùa ${item.season} · Tập ${item.episode}` + (tl ? ` · ${tl}` : ""),
      season: item.season, episode: item.episode,
    };
    const thumb = cleanText(e.image_url || d.thumbnail_url);
    if (thumb) video.thumbnail = thumb;
    const released = episodeReleasedDate(e, d);
    if (released) video.released = released;
    meta.videos.push(video);
  }
  return meta;
}

let detailCache = new Map();   // type:id -> {d, at}
const DETAIL_CACHE_TTL_MS = 10 * 60 * 1000;

async function getDetailCached(videosId, type, cfg, ms) {
  const key = (type === "series" ? "s" : "m") + ":" + videosId;
  const hit = detailCache.get(key);
  if (hit && Date.now() - hit.at < DETAIL_CACHE_TTL_MS) return hit.d;
  const ck = "detail/" + (type === "series" ? "s" : "m") + "/" + videosId;
  const shared = await cacheGetJSON(ck);
  if (shared && shared.videos_id) {
    detailCache.set(key, { d: shared, at: Date.now() });
    return shared;
  }
  const d = await fetchDetail(videosId, type, cfg, ms);
  if (d && d.videos_id) {
    detailCache.set(key, { d, at: Date.now() });
    await cachePutJSON(ck, d, 600);
  }
  return d;
}

async function fetchDetail(videosId, type, cfg, ms) {
  const otvType = type === "series" ? "tvseries" : "movie";
  return getJson("single_details?type=" + otvType + "&id=" + encodeURIComponent(videosId), cfg, ms);
}

/* ═══════════════════════ 9. HANDLERS ═══════════════════════ */

// Title stream 5 dòng:
//   💎/⭐/🎬 (4K/FHD/khác) Tên phim (+ Mùa · Tập)
//   💿 Chất lượng · Nguồn · Codec video (H264/H265/HDR/DV…) [IN HOA]
//   🔊 Chuẩn âm thanh
//   🌐 Ngôn ngữ · Phụ đề (PĐ / TM / PĐ-TM)
//   💾 Dung lượng (MB/GB/TB)
function parseStreamMeta(source, subtitle, extraContext = {}) {
  const s = String(source || "").replace(/_/g, " ").toUpperCase();
  const ctx = String([extraContext.variant, extraContext.releaseInfo].filter(Boolean).join(" ")).replace(/_/g, " ").toUpperCase();
  const allText = (s + " " + ctx).trim();

  const m = { quality: "-", video: "-", hdr: [], audio: [], size: "-", lang: [], container: "-" };

  // 1. Container: ưu tiên đuôi file thực từ source, fallback về container API nếu hợp lệ
  const ct = s.match(/\.(MKV|MP4|AVI|MOV|WEBM|M2TS|TS)\b/);
  const VALID_CONTAINERS = new Set(["MKV", "MP4", "AVI", "MOV", "WEBM", "M2TS", "TS"]);
  if (ct) {
    m.container = ct[1];
  } else if (extraContext.container) {
    const cUpper = String(extraContext.container).toUpperCase();
    if (VALID_CONTAINERS.has(cUpper)) m.container = cUpper;
  }

  // 2. Dung lượng
  const sz = s.match(/\b(\d[\d.,]*)\s?(GB|MB|TB)\b/);
  if (sz) m.size = sz[1] + " " + sz[2];

  // 3. Chất lượng (Quality): ưu tiên tên file, dự phòng sang variant / releaseInfo
  const q = s.match(/\b(2160P|2160|4K|UHD|1080P|1080|FULL.?HD|FHD|720P|720|MHD|HD|CAM|TS)\b/)
         || allText.match(/\b(2160P|2160|4K|UHD|1080P|1080|FULL.?HD|FHD|720P|720|MHD|HD|CAM|TS)\b/);
  if (q) {
    const t = q[1].replace(/\s+/g, "").toUpperCase();
    m.quality = /2160P|2160|4K|UHD/.test(t) ? "4K"
      : /1080P|1080|FULL.?HD|FHD/.test(t) ? "FHD"
      : /720P|720/.test(t) ? "720p"
      : /CAM|TS/.test(t) ? "CAM" : "HD";
  }

  // 4. Codec Video (nhận diện cả H.264, H.265, X.264, X.265 có dấu chấm)
  const vc = s.match(/\b(X\.?265|H\.?265|HEVC|X\.?264|H\.?264|AVC|VC-1|AV1|VP9|MPEG-?4|DIVX|XVID)\b/);
  if (vc) {
    const t = vc[1].replace(/\./g, "").toUpperCase();
    m.video = /HEVC/.test(t) ? "H265"
      : /H26[45]/.test(t) ? t
      : /X26[45]/.test(t) ? t
      : /AV1/.test(t) ? "AV1" : /VP9/.test(t) ? "VP9" : t;
  }
  if (/\b10BIT\b/.test(s) && m.video !== "-") m.video += " (10bit)";

  // 5. Nguồn phát (Source)
  if (/\b(BDREMUX|REMUX)\b/.test(s)) m.source = "BluRay Remux";
  else if (/\b(WEBRIP)\b/.test(s)) m.source = "WebRip";
  else if (/\b(WEB-?DL|WEBDL)\b/.test(s)) m.source = "WEB-DL";
  else if (/\b(BLURAY|BLU-?RAY|BLU)\b/.test(s)) m.source = "BluRay";
  else if (/\b(BRRIP)\b/.test(s)) m.source = "BDRip";
  else if (/\b(HDRIP)\b/.test(s)) m.source = "HDRip";
  else if (/\b(DVDRIP)\b/.test(s)) m.source = "DVDRip";
  else if (/\b(HDTV)\b/.test(s)) m.source = "HDTV";
  else if (/\bWEB\b/.test(s)) m.source = "WEB";
  else m.source = "";

  // 6. HDR / Dải động (quét cả trong tên file và context)
  if (/DOLBY.?VISION|\bDOVI\b|\bDV\b/.test(allText)) m.hdr.push("Dolby Vision");
  if (/HDR10\s*\+|HDR10 ?PLUS|HDR ?\+|HDR ?PLUS/.test(allText)) m.hdr.push("HDR10+");
  else if (/\bHDR10\b/.test(allText)) m.hdr.push("HDR10");
  else if (/\bHLG\b/.test(allText)) m.hdr.push("HLG");
  else if (/\bHDR\b/.test(allText)) m.hdr.push("HDR");
  else if (/\bEDR\b/.test(allText)) m.hdr.push("EDR");
  else if (/\bSDR\b/.test(allText)) m.hdr.push("SDR");
  if (/\b60FPS\b/.test(allText)) m.hdr.push("60fps");

  // 7. Âm thanh (Audio)
  if (/ATMOS/.test(allText)) m.audio.push("Atmos");
  if (/TRUEHD/.test(allText)) m.audio.push("TRUEHD");
  if (/DTS-HD/.test(allText)) m.audio.push("DTS-HD");
  else if (/\bDTS\b/.test(allText)) m.audio.push("DTS");
  if (/\bDDP|EAC-?3|AC-?3|DOLBY.?DIGITAL|\bDD\b|\bDD\+/.test(allText)) m.audio.push("DDP/EAC3");
  if (/AAC/.test(allText)) m.audio.push("AAC");
  if (/FLAC/.test(allText)) m.audio.push("FLAC");
  const ch = allText.match(/\b([2567]\.1|2\.0|MONO)\b/)
          || allText.match(/(?:DDP|DD\+|DD|EAC-?3|AC-?3|TRUEHD|DTS-HD|DTS|AAC)[._\s]*([2567])[._\s]*([01])(?!\d)/i);
  if (ch) {
    if (ch[2] !== undefined) m.audio.push(ch[1] + "." + ch[2]);
    else m.audio.push(ch[1]);
  }

  // 8. Định dạng nguồn Việt / Phụ đề / Thuyết minh / Lồng tiếng
  const hasSub = /(?:^|[^a-zA-Z0-9])(?:P[ĐD]|PHỤ\s?ĐỀ|PHU\s?DE|VIET\s?SUB|VST|SUBTITLE)(?:$|[^a-zA-Z0-9])|\bVIE\b|\bSUB\b/i.test(allText)
    || (Array.isArray(subtitle) && subtitle.length > 0)
    || (Array.isArray(extraContext.subtitleLangs) && extraContext.subtitleLangs.length > 0);
  const hasTM = /THUYET.?MINH|THUYẾT.?MINH|THUYETMINH|(?:^|[^a-zA-Z0-9])(?:TM|T\.M)(?:$|[^a-zA-Z0-9])/i.test(allText);
  const hasLT = /LỒNG.?TIẾNG|LONG.?TIENG|LỒNG TIẾNG|(?:^|[^a-zA-Z0-9])LT(?:$|[^a-zA-Z0-9])|DUBBED/i.test(allText);
  const hasAI = /(?:^|[^a-zA-Z0-9])AI(?:$|[^a-zA-Z0-9])/i.test(allText);
  const hasVS = /(?:^|[^a-zA-Z0-9])VS(?:$|[^a-zA-Z0-9])/i.test(allText);

  const tags = [];
  if (hasSub) tags.push("PĐ");
  if (hasTM) tags.push(hasAI ? "TM (AI)" : "TM");
  else if (hasAI) tags.push("AI");
  if (hasLT) tags.push("LT");
  if (hasVS && !hasSub) tags.push("VS");
  m.lang = tags;

  m.audio = [...new Set(m.audio)];
  m.lang = [...new Set(m.lang)];
  m.hdr = [...new Set(m.hdr)];
  return m;
}
const MAX_STREAMS = Infinity; // không giới hạn số luồng — trả đủ mọi file; ép xuống bằng ?maxstreams=N

// Xếp hạng luồng theo thứ tự ưu tiên (cao → thấp):
//   CHẤT LƯỢNG: 4K > FHD > 720p > HD > CAM/TS
//   NGUỒN:      BluRay Remux > BluRay > Web-DL > WebRip > BDRip > HDRip > DVDRip
//   DUNG LƯỢNG: lớn hơn xếp trước (HDR/codec/audio chỉ là tiêu chí phụ).
function streamTierScore(label, extra = {}) {
  const s = String(label || "").replace(/_/g, " ").toUpperCase();
  const ctx = String([extra.variant, extra.releaseInfo].filter(Boolean).join(" ")).replace(/_/g, " ").toUpperCase();
  const allText = (s + " " + ctx).trim();

  // Ưu tiên trích xuất từ chính tên file trước (nếu tên file có 1080p thì chắc chắn là FHD).
  // Chỉ khi tên file không có độ phân giải mới xét đến variant (tên season) hoặc releaseInfo.
  const qMatch = s.match(/\b(2160P|2160|4K|UHD|1080P|1080|FULL.?HD|FHD|720P|720|MHD|HD|CAM|TS)\b/)
              || allText.match(/\b(2160P|2160|4K|UHD|1080P|1080|FULL.?HD|FHD|720P|720|MHD|HD|CAM|TS)\b/);
  let q = 2;
  if (qMatch) {
    const t = qMatch[1].replace(/\s+/g, "").toUpperCase();
    q = /2160P|2160|4K|UHD/.test(t) ? 5       // 4K
      : /1080P|1080|FULL.?HD|FHD/.test(t) ? 4 // FHD
      : /720P|720/.test(t) ? 3                // 720p
      : /CAM|TS/.test(t) ? 1 : 2;             // CAM hoặc HD
  }

  const src = /\b(BDREMUX|REMUX)\b/.test(s) ? 7
    : /\b(BLURAY|BLU-?RAY|BLU)\b/.test(s) ? 6
    : /\b(WEB-?DL|WEBDL)\b/.test(s) ? 5
    : /\b(WEBRIP)\b/.test(s) ? 4
    : /\b(BRRIP)\b/.test(s) ? 3
    : /\b(HDRIP)\b/.test(s) ? 2
    : /\b(DVDRIP)\b/.test(s) ? 1 : 0;

  let gb = 0;
  const sz = s.match(/\b([\d.]+)\s*(TB|GB|MB)\b/);
  if (sz) {
    const n = parseFloat(sz[1]) || 0;
    gb = sz[2] === "TB" ? n * 1024 : sz[2] === "MB" ? n / 1024 : n;
  }

  let tie = 0;
  if (/HDR10\+/.test(allText)) tie += 40;
  else if (/\bHDR\b|DOLBY.?VISION|\bDV\b/.test(allText)) tie += 30;
  if (/\b(HEVC|H\.?265|X\.?265)\b/.test(allText)) tie += 10;
  if (/\b10BIT\b/.test(allText)) tie += 5;
  if (/\b60FPS\b/.test(allText)) tie += 5;

  // Trọng số tách tầng: quality(1e9) > nguồn(1e7) > dung lượng(<1e7) > phụ(<1000).
  // 4K: q=5 -> tối thiểu 5.000.000.000 điểm.
  // FHD: q=4 -> tối đa 4.079.999.040 điểm (kể cả Remux 100GB).
  // Vì vậy mọi file 4K luôn luôn đứng trước FHD bất kể dung lượng và loại nguồn.
  return q * 1e9 + src * 1e7 + Math.min(Math.round(gb), 9999) * 1000 + tie;
}

function buildStreamTitle(src) {
  const m = parseStreamMeta(src.source, src.subtitle, {
    container: src.container,
    variant: src.variant,
    releaseInfo: src.releaseInfo,
    subtitleLangs: src.subtitleLangs,
  });
  const name = cleanText(src.title);
  const icon = m.quality === "4K" ? "💎" : m.quality === "FHD" ? "⭐" : "🎬";
  const ep = src.season != null && src.episode != null ? ` (Mùa ${src.season} · Tập ${src.episode})` : "";
  const l1 = icon + " " + (name || "Phim") + ep;
  const vid = [m.quality, m.source, m.video, ...m.hdr, m.container].filter((x) => x && x !== "-").join(" · ");
  const l2 = "💿 " + (vid || "HD").toUpperCase();
  const l3 = "🔊 " + (m.audio.length ? m.audio.join(" · ") : "N/A");
  const l4 = "🌐 " + (m.lang.length ? m.lang.join(" · ") : "N/A");
  const l5 = "💾 " + (m.size !== "-" ? m.size : "N/A");
  return [l1, l2, l3, l4, l5].join("\n");
}

async function handleCatalog(type, catalogId, extras, params, cfg, ms) {
  let skip = null, query = null;
  if (params) {
    if (params.search != null && String(params.search).trim() !== "") {
      query = safeDecode(String(params.search));
    }
    if (params.skip != null && String(params.skip).trim() !== "") {
      const n = parseInt(String(params.skip), 10);
      if (!isNaN(n)) skip = n;
    }
  }
  if (extras.length > 0) {
    for (const raw of extras.flatMap((x) => String(x).split("&"))) {
      const part = safeDecode(raw);
      if (!part) continue;
      const eq = part.indexOf("=");
      if (eq > 0) {
        const k = part.slice(0, eq).toLowerCase(), v = part.slice(eq + 1);
        if (k === "skip" && skip == null) { const n = parseInt(v, 10); if (!isNaN(n)) skip = n; }
        else if (k === "search" && query == null) query = v || null;
        continue;
      }
      if (/^\d+$/.test(part)) { if (skip == null) skip = parseInt(part, 10); continue; }
      if (["search", "skip", "0"].includes(part.toLowerCase())) continue;
      if (query == null) query = part;
    }
  }
  const page = Math.floor((skip || 0) / 20) + 1;
  let list;
  if (query) {
    list = await searchP4K(query, type, cfg, ms);
  } else {
    const path = catalogQuery(catalogId, page);
    if (!path) throw new HttpError(404, "unknown catalog: " + catalogId);
    const data = await getJson(path, cfg, ms);
    list = Array.isArray(data) ? data : [];
  }
  const forceType = (catalogId === "phim4k-otv-series" || catalogId === "phim4k-otv-series-latest") ? "series" : null;
  return {
    metas: list.filter((m) => m && (m.videos_id || m.id)).map((m) => {
      const t = forceType || (String(m.is_tvseries) === "1" ? "series" : "movie");
      return {
        id: "otv:" + (m.videos_id || m.id),
        type: t,
        name: m.title || "",
        poster: m.poster_url || "",
        background: m.thumbnail_url || "",
        year: parseInt(m.release, 10) || undefined,
        releaseInfo: m.video_quality || undefined,
        imdbRating: m.imdb_rating || undefined,
        description: m.description || undefined,
      };
    }),
  };
}

async function handleMeta(type, id, cfg, ms) {
  const metaType = type === "series" || type === "tvseries" ? "series" : "movie";
  const parsed = parseId(id);
  if (!parsed || !parsed.cleanId) throw new HttpError(400, "bad meta id");
  const realId = parsed.internal ? parsed.cleanId
    : await resolveExternalToVideosId(parsed.baseId, metaType, cfg, ms);
  if (!realId) throw new HttpError(404, "cannot resolve id to internal");
  const d = await getDetailCached(realId, metaType, cfg, ms);
  if (!d || !d.videos_id) throw new HttpError(404, "detail not found");
  return detailToMeta(d, metaType);
}

async function handleStream(type, id, cfg, ms, opts) {
  const metaType = type === "series" || type === "tvseries" ? "series" : "movie";
  const parsed = parseId(id);
  if (!parsed || !parsed.cleanId) throw new HttpError(400, "bad stream id");
  const realId = parsed.internal ? parsed.cleanId
    : await resolveExternalToVideosId(parsed.baseId, metaType, cfg, ms);
  if (!realId) throw new HttpError(404, "cannot resolve external id to internal");

  if (metaType === "movie") {
    const d = await getDetailCached(realId, "movie", cfg, ms);
    if (!d || !d.videos_id) throw new HttpError(404, "detail not found");
    const items = (Array.isArray(d.videos) ? d.videos : []).map((v) => {
      const srcObj = {
        title: d.title, source: v.label, subtitle: v.subtitle,
        container: v.file_type,
        subtitleLangs: Array.isArray(v.subtitle) ? v.subtitle.map((x) => x && (x.srclang || x.language)).filter(Boolean) : [],
        releaseInfo: d.video_quality,
      };
      return {
        fileUrl: v.file_url,
        label: buildStreamTitle(srcObj),
        subtitles: v.subtitle,
        w: streamTierScore(v.label, srcObj),
      };
    });
    // Đủ mọi luồng theo ý người dùng, nhưng chặn trần 40 file để không vượt
    // giới hạn subrequest của CF (mặc định 50/đợt gọi) — ghi đè bằng ?maxstreams=.
    const MAX_STREAMS_HARD = 40;
    const asked = parseInt(opts.maxstreams, 10);
    const max = Math.max(1, asked && asked < MAX_STREAMS_HARD ? asked : asked === 0 ? Infinity : Math.min(MAX_STREAMS, MAX_STREAMS_HARD));
    const picked = items.sort((a, b) => b.w - a.w).slice(0, max);
    return { streams: await resolveMediaSet(picked, cfg, opts) };
  }
  if (parsed.season === null || parsed.episode === null) {
    throw new HttpError(400, "series stream id needs season & episode");
  }
  const d = await getDetailCached(realId, "series", cfg, ms);
  const fileUrls = collectEpisodeSources(d, parsed.season, parsed.episode);
  if (fileUrls.length === 0) throw new HttpError(404, "episode has no stream source");
  fileUrls.sort((a, b) => b.w - a.w);
  return { streams: await resolveMediaSet(fileUrls, cfg, opts) };
}

/* ═══════════════════════ 10. MANIFEST ═══════════════════════ */

const PREFIXES = ["otv:", "tmdb:", "tmdb_", "tt", "videos_"];
function buildManifest() {
  return {
    id: "community.phim4k.otv",
    version: VERSION,
    name: ADDON_NAME,
    description: "Nguồn phim FHD + 4K • resolver có cache & retry",
    resources: [
      { name: "catalog", types: ["movie", "series"], idPrefixes: PREFIXES },
      { name: "meta", types: ["movie", "series"], idPrefixes: PREFIXES },
      { name: "stream", types: ["movie", "series"], idPrefixes: PREFIXES },
    ],
    types: ["movie", "series"],
    idPrefixes: PREFIXES,
    catalogs: [
      { id: "phim4k-otv-movies", name: "Nothing-P • P4K · Phim lẻ", type: "movie",
        extra: [{ name: "search", isRequired: false }, { name: "skip", isRequired: false }],
        extraSupported: ["search", "skip"] },
      { id: "phim4k-otv-series", name: "Nothing-P • P4K · Phim bộ", type: "series",
        extra: [{ name: "search", isRequired: false }, { name: "skip", isRequired: false }],
        extraSupported: ["search", "skip"] },
      { id: "phim4k-otv-series-latest", name: "Nothing-P • P4K · Phim bộ mới nhất", type: "series",
        extra: [{ name: "search", isRequired: false }, { name: "skip", isRequired: false }],
        extraSupported: ["search", "skip"] },
      { id: "phim4k-otv-top", name: "Nothing-P • P4K · Top xem nhiều", type: "series",
        extra: [{ name: "search", isRequired: false }, { name: "skip", isRequired: false }],
        extraSupported: ["search", "skip"] },
    ],
  };
}

/* ═══════════════════════ 11. ROUTER ═══════════════════════ */

/* ── Chẩn đoán phonenic: /_diag ─────────────────────────────────────────────
 * Một call biết ngay từ colo đang chạy: colo nào (cf-ray), resolver trực tiếp
 * còn 503 không, mirror nào trả lời được (probe), mirror nào mint vé thật.
 * Không lộ bí mật — chỉ hostname + trạng thái.
 */
async function diagProbe(mirror, target) {
  const t0 = Date.now();
  try {
    const res = await fetchWithTimeout(mirror.wrap(target),
      { headers: { Accept: "application/json", ...(mirror.headers || {}) } }, 6000);
    discardResponse(res); // chỉ cần status, không đọc body
    return { status: res.status, ms: Date.now() - t0 };
  } catch (e) { return { status: "ERR", ms: Date.now() - t0, err: String(e.message || e.name || e).slice(0, 80) }; }
}
async function diagMint(fileName, mirror, host, secret) {
  const t0 = Date.now();
  try {
    const signed = await genVideoUrl(fileName, host, secret);
    const res = await fetchWithTimeout(mirror.wrap(signed),
      { headers: { Accept: "application/json", ...(mirror.headers || {}) } }, 8000);
    if (!res.ok) { discardResponse(res); return { status: res.status, ms: Date.now() - t0, mint: false }; }
    const txt = await res.text().catch(() => "");
    const minted = extractMint(txt, host);
    const node = minted ? safeHostname(minted.url) : null;
    return { status: res.status, ms: Date.now() - t0, mint: !!minted, node };
  } catch (e) { return { status: "ERR", ms: Date.now() - t0, mint: false, err: String(e.message || e.name || e).slice(0, 80) }; }
}
async function diagAll(request) {
  const out = { colo: null, ip: null, config: null, direct: null, mirrors: [], mint: [] };
  const ray = request.headers.get("cf-ray") || "";
  out.colo = ray.slice(0, ray.indexOf("-")) || null;
  const ip = (request.headers.get("cf-connecting-ip") || "").split(".");
  out.ip = ip.length === 4 ? ip[0] + "." + ip[1] + ".*.*" : (ip[0] || "") + ":*";
  try {
    const t0 = Date.now();
    const cfg = await fetchDynamicConfig();
    out.config = {
      cdnBase: String((cfg && cfg.cdnBase) || "").replace(/^https?:\/\//, ""),
      cdnFallback: String((cfg && cfg.cdnFallback) || "").replace(/^https?:\/\//, ""),
      dynMs: Date.now() - t0,
    };
  } catch (e) { out.configError = String(e.message || e).slice(0, 60); }
  const probeTarget = "https://api.phim4k.space/rest-api/v130/movies?page=1";
  out.mirrors = await Promise.all(EGRESS_MIRRORS.map(async (m) => ({ name: m.name, ...await diagProbe(m, probeTarget) })));
  const fileName = "JE2TRZGY7TCW99A";
  const host = "https://svpc.cryboiz.workers.dev";
  const secret = HMAC_SECRETS[host.replace(/^https?:\/\//, "")];
  try {
    const signed = await genVideoUrl(fileName, host, secret);
    const t0 = Date.now();
    const res = await fetchWithTimeout(signed, { headers: { Accept: "application/json" } }, 6000);
    out.direct = { status: res.status, ms: Date.now() - t0, body: (await res.text().catch(() => "")).slice(0, 70) };
  } catch (e) { out.direct = { status: "ERR", err: String(e.message || e.name || e).slice(0, 60) }; }
  out.mint = await Promise.all(EGRESS_MIRRORS.map(async (m) => ({ name: m.name, ...await diagMint(fileName, m, host, secret) })));
  return out;
}

export const addon = {
  async fetch(request) {
    if (request.method === "OPTIONS") return json({ ok: true }, 200);
    const url = new URL(request.url);
    const segments = url.pathname.split("/").filter(Boolean);

    if (segments.length === 0 || segments[0] === "manifest.json") {
      return json(buildManifest());
    }
    if (segments[0] === "_diag") {
      return json(await diagAll(request));
    }
    try {
      const cfg = await fetchDynamicConfig();
      const ms = UPSTREAM_TIMEOUT_MS;
      const p = url.searchParams.get("prefer");
      const opts = {
        prefer: p !== "0",
        emint: url.searchParams.get("emint") === "1",
        maxstreams: String(url.searchParams.get("maxstreams") || ""),
      };

      if (segments[0] === "catalog" && segments.length >= 3) {
        const type = segments[1];
        const catalogId = segments[2].replace(/\.json$/, "");
        const extras = segments.slice(3).map((s) => s.replace(/\.json$/, ""));
        const params = {
          search: url.searchParams.get("search"),
          skip: url.searchParams.get("skip"),
        };
        return json(await handleCatalog(type, catalogId, extras, params, cfg, ms));
      }
      if (segments[0] === "meta" && segments.length >= 3) {
        const type = segments[1];
        const id = safeDecode(segments[2].replace(/\.json$/, ""));
        return json({ meta: await handleMeta(type, id, cfg, ms) });
      }
      if (segments[0] === "stream" && segments.length >= 3) {
        const type = segments[1];
        const id = safeDecode(segments[2].replace(/\.json$/, ""));
        return json(await handleStream(type, id, cfg, ms, opts));
      }
      return json({ error: "not found" }, 404);
    } catch (err) {
      if (err instanceof HttpError) return json({ error: err.message }, err.status);
      return json({ error: "internal error" }, 500);
    }
  },
};

