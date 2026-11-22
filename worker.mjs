// worker.mjs — entry Cloudflare Workers cho addon Phim4K (DB Turso + Streaming XOR Decryptor cho Hugging Face).
import { setDb, setEnv, addon } from "./core.mjs";
import { tursoDb } from "./turso-db.mjs";

let ready = false;

/* ═══════════════ BỘ GIẢI MÃ LUỒNG HUGGING FACE (STREAMING XOR 256-BYTE SIÊU TỐC) ═══════════════ */

const HEADER_SIZE = 64; // 64 bytes (0x40) header giả ở đầu mọi file mã hóa

// Khóa XOR 256-byte mặc định (đã căn chỉnh theo mốc offset 0x40 của payload MKV)
const DEFAULT_KEY = new Uint8Array([
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
  0xad, 0x86, 0xe1, 0x40, 0x46, 0xdd, 0x25, 0xe9, 0x47, 0xe9, 0xca, 0x3c, 0x00, 0xe3, 0x16, 0xdb
]);

/**
 * Tạo sẵn bảng 256 góc xoay của khóa (dạng Uint8Array và Uint32Array) để XOR 32-bit siêu tốc
 */
function buildKeyTables(baseKey) {
  const u8 = new Array(256);
  const u32 = new Array(256);
  for (let rot = 0; rot < 256; rot++) {
    const buf = new ArrayBuffer(256);
    const k8 = new Uint8Array(buf);
    for (let i = 0; i < 256; i++) {
      k8[i] = baseKey[(rot + i) & 255];
    }
    u8[rot] = k8;
    u32[rot] = new Uint32Array(buf);
  }
  return { u8, u32 };
}

const DEFAULT_TABLES = buildKeyTables(DEFAULT_KEY);

// Cache lưu CDN URL trực tiếp & metadata theo từng file HF (TTL 15 phút)
const metaCache = new Map();
const inFlightMeta = new Map();
const streamDiag = [];
const ISOLATE_ID = Math.random().toString(36).slice(2, 8);
const BOOT_TS = Date.now();

function recordStreamDiag(entry, diagId = "", ctx = null) {
  const rec = { t: new Date().toISOString(), isolate: ISOLATE_ID, upMs: Date.now() - BOOT_TS, ...entry };
  streamDiag.push(rec);
  if (streamDiag.length > 40) streamDiag.shift();
  if (diagId && typeof caches !== "undefined" && caches.default) {
    const p = caches.default.put(
      new Request("https://diag-log.internal/" + encodeURIComponent(diagId)),
      new Response(JSON.stringify(rec), { headers: { "Cache-Control": "max-age=300" } })
    ).catch(() => {});
    if (ctx && ctx.waitUntil) ctx.waitUntil(p);
  }
}

function xor32Blocks(src32, dst32, k32, n32) {
  let i = 0;
  for (; i + 16 <= n32; i += 16) {
    const o = i & 63;
    dst32[i] = src32[i] ^ k32[o];
    dst32[i + 1] = src32[i + 1] ^ k32[o + 1];
    dst32[i + 2] = src32[i + 2] ^ k32[o + 2];
    dst32[i + 3] = src32[i + 3] ^ k32[o + 3];
    dst32[i + 4] = src32[i + 4] ^ k32[o + 4];
    dst32[i + 5] = src32[i + 5] ^ k32[o + 5];
    dst32[i + 6] = src32[i + 6] ^ k32[o + 6];
    dst32[i + 7] = src32[i + 7] ^ k32[o + 7];
    dst32[i + 8] = src32[i + 8] ^ k32[o + 8];
    dst32[i + 9] = src32[i + 9] ^ k32[o + 9];
    dst32[i + 10] = src32[i + 10] ^ k32[o + 10];
    dst32[i + 11] = src32[i + 11] ^ k32[o + 11];
    dst32[i + 12] = src32[i + 12] ^ k32[o + 12];
    dst32[i + 13] = src32[i + 13] ^ k32[o + 13];
    dst32[i + 14] = src32[i + 14] ^ k32[o + 14];
    dst32[i + 15] = src32[i + 15] ^ k32[o + 15];
  }
  for (; i < n32; i++) {
    dst32[i] = src32[i] ^ k32[i & 63];
  }
}

/**
 * Giải mã XOR 32-bit không làm biến đổi buffer đầu vào của runtime (tránh xung đột buffer trong workerd)
 */
function xorChunkFast(chunk, rotOffset, tables) {
  const len = chunk.byteLength;
  if (len === 0) return chunk;

  const k8 = tables.u8[rotOffset];
  const k32 = tables.u32[rotOffset];
  const out = new Uint8Array(len);
  const n32 = len >>> 2;

  if (n32 > 0) {
    const out32 = new Uint32Array(out.buffer, 0, n32);
    if ((chunk.byteOffset & 3) === 0) {
      xor32Blocks(new Uint32Array(chunk.buffer, chunk.byteOffset, n32), out32, k32, n32);
    } else {
      out.set(chunk);
      xor32Blocks(out32, out32, k32, n32);
    }
  }

  for (let b = n32 << 2; b < len; b++) {
    out[b] = chunk[b] ^ k8[b & 255];
  }

  return out;
}

/**
 * Lấy link CDN trực tiếp (us.aws.cdn.hf.co) và tổng dung lượng file (X-Linked-Size)
 * Phân biệt file Raw vs Disguised và lưu Cache 15 phút để tua không bị 302 Redirect delay
 */
async function getFileMeta(hfUrl, autoKey = false, ctx = null) {
  const now = Date.now();
  const cached = metaCache.get(hfUrl);
  if (cached && now - cached.time < 15 * 60 * 1000) {
    return cached;
  }

  // Khử trùng lặp khi nhiều chunk song song (ví dụ 4 kết nối đồng thời từ Nuvio) cùng gọi lúc cold start
  if (inFlightMeta.has(hfUrl)) {
    return inFlightMeta.get(hfUrl);
  }

  const resolvePromise = (async () => {
    // L2 Edge Cache: caches.default (chia sẻ giữa mọi isolate trên cùng PoP/colo)
    const cacheKey = new Request("https://meta-cache.internal/" + encodeURIComponent(hfUrl));
    if (typeof caches !== "undefined" && caches.default) {
      try {
        const edgeHit = await caches.default.match(cacheKey);
        if (edgeHit) {
          const edgeMeta = await edgeHit.json();
          if (edgeMeta && edgeMeta.cdnUrl && typeof edgeMeta.time === "number" && now - edgeMeta.time < 25 * 60 * 1000) {
            if (!edgeMeta.isRaw && Array.isArray(edgeMeta.keyBytes) && edgeMeta.keyBytes.length === 256) {
              edgeMeta.tables = buildKeyTables(Uint8Array.from(edgeMeta.keyBytes));
            }
            metaCache.set(hfUrl, edgeMeta);
            return edgeMeta;
          }
        }
      } catch {}
    }

    let cdnUrl = hfUrl;
    let totalH5Size = 0;

    // Bước 1: Bắt 302 Redirect từ Hugging Face để lấy Location CDN và X-Linked-Size
    const probeResp = await fetch(hfUrl, {
      method: "GET",
      headers: { Range: `bytes=${HEADER_SIZE}-${HEADER_SIZE + 15}` },
      redirect: "manual"
    });

    if (probeResp.status >= 300 && probeResp.status < 400) {
      const loc = probeResp.headers.get("Location");
      if (loc) cdnUrl = new URL(loc, hfUrl).toString();
      const linkedSize = probeResp.headers.get("X-Linked-Size");
      if (linkedSize) totalH5Size = parseInt(linkedSize, 10);
    } else if (probeResp.status === 206) {
      const cr = probeResp.headers.get("Content-Range");
      if (cr && cr.includes("/")) {
        totalH5Size = parseInt(cr.split("/")[1], 10);
      }
    }
    try { await probeResp.body?.cancel(); } catch {}

    // Bước 2: Lấy mẫu 128 byte đầu từ CDN để phân loại chính xác Raw (không mã hóa) vs Disguised (mã hóa)
    const sampleResp = await fetch(cdnUrl, { headers: { Range: "bytes=0-127" } });
    if (!totalH5Size) {
      const cr = sampleResp.headers.get("Content-Range");
      if (cr && cr.includes("/")) {
        totalH5Size = parseInt(cr.split("/")[1], 10);
      }
    }
    const sBuf = new Uint8Array(await sampleResp.arrayBuffer());

    // 1. Kiểm tra video gốc chưa mã hóa (offset 0)
    const isRawMkv = sBuf.length >= 4 && sBuf[0] === 0x1a && sBuf[1] === 0x45 && sBuf[2] === 0xdf && sBuf[3] === 0xa3;
    const isRawMp4 = sBuf.length >= 8 && (
      (sBuf[4] === 0x66 && sBuf[5] === 0x74 && sBuf[6] === 0x79 && sBuf[7] === 0x70) || // ftyp
      (sBuf[4] === 0x6d && sBuf[5] === 0x6f && sBuf[6] === 0x6f && sBuf[7] === 0x76)    // moov
    );
    const isRawAvi = sBuf.length >= 4 && sBuf[0] === 0x52 && sBuf[1] === 0x49 && sBuf[2] === 0x46 && sBuf[3] === 0x46; // RIFF
    const isRawTs = sBuf.length >= 1 && sBuf[0] === 0x47; // MPEG-TS sync byte

    // 2. Kiểm tra video ngụy trang (offset 64 có XOR key)
    const isDisguisedMkv = sBuf.length >= 68 &&
      (sBuf[64] ^ DEFAULT_KEY[0]) === 0x1a &&
      (sBuf[65] ^ DEFAULT_KEY[1]) === 0x45 &&
      (sBuf[66] ^ DEFAULT_KEY[2]) === 0xdf &&
      (sBuf[67] ^ DEFAULT_KEY[3]) === 0xa3;

    const isDisguisedMp4 = sBuf.length >= 72 &&
      (sBuf[68] ^ DEFAULT_KEY[4]) === 0x66 &&
      (sBuf[69] ^ DEFAULT_KEY[5]) === 0x74 &&
      (sBuf[70] ^ DEFAULT_KEY[6]) === 0x79 &&
      (sBuf[71] ^ DEFAULT_KEY[7]) === 0x70;

    const cleanPath = hfUrl.split("?")[0].toLowerCase();
    const isVideoExt = /\.(mkv|mp4|avi|ts|webm|mov|m4v|flv)$/i.test(cleanPath);

    // File không mã hóa: offset 0 là video chuẩn HOẶC có đuôi video nhưng KHÔNG bị mã hóa ở offset 64
    if (isRawMkv || isRawMp4 || isRawAvi || isRawTs || (!isDisguisedMkv && !isDisguisedMp4 && isVideoExt)) {
      const rawMeta = {
        isRaw: true,
        headerSize: 0,
        cdnUrl,
        totalSize: totalH5Size,
        mediaSize: totalH5Size,
        tables: null,
        contentType: (isRawMp4 || cleanPath.endsWith(".mp4")) ? "video/mp4" :
                     (isRawAvi || cleanPath.endsWith(".avi")) ? "video/x-msvideo" :
                     (isRawTs || cleanPath.endsWith(".ts")) ? "video/mp2t" : "video/x-matroska",
        time: now
      };
      metaCache.set(hfUrl, rawMeta);
      if (typeof caches !== "undefined" && caches.default) {
        try {
          const saveResp = new Response(JSON.stringify(rawMeta), {
            headers: { "Cache-Control": "max-age=2400" }
          });
          if (ctx && ctx.waitUntil) ctx.waitUntil(caches.default.put(cacheKey, saveResp));
          else caches.default.put(cacheKey, saveResp).catch(() => {});
        } catch {}
      }
      return rawMeta;
    }

    // 2. File ngụy trang (có header 64 byte + XOR 256-byte)
    let tables = DEFAULT_TABLES;
    let customKeyBytes = null;
    if (autoKey) {
      const keySampleResp = await fetch(cdnUrl, {
        headers: { Range: `bytes=${HEADER_SIZE}-${HEADER_SIZE + 65535}` }
      });
      const kBuf = new Uint8Array(await keySampleResp.arrayBuffer());
      const isDefaultValid =
        kBuf.length >= 4 &&
        (kBuf[0] ^ DEFAULT_KEY[0]) === 0x1a &&
        (kBuf[1] ^ DEFAULT_KEY[1]) === 0x45 &&
        (kBuf[2] ^ DEFAULT_KEY[2]) === 0xdf &&
        (kBuf[3] ^ DEFAULT_KEY[3]) === 0xa3;

      if (!isDefaultValid && kBuf.length >= 256) {
        const recovered = new Uint8Array(256);
        const counts = new Int32Array(256);
        for (let col = 0; col < 256; col++) {
          counts.fill(0);
          let maxC = -1;
          let bestB = 0;
          for (let i = col; i < kBuf.length; i += 256) {
            const b = kBuf[i];
            const c = ++counts[b];
            if (c > maxC) {
              maxC = c;
              bestB = b;
            }
          }
          recovered[col] = bestB;
        }
        tables = buildKeyTables(recovered);
        customKeyBytes = Array.from(recovered);
      }
    }

    const meta = {
      isRaw: false,
      headerSize: HEADER_SIZE,
      cdnUrl,
      totalSize: totalH5Size,
      mediaSize: Math.max(0, totalH5Size - HEADER_SIZE),
      tables,
      keyBytes: customKeyBytes,
      contentType: isDisguisedMp4 ? "video/mp4" : "video/x-matroska",
      time: now
    };
    metaCache.set(hfUrl, meta);
    if (typeof caches !== "undefined" && caches.default) {
      try {
        const saveResp = new Response(JSON.stringify({ ...meta, tables: null }), {
          headers: { "Cache-Control": "max-age=1500" }
        });
        if (ctx && ctx.waitUntil) ctx.waitUntil(caches.default.put(cacheKey, saveResp));
        else caches.default.put(cacheKey, saveResp).catch(() => {});
      } catch {}
    }
    return meta;
  })();

  inFlightMeta.set(hfUrl, resolvePromise);
  try {
    return await resolvePromise;
  } finally {
    inFlightMeta.delete(hfUrl);
  }
}

/**
 * Chuyển đổi URL hoặc đường dẫn thành direct resolve link của Hugging Face
 */
function resolveHfUrl(urlObj) {
  const paramUrl = urlObj.searchParams.get("url");
  if (paramUrl) {
    return paramUrl.replace("/blob/", "/resolve/");
  }

  let path = urlObj.pathname.replace(/^\/+/, "");
  if (path.startsWith("hf/")) {
    path = path.slice(3).replace(/^\/+/, "");
  }

  const parts = path.split("/").filter(Boolean);
  const RESERVED = new Set(["manifest.json", "catalog", "meta", "stream", "_diag", "api-proxy"]);
  if (parts.length === 0 || RESERVED.has(parts[0])) {
    return null;
  }

  // Bỏ đuôi ảo .mkv nếu có gắn thêm (hỗ trợ cả link mới không đuôi và link cũ có đuôi .mkv)
  path = path.replace(/(\.[a-z0-9]+)\.mkv$/i, "$1").replace(/\.mkv$/i, "");

  if (path.startsWith("datasets/") || path.startsWith("buckets/")) {
    return `https://huggingface.co/${path.replace("/blob/", "/resolve/")}`;
  }

  if (parts.length >= 3) {
    const cleanParts = path.split("/").filter(Boolean);
    const [user, repo, ...fileParts] = cleanParts;
    if (fileParts[0] === "resolve" && fileParts[1] === "main") {
      return `https://huggingface.co/datasets/${user}/${repo}/${fileParts.join("/")}`;
    }
    return `https://huggingface.co/datasets/${user}/${repo}/resolve/main/${fileParts.join("/")}`;
  }

  return null;
}

async function handleHfStream(request, url, hfUrl, ctx) {
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges"
      }
    });
  }

  const autoKey = url.searchParams.get("autokey") === "1";
  let meta = await getFileMeta(hfUrl, autoKey, ctx);
  const mediaSize = meta.mediaSize;

  // 1. HEAD request
  if (request.method === "HEAD") {
    return new Response(null, {
      status: 200,
      headers: {
        "Content-Type": meta.contentType || "video/x-matroska",
        "Content-Length": String(mediaSize),
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store, no-cache, must-revalidate",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type, Accept",
        "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges"
      }
    });
  }

  // 2. Hỗ trợ chuẩn HTTP Range (START-END, START- liên tục không cắt 16MB, -SUFFIX) và GET không Range -> 200 OK
  const rangeHeader = request.headers.get("Range");
  let start = 0;
  let end = mediaSize - 1;
  let isPartial = false;

  if (rangeHeader && rangeHeader.startsWith("bytes=")) {
    isPartial = true;
    const rangeSpec = rangeHeader.replace("bytes=", "").split(",")[0].trim();
    const dashIdx = rangeSpec.indexOf("-");

    if (dashIdx === 0) {
      const suffixLen = parseInt(rangeSpec.slice(1), 10);
      if (!Number.isFinite(suffixLen) || suffixLen <= 0) {
        start = mediaSize;
      } else {
        start = Math.max(0, mediaSize - suffixLen);
        end = mediaSize - 1;
      }
    } else if (dashIdx > 0) {
      const sStr = rangeSpec.slice(0, dashIdx).trim();
      const eStr = rangeSpec.slice(dashIdx + 1).trim();
      const parsedStart = sStr.length > 0 ? parseInt(sStr, 10) : 0;
      start = Number.isFinite(parsedStart) && parsedStart >= 0 ? parsedStart : mediaSize;
      if (eStr.length > 0) {
        const parsedEnd = parseInt(eStr, 10);
        end = Number.isFinite(parsedEnd) ? Math.min(mediaSize - 1, parsedEnd) : mediaSize - 1;
      } else {
        end = mediaSize - 1;
      }
    } else {
      start = mediaSize;
    }
  }

  if (mediaSize <= 0 || start < 0 || start >= mediaSize || start > end) {
    return new Response(null, {
      status: 416,
      headers: {
        "Content-Range": `bytes */${mediaSize}`,
        "Access-Control-Allow-Origin": "*"
      }
    });
  }

  // 3. Gọi trực tiếp CDN qua link và vị trí thực tế
  const hfStart = start + meta.headerSize;
  const hfEnd = end + meta.headerSize;

  let upstreamResp = await fetch(meta.cdnUrl, {
    headers: { Range: `bytes=${hfStart}-${hfEnd}` }
  });

  if (upstreamResp.status === 401 || upstreamResp.status === 403) {
    try { await upstreamResp.body?.cancel(); } catch {}
    metaCache.delete(hfUrl);
    if (typeof caches !== "undefined" && caches.default) {
      const cacheKey = new Request("https://meta-cache.internal/" + encodeURIComponent(hfUrl));
      try { await caches.default.delete(cacheKey); } catch {}
    }
    meta = await getFileMeta(hfUrl, autoKey, ctx);
    upstreamResp = await fetch(meta.cdnUrl, {
      headers: { Range: `bytes=${hfStart}-${hfEnd}` }
    });
  }

  if (!upstreamResp.ok && upstreamResp.status !== 206) {
    try { await upstreamResp.body?.cancel(); } catch {}
    return new Response(`Upstream CDN error: ${upstreamResp.status}`, {
      status: upstreamResp.status
    });
  }

  const contentLength = end - start + 1;
  const respHeaders = new Headers({
    "Content-Type": meta.contentType || "video/x-matroska",
    "Accept-Ranges": "bytes",
    "Content-Length": String(contentLength),
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
    "Access-Control-Allow-Headers": "Range, Content-Type, Accept",
    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    "Content-Disposition": "inline"
  });

  if (isPartial) {
    respHeaders.set("Content-Range", `bytes ${start}-${end}/${mediaSize}`);
  }

  const diagMode = url.searchParams.get("__diag") || "";
  const diagId = url.searchParams.get("__diag_id") || "";
  const diagFile = hfUrl.split("/").pop() || "";
  const diagRange = `${start}-${end}/${mediaSize}`;
  const tStart = Date.now();
  let bodyStream = upstreamResp.body;
  if (diagMode || diagId) {
    respHeaders.set("X-Isolate", ISOLATE_ID);
    respHeaders.set("X-Uptime", String(Date.now() - BOOT_TS));
    recordStreamDiag({ stage: "start", mode: diagMode || "fls", file: diagFile, range: diagRange, expected: contentLength }, diagId ? diagId + ":start" : "", ctx);
  }

  if (diagMode === "rawbody") {
    return new Response(bodyStream, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  if (diagMode === "pipefls") {
    if (typeof FixedLengthStream !== "undefined") {
      try {
        const fls = new FixedLengthStream(contentLength);
        bodyStream = bodyStream.pipeThrough(fls);
      } catch {}
    }
    return new Response(bodyStream, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  // File raw nguyên bản -> truyền trực tiếp body (kèm FixedLengthStream nếu có để đảm bảo Content-Length)
  if (meta.isRaw) {
    if (typeof FixedLengthStream !== "undefined") {
      try {
        const fls = new FixedLengthStream(contentLength);
        bodyStream = bodyStream.pipeThrough(fls);
      } catch {}
    }
    return new Response(bodyStream, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  // 4. File mã hóa: Giải mã XOR
  let rotOffset = ((start % 256) + 256) % 256;
  const tables = meta.tables || DEFAULT_TABLES;

  if (diagMode === "ts_only" || diagMode === "ts_xor" || diagMode === "ts_xor_fls") {
    let remaining = contentLength;
    let readBytes = 0;
    let chunks = 0;
    const doXor = diagMode !== "ts_only";
    const ts = new TransformStream({
      transform(chunk, controller) {
        if (remaining <= 0 || !chunk || chunk.byteLength === 0) return;
        const slice = chunk.byteLength > remaining ? chunk.subarray(0, remaining) : chunk;
        const out = doXor ? xorChunkFast(slice, rotOffset, tables) : slice;
        rotOffset = (rotOffset + slice.byteLength) & 255;
        remaining -= slice.byteLength;
        readBytes += slice.byteLength;
        chunks++;
        controller.enqueue(out);
      },
      flush() {
        recordStreamDiag({ stage: "end", mode: diagMode, file: diagFile, range: diagRange, status: remaining === 0 ? "ok" : "trunc", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart }, diagId, ctx);
      }
    });
    let outStream = bodyStream.pipeThrough(ts);
    if (diagMode === "ts_xor_fls" && typeof FixedLengthStream !== "undefined") {
      outStream = outStream.pipeThrough(new FixedLengthStream(contentLength));
    }
    return new Response(outStream, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  if (diagMode === "stitch8" || diagMode === "stitch8_fls") {
    try { await bodyStream?.cancel(); } catch {}
    const CHUNK_UP = 8 * 1024 * 1024;
    let cursor = start;
    let curReader = null;
    let readBytes = 0;
    let chunks = 0;
    let subReqs = 0;
    const stitchStream = new ReadableStream({
      async pull(controller) {
        try {
          while (cursor <= end) {
            if (!curReader) {
              const partEnd = Math.min(end, cursor + CHUNK_UP - 1);
              subReqs++;
              const sr = await fetch(meta.cdnUrl, {
                headers: { Range: `bytes=${cursor + meta.headerSize}-${partEnd + meta.headerSize}` }
              });
              if (!sr.ok && sr.status !== 206) {
                throw new Error(`stitch upstream ${sr.status}`);
              }
              curReader = sr.body.getReader();
            }
            const { done, value } = await curReader.read();
            if (done) {
              curReader = null;
              continue;
            }
            if (!value || value.byteLength === 0) continue;
            const left = end - cursor + 1;
            const slice = value.byteLength > left ? value.subarray(0, left) : value;
            const dec = xorChunkFast(slice, rotOffset, tables);
            rotOffset = (rotOffset + slice.byteLength) & 255;
            cursor += slice.byteLength;
            readBytes += slice.byteLength;
            chunks++;
            controller.enqueue(dec);
            if (cursor > end) {
              recordStreamDiag({ stage: "end", mode: diagMode, status: "ok", readBytes, expected: contentLength, chunks, subReqs, ms: Date.now() - tStart }, diagId, ctx);
              await curReader.cancel().catch(() => {});
              curReader = null;
              controller.close();
            }
            return;
          }
          controller.close();
        } catch (err) {
          recordStreamDiag({ stage: "end", mode: diagMode, status: "err", readBytes, expected: contentLength, chunks, subReqs, ms: Date.now() - tStart, msg: err?.message || String(err) }, diagId, ctx);
          if (curReader) await curReader.cancel(err).catch(() => {});
          controller.error(err);
        }
      },
      cancel(reason) {
        recordStreamDiag({ stage: "end", mode: diagMode, status: "cancel", readBytes, expected: contentLength, chunks, subReqs, ms: Date.now() - tStart, msg: String(reason || "") }, diagId, ctx);
        return curReader ? curReader.cancel(reason).catch(() => {}) : Promise.resolve();
      }
    });
    let finalOut = stitchStream;
    if (diagMode === "stitch8_fls" && typeof FixedLengthStream !== "undefined") {
      finalOut = finalOut.pipeThrough(new FixedLengthStream(contentLength));
    }
    return new Response(finalOut, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  const reader = bodyStream.getReader();

  if (diagMode === "pull") {
    let remaining = contentLength;
    let readBytes = 0;
    let chunks = 0;
    const pullStream = new ReadableStream({
      async pull(controller) {
        try {
          while (remaining > 0) {
            const { done, value } = await reader.read();
            if (done) {
              recordStreamDiag({ mode: "pull", file: diagFile, range: diagRange, status: "trunc", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart }, diagId, ctx);
              controller.error(new Error(`upstream truncated (${remaining}B short)`));
              return;
            }
            if (!value || value.byteLength === 0) continue;
            const slice = value.byteLength > remaining ? value.subarray(0, remaining) : value;
            const dec = xorChunkFast(slice, rotOffset, tables);
            rotOffset = (rotOffset + slice.byteLength) & 255;
            remaining -= slice.byteLength;
            readBytes += slice.byteLength;
            chunks++;
            controller.enqueue(dec);
            if (remaining === 0) {
              recordStreamDiag({ mode: "pull", file: diagFile, range: diagRange, status: "ok", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart }, diagId, ctx);
              reader.cancel().catch(() => {});
              controller.close();
            }
            return;
          }
          controller.close();
        } catch (err) {
          recordStreamDiag({ mode: "pull", file: diagFile, range: diagRange, status: "err", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart, msg: err?.message || String(err) }, diagId, ctx);
          await reader.cancel(err).catch(() => {});
          controller.error(err);
        }
      },
      cancel(reason) {
        if (remaining > 0) {
          recordStreamDiag({ mode: "pull", file: diagFile, range: diagRange, status: "cancel", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart, msg: String(reason || "") }, diagId, ctx);
        }
        return reader.cancel(reason).catch(() => {});
      }
    });
    return new Response(pullStream, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  let fls = null;
  if (typeof FixedLengthStream !== "undefined") {
    try {
      fls = new FixedLengthStream(contentLength);
    } catch {}
  }

  if (fls) {
    const writer = fls.writable.getWriter();
    let remaining = contentLength;
    let readBytes = 0;
    let chunks = 0;
    const modeName = diagMode || "fls";
    const onAbort = () => {
      recordStreamDiag({ mode: modeName, file: diagFile, range: diagRange, status: "client_abort", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart });
      reader.cancel("client abort").catch(() => {});
      writer.abort("client abort").catch(() => {});
    };
    if (request.signal) {
      request.signal.addEventListener("abort", onAbort, { once: true });
    }

    const pump = (async () => {
      try {
        while (remaining > 0) {
          const { done, value } = await reader.read();
          if (done) break;
          if (!value || value.byteLength === 0) continue;
          const slice = value.byteLength > remaining ? value.subarray(0, remaining) : value;
          const out = diagMode === "pass" ? slice : xorChunkFast(slice, rotOffset, tables);
          rotOffset = (rotOffset + slice.byteLength) & 255;
          remaining -= slice.byteLength;
          readBytes += slice.byteLength;
          chunks++;
          await writer.ready;
          await writer.write(out);
        }
        if (remaining === 0) {
          recordStreamDiag({ mode: modeName, file: diagFile, range: diagRange, status: "ok", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart });
          await writer.close();
        } else {
          recordStreamDiag({ mode: modeName, file: diagFile, range: diagRange, status: "trunc", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart, msg: `short ${remaining}B` });
          await writer.abort(new Error(`upstream truncated (${remaining}B short)`));
        }
      } catch (err) {
        recordStreamDiag({ mode: modeName, file: diagFile, range: diagRange, status: "err", readBytes, expected: contentLength, chunks, ms: Date.now() - tStart, msg: err?.message || String(err) });
        await reader.cancel(err).catch(() => {});
        await writer.abort(err).catch(() => {});
      } finally {
        if (request.signal) {
          try { request.signal.removeEventListener("abort", onAbort); } catch {}
        }
      }
    })();

    if (ctx && ctx.waitUntil) ctx.waitUntil(pump);
    else pump.catch(() => {});

    return new Response(fls.readable, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  const fallbackStream = new ReadableStream({
    async pull(controller) {
      try {
        const { done, value } = await reader.read();
        if (done) {
          controller.close();
          return;
        }
        const dec = xorChunkFast(value, rotOffset, tables);
        rotOffset = (rotOffset + value.byteLength) & 255;
        controller.enqueue(dec);
      } catch (err) {
        controller.error(err);
      }
    },
    cancel(reason) {
      return reader.cancel(reason).catch(() => {});
    }
  });

  return new Response(fallbackStream, {
    status: isPartial ? 206 : 200,
    headers: respHeaders
  });
}

/* ═══════════════ ENTRY CLOUDFLARE WORKER ═══════════════ */

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // 1. Proxy API nội bộ
    if (url.pathname === "/api-proxy" || url.pathname.startsWith("/api-proxy/")) {
      let targetPath = url.pathname.replace(/^\/api-proxy\/?/, "");
      let query = url.search;
      if (url.searchParams.has("path")) {
        const p = url.searchParams.get("path");
        const qIndex = p.indexOf("?");
        if (qIndex !== -1) {
          targetPath = p.slice(0, qIndex);
          query = p.slice(qIndex);
        } else {
          targetPath = p;
          query = "";
        }
      }
      if (!targetPath) return new Response("Missing path", { status: 400 });
      const targetUrl = "https://api.phim4k.space/rest-api/v130/" + targetPath.replace(/^\/+/, "") + query;
      try {
        const upstream = await fetch(targetUrl, {
          headers: {
            "Accept": "application/json",
            "API-KEY": "bbbb411dea44849",
            "Authorization": "Basic " + btoa("admin:"),
            "User-Agent": "okhttp/4.9.0",
          },
        });
        const text = await upstream.text();
        return new Response(text, {
          status: upstream.status,
          headers: {
            "Content-Type": upstream.headers.get("content-type") || "application/json",
            "Access-Control-Allow-Origin": "*",
          },
        });
      } catch (e) {
        return new Response(JSON.stringify({ error: e.message }), { status: 502, headers: { "Content-Type": "application/json" } });
      }
    }

    if (url.pathname === "/__stream_diag") {
      const id = url.searchParams.get("id");
      let startRec = null;
      let endRec = null;
      if (id && typeof caches !== "undefined" && caches.default) {
        try {
          const r1 = await caches.default.match(new Request("https://diag-log.internal/" + encodeURIComponent(id + ":start")));
          if (r1) startRec = await r1.json();
          const r2 = await caches.default.match(new Request("https://diag-log.internal/" + encodeURIComponent(id)));
          if (r2) endRec = await r2.json();
        } catch {}
      }
      return new Response(JSON.stringify({ isolate: ISOLATE_ID, upMs: Date.now() - BOOT_TS, startRec, endRec, count: streamDiag.length, logs: streamDiag }, null, 2), {
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" }
      });
    }

    // 2. Giải mã & Stream trực tiếp file video từ Hugging Face (/hf/... hoặc /<user>/<repo>/<file> hoặc /?url=...)
    const hfUrl = resolveHfUrl(url);
    if (hfUrl) {
      return handleHfStream(request, url, hfUrl, ctx);
    }

    // 3. Stremio Addon Router
    if (!ready) {
      setEnv(env);
      setDb(tursoDb(env));
      ready = true;
    }
    return addon.fetch(request);
  },

  // Cron Trigger: tự dispatch workflow crawl, upgrade-quality, health-check (GitHub schedule hay bị bỏ nhịp).
  async scheduled(event, env, ctx) {
    const tok = env.GH_TOKEN;
    if (!tok) return;

    const ghHeaders = {
      Authorization: "Bearer " + tok,
      Accept: "application/vnd.github+json",
      "User-Agent": "p4k-cron",
      "Content-Type": "application/json",
    };

    async function dispatch(file, inputs = {}) {
      try {
        const res = await fetch(`https://api.github.com/repos/anpham14121993-cloud/P4k-addon/actions/workflows/${file}/dispatches`, {
          method: "POST",
          headers: ghHeaders,
          body: JSON.stringify({ ref: "main", inputs }),
        });
        if (!res.ok) console.log(`dispatch ${file} failed:`, res.status, await res.text());
      } catch (e) {
        console.error(`dispatch ${file} error:`, e.message);
      }
    }

    const now = new Date(event && event.scheduledTime ? event.scheduledTime : Date.now());
    const min = now.getUTCMinutes();
    const hour = now.getUTCHours();
    const cron = String((event && event.cron) || "");
    const isMatchSlot = cron === "10,40 * * * *" || (!cron && ((min >= 5 && min < 25) || min >= 35));

    // 2. Tuyến 2 (match-hf): chạy lúc :10 và :40 (sau sync-realtime 10 phút) để đối chiếu ngay phim/tập vừa nạp.
    // Lượt thường (hot_only="true"): 800 mã / 1500s / 4 luồng (mã mới + retry nóng 2h/6h + bắt no_hf).
    // Mỗi 6 giờ (:10 giờ % 6 === 0 UTC) tắt hot_only: tăng 1500 mã / 1600s để quét vét đều miss lạnh 24h / 7 ngày.
    if (isMatchSlot) {
      const hotOnly = (min < 25 && hour % 6 === 0) ? "false" : "true";
      const limit = hotOnly === "false" ? "1500" : "800";
      const budget = hotOnly === "false" ? "1600" : "1500";
      await dispatch("match-hf.yml", { limit, budget, concurrency: "4", hot_only: hotOnly, new_only: "false", dry_run: "false" });
      return;
    }

    // 0. Tuyến 0 hàng ngày (19:30 UTC = 02:30 VN): đối soát toàn bộ danh mục 100% API <-> DB (sau đó :40 match-hf sẽ tự đối chiếu HF).
    if (hour === 19 && min >= 20) {
      await dispatch("reconcile-api.yml", { delay: "180", batch_size: "50", dry_run: "false" });
      return;
    }

    // Tuyến 0 (sync-realtime): chạy mỗi 30 phút một lần vào :00 và :30 (quét 5 trang cập nhật + trang 1 ID mới)
    await dispatch("sync-realtime.yml", { pages: "5" });

    // 1. Tuyến 1 (crawl): chạy mỗi giờ một lần (vào đầu giờ :00)
    if (min < 15) {
      await dispatch("crawl.yml", { budget: "3200", feed_pages: "25" });
    }
  },
};
