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

/**
 * Giải mã XOR 32-bit trực tiếp trên chunk (In-place, 0 allocation, unrolled 16 từ 32-bit)
 */
function xorChunkFast(chunk, rotOffset, tables) {
  const len = chunk.byteLength;
  if (len === 0) return chunk;

  const k8 = tables.u8[rotOffset];
  const k32 = tables.u32[rotOffset];

  let target = chunk;
  if ((chunk.byteOffset & 3) !== 0) {
    target = new Uint8Array(len);
    target.set(chunk);
  }

  const n32 = len >>> 2;
  if (n32 > 0) {
    const u32 = new Uint32Array(target.buffer, target.byteOffset, n32);
    let i = 0;
    for (; i + 16 <= n32; i += 16) {
      const o = i & 63;
      u32[i] ^= k32[o];
      u32[i + 1] ^= k32[o + 1];
      u32[i + 2] ^= k32[o + 2];
      u32[i + 3] ^= k32[o + 3];
      u32[i + 4] ^= k32[o + 4];
      u32[i + 5] ^= k32[o + 5];
      u32[i + 6] ^= k32[o + 6];
      u32[i + 7] ^= k32[o + 7];
      u32[i + 8] ^= k32[o + 8];
      u32[i + 9] ^= k32[o + 9];
      u32[i + 10] ^= k32[o + 10];
      u32[i + 11] ^= k32[o + 11];
      u32[i + 12] ^= k32[o + 12];
      u32[i + 13] ^= k32[o + 13];
      u32[i + 14] ^= k32[o + 14];
      u32[i + 15] ^= k32[o + 15];
    }
    for (; i < n32; i++) {
      u32[i] ^= k32[i & 63];
    }
  }

  for (let b = n32 << 2; b < len; b++) {
    target[b] ^= k8[b & 255];
  }

  return target;
}

/**
 * Lấy link CDN trực tiếp (us.aws.cdn.hf.co) và tổng dung lượng file (X-Linked-Size)
 * Phân biệt file Raw vs Disguised và lưu Cache 15 phút để tua không bị 302 Redirect delay
 */
async function getFileMeta(hfUrl, autoKey = false) {
  const now = Date.now();
  const cached = metaCache.get(hfUrl);
  if (cached && now - cached.time < 15 * 60 * 1000) {
    return cached;
  }

  let cdnUrl = hfUrl;
  let totalH5Size = 0;

  const probeResp = await fetch(hfUrl, {
    method: "GET",
    headers: { Range: "bytes=0-127" },
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

  // Probe 128 byte đầu từ CDN để kiểm tra Raw vs Disguised và kích thước (nếu thiếu)
  const sampleResp = await fetch(cdnUrl, {
    headers: { Range: "bytes=0-127" }
  });
  const sampleBuf = new Uint8Array(await sampleResp.arrayBuffer());
  if (!totalH5Size) {
    const cr = sampleResp.headers.get("Content-Range");
    if (cr && cr.includes("/")) {
      totalH5Size = parseInt(cr.split("/")[1], 10);
    }
  }

  // 1. Kiểm tra video gốc chưa mã hóa (offset 0)
  const isRawMkv = sampleBuf.length >= 4 && sampleBuf[0] === 0x1a && sampleBuf[1] === 0x45 && sampleBuf[2] === 0xdf && sampleBuf[3] === 0xa3;
  const isRawMp4 = sampleBuf.length >= 8 && sampleBuf[4] === 0x66 && sampleBuf[5] === 0x74 && sampleBuf[6] === 0x79 && sampleBuf[7] === 0x70;
  const isRawAvi = sampleBuf.length >= 4 && sampleBuf[0] === 0x52 && sampleBuf[1] === 0x49 && sampleBuf[2] === 0x46 && sampleBuf[3] === 0x46;

  if (isRawMkv || isRawMp4 || isRawAvi) {
    const meta = {
      isRaw: true,
      headerSize: 0,
      cdnUrl,
      totalSize: totalH5Size,
      mediaSize: totalH5Size,
      tables: null,
      contentType: isRawMp4 ? "video/mp4" : isRawAvi ? "video/x-msvideo" : "video/x-matroska",
      time: now
    };
    metaCache.set(hfUrl, meta);
    return meta;
  }

  // 2. File ngụy trang (có header 64 byte + XOR 256-byte)
  let tables = DEFAULT_TABLES;
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
    }
  }

  const meta = {
    isRaw: false,
    headerSize: HEADER_SIZE,
    cdnUrl,
    totalSize: totalH5Size,
    mediaSize: Math.max(0, totalH5Size - HEADER_SIZE),
    tables,
    contentType: "video/x-matroska",
    time: now
  };
  metaCache.set(hfUrl, meta);
  return meta;
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

  // Bỏ đuôi ảo .mkv được gắn thêm sau đuôi thật (VD: .arrow.mkv -> .arrow, .h5.mkv -> .h5)
  path = path.replace(/(\.[a-z0-9]+)\.mkv$/i, "$1");

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

async function handleHfStream(request, url, hfUrl) {
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
  const meta = await getFileMeta(hfUrl, autoKey);
  const mediaSize = meta.mediaSize;

  // 1. HEAD request
  if (request.method === "HEAD") {
    return new Response(null, {
      status: 200,
      headers: {
        "Content-Type": meta.contentType,
        "Content-Length": String(mediaSize),
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*"
      }
    });
  }

  // 2. Hỗ trợ đầy đủ các dạng Range (START-END, START-, suffix -SUFFIX)
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
      if (suffixLen > 0) {
        start = Math.max(0, mediaSize - suffixLen);
        end = mediaSize - 1;
      }
    } else if (dashIdx > 0) {
      const sStr = rangeSpec.slice(0, dashIdx);
      const eStr = rangeSpec.slice(dashIdx + 1);
      start = parseInt(sStr, 10) || 0;
      if (eStr.length > 0) {
        end = Math.min(mediaSize - 1, parseInt(eStr, 10));
      }
    }
  }

  if (start >= mediaSize || start > end) {
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
    metaCache.delete(hfUrl);
    const freshMeta = await getFileMeta(hfUrl, autoKey);
    upstreamResp = await fetch(freshMeta.cdnUrl, {
      headers: { Range: `bytes=${hfStart}-${hfEnd}` }
    });
  }

  if (!upstreamResp.ok && upstreamResp.status !== 206) {
    return new Response(`Upstream CDN error: ${upstreamResp.status}`, {
      status: upstreamResp.status
    });
  }

  const contentLength = end - start + 1;
  const respHeaders = new Headers({
    "Content-Type": meta.contentType,
    "Accept-Ranges": "bytes",
    "Content-Length": String(contentLength),
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges",
    "Content-Disposition": 'inline; filename="video.mkv"'
  });

  if (isPartial) {
    respHeaders.set("Content-Range", `bytes ${start}-${end}/${mediaSize}`);
  }

  // File raw nguyên bản -> truyền trực tiếp body (kèm FixedLengthStream nếu có)
  if (meta.isRaw) {
    let rawBody = upstreamResp.body;
    if (typeof FixedLengthStream !== "undefined") {
      try {
        const fls = new FixedLengthStream(contentLength);
        rawBody = rawBody.pipeThrough(fls);
      } catch {}
    }
    return new Response(rawBody, {
      status: isPartial ? 206 : 200,
      headers: respHeaders
    });
  }

  // 4. File mã hóa: Dùng TransformStream + FixedLengthStream để giải mã trực tiếp
  //    và ĐẢM BẢO giữ nguyên Content-Length (tránh bị Cloudflare chuyển sang Transfer-Encoding: chunked làm player lỗi tua)
  let rotOffset = ((start % 256) + 256) % 256;
  const tables = meta.tables || DEFAULT_TABLES;

  const ts = new TransformStream({
    transform(chunk, controller) {
      try {
        const dec = xorChunkFast(chunk, rotOffset, tables);
        rotOffset = (rotOffset + chunk.byteLength) & 255;
        controller.enqueue(dec);
      } catch (err) {
        controller.error(err);
      }
    }
  });

  let bodyStream = upstreamResp.body.pipeThrough(ts);

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

    // 2. Giải mã & Stream trực tiếp file video từ Hugging Face (/hf/... hoặc /<user>/<repo>/<file> hoặc /?url=...)
    const hfUrl = resolveHfUrl(url);
    if (hfUrl) {
      return handleHfStream(request, url, hfUrl);
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

    // 0. Tuyến 0 (sync-realtime): chạy mỗi 30 phút một lần (smart dedup + cập nhật phim mới)
    await dispatch("sync-realtime.yml", { pages: "2" });

    const min = new Date().getUTCMinutes();
    const hour = new Date().getUTCHours();

    // 1. Tuyến 1 (crawl): chạy mỗi giờ một lần (vào đầu giờ :00)
    if (min < 15) {
      await dispatch("crawl.yml", { budget: "3600" });

      // 2. Tuyến 2 (upgrade-quality): 05:00 UTC (12:00 VN) & 17:00 UTC (00:00 VN)
      if (hour === 5 || hour === 17) {
        await dispatch("upgrade-quality.yml", { type: "all", limit: "0", dry_run: "false" });
      }

      // 3. Tuyến 3 (health-check): 20:00 UTC (03:00 VN)
      if (hour === 20) {
        await dispatch("health-check.yml", { limit: "0", dry_run: "false" });
      }
    }
  },
};
