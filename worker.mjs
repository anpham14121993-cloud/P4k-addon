// worker.mjs — entry Cloudflare Workers cho addon Phim4K (DB Turso + Streaming XOR Decryptor cho Hugging Face).
import { setDb, setEnv, addon } from "./core.mjs";
import { tursoDb } from "./turso-db.mjs";

let ready = false;

/* ═══════════════ BỘ GIẢI MÃ LUỒNG HUGGING FACE (STREAMING XOR 256-BYTE) ═══════════════ */

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

// Cache lưu trạng thái mã hóa & khóa theo từng URL Hugging Face
const hfFileCache = new Map();

function cacheHfInfo(hfUrl, info) {
  if (hfFileCache.size > 2000) {
    const firstKey = hfFileCache.keys().next().value;
    if (firstKey) hfFileCache.delete(firstKey);
  }
  hfFileCache.set(hfUrl, info);
  return info;
}

/**
 * Tự động dò khóa XOR 256-byte từ 64 KB đầu của file (nếu file dùng khóa mới khác DEFAULT_KEY)
 */
async function autoDetectKey(hfUrl) {
  const resp = await fetch(hfUrl, {
    headers: { Range: `bytes=${HEADER_SIZE}-${HEADER_SIZE + 65535}` },
    redirect: "follow"
  });
  const buf = new Uint8Array(await resp.arrayBuffer());

  if (
    buf.length >= 4 &&
    (buf[0] ^ DEFAULT_KEY[0]) === 0x1a &&
    (buf[1] ^ DEFAULT_KEY[1]) === 0x45 &&
    (buf[2] ^ DEFAULT_KEY[2]) === 0xdf &&
    (buf[3] ^ DEFAULT_KEY[3]) === 0xa3
  ) {
    return DEFAULT_KEY;
  }

  const recovered = new Uint8Array(256);
  const counts = new Int32Array(256);
  for (let col = 0; col < 256; col++) {
    counts.fill(0);
    let maxCount = -1;
    let bestByte = 0;
    for (let i = col; i < buf.length; i += 256) {
      const b = buf[i];
      const c = ++counts[b];
      if (c > maxCount) {
        maxCount = c;
        bestByte = b;
      }
    }
    recovered[col] = bestByte;
  }
  return recovered;
}

/**
 * Kiểm tra nhanh 128 byte đầu để phân biệt:
 *  - File đời cũ nguyên bản (Raw MKV / MP4 / AVI từ byte 0) -> phát trực tiếp không dịch +64, không XOR
 *  - File đời mới ngụy trang 64-byte header + XOR 256-byte -> cắt 64 byte và giải mã XOR
 */
async function detectHfFile(hfUrl, forceAutoKey = false) {
  if (!forceAutoKey && hfFileCache.has(hfUrl)) {
    return hfFileCache.get(hfUrl);
  }

  const probeResp = await fetch(hfUrl, {
    headers: { Range: "bytes=0-127" },
    redirect: "follow"
  });

  if (!probeResp.ok && probeResp.status !== 206) {
    return { errorStatus: probeResp.status };
  }

  let rawTotalSize = 0;
  const cr = probeResp.headers.get("Content-Range");
  if (cr && cr.includes("/")) {
    rawTotalSize = parseInt(cr.split("/")[1], 10) || 0;
  }

  const buf = new Uint8Array(await probeResp.arrayBuffer());

  // 1. Kiểm tra file video gốc chưa mã hóa (offset 0)
  const isRawMkv = buf.length >= 4 && buf[0] === 0x1a && buf[1] === 0x45 && buf[2] === 0xdf && buf[3] === 0xa3;
  const isRawMp4 = buf.length >= 8 && buf[4] === 0x66 && buf[5] === 0x74 && buf[6] === 0x79 && buf[7] === 0x70;
  const isRawAvi = buf.length >= 4 && buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46;

  if (isRawMkv || isRawMp4 || isRawAvi) {
    const contentType = isRawMp4 ? "video/mp4" : isRawAvi ? "video/x-msvideo" : "video/x-matroska";
    return cacheHfInfo(hfUrl, { isRaw: true, key: null, contentType, rawTotalSize });
  }

  // 2. Kiểm tra file mã hóa bằng DEFAULT_KEY tại offset 64 (0x40)
  if (!forceAutoKey && buf.length >= HEADER_SIZE + 8) {
    const d0 = buf[HEADER_SIZE] ^ DEFAULT_KEY[0];
    const d1 = buf[HEADER_SIZE + 1] ^ DEFAULT_KEY[1];
    const d2 = buf[HEADER_SIZE + 2] ^ DEFAULT_KEY[2];
    const d3 = buf[HEADER_SIZE + 3] ^ DEFAULT_KEY[3];
    const d4 = buf[HEADER_SIZE + 4] ^ DEFAULT_KEY[4];
    const d5 = buf[HEADER_SIZE + 5] ^ DEFAULT_KEY[5];
    const d6 = buf[HEADER_SIZE + 6] ^ DEFAULT_KEY[6];
    const d7 = buf[HEADER_SIZE + 7] ^ DEFAULT_KEY[7];

    const isEncMkv = d0 === 0x1a && d1 === 0x45 && d2 === 0xdf && d3 === 0xa3;
    const isEncMp4 = d4 === 0x66 && d5 === 0x74 && d6 === 0x79 && d7 === 0x70;

    if (isEncMkv || isEncMp4) {
      const contentType = isEncMp4 ? "video/mp4" : "video/x-matroska";
      return cacheHfInfo(hfUrl, { isRaw: false, key: DEFAULT_KEY, contentType, rawTotalSize });
    }
  }

  // 3. Nếu dùng khóa 256-byte mới hoặc truyền ?autokey=1 -> tự động khôi phục khóa
  const recoveredKey = await autoDetectKey(hfUrl);
  return cacheHfInfo(hfUrl, { isRaw: false, key: recoveredKey, contentType: "video/x-matroska", rawTotalSize });
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
        "Access-Control-Allow-Headers": "Range, Content-Type"
      }
    });
  }

  const forceAutoKey = url.searchParams.get("autokey") === "1";
  const fileInfo = await detectHfFile(hfUrl, forceAutoKey);

  if (fileInfo.errorStatus) {
    return new Response(`Hugging Face upstream error: ${fileInfo.errorStatus}`, {
      status: fileInfo.errorStatus,
      headers: { "Access-Control-Allow-Origin": "*" }
    });
  }

  const headerOffset = fileInfo.isRaw ? 0 : HEADER_SIZE;
  const mediaTotalSize = fileInfo.rawTotalSize > headerOffset ? fileInfo.rawTotalSize - headerOffset : 0;

  // Xử lý HEAD request
  if (request.method === "HEAD") {
    const headers = new Headers({
      "Content-Type": fileInfo.contentType,
      "Accept-Ranges": "bytes",
      "Access-Control-Allow-Origin": "*"
    });
    if (mediaTotalSize > 0) {
      headers.set("Content-Length", String(mediaTotalSize));
    }
    return new Response(null, { status: 200, headers });
  }

  // Đọc Range header từ trình phát video
  const rangeHeader = request.headers.get("Range");
  let start = 0;
  let end = null;
  let isSuffix = false;
  let suffixLength = 0;

  if (rangeHeader && rangeHeader.startsWith("bytes=")) {
    const rawRange = rangeHeader.replace("bytes=", "").trim();
    if (rawRange.startsWith("-")) {
      isSuffix = true;
      suffixLength = parseInt(rawRange.slice(1), 10) || 0;
    } else {
      const [s, e] = rawRange.split("-");
      start = parseInt(s, 10) || 0;
      if (e !== undefined && e !== "") {
        const parsedEnd = parseInt(e, 10);
        if (!Number.isNaN(parsedEnd)) end = parsedEnd;
      }
    }
  }

  let hfRangeHeader = "";
  if (isSuffix) {
    hfRangeHeader = `bytes=-${suffixLength}`;
  } else {
    const hfStart = start + headerOffset;
    const hfEnd = end !== null ? end + headerOffset : "";
    hfRangeHeader = `bytes=${hfStart}-${hfEnd}`;
  }

  const hfResp = await fetch(hfUrl, {
    headers: { Range: hfRangeHeader },
    redirect: "follow"
  });

  if (!hfResp.ok && hfResp.status !== 206) {
    return new Response(`Hugging Face upstream error: ${hfResp.status}`, {
      status: hfResp.status,
      headers: { "Access-Control-Allow-Origin": "*" }
    });
  }

  const contentRange = hfResp.headers.get("Content-Range");
  let totalSize = mediaTotalSize;
  let actualStart = start;
  let actualEnd = end !== null ? end : (totalSize > 0 ? totalSize - 1 : 0);

  if (contentRange && contentRange.includes("/")) {
    const [rangePart, totalPart] = contentRange.replace("bytes ", "").split("/");
    const [rStart, rEnd] = rangePart.split("-");
    totalSize = parseInt(totalPart, 10) - headerOffset;
    actualStart = Math.max(0, parseInt(rStart, 10) - headerOffset);
    actualEnd = Math.max(0, parseInt(rEnd, 10) - headerOffset);
  }

  const respHeaders = new Headers({
    "Content-Type": fileInfo.contentType,
    "Accept-Ranges": "bytes",
    "Access-Control-Allow-Origin": "*",
    "Content-Disposition": 'inline; filename="video.mkv"'
  });

  if (rangeHeader && contentRange) {
    respHeaders.set("Content-Range", `bytes ${actualStart}-${actualEnd}/${totalSize}`);
    respHeaders.set("Content-Length", String(actualEnd - actualStart + 1));
  } else if (totalSize > 0) {
    respHeaders.set("Content-Length", String(totalSize));
  }

  // Trường hợp file đời cũ nguyên bản (không mã hóa): truyền thẳng body không tốn CPU XOR
  if (fileInfo.isRaw) {
    return new Response(hfResp.body, {
      status: rangeHeader ? 206 : 200,
      headers: respHeaders
    });
  }

  // Trường hợp file mã hóa: Giải mã luồng (Streaming XOR Decryption) trên RAM bằng TransformStream
  const activeKey = fileInfo.key || DEFAULT_KEY;
  let currentOffset = actualStart;
  const { readable, writable } = new TransformStream({
    transform(chunk, controller) {
      const out = new Uint8Array(chunk.length);
      for (let i = 0; i < chunk.length; i++) {
        out[i] = chunk[i] ^ activeKey[(currentOffset + i) & 255];
      }
      currentOffset += chunk.length;
      controller.enqueue(out);
    }
  });

  hfResp.body.pipeTo(writable).catch(() => {});

  return new Response(readable, {
    status: rangeHeader ? 206 : 200,
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
