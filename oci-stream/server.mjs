// oci-stream/server.mjs — Streaming XOR Decryptor proxy trên nothingp-sever (ARM64) cho Hugging Face qua Caddy HTTPS.
import http from "node:http";

const PORT = Number(process.env.PORT || 8090);
const HOST = process.env.HOST || "0.0.0.0";
const HEADER_SIZE = 64;
const META_TTL_MS = 15 * 60 * 1000;
const META_MAX_ENTRIES = 2000;

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
const metaCache = new Map();
const inFlightMeta = new Map();

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

export function xorChunkFast(chunk, rotOffset, tables) {
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

function setMetaCache(key, value) {
  if (metaCache.size >= META_MAX_ENTRIES) {
    const oldest = metaCache.keys().next().value;
    if (oldest) metaCache.delete(oldest);
  }
  metaCache.set(key, value);
}

export function resolveHfUrl(urlObj) {
  const paramUrl = urlObj.searchParams.get("url");
  if (paramUrl) {
    try {
      const parsed = new URL(paramUrl.replace("/blob/", "/resolve/"));
      if (parsed.origin === "https://huggingface.co" &&
          (parsed.pathname.startsWith("/datasets/") || parsed.pathname.startsWith("/buckets/"))) {
        return parsed.toString();
      }
    } catch {}
    return null;
  }

  let path = urlObj.pathname.replace(/^\/+/, "");
  if (path.startsWith("hf/")) {
    path = path.slice(3).replace(/^\/+/, "");
  }
  const parts = path.split("/").filter(Boolean);
  if (parts.length === 0 || parts[0] === "health") return null;

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

async function getFileMeta(hfUrl, autoKey = false) {
  const now = Date.now();
  const cacheKey = autoKey ? `${hfUrl}#ak` : hfUrl;
  const cached = metaCache.get(cacheKey);
  if (cached && now - cached.time < META_TTL_MS) {
    return cached;
  }
  if (inFlightMeta.has(cacheKey)) {
    return inFlightMeta.get(cacheKey);
  }

  const p = (async () => {
    let cdnUrl = hfUrl;
    let totalH5Size = 0;

    const probeResp = await fetch(hfUrl, {
      method: "GET",
      headers: { Range: `bytes=${HEADER_SIZE}-${HEADER_SIZE + 15}` },
      redirect: "manual"
    });
    if (probeResp.status >= 300 && probeResp.status < 400) {
      const loc = probeResp.headers.get("location");
      if (loc) cdnUrl = new URL(loc, hfUrl).toString();
      const linkedSize = probeResp.headers.get("x-linked-size");
      if (linkedSize) totalH5Size = parseInt(linkedSize, 10);
    } else if (probeResp.status === 206) {
      const cr = probeResp.headers.get("content-range");
      if (cr && cr.includes("/")) totalH5Size = parseInt(cr.split("/")[1], 10);
    }
    try { await probeResp.body?.cancel(); } catch {}

    const sampleResp = await fetch(cdnUrl, { headers: { Range: "bytes=0-127" } });
    if (!sampleResp.ok && sampleResp.status !== 206) {
      try { await sampleResp.body?.cancel(); } catch {}
      throw new Error(`HF sample probe ${sampleResp.status}`);
    }
    if (!totalH5Size) {
      const cr = sampleResp.headers.get("content-range");
      if (cr && cr.includes("/")) totalH5Size = parseInt(cr.split("/")[1], 10);
    }
    const sBuf = new Uint8Array(await sampleResp.arrayBuffer());

    const isRawMkv = sBuf.length >= 4 && sBuf[0] === 0x1a && sBuf[1] === 0x45 && sBuf[2] === 0xdf && sBuf[3] === 0xa3;
    const isRawMp4 = sBuf.length >= 8 && (
      (sBuf[4] === 0x66 && sBuf[5] === 0x74 && sBuf[6] === 0x79 && sBuf[7] === 0x70) ||
      (sBuf[4] === 0x6d && sBuf[5] === 0x6f && sBuf[6] === 0x6f && sBuf[7] === 0x76)
    );
    const isRawAvi = sBuf.length >= 4 && sBuf[0] === 0x52 && sBuf[1] === 0x49 && sBuf[2] === 0x46 && sBuf[3] === 0x46;
    const isRawTs = sBuf.length >= 1 && sBuf[0] === 0x47;

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
      setMetaCache(cacheKey, rawMeta);
      return rawMeta;
    }

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
      contentType: isDisguisedMp4 ? "video/mp4" : "video/x-matroska",
      time: now
    };
    setMetaCache(cacheKey, meta);
    return meta;
  })();

  inFlightMeta.set(cacheKey, p);
  try {
    return await p;
  } finally {
    inFlightMeta.delete(cacheKey);
  }
}

function waitForDrain(res, signal) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(new Error("aborted"));
      return;
    }
    const onDrain = () => {
      cleanup();
      resolve();
    };
    const onAbort = () => {
      cleanup();
      reject(new Error("aborted"));
    };
    const cleanup = () => {
      res.off("drain", onDrain);
      signal.removeEventListener("abort", onAbort);
    };
    res.on("drain", onDrain);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
  "Access-Control-Allow-Headers": "Range, Content-Type, Accept",
  "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges"
};

async function handleStream(req, res, urlObj) {
  if (req.method === "OPTIONS") {
    res.writeHead(204, CORS_HEADERS);
    res.end();
    return;
  }

  const hfUrl = resolveHfUrl(urlObj);
  if (!hfUrl) {
    res.writeHead(404, { "Content-Type": "application/json", ...CORS_HEADERS });
    res.end(JSON.stringify({ error: "Invalid HF path" }));
    return;
  }

  const autoKey = urlObj.searchParams.get("autokey") === "1";
  const cacheKey = autoKey ? `${hfUrl}#ak` : hfUrl;
  let meta = await getFileMeta(hfUrl, autoKey);
  const mediaSize = meta.mediaSize;

  if (req.method === "HEAD") {
    res.writeHead(200, {
      "Content-Type": meta.contentType || "video/x-matroska",
      "Content-Length": String(mediaSize),
      "Accept-Ranges": "bytes",
      "Cache-Control": "no-store, no-cache, must-revalidate",
      ...CORS_HEADERS
    });
    res.end();
    return;
  }

  const rangeHeader = req.headers.range;
  let start = 0;
  let end = mediaSize - 1;
  let isPartial = false;

  if (rangeHeader && rangeHeader.startsWith("bytes=")) {
    isPartial = true;
    const rangeSpec = rangeHeader.replace("bytes=", "").split(",")[0].trim();
    const dashIdx = rangeSpec.indexOf("-");
    if (dashIdx === 0) {
      const suffixLen = parseInt(rangeSpec.slice(1), 10);
      if (!Number.isFinite(suffixLen) || suffixLen <= 0) start = mediaSize;
      else start = Math.max(0, mediaSize - suffixLen);
    } else if (dashIdx > 0) {
      const sStr = rangeSpec.slice(0, dashIdx).trim();
      const eStr = rangeSpec.slice(dashIdx + 1).trim();
      const parsedStart = sStr.length > 0 ? parseInt(sStr, 10) : 0;
      start = Number.isFinite(parsedStart) && parsedStart >= 0 ? parsedStart : mediaSize;
      if (eStr.length > 0) {
        const parsedEnd = parseInt(eStr, 10);
        end = Number.isFinite(parsedEnd) ? Math.min(mediaSize - 1, parsedEnd) : mediaSize - 1;
      }
    } else {
      start = mediaSize;
    }
  }

  if (mediaSize <= 0 || start < 0 || start >= mediaSize || start > end) {
    res.writeHead(416, { "Content-Range": `bytes */${mediaSize}`, ...CORS_HEADERS });
    res.end();
    return;
  }

  const ac = new AbortController();
  const onClose = () => {
    if (!res.writableFinished) ac.abort();
  };
  res.once("close", onClose);

  const hfStart = start + meta.headerSize;
  const hfEnd = end + meta.headerSize;

  let upstreamResp = await fetch(meta.cdnUrl, {
    headers: { Range: `bytes=${hfStart}-${hfEnd}` },
    signal: ac.signal
  });

  if (upstreamResp.status === 401 || upstreamResp.status === 403) {
    try { await upstreamResp.body?.cancel(); } catch {}
    metaCache.delete(cacheKey);
    meta = await getFileMeta(hfUrl, autoKey);
    upstreamResp = await fetch(meta.cdnUrl, {
      headers: { Range: `bytes=${hfStart}-${hfEnd}` },
      signal: ac.signal
    });
  }

  if (!upstreamResp.ok && upstreamResp.status !== 206) {
    try { await upstreamResp.body?.cancel(); } catch {}
    res.writeHead(upstreamResp.status, CORS_HEADERS);
    res.end(`Upstream CDN error: ${upstreamResp.status}`);
    return;
  }

  const contentLength = end - start + 1;
  const outHeaders = {
    "Content-Type": meta.contentType || "video/x-matroska",
    "Accept-Ranges": "bytes",
    "Content-Length": String(contentLength),
    "Cache-Control": "no-store, no-cache, must-revalidate",
    "Pragma": "no-cache",
    "Content-Disposition": "inline",
    ...CORS_HEADERS
  };
  if (isPartial) {
    outHeaders["Content-Range"] = `bytes ${start}-${end}/${mediaSize}`;
  }

  res.writeHead(isPartial ? 206 : 200, outHeaders);

  const reader = upstreamResp.body.getReader();
  let remaining = contentLength;
  let rotOffset = ((start % 256) + 256) % 256;
  const tables = meta.tables || DEFAULT_TABLES;

  try {
    while (remaining > 0 && !ac.signal.aborted) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value || value.byteLength === 0) continue;
      const slice = value.byteLength > remaining ? value.subarray(0, remaining) : value;
      const out = meta.isRaw ? slice : xorChunkFast(slice, rotOffset, tables);
      rotOffset = (rotOffset + slice.byteLength) & 255;
      remaining -= slice.byteLength;
      if (!res.write(out)) {
        await waitForDrain(res, ac.signal);
      }
    }
    if (!ac.signal.aborted) {
      if (remaining === 0) res.end();
      else res.destroy(new Error(`upstream truncated (${remaining}B short)`));
    }
  } catch (err) {
    if (!ac.signal.aborted) {
      console.error("[p4k-stream] err:", err?.message || err);
      res.destroy(err);
    }
  } finally {
    res.off("close", onClose);
    reader.cancel().catch(() => {});
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const urlObj = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    if (urlObj.pathname === "/hf/health" || urlObj.pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json", ...CORS_HEADERS });
      res.end(JSON.stringify({ ok: true, cacheSize: metaCache.size }));
      return;
    }
    await handleStream(req, res, urlObj);
  } catch (err) {
    if (!res.headersSent) {
      res.writeHead(502, { "Content-Type": "application/json", ...CORS_HEADERS });
      res.end(JSON.stringify({ error: err?.message || String(err) }));
    } else if (!res.destroyed) {
      res.destroy(err);
    }
  }
});

if (import.meta.url === `file://${process.argv[1]}`) {
  server.requestTimeout = 0;
  server.headersTimeout = 30000;
  server.listen(PORT, HOST, () => {
    console.log(`[p4k-stream] listening on http://${HOST}:${PORT}`);
  });
}
