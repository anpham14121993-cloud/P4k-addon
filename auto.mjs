// auto.mjs — 1 process, 5 token-lane, 3 role (feed/inv/index) tự cân bằng theo backlog.
// Chạy: HF_TOKEN="t1,t2,t3,t4,t5" node auto.mjs [giây]
// Env: P4K_DB, P4K_GAP (ms/token), P4K_INV_TTL (bỏ trống = TTL thích ứng: has_id 5h / nrepo 7d / còn lại 14d;
//      account junk thử lại sau 1 năm), P4K_REPO_TTL (ms, mặc định 6h — repo done quá hạn được kiểm tra lại
//      và ưu tiên trước feed), P4K_REPO_TTL_NO_ID (mặc định 12h), P4K_SLOTS (mặc định 3/token),
//      P4K_FEED_LIMIT (mặc định 1000 mục/trang), P4K_FEED_TOP=1 (luôn quét từ đầu feed mỗi lượt),
//      P4K_FEED_PAGES (số trang quét đầu run), P4K_POLL_MS (ms giữa 2 lần poll top, 0 = tắt),
//      P4K_POLL_PAGES (mặc định 1 trang/lần poll), P4K_POLL_UNTIL (giây đầu run còn poll top),
//      P4K_BKT=0 (tắt lane bucket), P4K_BKT_WORKERS (mặc định 4)
const P = await import(process.env.P4K_TURSO === "1" ? "./pdb-turso.mjs" : "./pdb.mjs");
const {
  openDB, ensureSchema, metaGet, metaSet, storeTree, finishRepo, touchRepo, unclaimRepo, reevalAccount,
  upsertRepoMeta, claimRepo, recoverStale, isFarm, isFarmStrict, isId, log, repoCounts,
  applyGitDiff, accountScore, getAccount, insertAccount, setScanned, pendingAccounts, countPending,
  countNew, stateCounts, getRepoPrev, pendingAccountsAdaptive, countPendingAdaptive,
  claimRepoStale, clearJunk, markJunk, countStale, flushTouches, releaseStale,
} = P;

const db = openDB();
await ensureSchema(db);

const TOKENS = String(process.env.HF_TOKEN || "").split(",").map((x) => x.trim()).filter(Boolean);
const GAP = Math.max(30, parseInt(process.env.P4K_GAP || "150", 10));
const SLOTS_PER_TOKEN = Math.max(1, parseInt(process.env.P4K_SLOTS || "3", 10));
const BUDGET = (parseInt(process.argv[2] || process.env.P4K_BUDGET || "3600", 10) || 1e9) * 1000;
const DEADLINE = Date.now() + BUDGET;
const START = Date.now();
const ENABLE_FEED = process.env.P4K_FEED !== "0";
const FEED_MAX_PAGES = parseInt(process.env.P4K_FEED_PAGES || "0", 10);
const FEED_LIMIT = Math.max(1, Math.min(1000, parseInt(process.env.P4K_FEED_LIMIT || "1000", 10)));
// P4K_FEED_TOP=1: luôn quét từ đầu feed mỗi lượt (lịch sử đã quét hết), không đi tiếp cursor cũ.
const FEED_TOP = process.env.P4K_FEED_TOP === "1";
// P4K_POLL_MS>0: sau lượt quét đầu, poll top feed mỗi POLL_MS giây trong POLL_UNTIL giây đầu run.
// 1 trang top đủ phát hiện dataset mới (1000 item ~ 8-9h ở nhịp hiện tại).
const POLL_MS = Math.max(0, parseInt(process.env.P4K_POLL_MS || "0", 10));
const POLL_PAGES = Math.max(1, parseInt(process.env.P4K_POLL_PAGES || "1", 10));
const POLL_END = Math.min(DEADLINE, START + Math.max(0, parseInt(process.env.P4K_POLL_UNTIL || "1800", 10)) * 1000);
// Lane bucket: mỗi account 1 request /api/buckets/{user}, xoay vòng 1 lượt/run.
const ENABLE_BKT = process.env.P4K_BKT !== "0";
const BKT_WORKERS = Math.max(1, parseInt(process.env.P4K_BKT_WORKERS || "4", 10));
const BKT_SWEEPS = Math.max(1, parseInt(process.env.P4K_BKT_SWEEPS || "1", 10));
const BKT_EVERY_MS = Math.max(60000, parseInt(process.env.P4K_BKT_EVERY || "900000", 10));
const IDLE_EXIT = Math.max(10, parseInt(process.env.P4K_IDLE_EXIT || "30", 10)) * 1000;
const RECOVER_MS = parseInt(process.env.P4K_RECOVER_MS || "300000", 10);
await recoverStale(db, RECOVER_MS);
const FIXED = process.env.P4K_INV_TTL ? parseInt(process.env.P4K_INV_TTL, 10) : null;
const base = "https://huggingface.co/api";
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
class Stop extends Error {}

// ---------------- token pool: mỗi token 1 gate riêng, 429 -> cooldown riêng ----------------
const lanes = (TOKENS.length ? TOKENS : [null]).map((tok) => ({ tok, next: 0, cool: 0, req: 0, n429: 0, err: 0 }));
let rr = 0;
function stat() {
  return lanes.map((t, i) => `T${i + 1}:${t.req}${t.n429 ? "/429=" + t.n429 : ""}${t.err ? "/e=" + t.err : ""}`).join(" ");
}
async function takeToken() {
  for (;;) {
    const now = Date.now();
    if (now > DEADLINE) throw new Stop();
    // Chọn token KHÔNG cooldown có slot sớm nhất; token 429 bị bỏ qua, không giữ worker.
    let best = null, bt = Infinity, bi = -1;
    for (let i = 0; i < lanes.length; i++) {
      const idx = (rr + i) % lanes.length;
      const t = lanes[idx];
      if (t.cool > now) continue;
      const at = Math.max(now, t.next);
      if (at < bt) { bt = at; best = t; bi = idx; }
    }
    if (!best) { await sleep(300); continue; }
    rr = (bi + 1) % lanes.length;
    best.next = bt + GAP; // reserve ngay (đồng bộ) -> không tranh slot giữa các worker
    if (bt > now) await sleep(bt - now);
    if (best.cool > Date.now()) continue;
    best.req++;
    return best;
  }
}
async function req(u, { text = false, timeout = 30000 } = {}) {
  for (let a = 0; a < 60; a++) {
    const t = await takeToken();
    try {
      const r = await fetch(u, {
        headers: t.tok ? { Accept: "application/json", Authorization: "Bearer " + t.tok } : { Accept: "application/json" },
        signal: AbortSignal.timeout(timeout),
      });
      if (r.status === 429) {
        const ra = parseInt(r.headers.get("retry-after") || "0", 10);
        t.cool = Date.now() + (ra > 0 ? ra * 1000 : 15000);
        t.n429++;
        continue;
      }
      if (r.status === 404 || r.status === 401 || r.status === 403) return { ok: false, status: r.status };
      if (!r.ok) { t.err++; await sleep(900); continue; }
      const body = await r.text();
      if (text) return { ok: true, text: body };
      let s = body;
      const mk = s.indexOf("Markdown Content:");
      if (mk >= 0) { const jb = s.indexOf("{", mk), ab = s.indexOf("[", mk); s = s.slice((jb >= 0 && (ab < 0 || jb < ab)) ? jb : ab); }
      try { return { ok: true, data: JSON.parse(s), link: r.headers.get("link") || "" }; } catch { t.err++; await sleep(200); }
    } catch { t.err++; await sleep(900); }
  }
  return { ok: false, status: 0 };
}
const plural = (kind) => (kind === "model" ? "models" : kind === "bucket" ? "buckets" : "datasets");
const treePath = (kind, id) => (kind === "bucket" ? `/buckets/${id}/tree?recursive=true` : `/${plural(kind)}/${id}/tree/main?recursive=true`);
const nextLink = (link) => { for (const p of (link || "").split(",")) { const m = p.match(/<([^>]+)>;\s*rel="next"/); if (m) return m[1]; } return null; };
const nextCursor = (link) => { const m = (link || "").match(/cursor=([^&>]+)[^>]*>;\s*rel="next"/); return m ? decodeURIComponent(m[1]) : null; };
async function getAllPages(url) {
  let out = [], u = url;
  for (let p = 0; p < 2000; p++) {
    const r = await req(u);
    if (r.status === 404) return { data: out, complete: true, status: 404 };
    if (!r.ok || !Array.isArray(r.data)) return { data: out, complete: false, status: r.status };
    out.push(...r.data);
    const nx = nextLink(r.link);
    if (!nx) return { data: out, complete: true, status: r.status };
    u = nx;
  }
  return { data: out, complete: false, status: 200 };
}
async function getTree(id, kind) {
  let u = base + treePath(kind, id); const out = [];
  for (let p = 0; p < 500; p++) {
    const r = await req(u);
    if (!r.ok || !Array.isArray(r.data)) return { ok: false, status: r.status };
    out.push(...r.data);
    const nx = nextLink(r.link);
    if (!nx) return { ok: true, data: out };
    u = nx;
  }
  return { ok: true, data: out };
}
async function repoInfo(kind, id) {
  const r = await req(`${base}/${plural(kind)}/${id}`);
  if (!r.ok || !r.data || typeof r.data !== "object") return { ok: false, status: r.status };
  const d = r.data;
  const updatedAt = d.lastModified ? (Date.parse(d.lastModified) || 0) : d.updatedAt ? (Date.parse(d.updatedAt) || 0) : 0;
  return { ok: true, sha: d.sha || null, lastModified: updatedAt, updatedAt };
}

// ---------------- scheduler: chia slot theo backlog ----------------
const TOTAL = Math.max(1, lanes.length * SLOTS_PER_TOKEN);
const limits = { feed: 1, inv: 1, index: 1 };
const inflight = { feed: 0, inv: 0, index: 0 };
let pending = { inv: 1, index: 0, recheck: 0, feedPages: ENABLE_FEED ? 1 : 0, bkt: 0 }; // inv=1 để role inv có slot ngay, refreshInv sẽ chỉnh lại
let staleBusy = 0;
// Quota ghi của Turso free = 10M dòng/tháng -> giới hạn số dòng file ghi mỗi run (P4K_WRITE_CAP, 0 = không giới hạn).
const WRITE_CAP = Math.max(0, parseInt(process.env.P4K_WRITE_CAP || "250000", 10));
let wFiles = 0;
const stopState = { stop: false };
// TTL động: has_id=1 (có file phim) 1h, has_id=0 (không có phim) 12h
const REPO_TTL_HAS_ID_MS = Math.max(600000, parseInt(process.env.P4K_REPO_TTL || "3600000", 10)); // 1h
const REPO_TTL_NO_ID_MS = Math.max(600000, parseInt(process.env.P4K_REPO_TTL_NO_ID || "43200000", 10)); // 12h
let staleBacklog = await countStale(db, Date.now() - REPO_TTL_HAS_ID_MS, Date.now() - REPO_TTL_NO_ID_MS); // backlog recheck thật (đếm 1 lần, giảm dần)
// pending.index = counter (1 dòng). pending.inv giữ bằng ước lượng trong tiến trình:
// refreshInv đặt theo số dòng vừa đọc (đã phải đọc rồi), mỗi account xử lý xong thì trừ 1,
// feed thêm account mới thì +1 -> KHÔNG cần COUNT quét accounts mỗi tick.
async function demand() {
  pending.index = wFiles >= WRITE_CAP ? 0 : await countNew(db);
}
async function rebalance() {
  await demand();
  pending.recheck = staleBusy > 0 ? 1 : 0;
  const wInv = pending.inv > 0 ? 4 * Math.min(1, pending.inv / 2000) : 0;
  const wIdx = pending.index > 0 || staleBacklog > 0 ? 2.6 * Math.min(1, Math.max(pending.index, staleBacklog) / 200) : 0;
  const wFeed = ENABLE_FEED && pending.recheck === 0 && pending.feedPages > 0 ? 1 : 0;
  const sw = wInv + wIdx + wFeed;
  let lInv = sw ? Math.round((TOTAL * wInv) / sw) : 0;
  let lIdx = sw ? Math.round((TOTAL * wIdx) / sw) : 0;
  let lFeed = wFeed > 0 ? TOTAL - lInv - lIdx : 0;
  if (sw === 0) { lInv = 0; lIdx = 0; lFeed = 0; }
  if (wInv > 0 && lInv < 1) lInv = 1;
  if (wIdx > 0 && lIdx < 1) lIdx = 1;
  if (wFeed > 0 && lFeed < 1) lFeed = 1;
  while (lInv + lIdx + lFeed > TOTAL) { if (lFeed > 1) lFeed--; else if (lInv > 1) lInv--; else lIdx--; }
  if (staleBusy > 0) {
    // Ưu tiên cập nhật repo cũ: chặn feed (giữ 1 slot khám phá nếu nhiều slot), dồn cho index.
    lFeed = ENABLE_FEED && pending.feedPages > 0 && TOTAL >= 12 ? 1 : 0;
    lIdx = Math.max(1, TOTAL - lInv - lFeed);
    while (lInv + lIdx + lFeed > TOTAL) { if (lIdx > 1) lIdx--; else if (lInv > 1) lInv--; else lFeed = 0; }
  }
  limits.inv = lInv; limits.index = lIdx; limits.feed = lFeed;
  log(`sched slots=${TOTAL} feed=${lFeed} inv=${lInv} index=${lIdx} | backlog feed=${pending.feedPages}p inv=${pending.inv} new=${pending.index} stale=${staleBacklog} recheck=${staleBusy} | ${stat()}`);
}
async function slot(role) {
  for (;;) {
    if (stopState.stop) throw new Stop();
    if (inflight[role] < limits[role]) { inflight[role]++; return; }
    await sleep(40);
  }
}
const done = (role) => { inflight[role] = Math.max(0, inflight[role] - 1); };
async function waitStop(ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (stopState.stop) throw new Stop();
    await sleep(Math.min(200, end - Date.now()));
  }
}

// ---------------- role: feed ----------------
let feedCursor = FEED_TOP ? null : (await metaGet(db, "feed_cursor")) || null;
let feedPages = 0, feedSeen = 0, feedEnq = 0, feedShape = 0, feedStyle = 0, feedAtEnd = false;
let feedPageCap = FEED_MAX_PAGES, feedPolling = false, feedPollN = 0, feedPagesAll = 0;
// Chẩn đoán feed: đếm item trùng trong cùng run + mốc lastModified mỗi trang và của cursor.
let feedDup = 0, feedCurLt = "-", feedNoLink = 0, feedNoLinkStreak = 0;
const feedIds = new Set();
const accCache = new Map();
async function accOf(u) {
  if (!accCache.has(u)) accCache.set(u, (await getAccount(db, u)) || null);
  return accCache.get(u);
}
async function feedSweepDone() {
  feedAtEnd = true;
  pending.feedPages = 0;
  log(`feed xong ${feedPageCap} trang enq=${feedEnq}`);
  if (!(POLL_MS > 0) || Date.now() >= POLL_END) return;
  feedPolling = true;
  while (!stopState.stop && Date.now() < POLL_END) {
    await waitStop(Math.min(POLL_MS, POLL_END - Date.now()));
    if (stopState.stop || Date.now() >= POLL_END) break;
    feedPageCap = POLL_PAGES;
    feedPages = 0;
    feedCursor = null;
    feedAtEnd = false;
    pending.feedPages = 1;
    log(`feed poll top #${++feedPollN}`);
    return;
  }
  feedPolling = false;
}
async function feedOne() {
  if (!ENABLE_FEED || feedAtEnd) { await sleep(1000); return; }
  if (FEED_MAX_PAGES > 0 && feedPages >= feedPageCap) { await feedSweepDone(); return; }
  const url = `${base}/datasets?limit=${FEED_LIMIT}&sort=lastModified` + (feedCursor ? `&cursor=${encodeURIComponent(feedCursor)}` : "");
  const j = await req(url);
  if (!j.ok || !Array.isArray(j.data)) { pending.feedPages = 0; log("feed stop status=" + j.status); await waitStop(30000); pending.feedPages = 1; return; }
  if (!j.data.length) { feedCursor = null; await metaSet(db, "feed_cursor", ""); feedAtEnd = true; pending.feedPages = 0; log("feed het cursor, nghi 300s"); await waitStop(300000); feedAtEnd = false; pending.feedPages = ENABLE_FEED ? 1 : 0; return; }
  for (const x of j.data) {
    feedSeen++;
    const id = String(x.id), author = id.split("/")[0];
    if (feedIds.has(id)) { feedDup++; continue; } // trùng trong run: đã xử lý rồi, khỏi đọc DB lại
    feedIds.add(id);
    if (!isFarmStrict(author)) continue;
    const acc = await accOf(author);
    if (acc?.junk) continue;
    const hasId = !!(acc && acc.has_id);
    if (/^[A-Z]/.test(author) && !hasId) { feedStyle++; continue; }
    if (acc && acc.nrepo >= 20 && !hasId) { feedStyle++; continue; }
    const rbase = id.split("/").slice(1).join("/");
    const isSelf = id === author + "/" + author;
    const lowerNum = /^[a-z]{2,}[0-9]{1,7}$/.test(author);
    const oneWord = /^[a-z]{3,20}$/.test(rbase);
    const capWord = /^[A-Za-z]{3,20}$/.test(rbase);
    // Account đã có phim (hasId): chấp nhận mọi repo của account đó
    // (kể cả tên chứa số/dấu '-'/'_'/generic) để không sót phim.
    // Account đã có phim (hasId): chấp nhận mọi repo của account đó
    // (kể cả tên chứa số/dấu '-'/'_'/generic) để không sót phim.
    const hyphenWord = rbase.includes("-") && /^[A-Za-z0-9-]{3,30}$/.test(rbase);
    const take = isSelf || hasId || (oneWord && lowerNum) || (capWord && lowerNum) || (hasId && hyphenWord);
    if (!take) { feedShape++; continue; }
    if (!acc) { // account đã có trong DB thì INSERT OR IGNORE là thừa (1 lượt đọc PK)
      if (await insertAccount(db, author)) pending.inv++; // account mới = đến hạn quét ngay
      accCache.delete(author);
    }
    if (await upsertRepoMeta(db, id, author, "dataset", x.lastModified)) feedEnq++;
  }
  const nx = nextCursor(j.link);
  if (!nx && j.data.length >= FEED_LIMIT) {
    // Trang đầy nhưng thiếu Link: nếu lưu cursor rỗng thì lượt sau chạy lại từ đầu feed.
    feedNoLink++; feedNoLinkStreak++;
    const lastLM = [...j.data].reverse().find((x) => x.lastModified)?.lastModified;
    if (feedNoLinkStreak >= 2 && lastLM) {
      const synth = Buffer.from(JSON.stringify({ $or: [{ lastModified: { $lt: lastLM } }, { lastModified: null }] })).toString("base64url");
      feedCursor = synth;
      await metaSet(db, "feed_cursor", synth);
      feedNoLinkStreak = 0;
      log(`feed thieu link p=${feedPages + 1}, tu dung cursor lt=${lastLM}`);
      await waitStop(3000);
    } else {
      log(`feed thieu link p=${feedPages + 1} (giu cursor cu)`);
      await waitStop(5000);
    }
    return;
  }
  if (!nx) {
    // Trang cuối thật (ít hơn limit, không có link): kết thúc feed, không để cursor rỗng
    // vì lượt sau sẽ quét lại từ đầu.
    feedPages++; feedPagesAll++;
    feedCursor = null;
    await metaSet(db, "feed_cursor", "");
    feedAtEnd = true; pending.feedPages = 0;
    log("feed het cursor (trang cuoi), nghi 300s");
    await waitStop(300000);
    feedAtEnd = false;
    pending.feedPages = ENABLE_FEED ? 1 : 0;
    return;
  }
  feedPages++; feedPagesAll++;
  feedNoLinkStreak = 0;
  feedCursor = nx;
  await metaSet(db, "feed_cursor", feedCursor || "");
  const lms = j.data.map((x) => x.lastModified).filter(Boolean);
  feedCurLt = "-";
  try {
    const o = JSON.parse(Buffer.from(feedCursor || "", "base64url").toString());
    const lt = (o?.$or || []).find((c) => c && c.lastModified && typeof c.lastModified === "object")?.lastModified?.$lt;
    feedCurLt = lt || o?.$or?.[0]?.lastModified || "-";
  } catch { }
  if (feedPages === 1 || feedPages % 10 === 0) log(`feed pages=${feedPages} seen=${feedSeen} dup=${feedDup} enq=${feedEnq} shape=${feedShape} style=${feedStyle} lm=${lms[0] || "-"}..${lms[lms.length - 1] || "-"} next_lt=${feedCurLt}`);
  if (FEED_MAX_PAGES > 0 && feedPages >= feedPageCap) { await feedSweepDone(); }
}

// ---------------- role: inv ----------------
let invRows = [], invAt = 0, invIdx = 0, invDone = 0, invOk = 0, invSkip = 0, invFail = 0, invEnq = 0;
const invTried = new Map();
const invBusy = new Set();
async function refreshInv() {
  const now = Date.now();
  const all = FIXED == null ? await pendingAccountsAdaptive(db, now) : await pendingAccounts(db, FIXED, false);
  pending.inv = all.length >= 1000 ? Math.max(pending.inv, 5000) : all.length;
  invRows = all.filter((a) => !invBusy.has(a.username) && (!invTried.has(a.username) || invTried.get(a.username) <= now));
  const key = (x) => (x.junk ? 1e6 : accountScore(x.username, x)); // account junk thử lại được ưu tiên trước
  invRows.sort((a, b) => key(b) - key(a) || a.scanned_at - b.scanned_at);
  invIdx = 0; invAt = now;
  if (invRows.length && invDone % 100 !== 0) log(`inv refresh todo=${invRows.length}`);
}
async function invOne(a) {
  const u = a.username;
  // Account junk (hiếm khi tới vì TTL ~1 năm): mở lại để quét, reevalAccount sẽ dán junk lại nếu vẫn rác.
  if (a.junk) { await clearJunk(db, u); accCache.delete(u); }
  // Lấy overview + 2 listing song song; overview dùng để đối chiếu số bucket/dataset thực tế.
  const [ov, bk, ds] = await Promise.all([
    req(`${base}/users/${encodeURIComponent(u)}/overview`),
    getAllPages(`${base}/buckets/${encodeURIComponent(u)}`),
    getAllPages(`${base}/datasets?author=${encodeURIComponent(u)}&limit=1000`),
  ]);
  const ovCounts = ov.ok && ov.data && Number.isFinite(Number(ov.data.numDatasets)) && Number.isFinite(Number(ov.data.numBuckets))
    ? { b: Number(ov.data.numBuckets), d: Number(ov.data.numDatasets) } : null;
  if (!a.has_id && a.nrepo === 0 && ovCounts && ovCounts.b + ovCounts.d === 0) {
    await markJunk(db, u); accCache.delete(u);
    log(`junk rong ${u}`);
    if (pending.inv > 0) pending.inv--;
    invSkip++; return;
  }
  const own = (id) => String(id).startsWith(u + "/");
  if (Array.isArray(bk.data)) for (const b of bk.data) if (own(b.id) || isFarm(String(b.id).split("/")[0])) { if (await upsertRepoMeta(db, b.id, u, "bucket", b.updatedAt)) invEnq++; }
  if (Array.isArray(ds.data)) for (const z of ds.data) if (own(z.id) || isFarm(String(z.id).split("/")[0])) { if (await upsertRepoMeta(db, z.id, u, "dataset", z.lastModified)) invEnq++; }
  if (ds.complete && bk.complete) {
    // API có thể trả thiếu mà không báo lỗi: chỉ đánh dấu đã quét khi khớp overview.
    const short = ovCounts && (bk.data.length < ovCounts.b || ds.data.length < ovCounts.d);
    if (short) {
      invFail++;
      invTried.set(u, Date.now() + 10 * 60e3);
      log(`inv thieu ${u} bucket=${bk.data.length}/${ovCounts.b} dataset=${ds.data.length}/${ovCounts.d}`);
    } else {
      await setScanned(db, u);
      await reevalAccount(db, u); // junk nếu hết phim + không còn repo 'new'
      invOk++;
    }
    if (pending.inv > 0) pending.inv--;
  }
  else { invFail++; invTried.set(u, Date.now() + 10 * 60e3); if (pending.inv > 0) pending.inv--; }
}

// ---------------- role: bucket ----------------
// Chỉ gọi /api/buckets/{user} (1 request/account), xoay vòng toàn bộ account active.
// Không phụ thuộc inv TTL nên bucket mới được phát hiện sớm; P4K_BKT_SWEEPS vòng/run, cách P4K_BKT_EVERY ms.
let bktRows = [], bktIdx = 0, bktDone = 0, bktEnq = 0, bktErr = 0, bkt404 = 0, bktBusy = 0, bktSweeps = 0, bktNextAt = 0, bktLoaded = false, bktAtEnd = false, bktLoading = null;
const bktWaiting = () => bktAtEnd && bktRows.length > 0 && bktSweeps > 0 && bktSweeps < BKT_SWEEPS && bktNextAt + 60000 < DEADLINE;
async function loadBkt() {
  const rows = (await db.exec("SELECT username FROM accounts WHERE junk=0 ORDER BY username")).rows.map((r) => r.username);
  bktRows = rows;
  const saved = parseInt((await metaGet(db, "bkt_cursor")) || "0", 10) || 0;
  bktIdx = rows.length ? saved % rows.length : 0;
  bktLoaded = true;
  pending.bkt = rows.length ? rows.length - bktIdx : 0;
  log(`bkt sweep accounts=${rows.length} start=${bktIdx}`);
}
async function ensureBkt() {
  if (!bktLoaded) {
    if (!bktLoading) bktLoading = loadBkt().catch(() => { bktLoading = null; });
    await bktLoading;
  }
  return bktLoaded;
}
async function bktOne(u) {
  const r = await req(`${base}/buckets/${encodeURIComponent(u)}`);
  bktDone++;
  if (!Array.isArray(r.data)) {
    if (r.status === 404) bkt404++; else bktErr++;
    return;
  }
  for (const b of r.data) {
    const id = String(b.id || ""), au = id.split("/")[0];
    if (!id || (au !== u && !isFarm(au))) continue;
    if (await upsertRepoMeta(db, id, u, "bucket", b.updatedAt)) bktEnq++;
  }
}
async function bktWorker() {
  if (!ENABLE_BKT) return;
  while (!stopState.stop) {
    if (!(await ensureBkt())) { await sleep(5000); continue; }
    if (bktAtEnd) {
      // Chờ tới lượt quét kế tiếp; cờ đổi trước await nên chỉ 1 worker mở vòng.
      if (bktWaiting() && Date.now() >= bktNextAt) {
        bktAtEnd = false;
        bktIdx = 0;
        pending.bkt = bktRows.length;
        log(`bkt vong #${bktSweeps + 1} bat dau`);
      } else {
        await sleep(5000);
      }
      continue;
    }
    if (bktIdx >= bktRows.length) {
      if (!bktAtEnd) {
        bktAtEnd = true;
        pending.bkt = 0;
        bktSweeps++;
        bktNextAt = Date.now() + BKT_EVERY_MS;
        try { await metaSet(db, "bkt_cursor", "0"); } catch { }
        log(`bkt xong vong #${bktSweeps} done=${bktDone} enq=${bktEnq} err=${bktErr} 404=${bkt404}`);
      }
      continue;
    }
    const u = bktRows[bktIdx++];
    if (pending.bkt > 0) pending.bkt--;
    bktBusy++;
    try { await bktOne(u); } catch { bktErr++; } finally { bktBusy--; }
    if (bktDone % 250 === 0) {
      try { await metaSet(db, "bkt_cursor", String(bktIdx % bktRows.length)); } catch { }
      log(`bkt done=${bktDone}/${bktRows.length} enq=${bktEnq} err=${bktErr} 404=${bkt404}`);
    }
  }
  try { await metaSet(db, "bkt_cursor", String(bktRows.length ? bktIdx % bktRows.length : 0)); } catch { }
}

// ---------------- role: index ----------------
let idxN = 0, idxInc = 0;
async function indexOne() {
  const now = Date.now();
  // Ưu tiên stale has_id=1 (TTL 1h), hết mới tới has_id=0 (TTL 12h)
  let stale = staleBacklog > 0 ? await claimRepoStale(db, "auto", now - REPO_TTL_HAS_ID_MS, 1) : null;
  if (!stale && staleBacklog > 0) stale = await claimRepoStale(db, "auto", now - REPO_TTL_NO_ID_MS, 0);
  const r = stale || (await claimRepo(db, "auto"));
  if (!r) return false;
  if (stale) { staleBusy++; if (staleBacklog > 0) staleBacklog--; }
  const prev = await getRepoPrev(db, r.id, r.kind);
  const info = await repoInfo(r.kind, r.id);
  let handled = false, changed = false;
  try {
    if (info.ok) {
      if (r.kind !== "bucket" && prev.sha && info.sha) {
        if (prev.sha === info.sha) {
          await touchRepo(db, r.id, r.kind, info.sha, info.lastModified);
          handled = true;
        } else {
          const cmp = await req(`${base}/${plural(r.kind)}/${r.id}/compare/${prev.sha}..${info.sha}`, { text: true });
          if (cmp.ok) {
            const res = await applyGitDiff(db, r.id, r.kind, cmp.text);
            if (res.ok) {
              if (res.ups) wFiles += res.ups;
              const c = await repoCounts(db, r.id, r.kind);
              await finishRepo(db, r.id, r.kind, { n: c.n, size: c.size, sha: info.sha, updated: info.lastModified });
              idxInc++; handled = true; changed = true;
            }
          }
        }
      } else if (r.kind === "bucket" && prev.updated && info.updatedAt && prev.updated === info.updatedAt) {
        await touchRepo(db, r.id, r.kind, null, info.updatedAt);
        handled = true;
      }
    }
    if (!handled) {
      const t = await getTree(r.id, r.kind);
      if (t.ok && Array.isArray(t.data)) {
        const { n, size } = await storeTree(db, r.id, r.kind, t.data, r.account);
        wFiles += n;
        await finishRepo(db, r.id, r.kind, { n, size, entries: t.data.length, sha: info.sha || null, updated: info.lastModified || info.updatedAt || 0 });
        await reevalAccount(db, r.account);
      } else if (t.status === 404 || t.status === 401 || t.status === 403) {
        await finishRepo(db, r.id, r.kind, { dead: true });
        if (r.account) await reevalAccount(db, r.account);
      }
      else { if (stale) releaseStale(db, r.id, r.kind); else await unclaimRepo(db, r.id, r.kind); await sleep(1500); }
    } else if (changed && r.account) await reevalAccount(db, r.account);
  } catch (e) { if (stale) releaseStale(db, r.id, r.kind); else await unclaimRepo(db, r.id, r.kind); throw e; }
  finally { if (stale) staleBusy--; }
  if (++idxN % 20 === 0) log(`index ${idxN} (last ${r.kind}:${r.id}) inc=${idxInc} upd=${stale ? "yes" : "no"}`);
  return true;
}

// ---------------- workers ----------------
const MAXW = Math.max(8, TOTAL);
async function feedWorker() {
  while (!stopState.stop) {
    if (!ENABLE_FEED || pending.feedPages <= 0) { await sleep(1000); continue; }
    await slot("feed");
    try { await feedOne(); } catch (e) { if (e instanceof Stop) { stopState.stop = true; } else log("feed err " + (e.message || e)); } finally { done("feed"); }
  }
}
async function invWorker() {
  while (!stopState.stop) {
    if (!invRows.length || invIdx >= invRows.length) {
      await sleep(1000);
      if (Date.now() - invAt > 30000) await refreshInv();
      continue;
    }
    const a = invRows[invIdx++];
    await slot("inv");
    invBusy.add(a.username);
    try { await invOne(a); } catch (e) { if (e instanceof Stop) { stopState.stop = true; } else { log("inv err " + (e.message || e)); invTried.set(a.username, Date.now() + 10 * 60e3); } } finally { invBusy.delete(a.username); done("inv"); }
    if (++invDone % 100 === 0) log(`inv ${invDone} ok=${invOk} skip=${invSkip} fail=${invFail} enq=${invEnq} | ${stat()}`);
  }
}
async function indexWorker() {
  while (!stopState.stop) {
    if (limits.index < 1 || (pending.index === 0 && staleBacklog === 0)) { await sleep(1000); continue; }
    await slot("index");
    try { if (!(await indexOne())) { done("index"); await sleep(1500); continue; } } catch (e) { if (e instanceof Stop) { stopState.stop = true; } else log("index err " + (e.message || e)); } finally { done("index"); }
  }
}

// ---------------- main ----------------
log(`AUTO start ttl_has_id=${REPO_TTL_HAS_ID_MS/3600000}h ttl_no_id=${REPO_TTL_NO_ID_MS/3600000}h db=${process.env.P4K_DB || "default"} tokens=${lanes.length} gap=${GAP} slots/token=${SLOTS_PER_TOKEN} total=${TOTAL} ttl=${FIXED == null ? "adaptive" : FIXED} feed=${ENABLE_FEED ? (FEED_MAX_PAGES || "inf") : "off"} poll=${POLL_MS ? `${POLL_MS/1000}s/${Math.max(0, Math.round((POLL_END - START) / 1000))}s` : "off"} bkt=${ENABLE_BKT ? `${BKT_WORKERS}x${BKT_SWEEPS}/${BKT_EVERY_MS/1000}s` : "off"} budget=${BUDGET === 1e9 ? "inf" : BUDGET / 1000 + "s"} idle_exit=${IDLE_EXIT / 1000}s`);
await refreshInv();
await rebalance();
let idleTicks = 0, tickN = 0;
const timer = setInterval(async () => {
  tickN++;
  if (Date.now() > DEADLINE) { stopState.stop = true; clearInterval(timer); log("AUTO het budget"); return; }
  try { await rebalance(); } catch (e) { log("sched err " + (e.message || e)); }
  try { await flushTouches(db); } catch (e) { log("touch flush err " + (e.message || e)); }
  if (tickN % 30 === 0) {
    recoverStale(db, 300000).catch(() => {});
    try { staleBacklog = await countStale(db, Date.now() - REPO_TTL_HAS_ID_MS, Date.now() - REPO_TTL_NO_ID_MS); } catch (e) { log("stale resync err " + (e.message || e)); }
  }
  const noWork = pending.inv === 0 && pending.index === 0 && staleBacklog === 0 && pending.feedPages === 0 && pending.bkt === 0 && !feedPolling && !bktWaiting();
  const quiet = inflight.feed + inflight.inv + inflight.index === 0 && bktBusy === 0;
  if (noWork && quiet) {
    idleTicks++;
    if (idleTicks * 10000 >= IDLE_EXIT) { log("AUTO idle-exit: het viec"); stopState.stop = true; clearInterval(timer); }
  } else idleTicks = 0;
}, 10000);
for (const t of ["SIGINT", "SIGTERM"]) process.on(t, () => { log(`AUTO ${t} -> stop`); stopState.stop = true; });
const workers = [];
if (ENABLE_FEED) workers.push(feedWorker());
if (ENABLE_BKT) for (let i = 0; i < BKT_WORKERS; i++) workers.push(bktWorker());
for (let i = 0; i < MAXW; i++) { workers.push(invWorker()); workers.push(indexWorker()); }
try { await Promise.allSettled(workers); } catch (e) { log("AUTO FATAL " + (e && e.stack ? e.stack : e)); process.exit(1); }
clearInterval(timer);
await flushTouches(db);
log(`AUTO done feed pages=${feedPagesAll} enq=${feedEnq} poll=${feedPollN} | inv done=${invDone} ok=${invOk} skip=${invSkip} fail=${invFail} enq=${invEnq} | index n=${idxN} inc=${idxInc} wfiles=${wFiles} stale_left=${staleBacklog} | bkt done=${bktDone} sweeps=${bktSweeps} enq=${bktEnq} err=${bktErr} 404=${bkt404} | ${stat()}`);
log(`AUTO SUMMARY ${JSON.stringify({ elapsed: Math.round((Date.now() - START) / 1000), feed_pages: feedPagesAll, feed_poll: feedPollN, feed_seen: feedSeen, feed_dup: feedDup, feed_nolink: feedNoLink, feed_cursor_lt: feedCurLt, feed_enq: feedEnq, inv_done: invDone, inv_ok: invOk, inv_skip: invSkip, inv_fail: invFail, inv_enq: invEnq, bkt_done: bktDone, bkt_sweeps: bktSweeps, bkt_enq: bktEnq, bkt_err: bktErr, bkt_404: bkt404, index_n: idxN, index_inc: idxInc, wfiles: wFiles, write_cap: WRITE_CAP, req: lanes.reduce((a, t) => a + t.req, 0), n429: lanes.reduce((a, t) => a + t.n429, 0), errors: lanes.reduce((a, t) => a + t.err, 0) })}`);
// STATUS chỉ để theo dõi; GROUP BY state quét covering index toàn bộ repos
// (~242k dòng read mỗi lần — chiếm ~94% reads/run). Cache 6h trong meta: các
// run sau chỉ đọc 1 dòng thay vì quét cả bảng, số liệu lệch tối đa 6h.
let statusRows = null;
try {
  const c = JSON.parse((await metaGet(db, "state_counts_v1")) || "null");
  if (c && Array.isArray(c.rows) && Date.now() - c.at < 6 * 3600e3) statusRows = c.rows;
} catch { }
if (!statusRows) {
  statusRows = await stateCounts(db);
  try { await metaSet(db, "state_counts_v1", JSON.stringify({ at: Date.now(), rows: statusRows })); } catch { }
}
log(`STATUS ${JSON.stringify(statusRows)}`);
process.exit(0);
