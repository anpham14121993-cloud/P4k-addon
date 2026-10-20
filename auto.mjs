// auto.mjs — 1 process, 5 token-lane, 3 role (feed/inv/index) tự cân bằng theo backlog.
// Chạy: HF_TOKEN="t1,t2,t3,t4,t5" node auto.mjs [giây]
// Env: P4K_DB, P4K_GAP (ms/token), P4K_INV_TTL (bỏ trống = TTL thích ứng: has_id 12h / nrepo 7d / còn lại 14d;
//      account junk thử lại sau 90d), P4K_REPO_TTL (ms, mặc định 1 ngày — repo done quá hạn được kiểm tra lại
//      và ưu tiên trước feed), P4K_SLOTS (mặc định 3/token)
const P = await import(process.env.P4K_TURSO === "1" ? "./pdb-turso.mjs" : "./pdb.mjs");
const {
  openDB, ensureSchema, metaGet, metaSet, storeTree, finishRepo, unclaimRepo, reevalAccount,
  upsertRepoMeta, claimRepo, recoverStale, isFarm, isFarmStrict, isId, log, repoCounts,
  applyGitDiff, accountScore, getAccount, insertAccount, setScanned, pendingAccounts, countPending,
  countNew, stateCounts, getRepoPrev, pendingAccountsAdaptive, countPendingAdaptive,
  claimRepoStale, clearJunk,
} = P;

const db = openDB();
await ensureSchema(db);

const TOKENS = String(process.env.HF_TOKEN || "").split(",").map((x) => x.trim()).filter(Boolean);
const GAP = Math.max(30, parseInt(process.env.P4K_GAP || "150", 10));
const SLOTS_PER_TOKEN = Math.max(1, parseInt(process.env.P4K_SLOTS || "3", 10));
const BUDGET = (parseInt(process.argv[2] || process.env.P4K_BUDGET || "21000", 10) || 1e9) * 1000;
const DEADLINE = Date.now() + BUDGET;
const START = Date.now();
const ENABLE_FEED = process.env.P4K_FEED !== "0";
const FEED_MAX_PAGES = parseInt(process.env.P4K_FEED_PAGES || "0", 10);
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
    let best = null, bt = Infinity;
    for (let i = 0; i < lanes.length; i++) {
      const t = lanes[(rr + i) % lanes.length];
      if (t.cool > now) continue;
      const at = Math.max(now, t.next);
      if (at < bt) { bt = at; best = t; }
    }
    if (!best) { await sleep(300); continue; }
    rr = (rr + lanes.indexOf(best) + 1) % lanes.length;
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
let pending = { inv: 0, index: 0, recheck: 0, feedPages: ENABLE_FEED ? 1 : 0 };
let staleBusy = 0;
const stopState = { stop: false };
const REPO_TTL_MS = process.env.P4K_REPO_TTL ? Math.max(3600e3, parseInt(process.env.P4K_REPO_TTL, 10)) : 864e5; // mặc định 1 ngày
async function demand() {
  const now = Date.now();
  pending.inv = FIXED == null ? await countPendingAdaptive(db, now) : await countPending(db, FIXED, false);
  pending.index = await countNew(db);
}
async function rebalance() {
  await demand();
  pending.recheck = staleBusy > 0 ? 1 : 0;
  const wInv = pending.inv > 0 ? 4 * Math.min(1, pending.inv / 2000) : 0;
  const wIdx = pending.index > 0 || staleBusy > 0 ? 2.6 * Math.min(1, Math.max(pending.index, staleBusy) / 200) : 0;
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
  log(`sched slots=${TOTAL} feed=${lFeed} inv=${lInv} index=${lIdx} | backlog feed=${pending.feedPages}p inv=${pending.inv} index=${pending.index} recheck=${staleBusy} | ${stat()}`);
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
let feedCursor = (await metaGet(db, "feed_cursor")) || null;
let feedPages = 0, feedSeen = 0, feedEnq = 0, feedShape = 0, feedStyle = 0, feedAtEnd = false;
const accCache = new Map();
async function accOf(u) {
  if (!accCache.has(u)) accCache.set(u, (await getAccount(db, u)) || null);
  return accCache.get(u);
}
async function feedOne() {
  if (!ENABLE_FEED || feedAtEnd) { await sleep(1000); return; }
  if (FEED_MAX_PAGES > 0 && feedPages >= FEED_MAX_PAGES) { feedAtEnd = true; pending.feedPages = 0; log(`feed dat gioi han ${FEED_MAX_PAGES} trang`); return; }
  const url = `${base}/datasets?limit=100&sort=lastModified` + (feedCursor ? `&cursor=${encodeURIComponent(feedCursor)}` : "");
  const j = await req(url);
  if (!j.ok || !Array.isArray(j.data)) { pending.feedPages = 0; log("feed stop status=" + j.status); await waitStop(30000); pending.feedPages = 1; return; }
  if (!j.data.length) { feedCursor = null; await metaSet(db, "feed_cursor", ""); feedAtEnd = true; pending.feedPages = 0; log("feed het cursor, nghi 300s"); await waitStop(300000); feedAtEnd = false; pending.feedPages = ENABLE_FEED ? 1 : 0; return; }
  for (const x of j.data) {
    feedSeen++;
    const id = String(x.id), author = id.split("/")[0];
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
    const take = isSelf || (oneWord && (lowerNum || hasId)) || (capWord && hasId);
    if (!take) { feedShape++; continue; }
    await insertAccount(db, author);
    accCache.delete(author);
    if (await upsertRepoMeta(db, id, author, "dataset", x.lastModified)) feedEnq++;
  }
  feedPages++;
  feedCursor = nextCursor(j.link);
  await metaSet(db, "feed_cursor", feedCursor || "");
  if (FEED_MAX_PAGES > 0 && feedPages >= FEED_MAX_PAGES) { feedAtEnd = true; pending.feedPages = 0; log(`feed dat gioi han ${FEED_MAX_PAGES} trang`); }
  if (feedPages % 20 === 0) log(`feed pages=${feedPages} seen=${feedSeen} enq=${feedEnq} shape=${feedShape} style=${feedStyle}`);
}

// ---------------- role: inv ----------------
let invRows = [], invAt = 0, invIdx = 0, invDone = 0, invOk = 0, invSkip = 0, invFail = 0, invEnq = 0;
const invTried = new Map();
const invBusy = new Set();
async function refreshInv() {
  const now = Date.now();
  const all = FIXED == null ? await pendingAccountsAdaptive(db, now) : await pendingAccounts(db, FIXED, false);
  invRows = all.filter((a) => !invBusy.has(a.username) && (!invTried.has(a.username) || invTried.get(a.username) <= now));
  const key = (x) => (x.junk ? 1e6 : accountScore(x.username, x)); // account junk thử lại được ưu tiên trước
  invRows.sort((a, b) => key(b) - key(a) || a.scanned_at - b.scanned_at);
  invIdx = 0; invAt = now;
  if (invRows.length && invDone % 100 !== 0) log(`inv refresh todo=${invRows.length}`);
}
async function invOne(a) {
  const u = a.username;
  if (!a.has_id && a.nrepo === 0) {
    const ov = await req(`${base}/users/${encodeURIComponent(u)}/overview`);
    if (ov.ok && ov.data && Number(ov.data.numDatasets) + Number(ov.data.numBuckets) === 0) {
      if (a.junk) { await clearJunk(db, u); accCache.delete(u); log(`bo junk ${u}`); }
      await setScanned(db, u);
      invSkip++; return;
    }
  }
  const bk = await getAllPages(`${base}/buckets/${encodeURIComponent(u)}`);
  if (Array.isArray(bk.data)) for (const b of bk.data) if (isFarm(String(b.id).split("/")[0])) { if (await upsertRepoMeta(db, b.id, u, "bucket", b.updatedAt)) invEnq++; }
  const ds = await getAllPages(`${base}/datasets?author=${encodeURIComponent(u)}&limit=1000`);
  if (Array.isArray(ds.data)) for (const z of ds.data) if (isFarm(String(z.id).split("/")[0])) { if (await upsertRepoMeta(db, z.id, u, "dataset", z.lastModified)) invEnq++; }
  if (ds.complete && bk.complete) {
    if (a.junk) { await clearJunk(db, u); accCache.delete(u); log(`bo junk ${u}`); }
    await setScanned(db, u); invOk++;
  }
  else { invFail++; invTried.set(u, Date.now() + 10 * 60e3); }
}

// ---------------- role: index ----------------
let idxN = 0, idxInc = 0;
async function indexOne() {
  const stale = await claimRepoStale(db, "auto", Date.now() - REPO_TTL_MS);
  const r = stale || (await claimRepo(db, "auto"));
  if (!r) return false;
  if (stale) staleBusy++;
  const prev = await getRepoPrev(db, r.id, r.kind);
  const info = await repoInfo(r.kind, r.id);
  let handled = false;
  try {
    if (info.ok) {
      if (r.kind !== "bucket" && prev.sha && info.sha) {
        if (prev.sha === info.sha) {
          const c = await repoCounts(db, r.id, r.kind);
          await finishRepo(db, r.id, r.kind, { n: c.n, size: c.size, sha: info.sha, updated: info.lastModified });
          handled = true;
        } else {
          const cmp = await req(`${base}/${plural(r.kind)}/${r.id}/compare/${prev.sha}..${info.sha}`, { text: true });
          if (cmp.ok) {
            const res = await applyGitDiff(db, r.id, r.kind, cmp.text);
            if (res.ok) {
              const c = await repoCounts(db, r.id, r.kind);
              await finishRepo(db, r.id, r.kind, { n: c.n, size: c.size, sha: info.sha, updated: info.lastModified });
              idxInc++; handled = true;
            }
          }
        }
      } else if (r.kind === "bucket" && prev.updated && info.updatedAt && prev.updated === info.updatedAt) {
        const c = await repoCounts(db, r.id, r.kind);
        await finishRepo(db, r.id, r.kind, { n: c.n, size: c.size, updated: info.updatedAt });
        handled = true;
      }
    }
    if (!handled) {
      const t = await getTree(r.id, r.kind);
      if (t.ok && Array.isArray(t.data)) {
        const { n, size } = await storeTree(db, r.id, r.kind, t.data, r.account);
        await finishRepo(db, r.id, r.kind, { n, size, entries: t.data.length, sha: info.sha || null, updated: info.lastModified || info.updatedAt || 0 });
        await reevalAccount(db, r.account);
      } else if (t.status === 404 || t.status === 401 || t.status === 403) await finishRepo(db, r.id, r.kind, { dead: true });
      else { await unclaimRepo(db, r.id, r.kind); await sleep(1500); }
    } else await reevalAccount(db, r.account);
  } catch (e) { await unclaimRepo(db, r.id, r.kind); throw e; }
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
    if (limits.index < 1 || pending.index === 0) { await sleep(1000); continue; }
    await slot("index");
    try { if (!(await indexOne())) { done("index"); await sleep(1500); continue; } } catch (e) { if (e instanceof Stop) { stopState.stop = true; } else log("index err " + (e.message || e)); } finally { done("index"); }
  }
}

// ---------------- main ----------------
log(`AUTO start repo_ttl=${(REPO_TTL_MS / 864e5).toFixed(1)}d db=${process.env.P4K_DB || "default"} tokens=${lanes.length} gap=${GAP} slots/token=${SLOTS_PER_TOKEN} total=${TOTAL} ttl=${FIXED == null ? "adaptive" : FIXED} feed=${ENABLE_FEED ? (FEED_MAX_PAGES || "inf") : "off"} budget=${BUDGET === 1e9 ? "inf" : BUDGET / 1000 + "s"} idle_exit=${IDLE_EXIT / 1000}s`);
await refreshInv();
await rebalance();
let idleTicks = 0, tickN = 0;
const timer = setInterval(async () => {
  tickN++;
  if (Date.now() > DEADLINE) { stopState.stop = true; clearInterval(timer); log("AUTO het budget"); return; }
  try { await rebalance(); } catch (e) { log("sched err " + (e.message || e)); }
  if (tickN % 30 === 0) recoverStale(db, 300000).catch(() => {});
  const noWork = pending.inv === 0 && pending.index === 0 && pending.feedPages === 0;
  const quiet = inflight.feed + inflight.inv + inflight.index === 0;
  if (noWork && quiet) {
    idleTicks++;
    if (idleTicks * 10000 >= IDLE_EXIT) { log("AUTO idle-exit: het viec"); stopState.stop = true; clearInterval(timer); }
  } else idleTicks = 0;
}, 10000);
for (const t of ["SIGINT", "SIGTERM"]) process.on(t, () => { log(`AUTO ${t} -> stop`); stopState.stop = true; });
const workers = [];
if (ENABLE_FEED) workers.push(feedWorker());
for (let i = 0; i < MAXW; i++) { workers.push(invWorker()); workers.push(indexWorker()); }
try { await Promise.allSettled(workers); } catch (e) { log("AUTO FATAL " + (e && e.stack ? e.stack : e)); process.exit(1); }
clearInterval(timer);
log(`AUTO done feed pages=${feedPages} seen=${feedSeen} enq=${feedEnq} | inv done=${invDone} ok=${invOk} skip=${invSkip} fail=${invFail} enq=${invEnq} | index n=${idxN} inc=${idxInc} | ${stat()}`);
log(`AUTO SUMMARY ${JSON.stringify({ elapsed: Math.round((Date.now() - START) / 1000), feed_pages: feedPages, feed_seen: feedSeen, feed_enq: feedEnq, inv_done: invDone, inv_ok: invOk, inv_skip: invSkip, inv_fail: invFail, inv_enq: invEnq, index_n: idxN, index_inc: idxInc, req: lanes.reduce((a, t) => a + t.req, 0), n429: lanes.reduce((a, t) => a + t.n429, 0), errors: lanes.reduce((a, t) => a + t.err, 0) })}`);
log(`STATUS ${JSON.stringify(await stateCounts(db))}`);
process.exit(0);
