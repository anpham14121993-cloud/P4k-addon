// fullscan-feed.mjs — Quét lại toàn bộ feed HuggingFace theo bộ lọc hiện tại của auto.mjs.
// Dùng pool đa HF_TOKEN (5 token trên GitHub Secrets), vừa phân trang feed vừa kiểm tra
// account (overview + datasets + buckets) và nạp trực tiếp repo/file vào Oracle/Turso DB.
import {
  openDB, ensureSchema, metaGet, metaSet, storeTree, applyGitDiff, repoCounts,
  markJunk, insertAccount, isFarm, isFarmStrict, isInvalidFarmName, isPureLowerFarm,
  isId, repoScore, log, sleep, JUNK_RETRY_MS,
} from "./pdb-turso.mjs";

const db = openDB();
for (let attempt = 1; attempt <= 3; attempt++) {
  try {
    await ensureSchema(db);
    break;
  } catch (e) {
    if (attempt === 3) log("ensureSchema warning: " + (e.message || e));
    else await sleep(2000);
  }
}

const TOKENS = String(process.env.HF_TOKEN || "").split(",").map((x) => x.trim()).filter(Boolean);
const GAP = Math.max(30, parseInt(process.env.P4K_GAP || "160", 10));
const BUDGET = Math.max(60, parseInt(process.argv[2] || process.env.P4K_BUDGET || "18000", 10)) * 1000;
const START = Date.now();
const DEADLINE = START + BUDGET;
const FEED_CUTOFF = DEADLINE - Math.min(180_000, Math.floor(BUDGET * 0.15));
const FEED_LIMIT = Math.max(1, Math.min(1000, parseInt(process.env.P4K_FEED_LIMIT || "1000", 10)));
const MAX_PAGES = Math.max(0, parseInt(process.env.P4K_MAX_PAGES || "0", 10));
const RESUME = process.env.P4K_RESUME === "1" || process.env.P4K_RESUME === "true";
const RECHECK_DEAD = process.env.P4K_RECHECK_DEAD !== "0" && process.env.P4K_RECHECK_DEAD !== "false";
const BKT_EXTRA = process.env.P4K_BKT_EXTRA !== "0" && process.env.P4K_BKT_EXTRA !== "false";
const DRY_RUN = process.env.P4K_DRY_RUN === "1" || process.env.P4K_DRY_RUN === "true";
const ACC_WORKERS = Math.max(2, parseInt(process.env.P4K_ACC_WORKERS || "6", 10));
const PROBE_CONCURRENCY = Math.max(2, parseInt(process.env.P4K_PROBE_CONCURRENCY || "8", 10));
const MAX_ACC_QUEUE = Math.max(50, parseInt(process.env.P4K_MAX_ACC_QUEUE || "120", 10));

const REPO_TTL_HAS_MS = Math.max(600000, parseInt(process.env.P4K_REPO_TTL || "21600000", 10));
const REPO_TTL_NOID_MS = Math.max(600000, parseInt(process.env.P4K_REPO_TTL_NO_ID || "604800000", 10));
const JITTER_MS = 3600000;
const h32 = (s) => {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); }
  return h >>> 0;
};
const computeDueAt = (hasId, key) => Date.now() + (hasId ? REPO_TTL_HAS_MS : REPO_TTL_NOID_MS) - (h32(key) % JITTER_MS);

const META_CURSOR_KEY = "fullscan_feed_cursor";
const META_STATE_KEY = "fullscan_feed_state";
const BASE = "https://huggingface.co/api";
const AI_TAG_RE = /^(?:task_categories|language|license|library|size_categories|format|modality|arxiv):/;
const AI_SLUG_RE = /(?:^|-)(?:ai|ml|dl|llm|gpt|rag|nlp|bench|benchmark|eval|dataset|datasets|model|models|corpus|results|runs|notes|status|logs|backup|train|pretrain|sft|dpo|rlhf|test|data|code|docs)(?:-|$)/;

class Stop extends Error {}

// ---------------- pool token HF: phân tải tròn + cooldown riêng mỗi token ----------------
const lanes = (TOKENS.length ? TOKENS : [null]).map((tok) => ({ tok, next: 0, cool: 0, req: 0, n429: 0, err: 0 }));
let rr = 0;

function laneStat() {
  return lanes.map((t, i) => `T${i + 1}:${t.req}${t.n429 ? "/429=" + t.n429 : ""}${t.err ? "/e=" + t.err : ""}`).join(" ");
}

async function takeToken() {
  for (;;) {
    const now = Date.now();
    if (now > DEADLINE) throw new Stop();
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
    best.next = bt + GAP;
    if (bt > now) await sleep(Math.min(bt - now, Math.max(0, DEADLINE - Date.now())));
    if (Date.now() > DEADLINE) throw new Stop();
    if (best.cool > Date.now()) continue;
    best.req++;
    return best;
  }
}

function parseResetMs(headers) {
  const ra = parseInt(headers.get("retry-after") || "0", 10);
  if (ra > 0) return ra * 1000;
  const rl = headers.get("ratelimit") || "";
  const mt = rl.match(/\bt=(\d+)/);
  if (mt) return (Math.min(120, Math.max(1, parseInt(mt[1], 10))) + 1) * 1000;
  return 15000;
}

async function req(u, { text = false, timeout = 30000 } = {}) {
  for (let a = 0; a < 40; a++) {
    const t = await takeToken();
    try {
      const r = await fetch(u, {
        headers: t.tok ? { Accept: "application/json", Authorization: "Bearer " + t.tok } : { Accept: "application/json" },
        signal: AbortSignal.timeout(timeout),
      });
      const rl = r.headers.get("ratelimit") || "";
      const mr = rl.match(/\br=(\d+)/);
      if (r.status === 429) {
        t.cool = Date.now() + parseResetMs(r.headers);
        t.n429++;
        continue;
      }
      // Chủ động né trước khi cạn quota cửa sổ nếu token có header ratelimit
      if (mr && parseInt(mr[1], 10) <= 1) {
        t.cool = Math.max(t.cool, Date.now() + parseResetMs(r.headers));
      }
      if (r.status === 404 || r.status === 401 || r.status === 403) return { ok: false, status: r.status };
      if (!r.ok) { t.err++; await sleep(900); continue; }
      const body = await r.text();
      if (text) return { ok: true, text: body };
      let s = body;
      const mk = s.indexOf("Markdown Content:");
      if (mk >= 0) { const jb = s.indexOf("{", mk), ab = s.indexOf("[", mk); s = s.slice((jb >= 0 && (ab < 0 || jb < ab)) ? jb : ab); }
      try { return { ok: true, data: JSON.parse(s), link: r.headers.get("link") || "" }; } catch { t.err++; await sleep(200); }
    } catch (e) {
      if (e instanceof Stop) throw e;
      t.err++;
      await sleep(900);
    }
  }
  return { ok: false, status: 0 };
}

const nextLink = (link) => {
  for (const p of (link || "").split(",")) {
    const m = p.match(/<([^>]+)>;\s*rel="next"/);
    if (m) return m[1];
  }
  return null;
};
const nextCursor = (link) => {
  const m = (link || "").match(/cursor=([^&>]+)[^>]*>;\s*rel="next"/);
  return m ? decodeURIComponent(m[1]) : null;
};

async function getAllPages(url) {
  const out = [];
  let u = url;
  for (let p = 0; p < 200; p++) {
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

async function getTree(id, kind, maxPages = 60) {
  let u = BASE + (kind === "bucket" ? `/buckets/${id}/tree?recursive=true` : `/datasets/${id}/tree/main?recursive=true`);
  const out = [];
  for (let p = 0; p < maxPages; p++) {
    const r = await req(u);
    if (!r.ok || !Array.isArray(r.data)) return { ok: false, status: r.status };
    out.push(...r.data);
    const nx = nextLink(r.link);
    if (!nx) return { ok: true, data: out };
    u = nx;
  }
  return { ok: true, data: out };
}

async function probeDatasetForId(id) {
  stats.feedProbed++;
  const r = await req(`${BASE}/datasets/${id}/tree/main`, { timeout: 15000 });
  if (!r.ok || !Array.isArray(r.data)) return false;
  return r.data.some((f) => f.type === "file" && isId(f.path));
}

// ---------------- thống kê & trạng thái toàn phiên ----------------
const stats = {
  feedPages: 0,
  feedSeen: 0,
  feedDup: 0,
  feedStyle: 0,
  feedShape: 0,
  feedProbed: 0,
  feedProbeHit: 0,
  feedNoLink: 0,
  accQueued: 0,
  accDone: 0,
  accSkipEmpty: 0,
  accErr: 0,
  accHasId: 0,
  accNewHasId: 0,
  accJunk: 0,
  bktExtraDone: 0,
  repoTotal: 0,
  repoUnchanged: 0,
  repoInc: 0,
  repoTree: 0,
  repoDead: 0,
  repoHasId: 0,
  filesUpserted: 0,
  filesDeleted: 0,
};

const feedIds = new Set();
const accMem = new Map();
const accHandled = new Set();
const repoHandled = new Set();
let feedDone = false;
let reachedEnd = false;
let lastLmRange = "-";
let safeCursor = RESUME ? ((await metaGet(db, META_CURSOR_KEY)) || null) : null;
let stopRequested = false;
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    log(`FULLSCAN ${sig} -> dừng an toàn`);
    stopRequested = true;
  });
}

async function loadAccountsBatch(authors) {
  const missing = authors.filter((u) => !accMem.has(u));
  for (let i = 0; i < missing.length; i += 100) {
    const ch = missing.slice(i, i + 100);
    const ph = ch.map(() => "?").join(",");
    const q = await db.exec(
      `SELECT username, has_id, junk, nrepo, scanned_at, verified FROM accounts WHERE username IN (${ph})`,
      ch
    );
    const found = new Set();
    for (const r of q.rows) {
      accMem.set(r.username, r);
      found.add(r.username);
    }
    for (const u of ch) if (!found.has(u)) accMem.set(u, null);
  }
  if (accMem.size > 25000) {
    // Giữ cache gọn để tránh tốn RAM ở các trang về sau
    let drop = accMem.size - 15000;
    for (const k of accMem.keys()) {
      accMem.delete(k);
      if (--drop <= 0) break;
    }
  }
}

async function saveRepoRow({ id, kind, account, size, nfile, lastModified, state, hasId, entries, sha, updated, prio, junk, dueAt }) {
  if (DRY_RUN) return;
  const now = Date.now();
  await db.exec(
    `INSERT INTO repos(id, kind, account, size, nfile, last_modified, state, has_id, entries, indexed_at, worker, sha, updated, prio, junk, due_at)
     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
     ON CONFLICT(id, kind) DO UPDATE SET
       account=excluded.account,
       size=excluded.size,
       nfile=excluded.nfile,
       last_modified=CASE WHEN excluded.last_modified > COALESCE(repos.last_modified, 0) THEN excluded.last_modified ELSE repos.last_modified END,
       state=excluded.state,
       has_id=excluded.has_id,
       entries=excluded.entries,
       indexed_at=excluded.indexed_at,
       worker=NULL,
       sha=COALESCE(excluded.sha, repos.sha),
       updated=CASE WHEN excluded.updated > COALESCE(repos.updated, 0) THEN excluded.updated ELSE repos.updated END,
       prio=excluded.prio,
       junk=excluded.junk,
       due_at=excluded.due_at`,
    [
      id, kind, account, size || 0, nfile || 0, lastModified || 0,
      state, hasId ? 1 : 0, entries || 0, now, null,
      sha || null, updated || 0, prio || 0, junk ? 1 : 0, dueAt || 0,
    ]
  );
}

async function processSingleRepo(item, u, prev, accForScore) {
  const rKey = item.kind + ":" + item.id;
  if (repoHandled.has(rKey)) return prev ? !!prev.has_id : false;
  repoHandled.add(rKey);
  if (repoHandled.size > 120000) repoHandled.clear();
  stats.repoTotal++;

  const remoteLm = item.remoteLm || 0;
  const remoteSha = item.remoteSha || null;

  // 1. Bỏ qua /tree nếu chắc chắn chưa thay đổi
  if (item.kind === "bucket") {
    if (item.totalFiles === 0) {
      if (!prev || prev.state !== "done" || prev.nfile !== 0) {
        await saveRepoRow({
          id: item.id, kind: "bucket", account: u, size: 0, nfile: 0, lastModified: remoteLm,
          state: "done", hasId: 0, entries: 0, sha: null, updated: remoteLm,
          prio: repoScore(item.id, u, "bucket", accForScore),
          junk: prev?.junk ?? (accForScore.junk ? 1 : 0),
          dueAt: computeDueAt(false, rKey),
        });
      }
      stats.repoUnchanged++;
      return false;
    }
    if (prev?.state === "done" && prev.updated && remoteLm && prev.updated === remoteLm) {
      stats.repoUnchanged++;
      return !!prev.has_id;
    }
  } else {
    if (prev?.state === "done" && prev.sha && remoteSha && prev.sha === remoteSha) {
      if (!DRY_RUN && remoteLm > (prev.last_modified || 0)) {
        await db.exec(
          "UPDATE repos SET last_modified=?, due_at=? WHERE id=? AND kind='dataset'",
          [remoteLm, computeDueAt(!!prev.has_id, rKey), item.id]
        );
      }
      stats.repoUnchanged++;
      return !!prev.has_id;
    }
    if (prev?.state === "dead") {
      const lmBumped = remoteLm > (prev.last_modified || 0);
      if (!RECHECK_DEAD && !lmBumped) {
        stats.repoUnchanged++;
        return false;
      }
      if (prev.indexed_at > 0 && !lmBumped) {
        stats.repoUnchanged++;
        return false;
      }
    }
  }

  // 2. Nếu là dataset đã done & có cả prev.sha lẫn remoteSha khác nhau -> thử git diff trước
  if (item.kind === "dataset" && prev?.state === "done" && prev.sha && remoteSha && prev.sha !== remoteSha) {
    const cmp = await req(`${BASE}/datasets/${item.id}/compare/${prev.sha}..${remoteSha}`, { text: true });
    if (cmp.ok) {
      if (!DRY_RUN) {
        const res = await applyGitDiff(db, item.id, "dataset", cmp.text);
        if (res.ok) {
          stats.filesUpserted += res.ups || 0;
          stats.filesDeleted += res.dels || 0;
          const c = await repoCounts(db, item.id, "dataset");
          const hasId = c.n > 0 ? 1 : 0;
          await saveRepoRow({
            id: item.id, kind: "dataset", account: u, size: c.size, nfile: c.n, lastModified: remoteLm,
            state: "done", hasId, entries: prev.entries || 0, sha: remoteSha, updated: remoteLm,
            prio: repoScore(item.id, u, "dataset", { ...accForScore, has_id: hasId || accForScore.has_id }),
            junk: hasId ? 0 : (prev.junk ?? (accForScore.junk ? 1 : 0)),
            dueAt: computeDueAt(!!hasId, rKey),
          });
          stats.repoInc++;
          if (hasId) {
            stats.repoHasId++;
            log(`+PHIM (diff) ${rKey} nfile=${c.n} (+${res.ups}/-${res.dels})`);
          }
          return !!hasId;
        }
      } else {
        stats.repoInc++;
        return !!prev.has_id;
      }
    }
  }

  // 3. Lấy full tree đệ quy từ HF (repo chưa has_id giới hạn 60 trang = 60k file tránh kẹt vào kho ML khổng lồ)
  const t = await getTree(item.id, item.kind, prev?.has_id ? 500 : 60);
  if (t.status === 404 || t.status === 401 || t.status === 403) {
    if (!DRY_RUN && prev?.nfile > 0) {
      const delRes = await db.exec("DELETE FROM files WHERE repo_id=? AND kind=?", [item.id, item.kind]);
      stats.filesDeleted += delRes.changes || 0;
    }
    await saveRepoRow({
      id: item.id, kind: item.kind, account: u, size: 0, nfile: 0, lastModified: remoteLm,
      state: "dead", hasId: 0, entries: 0, sha: prev?.sha || remoteSha, updated: prev?.updated || remoteLm,
      prio: repoScore(item.id, u, item.kind, accForScore),
      junk: prev?.junk ?? (accForScore.junk ? 1 : 0),
      dueAt: 0,
    });
    stats.repoDead++;
    return false;
  }

  if (!t.ok || !Array.isArray(t.data)) return prev ? !!prev.has_id : false;

  stats.repoTree++;
  if (DRY_RUN) {
    let n = 0;
    for (const f of t.data) if ((!f.type || f.type === "file") && isId(f.path || f.name)) n++;
    if (n > 0) stats.repoHasId++;
    return n > 0;
  }

  const isNew = !prev || !prev.nfile;
  const { n, size, del, ins, upd } = await storeTree(db, item.id, item.kind, t.data, u, isNew);
  stats.filesUpserted += (ins || 0) + (upd || 0);
  stats.filesDeleted += del || 0;
  const hasId = n > 0 ? 1 : 0;
  await saveRepoRow({
    id: item.id, kind: item.kind, account: u, size, nfile: n, lastModified: remoteLm,
    state: "done", hasId, entries: t.data.length,
    sha: remoteSha || prev?.sha || null,
    updated: remoteLm || prev?.updated || 0,
    prio: repoScore(item.id, u, item.kind, { ...accForScore, has_id: hasId || accForScore.has_id }),
    junk: hasId ? 0 : (prev?.junk ?? (accForScore.junk ? 1 : 0)),
    dueAt: computeDueAt(!!hasId, rKey),
  });
  if (hasId) {
    stats.repoHasId++;
    if (ins > 0 || upd > 0 || !prev?.has_id) {
      log(`+PHIM ${rKey} files=${n} (+${ins} ins, ~${upd} upd, -${del} del)`);
    }
  }
  return !!hasId;
}

async function processAccountJob(job) {
  const { author: u, seedItems } = job;
  let acc = job.acc;
  try {
    if (!acc && !DRY_RUN) {
      await insertAccount(db, u);
      acc = { username: u, has_id: 0, junk: 0, nrepo: 0, scanned_at: 0, verified: 0 };
      accMem.set(u, acc);
    }

    const [ov, bk] = await Promise.all([
      req(`${BASE}/users/${encodeURIComponent(u)}/overview`),
      getAllPages(`${BASE}/buckets/${encodeURIComponent(u)}`),
    ]);

    const ovCounts = ov.ok && ov.data && Number.isFinite(Number(ov.data.numDatasets)) && Number.isFinite(Number(ov.data.numBuckets))
      ? { b: Number(ov.data.numBuckets), d: Number(ov.data.numDatasets) } : null;

    if (!acc?.has_id && (acc?.nrepo || 0) === 0 && ovCounts && ovCounts.b + ovCounts.d === 0) {
      if (!DRY_RUN) await markJunk(db, u);
      stats.accSkipEmpty++;
      stats.accDone++;
      return;
    }

    // Batch-load mọi repo hiện có của account u (1 query duy nhất)
    const prevMap = new Map();
    const qPrev = await db.exec(
      "SELECT id, kind, state, sha, updated, nfile, size, entries, last_modified, has_id, junk, indexed_at, prio FROM repos WHERE account=?",
      [u]
    );
    for (const r of qPrev.rows) prevMap.set(r.kind + ":" + r.id, r);

    const own = (id) => String(id).startsWith(u + "/");
    const bucketItems = [];
    const seenKeys = new Set();
    if (Array.isArray(bk.data)) {
      for (const b of bk.data) {
        const id = String(b.id || "");
        if (!id || (!own(id) && !isFarm(id.split("/")[0]))) continue;
        const k = "bucket:" + id;
        if (seenKeys.has(k)) continue;
        seenKeys.add(k);
        bucketItems.push({
          kind: "bucket", id, totalFiles: b.totalFiles ?? null,
          remoteLm: b.updatedAt ? Date.parse(b.updatedAt) || 0 : 0, remoteSha: null,
        });
      }
    }

    const accForScore = acc || { has_id: 0, junk: 0, nrepo: Math.max(ovCounts?.d || 0, bucketItems.length) };
    let anyHasId = false;

    for (const it of bucketItems) {
      if (stopRequested) break;
      const prev = prevMap.get(it.kind + ":" + it.id) || null;
      if (await processSingleRepo(it, u, prev, accForScore)) anyHasId = true;
    }

    // Theo đúng rule auto.mjs (nrepo >= 20 && !has_id -> loại): nếu overview báo >= 20 datasets
    // mà không phải account đã có phim (kể cả trong bucket vừa quét) và không có repo được probe trúng -> junk luôn.
    const hasProbedHit = seedItems.some((s) => s.probedOk);
    if (!acc?.has_id && !anyHasId && !hasProbedHit && ovCounts && ovCounts.d >= 20) {
      const actualNrepo = Math.max(acc?.nrepo || 0, ovCounts.d + bucketItems.length);
      if (!DRY_RUN) {
        const now = Date.now();
        await db.exec(
          "UPDATE accounts SET junk=1, nrepo=?, scanned_at=?, next_scan_at=? WHERE username=?",
          [actualNrepo, now, now + JUNK_RETRY_MS, u]
        );
        await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=1 WHERE account=? AND junk=0", [u]);
        accMem.set(u, { username: u, has_id: 0, junk: 1, nrepo: actualNrepo, scanned_at: now, verified: acc?.verified || 0 });
      }
      stats.accJunk++;
      stats.accDone++;
      return;
    }

    const ds = await getAllPages(`${BASE}/datasets?author=${encodeURIComponent(u)}&limit=1000`);
    const datasetItems = [];
    if (Array.isArray(ds.data)) {
      for (const z of ds.data) {
        const id = String(z.id || "");
        if (!id || (!own(id) && !isFarm(id.split("/")[0]))) continue;
        const k = "dataset:" + id;
        if (seenKeys.has(k)) continue;
        seenKeys.add(k);
        datasetItems.push({
          kind: "dataset", id, remoteSha: z.sha || null,
          remoteLm: z.lastModified ? Date.parse(z.lastModified) || 0 : 0,
        });
      }
    }

    // Phòng hờ trường hợp dataset từ feed chưa nằm trong ?author=
    for (const s of seedItems) {
      const k = "dataset:" + s.id;
      if (seenKeys.has(k)) continue;
      seenKeys.add(k);
      datasetItems.push({ kind: "dataset", id: s.id, remoteSha: s.sha || null, remoteLm: s.lm || 0 });
    }

    // Kiểm tra tối đa 3 dataset đồng thời trong 1 account qua pool 5 token
    let dsIdx = 0;
    const dsConc = Math.min(3, Math.max(1, datasetItems.length));
    await Promise.all(Array.from({ length: dsConc }, async () => {
      while (!stopRequested) {
        const i = dsIdx++;
        if (i >= datasetItems.length) break;
        const it = datasetItems[i];
        const prev = prevMap.get(it.kind + ":" + it.id) || null;
        if (await processSingleRepo(it, u, prev, accForScore)) anyHasId = true;
      }
    }));

    const items = [...bucketItems, ...datasetItems];

    // Chốt trạng thái tài khoản cleanly (không lật junk hàng loạt lên tài khoản rác)
    if (!DRY_RUN && ds.complete && bk.complete) {
      const now = Date.now();
      const short = ovCounts && (bk.data.length < ovCounts.b || ds.data.length < ovCounts.d);
      if (!short) {
        const hitRow = anyHasId
          ? { rows: [1] }
          : await db.exec("SELECT 1 x FROM repos WHERE account=? AND has_id=1 LIMIT 1", [u]);
        const hasMovies = hitRow.rows.length > 0;
        const actualNrepo = Math.max(acc?.nrepo || 0, items.length);

        if (hasMovies) {
          stats.accHasId++;
          if (!acc?.has_id || acc?.junk) {
            stats.accNewHasId++;
            log(`★ MỞ KHÓA ACCOUNT CÓ PHIM: ${u} (repos=${actualNrepo}, wasJunk=${acc?.junk || 0})`);
          }
          await db.exec(
            "UPDATE accounts SET verified=1, has_id=1, junk=0, nrepo=?, scanned_at=?, next_scan_at=? WHERE username=?",
            [actualNrepo, now, now + 18000000, u]
          );
          await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=0 WHERE account=? AND junk=1", [u]);
          accMem.set(u, { username: u, has_id: 1, junk: 0, nrepo: actualNrepo, scanned_at: now, verified: 1 });
        } else {
          const pendingWork = await db.exec("SELECT 1 x FROM repos WHERE account=? AND state IN ('new','fetching') LIMIT 1", [u]);
          if (!pendingWork.rows.length) {
            stats.accJunk++;
            await db.exec(
              "UPDATE accounts SET junk=1, nrepo=?, scanned_at=?, next_scan_at=? WHERE username=?",
              [actualNrepo, now, now + JUNK_RETRY_MS, u]
            );
            await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=1 WHERE account=? AND junk=0", [u]);
            accMem.set(u, { username: u, has_id: 0, junk: 1, nrepo: actualNrepo, scanned_at: now, verified: acc?.verified || 0 });
          } else {
            await db.exec("UPDATE accounts SET nrepo=?, scanned_at=?, next_scan_at=? WHERE username=?", [actualNrepo, now, now + 604800000, u]);
          }
        }
      }
    } else if (!ds.complete || !bk.complete) {
      accHandled.delete(u);
    }
    stats.accDone++;
  } catch (e) {
    if (e instanceof Stop) throw e;
    stats.accErr++;
    log(`acc err ${u}: ${e.message || e}`);
  } finally {
    if (job.pageTracker) {
      job.pageTracker.remaining--;
      advanceWatermark();
    }
  }
}

// ---------------- watermark cursor an toàn khi feed + workers chạy song song ----------------
const pendingPages = [];
let watermarkDirty = false;
function advanceWatermark() {
  while (pendingPages.length && pendingPages[0].remaining <= 0) {
    const donePage = pendingPages.shift();
    safeCursor = donePage.cursorNext || "";
    watermarkDirty = true;
  }
}

async function flushCursorMeta() {
  if (DRY_RUN || !watermarkDirty || safeCursor === null) return;
  watermarkDirty = false;
  try {
    await metaSet(db, META_CURSOR_KEY, safeCursor);
    await metaSet(db, META_STATE_KEY, JSON.stringify({
      pages: stats.feedPages,
      seen: stats.feedSeen,
      accDone: stats.accDone,
      repoHasId: stats.repoHasId,
      filesUpserted: stats.filesUpserted,
      lastLmRange,
      reachedEnd,
      at: new Date().toISOString(),
    }));
  } catch (e) {
    log("flushCursorMeta err: " + (e.message || e));
  }
}

// ---------------- hàng đợi & workers ----------------
const accQueue = [];
async function accWorkerLoop() {
  while (!stopRequested) {
    const job = accQueue.shift();
    if (!job) {
      if (feedDone) break;
      await sleep(80);
      continue;
    }
    try {
      await processAccountJob(job);
    } catch (e) {
      if (e instanceof Stop) break;
    }
  }
}

async function probeBatch(cands) {
  let idx = 0;
  const out = new Array(cands.length).fill(false);
  const n = Math.min(PROBE_CONCURRENCY, cands.length);
  await Promise.all(Array.from({ length: n }, async () => {
    while (!stopRequested) {
      const i = idx++;
      if (i >= cands.length) break;
      const ok = await probeDatasetForId(cands[i].id);
      if (ok) { out[i] = true; stats.feedProbeHit++; }
    }
  }));
  return out;
}

async function feedLoop() {
  let cursor = safeCursor;
  let noLinkStreak = 0;

  while (!stopRequested && Date.now() < FEED_CUTOFF) {
    if (MAX_PAGES > 0 && stats.feedPages >= MAX_PAGES) break;
    while (accQueue.length >= MAX_ACC_QUEUE && !stopRequested && Date.now() < FEED_CUTOFF) {
      await sleep(100);
    }
    if (stopRequested || Date.now() >= FEED_CUTOFF) break;

    const url = `${BASE}/datasets?limit=${FEED_LIMIT}&sort=lastModified` + (cursor ? `&cursor=${encodeURIComponent(cursor)}` : "");
    const j = await req(url);
    if (!j.ok || !Array.isArray(j.data)) {
      log(`feed stop status=${j.status}`);
      break;
    }
    if (!j.data.length) {
      reachedEnd = true;
      cursor = "";
      pendingPages.push({ page: stats.feedPages + 1, cursorNext: "", remaining: 0 });
      advanceWatermark();
      break;
    }

    // Bước 1: tách ứng viên qua isInvalidFarmName + xác định nhóm cần probe
    const directItems = [];
    const probeCands = [];
    for (const x of j.data) {
      stats.feedSeen++;
      const id = String(x.id || "");
      const author = id.split("/")[0];
      const rbase = id.split("/").slice(1).join("/");
      if (!author || !rbase) continue;
      if (feedIds.has(id)) { stats.feedDup++; continue; }
      feedIds.add(id);

      if (isInvalidFarmName(author)) { stats.feedStyle++; continue; }
      const isSelf = id === author + "/" + author;
      const pureLower = isPureLowerFarm(author);
      const hasAiTags = Array.isArray(x.tags) && x.tags.some((t) => AI_TAG_RE.test(t));
      const oneWord = /^[a-z]{3,20}$/.test(rbase);
      const hyphenLower = /^[a-z]+(-[a-z]+){1,6}$/.test(rbase);
      const probePure = pureLower && !isSelf && !hasAiTags && (oneWord || hyphenLower) && !AI_SLUG_RE.test(rbase);
      const probeTwoDig = /^[a-z]{4,20}\d{2}$/.test(author) && isSelf && !hasAiTags;
      const itemMeta = {
        id, author, rbase, isSelf, oneWord,
        sha: x.sha || null,
        lm: x.lastModified ? Date.parse(x.lastModified) || 0 : 0,
        probedOk: false,
      };

      if (!isFarmStrict(author) && !(pureLower && isSelf)) {
        if (!(probePure || probeTwoDig)) continue;
        probeCands.push(itemMeta);
      } else {
        directItems.push(itemMeta);
      }
    }

    // Bước 2: probe song song các ứng viên pureLower / 2-digit qua 5 HF tokens
    if (probeCands.length) {
      const hits = await probeBatch(probeCands);
      for (let i = 0; i < probeCands.length; i++) {
        if (hits[i]) {
          probeCands[i].probedOk = true;
          directItems.push(probeCands[i]);
        } else {
          stats.feedShape++;
        }
      }
    }

    // Bước 3: đọc batch accounts & lọc theo chuẩn auto.mjs (lines 296-305)
    const uniqueAuthors = [...new Set(directItems.map((c) => c.author))];
    await loadAccountsBatch(uniqueAuthors);

    const byAuthor = new Map();
    for (const it of directItems) {
      const acc = accMem.get(it.author) || null;
      const hasId = !!(acc && acc.has_id);
      if (/^[A-Z]/.test(it.author) && !hasId) { stats.feedStyle++; continue; }
      if (acc && acc.nrepo >= 20 && !hasId) { stats.feedStyle++; continue; }

      const lowerNum = /^[a-z]{2,}[0-9]{1,7}$/.test(it.author);
      const capWord = /^[A-Za-z]{3,20}$/.test(it.rbase);
      const hyphenWord = it.rbase.includes("-") && /^[A-Za-z0-9-]{3,30}$/.test(it.rbase);
      const take = it.isSelf || hasId || it.probedOk || (it.oneWord && lowerNum) || (capWord && lowerNum) || (hasId && hyphenWord);
      if (!take) { stats.feedShape++; continue; }

      let list = byAuthor.get(it.author);
      if (!list) { list = []; byAuthor.set(it.author, list); }
      list.push(it);
    }

    // Bước 4: tính cursor tiếp theo trước khi enqueue để gắn watermark
    const nx = nextCursor(j.link);
    let nextCurForPage = nx;
    let isLastRealPage = false;

    if (!nx && j.data.length >= FEED_LIMIT) {
      stats.feedNoLink++;
      noLinkStreak++;
      const lastLM = [...j.data].reverse().find((x) => x.lastModified)?.lastModified;
      if (noLinkStreak >= 2 && lastLM) {
        nextCurForPage = Buffer.from(JSON.stringify({ $or: [{ lastModified: { $lt: lastLM } }, { lastModified: null }] })).toString("base64url");
        noLinkStreak = 0;
        log(`feed thieu link p=${stats.feedPages + 1}, tu dung cursor lt=${lastLM}`);
      } else {
        log(`feed thieu link p=${stats.feedPages + 1}, thu lai...`);
        await sleep(3000);
        continue;
      }
    } else {
      noLinkStreak = 0;
      if (!nx) isLastRealPage = true;
    }

    stats.feedPages++;
    const lms = j.data.map((x) => x.lastModified).filter(Boolean);
    lastLmRange = `${lms[0] || "-"}..${lms[lms.length - 1] || "-"}`;

    const jobsForPage = [];
    for (const [author, seedItems] of byAuthor) {
      if (accHandled.has(author)) continue;
      accHandled.add(author);
      jobsForPage.push({ author, acc: accMem.get(author) || null, seedItems });
    }

    const pageTracker = { page: stats.feedPages, cursorNext: isLastRealPage ? "" : (nextCurForPage || ""), remaining: jobsForPage.length };
    pendingPages.push(pageTracker);
    for (const jb of jobsForPage) {
      jb.pageTracker = pageTracker;
      accQueue.push(jb);
      stats.accQueued++;
    }
    advanceWatermark();

    if (stats.feedPages === 1 || stats.feedPages % 10 === 0 || isLastRealPage) {
      await flushCursorMeta();
      log(`feed p=${stats.feedPages} seen=${stats.feedSeen} lm=${lastLmRange} accQ=${accQueue.length} accDone=${stats.accDone}/${stats.accQueued} repos=${stats.repoTotal}(tree=${stats.repoTree},inc=${stats.repoInc},unchg=${stats.repoUnchanged},dead=${stats.repoDead}) phimRepos=${stats.repoHasId} files=+${stats.filesUpserted}/-${stats.filesDeleted} | ${laneStat()}`);
    }

    if (isLastRealPage) {
      reachedEnd = true;
      cursor = "";
      break;
    }
    cursor = nextCurForPage;
  }

  feedDone = true;
}

// ---------------- quét bổ sung bucket cho các account junk=0 chưa gặp qua feed ----------------
async function extraBucketSweep() {
  if (!BKT_EXTRA || !reachedEnd || stopRequested || Date.now() >= DEADLINE - 30000) return;
  const rows = (await db.exec("SELECT username, has_id, junk, nrepo, scanned_at, verified FROM accounts WHERE junk=0 ORDER BY username")).rows;
  const todo = rows.filter((r) => !accHandled.has(r.username));
  if (!todo.length) {
    log("bktExtra: toan bo account junk=0 da duoc kiem tra trong feed");
    return;
  }
  log(`bktExtra: kiem tra bucket cho ${todo.length} account junk=0 chua gap trong feed...`);
  let idx = 0;
  await Promise.all(Array.from({ length: ACC_WORKERS }, async () => {
    while (!stopRequested && Date.now() < DEADLINE - 15000) {
      const i = idx++;
      if (i >= todo.length) break;
      const acc = todo[i];
      accHandled.add(acc.username);
      try {
        const bk = await getAllPages(`${BASE}/buckets/${encodeURIComponent(acc.username)}`);
        if (!Array.isArray(bk.data) || !bk.data.length) { stats.bktExtraDone++; continue; }
        const qPrev = await db.exec(
          "SELECT id, kind, state, sha, updated, nfile, size, entries, last_modified, has_id, junk, indexed_at, prio FROM repos WHERE account=? AND kind='bucket'",
          [acc.username]
        );
        const prevMap = new Map(qPrev.rows.map((r) => [r.kind + ":" + r.id, r]));
        let foundMovie = false;
        for (const b of bk.data) {
          const id = String(b.id || "");
          if (!id || isInvalidFarmName(id.split("/")[0])) continue;
          const prev = prevMap.get("bucket:" + id) || null;
          const hit = await processSingleRepo(
            { kind: "bucket", id, totalFiles: b.totalFiles ?? null, remoteLm: b.updatedAt ? Date.parse(b.updatedAt) || 0 : 0, remoteSha: null },
            acc.username, prev, acc
          );
          if (hit) foundMovie = true;
        }
        if (foundMovie && !DRY_RUN && !acc.has_id) {
          const now = Date.now();
          await db.exec("UPDATE accounts SET verified=1, has_id=1, junk=0, scanned_at=?, next_scan_at=? WHERE username=?", [now, now + 18000000, acc.username]);
        }
        stats.bktExtraDone++;
      } catch (e) {
        if (e instanceof Stop) break;
        stats.accErr++;
      }
    }
  }));
}

// ---------------- main ----------------
log(`FULLSCAN start tokens=${lanes.length} gap=${GAP}ms accWorkers=${ACC_WORKERS} probeConc=${PROBE_CONCURRENCY} budget=${Math.round(BUDGET / 1000)}s resume=${RESUME ? (safeCursor ? "yes" : "empty-top") : "fresh-top"} recheckDead=${RECHECK_DEAD} bktExtra=${BKT_EXTRA} dry=${DRY_RUN}`);

const workers = Array.from({ length: ACC_WORKERS }, () => accWorkerLoop());
try {
  await Promise.all([feedLoop(), ...workers]);
  await extraBucketSweep();
} catch (e) {
  if (!(e instanceof Stop)) {
    log("FULLSCAN FATAL " + (e && e.stack ? e.stack : e));
    await flushCursorMeta();
    process.exit(1);
  }
}

advanceWatermark();
await flushCursorMeta();

const elapsed = Math.round((Date.now() - START) / 1000);
const reqTotal = lanes.reduce((a, t) => a + t.req, 0);
const n429Total = lanes.reduce((a, t) => a + t.n429, 0);
const errTotal = lanes.reduce((a, t) => a + t.err, 0);
const summary = {
  elapsed,
  reached_end: reachedEnd,
  feed_pages: stats.feedPages,
  feed_seen: stats.feedSeen,
  feed_dup: stats.feedDup,
  feed_probed: stats.feedProbed,
  feed_probe_hit: stats.feedProbeHit,
  last_lm_range: lastLmRange,
  acc_queued: stats.accQueued,
  acc_done: stats.accDone,
  acc_has_id: stats.accHasId,
  acc_new_has_id: stats.accNewHasId,
  acc_junk: stats.accJunk,
  acc_skip_empty: stats.accSkipEmpty,
  acc_err: stats.accErr,
  bkt_extra_done: stats.bktExtraDone,
  repo_total: stats.repoTotal,
  repo_tree: stats.repoTree,
  repo_inc: stats.repoInc,
  repo_unchanged: stats.repoUnchanged,
  repo_dead: stats.repoDead,
  repo_has_id: stats.repoHasId,
  files_upserted: stats.filesUpserted,
  files_deleted: stats.filesDeleted,
  req: reqTotal,
  n429: n429Total,
  errors: errTotal,
  resumable_cursor_set: Boolean(safeCursor),
};

log(`FULLSCAN DONE ${reachedEnd ? "HOAN_TAT_TOAN_BO_HF" : "TAM_DUNG_CO_THE_RESUME"} | ${laneStat()}`);
log(`FULLSCAN SUMMARY ${JSON.stringify(summary)}`);
process.exit(0);
