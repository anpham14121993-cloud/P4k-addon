// recheck-nov13.mjs — Kiểm tra 100% và cứu vãn (nếu còn sống 200) toàn bộ:
//   1) ~3,888 account has_id=1 hiện đang không có file sống nào
//   2) ~5,520 repo bị đánh state='dead' trong giai đoạn 10/11 - 13/11/2026 (giờ VN)
//   3) ~459 repo state='done', nfile=0 thuộc các account has_id=1
// Dùng 5 HF_TOKEN qua pool song song, có sao cứu thẳng vào Oracle/Turso DB.
import {
  openDB, ensureSchema, storeTree, isFarm, isId, PHIM_EXT, repoScore, log, sleep,
} from "./pdb-turso.mjs";

const db = openDB();
try { await ensureSchema(db); } catch (e) { log("ensureSchema warn: " + (e.message || e)); }

const TOKENS = String(process.env.HF_TOKEN || "").split(",").map((x) => x.trim()).filter(Boolean);
const GAP = Math.max(30, parseInt(process.env.P4K_GAP || "140", 10));
const CONCURRENCY = Math.max(2, parseInt(process.env.P4K_CONCURRENCY || "12", 10));
const LIMIT = Math.max(0, parseInt(process.env.P4K_LIMIT || "0", 10));
const BUDGET = Math.max(60, parseInt(process.argv[2] || process.env.P4K_BUDGET || "3600", 10)) * 1000;
const START = Date.now();
const DEADLINE = START + BUDGET;
const BASE = "https://huggingface.co/api";

const T10 = Date.parse("2026-11-09T17:00:00Z"); // 2026-11-10 00:00 +07:00
const T14 = Date.parse("2026-11-13T17:00:00Z"); // 2026-11-14 00:00 +07:00

const h32 = (s) => { let h = 2166136261; for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 16777619); } return h >>> 0; };
const computeDueAt = (hasId, key) => Date.now() + (hasId ? 21600000 : 604800000) - (h32(key) % 3600000);

class Stop extends Error {}
const lanes = (TOKENS.length ? TOKENS : [null]).map((tok) => ({ tok, next: 0, cool: 0, req: 0, n429: 0, err: 0 }));
let rr = 0;
const laneStat = () => lanes.map((t, i) => `T${i + 1}:${t.req}${t.n429 ? "/429=" + t.n429 : ""}${t.err ? "/e=" + t.err : ""}`).join(" ");

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

async function req(u, { timeout = 30000 } = {}) {
  for (let a = 0; a < 30; a++) {
    const t = await takeToken();
    try {
      const r = await fetch(u, {
        headers: t.tok ? { Accept: "application/json", Authorization: "Bearer " + t.tok } : { Accept: "application/json" },
        signal: AbortSignal.timeout(timeout),
      });
      const rl = r.headers.get("ratelimit") || "";
      const mr = rl.match(/\br=(\d+)/);
      if (r.status === 429) { t.cool = Date.now() + parseResetMs(r.headers); t.n429++; continue; }
      if (mr && parseInt(mr[1], 10) <= 1) t.cool = Math.max(t.cool, Date.now() + parseResetMs(r.headers));
      if (r.status === 404 || r.status === 401 || r.status === 403) return { ok: false, status: r.status };
      if (!r.ok) { t.err++; await sleep(900); continue; }
      const body = await r.text();
      try { return { ok: true, status: r.status, data: JSON.parse(body), link: r.headers.get("link") || "" }; }
      catch { t.err++; await sleep(200); }
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

async function getTree(id, kind, maxPages = 100) {
  let u = BASE + (kind === "bucket" ? `/buckets/${id}/tree?recursive=true` : `/datasets/${id}/tree/main?recursive=true`);
  const out = [];
  for (let p = 0; p < maxPages; p++) {
    const r = await req(u);
    if (!r.ok || !Array.isArray(r.data)) return { ok: false, status: r.status };
    out.push(...r.data);
    const nx = nextLink(r.link);
    if (!nx) return { ok: true, status: r.status, data: out };
    u = nx;
  }
  return { ok: true, status: 200, data: out };
}

async function saveSalvagedRepo({ id, kind, account, size, nfile, lastModified, state, hasId, entries, sha, updated, junk }) {
  const now = Date.now();
  const rKey = kind + ":" + id;
  await db.exec(
    `INSERT INTO repos(id, kind, account, size, nfile, last_modified, state, has_id, entries, indexed_at, worker, sha, updated, prio, junk, due_at)
     VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
     ON CONFLICT(id, kind) DO UPDATE SET
       account=excluded.account, size=excluded.size, nfile=excluded.nfile,
       last_modified=CASE WHEN excluded.last_modified > COALESCE(repos.last_modified,0) THEN excluded.last_modified ELSE repos.last_modified END,
       state=excluded.state, has_id=excluded.has_id, entries=excluded.entries, indexed_at=excluded.indexed_at,
       worker=NULL, sha=COALESCE(excluded.sha, repos.sha),
       updated=CASE WHEN excluded.updated > COALESCE(repos.updated,0) THEN excluded.updated ELSE repos.updated END,
       prio=excluded.prio, junk=excluded.junk, due_at=excluded.due_at`,
    [id, kind, account, size || 0, nfile || 0, lastModified || 0, state, hasId ? 1 : 0, entries || 0, now, null, sha || null, updated || 0, repoScore(id, account, kind, { has_id: hasId ? 1 : 0 }), junk ? 1 : 0, state === "dead" ? 0 : computeDueAt(!!hasId, rKey)]
  );
}

const stats = {
  acc_total: 0, acc_done: 0, acc_ov_200: 0, acc_ov_404: 0, acc_ov_401_403: 0, acc_ov_other: 0,
  acc_salvaged: 0, acc_salvaged_names: [],
  dead_total: 0, dead_done: 0, dead_401_403: 0, dead_404: 0, dead_other: 0,
  dead_alive_200: 0, dead_salvaged_movies: 0, dead_alive_empty: 0,
  done0_total: 0, done0_done: 0, done0_401_404: 0, done0_200_empty: 0, done0_salvaged_movies: 0,
  files_saved: 0, case_id_alerts: 0,
};

const checkedRepoKeys = new Set();
const aliveUsers = [];
const salvagedRepos = [];

async function checkAndIndexRepo(id, kind, account, sha = null, lm = 0) {
  const key = kind + ":" + id;
  checkedRepoKeys.add(key);
  const t = await getTree(id, kind, 100);
  if (!t.ok || !Array.isArray(t.data)) return { status: t.status, n: 0 };
  // Phát hiện file dạng 10-17 ký tự có chữ thường (vd case 5VL9S420W0EWDd0T)
  for (const f of t.data) {
    if (f.type && f.type !== "file") continue;
    const bn = String(f.path || f.name || "").split("/").pop();
    const m = bn.match(/\.[^.]+$/);
    if (!m || !PHIM_EXT.has(m[0].toLowerCase())) continue;
    const stem = bn.slice(0, bn.length - m[0].length);
    if (!isId(bn) && /^[A-Za-z0-9]{10,17}$/.test(stem) && !/^\d+$/.test(stem)) {
      stats.case_id_alerts++;
      log(`!LOWERCASE_ID_CANDIDATE ${key}/${bn}`);
    }
  }
  const { n, size, ins, upd } = await storeTree(db, id, kind, t.data, account, false);
  stats.files_saved += (ins || 0) + (upd || 0);
  const hasId = n > 0 ? 1 : 0;
  await saveSalvagedRepo({
    id, kind, account, size, nfile: n, lastModified: lm, state: "done",
    hasId, entries: t.data.length, sha, updated: lm, junk: hasId ? 0 : 1,
  });
  return { status: 200, n, entries: t.data.length, ins, upd };
}

// ---------------- 1. Tải danh sách từ DB theo chunk an toàn ----------------
async function fetchPaged(sql, params = []) {
  const out = [];
  let off = 0;
  while (true) {
    const q = await db.exec(`${sql} LIMIT 2000 OFFSET ?`, [...params, off]);
    if (!q.rows.length) break;
    out.push(...q.rows);
    if (q.rows.length < 2000) break;
    off += 2000;
  }
  return LIMIT > 0 ? out.slice(0, LIMIT) : out;
}

log(`RECHECK-NOV13 start tokens=${lanes.length} gap=${GAP}ms conc=${CONCURRENCY} limit=${LIMIT || "all"}`);
const accList = await fetchPaged(
  "SELECT username, nrepo, junk FROM accounts WHERE has_id=1 AND username NOT IN (SELECT DISTINCT account FROM repos WHERE nfile>0) ORDER BY username"
);
const deadList = await fetchPaged(
  "SELECT id, kind, account, last_modified FROM repos WHERE state='dead' AND indexed_at>=? AND indexed_at<? ORDER BY kind, id",
  [T10, T14]
);
const done0List = await fetchPaged(
  "SELECT id, kind, account, last_modified FROM repos WHERE state='done' AND nfile=0 AND account IN (SELECT username FROM accounts WHERE has_id=1) ORDER BY kind, id"
);

stats.acc_total = accList.length;
stats.dead_total = deadList.length;
stats.done0_total = done0List.length;
log(`Loaded lists: empty_has_id_acc=${accList.length}, dead_nov10_13=${deadList.length}, done0_under_has_id=${done0List.length}`);

// ---------------- 2. Quét 100% account has_id=1 rỗng ----------------
{
  let idx = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = idx++;
      if (i >= accList.length) break;
      const u = accList[i].username;
      try {
        const ov = await req(`${BASE}/users/${encodeURIComponent(u)}/overview`);
        if (ov.status === 404) stats.acc_ov_404++;
        else if (ov.status === 401 || ov.status === 403) stats.acc_ov_401_403++;
        else if (ov.ok && ov.data) {
          stats.acc_ov_200++;
          const bCnt = Number(ov.data.numBuckets || 0);
          const dCnt = Number(ov.data.numDatasets || 0);
          aliveUsers.push({ u, d: dCnt, b: bCnt });
          log(`★ ACCOUNT SONG 200: ${u} (datasets=${dCnt}, buckets=${bCnt})`);

          const [bk, ds] = await Promise.all([
            getAllPages(`${BASE}/buckets/${encodeURIComponent(u)}`),
            getAllPages(`${BASE}/datasets?author=${encodeURIComponent(u)}&limit=1000`),
          ]);
          let foundFiles = 0;
          const own = (id) => String(id).startsWith(u + "/");
          if (Array.isArray(bk.data)) {
            for (const b of bk.data) {
              if (!b.id || (!own(b.id) && !isFarm(String(b.id).split("/")[0]))) continue;
              const r = await checkAndIndexRepo(String(b.id), "bucket", u, null, b.updatedAt ? Date.parse(b.updatedAt) || 0 : 0);
              if (r.n > 0) { foundFiles += r.n; salvagedRepos.push({ kind: "bucket", id: b.id, n: r.n }); }
            }
          }
          if (Array.isArray(ds.data)) {
            for (const z of ds.data) {
              if (!z.id || (!own(z.id) && !isFarm(String(z.id).split("/")[0]))) continue;
              const r = await checkAndIndexRepo(String(z.id), "dataset", u, z.sha || null, z.lastModified ? Date.parse(z.lastModified) || 0 : 0);
              if (r.n > 0) { foundFiles += r.n; salvagedRepos.push({ kind: "dataset", id: z.id, n: r.n }); }
            }
          }
          // Quét nốt repo đang nằm DB dưới account này phòng trường hợp không hiện trong listing
          const dbRepos = (await db.exec("SELECT id, kind, last_modified FROM repos WHERE account=?", [u])).rows;
          for (const dr of dbRepos) {
            if (checkedRepoKeys.has(dr.kind + ":" + dr.id)) continue;
            const r = await checkAndIndexRepo(dr.id, dr.kind, u, null, dr.last_modified || 0);
            if (r.n > 0) { foundFiles += r.n; salvagedRepos.push({ kind: dr.kind, id: dr.id, n: r.n }); }
          }
          if (foundFiles > 0) {
            stats.acc_salvaged++;
            stats.acc_salvaged_names.push(`${u}(${foundFiles})`);
            const now = Date.now();
            await db.exec("UPDATE accounts SET verified=1, has_id=1, junk=0, scanned_at=?, next_scan_at=? WHERE username=?", [now, now + 18000000, u]);
            await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=0 WHERE account=? AND junk=1", [u]);
            log(`🔥 CUU DUOC ACCOUNT: ${u} -> ${foundFiles} files!`);
          }
        } else {
          stats.acc_ov_other++;
        }
      } catch (e) {
        if (e instanceof Stop) break;
        stats.acc_ov_other++;
      } finally {
        stats.acc_done++;
        if (stats.acc_done % 500 === 0 || stats.acc_done === accList.length) {
          log(`[1/3 ACC] ${stats.acc_done}/${accList.length} | 404=${stats.acc_ov_404} 401/403=${stats.acc_ov_401_403} 200=${stats.acc_ov_200} cuu=${stats.acc_salvaged} | ${laneStat()}`);
        }
      }
    }
  }));
}

// ---------------- 3. Quét 100% repo state='dead' ngày 10-13/11 ----------------
{
  let idx = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = idx++;
      if (i >= deadList.length) break;
      const r = deadList[i];
      const key = r.kind + ":" + r.id;
      if (checkedRepoKeys.has(key)) { stats.dead_done++; continue; }
      checkedRepoKeys.add(key);
      try {
        const u = BASE + (r.kind === "bucket" ? `/buckets/${r.id}/tree?recursive=true` : `/datasets/${r.id}/tree/main`);
        const probe = await req(u);
        if (probe.status === 401 || probe.status === 403) stats.dead_401_403++;
        else if (probe.status === 404) stats.dead_404++;
        else if (probe.ok) {
          stats.dead_alive_200++;
          const res = await checkAndIndexRepo(r.id, r.kind, r.account || r.id.split("/")[0], null, r.last_modified || 0);
          if (res.n > 0) {
            stats.dead_salvaged_movies++;
            salvagedRepos.push({ kind: r.kind, id: r.id, n: res.n });
            const acc = r.account || r.id.split("/")[0];
            const now = Date.now();
            await db.exec("UPDATE accounts SET verified=1, has_id=1, junk=0, scanned_at=?, next_scan_at=? WHERE username=?", [now, now + 18000000, acc]);
            await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=0 WHERE account=? AND junk=1", [acc]);
            log(`🔥 CUU REPO DEAD NOV10-13: ${key} -> ${res.n} files!`);
          } else {
            stats.dead_alive_empty++;
          }
        } else {
          stats.dead_other++;
        }
      } catch (e) {
        if (e instanceof Stop) break;
        stats.dead_other++;
      } finally {
        stats.dead_done++;
        if (stats.dead_done % 500 === 0 || stats.dead_done === deadList.length) {
          log(`[2/3 DEAD 10-13/11] ${stats.dead_done}/${deadList.length} | 401/403=${stats.dead_401_403} 404=${stats.dead_404} alive200=${stats.dead_alive_200}(phim=${stats.dead_salvaged_movies}) | ${laneStat()}`);
        }
      }
    }
  }));
}

// ---------------- 4. Quét 100% repo done nfile=0 dưới account has_id=1 ----------------
{
  let idx = 0;
  await Promise.all(Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = idx++;
      if (i >= done0List.length) break;
      const r = done0List[i];
      const key = r.kind + ":" + r.id;
      if (checkedRepoKeys.has(key)) { stats.done0_done++; continue; }
      checkedRepoKeys.add(key);
      try {
        const res = await checkAndIndexRepo(r.id, r.kind, r.account || r.id.split("/")[0], null, r.last_modified || 0);
        if (res.status === 401 || res.status === 403 || res.status === 404) stats.done0_401_404++;
        else if (res.n > 0) {
          stats.done0_salvaged_movies++;
          salvagedRepos.push({ kind: r.kind, id: r.id, n: res.n });
          const acc = r.account || r.id.split("/")[0];
          const now = Date.now();
          await db.exec("UPDATE accounts SET verified=1, has_id=1, junk=0, scanned_at=?, next_scan_at=? WHERE username=?", [now, now + 18000000, acc]);
          await db.exec("UPDATE repos INDEXED BY idx_repos_account_junk SET junk=0 WHERE account=? AND junk=1", [acc]);
          log(`🔥 CUU REPO DONE0: ${key} -> ${res.n} files!`);
        } else {
          stats.done0_200_empty++;
        }
      } catch (e) {
        if (e instanceof Stop) break;
      } finally {
        stats.done0_done++;
      }
    }
  }));
}

const summary = {
  elapsed: Math.round((Date.now() - START) / 1000),
  ...stats,
  alive_users_sample: aliveUsers.slice(0, 30),
  salvaged_repos: salvagedRepos.slice(0, 50),
  req: lanes.reduce((a, t) => a + t.req, 0),
  n429: lanes.reduce((a, t) => a + t.n429, 0),
  errors: lanes.reduce((a, t) => a + t.err, 0),
};
log(`RECHECK DONE | ${laneStat()}`);
log(`RECHECK SUMMARY ${JSON.stringify(summary)}`);
process.exit(0);
