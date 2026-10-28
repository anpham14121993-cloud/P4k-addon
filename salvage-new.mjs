// salvage-new.mjs — Quét rà soát toàn bộ các repo đang kẹt ở state='new' có nghi vấn farm/bucket.
// Nếu phát hiện file ID phim hợp lệ: nạp ngay vào DB và mở khóa tài khoản.
// Nếu rác/chết: đánh dấu hoàn tất hoặc dead để làm sạch hàng đợi state='new'.
import { openDB, isId, isFarmStrict, PHIM_EXT } from "./pdb-turso.mjs";

const db = openDB();
const LIMIT = parseInt(process.argv[2] || "1000", 10);
const CONCURRENCY = 8;

// Hỗ trợ nhiều HF token nếu có
const tokens = (process.env.HF_TOKEN || "").split(",").map(t => t.trim()).filter(Boolean);
let tokenIdx = 0;
function getHeaders() {
  const h = { "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" };
  if (tokens.length) {
    h["Authorization"] = `Bearer ${tokens[tokenIdx++ % tokens.length]}`;
  }
  return h;
}

console.log(`[SALVAGE] Đang tải danh sách repo 'new' từ Turso DB (giới hạn: ${LIMIT})...`);
const q = await db.exec("SELECT r.id, r.kind, r.account FROM repos r WHERE r.state='new'");
const allCandidates = q.rows.filter(r => r.kind === "bucket" || isFarmStrict(r.account));
const targetList = allCandidates.slice(0, LIMIT);

console.log(`[SALVAGE] Tìm thấy ${allCandidates.length} repo ứng viên nghi vấn. Sẽ quét ${targetList.length} repo...`);

let doneCount = 0;
let salvagedRepos = 0;
let salvagedFiles = 0;
let deadRepos = 0;
let emptyRepos = 0;
const salvagedList = [];

async function checkOne(r) {
  const url = r.kind === "bucket"
    ? `https://huggingface.co/api/buckets/${r.id}/tree?recursive=true`
    : `https://huggingface.co/api/datasets/${r.id}/tree/main?recursive=true`;

  try {
    const res = await fetch(url, { headers: getHeaders() });
    if (res.status === 404 || res.status === 401 || res.status === 403) {
      await db.exec("UPDATE repos SET state='dead', junk=1 WHERE id=? AND kind=?", [r.id, r.kind]);
      deadRepos++;
      return;
    }
    if (!res.ok) return;

    const data = await res.json();
    if (!Array.isArray(data)) return;

    const mediaFiles = data.filter(f => isId(f.path));
    if (mediaFiles.length > 0) {
      // TÌM THẤY KHO PHIM THẬT!
      salvagedRepos++;
      salvagedFiles += mediaFiles.length;
      salvagedList.push({ id: r.id, kind: r.kind, count: mediaFiles.length });
      console.log(`🔥 [TÌM THẤY!] ${r.kind}:${r.id} có ${mediaFiles.length} file phim hợp lệ!`);

      // 1. Nạp file vào bảng files
      const stmts = mediaFiles.map(f => {
        const bn = f.path.split("/").pop();
        return {
          sql: "INSERT OR IGNORE INTO files(repo_id, kind, path, name, size, off) VALUES(?,?,?,?,?,?)",
          args: [r.id, r.kind, f.path, bn, f.size || 0, f.oid || null]
        };
      });
      if (stmts.length) await db.batch(stmts, 500);

      // 2. Cập nhật repo thành done, has_id=1, junk=0
      const totalSize = mediaFiles.reduce((acc, f) => acc + (f.size || 0), 0);
      await db.exec(
        "UPDATE repos SET state='done', has_id=1, junk=0, nfile=?, size=?, indexed_at=? WHERE id=? AND kind=?",
        [mediaFiles.length, totalSize, Date.now(), r.id, r.kind]
      );

      // 3. Mở khóa account để các run cron sau tiếp quản
      if (r.account) {
        await db.exec("UPDATE accounts SET has_id=1, junk=0 WHERE username=?", [r.account]);
      }
    } else {
      // Repo không có file phim hợp lệ -> đánh dấu done has_id=0 để không kẹt ở new nữa
      emptyRepos++;
      await db.exec("UPDATE repos SET state='done', has_id=0, junk=1, indexed_at=? WHERE id=? AND kind=?", [Date.now(), r.id, r.kind]);
    }
  } catch (err) {
    // bỏ qua lỗi tạm thời
  } finally {
    doneCount++;
    if (doneCount % 50 === 0 || doneCount === targetList.length) {
      console.log(`[SALVAGE TIẾN ĐỘ] Đã quét ${doneCount}/${targetList.length} repo | Cứu được: ${salvagedRepos} repo (${salvagedFiles} files) | Dead: ${deadRepos} | Không có phim: ${emptyRepos}`);
    }
  }
}

// Chạy song song CONCURRENCY worker
const queue = [...targetList];
const workers = Array.from({ length: CONCURRENCY }, async () => {
  while (queue.length > 0) {
    const item = queue.shift();
    if (item) await checkOne(item);
  }
});

await Promise.all(workers);

console.log("\n=================== TỔNG KẾT SALVAGE ===================");
console.log(`Tổng repo đã quét      : ${doneCount}`);
console.log(`Repo phim cứu được      : ${salvagedRepos}`);
console.log(`Số file phim nạp mới   : ${salvagedFiles}`);
console.log(`Repo dead/404          : ${deadRepos}`);
console.log(`Repo không có phim     : ${emptyRepos}`);
if (salvagedList.length) {
  console.log("Danh sách repo đã cứu:");
  for (const s of salvagedList) console.log(`  * [${s.kind}] ${s.id} (+${s.count} files)`);
}
console.log("========================================================");
process.exit(0);
