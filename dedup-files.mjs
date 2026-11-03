// dedup-files.mjs — Dọn sạch các dòng duplicate trong bảng files cho từng repo.
import { openDB } from "./pdb-turso.mjs";

async function main() {
  const db = await openDB();
  console.log("Finding repos with duplicate files...");
  const dupRepos = (await db.exec(`
    SELECT repo_id, kind, count(*) as dup_c 
    FROM (SELECT repo_id, kind, path FROM files GROUP BY repo_id, kind, path HAVING count(*) > 1) 
    GROUP BY repo_id, kind 
    ORDER BY dup_c DESC
  `)).rows;

  console.log(`Found ${dupRepos.length} repos with duplicates.`);
  let totalDeleted = 0;

  for (const { repo_id, kind, dup_c } of dupRepos) {
    console.log(`Cleaning ${kind}:${repo_id} (~${dup_c} duplicates)...`);
    const res = await db.exec(`
      DELETE FROM files 
      WHERE repo_id=? AND kind=? AND rowid NOT IN (
        SELECT min(rowid) FROM files WHERE repo_id=? AND kind=? GROUP BY path
      )
    `, [repo_id, kind, repo_id, kind]);
    
    totalDeleted += res.changes;
    console.log(`  -> Deleted ${res.changes} duplicate rows for ${repo_id}`);

    // Update nfile and size in repos table to reflect accurate counts
    const counts = (await db.exec(`
      SELECT count(*) as n, sum(size) as sz FROM files WHERE repo_id=? AND kind=?
    `, [repo_id, kind])).rows[0];
    if (counts) {
      await db.exec(`
        UPDATE repos SET nfile=?, size=? WHERE id=? AND kind=?
      `, [counts.n || 0, counts.sz || 0, repo_id, kind]);
    }
  }

  console.log(`=== DEDUP COMPLETE: Total duplicate rows removed: ${totalDeleted} ===`);
}

main().catch(console.error);
