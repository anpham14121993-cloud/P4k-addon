// worker.mjs — entry Cloudflare Workers cho addon Phim4K (DB Turso, stream HF direct).
import { setDb, setEnv, addon } from "./core.mjs";
import { tursoDb } from "./turso-db.mjs";

let ready = false;

export default {
  async fetch(request, env, ctx) {
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

    // 1. Tuyến 1 (crawl): chạy mỗi giờ
    await dispatch("crawl.yml", { budget: "3600" });

    const hour = new Date().getUTCHours();

    // 2. Tuyến 2 (upgrade-quality): 05:00 UTC (12:00 VN) & 17:00 UTC (00:00 VN)
    if (hour === 5 || hour === 17) {
      await dispatch("upgrade-quality.yml", { type: "all", limit: "0", dry_run: "false" });
    }

    // 3. Tuyến 3 (health-check): 20:00 UTC (03:00 VN)
    if (hour === 20) {
      await dispatch("health-check.yml", { limit: "0", dry_run: "false" });
    }
  },
};
