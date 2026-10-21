// worker.mjs — entry Cloudflare Workers cho addon Phim4K (DB Turso, stream HF direct).
import { setDb, addon } from "./core.mjs";
import { tursoDb } from "./turso-db.mjs";

let ready = false;

export default {
  async fetch(request, env, ctx) {
    if (!ready) {
      setDb(tursoDb(env));
      ready = true;
    }
    return addon.fetch(request);
  },
  // Cron Trigger: tự dispatch workflow crawl (GitHub schedule hay bị bỏ nhịp).
  async scheduled(event, env, ctx) {
    const tok = env.GH_TOKEN;
    if (!tok) return;
    const res = await fetch("https://api.github.com/repos/anpham14121993-cloud/P4k-addon/actions/workflows/crawl.yml/dispatches", {
      method: "POST",
      headers: {
        Authorization: "Bearer " + tok,
        Accept: "application/vnd.github+json",
        "User-Agent": "p4k-cron",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ ref: "main", inputs: { budget: "19800" } }),
    });
    if (!res.ok) console.log("dispatch failed", res.status, await res.text());
  },
};
