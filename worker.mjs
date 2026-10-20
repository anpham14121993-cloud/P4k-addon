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
};
