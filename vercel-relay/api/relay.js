// Vercel pass-through relay for egress mint testing.
// GET /api/relay?url=<encoded full URL>  ->  fetches the URL from Vercel's egress
// IP and returns the body. Restricted to *.cryboiz.workers.dev to avoid being an
// open proxy. No signing here: the caller passes the already-signed resolver URL.
const RELAY_SECRET = process.env.RELAY_SECRET || "p4k_sec_7a9f82d1e0b5c439";

export default async function handler(req, res) {
  const auth = req.headers["x-relay-key"] || (req.query && req.query.key);
  if (RELAY_SECRET && auth !== RELAY_SECRET) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }

  const raw = req.query && req.query.url;
  const target = Array.isArray(raw) ? raw[0] : raw;
  if (!target) {
    res.status(400).json({ error: "missing url" });
    return;
  }

  let host;
  try { host = new URL(target).hostname; } catch { res.status(400).json({ error: "bad url" }); return; }
  if (!/(^|\.)cryboiz\.workers\.dev$/i.test(host) && host !== "api.phim4k.space") {
    res.status(403).json({ error: "host not allowed" });
    return;
  }

  const fwdHeaders = {
    Accept: "application/json",
    "User-Agent": req.headers["x-user-agent"] || req.headers["user-agent"] || "okhttp/4.9.0"
  };
  if (req.headers["api-key"]) fwdHeaders["API-KEY"] = req.headers["api-key"];
  if (req.headers["authorization"]) fwdHeaders["Authorization"] = req.headers["authorization"];

  try {
    const r = await fetch(target, {
      headers: fwdHeaders,
      redirect: "follow",
    });
    const body = await r.text();
    res.status(r.status);
    res.setHeader("content-type", r.headers.get("content-type") || "application/json");
    res.setHeader("x-relay-host", host);
    res.send(body);
  } catch (e) {
    res.status(502).json({ error: String((e && e.message) || e) });
  }
}
