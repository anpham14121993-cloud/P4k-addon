// Cloudflare Worker pass-through relay for Phim4K CDN resolvers (*.cryboiz.workers.dev)
// and upstream API. Supports both HTTP fetch and Cloudflare Service Binding calls.

const DEFAULT_RELAY_SECRET = "p4k_sec_7a9f82d1e0b5c439";

function jsonError(status, message) {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const relaySecret = (env && env.RELAY_SECRET) || DEFAULT_RELAY_SECRET;
    const auth = request.headers.get("x-relay-key") || url.searchParams.get("key");
    if (relaySecret && auth !== relaySecret) {
      return jsonError(401, "unauthorized");
    }

    const target = url.searchParams.get("url");
    if (!target) {
      return jsonError(400, "missing url");
    }

    let host;
    try {
      host = new URL(target).hostname;
    } catch {
      return jsonError(400, "bad url");
    }

    if (!/(^|\.)cryboiz\.workers\.dev$/i.test(host) && host !== "api.phim4k.space") {
      return jsonError(403, "host not allowed");
    }

    const fwdHeaders = new Headers({
      Accept: "application/json",
    });
    const xUa = request.headers.get("x-user-agent");
    if (xUa) fwdHeaders.set("User-Agent", xUa);
    const apiKey = request.headers.get("api-key");
    if (apiKey) fwdHeaders.set("API-KEY", apiKey);
    const authorization = request.headers.get("authorization");
    if (authorization) fwdHeaders.set("Authorization", authorization);

    try {
      const upstream = await fetch(target, {
        headers: fwdHeaders,
        redirect: "follow",
      });
      const body = await upstream.text();
      return new Response(body, {
        status: upstream.status,
        headers: {
          "content-type": upstream.headers.get("content-type") || "application/json",
          "x-relay-host": host,
          "x-relay-colo": (request.cf && request.cf.colo) || "cf",
        },
      });
    } catch (e) {
      return jsonError(502, String((e && e.message) || e));
    }
  },
};
