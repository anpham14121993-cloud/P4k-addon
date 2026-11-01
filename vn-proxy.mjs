// vn-proxy.mjs — Quản lý và tự động xoay vòng Proxy Việt Nam (vượt Cloudflare WAF Geo-Block).
import { exec } from "child_process";
import { promisify } from "util";

const execAsync = promisify(exec);

export const IS_GH_ACTIONS = !!process.env.GITHUB_ACTIONS;

export const P4K_HEADERS = {
  "Accept": "application/json",
  "API-KEY": "bbbb411dea44849",
  "Authorization": "Basic " + Buffer.from("admin:").toString("base64"),
  "User-Agent": "okhttp/4.9.0",
};

export class VNProxyManager {
  constructor() {
    this.activeProxy = process.env.VN_PROXY || null;
    this.cachedList = [];
    this.deadProxies = new Set();
  }

  async loadProxies(forceRefresh = false) {
    if (!forceRefresh && this.cachedList.length > 0) return this.cachedList;
    const all = new Set();

    // 1. Proxifly VN (~390 proxies: http, https, socks4, socks5)
    try {
      const res = await fetch("https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/countries/VN/data.txt", { signal: AbortSignal.timeout(6000) });
      if (res.ok) {
        const text = await res.text();
        for (const l of text.split("\n")) {
          const p = l.trim();
          if (p) all.add(p);
        }
      }
    } catch (e) {
      console.warn(`[Proxy VN] Lỗi tải Proxifly: ${e.message}`);
    }

    // 2. Geonode VN API (~100 proxies hoạt động kèm protocol)
    try {
      const res = await fetch("https://proxylist.geonode.com/api/proxy-list?country=VN&limit=100&page=1&sort_by=lastChecked&sort_type=desc", { signal: AbortSignal.timeout(6000) });
      if (res.ok) {
        const json = await res.json();
        if (Array.isArray(json.data)) {
          for (const item of json.data) {
            const proto = (item.protocols && item.protocols[0]) || "http";
            all.add(`${proto}://${item.ip}:${item.port}`);
          }
        }
      }
    } catch (e) {
      console.warn(`[Proxy VN] Lỗi tải Geonode: ${e.message}`);
    }

    // 3. Proxyscrape VN (phân loại rõ socks5, socks4, http)
    for (const proto of ["socks5", "socks4", "http"]) {
      try {
        const res = await fetch(`https://api.proxyscrape.com/v2/?request=displayproxies&protocol=${proto}&timeout=5000&country=VN&ssl=all&anonymity=all`, { signal: AbortSignal.timeout(6000) });
        if (res.ok) {
          const text = await res.text();
          for (const l of text.split("\n")) {
            const p = l.trim();
            if (p) all.add(`${proto}://${p}`);
          }
        }
      } catch {}
    }

    this.cachedList = Array.from(all);
    console.log(`[Proxy VN] Tổng hợp thành công ${this.cachedList.length} proxy VN từ các nguồn.`);
    return this.cachedList;
  }

  async findWorkingProxy() {
    let list = await this.loadProxies();
    if (!list.length) {
      console.warn("[Proxy VN] Danh sách proxy trống!");
      return null;
    }

    for (let round = 1; round <= 2; round++) {
      let candidates = list.filter(p => !this.deadProxies.has(p));
      if (candidates.length === 0) {
        console.warn(`[Proxy VN] Toàn bộ ${this.deadProxies.size} proxy đã bị đánh dấu hỏng, reset bộ nhớ và quét lại vòng ${round}...`);
        this.deadProxies.clear();
        candidates = list.slice();
      }

      console.log(`[Proxy VN] Đang tìm proxy VN sống (vòng ${round}, còn ${candidates.length} ứng viên)...`);
      // Ưu tiên SOCKS5 > HTTP/HTTPS > SOCKS4
      candidates.sort((a, b) => {
        const score = (p) => p.startsWith("socks5://") ? 2 : (p.startsWith("http") ? 1 : 0);
        return score(b) - score(a);
      });

      const batchSize = 15;
      for (let i = 0; i < candidates.length; i += batchSize) {
        const batch = candidates.slice(i, i + batchSize);
        try {
          const winner = await Promise.any(
            batch.map(p => new Promise((resolve, reject) => {
              const cmd = `curl -s -x "${p}" --compressed --connect-timeout 4 -m 10 -H "Accept: application/json" -H "API-KEY: bbbb411dea44849" -H "Authorization: Basic YWRtaW46" -H "User-Agent: okhttp/4.9.0" "https://api.phim4k.space/rest-api/v130/movies?page=1"`;
              execAsync(cmd)
                .then(({ stdout }) => {
                  if (stdout && stdout.startsWith("[") && stdout.includes("videos_id")) {
                    resolve({ proxy: p, sample: stdout });
                  } else {
                    this.deadProxies.add(p);
                    reject(new Error("invalid payload"));
                  }
                })
                .catch((err) => {
                  this.deadProxies.add(p);
                  reject(err);
                });
            }))
          );

          this.activeProxy = winner.proxy;
          console.log(`[Proxy VN] => Đã kết nối thành công qua Proxy VN: ${this.activeProxy}`);
          return winner;
        } catch {}
      }
    }

    console.warn("[Proxy VN] Cảnh báo: Không tìm thấy proxy VN nào phản hồi 200 OK sau 2 vòng quét!");
    return null;
  }

  async fetchJson(url, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      if (!this.activeProxy) {
        const win = await this.findWorkingProxy();
        if (!win || !win.proxy) {
          console.warn(`[Proxy VN] Chưa tìm được proxy khả dụng (lần thử ${attempt}/${maxRetries})...`);
          continue;
        }
        if (url.includes("movies?page=1") && win && win.sample) {
          try { return JSON.parse(win.sample); } catch {}
        }
      }

      // Cho activeProxy thử tối đa 2 lần trước khi hủy bỏ
      for (let sub = 1; sub <= 2; sub++) {
        const cmd = `curl -s -x "${this.activeProxy}" --compressed --connect-timeout 8 -m 25 -H "Accept: application/json" -H "API-KEY: bbbb411dea44849" -H "Authorization: Basic YWRtaW46" -H "User-Agent: okhttp/4.9.0" "${url}"`;
        try {
          const { stdout } = await execAsync(cmd);
          if (!stdout || stdout.trim().length === 0) throw new Error("Empty response");
          const data = JSON.parse(stdout);
          if (data && (Array.isArray(data) || data.videos_id || data.status !== "error")) {
            return data;
          }
          throw new Error("Invalid payload: " + stdout.slice(0, 100));
        } catch (e) {
          if (sub === 1) {
            console.warn(`[Proxy VN] Proxy ${this.activeProxy} chập chờn (${e.message}), thử lại lần 2...`);
          } else {
            console.warn(`[Proxy VN] Proxy ${this.activeProxy} thất bại sau 2 lần (${e.message}), chuyển proxy khác (lần thử ${attempt}/${maxRetries})...`);
            this.deadProxies.add(this.activeProxy);
            this.activeProxy = null;
          }
        }
      }
    }
    return null;
  }
}

export const vnProxy = new VNProxyManager();
