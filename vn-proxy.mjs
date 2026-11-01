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

  async loadProxies() {
    if (this.cachedList.length > 0) return this.cachedList;
    const sources = [
      "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/countries/VN/data.txt",
      "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=all&timeout=5000&country=VN&ssl=all&anonymity=all"
    ];
    for (const src of sources) {
      try {
        const res = await fetch(src, { signal: AbortSignal.timeout(6000) });
        if (res.ok) {
          const text = await res.text();
          const lines = text.split("\n").map(l => l.trim()).filter(Boolean);
          if (lines.length > 0) {
            this.cachedList = lines;
            console.log(`[Proxy VN] Tải thành công ${lines.length} proxy từ ${src.slice(0, 50)}...`);
            return this.cachedList;
          }
        }
      } catch (e) {
        console.warn(`[Proxy VN] Lỗi tải danh sách từ ${src}: ${e.message}`);
      }
    }
    return [];
  }

  async findWorkingProxy() {
    const list = await this.loadProxies();
    if (!list.length) throw new Error("Không có proxy VN nào trong danh sách!");

    console.log(`[Proxy VN] Đang tìm proxy VN sống (đã loại ${this.deadProxies.size} proxy hỏng)...`);
    const candidates = list.filter(p => !this.deadProxies.has(p));
    // Ưu tiên SOCKS5 và HTTP/HTTPS trước SOCKS4 để tốc độ và độ ổn định cao nhất
    candidates.sort((a, b) => {
      const score = (p) => p.startsWith("socks5://") ? 2 : (p.startsWith("http") ? 1 : 0);
      return score(b) - score(a);
    });
    const batchSize = 25;

    for (let i = 0; i < candidates.length; i += batchSize) {
      const batch = candidates.slice(i, i + batchSize);
      try {
        const winner = await Promise.any(
          batch.map(p => new Promise((resolve, reject) => {
            const cmd = `curl -s -x "${p}" --compressed -m 4 -H "Accept: application/json" -H "API-KEY: bbbb411dea44849" -H "Authorization: Basic YWRtaW46" -H "User-Agent: okhttp/4.9.0" "https://api.phim4k.space/rest-api/v130/movies?page=1"`;
            execAsync(cmd)
              .then(({ stdout }) => {
                if (stdout && stdout.startsWith("[") && stdout.includes("videos_id")) {
                  resolve({ proxy: p, sample: stdout });
                } else {
                  this.deadProxies.add(p);
                  reject();
                }
              })
              .catch(() => {
                this.deadProxies.add(p);
                reject();
              });
          }))
        );

        this.activeProxy = winner.proxy;
        console.log(`[Proxy VN] => Đã kết nối thành công qua Proxy VN: ${this.activeProxy}`);
        return winner;
      } catch {}
    }
    throw new Error("Đã thử hết danh sách nhưng không tìm được proxy VN nào phản hồi 200 OK!");
  }

  async fetchJson(url, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      if (!this.activeProxy) {
        const win = await this.findWorkingProxy();
        if (url.includes("movies?page=1") && win && win.sample) {
          try { return JSON.parse(win.sample); } catch {}
        }
      }

      const cmd = `curl -s -x "${this.activeProxy}" --compressed -m 12 -H "Accept: application/json" -H "API-KEY: bbbb411dea44849" -H "Authorization: Basic YWRtaW46" -H "User-Agent: okhttp/4.9.0" "${url}"`;
      try {
        const { stdout } = await execAsync(cmd);
        if (!stdout || stdout.trim().length === 0) throw new Error("Empty response");
        const data = JSON.parse(stdout);
        if (data && (Array.isArray(data) || data.videos_id || data.status !== "error")) {
          return data;
        }
        throw new Error("Invalid payload: " + stdout.slice(0, 100));
      } catch (e) {
        console.warn(`[Proxy VN] Proxy ${this.activeProxy} gặp lỗi (${e.message}), đổi proxy khác (lần thử ${attempt}/${maxRetries})...`);
        this.deadProxies.add(this.activeProxy);
        this.activeProxy = null;
      }
    }
    return null;
  }
}

export const vnProxy = new VNProxyManager();
