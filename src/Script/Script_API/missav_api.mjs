import fs from "node:fs/promises";
import path from "node:path";
import { DATA_DIR, MIDDLE_DATA_DIR } from "./missav_session.mjs";

export const MISSAV_SAVED_URL = "https://missav.ai/cn/saved";
export const MISSAV_ASSET_DIR = path.join(MIDDLE_DATA_DIR, "missav_assets");
export const MISSAV_OUTPUT_FILE = path.join(DATA_DIR, "Data_End", "missav.json");

const FIELD_LABELS = ["发行日期", "番号", "标题", "女优", "男优", "类型", "系列", "发行商", "导演", "标签"];
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function gotoWithRetry(page, url, options = {}) {
  const attempts = options.attempts ?? 3;
  const waitUntil = options.waitUntil ?? "domcontentloaded";
  const timeout = options.timeout ?? 60000;
  let lastError;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      await page.goto(url, { waitUntil, timeout });
      return;
    } catch (error) {
      lastError = error;
      console.log(`[MissAV] 页面打开失败，重试 ${attempt}/${attempts}: ${url}`);
      await sleep(1500 * attempt);
    }
  }

  throw lastError;
}

function safeName(value) {
  return String(value ?? "")
    .toLowerCase()
    .replace(/^https?:\/\//, "")
    .replace(/[^a-z0-9._-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 120) || "unknown";
}

export function toSimplifiedMissavUrl(rawUrl) {
  const url = new URL(rawUrl);
  const localePrefixes = new Set(["cn", "en", "ja", "ko", "ms", "th", "de", "fr", "vi", "id", "fil", "pt"]);
  const parts = url.pathname.split("/").filter(Boolean);

  if (parts[0] === "cn" && /^dm\d+$/i.test(parts[1] || "")) {
    parts.shift();
    parts.splice(1, 0, "cn");
  } else if (/^dm\d+$/i.test(parts[0] || "")) {
    if (parts[1] !== "cn") {
      if (localePrefixes.has(parts[1])) parts[1] = "cn";
      else parts.splice(1, 0, "cn");
    }
  } else if (parts[0] !== "cn") {
    if (localePrefixes.has(parts[0])) parts[0] = "cn";
    else parts.unshift("cn");
  }

  url.pathname = `/${parts.join("/")}`;
  return url.toString();
}

async function waitForThumbnails(page) {
  try {
    await page.locator(".thumbnail").first().waitFor({ state: "attached", timeout: 60000 });
  } catch (error) {
    const state = await page.evaluate(() => ({
      url: location.href,
      title: document.title,
      text: (document.body?.innerText || "").replace(/\s+/g, " ").trim().slice(0, 300),
      thumbnailCount: document.querySelectorAll(".thumbnail").length,
      linkCount: document.querySelectorAll("a[href]").length,
    })).catch(() => null);
    if (state) {
      console.log(`[MissAV] 未找到收藏卡片，当前页面：${state.url}`);
      console.log(`[MissAV] 页面标题：${state.title || "无标题"}`);
      console.log(`[MissAV] 页面内容预览：${state.text || "无内容"}`);
    }
    throw error;
  }
}

async function downloadAsset(url, outputPath, referer) {
  const response = await fetch(url, {
    headers: {
      Referer: referer,
      ...(url.endsWith(".mp4") ? { Range: "bytes=0-" } : {}),
      "User-Agent": USER_AGENT,
    },
  });
  if (!response.ok && response.status !== 206) {
    throw new Error(`资源下载失败，状态码 ${response.status}：${url}`);
  }
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, Buffer.from(await response.arrayBuffer()));
}

async function getSavedPageUrls(page) {
  await gotoWithRetry(page, MISSAV_SAVED_URL);
  await waitForThumbnails(page);
  const urls = await page.evaluate(() => {
    const links = [...document.querySelectorAll("a[href]")]
      .map((anchor) => ({ text: (anchor.textContent || "").trim(), href: anchor.href }))
      .filter((link) => /^\d+$/.test(link.text) && link.href.includes("/saved"));
    return links.map((link) => link.href);
  });
  const unique = [...new Set([MISSAV_SAVED_URL, ...urls])];
  unique.sort((a, b) => {
    const pageOf = (value) => Number(new URL(value).searchParams.get("page") || (value === MISSAV_SAVED_URL ? 1 : 0));
    return pageOf(a) - pageOf(b);
  });
  return unique;
}

async function extractSavedItemsFromPage(page, pageUrl) {
  await gotoWithRetry(page, pageUrl);
  await waitForThumbnails(page);
  return page.locator(".thumbnail").evaluateAll((cards) => {
    const seen = new Set();
    const clean = (value) => String(value ?? "").replace(/\s+/g, " ").trim();
    const isVideoHref = (href) => {
      try {
        const url = new URL(href);
        if (!url.hostname.includes("missav.ai")) return false;
        if (/\.(css|js|png|jpg|jpeg|gif|svg|ico|woff2?)$/i.test(url.pathname)) return false;
        const last = url.pathname.replace(/\/$/, "").split("/").pop() || "";
        return !/(saved|login|account|genre|actresses|makers|labels|series|search|cdn-cgi|fonts|img|build)$/i.test(last);
      } catch {
        return false;
      }
    };

    return cards
      .map((card) => {
        const img = card.querySelector("img");
        const anchor = card.querySelector("a[href*='missav.ai/']");
        const video = card.querySelector("video");
        const href = anchor?.href || "";
        const titleFromLink = clean(anchor?.getAttribute("title") || anchor?.textContent || "");
        const titleFromCard = clean(card?.textContent)
          .replace(/^(\u65e0\u7801\u5f71\u7247|\u4e2d\u6587\u5b57\u5e55|\u7121\u78bc\u5f71\u7247)\s+/u, "")
          .replace(/^\d{1,2}:\d{2}(?::\d{2})?\s+/, "");
        const duration = card?.querySelector(".absolute.bottom-1.right-1, .bottom-1.right-1, [class*='bottom'][class*='right']")?.textContent || "";
        const badge = card?.querySelector(".absolute.bottom-1.left-1, .bottom-1.left-1, [class*='bottom'][class*='left']")?.textContent || "";
        return {
          title: titleFromLink || titleFromCard || clean(img?.alt || img?.getAttribute("title") || ""),
          href,
          cover: img?.currentSrc || img?.src || "",
          preview: video?.currentSrc || video?.src || video?.getAttribute("data-src") || "",
          duration: clean(duration),
          badge: clean(badge),
        };
      })
      .filter((item) => {
        if (!item.href || !item.cover || !item.title) return false;
        if (!/[A-Z]{2,6}-?\d{2,5}/i.test(`${item.href} ${item.title}`)) return false;
        if (!isVideoHref(item.href)) return false;
        if (seen.has(item.href)) return false;
        seen.add(item.href);
        return true;
      });
  });
}

async function clickByText(page, text) {
  return page.evaluate((needle) => {
    const node = [...document.querySelectorAll("button, a, span, div")]
      .find((element) => (element.textContent || "").replace(/\s+/g, " ").trim().includes(needle));
    if (!node) return false;
    node.click();
    return true;
  }, text);
}

async function extractDetail(page, href) {
  const detailUrl = toSimplifiedMissavUrl(href);
  await gotoWithRetry(page, detailUrl, { attempts: 4 });
  await clickByText(page, "显示更多").catch(() => false);
  await sleep(600);
  await clickByText(page, "磁力下载").catch(() => false);
  await sleep(600);

  return page.evaluate((labels) => {
    const clean = (value) => String(value ?? "").replace(/\s+/g, " ").trim();
    const unique = (values) => {
      const seen = new Set();
      return values.filter((value) => {
        if (!value || seen.has(value)) return false;
        seen.add(value);
        return true;
      });
    };
    const fieldPattern = new RegExp(`^(${labels.join("|")}):\\s*(.+)$`);
    const lines = document.body.innerText.split(/\r?\n/).map(clean).filter(Boolean);

    const detailIndex = lines.findIndex((line) => line === "详情");
    const showMoreIndex = lines.findIndex((line, index) => index > detailIndex && line.includes("显示更多"));
    const firstFieldIndex = lines.findIndex((line, index) => index > detailIndex && fieldPattern.test(line));
    const descriptionEnd = [showMoreIndex, firstFieldIndex].filter((index) => index > detailIndex).sort((a, b) => a - b)[0];
    const description = detailIndex >= 0 && descriptionEnd > detailIndex
      ? clean(lines.slice(detailIndex + 2, descriptionEnd).join(" "))
      : "";

    const fields = {};
    for (const line of lines) {
      const match = line.match(fieldPattern);
      if (!match || fields[match[1]]) continue;
      let value = match[2].trim();
      const footerIndex = value.search(/\bAD\b|MISSAV|\u514d\u8d39\u9ad8\u6e05|\u5f71\u7247\s+\u6700\u8fd1\u66f4\u65b0/i);
      if (footerIndex > 0) value = value.slice(0, footerIndex).trim();
      fields[match[1]] = value;
    }

    const visibleMagnetLinks = unique(
      [...document.querySelectorAll("a[href]")]
        .map((anchor) => anchor.href)
        .filter((link) => /^magnet:/i.test(link)),
    );

    return { url: location.href, description, fields, visibleMagnetLinks };
  }, FIELD_LABELS);
}

async function downloadItemAssets(item) {
  const slug = item.href.split("/").filter(Boolean).pop() ?? safeName(item.title);
  const itemDir = path.join(MISSAV_ASSET_DIR, safeName(slug));
  if (item.cover) {
    await downloadAsset(item.cover, path.join(itemDir, "cover.jpg"), item.href);
  }
  if (item.preview) {
    await downloadAsset(item.preview, path.join(itemDir, "preview.mp4"), item.href);
  }
}

export async function collectMissavArchive(context, options = {}) {
  const listPage = await context.newPage();
  const detailPage = await context.newPage();
  const downloadAssets = options.downloadAssets ?? true;
  const pageUrls = await getSavedPageUrls(listPage);
  const seen = new Set();
  const output = [];

  for (let pageIndex = 0; pageIndex < pageUrls.length; pageIndex += 1) {
    const pageUrl = pageUrls[pageIndex];
    console.log(`[MissAV] 抓取收藏页 ${pageIndex + 1}/${pageUrls.length}: ${pageUrl}`);
    const pageItems = await extractSavedItemsFromPage(listPage, pageUrl);

    for (let itemIndex = 0; itemIndex < pageItems.length; itemIndex += 1) {
      const item = pageItems[itemIndex];
      const simplifiedHref = toSimplifiedMissavUrl(item.href);
      if (seen.has(simplifiedHref)) continue;
      seen.add(simplifiedHref);

      console.log(`[MissAV] 抓取详情 ${output.length + 1}: ${item.title}`);
      const detail = await extractDetail(detailPage, simplifiedHref);
      const merged = {
        title: item.title,
        href: detail.url || simplifiedHref,
        cover: item.cover,
        preview: item.preview,
        duration: item.duration,
        badge: item.badge,
        description: detail.description,
        fields: detail.fields,
        visibleMagnetLinks: detail.visibleMagnetLinks,
      };

      output.push(merged);
      if (downloadAssets) await downloadItemAssets(merged);
      await sleep(800);
    }
  }

  await listPage.close().catch(() => {});
  await detailPage.close().catch(() => {});
  return output;
}

export async function saveMissavArchive(items, outputFile = MISSAV_OUTPUT_FILE) {
  await fs.mkdir(path.dirname(outputFile), { recursive: true });
  await fs.writeFile(outputFile, JSON.stringify(items, null, 2), "utf8");
  return outputFile;
}
