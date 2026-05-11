import { chromium } from "file:///C:/Users/28484/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs";
import fs from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import { spawn } from "node:child_process";
import readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { fileURLToPath } from "node:url";

export const SRC_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const CONFIG_DIR = path.join(SRC_ROOT, "config");
export const DATA_DIR = path.join(SRC_ROOT, "data");
export const MIDDLE_DATA_DIR = path.join(DATA_DIR, "middle_data");
export const MISSAV_LOGIN_PROFILE_ROOT = path.resolve(SRC_ROOT, "..", ".codex-missav-browser-profile");
export const MISSAV_COOKIE_FILE = path.join(CONFIG_DIR, "missav_cookie.json");

const CHROME_PATH = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe";
const AUTH_SAVED_URL = "https://missav.ai/cn/saved";

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function pathExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function gotoWithRetry(page, url, attempts = 4) {
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
      return;
    } catch (error) {
      lastError = error;
      console.log(`[MissAV] 页面打开失败，重试 ${attempt}/${attempts}: ${url}`);
      await sleep(1500 * attempt);
    }
  }
  throw lastError;
}

async function waitForCdp(cdpUrl, timeoutMs = 30000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const response = await fetch(`${cdpUrl}/json/version`);
      if (response.ok) return;
    } catch {
      // Chrome 可能还在启动。
    }
    await sleep(500);
  }
  throw new Error(`Chrome 调试端口未启动: ${cdpUrl}`);
}

async function getFreePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.unref();
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : undefined;
      server.close(() => {
        if (!port) reject(new Error("无法分配本地空闲端口。"));
        else resolve(port);
      });
    });
  });
}

async function launchNormalChromeForLogin() {
  const cdpPort = await getFreePort();
  const loginProfileDir = MISSAV_LOGIN_PROFILE_ROOT;
  await fs.mkdir(loginProfileDir, { recursive: true });
  const args = [
    `--remote-debugging-port=${cdpPort}`,
    `--user-data-dir=${loginProfileDir}`,
    "--no-first-run",
    "--no-default-browser-check",
    "--new-window",
    AUTH_SAVED_URL,
  ];
  const child = spawn(CHROME_PATH, args, {
    detached: true,
    stdio: "ignore",
    windowsHide: false,
  });
  child.unref();

  const cdpUrl = `http://127.0.0.1:${cdpPort}`;
  await waitForCdp(cdpUrl);
  return cdpUrl;
}

async function isLoggedIn(page, timeoutMs = 12000) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    try {
      const state = await page.evaluate(() => {
        const text = document.body?.innerText || "";
        const videoLinks = [...document.querySelectorAll("a[href]")].filter((anchor) =>
          /missav\.ai\/dm\d+\/.*[a-z]{2,8}-?\d{2,6}/i.test(anchor.href || ""),
        ).length;
        const coverImages = [...document.querySelectorAll("img")].filter((img) =>
          /cover|fourhoi|missav/i.test(img.currentSrc || img.src || ""),
        ).length;
        const thumbnailCards = document.querySelectorAll(".thumbnail").length;
        const hasLoginPrompt =
          text.includes("登入你的帳戶")
          || text.includes("登入你的帐户")
          || text.includes("登录你的帐户");
        const hasErrorPage =
          text.includes("Oops! An Error Occurred")
          || text.includes("Method Not Allowed")
          || text.includes("403 Forbidden")
          || text.includes("405 Method Not Allowed");
        return { url: location.href, text, videoLinks, coverImages, thumbnailCards, hasLoginPrompt, hasErrorPage };
      });

      const onSavedPage = /missav\.ai\/(?:cn\/)?saved/i.test(state.url);
      const hasSavedTitle =
        state.text.includes("我的影片收藏")
        || state.text.includes("我的视频收藏")
        || state.text.includes("我的收藏");
      const hasCards = state.thumbnailCards >= 1 && state.videoLinks >= 1;

      if (state.hasErrorPage || state.hasLoginPrompt) return false;
      if (onSavedPage && hasSavedTitle && hasCards) return true;
    } catch {
      // 页面可能还在加载或跳转。
    }

    await sleep(500);
  }

  return false;
}

async function openSavedPage(context) {
  const pages = context.pages();
  const page = pages.find((candidate) => /missav\.ai\/(?:cn\/)?saved/i.test(candidate.url())) ?? await context.newPage();
  await gotoWithRetry(page, AUTH_SAVED_URL);
  return page;
}

async function saveSessionState(context) {
  await fs.mkdir(CONFIG_DIR, { recursive: true });
  const storageState = await context.storageState();
  await fs.writeFile(MISSAV_COOKIE_FILE, JSON.stringify(storageState, null, 2), "utf8");
}

async function waitForUserLoginConfirmation() {
  const rl = readline.createInterface({ input, output });
  try {
    await rl.question("[MissAV] 请在弹出的 Chrome 中登录。确认收藏页加载出来后，回到这里按 Enter：");
  } finally {
    rl.close();
  }
}

async function launchHeadlessWithSavedState() {
  if (!(await pathExists(MISSAV_COOKIE_FILE))) return null;

  const browser = await chromium.launch({
    executablePath: CHROME_PATH,
    headless: true,
  });
  const context = await browser.newContext({
    storageState: MISSAV_COOKIE_FILE,
    viewport: { width: 1366, height: 900 },
  });
  const page = await openSavedPage(context);

  if (await isLoggedIn(page)) {
    return { browser, context, page, shouldCloseBrowser: true, mode: "headless" };
  }

  await browser.close().catch(() => {});
  return null;
}

async function loginWithVisibleChrome() {
  const cdpUrl = await launchNormalChromeForLogin();
  console.log("[MissAV] 已打开普通 Chrome 登录窗口。登录阶段不会自动控制浏览器。");
  await waitForUserLoginConfirmation();

  const browser = await chromium.connectOverCDP(cdpUrl);
  const context = browser.contexts()[0] ?? await browser.newContext();
  const page = context.pages().find((candidate) => /missav\.ai\/(?:cn\/)?saved/i.test(candidate.url())) ?? await openSavedPage(context);

  if (!(await isLoggedIn(page, 30000))) {
    await browser.close().catch(() => {});
    throw new Error("按 Enter 后仍未检测到 MissAV 登录态。");
  }

  await saveSessionState(context);
  return { browser, context, page, shouldCloseBrowser: true, mode: "visible" };
}

export async function ensureMissavSession() {
  console.log("[MissAV] 正在检查后台登录态。");
  const headlessSession = await launchHeadlessWithSavedState();
  if (headlessSession) {
    console.log("[MissAV] 后台登录态可用，将使用无头浏览器执行。");
    return headlessSession;
  }

  console.log("[MissAV] 后台登录态不可用，需要弹出浏览器登录一次。");
  const visibleSession = await loginWithVisibleChrome();

  console.log("[MissAV] 登录态已保存，尝试切回无头浏览器。");
  const refreshedHeadlessSession = await launchHeadlessWithSavedState();
  if (refreshedHeadlessSession) {
    await visibleSession.browser.close().catch(() => {});
    console.log("[MissAV] 无头浏览器可用，将在后台继续执行。");
    return refreshedHeadlessSession;
  }

  console.log("[MissAV] 当前站点不接受无头浏览器会话，将复用已登录的普通 Chrome 执行本次任务。");
  return visibleSession;
}
