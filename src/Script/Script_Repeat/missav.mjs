import { ensureMissavSession } from "../Script_API/missav_session.mjs";
import { collectMissavArchive, MISSAV_OUTPUT_FILE, saveMissavArchive } from "../Script_API/missav_api.mjs";

async function main() {
  console.log("[MissAV] 开始执行收藏归档任务。");
  const session = await ensureMissavSession();

  try {
    const items = await collectMissavArchive(session.context, { downloadAssets: true });
    const outputFile = await saveMissavArchive(items, MISSAV_OUTPUT_FILE);
    console.log(`[MissAV] 任务完成，共归档 ${items.length} 条。`);
    console.log(`[MissAV] 输出文件：${outputFile}`);
  } finally {
    if (session.shouldCloseBrowser) {
      await session.browser.close().catch(() => {});
    }
  }
}

main().catch((error) => {
  console.error("[MissAV] 任务失败。");
  console.error(error);
  process.exit(1);
});
