import path from "path";
import { app } from "electron";
import fs from "fs/promises";
import { logToFile, settings, TransferClient } from "../../utils";

const REMOTE_DIR = "/Database_Download";

function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  let value = bytes;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return `${value.toFixed(i === 0 ? 0 : 2)} ${units[i]}`;
}

function formatMs(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "0ms";
  if (ms < 1000) return `${ms.toFixed(0)}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)}s`;
  const m = Math.floor(s / 60);
  const remS = s % 60;
  return `${m}m ${remS.toFixed(1)}s`;
}

function formatRate(bytes: number, ms: number): string {
  if (ms <= 0) return "n/a";
  const bytesPerSec = bytes / (ms / 1000);
  return `${formatBytes(bytesPerSec)}/s`;
}

/**
 * Downloads all .bak files from SFTP Three's database download directory.
 * @returns A promise resolving to an array of paths to the downloaded .bak files.
 * @throws An error if the SFTP Three config is missing or incomplete.
 */
export async function downloadBakFilesFromSftpThree(): Promise<string[]> {
  await app.whenReady();

  const LOCAL_DIR = path.join(app.getPath("documents"), "DolphinBackups");

  const config = await settings.getSFTPConfigThree();
  if (!config) {
    throw new Error("SFTP Three config is missing or incomplete.");
  }

  await fs.mkdir(LOCAL_DIR, { recursive: true });

  const client = new TransferClient(config);
  const downloaded: string[] = [];

  const overallStart = Date.now();

  let skippedCount = 0;
  let failedCount = 0;
  let downloadedBytes = 0;
  let skippedBytes = 0;

  try {
    logToFile(
      "database-downloader",
      `[SFTP] Starting .bak download | Remote: ${REMOTE_DIR} | Local: ${LOCAL_DIR}`
    );

    await client.connect();
    logToFile("database-downloader", `[SFTP] Connected`);

    const remoteEntries = await client.list(REMOTE_DIR);

    const bakFiles = remoteEntries.filter(
      (f) => f.type === "file" && f.name.toLowerCase().endsWith(".bak")
    );

    if (bakFiles.length === 0) {
      logToFile("database-downloader", `[SFTP] No .bak files found in ${REMOTE_DIR}`);
      return [];
    }

    const totalRemoteBytes = bakFiles.reduce((sum, f) => sum + (f.size ?? 0), 0);

    logToFile(
      "database-downloader",
      `[SFTP] Found ${bakFiles.length} .bak file(s) (${formatBytes(totalRemoteBytes)}) in ${REMOTE_DIR}`
    );

    for (let i = 0; i < bakFiles.length; i++) {
      const file = bakFiles[i];
      const index = i + 1;

      const safeName = path.basename(file.name);
      const remoteFile = path.posix.join(REMOTE_DIR, safeName);
      const localFile = path.join(LOCAL_DIR, safeName);

      const remoteSize = file.size ?? 0;

      logToFile(
        "database-downloader",
        `[SFTP] (${index}/${bakFiles.length}) ${safeName} | size=${formatBytes(remoteSize)}`
      );

      try {
        const stat = await fs.stat(localFile);
        if (stat.size === remoteSize && stat.size > 0) {
          skippedCount++;
          skippedBytes += stat.size;

          logToFile(
            "database-downloader",
            `[SFTP] (${index}/${bakFiles.length}) Skipping (already downloaded): ${localFile} (${formatBytes(
              stat.size
            )})`
          );
          continue;
        }

        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) Local file exists but size mismatch: local=${formatBytes(
            stat.size
          )} remote=${formatBytes(remoteSize)} -> re-downloading`
        );
      } catch {
      }

      const start = Date.now();
      try {
        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) Downloading: ${remoteFile} -> ${localFile}`
        );

        await client.get(remoteFile, localFile);

        let localSize = 0;
        try {
          const stat = await fs.stat(localFile);
          localSize = stat.size;
        } catch {
          logToFile(
            "database-downloader",
            `[SFTP] (${index}/${bakFiles.length}) Warning: download completed but local stat failed: ${localFile}`
          );
        }

        const elapsed = Date.now() - start;
        downloaded.push(localFile);

        downloadedBytes += localSize || remoteSize;

        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) ✅ Downloaded: ${safeName} | bytes=${formatBytes(
            localSize || remoteSize
          )} | time=${formatMs(elapsed)} | rate=${formatRate(localSize || remoteSize, elapsed)}`
        );
      } catch (err) {
        failedCount++;
        const msg = err instanceof Error ? err.message : String(err);

        const elapsed = Date.now() - start;
        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) Failed: ${remoteFile} | time=${formatMs(
            elapsed
          )} | error=${msg}`
        );
      }

      if (index % 5 === 0 || index === bakFiles.length) {
        const done = index;
        const overallElapsed = Date.now() - overallStart;
        logToFile(
          "database-downloader",
          `[SFTP] Progress: ${done}/${bakFiles.length} processed | downloaded=${downloaded.length} (${formatBytes(
            downloadedBytes
          )}) | skipped=${skippedCount} (${formatBytes(skippedBytes)}) | failed=${failedCount} | elapsed=${formatMs(
            overallElapsed
          )}`
        );
      }
    }

    const overallElapsed = Date.now() - overallStart;

    logToFile(
      "database-downloader",
      `[SFTP] Finished | downloaded=${downloaded.length} (${formatBytes(
        downloadedBytes
      )}) | skipped=${skippedCount} (${formatBytes(skippedBytes)}) | failed=${failedCount} | totalTime=${formatMs(
        overallElapsed
      )}`
    );

    return downloaded;
  } finally {
    try {
      await client.end();
      logToFile("database-downloader", `[SFTP] Connection closed`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logToFile("database-downloader", `[SFTP] Warning: failed closing connection: ${msg}`);
    }
  }
}
