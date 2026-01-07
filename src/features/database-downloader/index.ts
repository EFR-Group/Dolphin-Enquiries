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

async function safeStatSize(filePath: string): Promise<number> {
  try {
    const stat = await fs.stat(filePath);
    return stat.size;
  } catch {
    return 0;
  }
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

    // ✅ local write test (catches permissions / Defender Controlled Folder Access)
    try {
      const testFile = path.join(LOCAL_DIR, ".write-test.tmp");
      await fs.writeFile(testFile, "ok");
      await fs.unlink(testFile);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logToFile(
        "database-downloader",
        `[SFTP] ❌ Local write test failed in ${LOCAL_DIR}: ${msg}`
      );
      throw err;
    }

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

      // skip if already downloaded
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
        // ignore
      }

      // ✅ Download to temp file first
      const tempFile = `${localFile}.downloading`;

      // Clean up any old temp file from previous run
      try {
        await fs.unlink(tempFile);
      } catch {
        // ignore
      }

      const start = Date.now();
      let heartbeat: NodeJS.Timeout | null = null;

      // ✅ Set a timeout so downloads can't hang forever
      const DOWNLOAD_TIMEOUT_MS = 2 * 60 * 60 * 1000; // 2 hours

      try {
        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) Downloading (temp): ${remoteFile} -> ${tempFile}`
        );

        // ✅ Heartbeat logs while downloading
        heartbeat = setInterval(async () => {
          const localSoFar = await safeStatSize(tempFile);
          const elapsed = Date.now() - start;

          // Only spam logs if something is happening OR every 10 seconds
          logToFile(
            "database-downloader",
            `[SFTP] (${index}/${bakFiles.length}) ⏳ In progress: ${safeName} | local=${formatBytes(
              localSoFar
            )} (${remoteSize > 0 ? ((localSoFar / remoteSize) * 100).toFixed(1) : "?"}%) | elapsed=${formatMs(
              elapsed
            )}`
          );
        }, 2000);

        // ✅ Race download with timeout
        await Promise.race([
          client.get(remoteFile, tempFile),
          new Promise((_, reject) =>
            setTimeout(
              () =>
                reject(
                  new Error(`Download timed out after ${formatMs(DOWNLOAD_TIMEOUT_MS)}`)
                ),
              DOWNLOAD_TIMEOUT_MS
            )
          ),
        ]);

        if (heartbeat) clearInterval(heartbeat);

        const finalSize = await safeStatSize(tempFile);

        if (finalSize <= 0) {
          throw new Error(`Download completed but temp file is 0 bytes: ${tempFile}`);
        }

        // Rename into place
        await fs.rename(tempFile, localFile);

        const elapsed = Date.now() - start;
        downloaded.push(localFile);
        downloadedBytes += finalSize;

        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) ✅ Downloaded: ${safeName} | bytes=${formatBytes(
            finalSize
          )} | time=${formatMs(elapsed)} | rate=${formatRate(finalSize, elapsed)}`
        );
      } catch (err) {
        if (heartbeat) clearInterval(heartbeat);

        failedCount++;
        const msg = err instanceof Error ? err.message : String(err);
        const elapsed = Date.now() - start;

        const sizeSoFar = await safeStatSize(tempFile);

        logToFile(
          "database-downloader",
          `[SFTP] (${index}/${bakFiles.length}) ❌ Failed: ${remoteFile} | time=${formatMs(
            elapsed
          )} | downloadedSoFar=${formatBytes(sizeSoFar)} | error=${msg}`
        );

        // leave temp file for investigation if it has data; otherwise clean it up
        try {
          if (sizeSoFar === 0) {
            await fs.unlink(tempFile);
          }
        } catch {
          // ignore
        }
      }

      if (index % 5 === 0 || index === bakFiles.length) {
        const overallElapsed = Date.now() - overallStart;
        logToFile(
          "database-downloader",
          `[SFTP] Progress: ${index}/${bakFiles.length} processed | downloaded=${downloaded.length} (${formatBytes(
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
