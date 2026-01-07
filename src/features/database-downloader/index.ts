import path from "path";
import { app } from "electron";
import fs from "fs/promises";
import { logToFile, settings, TransferClient } from "../../utils";

const REMOTE_DIR = "/Database_Download";

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

  try {
    await client.connect();

    const remoteEntries = await client.list(REMOTE_DIR);

    const bakFiles = remoteEntries
      .filter((f) => f.type === "file" && f.name.toLowerCase().endsWith(".bak"));

    if (bakFiles.length === 0) {
      logToFile("database-downloader", `[SFTP] No .bak files found in ${REMOTE_DIR}`);
      return [];
    }

    for (const file of bakFiles) {
      const safeName = path.basename(file.name);

      const remoteFile = path.posix.join(REMOTE_DIR, safeName);
      const localFile = path.join(LOCAL_DIR, safeName);

      try {
        const stat = await fs.stat(localFile);
        if (stat.size === file.size && stat.size > 0) {
          logToFile("database-downloader", `[SFTP] Skipping (already downloaded): ${localFile}`);
          continue;
        }
      } catch {
      }

      try {
        logToFile("database-downloader", `[SFTP] Downloading ${remoteFile} -> ${localFile}`);
        await client.get(remoteFile, localFile);

        downloaded.push(localFile);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logToFile("database-downloader", `[SFTP] Failed downloading ${remoteFile}: ${msg}`);
      }
    }

    return downloaded;
  } finally {
    await client.end();
  }
}
