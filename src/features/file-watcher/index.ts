import fs from 'fs';
import path from 'path';
import { documentsFolder, isRegularFile, logToFile, settings, TransferClient } from '../../utils';
import { ping, saveParsedTravelFolder } from '..';

let isTransferring: boolean = false;
const failureStorePath: string = path.join(documentsFolder(), "DolphinEnquiries", "cache", "file-transfer-failures.json");

/**
 * A class representing the file paths for a given file, including remote and local paths.
 */
class FilePaths {
    remoteFile: string;
    localFile: string;
    destFolder: string;
    destRemoteFile: string;

    /**
     * Creates an instance of the FilePaths class.
     * 
     * @param {string} remoteFile - The full remote file path (combines basePath and fileName).
     * @param {string} localFile - The local file path where the file will be saved.
     * @param {string} destFolder - The destination folder in the upload base.
     * @param {string} destRemoteFile - The full remote file path in the destination folder.
     */
    constructor(remoteFile: string, localFile: string, destFolder: string, destRemoteFile: string) {
        this.remoteFile = remoteFile;
        this.localFile = localFile;
        this.destFolder = destFolder;
        this.destRemoteFile = destRemoteFile;
    }
}

/**
 * @returns {Array.<Object>} List of failed file transfers.
 * @property {string} localFile - The local file path.
 * @property {string} destRemoteFile - The destination remote file path.
 * @property {string} fileName - The name of the file.
 */
export function loadFailures(): { localFile: string; destRemoteFile: string; fileName: string }[] {
    try {
        if (fs.existsSync(failureStorePath)) {
            const content = fs.readFileSync(failureStorePath, 'utf-8');
            const parsed = JSON.parse(content);
            return Array.isArray(parsed) ? parsed : [];
        }
    } catch (err) {
        logToFile("file-movements", `Failed to load failure cache: ${err instanceof Error ? err.message : String(err)}`);
    }
    return [];
}

/**
 * Saves the failed transfer cache to disk.
 * @param {Array.<Object>} failures - List of failed file transfers to save.
 * @property {string} failures.localFile - The local file path.
 * @property {string} failures.destRemoteFile - The remote destination file path.
 * @property {string} failures.fileName - The name of the file.
 */
export function saveFailures(failures: { localFile: string; destRemoteFile: string; fileName: string }[]): void {
    try {
        const dir = path.dirname(failureStorePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }
        fs.writeFileSync(failureStorePath, JSON.stringify(failures, null, 2));
    } catch (err) {
        logToFile("file-movements", `Failed to save failure cache: ${err instanceof Error ? err.message : String(err)}`);
    }
}

/**
 * Resolves file paths based on base path, upload base, and file name.
 * 
 * This function returns an instance of the FilePaths class containing the resolved paths
 * for the file's remote and local locations, as well as the destination folder and the final
 * remote file path. It determines the destination folder based on the file's name (e.g., whether 
 * it belongs to "egr" or "lwc").
 * 
 * @param {string} basePath - The base remote path (e.g., SFTP server path).
 * @param {string} uploadBase - The base path for uploading files to the destination.
 * @param {string} fileName - The name of the file being processed.
 * @returns {FilePaths} - An instance of the FilePaths class containing the resolved file paths.
 */
export function resolveFilePaths(basePath: string, uploadBase: string, fileName: string): FilePaths {
    const remoteFile = path.posix.join(basePath, fileName);
    const todayFolderName = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const localPath = path.posix.join(documentsFolder(), "DolphinEnquiries", "completed", todayFolderName);
    const localFile = path.posix.join(localPath, fileName);
    let destFolder = uploadBase;

    if (fileName.toLowerCase().startsWith('egr')) {
        destFolder = path.posix.join(destFolder, 'XML-EGR/');
    } else if (fileName.toLowerCase().startsWith('lwc')) {
        destFolder = path.posix.join(destFolder, 'XML-LWC/');
    }

    const destRemoteFile = path.posix.join(destFolder, fileName);

    return new FilePaths(remoteFile, localFile, destFolder, destRemoteFile);
}

/**
 * Tries to upload a file with retry logic.
 * @param {TransferClient} client - The transfer client to use for uploading.
 * @param {string} localFile - The local file path.
 * @param {string} destRemoteFile - The destination remote file path.
 * @param {string} fileName - The name of the file.
 * @param {number} [maxRetries=3] - The maximum number of retry attempts.
 * @returns {Promise<boolean>} - Returns true if the upload succeeds, false otherwise.
 */
export async function tryUploadWithRetry(client: TransferClient, localFile: string, destRemoteFile: string, fileName: string, maxRetries: number = 3): Promise<boolean> {
    let attempts = 0;
    while (attempts < maxRetries) {
        try {
            await client.put(localFile, destRemoteFile);
            logToFile("file-movements", `Successfully uploaded ${fileName} to client3 at ${destRemoteFile} (attempt ${attempts + 1})`);
            return true;
        } catch (err) {
            attempts++;
            logToFile("file-movements", `Upload attempt ${attempts} failed for ${fileName}: ${err instanceof Error ? err.message : String(err)}`);
            if (attempts >= maxRetries) {
                logToFile("file-movements", `All upload attempts failed for ${fileName}`);
                return false;
            }
            await new Promise(res => setTimeout(res, 1000));
        }
    }
    return false;
}

/**
 * Main function to watch and transfer files.
 * @returns {Promise<void>} - Resolves when the file transfer process is complete.
 */
export async function watchAndTransferFiles(): Promise<void> {
    const configOne = await settings.getSFTPConfigOne();
    const configTwo = await settings.getSFTPConfigTwo();
    const configThree = await settings.getSFTPConfigThree();

    if (!configOne || !configTwo || !configThree) {
        logToFile("file-movements", "SFTP configurations are not set up correctly.");
        return;
    }

    const client1 = new TransferClient(configOne);
    const client2 = new TransferClient(configTwo);
    const client3 = new TransferClient(configThree);

    await client1.connect();
    await client2.connect();
    await client3.connect();

    const remotePath1 = configOne.remotePath || "/";
    const remotePath2 = configTwo.remotePath || "/";
    const uploadPath3 = configThree.uploadPath || "/";

    const todayFolderName = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const localPath = path.join(documentsFolder(), "DolphinEnquiries", "completed", todayFolderName);
    if (!fs.existsSync(localPath)) {
        fs.mkdirSync(localPath, { recursive: true });
    }

    const transferredFiles = new Set<string>();

    if (isTransferring) return;
    // Check for downloadable files before proceeding
    async function hasDownloadableFiles(): Promise<boolean> {
        const paths = [
            { client: client1, path: remotePath1 },
            { client: client2, path: remotePath2 }
        ];

        for (const { client, path: p } of paths) {
            try {
                const list = await client.list(p);
                if (list.some(f => isRegularFile(f))) {
                    return true;
                }
            } catch (err) {
                logToFile("file-movements", `Error checking files in ${p}: ${err instanceof Error ? err.message : String(err)}`);
            }
        }
        return false;
    }

    let failureCache = loadFailures();

    if (!(await hasDownloadableFiles()) && failureCache.length === 0) {
        await client1.end();
        await client2.end();
        await client3.end();
        return;
    }
    isTransferring = true;

    logToFile("file-movements", `=== Begin transfer session at ${new Date().toISOString()} ===`);

    let currentFile: string | null = null;

    try {
        async function ingestLocalFile(localFile: string, fileName: string): Promise<boolean> {
            try {
                const xmlContent = await fs.promises.readFile(localFile, 'utf-8');
                const saved = await saveParsedTravelFolder(xmlContent, fileName);
                if (!saved) {
                    logToFile("file-movements", `Database ingestion failed for ${fileName}`);
                }
                return saved;
            } catch (err) {
                logToFile("file-movements", `Database ingestion error for ${fileName}: ${err instanceof Error ? err.message : String(err)}`);
                return false;
            }
        }

        async function transferFilesFromClient(client: TransferClient, sourceRemotePath: string) {
            const fileList = await client.list(sourceRemotePath);
            logToFile("file-movements", `Found ${fileList.length} files/folders in ${sourceRemotePath} from ${client.toString()}`);

            for (const file of fileList) {
                const fileName = file.name;
                const isFile = isRegularFile(file);

                if (isFile && !transferredFiles.has(fileName)) {
                    currentFile = fileName;

                    ping('EFR-Electron-Mover', { state: 'run' });
                    ping('EFR-Electron-Uploading', { message: fileName });

                    const { remoteFile, localFile, destFolder, destRemoteFile } = resolveFilePaths(sourceRemotePath, uploadPath3, fileName);
                    const startTime = Date.now();

                    await client.get(remoteFile, localFile);
                    if (fs.existsSync(localFile)) {
                        logToFile("file-movements", `Downloaded ${fileName} from ${sourceRemotePath}`);

                        const savedToDatabase = await ingestLocalFile(localFile, fileName);
                        if (!savedToDatabase) {
                            ping('EFR-Electron-Mover', {
                                state: 'fail',
                                message: `Database ingestion failed for ${fileName}`,
                            });
                            continue;
                        }

                        await client.delete(remoteFile);
                        logToFile("file-movements", `Deleted source file ${fileName} from ${sourceRemotePath}`);
                    } else {
                        logToFile("file-movements", `Failed to download ${fileName} from ${sourceRemotePath}. Skipping deletion.`);
                        ping('EFR-Electron-Mover', { state: 'fail' });
                        continue;
                    }

                    const success = await tryUploadWithRetry(client3, localFile, destRemoteFile, fileName);
                    if (!success) {
                        logToFile("file-movements", `Failed to upload ${fileName} to client3 after multiple attempts.`);
                        failureCache.push({ localFile, destRemoteFile, fileName });
                        ping('EFR-Electron-Mover', { state: 'fail' });
                    } else {
                        failureCache = failureCache.filter(f => f.fileName !== fileName);
                        transferredFiles.add(fileName);

                        logToFile(
                            "file-movements",
                            `${fileName} - ${destFolder} - ${Date.now() - startTime}ms`
                        );

                        ping('EFR-Electron-Mover', { state: 'complete' });
                    }

                    currentFile = null;
                }
            }
        }

        if (failureCache.length > 0) {
            logToFile("file-movements", `Retrying ${failureCache.length} failed uploads from previous runs...`);
            for (const { localFile, destRemoteFile, fileName } of failureCache.slice()) {
                currentFile = fileName;
                ping('EFR-Electron-Mover', { state: 'run' });
                ping('EFR-Electron-Uploading', { message: `Retry ${fileName}` });

                const success = await tryUploadWithRetry(client3, localFile, destRemoteFile, fileName);
                if (success) {
                    failureCache = failureCache.filter(f => f.fileName !== fileName);
                    transferredFiles.add(fileName);

                    logToFile(
                        "file-movements",
                        `${fileName} - ${path.posix.dirname(destRemoteFile)} - RETRY SUCCESS`
                    );

                    ping('EFR-Electron-Mover', { state: 'complete' });
                } else {
                    logToFile("file-movements", `Retry upload failed for ${fileName}`);
                    ping('EFR-Electron-Mover', { state: 'fail', message: `Retry failed for ${fileName}` });
                }
                currentFile = null;
            }
        }

        saveFailures(failureCache);

        await transferFilesFromClient(client1, remotePath1);
        await transferFilesFromClient(client2, remotePath2);

    } catch (err) {
        const errorMessage = err instanceof Error ? err.message : String(err);
        logToFile("file-movements", `File transfer error: ${errorMessage}`);

        ping('EFR-Electron-Mover', {
            state: 'fail',
            message: `Transfer failed${currentFile ? ` for file \"${currentFile}\"` : ''}: ${errorMessage}`
        });
    } finally {
        await client1.end();
        await client2.end();
        await client3.end();
        isTransferring = false;

        logToFile("file-movements", `=== End transfer session ===`);
    }
}
