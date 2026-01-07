import SFTPClient from 'ssh2-sftp-client';
import { Client as FTPClient } from 'basic-ftp';
import { delay, logToFile } from '.';

/**
 * Unified file information structure for both FTP and SFTP.
 * @typedef {Object} UnifiedFileInfo
 * @property {string} name - The name of the file or directory.
 * @property {'file' | 'directory' | 'symbolicLink'} type - The type of the entry (file, directory, symbolic link).
 * @property {number} size - The size of the file or directory.
 * @property {string} [rawModifiedAt] - For FTP, the raw modified timestamp.
 * @property {boolean} [isDirectory] - For FTP, indicates if the entry is a directory.
 * @property {boolean} [isSymbolicLink] - For FTP, indicates if the entry is a symbolic link.
 * @property {boolean} [isFile] - For FTP, indicates if the entry is a file.
 * @property {number} [accessTime] - Access time of the file.
 * @property {number} [modifyTime] - Modify time of the file.
 */
type UnifiedFileInfo = {
    name: string;
    type: 'file' | 'directory' | 'symbolicLink';
    size: number;
    rawModifiedAt?: string; // For FTP
    isDirectory?: boolean;  // For FTP
    isSymbolicLink?: boolean;  // For FTP
    isFile?: boolean;        // For FTP
    accessTime?: number;
    modifyTime?: number;
};

/**
 * Retry options for file operations.
 * @typedef {Object} RetryOptions
 * @property {string} label - The label for the operation being retried.
 * @property {number} [maxRetries=3] - The maximum number of retries.
 * @property {number} [retryDelayMs=2000] - The delay in milliseconds between retries.
 */
interface RetryOptions {
    label: string;
    maxRetries?: number;
    retryDelayMs?: number;
}

export class TransferClient {
    private sftpClient: SFTPClient | null = null;
    private ftpClient: FTPClient | null = null;
    private readonly isSFTP: boolean;

    /**
     * Creates an instance of TransferClient.
     * 
     * @param {FTPConfig} config - The configuration object for the FTP/SFTP server.
     */
    constructor(private config: FTPConfig) {
        this.isSFTP = [22, 54872, 42870].includes(config.port);
    }

    /**
     * Checks if the provided client is an instance of SFTPClient.
     * 
     * @param {SFTPClient | FTPClient} client - The client to check.
     * @returns {boolean} - True if the client is an instance of SFTPClient.
     */
    private isSFTPClient(client: SFTPClient | FTPClient): boolean {
        return (client as SFTPClient).list !== undefined;
    }

    /**
     * Checks if the provided client is an instance of FTPClient.
     * 
     * @param {SFTPClient | FTPClient} client - The client to check.
     * @returns {boolean} - True if the client is an instance of FTPClient.
     */
    private isFTPClient(client: SFTPClient | FTPClient): boolean {
        return (client as FTPClient).access !== undefined;
    }

    /**
     * Gets the active FTP/SFTP client.
     * 
     * @returns {SFTPClient | FTPClient} - The active client.
     * @throws {Error} - If neither SFTP nor FTP client is connected.
     */
    private getClient(): SFTPClient | FTPClient {
        if (this.isSFTP && this.sftpClient) {
            return this.sftpClient;
        }
        if (!this.isSFTP && this.ftpClient) {
            return this.ftpClient;
        }
        throw new Error(`${this.isSFTP ? "SFTP" : "FTP"} client not connected`);
    }

    /**
     * Checks if the client is connected.
     * 
     * @returns {boolean} - True if the client is connected, false otherwise.
     */
    isConnected(): boolean {
        return this.isSFTP ? this.sftpClient !== null : this.ftpClient !== null;
    }

    /**
     * Logs a message to the log file.
     * 
     * @param {string} message - The message to log.
     */
    private log(message: string): void {
        const kind = this.isSFTP ? 'SFTP' : 'FTP';
        logToFile('transfer-files', `[${kind}] ${message}`);
    }

    /**
     * Executes a function with retry logic.
     * 
     * @template T
     * @param {Function} fn - The function to execute.
     * @returns {Promise.<T>} - The promise resolving to the result of executing the function.
     * @param {RetryOptions} options - The options for retries.
     * @returns {Promise<T>} - The result of the function execution.
     * @throws {Error} - If the function fails after maxRetries attempts.
     */
    private async withRetries<T>(fn: () => Promise<T>, options: RetryOptions): Promise<T> {
        const { label, maxRetries = 3, retryDelayMs = 2000 } = options;
        const target = this.toString();

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return await fn();
            } catch (err) {
                const msg = err instanceof Error ? err.message : String(err);
                this.log(`${label} attempt ${attempt} failed (${target}): ${msg}`);
                if (attempt === maxRetries) {
                    this.log(`${label} failed after ${attempt} attempts (${target})`);
                    throw err;
                }
                await delay(retryDelayMs);
                this.log(`Retrying operation for ${label}, attempt ${attempt} of ${maxRetries} after ${retryDelayMs}ms delay`);
            }
        }
        throw new Error(`Unexpected failure in ${label}`);
    }

    /**
     * Logs the execution time of a function.
     * 
     * @template T
     * @param {string} label - The label for the operation being timed.
     * @param {Function} fn - The function to execute.
     * @returns {Promise<T>} - The result of the function execution.
     */
    private async timeLog<T>(label: string, fn: () => Promise<T>): Promise<T> {
        const start = Date.now();
        this.log(`Starting operation: ${label}`);
        const result = await fn();
        const duration = Date.now() - start;
        this.log(`${label} completed in ${duration}ms`);
        return result;
    }

    /**
     * Creates and connects an SFTP client.
     * 
     * @returns {Promise<SFTPClient>} - The connected SFTPClient instance.
     */
    private async createSFTPClient(): Promise<SFTPClient> {
        const client = new SFTPClient();
        await client.connect({
            host: this.config.host,
            port: this.config.port,
            username: this.config.username,
            password: this.config.password,
            keepaliveInterval: 10000,
            readyTimeout: 60000,
        });
        this.log(`Successfully connected to SFTP: ${this.toString()}`);
        return client;
    }

    /**
     * Creates and connects an FTP client.
     * 
     * @returns {Promise<FTPClient>} - The connected FTPClient instance.
     */
    private async createFTPClient(): Promise<FTPClient> {
        const client = new FTPClient();
        await client.access({
            host: this.config.host,
            port: this.config.port,
            user: this.config.username,
            password: this.config.password,
        });
        this.log(`Successfully connected to FTP: ${this.toString()}`);
        return client;
    }

    /**
     * Connects to the FTP or SFTP server.
     * 
     * @param {number} [maxRetries=3] - The maximum number of retries for connection attempts.
     * @param {number} [retryDelayMs=2000] - The delay in milliseconds between retry attempts.
     * @returns {Promise<void>} - Resolves when the connection is successful.
     */
    async connect(maxRetries: number = 3, retryDelayMs: number = 2000): Promise<void> {
        const client = this.isSFTP
            ? await this.withRetries(() => this.createSFTPClient(), { label: 'SFTP connect', maxRetries, retryDelayMs })
            : await this.withRetries(() => this.createFTPClient(), { label: 'FTP connect', maxRetries, retryDelayMs });

        if (this.isSFTP) {
            this.sftpClient = client as SFTPClient;
        } else {
            this.ftpClient = client as FTPClient;
        }

        this.log(`Connected to ${this.toString()}`);
    }

    /**
     * Lists files in a remote directory.
     * 
     * @param {string} remotePath - The remote directory path to list files from.
     * @returns {Promise<UnifiedFileInfo[]>} - A promise that resolves to a list of UnifiedFileInfo objects.
     */
    async list(remotePath: string): Promise<UnifiedFileInfo[]> {
        this.log(`Listing files in ${remotePath}`);

        const client = this.getClient();

        if (this.isSFTPClient(client)) {
            return this.withRetries(async () => {
                const sftpList = await client.list(remotePath);
                return sftpList.map(item => ({
                    name: item.name,
                    type: item.type === 'd' ? 'directory' : item.type === 'l' ? 'symbolicLink' : 'file',
                    size: item.size,
                }));
            }, { label: `SFTP list(${remotePath})` });
        } else if (this.isFTPClient(client)) {
            await this.ftpClient?.cd(remotePath);
            return this.withRetries(async () => {
                const ftpList = await this.ftpClient?.list() || [];
                return ftpList.map(item => ({
                    name: item.name,
                    type: item.isDirectory ? 'directory' : 'file',
                    size: item.size,
                }));
            }, { label: `FTP list(${remotePath})` });
        }

        return [];
    }

    async get(remoteFile: string, localFile: string): Promise<void> {
        this.log(`Downloading file from ${remoteFile} to ${localFile}`);

        const client = this.getClient();

        if (this.isSFTPClient(client)) {
            const sftp = client as SFTPClient;

            const concurrency = 16;
            const chunkSize = 1024 * 1024;

            try {
                await sftp.fastGet(remoteFile, localFile, {
                    concurrency,
                    chunkSize,
                });

                this.log(`fastGet completed (${concurrency}x${chunkSize}) for ${remoteFile}`);
                return;
            } catch (err) {
                const msg = err instanceof Error ? err.message : String(err);
                this.log(`fastGet failed, falling back to get(): ${msg}`);

                await sftp.get(remoteFile, localFile);
                return;
            }
        }

        if (this.isFTPClient(client)) {
            await this.ftpClient?.downloadTo(localFile, remoteFile);
        }
    }

    async put(localFile: string, remoteFile: string): Promise<void> {
        this.log(`Uploading file from ${localFile} to ${remoteFile}`);
        const client = this.getClient();
        if (this.isSFTPClient(client)) {
            await (client as SFTPClient).put(localFile, remoteFile);
        } else if (this.isFTPClient(client)) {
            await this.ftpClient?.uploadFrom(localFile, remoteFile);
        }
    }

    async delete(remoteFile: string): Promise<void> {
        this.log(`Deleting file: ${remoteFile}`);
        const client = this.getClient();
        if (this.isSFTPClient(client)) {
            await (client as SFTPClient).delete(remoteFile);
        } else if (this.isFTPClient(client)) {
            await this.ftpClient?.remove(remoteFile);
        }
    }

    /**
     * Ends the connection to the server.
     * 
     * @returns {Promise<void>} - Resolves when the connection is closed.
     */
    async end(): Promise<void> {
        try {
            const client = this.getClient();
            if (this.isSFTPClient(client)) {
                await this.sftpClient?.end();
                this.log(`Closed connection to ${this.toString()}`);
                this.sftpClient = null;
            } else if (this.isFTPClient(client)) {
                await this.ftpClient?.close();
                this.log(`Closed connection to ${this.toString()}`);
                this.ftpClient = null;
            }
        } catch (err) {
            const errorMsg = err instanceof Error ? err.message : String(err);
            this.log(`Error while closing ${this.toString()}: ${errorMsg}`);
        }
    }

    /**
     * Returns a string representation of the current connection.
     * 
     * @returns {string} - The string representation of the connection.
     */
    toString(): string {
        return `${this.isSFTP ? 'SFTP' : 'FTP'}://${this.config.username}@${this.config.host}:${this.config.port}`;
    }
}
