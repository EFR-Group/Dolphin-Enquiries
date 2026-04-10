import Store from 'electron-store';
import keytar from 'keytar';
import crypto from 'crypto';

const SERVICE_NAME = 'dolphin-enquiries-tray';
const ACCOUNT_NAME = 'encryption-key';

class Settings {
  private store?: Store<StoreSchema>;

  private async initStore(): Promise<void> {
    if (!this.store) {
      let key = await keytar.getPassword(SERVICE_NAME, ACCOUNT_NAME);
      if (!key) {
        key = crypto.randomBytes(32).toString('hex');
        await keytar.setPassword(SERVICE_NAME, ACCOUNT_NAME, key);
      }

      this.store = new Store<StoreSchema>({
        name: 'smtp-config',
        encryptionKey: key,
        defaults: {
          smtp: {
            host: '',
            port: 587,
            secure: true,
            user: '',
            pass: '',
            to: ''
          },
          sftpOne: {
            host: '',
            port: 22,
            useSFTP: true,
            username: '',
            password: '',
            remotePath: '',
          },
          sftpTwo: {
            host: '',
            port: 22,
            useSFTP: true,
            username: '',
            password: '',
            uploadPath: '',
          },
          sftpThree: {
            host: '',
            port: 22,
            useSFTP: true,
            username: '',
            password: '',
            uploadPath: '',
          },
          snowflake: {
            account: '',
            username: '',
            password: '',
            warehouse: '',
            database: '',
            schema: '',
            role: '',
          },
          cronitor: {
            apiKey: ''
          },
          mssql: {
            server: '',
            database: '',
            user: '',
            password: '',
            options: {
              trustServerCertificate: true,
              encrypt: false,
            },
          }
        }
      });
    }
  }

  async getSMTPConfig(): Promise<SMTPConfig | null> {
    await this.initStore();
    const config = this.store!.get('smtp');
    return config && config.host ? config : null;
  }

  async getSFTPConfigOne(): Promise<FTPConfig | null> {
    await this.initStore();
    const config = this.store!.get('sftpOne');
    return config && config.host ? config : null;
  }

  async getSFTPConfigTwo(): Promise<FTPConfig | null> {
    await this.initStore();
    const config = this.store!.get('sftpTwo');
    return config && config.host ? config : null;
  }

  async getSFTPConfigThree(): Promise<FTPConfig | null> {
    await this.initStore();
    const config = this.store!.get('sftpThree');
    return config && config.host ? config : null;
  }

  async getSnowflakeConfig(): Promise<SnowflakeConfig | null> {
    await this.initStore();
    const config = this.store!.get('snowflake');
    return config && config.account ? config : null;
  }

  async getCronitorConfig(): Promise<CronitorConfig | null> {
    await this.initStore();
    const config = this.store!.get('cronitor');
    return config && config.apiKey ? config : null;
  }

  async getMsSQLConfig(): Promise<MsSQLConfig | null> {
    await this.initStore();
    const config = this.store!.get('mssql');
    return config && config.server ? config : null;
  }

  async setSMTPConfig(config: SMTPConfig): Promise<void> {
    await this.initStore();
    this.store!.set('smtp', config);
  }

  async setSFTPConfigOne(config: FTPConfig): Promise<void> {
    await this.initStore();
    this.store!.set('sftpOne', config);
  }

  async setSFTPConfigTwo(config: FTPConfig): Promise<void> {
    await this.initStore();
    this.store!.set('sftpTwo', config);
  }

  async setSFTPConfigThree(config: FTPConfig): Promise<void> {
    await this.initStore();
    this.store!.set('sftpThree', config);
  }

  async setSnowflakeConfig(config: SnowflakeConfig): Promise<void> {
    await this.initStore();
    this.store!.set('snowflake', config);
  }

  async setCronitorConfig(config: CronitorConfig): Promise<void> {
    await this.initStore();
    this.store!.set('cronitor', config);
  }

  async setMsSQLConfig(config: MsSQLConfig): Promise<void> {
    await this.initStore();
    this.store!.set('mssql', config);
  }

  async hasSMTPConfig(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('smtp');
    return Boolean(config?.host);
  }

  async hasSFTPConfigOne(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('sftpOne');
    return Boolean(config?.host);
  }

  async hasSFTPConfigTwo(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('sftpTwo');
    return Boolean(config?.host);
  }

  async hasSFTPConfigThree(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('sftpThree');
    return Boolean(config?.host);
  }

  async hasSnowflakeConfig(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('snowflake');
    return Boolean(config?.account);
  }

  async hasCronitorConfig(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('cronitor');
    return Boolean(config?.apiKey);
  }

  async hasMsSQLConfig(): Promise<boolean> {
    await this.initStore();
    const config = this.store!.get('mssql');
    return Boolean(config?.server);
  }
}

export const settings = new Settings();
