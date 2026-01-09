/**
 * SMTP configuration used for sending emails.
 * 
 * @type {Object}
 */
type SMTPConfig = {
    /** The SMTP server host */
    host: string;
    /** The SMTP server port */
    port: number;
    /** Whether to use a secure connection (TLS/SSL) */
    secure: boolean;
    /** The username for authenticating with the SMTP server */
    user: string;
    /** The password for authenticating with the SMTP server */
    pass: string;
    /** The email address to send the email to */
    to: string;
}

/**
 * FTP configuration for connecting to an FTP server.
 * 
 * @type {Object}
 */
type FTPConfig = {
    /** The FTP server host */
    host: string;
    /** The FTP server port */
    port: number;
    /** The username for authenticating with the FTP server */
    username: string;
    /** The password for authenticating with the FTP server */
    password: string;
    /** The remote path to retrieve files from (optional, required for SFTP One) */
    remotePath?: string;
    /** The path to upload files to (optional, required for SFTP Two) */
    uploadPath?: string;
}

/**
 * Snowflake configuration for connecting to a Snowflake account.
 * 
 * @type {Object}
 */
type SnowflakeConfig = {
    /** The Snowflake account identifier */
    account: string;
    /** The Snowflake username for authentication */
    username: string;
    /** The Snowflake password for authentication */
    password: string;
    /** The Snowflake warehouse to use for queries */
    warehouse: string;
    /** The Snowflake database to use */
    database: string;
    /** The Snowflake schema to use */
    schema: string;
    /** The Snowflake role to use */
    role: string;
}

/**
 * Cronitor configuration for monitoring tasks.
 * 
 * @type {Object}
 */
type CronitorConfig = {
    /** The API key for authenticating with Cronitor */
    apiKey: string;
}

/**
 * MsSQL configuration for connecting to an MS SQL Server.
 * 
 * @type {Object}
 */
type MsSQLConfig = {
    /** The MS SQL server address */
    server: string;
    /** The MS SQL database name */
    database: string;
    /** The MS SQL username for authentication */
    user: string;
    /** The MS SQL password for authentication */
    password: string;
    /** Options for MS SQL connection */
    options: {
        /** Whether to trust the server certificate */
        trustServerCertificate: boolean;
        /** Whether to encrypt the connection */
        encrypt: boolean;
    };
}

/**
 * Store schema for storing configurations.
 * 
 * @type {Object}
 */
type StoreSchema = {
    /** The SMTP configuration */
    smtp: SMTPConfig;
    /** The SFTP configuration for the first server */
    sftpOne: FTPConfig;
    /** The SFTP configuration for the second server */
    sftpTwo: FTPConfig;
    /** The SFTP configuration for the third server */
    sftpThree: FTPConfig;
    /** The Snowflake configuration */
    snowflake: SnowflakeConfig;
    /** The Cronitor configuration */
    cronitor: CronitorConfig;
    /** The MS SQL configuration */
    mssql: MsSQLConfig;
}

/**
 * Represents a scheduled task with a cron schedule.
 * 
 * @type {Object}
 */
type ScheduledTask = {
    /** The task to execute */
    task: () => void;
    /** The cron schedule for the task (optional, default is 1:00 AM) */
    schedule?: string;
};

/**
 * Represents an enquiry with customer and trip details.
 * 
 * @type {Object}
 */
type Enquiry = {
    /** The source booking ID for the enquiry */
    source_booking_id: string | null;
    /** The departure date for the trip */
    departure_date: string | null;
    /** The date the booking was created */
    create_date: string | null;
    /** The current status of the enquiry */
    STATUS: string | null;
    /** Whether the enquiry is a quote-only enquiry */
    is_quote_only: number;
    /** The destination name for the trip */
    destination_name: string | null;
    /** The country code for the destination */
    destination_country: string | null;
    /** The airport for the trip */
    airport: string | null;
    /** The source type of the enquiry */
    source_type: string | null;
};

/**
 * Represents the details of the trip associated with an enquiry.
 * 
 * @type {Object}
 */
type TripDetails = {
    /** The hotel for the trip */
    hotel: string | null;
    /** The number of nights for the trip */
    nights: number | null;
    /** The number of golfers for the trip */
    golfers: number | null;
    /** The number of non-golfers for the trip */
    non_golfers: number | null;
    /** The number of rounds of golf for the trip */
    rounds: number | null;
    /** The number of adults for the trip */
    adults: number | null;
    /** The number of children for the trip */
    children: number | null;
    /** The holiday plans for the trip */
    holiday_plans: string | null;
    /** The budget range for the trip (from) */
    budget_from: number | null;
    /** The budget range for the trip (to) */
    budget_to: number | null;
    /** The airport for the trip */
    airport: string | null;
};

/**
 * Represents customer data associated with an enquiry.
 * 
 * @type {Object}
 */
type CustomerData = {
    /** The given name of the customer */
    given_name: string | null;
    /** The surname of the customer */
    surname: string | null;
    /** The email of the customer */
    email: string | null;
    /** The phone number of the customer */
    phone_number: string | null;
    /** Whether the customer opted in to newsletters */
    newsletter_opt_in: number | null;
}

/**
 * Represents a passenger associated with an enquiry.
 * 
 * @type {Object}
 */
type Passenger = {
    /** The given name of the passenger */
    given_name: string | null;
    /** The surname of the passenger */
    surname: string | null;
}

/**
 * Represents marketing information associated with an enquiry.
 * 
 * @type {Object}
 */
type Marketing = {
    /** The campaign code associated with the marketing */
    campaign_code: string | null;
    /** The source of the marketing */
    source: string | null;
    /** The medium of the marketing */
    medium: string | null;
    /** The ad ID of the marketing */
    ad_id: string | null;
}

/**
 * Represents a file that could be from either an SFTP or FTP source.
 * 
 * @type {Object}
 */
type UnifiedFileInfo = SftpFileInfo | FtpFileInfo;

/**
 * The input data for pinging Cronitor to report the task's status.
 * 
 * @type {Object}
 */
type CronitorPingInput = {
    /** The state of the task (e.g., 'run', 'start', 'complete', 'fail', 'warn') */
    state?: 'run' | 'start' | 'complete' | 'fail' | 'warn';
    /** An optional message associated with the ping */
    message?: string;
};

/**
 * A monitor for Cronitor to track the status of a task.
 * 
 * @type {Object}
 */
type CronitorMonitor = {
    /** Pings the Cronitor monitor with the task's status */
    ping: (input: CronitorPingInput) => void;
};

/**
 * The Cronitor module responsible for managing task monitors.
 * 
 * @type {Object}
 */
type CronitorModule = {
    /** Creates a new Cronitor monitor instance */
    Monitor: new (name: string) => CronitorMonitor;
};

type RunPowershellOptions = {
  cwd?: string;
  timeoutMs?: number;
};