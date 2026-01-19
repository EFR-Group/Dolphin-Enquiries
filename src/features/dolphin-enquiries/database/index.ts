import * as fs from "fs";
import * as path from "path";
import { decode } from "he";
import { XMLParser } from "fast-xml-parser";
import {
  documentsFolder,
  getSourceTypeFromFileName,
  initDbConnection,
  query,
} from "../../../utils";

let tablesEnsured = false;

/**
 * Logs the processing status of a travel folder.
 *
 * This function logs the status of a travel folder (SUCCESS, FAILED, or SKIPPED) along with the time taken
 * for the process and any error messages (if applicable).
 *
 * @param {string} fileName - The name of the travel folder file being processed.
 * @param {"SUCCESS" | "FAILED" | "SKIPPED"} status - The status of the processing (SUCCESS, FAILED, or SKIPPED).
 * @param {number} timeTaken - The time taken to process the file in milliseconds.
 * @param {string} [errorMessage] - An optional error message if the process failed.
 */
function logTravelFolderProcessing(
  fileName: string,
  status: "SUCCESS" | "FAILED" | "SKIPPED",
  timeTaken: number,
  errorMessage?: string
): void {
  const logDir = path.join(
    documentsFolder(),
    "DolphinEnquiries",
    "logs",
    "mssql"
  );
  const logFile = path.join(
    logDir,
    `${new Date().toISOString().slice(0, 10).replace(/-/g, "")}.txt`
  );

  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }

  let logEntry = `${new Date().toLocaleTimeString()} - ${fileName} - ${status} - ${timeTaken}ms`;

  if (errorMessage && status === "FAILED") {
    const sanitizedError = errorMessage.replace(/\s+/g, " ").substring(0, 500);
    logEntry += ` - ERROR: ${sanitizedError}`;
  }

  logEntry += `\n`;

  fs.appendFile(logFile, logEntry, (err) => {
    if (err) {
      console.error(`Failed to write log: ${err}`);
    }
  });
}

// Helper: normalise boolean storage for MSSQL BIT columns
const toBool = (v: any) => (v ? 1 : 0);

/**
 * Parses the provided XML string, extracts relevant data, and saves it into a database.
 *
 * This function processes the XML string representing a travel folder, extracts the necessary details,
 * and saves the data into the database. If the data is new, it inserts it into the corresponding tables;
 * otherwise, it updates the existing records.
 *
 * @param {string} xmlString - The XML string representing the travel folder data.
 * @param {string} fileName - The name of the file being processed (used for logging and source type determination).
 * @returns {Promise<boolean>} - Returns a promise that resolves to `true` if the enquiry is new and successfully saved,
 *                                `false` if it was skipped or the process failed.
 */
export async function saveParsedTravelFolder(
  xmlString: string,
  fileName: string
): Promise<boolean> {
  const startTime = Date.now();

  try {
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "",
    });
    const json = parser.parse(xmlString);
    const travelFolder = json.DTM_TravelFolder.TravelFolder;
    const sourceType = getSourceTypeFromFileName(fileName);

    const enquiry: Enquiry = {
      source_booking_id: travelFolder.SourceBookingID ?? "",
      departure_date: travelFolder.BookingDepartureDate ?? null,
      create_date: travelFolder.SourceBookingCreateDate ?? null,
      STATUS: travelFolder.WorkflowStatus ?? null,
      is_quote_only: travelFolder.IsQuoteOnly === "true" ? 1 : 0,
      destination_name: "",
      destination_country: travelFolder.BookingDestinationCountryCode ?? "",
      airport: "",
      source_type: sourceType || "",
    };

    const comments = travelFolder.ReservationCommentItems?.ReservationCommentItem;
    let tripDetailsRawText = Array.isArray(comments)
      ? comments.map((c: any) => c.Text).join(" | ")
      : comments?.Text || "";

    const parts = tripDetailsRawText.split("|").map((p: string) => p.trim());

    const kvMap: Record<string, string> = {};
    for (const part of parts) {
      if (!part) continue;

      const colonIndex = part.indexOf(":");
      let key = "";
      let value = "";

      if (colonIndex !== -1) {
        key = part.slice(0, colonIndex).trim().toLowerCase();
        value = part.slice(colonIndex + 1).trim();
      } else {
        const spaceIndex = part.indexOf(" ");
        if (spaceIndex !== -1) {
          key = part.slice(0, spaceIndex).trim().toLowerCase();
          value = part.slice(spaceIndex + 1).trim();
        } else {
          key = part.trim().toLowerCase();
          value = "";
        }
      }

      if (key) kvMap[key] = decode(value);
    }

    const tripDetails: TripDetails = {
      hotel: kvMap["hotel"] || "",
      nights: kvMap["nights"] ? parseInt(kvMap["nights"]) || null : null,
      golfers: kvMap["golfers"] ? parseInt(kvMap["golfers"]) || null : null,
      non_golfers: kvMap["non golfers"]
        ? parseInt(kvMap["non golfers"]) || null
        : null,
      rounds: kvMap["rounds"] ? parseInt(kvMap["rounds"]) || null : null,
      adults: kvMap["adults"] ? parseInt(kvMap["adults"]) || null : null,
      children: kvMap["children"] ? parseInt(kvMap["children"]) || null : null,
      holiday_plans: kvMap["holiday plans"] || null,
      airport: kvMap["airport"] || null,
      budget_from: null,
      budget_to: null,
    };

    enquiry.destination_name = (kvMap["destination"] as any) ?? null;

    const budgetMatch = tripDetailsRawText.match(
      /Budget\s*:\s*£?([\d,]+)pp\s*-\s*£?([\d,]+)pp/i
    );
    if (budgetMatch) {
      const toFloat = (val: string) => parseFloat(val.replace(/,/g, "")) || null;
      tripDetails.budget_from = toFloat(budgetMatch[1]);
      tripDetails.budget_to = toFloat(budgetMatch[2]);
    }

    enquiry.airport = tripDetails.airport;

    const customer = travelFolder.CustomerForBooking?.DirectCustomer?.Customer;
    const customerData: CustomerData = {
      given_name: customer?.PersonName?.GivenName || null,
      surname: customer?.PersonName?.Surname || null,
      email: customer?.Email || null,
      phone_number: customer?.TelephoneInfo?.Telephone?.PhoneNumber || null,
      newsletter_opt_in: customer?.CommunicationPreferences?.Newsletter ? 1 : 0,
    };

    const rawPassengers = travelFolder.PassengerListItems?.PassengerListItem;
    const passengers: Passenger[] = (
      Array.isArray(rawPassengers)
        ? rawPassengers
        : rawPassengers
          ? [rawPassengers]
          : []
    ).map((p: any) => ({
      given_name: p.PersonName?.GivenName || null,
      surname: p.PersonName?.Surname || null,
    }));

    const marketing: Marketing = {
      campaign_code: travelFolder.MarketingCampaignCode ?? null,
      source: travelFolder.EnhancedData01 ?? null,
      medium: travelFolder.EnhancedData02 ?? null,
      ad_id: travelFolder.EnhancedData00 ?? null,
    };

    const conn = await initDbConnection();
    await ensureTablesExistOnce(conn);

    let enquiryId: number;
    let isNewEnquiry = false;

    const existing = await query(
      conn,
      `SELECT TOP 1 ID FROM ENQUIRIES WHERE SOURCE_BOOKING_ID = ?`,
      [enquiry.source_booking_id]
    );

    if (existing && existing.length > 0) {
      enquiryId = existing[0].ID;
      console.debug(
        `Enquiry with SOURCE_BOOKING_ID ${enquiry.source_booking_id} already exists, continuing to ensure all child data is inserted.`
      );
    } else {
      console.debug(
        `Inserting new enquiry with SOURCE_BOOKING_ID ${enquiry.source_booking_id}`
      );

      const inserted = await query(
        conn,
        `
  INSERT INTO ENQUIRIES
    (SOURCE_BOOKING_ID, DEPARTURE_DATE, CREATE_DATE, [STATUS], IS_QUOTE_ONLY, DESTINATION_NAME, DESTINATION_COUNTRY, AIRPORT, SOURCE_TYPE)
  OUTPUT INSERTED.ID AS ID
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `,
        [
          enquiry.source_booking_id,
          enquiry.departure_date,
          enquiry.create_date,
          enquiry.STATUS,
          toBool(enquiry.is_quote_only),
          enquiry.destination_name,
          enquiry.destination_country,
          enquiry.airport,
          enquiry.source_type,
        ]
      );

      if (!inserted?.length || inserted[0]?.ID == null) {
        throw new Error("Insert into ENQUIRIES did not return an inserted ID");
      }

      enquiryId = inserted[0].ID;
      isNewEnquiry = true;
    }

    // -------------------------
    // INSERT / UPDATE CHILD ROWS
    // -------------------------

    // TRIP_DETAILS: one-per-enquiry (upsert behaviour)
    const existingTrip = await query(
      conn,
      `SELECT TOP 1 ID FROM TRIP_DETAILS WHERE ENQUIRY_ID = ?`,
      [enquiryId]
    );

    if (existingTrip && existingTrip.length > 0) {
      await query(
        conn,
        `
        UPDATE TRIP_DETAILS
        SET HOTEL = ?, NIGHTS = ?, GOLFERS = ?, NON_GOLFERS = ?, ROUNDS = ?, ADULTS = ?, CHILDREN = ?, HOLIDAY_PLANS = ?, BUDGET_FROM = ?, BUDGET_TO = ?
        WHERE ENQUIRY_ID = ?
        `,
        [
          tripDetails.hotel,
          tripDetails.nights,
          tripDetails.golfers,
          tripDetails.non_golfers,
          tripDetails.rounds,
          tripDetails.adults,
          tripDetails.children,
          tripDetails.holiday_plans,
          tripDetails.budget_from,
          tripDetails.budget_to,
          enquiryId,
        ]
      );
    } else {
      await query(
        conn,
        `
        INSERT INTO TRIP_DETAILS
          (ENQUIRY_ID, HOTEL, NIGHTS, GOLFERS, NON_GOLFERS, ROUNDS, ADULTS, CHILDREN, HOLIDAY_PLANS, BUDGET_FROM, BUDGET_TO)
        VALUES
          (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          enquiryId,
          tripDetails.hotel,
          tripDetails.nights,
          tripDetails.golfers,
          tripDetails.non_golfers,
          tripDetails.rounds,
          tripDetails.adults,
          tripDetails.children,
          tripDetails.holiday_plans,
          tripDetails.budget_from,
          tripDetails.budget_to,
        ]
      );
    }

    // CUSTOMERS: one-per-enquiry (upsert behaviour)
    const existingCustomer = await query(
      conn,
      `SELECT TOP 1 ID FROM CUSTOMERS WHERE ENQUIRY_ID = ?`,
      [enquiryId]
    );

    if (existingCustomer && existingCustomer.length > 0) {
      await query(
        conn,
        `
        UPDATE CUSTOMERS
        SET GIVEN_NAME = ?, SURNAME = ?, EMAIL = ?, PHONE_NUMBER = ?, NEWSLETTER_OPT_IN = ?
        WHERE ENQUIRY_ID = ?
        `,
        [
          customerData.given_name,
          customerData.surname,
          customerData.email,
          customerData.phone_number,
          toBool(customerData.newsletter_opt_in),
          enquiryId,
        ]
      );
    } else {
      await query(
        conn,
        `
        INSERT INTO CUSTOMERS
          (ENQUIRY_ID, GIVEN_NAME, SURNAME, EMAIL, PHONE_NUMBER, NEWSLETTER_OPT_IN)
        VALUES
          (?, ?, ?, ?, ?, ?)
        `,
        [
          enquiryId,
          customerData.given_name,
          customerData.surname,
          customerData.email,
          customerData.phone_number,
          toBool(customerData.newsletter_opt_in),
        ]
      );
    }

    // MARKETING: one-per-enquiry (upsert behaviour)
    const existingMarketing = await query(
      conn,
      `SELECT TOP 1 ID FROM MARKETING WHERE ENQUIRY_ID = ?`,
      [enquiryId]
    );

    if (existingMarketing && existingMarketing.length > 0) {
      await query(
        conn,
        `
        UPDATE MARKETING
        SET CAMPAIGN_CODE = ?, SOURCE = ?, MEDIUM = ?, AD_ID = ?
        WHERE ENQUIRY_ID = ?
        `,
        [
          marketing.campaign_code,
          marketing.source,
          marketing.medium,
          marketing.ad_id,
          enquiryId,
        ]
      );
    } else {
      await query(
        conn,
        `
        INSERT INTO MARKETING
          (ENQUIRY_ID, CAMPAIGN_CODE, SOURCE, MEDIUM, AD_ID)
        VALUES
          (?, ?, ?, ?, ?)
        `,
        [
          enquiryId,
          marketing.campaign_code,
          marketing.source,
          marketing.medium,
          marketing.ad_id,
        ]
      );
    }

    // PASSENGERS: many-per-enquiry
    // Strategy: insert only missing passengers (avoid duplicates). We match on given+surname (case-insensitive).
    // If you prefer "replace all", see alternative block at the bottom.
    const existingPassengers = await query(
      conn,
      `SELECT GIVEN_NAME, SURNAME FROM PASSENGERS WHERE ENQUIRY_ID = ?`,
      [enquiryId]
    );

    const existingKey = new Set(
      (existingPassengers || []).map(
        (p: any) =>
          `${(p.GIVEN_NAME || "").toLowerCase()}|${(p.SURNAME || "").toLowerCase()}`
      )
    );

    for (const p of passengers) {
      const key = `${(p.given_name || "").toLowerCase()}|${(
        p.surname || ""
      ).toLowerCase()}`;
      if (existingKey.has(key)) continue;

      await query(
        conn,
        `
        INSERT INTO PASSENGERS (ENQUIRY_ID, GIVEN_NAME, SURNAME)
        VALUES (?, ?, ?)
        `,
        [enquiryId, p.given_name, p.surname]
      );
    }

    const timeTaken = Date.now() - startTime;
    logTravelFolderProcessing(fileName, isNewEnquiry ? "SUCCESS" : "SKIPPED", timeTaken);
    return isNewEnquiry ? true : false;
  } catch (e) {
    const timeTaken = Date.now() - startTime;
    const errorMessage = e instanceof Error ? e.message : String(e);
    logTravelFolderProcessing(fileName, "FAILED", timeTaken, errorMessage);
    console.error("Failed to save parsed travel folder:", e);
    return false;
  }
}

export async function ensureTablesExistOnce(conn: any) {
  if (tablesEnsured) return;
  await ensureTablesExist(conn);
  tablesEnsured = true;
}

export async function ensureTablesExist(conn: any) {
  await query(conn, `
    IF OBJECT_ID('ENQUIRIES', 'U') IS NULL
    BEGIN
      CREATE TABLE ENQUIRIES (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        SOURCE_BOOKING_ID NVARCHAR(100) NOT NULL,
        DEPARTURE_DATE DATETIME2 NULL,
        CREATE_DATE DATETIME2 NULL,
        [STATUS] NVARCHAR(50) NULL,
        IS_QUOTE_ONLY BIT NOT NULL DEFAULT 0,
        DESTINATION_NAME NVARCHAR(200) NULL,
        DESTINATION_COUNTRY NVARCHAR(10) NULL,
        AIRPORT NVARCHAR(100) NULL,
        SOURCE_TYPE NVARCHAR(50) NULL
      );

      CREATE UNIQUE INDEX UX_ENQUIRIES_SOURCE_BOOKING_ID
      ON ENQUIRIES (SOURCE_BOOKING_ID);
    END
  `);

  await query(conn, `
    IF OBJECT_ID('TRIP_DETAILS', 'U') IS NULL
    BEGIN
      CREATE TABLE TRIP_DETAILS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        ENQUIRY_ID INT NOT NULL,
        HOTEL NVARCHAR(200) NULL,
        NIGHTS INT NULL,
        GOLFERS INT NULL,
        NON_GOLFERS INT NULL,
        ROUNDS INT NULL,
        ADULTS INT NULL,
        CHILDREN INT NULL,
        HOLIDAY_PLANS NVARCHAR(MAX) NULL,
        BUDGET_FROM FLOAT NULL,
        BUDGET_TO FLOAT NULL,
        CONSTRAINT FK_TRIP_DETAILS_ENQUIRY
          FOREIGN KEY (ENQUIRY_ID) REFERENCES ENQUIRIES(ID)
          ON DELETE CASCADE
      );
    END
  `);

  await query(conn, `
    IF OBJECT_ID('CUSTOMERS', 'U') IS NULL
    BEGIN
      CREATE TABLE CUSTOMERS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        ENQUIRY_ID INT NOT NULL,
        GIVEN_NAME NVARCHAR(100) NULL,
        SURNAME NVARCHAR(100) NULL,
        EMAIL NVARCHAR(254) NULL,
        PHONE_NUMBER NVARCHAR(50) NULL,
        NEWSLETTER_OPT_IN BIT NOT NULL DEFAULT 0,
        CONSTRAINT FK_CUSTOMERS_ENQUIRY
          FOREIGN KEY (ENQUIRY_ID) REFERENCES ENQUIRIES(ID)
          ON DELETE CASCADE
      );
    END
  `);

  await query(conn, `
    IF OBJECT_ID('MARKETING', 'U') IS NULL
    BEGIN
      CREATE TABLE MARKETING (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        ENQUIRY_ID INT NOT NULL,
        CAMPAIGN_CODE NVARCHAR(100) NULL,
        SOURCE NVARCHAR(200) NULL,
        MEDIUM NVARCHAR(200) NULL,
        AD_ID NVARCHAR(200) NULL,
        CONSTRAINT FK_MARKETING_ENQUIRY
          FOREIGN KEY (ENQUIRY_ID) REFERENCES ENQUIRIES(ID)
          ON DELETE CASCADE
      );
    END
  `);

  await query(conn, `
    IF OBJECT_ID('PASSENGERS', 'U') IS NULL
    BEGIN
      CREATE TABLE PASSENGERS (
        ID INT IDENTITY(1,1) PRIMARY KEY,
        ENQUIRY_ID INT NOT NULL,
        GIVEN_NAME NVARCHAR(100) NULL,
        SURNAME NVARCHAR(100) NULL,
        CONSTRAINT FK_PASSENGERS_ENQUIRY
          FOREIGN KEY (ENQUIRY_ID) REFERENCES ENQUIRIES(ID)
          ON DELETE CASCADE
      );

      CREATE INDEX IX_PASSENGERS_ENQUIRY_ID
      ON PASSENGERS (ENQUIRY_ID);
    END
  `);
}

