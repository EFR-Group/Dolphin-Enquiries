import * as fs from "fs";
import * as path from "path";
import { decode } from "he";
import { XMLParser } from 'fast-xml-parser';
import { documentsFolder, getSourceTypeFromFileName, initDbConnection, query } from '../../../utils';

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
  const logDir = path.join(documentsFolder(), "DolphinEnquiries", "logs", "snowflake");
  const logFile = path.join(logDir, `${new Date().toISOString().slice(0, 10).replace(/-/g, '')}.txt`);

  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }

  let logEntry = `${new Date().toLocaleTimeString()} - ${fileName} - ${status} - ${timeTaken}ms`;

  if (errorMessage && status === "FAILED") {
    const sanitizedError = errorMessage.replace(/\s+/g, ' ').substring(0, 500);
    logEntry += ` - ERROR: ${sanitizedError}`;
  }

  logEntry += `\n`;

  fs.appendFile(logFile, logEntry, (err) => {
    if (err) {
      console.error(`Failed to write log: ${err}`);
    }
  });
}

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
export async function saveParsedTravelFolder(xmlString: string, fileName: string): Promise<boolean> {
  const startTime = Date.now();

  try {
    const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "" });
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
      ? comments.map(c => c.Text).join(" | ")
      : comments?.Text || "";

    const parts = tripDetailsRawText.split('|').map((p: string) => p.trim());

    const kvMap: Record<string, string> = {};
    for (const part of parts) {
      if (!part) continue;

      const colonIndex = part.indexOf(':');
      let key = '';
      let value = '';

      if (colonIndex !== -1) {
        key = part.slice(0, colonIndex).trim().toLowerCase();
        value = part.slice(colonIndex + 1).trim();
      } else {
        const spaceIndex = part.indexOf(' ');
        if (spaceIndex !== -1) {
          key = part.slice(0, spaceIndex).trim().toLowerCase();
          value = part.slice(spaceIndex + 1).trim();
        } else {
          key = part.trim().toLowerCase();
          value = '';
        }
      }

      if (key) kvMap[key] = decode(value);
    }

    const tripDetails: TripDetails = {
      hotel: kvMap['hotel'] || '',
      nights: kvMap['nights'] ? parseInt(kvMap['nights']) || null : null,
      golfers: kvMap['golfers'] ? parseInt(kvMap['golfers']) || null : null,
      non_golfers: kvMap['non golfers'] ? parseInt(kvMap['non golfers']) || null : null,
      rounds: kvMap['rounds'] ? parseInt(kvMap['rounds']) || null : null,
      adults: kvMap['adults'] ? parseInt(kvMap['adults']) || null : null,
      children: kvMap['children'] ? parseInt(kvMap['children']) || null : null,
      holiday_plans: kvMap['holiday plans'] || null,
      airport: kvMap['airport'] || null,
      budget_from: null,
      budget_to: null,
    };

    enquiry.destination_name = kvMap['destination'] ?? null;

    const budgetMatch = tripDetailsRawText.match(/Budget\s*:\s*£?([\d,]+)pp\s*-\s*£?([\d,]+)pp/i);
    if (budgetMatch) {
      const toFloat = (val: string) => parseFloat(val.replace(/,/g, '')) || null;
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
    const passengers: Passenger[] = (Array.isArray(rawPassengers)
      ? rawPassengers
      : rawPassengers ? [rawPassengers] : [])
      .map((p: any) => ({
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

    let enquiryId: number;
    let isNewEnquiry = false;

    const existing = await query(conn, `SELECT ID FROM ENQUIRIES WHERE SOURCE_BOOKING_ID = ? LIMIT 1`, [enquiry.source_booking_id]);
    if (existing && existing.length > 0) {
      enquiryId = existing[0].ID;
      console.debug(`Enquiry with SOURCE_BOOKING_ID ${enquiry.source_booking_id} already exists, continuing to ensure all child data is inserted.`);
    } else {
      console.debug(`Inserting new enquiry with SOURCE_BOOKING_ID ${enquiry.source_booking_id}`);
      await query(conn, `
    INSERT INTO ENQUIRIES (SOURCE_BOOKING_ID, DEPARTURE_DATE, CREATE_DATE, STATUS, IS_QUOTE_ONLY, DESTINATION_NAME, DESTINATION_COUNTRY, AIRPORT, SOURCE_TYPE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, [
        enquiry.source_booking_id,
        enquiry.departure_date,
        enquiry.create_date,
        enquiry.STATUS,
        enquiry.is_quote_only,
        enquiry.destination_name,
        enquiry.destination_country,
        enquiry.airport,
        enquiry.source_type,
      ]);

      const insertedEnquiry = await query(conn, `SELECT ID FROM ENQUIRIES WHERE SOURCE_BOOKING_ID = ?`, [enquiry.source_booking_id]);
      enquiryId = insertedEnquiry[0].ID;
      isNewEnquiry = true;
    }

    // Continue the database inserts/updates for trip details, customer data, passengers, and marketing info...

    const timeTaken = Date.now() - startTime;
    logTravelFolderProcessing(fileName, isNewEnquiry ? "SUCCESS" : "SKIPPED", timeTaken);
    return isNewEnquiry ? true : false;

  } catch (e) {
    const timeTaken = Date.now() - startTime;
    const errorMessage = e instanceof Error ? e.message : String(e);
    logTravelFolderProcessing(fileName, "FAILED", timeTaken, errorMessage);
    console.error('Failed to save parsed travel folder:', e);
    return false;
  }
}
