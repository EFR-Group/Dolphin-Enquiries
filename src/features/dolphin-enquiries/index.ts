import path from "path";
import fs from "fs/promises";
import fsSync from "fs";
import { format } from "date-fns";
import { ping, saveParsedTravelFolder, sendEmail } from "..";
import { getMainWindow, updateTrayTooltip } from "../../window";
import { assets, determineReportMode, documentsFolder, getWeekDateStrings, isWithinPastNDays, loadEmailTemplate, mergePerDateCountsPreferLatestSnapshot, runWithConcurrencyLimit } from "../../utils";

/**
 * Parses the Dolphin Enquiries XML files and processes them.
 * 
 * This function reads all files in the specified folder, processes them in parallel (using concurrency), 
 * and saves the parsed travel data to the database. It tracks the number of leisure and golf records 
 * processed per file, and returns these counts per date.
 * 
 * @param {number} howLong - The number of past days to consider when processing files.
 * @returns {Promise<Array<{ date: string, leisureCount: number, golfCount: number }>>} 
 *  A promise that resolves to an array of objects containing the date and counts of leisure and golf records.
 */
async function parseFilesAndSendToDatabase(howLong: number): Promise<Array<{ date: string, leisureCount: number, golfCount: number }>> {
  updateTrayTooltip("Parsing Dolphin Enquiries files...");

  ping('EFR-Electron-DolphinEnquiries', { state: 'run' });

  const baseFolder = path.join(documentsFolder(), "DolphinEnquiries", "completed");

  if (!fsSync.existsSync(baseFolder)) {
    console.error("Base folder does not exist:", baseFolder);
    return [];
  }

  const folderNames = await fs.readdir(baseFolder);
  const results: Array<{ date: string, leisureCount: number, golfCount: number }> = [];

  for (const folderName of folderNames) {
    const folderPath = path.join(baseFolder, folderName);
    const stat = await fs.stat(folderPath);
    if (!stat.isDirectory() || !isWithinPastNDays(folderName, howLong)) continue;

    const files = (await fs.readdir(folderPath)).filter(f => f.toLowerCase().endsWith(".xml"));
    let leisureCount = 0;
    let golfCount = 0;

    const resultsPerFile = await runWithConcurrencyLimit(files, 4, async (file) => {
      try {
        const fullPath = path.join(folderPath, file);
        const xmlContent = await fs.readFile(fullPath, "utf-8");
        const saved = await saveParsedTravelFolder(xmlContent, file);
        return { file, saved };
      } catch (error) {
        console.error(`Failed to process file ${file}`, error);

        const message =
          error instanceof Error ? error.message : String(error);

        ping('EFR-Electron-DolphinEnquiries', { state: 'warn', message: `Failed to process file ${file}: ${message}` });

        return { file, saved: false };
      }
    });

    for (const { file, saved } of resultsPerFile) {
      const lowerFile = file.toLowerCase();
      if (lowerFile.startsWith("lwc")) leisureCount++;
      else if (lowerFile.startsWith("egr")) golfCount++;

      if (!saved) {
        ping('EFR-Electron-DolphinEnquiries', {
          state: 'warn',
          message: `Counted ${file} for reporting, but database ingestion failed.`,
        });
      }
    }

    if (leisureCount + golfCount > 0) {
      const date = `${folderName.slice(0, 4)}-${folderName.slice(4, 6)}-${folderName.slice(6)}`;
      results.push({ date, leisureCount, golfCount });
    }
  }

  ping('EFR-Electron-DolphinEnquiries', { state: 'complete' });

  return results;
}

/**
 * Main function to check and process Dolphin Enquiries files.
 * 
 * This function processes files within the last `howLong` days, either parsing them for daily or weekly reports, 
 * and then sending the report via email. It also caches daily counts for weekly reporting.
 * 
 * @param {number} [howLong=10] - The number of days to look back when processing files (default is 10 days).
 * @returns {Promise<void>} Resolves when the process is complete and the report is sent.
 */
export async function checkDolphinFiles(howLong: number = 10): Promise<void> {
  const runDate = new Date();
  const reportDate = new Date(runDate);

  const dateKey = format(reportDate, "yyyyMMdd");
  const dateKeyFormatted = format(reportDate, "yyyy-MM-dd");
  const isFriday = reportDate.getDay() === 5;
  const weeklyStorePath = path.join(documentsFolder(), "DolphinEnquiries", "cache", "weekly");

  await fs.mkdir(weeklyStorePath, { recursive: true });

  let reportDateCounts: Array<{ date: string; leisureCount: number; golfCount: number }> = [];

  try {
    const parsedCounts = await parseFilesAndSendToDatabase(howLong);
    reportDateCounts = parsedCounts.filter(({ date }) => date === dateKeyFormatted);
    const cacheFile = path.join(weeklyStorePath, `${dateKey}.json`);
    await fs.writeFile(cacheFile, JSON.stringify(reportDateCounts, null, 2), "utf-8");
  } catch (err) {
    console.error("Error saving daily counts:", err);
    reportDateCounts = [];
  }

  let reportCounts = reportDateCounts;
  let subject = `Dolphin Enquiries Report for ${dateKeyFormatted}`;

  if (isFriday) {
    const weekDates = getWeekDateStrings(reportDate);
    const snapshots: Array<{ snapshotKey: string; perDateCounts: Array<{ date: string; leisureCount: number; golfCount: number }> }> = [];

    for (const day of weekDates) {
      const file = path.join(weeklyStorePath, `${day}.json`);
      try {
        if (fsSync.existsSync(file)) {
          const content = await fs.readFile(file, "utf-8");
          const parsed = JSON.parse(content);
          snapshots.push({ snapshotKey: day, perDateCounts: parsed });
        }
      } catch (err) {
        console.warn("Failed to read cached file for", day, err);
      }
    }

    const formatDashedDate = (yyyymmdd: string) =>
      `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`;

    const allowedDates = new Set(weekDates.map(formatDashedDate));
    reportCounts = mergePerDateCountsPreferLatestSnapshot(snapshots, allowedDates);

    subject = `Weekly Dolphin Enquiries Report (${formatDashedDate(weekDates[0])} to ${formatDashedDate(weekDates.at(-1)!)})`;

    for (const day of weekDates) {
      const file = path.join(weeklyStorePath, `${day}.json`);
      try {
        if (fsSync.existsSync(file)) await fs.unlink(file);
      } catch (err) {
        console.warn("Failed to delete cached file for", day, err);
      }
    }
  }

  getMainWindow()?.loadFile(assets.template('report.html'));
  getMainWindow()?.webContents.once('did-finish-load', () => {
    getMainWindow()?.webContents.send('report-data', { perDateCounts: reportCounts });
  });

  const totalLeisure = reportCounts.reduce((sum, d) => sum + d.leisureCount, 0);
  const totalGolf = reportCounts.reduce((sum, d) => sum + d.golfCount, 0);
  updateTrayTooltip(`Processed ${totalLeisure + totalGolf} enquiries across ${reportCounts.length} day(s)`);
  const mode = determineReportMode(reportCounts);
  const html = await loadEmailTemplate(reportCounts, totalLeisure, totalGolf, mode);

  await sendEmail(undefined, subject, undefined, html);
}
