import { app, Menu, ipcMain } from "electron";
import { enableAutoLaunch, setupAutoUpdater, setupScheduler, checkDolphinFiles, watchAndTransferFiles, downloadBakFilesFromSftpThree } from "./features";
import { createMainWindow, getMainWindow, setIsQuitting, setupSettingsHandlers, setupTray } from "./window";
import { setupSafeRelaunch } from "./utils";

// Disable default Electron crash / GPU dialogs
app.commandLine.appendSwitch("noerrdialogs");
app.commandLine.appendSwitch("disable-gpu-process-crash-limit");

// disable hardware acceleration to prevent GPU crashes
app.disableHardwareAcceleration();

process.on("uncaughtException", (err) => {
  if (!app.isReady()) console.error("Uncaught startup exception:", err);
});

process.on("unhandledRejection", (reason) => {
  if (!app.isReady()) console.error("Unhandled startup rejection:", reason);
});

const gotTheLock = app.requestSingleInstanceLock();

if (!gotTheLock) {
  // If another instance is already running, exit immediately
  app.quit();
  process.exit(0);
} else {
  app.on("second-instance", () => {
    // Prevent new windows or processes from spawning
    console.log("Secondary instance blocked — only one instance is allowed.");
    app.quit();
  });
}

/**
 * This function is executed when the app is ready and sets up the various components for the application.
 * It enables auto-launch, sets up auto-updating, creates the main window, and sets up tray functionality.
 * Additionally, it schedules tasks for periodic actions such as checking dolphin files and transferring files.
 * 
 * @returns {Promise<void>} Resolves when the app is fully initialized and tasks are scheduled.
 */
app.whenReady().then(async () => {
  // Enable auto-launch for the app when the system starts
  await enableAutoLaunch();

  // Set up auto-updater for automatic app updates
  setupAutoUpdater();

  // Set up IPC handlers for managing application settings
  setupSettingsHandlers(ipcMain);

  // Remove the default application menu
  Menu.setApplicationMenu(null);

  // Create the main application window
  createMainWindow();

  // Set up safe relaunch monitoring if the app is packaged
  if (app.isPackaged && getMainWindow()) {
    setupSafeRelaunch(getMainWindow());
  }

  // Set up the system tray with quit functionality
  setupTray(() => {
    setIsQuitting(true);  // Mark the app as quitting
    app.quit();  // Quit the app
  });

  // Set up the scheduler to run tasks at specified intervals
  setupScheduler(
    { task: checkDolphinFiles, schedule: '0 1 * * *' },  // runs at 1:00 AM 
    {
      task: () => downloadBakFilesFromSftpThree().catch(console.error),
      schedule: "30 1 * * *", // runs at 1:30 AM
    },
    // { task: getAllDataIntoSnowflake, schedule: '0 2 * * *' },  // runs at 2:00 AM (commented out)
    { task: watchAndTransferFiles, schedule: '*/30 * * * * *' } // runs every 30 seconds
  );
});

app.on("before-quit", () => {
  setIsQuitting(true);
  console.log("App quitting gracefully...");
});


app.on("window-all-closed", () => {
  console.log("All windows closed — keeping background service alive.");
  // No quit here — app remains running in tray.
});
