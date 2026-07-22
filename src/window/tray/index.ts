import { Tray, nativeTheme, nativeImage, Menu } from "electron";
import { checkForUpdates, downloadBakFilesFromSftpThree } from "../../features";
import { assets } from "../../utils";
import { getMainWindow } from "../main-window";
import { createSettingsWindow } from "../settings";

let tray: Tray;

/**
 * Sets up the system tray for the application, including a context menu with various options.
 * The tray displays an icon and allows users to interact with the app through different actions.
 *
 * Enquiry file processing and count emails run in Azure Functions; this app retains .bak download/restore.
 *
 * @param {Function} onQuit - A callback function that will be invoked when the "Quit" menu item is selected.
 * The `onQuit` callback should handle the termination of the application.
 */
export function setupTray(onQuit: () => void): void {
  const iconPath = assets.image(nativeTheme.shouldUseDarkColors ? "company-icon.png" : "company-icon-dark.png");
  tray = new Tray(nativeImage.createFromPath(iconPath));

  const contextMenu = Menu.buildFromTemplate([
    { label: "Dolphin Enquiries (bak only)", enabled: false },
    { type: "separator" },
    { label: "Download Dolphin Database", click: () => downloadBakFilesFromSftpThree().catch(console.error) },
    { type: "separator" },
    { label: "Settings", click: () => createSettingsWindow() },
    { label: "Check for Updates", click: () => checkForUpdates() },
    { type: "separator" },
    { label: "Quit", click: onQuit }
  ]);

  tray.setToolTip("Dolphin Enquiries (bak restore)");
  tray.setContextMenu(contextMenu);

  tray.on("click", () => {
    if (getMainWindow()?.isVisible()) {
      getMainWindow()?.focus();
    } else {
      getMainWindow()?.show();
    }
  });
}

/**
 * Updates the tooltip message of the system tray icon.
 * 
 * @param {string} message - The message to be displayed in the tray icon's tooltip.
 */
export function updateTrayTooltip(message: string): void {
  if (tray) tray.setToolTip(message);
}
