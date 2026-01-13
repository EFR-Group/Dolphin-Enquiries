import nodemailer from "nodemailer";
import { settings } from "../../utils/settings";
import { assets } from "../../utils";


/**
 * Sends an email using the configured SMTP settings.
 * @param {string | undefined} to Recipient email address. If not provided, the default recipient from the settings will be used.
 * @param {string} subject Email subject line.
 * @param {string} [text] Plain text content of the email.
 * @param {string} [html] HTML content of the email. If provided, the `{{logo}}` placeholder will be replaced with an embedded image of the logo.
 * @returns {Promise<void>} Resolves when the email has been sent.
 * @throws {Error} If SMTP settings are not configured.
 */
export async function sendEmail(to: string | undefined, subject: string, text?: string, html?: string): Promise<void> {
  const config = await settings.getSMTPConfig();

  // Check if SMTP settings are available
  if (!config) {
    throw new Error('SMTP settings not configured. Please set up your email settings in the tray menu.');
  }

  // Create the email transporter with the SMTP configuration
  const transporter = nodemailer.createTransport({
    host: config.host,
    port: config.port,
    secure: config.secure,
    auth: {
      user: config.user,
      pass: config.pass,
    },
  });

  // Define the email attachments (e.g., logo image)
  const attachments = [
    {
      filename: 'logo.png',
      path: assets.image("logo.png"),
      cid: 'logo@dolphin', // Content-ID for embedding the logo in the email body
    }
  ];

  // If HTML content is provided, replace the placeholder {{logo}} with the embedded image
  const updatedHtml = html ? html.replace(/{{logo}}/g, '<img src="cid:logo@dolphin" alt="EFR Travel" />') : undefined;

  // Send the email
  const info = await transporter.sendMail({
    from: `"Dolphin Enquiries" <no-reply@efrtravel.com>`, // Sender address
    to: to ?? config.to, // Recipient address
    subject, // Subject line
    text, // Plain text content (optional)
    html: updatedHtml, // HTML content with embedded logo (optional)
    attachments, // Attachments (in this case, the logo image)
  });

  console.debug("Email sent:", info.messageId); // Log the message ID for reference
}
