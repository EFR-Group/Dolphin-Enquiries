import { spawn } from "child_process";
import fs from "fs/promises";
import { killProcessTree, logToFile } from "../../utils";

export async function runPowershellScript(
  scriptPath: string,
  args: Record<string, string | number | boolean | undefined> = {},
  options: RunPowershellOptions = {}
): Promise<void> {
  try {
    await fs.access(scriptPath);
  } catch {
    throw new Error(`PowerShell script not found: ${scriptPath}`);
  }

  const psArgs: string[] = [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    scriptPath,
  ];

  for (const [key, value] of Object.entries(args)) {
    if (value === undefined) continue;

    if (typeof value === "boolean") {
      if (value) psArgs.push(`-${key}`);
      continue;
    }

    psArgs.push(`-${key}`, String(value));
  }

  logToFile("database-restorer", `[PS] Running: powershell.exe ${psArgs.join(" ")}`);

  const child = spawn("powershell.exe", psArgs, {
    cwd: options.cwd,
    windowsHide: true,
  });

  let stdout = "";
  let stderr = "";

  child.stdout.on("data", (d) => {
    const s = d.toString();
    stdout += s;

    for (const line of s.split(/\r?\n/)) {
      if (line.trim()) logToFile("database-restorer", `[PS][OUT] ${line}`);
    }
  });

  child.stderr.on("data", (d) => {
    const s = d.toString();
    stderr += s;

    for (const line of s.split(/\r?\n/)) {
      if (line.trim()) logToFile("database-restorer", `[PS][ERR] ${line}`);
    }
  });

  const timeoutMs = options.timeoutMs ?? 2 * 60 * 60 * 1000;
  let timeout: NodeJS.Timeout | null = null;

  const exitCode = await new Promise<number>((resolve, reject) => {
    timeout = setTimeout(() => {
      try {
        if (child.pid) killProcessTree(child.pid);
      } catch {}

      reject(new Error(`PowerShell restore timed out after ${timeoutMs}ms`));
    }, timeoutMs);

    child.on("error", (err) => reject(err));
    child.on("close", (code) => resolve(code ?? 0));
  }).finally(() => {
    if (timeout) clearTimeout(timeout);
  });

  if (exitCode !== 0) {
    const tail = (stdout + "\n" + stderr).slice(-8000);
    throw new Error(
      `Restore script failed (exit code ${exitCode}). Output tail:\n${tail}`
    );
  }

  logToFile("database-restorer", `[PS] Restore script completed successfully`);
}
