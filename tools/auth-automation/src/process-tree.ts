import { spawn } from "node:child_process";
import type { CommandSpec } from "./contracts.js";

const UTILITY_TIMEOUT_MS = 10_000;
const TERMINATION_GRACE_MS = 2_000;
const TERMINATION_CONFIRM_MS = 5_000;

export interface ProcessRecord {
  readonly pid: number;
  readonly parentPid: number;
}

export interface ProcessTreeTerminationRequest {
  readonly rootPid: number;
  readonly platform: NodeJS.Platform;
  readonly command: CommandSpec;
  readonly processExited: Promise<void>;
  readonly wslProcessId?: Promise<number | undefined>;
}

export interface ProcessTreeTerminator {
  terminate(request: ProcessTreeTerminationRequest): Promise<void>;
}

interface UtilityResult {
  readonly exitCode: number | null;
  readonly stdout: string;
  readonly stderr: string;
}

function appendBounded(current: string, chunk: string): string {
  return `${current}${chunk}`.slice(-64 * 1024);
}

function delay(delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, delayMs);
  });
}

async function waitForPromise(
  promise: Promise<void>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  let timeout: unknown;
  try {
    await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timeout = globalThis.setTimeout(() => {
          reject(new Error(message));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) {
      globalThis.clearTimeout(timeout as NodeJS.Timeout);
    }
  }
}

async function waitForOptionalPid(
  promise: Promise<number | undefined>,
  timeoutMs: number,
): Promise<number | undefined> {
  let timeout: unknown;
  try {
    return await Promise.race([
      promise,
      new Promise<undefined>((resolve) => {
        timeout = globalThis.setTimeout(resolve, timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) {
      globalThis.clearTimeout(timeout as NodeJS.Timeout);
    }
  }
}

function runUtility(
  command: string,
  args: readonly string[],
  timeoutMs = UTILITY_TIMEOUT_MS,
): Promise<UtilityResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, [...args], {
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    let settled = false;
    const timeout = globalThis.setTimeout(() => {
      timedOut = true;
      child.kill();
    }, timeoutMs);

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdout = appendBounded(stdout, chunk);
    });
    child.stderr.on("data", (chunk: string) => {
      stderr = appendBounded(stderr, chunk);
    });
    child.once("error", (error) => {
      if (!settled) {
        settled = true;
        globalThis.clearTimeout(timeout);
        reject(error);
      }
    });
    child.once("close", (exitCode) => {
      if (!settled) {
        settled = true;
        globalThis.clearTimeout(timeout);
        if (timedOut) {
          reject(
            new Error(
              `${command} process-tree helper timed out after ${timeoutMs}ms.`,
            ),
          );
        } else {
          resolve({ exitCode, stdout, stderr });
        }
      }
    });
  });
}

export function parseProcessTable(output: string): ProcessRecord[] {
  const records: ProcessRecord[] = [];
  for (const line of output.split(/\r?\n/)) {
    const match = line.trim().match(/^(\d+)\s+(\d+)$/);
    if (match) {
      records.push({
        pid: Number(match[1]),
        parentPid: Number(match[2]),
      });
    }
  }
  return records;
}

export function collectProcessTree(
  records: readonly ProcessRecord[],
  rootPid: number,
): number[] {
  const depthByPid = new Map<number, number>([[rootPid, 0]]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const record of records) {
      const parentDepth = depthByPid.get(record.parentPid);
      if (parentDepth !== undefined && !depthByPid.has(record.pid)) {
        depthByPid.set(record.pid, parentDepth + 1);
        changed = true;
      }
    }
  }
  return [...depthByPid.entries()]
    .sort(
      ([leftPid, leftDepth], [rightPid, rightDepth]) =>
        rightDepth - leftDepth || rightPid - leftPid,
    )
    .map(([pid]) => pid);
}

function windowsTreeScript(rootPid: number): string {
  return `
$ErrorActionPreference = 'Stop'
$RootProcessId = ${rootPid}
$TimeoutMs = ${TERMINATION_CONFIRM_MS}

function Get-TreeIds {
  $Snapshot = @(Get-CimInstance -ClassName Win32_Process | Select-Object ProcessId, ParentProcessId, CreationDate)
  $RootRecord = $Snapshot | Where-Object { [int]$_.ProcessId -eq $RootProcessId } | Select-Object -First 1
  if ($null -eq $RootRecord) {
    return @($RootProcessId)
  }
  $RootCreationTime = [DateTime]$RootRecord.CreationDate
  $DepthById = @{}
  $DepthById[$RootProcessId] = 0
  $Changed = $true
  while ($Changed) {
    $Changed = $false
    foreach ($ProcessRecord in $Snapshot) {
      $ProcessId = [int]$ProcessRecord.ProcessId
      $ParentProcessId = [int]$ProcessRecord.ParentProcessId
      $CreatedAfterRoot = [DateTime]$ProcessRecord.CreationDate -ge $RootCreationTime
      if ($CreatedAfterRoot -and -not $DepthById.ContainsKey($ProcessId) -and $DepthById.ContainsKey($ParentProcessId)) {
        $DepthById[$ProcessId] = [int]$DepthById[$ParentProcessId] + 1
        $Changed = $true
      }
    }
  }
  return @($DepthById.GetEnumerator() | Sort-Object Value -Descending | ForEach-Object { [int]$_.Key })
}

$KnownIds = New-Object 'System.Collections.Generic.HashSet[int]'
for ($Pass = 0; $Pass -lt 3; $Pass += 1) {
  $CurrentTreeIds = @(Get-TreeIds)
  foreach ($ProcessId in $CurrentTreeIds) {
    $null = $KnownIds.Add([int]$ProcessId)
    if ([int]$ProcessId -ne $RootProcessId) {
      Stop-Process -Id ([int]$ProcessId) -Force -ErrorAction SilentlyContinue
    }
  }
  Start-Sleep -Milliseconds 25
}
$null = $KnownIds.Add($RootProcessId)
Stop-Process -Id $RootProcessId -Force -ErrorAction SilentlyContinue
$TreeIds = @($KnownIds)

$Deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)
do {
  $RemainingIds = @()
  foreach ($ProcessId in $TreeIds) {
    if ($null -ne (Get-Process -Id ([int]$ProcessId) -ErrorAction SilentlyContinue)) {
      $RemainingIds += [int]$ProcessId
    }
  }
  if ($RemainingIds.Count -eq 0) {
    exit 0
  }
  foreach ($ProcessId in $RemainingIds) {
    Stop-Process -Id ([int]$ProcessId) -Force -ErrorAction SilentlyContinue
  }
  Start-Sleep -Milliseconds 50
} while ([DateTime]::UtcNow -lt $Deadline)

throw "Processes still running after Stop-Process -Id: $($RemainingIds -join ',')"
`;
}

async function terminateWindowsTree(rootPid: number): Promise<void> {
  const encodedScript = Buffer.from(
    windowsTreeScript(rootPid),
    "utf16le",
  ).toString("base64");
  const result = await runUtility("powershell.exe", [
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy",
    "Bypass",
    "-EncodedCommand",
    encodedScript,
  ]);
  if (result.exitCode !== 0) {
    throw new Error(
      `Stop-Process -Id tree cleanup failed for PID ${rootPid}: ${result.stderr.trim() || result.stdout.trim() || `exit ${result.exitCode}`}`,
    );
  }
}

async function queryWslProcesses(
  distro: string,
): Promise<ProcessRecord[]> {
  const result = await runUtility("wsl.exe", [
    "--distribution",
    distro,
    "--exec",
    "ps",
    "-eo",
    "pid=,ppid=",
  ]);
  if (result.exitCode !== 0) {
    throw new Error(
      `Unable to inspect processes in WSL distribution ${distro}: ${result.stderr.trim() || `exit ${result.exitCode}`}`,
    );
  }
  return parseProcessTable(result.stdout);
}

async function signalWslProcesses(
  distro: string,
  signal: "-TERM" | "-KILL",
  processIds: readonly number[],
): Promise<void> {
  if (processIds.length === 0) {
    return;
  }
  await runUtility("wsl.exe", [
    "--distribution",
    distro,
    "--exec",
    "kill",
    signal,
    ...processIds.map(String),
  ]);
}

async function terminateWslTree(
  distro: string,
  rootPid: number,
): Promise<void> {
  const knownProcessIds = new Set<number>();

  const refresh = async (): Promise<number[]> => {
    const records = await queryWslProcesses(distro);
    const runningIds = new Set(records.map((record) => record.pid));
    if (runningIds.has(rootPid)) {
      for (const processId of collectProcessTree(records, rootPid)) {
        knownProcessIds.add(processId);
      }
    }
    return [...knownProcessIds].filter((processId) =>
      runningIds.has(processId),
    );
  };

  let remaining = await refresh();
  if (remaining.length === 0) {
    return;
  }

  await signalWslProcesses(distro, "-TERM", remaining);
  const gracefulDeadline = Date.now() + TERMINATION_GRACE_MS;
  while (Date.now() < gracefulDeadline) {
    await delay(100);
    remaining = await refresh();
    if (remaining.length === 0) {
      return;
    }
  }

  await signalWslProcesses(distro, "-KILL", remaining);
  const confirmationDeadline = Date.now() + TERMINATION_CONFIRM_MS;
  while (Date.now() < confirmationDeadline) {
    await delay(100);
    remaining = await refresh();
    if (remaining.length === 0) {
      return;
    }
  }
  throw new Error(
    `WSL processes still running after PID-specific termination: ${remaining.join(",")}.`,
  );
}

function processGroupExists(processGroupId: number): boolean {
  try {
    process.kill(-processGroupId, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code !== "ESRCH";
  }
}

async function terminatePosixTree(rootPid: number): Promise<void> {
  if (!processGroupExists(rootPid)) {
    return;
  }
  try {
    process.kill(-rootPid, "SIGTERM");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
      throw error;
    }
  }

  const gracefulDeadline = Date.now() + TERMINATION_GRACE_MS;
  while (Date.now() < gracefulDeadline && processGroupExists(rootPid)) {
    await delay(100);
  }
  if (!processGroupExists(rootPid)) {
    return;
  }

  try {
    process.kill(-rootPid, "SIGKILL");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") {
      throw error;
    }
  }
  const confirmationDeadline = Date.now() + TERMINATION_CONFIRM_MS;
  while (Date.now() < confirmationDeadline && processGroupExists(rootPid)) {
    await delay(100);
  }
  if (processGroupExists(rootPid)) {
    throw new Error(
      `POSIX process group ${rootPid} is still running after termination.`,
    );
  }
}

export class NodeProcessTreeTerminator implements ProcessTreeTerminator {
  async terminate(request: ProcessTreeTerminationRequest): Promise<void> {
    if (request.platform === "win32") {
      let wslFailure: unknown;
      if (request.command.termination?.kind === "wsl") {
        try {
          const wslProcessId = request.wslProcessId
            ? await waitForOptionalPid(
                request.wslProcessId,
                TERMINATION_GRACE_MS,
              )
            : undefined;
          if (!wslProcessId) {
            throw new Error(
              "The WSL Azure CLI PID marker was not received; Linux process cleanup cannot be confirmed.",
            );
          }
          await terminateWslTree(
            request.command.termination.distro,
            wslProcessId,
          );
        } catch (error) {
          wslFailure = error;
        }
      }

      let windowsFailure: unknown;
      try {
        await terminateWindowsTree(request.rootPid);
        await waitForPromise(
          request.processExited,
          TERMINATION_CONFIRM_MS,
          `Windows process ${request.rootPid} did not report exit after Stop-Process -Id.`,
        );
      } catch (error) {
        windowsFailure = error;
      }

      if (wslFailure || windowsFailure) {
        const messages = [wslFailure, windowsFailure]
          .filter(Boolean)
          .map((error) =>
            error instanceof Error ? error.message : String(error),
          );
        throw new Error(messages.join(" "));
      }
      return;
    }

    await terminatePosixTree(request.rootPid);
    await waitForPromise(
      request.processExited,
      TERMINATION_CONFIRM_MS,
      `Process ${request.rootPid} did not report exit after process-group termination.`,
    );
  }
}
