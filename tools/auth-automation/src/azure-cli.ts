import type {
  CliTarget,
  CommandSpec,
  HostEnvironment,
  TargetOptions,
} from "./contracts.js";

export const DEFAULT_TENANT = "72f988bf-86f1-41af-91ab-2d7cd011db47";

const TENANT_PATTERN = /^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$/;
const WSL_DISTRO_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
export const WSL_PID_MARKER = "__AUTH_AUTOMATION_WSL_PID__=";
const WSL_COMMAND_WRAPPER =
  `printf '${WSL_PID_MARKER}%s\\n' "$$" >&2; exec "$@"`;
export const WSL_BROWSER_COMMAND =
  `sh -c 'exec sh "$AUTH_AUTOMATION_BROWSER_HELPER" "$1"' auth-automation-browser %s`;

export function validateTenant(tenant: string): string {
  const normalized = tenant.trim();
  if (!TENANT_PATTERN.test(normalized)) {
    throw new Error(
      "Tenant must be a GUID or verified tenant domain containing only letters, digits, dots, and hyphens.",
    );
  }
  return normalized;
}

export function validateTarget(target: string): CliTarget {
  if (target === "current" || target === "windows" || target === "wsl") {
    return target;
  }
  throw new Error(`Unsupported target "${target}". Use current, windows, or wsl.`);
}

export function validateWslDistro(distro: string | undefined): string {
  const normalized = distro?.trim() ?? "";
  if (!WSL_DISTRO_PATTERN.test(normalized)) {
    throw new Error(
      "--wsl-distro is required for the wsl target and may contain only letters, digits, dots, underscores, and hyphens.",
    );
  }
  return normalized;
}

function buildWslCommand(
  innerCommand: readonly string[],
  distro: string,
  environment: HostEnvironment,
  label: string,
): CommandSpec {
  if (environment.platform !== "win32") {
    throw new Error(
      "The wsl target must be launched from Windows so wsl.exe can select a named distribution.",
    );
  }
  return {
    command: "wsl.exe",
    args: [
      "--distribution",
      distro,
      "--exec",
      "sh",
      "-c",
      WSL_COMMAND_WRAPPER,
      "auth-automation",
      ...innerCommand,
    ],
    label,
    termination: {
      kind: "wsl",
      distro,
      pidMarker: WSL_PID_MARKER,
    },
  };
}

function wrapAzureCommand(
  azureArgs: readonly string[],
  targetOptions: TargetOptions,
  environment: HostEnvironment,
): CommandSpec {
  if (targetOptions.target === "wsl") {
    const distro = validateWslDistro(targetOptions.wslDistro);
    return buildWslCommand(
      ["az", ...azureArgs],
      distro,
      environment,
      `Azure CLI in WSL distribution ${distro}`,
    );
  }

  const useWindowsCli =
    targetOptions.target === "windows" ||
    (targetOptions.target === "current" && environment.platform === "win32");

  if (useWindowsCli) {
    if (environment.platform !== "win32") {
      throw new Error(
        "The windows target is supported only when auth-automation is running on native Windows. Inside WSL/Linux, use --target current.",
      );
    }
    return {
      command: "cmd.exe",
      args: ["/d", "/s", "/c", "az", ...azureArgs],
      label: "Windows Azure CLI",
    };
  }

  return {
    command: "az",
    args: azureArgs,
    label: "Azure CLI on the current host",
  };
}

export function buildLoginCommand(
  tenant: string,
  targetOptions: TargetOptions,
  environment: HostEnvironment,
): CommandSpec {
  const validatedTenant = validateTenant(tenant);
  return wrapAzureCommand(
    [
      "login",
      "--use-device-code",
      "--tenant",
      validatedTenant,
      "--output",
      "none",
    ],
    targetOptions,
    environment,
  );
}

export function buildAccountShowCommand(
  targetOptions: TargetOptions,
  environment: HostEnvironment,
): CommandSpec {
  return wrapAzureCommand(
    ["account", "show", "--output", "json", "--only-show-errors"],
    targetOptions,
    environment,
  );
}

function validateWslAbsolutePath(value: string, label: string): string {
  if (
    !value.startsWith("/") ||
    value.includes("\0") ||
    value.includes("\r") ||
    value.includes("\n")
  ) {
    throw new Error(`${label} must be an absolute single-line WSL path.`);
  }
  return value;
}

export function buildWslPathCommand(
  windowsPath: string,
  wslDistro: string,
  environment: HostEnvironment,
): CommandSpec {
  const distro = validateWslDistro(wslDistro);
  if (environment.platform !== "win32") {
    throw new Error(
      "Browser login path conversion is supported only from native Windows.",
    );
  }
  return {
    command: "wsl.exe",
    args: [
      "--distribution",
      distro,
      "--exec",
      "wslpath",
      "-a",
      "-u",
      windowsPath,
    ],
    label: `WSL path conversion in ${distro}`,
  };
}

export function buildBrowserLoginCommand(
  tenant: string,
  wslDistro: string,
  helperWslPath: string,
  urlFileWslPath: string,
  environment: HostEnvironment,
): CommandSpec {
  const validatedTenant = validateTenant(tenant);
  const distro = validateWslDistro(wslDistro);
  const helperPath = validateWslAbsolutePath(
    helperWslPath,
    "Browser helper path",
  );
  const urlFilePath = validateWslAbsolutePath(
    urlFileWslPath,
    "Authorization URL file path",
  );
  return buildWslCommand(
    [
      "env",
      `AUTH_AUTOMATION_BROWSER_HELPER=${helperPath}`,
      `AUTH_AUTOMATION_URL_FILE=${urlFilePath}`,
      `BROWSER=${WSL_BROWSER_COMMAND}`,
      "az",
      "login",
      "--tenant",
      validatedTenant,
      "--output",
      "none",
    ],
    distro,
    environment,
    `Azure CLI browser login in WSL distribution ${distro}`,
  );
}
