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

function validateWslDistro(distro: string | undefined): string {
  const normalized = distro?.trim() ?? "";
  if (!WSL_DISTRO_PATTERN.test(normalized)) {
    throw new Error(
      "--wsl-distro is required for the wsl target and may contain only letters, digits, dots, underscores, and hyphens.",
    );
  }
  return normalized;
}

function wrapAzureCommand(
  azureArgs: readonly string[],
  targetOptions: TargetOptions,
  environment: HostEnvironment,
): CommandSpec {
  if (targetOptions.target === "wsl") {
    if (environment.platform !== "win32") {
      throw new Error(
        "The wsl target must be launched from Windows so wsl.exe can select a named distribution.",
      );
    }
    const distro = validateWslDistro(targetOptions.wslDistro);
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
        "az",
        ...azureArgs,
      ],
      label: `Azure CLI in WSL distribution ${distro}`,
      termination: {
        kind: "wsl",
        distro,
        pidMarker: WSL_PID_MARKER,
      },
    };
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
