import type { ProcessResult, SafeLogger } from "./contracts.js";

const DEVICE_CODE_CONTEXT_PATTERN =
  /((?:enter|use)\s+(?:the\s+)?(?:user\s+|device\s+)?code\s*[:=]?\s*)["']?[A-Z0-9][A-Z0-9-]{5,20}/gi;
const JSON_SECRET_PATTERN =
  /(["']?(?:access[_-]?token|refresh[_-]?token|client[_-]?secret|password)["']?\s*[:=]\s*["']?)[^"',\s}]+/gi;
const BEARER_PATTERN = /(Bearer\s+)[A-Za-z0-9._~+/-]+=*/gi;

export interface BrowserAuthorizationRequest {
  readonly authorizationUrl: string;
  readonly redirectUri: string;
  readonly state: string;
}

export function assertSafeVerificationUrl(value: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("Azure CLI returned an invalid device-login URL.");
  }

  if (parsed.protocol !== "https:") {
    throw new Error("Azure device login must use HTTPS.");
  }

  const host = parsed.hostname.toLowerCase();
  const path = parsed.pathname.toLowerCase();
  const isMicrosoftDeviceLogin =
    (host === "microsoft.com" || host === "www.microsoft.com") &&
    (path === "/devicelogin" || path === "/link");
  const isAkaDeviceLogin =
    host === "aka.ms" && path.startsWith("/devicelogin");
  const isMicrosoftOnline =
    host === "login.microsoftonline.com" && path.length > 1;
  const isMicrosoftLoginDevice =
    host === "login.microsoft.com" && path === "/device";

  if (
    !isMicrosoftDeviceLogin &&
    !isAkaDeviceLogin &&
    !isMicrosoftOnline &&
    !isMicrosoftLoginDevice
  ) {
    throw new Error(
      `Refusing unrecognized device-login host "${parsed.hostname}".`,
    );
  }

  return parsed.toString();
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

export function redactSensitiveText(
  value: string,
  secrets: readonly string[] = [],
): string {
  let redacted = value
    .replace(DEVICE_CODE_CONTEXT_PATTERN, "$1[REDACTED]")
    .replace(JSON_SECRET_PATTERN, "$1[REDACTED]")
    .replace(BEARER_PATTERN, "$1[REDACTED]");

  for (const secret of secrets.filter(Boolean).sort((a, b) => b.length - a.length)) {
    redacted = redacted.replace(
      new RegExp(escapeRegExp(secret), "gi"),
      "[REDACTED]",
    );
  }
  return redacted;
}

export interface LogSink {
  info(message: string): void;
  warn(message: string): void;
  error(message: string): void;
}

export function isSafeMicrosoftAuthenticationUrl(value: string): boolean {
  try {
    const parsed = new URL(value);
    const host = parsed.hostname.toLowerCase();
    return (
      parsed.protocol === "https:" &&
      (host === "aka.ms" ||
        host === "microsoft.com" ||
        host.endsWith(".microsoft.com") ||
        host === "microsoftonline.com" ||
        host.endsWith(".microsoftonline.com"))
    );
  } catch {
    return false;
  }
}

export function assertSafeBrowserAuthorizationUrl(
  value: string,
  tenant: string,
): BrowserAuthorizationRequest {
  if (value !== value.trim()) {
    throw new Error("Captured browser authorization URL contains whitespace.");
  }

  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new Error("Captured browser authorization URL is invalid.");
  }

  const expectedPath = `/${tenant.toLowerCase()}/oauth2/v2.0/authorize`;
  if (
    parsed.protocol !== "https:" ||
    parsed.hostname.toLowerCase() !== "login.microsoftonline.com" ||
    parsed.pathname.toLowerCase() !== expectedPath ||
    parsed.username ||
    parsed.password ||
    parsed.hash
  ) {
    throw new Error(
      "Captured browser authorization URL is not the expected Microsoft tenant authorize endpoint.",
    );
  }

  const clientId = parsed.searchParams.get("client_id");
  const responseType = parsed.searchParams.get("response_type");
  const redirectValue = parsed.searchParams.get("redirect_uri");
  const state = parsed.searchParams.get("state");
  if (
    !clientId ||
    !responseType?.split(/\s+/).includes("code") ||
    !redirectValue ||
    !state
  ) {
    throw new Error(
      "Captured browser authorization URL is missing required OAuth parameters.",
    );
  }

  let redirectUri: URL;
  try {
    redirectUri = new URL(redirectValue);
  } catch {
    throw new Error("Captured browser redirect URI is invalid.");
  }
  const redirectPort = Number(redirectUri.port);
  if (
    redirectUri.protocol !== "http:" ||
    redirectUri.hostname.toLowerCase() !== "localhost" ||
    !Number.isInteger(redirectPort) ||
    redirectPort < 1 ||
    redirectPort > 65_535 ||
    redirectUri.username ||
    redirectUri.password ||
    redirectUri.hash
  ) {
    throw new Error(
      "Captured browser redirect URI must be an HTTP localhost callback with an explicit port.",
    );
  }

  return {
    authorizationUrl: value,
    redirectUri: redirectUri.toString(),
    state,
  };
}

export function isExpectedLocalhostRedirect(
  value: string,
  expectedRedirectUri: string,
  expectedState: string,
): boolean {
  try {
    const current = new URL(value);
    const expected = new URL(expectedRedirectUri);
    return (
      current.protocol === "http:" &&
      current.hostname.toLowerCase() === "localhost" &&
      current.hostname.toLowerCase() === expected.hostname.toLowerCase() &&
      current.port === expected.port &&
      current.pathname === expected.pathname &&
      Boolean(current.searchParams.get("code")) &&
      current.searchParams.get("state") === expectedState &&
      !current.username &&
      !current.password
    );
  } catch {
    return false;
  }
}

export class SecretSafeLogger implements SafeLogger {
  private readonly secrets = new Set<string>();

  constructor(private readonly sink: LogSink = console) {}

  addSecret(secret: string): void {
    if (secret) {
      this.secrets.add(secret);
    }
  }

  sanitize(message: string): string {
    return redactSensitiveText(message, [...this.secrets]);
  }

  info(message: string): void {
    this.sink.info(this.sanitize(message));
  }

  warn(message: string): void {
    this.sink.warn(this.sanitize(message));
  }

  error(message: string): void {
    this.sink.error(this.sanitize(message));
  }
}

export function describeProcessFailure(
  label: string,
  result: ProcessResult,
  logger: SafeLogger,
): string {
  const detail = logger
    .sanitize([result.stderr, result.stdout].filter(Boolean).join("\n"))
    .trim()
    .slice(-2_000);
  const status =
    result.exitCode === null
      ? `terminated by ${result.signal ?? "an unknown signal"}`
      : `exited with code ${result.exitCode}`;
  return detail ? `${label} ${status}: ${detail}` : `${label} ${status}.`;
}
