export {
  AuthAutomationService,
  AuthOperationCancelledError,
  type AuthAutomationDependencies,
  type BrowserLoginOptions,
  type LoginOptions,
  type StatusOptions,
  isAuthOperationCancelled,
} from "./auth-service.js";
export {
  buildAccountShowCommand,
  buildBrowserLoginCommand,
  buildLoginCommand,
  buildWslPathCommand,
  DEFAULT_TENANT,
  validateWslDistro,
  validateTarget,
  validateTenant,
} from "./azure-cli.js";
export {
  decideBrowserAction,
  decideBrowserRedirectAction,
  type BrowserDecision,
  type BrowserFlowState,
  type BrowserRedirectFlowState,
} from "./browser-policy.js";
export {
  BROWSER_CAPTURE_HELPER,
  BROWSER_CAPTURE_HELPER_FILE,
  BROWSER_CAPTURE_URL_FILE,
  parseCapturedUrlFile,
} from "./browser-login.js";
export type {
  BrowserAdapter,
  BrowserSession,
  CliTarget,
  CommandSpec,
  FileSystemAdapter,
  HostEnvironment,
  PageSnapshot,
  ProcessAdapter,
  ProcessResult,
  RunningProcess,
  SafeControl,
  SafeLogger,
  TargetOptions,
  TimerAdapter,
  WslTerminationMetadata,
} from "./contracts.js";
export {
  DeviceCodeParser,
  type DeviceCodeDetails,
} from "./device-code.js";
export {
  resolveBrowserProfile,
  resolveRuntimeRoot,
  type BrowserProfile,
} from "./profile.js";
export { NodeProcessAdapter } from "./process-adapter.js";
export {
  collectProcessTree,
  NodeProcessTreeTerminator,
  parseProcessTable,
  type ProcessRecord,
  type ProcessTreeTerminationRequest,
  type ProcessTreeTerminator,
} from "./process-tree.js";
export {
  assertSafeVerificationUrl,
  assertSafeBrowserAuthorizationUrl,
  isExpectedLocalhostRedirect,
  isSafeMicrosoftAuthenticationUrl,
  redactSensitiveText,
  SecretSafeLogger,
  type BrowserAuthorizationRequest,
} from "./security.js";
export {
  verifyAzureAccount,
  type AzureAccount,
} from "./tenant.js";
