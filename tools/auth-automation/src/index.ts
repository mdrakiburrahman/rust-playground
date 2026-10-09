export {
  AuthAutomationService,
  type AuthAutomationDependencies,
  type LoginOptions,
  type StatusOptions,
} from "./auth-service.js";
export {
  buildAccountShowCommand,
  buildLoginCommand,
  DEFAULT_TENANT,
  validateTarget,
  validateTenant,
} from "./azure-cli.js";
export {
  decideBrowserAction,
  type BrowserDecision,
  type BrowserFlowState,
} from "./browser-policy.js";
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
  isSafeMicrosoftAuthenticationUrl,
  redactSensitiveText,
  SecretSafeLogger,
} from "./security.js";
export {
  verifyAzureAccount,
  type AzureAccount,
} from "./tenant.js";
