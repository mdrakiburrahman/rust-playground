import os from "node:os";
import type { HostEnvironment } from "./contracts.js";
import { NodeFileSystemAdapter } from "./profile.js";
import { NodeProcessAdapter } from "./process-adapter.js";
import { PlaywrightBrowserAdapter } from "./playwright-browser.js";
import { SecretSafeLogger } from "./security.js";
import { SystemTimer } from "./timing.js";
import {
  AuthAutomationService,
  type AuthAutomationDependencies,
} from "./auth-service.js";

export class NodeHostEnvironment implements HostEnvironment {
  readonly platform = process.platform;
  readonly variables = process.env;

  homeDirectory(): string {
    return os.homedir();
  }
}

export function createRuntimeDependencies(): AuthAutomationDependencies {
  const timer = new SystemTimer();
  return {
    environment: new NodeHostEnvironment(),
    timer,
    processes: new NodeProcessAdapter(timer),
    browser: new PlaywrightBrowserAdapter(),
    fileSystem: new NodeFileSystemAdapter(),
    logger: new SecretSafeLogger(),
  };
}

export function createAuthAutomationService(): AuthAutomationService {
  return new AuthAutomationService(createRuntimeDependencies());
}
