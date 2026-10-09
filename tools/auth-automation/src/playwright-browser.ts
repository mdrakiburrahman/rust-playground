import {
  chromium,
  type BrowserContext,
  type Locator,
  type Page,
} from "playwright";
import type {
  BrowserAdapter,
  BrowserLaunchRequest,
  BrowserSession,
  PageSnapshot,
  SafeControl,
} from "./contracts.js";

const SAFE_CONTROLS: readonly SafeControl[] = [
  "Next",
  "Continue",
  "Accept",
  "Consent",
];
const DEVICE_CODE_PAGE_PATTERN =
  /enter (?:the )?code displayed (?:on|by)|code (?:from|shown in) (?:your )?(?:app|device)|device code/i;

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

async function firstVisible(
  locators: readonly Locator[],
): Promise<Locator | undefined> {
  for (const locator of locators) {
    const first = locator.first();
    if ((await first.count()) > 0 && (await first.isVisible())) {
      return first;
    }
  }
  return undefined;
}

class PlaywrightBrowserSession implements BrowserSession {
  private closed = false;
  private readonly navigationUrls: string[] = [];

  constructor(
    private readonly context: BrowserContext,
    private readonly page: Page,
    private readonly actionTimeoutMs: number,
  ) {
    this.page.on("framenavigated", (frame) => {
      if (frame === this.page.mainFrame()) {
        this.navigationUrls.push(frame.url());
        if (this.navigationUrls.length > 20) {
          this.navigationUrls.shift();
        }
      }
    });
  }

  async navigate(url: string): Promise<void> {
    await this.page.goto(url, {
      timeout: this.actionTimeoutMs,
      waitUntil: "domcontentloaded",
    });
  }

  private async deviceCodeInput(): Promise<Locator | undefined> {
    return firstVisible([
      this.page.getByRole("textbox", { name: /device.*code|code/i }),
      this.page.getByLabel(/device.*code|code/i),
    ]);
  }

  async observe(): Promise<PageSnapshot> {
    const bodyText = (
      await this.page
        .locator("body")
        .innerText({ timeout: this.actionTimeoutMs })
        .catch(() => "")
    ).slice(0, 40_000);
    const deviceCodeInput = await this.deviceCodeInput();
    const usernameInput = await firstVisible([
      this.page.getByRole("textbox", {
        name: /email|user\s*name|sign-in address/i,
      }),
      this.page.locator('input[type="email"]'),
      this.page.locator('input[autocomplete="username"]'),
    ]);
    const passwordInput = await firstVisible([
      this.page.locator('input[type="password"]'),
      this.page.locator('input[autocomplete="current-password"]'),
    ]);
    const oneTimeCodeInput = await firstVisible([
      this.page.locator('input[autocomplete="one-time-code"]'),
      this.page.locator('input[inputmode="numeric"]'),
    ]);

    const controls: SafeControl[] = [];
    for (const control of SAFE_CONTROLS) {
      const locator = this.page.getByRole("button", {
        name: new RegExp(`^\\s*${control}\\s*$`, "i"),
      });
      if (await firstVisible([locator])) {
        controls.push(control);
      }
    }

    const accountCandidates = new Set<string>();
    const buttons = this.page.getByRole("button");
    const buttonCount = Math.min(await buttons.count(), 30);
    for (let index = 0; index < buttonCount; index += 1) {
      const button = buttons.nth(index);
      if (!(await button.isVisible())) {
        continue;
      }
      const name = (
        (await button.getAttribute("aria-label")) ??
        (await button.innerText().catch(() => ""))
      ).trim();
      if (
        name &&
        !SAFE_CONTROLS.some(
          (control) => control.toLowerCase() === name.toLowerCase(),
        ) &&
        !/^use another account$/i.test(name)
      ) {
        accountCandidates.add(name);
      }
    }

    return {
      url: this.page.url(),
      navigationUrls: [...this.navigationUrls],
      deviceCodeInputVisible:
        Boolean(deviceCodeInput) && DEVICE_CODE_PAGE_PATTERN.test(bodyText),
      usernameInputVisible: Boolean(usernameInput),
      passwordInputVisible: Boolean(passwordInput),
      oneTimeCodeInputVisible: Boolean(oneTimeCodeInput),
      accountSelectionRequired:
        /pick an account|choose an account|select an account/i.test(bodyText),
      accountCandidates: [...accountCandidates],
      controls,
      bodyText,
    };
  }

  async fillDeviceCode(code: string): Promise<void> {
    const input = await this.deviceCodeInput();
    if (!input) {
      throw new Error(
        "The device-code input disappeared before it could be filled.",
      );
    }
    await input.fill(code, { timeout: this.actionTimeoutMs });
  }

  async clickControl(control: SafeControl): Promise<void> {
    const button = await firstVisible([
      this.page.getByRole("button", {
        name: new RegExp(`^\\s*${control}\\s*$`, "i"),
      }),
    ]);
    if (!button) {
      throw new Error(`The ${control} control is no longer available.`);
    }
    await button.click({ timeout: this.actionTimeoutMs });
  }

  async clickAccount(accountName: string): Promise<void> {
    const account = await firstVisible([
      this.page.getByRole("button", {
        name: new RegExp(escapeRegExp(accountName), "i"),
      }),
    ]);
    if (!account) {
      throw new Error(
        `The existing account tile "${accountName}" is no longer available.`,
      );
    }
    await account.click({ timeout: this.actionTimeoutMs });
  }

  async close(): Promise<void> {
    if (!this.closed) {
      this.closed = true;
      await this.context.close();
    }
  }
}

export class PlaywrightBrowserAdapter implements BrowserAdapter {
  async launch(request: BrowserLaunchRequest): Promise<BrowserSession> {
    try {
      const context = await chromium.launchPersistentContext(
        request.profilePath,
        {
          channel:
            request.browserKind === "edge" ? "msedge" : undefined,
          headless: false,
          timeout: request.actionTimeoutMs,
        },
      );
      context.setDefaultTimeout(request.actionTimeoutMs);
      const page = context.pages()[0] ?? (await context.newPage());
      return new PlaywrightBrowserSession(
        context,
        page,
        request.actionTimeoutMs,
      );
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      const guidance =
        request.browserKind === "edge"
          ? "Install Microsoft Edge on Windows and close any prior auth-automation window that is using the dedicated profile."
          : "Run `npx nx run auth-automation:browser-install` in WSL/Linux, then retry from a WSLg graphical session.";
      throw new Error(`Unable to launch the automation browser. ${guidance} ${detail}`);
    }
  }
}
