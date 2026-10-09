import type {
  PageSnapshot,
  SafeControl,
} from "./contracts.js";
import { isSafeMicrosoftAuthenticationUrl } from "./security.js";

export interface BrowserFlowState {
  readonly codeEntered: boolean;
  readonly codeSubmitted: boolean;
  readonly accountHint?: string;
  readonly accountSelected?: boolean;
}

export type BrowserDecision =
  | { readonly kind: "fill-device-code" }
  | { readonly kind: "click-control"; readonly control: SafeControl }
  | { readonly kind: "click-account"; readonly accountName: string }
  | { readonly kind: "complete" }
  | { readonly kind: "wait" }
  | { readonly kind: "fail"; readonly reason: string };

const MFA_TEXT_PATTERN =
  /approve (?:the )?sign-in request|authenticator app|two-step verification|multi-factor authentication|verify your identity|confirm your identity|enter (?:the )?verification code|text (?:me|a code)|send (?:me |a )?code|call (?:me|my phone)|security key|passkey|windows hello|more information required|keep your account secure|help us protect your account|additional security verification|choose a verification method/i;
const SUCCESS_TEXT_PATTERN =
  /you (?:have|'ve) signed in|you(?:'re| are) signed in|authentication complete|you may now close (?:this )?(?:window|browser)|device (?:has been )?authenticated/i;
const CONTROL_PRIORITY: readonly SafeControl[] = [
  "Next",
  "Continue",
  "Accept",
  "Consent",
];

function normalize(value: string): string {
  return value.trim().toLowerCase();
}

function chooseAccount(
  candidates: readonly string[],
  accountHint: string,
): BrowserDecision {
  const normalizedHint = normalize(accountHint);
  const exact = candidates.filter(
    (candidate) => normalize(candidate) === normalizedHint,
  );
  const matches =
    exact.length > 0
      ? exact
      : candidates.filter((candidate) =>
          normalize(candidate).includes(normalizedHint),
        );

  if (matches.length === 1) {
    return { kind: "click-account", accountName: matches[0] };
  }
  if (matches.length > 1) {
    return {
      kind: "fail",
      reason: `More than one account tile matched "${accountHint}". Use a more specific --account value.`,
    };
  }
  return {
    kind: "fail",
    reason: `No existing account tile matched "${accountHint}". The tool will not type a username.`,
  };
}

export function decideBrowserAction(
  snapshot: PageSnapshot,
  state: BrowserFlowState,
): BrowserDecision {
  if (!isSafeMicrosoftAuthenticationUrl(snapshot.url)) {
    return {
      kind: "fail",
      reason:
        "The browser left Microsoft authentication hosts. Refusing further automation.",
    };
  }

  if (snapshot.passwordInputVisible) {
    return {
      kind: "fail",
      reason:
        "A password page was detected. Password entry is outside this tool's security boundary.",
    };
  }

  if (snapshot.usernameInputVisible) {
    return {
      kind: "fail",
      reason:
        "A username-entry page was detected. The tool selects only an existing account tile and will not type a username.",
    };
  }

  if (MFA_TEXT_PATTERN.test(snapshot.bodyText)) {
    return {
      kind: "fail",
      reason:
        "An MFA or identity-verification challenge was detected. Complete it manually outside this tool.",
    };
  }

  if (SUCCESS_TEXT_PATTERN.test(snapshot.bodyText)) {
    return { kind: "complete" };
  }

  if (!state.codeEntered && snapshot.deviceCodeInputVisible) {
    return { kind: "fill-device-code" };
  }

  if (!state.codeEntered) {
    return { kind: "wait" };
  }

  if (state.codeSubmitted && snapshot.oneTimeCodeInputVisible) {
    return {
      kind: "fail",
      reason:
        "A second one-time-code field was detected after device-code submission. The tool will not automate MFA.",
    };
  }

  if (state.codeSubmitted && snapshot.deviceCodeInputVisible) {
    return { kind: "wait" };
  }

  if (snapshot.accountSelectionRequired) {
    if (state.accountSelected) {
      return { kind: "wait" };
    }
    if (!state.accountHint) {
      return {
        kind: "fail",
        reason:
          "Account selection is required. Re-run with --account matching an existing account tile.",
      };
    }
    return chooseAccount(snapshot.accountCandidates, state.accountHint);
  }

  const control = CONTROL_PRIORITY.find((candidate) =>
    snapshot.controls.includes(candidate),
  );
  if (control) {
    return { kind: "click-control", control };
  }

  return { kind: "wait" };
}
