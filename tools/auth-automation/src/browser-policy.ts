import type {
  PageSnapshot,
  SafeControl,
} from "./contracts.js";
import {
  isExpectedLocalhostRedirect,
  isSafeMicrosoftAuthenticationUrl,
} from "./security.js";

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
  | {
      readonly kind: "fail";
      readonly reason: string;
      readonly category?:
        | "unsafe-url"
        | "password"
        | "username"
        | "mfa"
        | "conditional-access"
        | "account";
    };

export interface BrowserRedirectFlowState {
  readonly accountHint: string;
  readonly accountSelected: boolean;
  readonly expectedRedirectUri: string;
  readonly expectedState: string;
}

const MFA_TEXT_PATTERN =
  /approve (?:the )?sign-in request|authenticator app|two-step verification|multi-factor authentication|verify your identity|confirm your identity|enter (?:the )?verification code|text (?:me|a code)|send (?:me |a )?code|call (?:me|my phone)|security key|passkey|windows hello|more information required|keep your account secure|help us protect your account|additional security verification|choose a verification method/i;
const SUCCESS_TEXT_PATTERN =
  /you (?:have|'ve) signed in|you(?:'re| are) signed in|authentication complete|you may now close (?:this )?(?:window|browser)|device (?:has been )?authenticated/i;
const CONDITIONAL_ACCESS_TEXT_PATTERN =
  /conditional access|aadsts53003|you cannot access this right now|you can't get there from here|access has been blocked|sign-in was successful but.*(?:criteria|policy)|does not meet.*(?:criteria|policy)/i;
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
      category: "account",
    };
  }
  return {
    kind: "fail",
    reason: `No existing account tile matched "${accountHint}". The tool will not type a username.`,
    category: "account",
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

export function decideBrowserRedirectAction(
  snapshot: PageSnapshot,
  state: BrowserRedirectFlowState,
): BrowserDecision {
  if (
    isExpectedLocalhostRedirect(
      snapshot.url,
      state.expectedRedirectUri,
      state.expectedState,
    )
  ) {
    return { kind: "complete" };
  }

  if (!isSafeMicrosoftAuthenticationUrl(snapshot.url)) {
    return {
      kind: "fail",
      reason:
        "The browser left Microsoft authentication hosts before the expected localhost callback.",
      category: "unsafe-url",
    };
  }

  if (snapshot.passwordInputVisible) {
    return {
      kind: "fail",
      reason:
        "A password page was detected. Password entry is outside this tool's security boundary.",
      category: "password",
    };
  }
  if (snapshot.usernameInputVisible) {
    return {
      kind: "fail",
      reason:
        "A username-entry page was detected. Browser login selects only an existing account tile.",
      category: "username",
    };
  }
  if (MFA_TEXT_PATTERN.test(snapshot.bodyText)) {
    return {
      kind: "fail",
      reason:
        "An MFA or identity-verification challenge was detected. The browser login flow will not automate it.",
      category: "mfa",
    };
  }
  if (CONDITIONAL_ACCESS_TEXT_PATTERN.test(snapshot.bodyText)) {
    return {
      kind: "fail",
      reason: "Conditional Access rejected the browser login flow.",
      category: "conditional-access",
    };
  }

  const hasRequestedAccountCandidate = snapshot.accountCandidates.some(
    (candidate) =>
      normalize(candidate).includes(normalize(state.accountHint)),
  );
  if (
    snapshot.accountSelectionRequired ||
    hasRequestedAccountCandidate
  ) {
    if (state.accountSelected) {
      return { kind: "wait" };
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
