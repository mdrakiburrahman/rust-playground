export interface DeviceCodeDetails {
  readonly verificationUrl: string;
  readonly userCode: string;
}

const ANSI_ESCAPE_PATTERN =
  /[\u001B\u009B][[\]()#;?]*(?:(?:[A-Za-z\d]*(?:;[-A-Za-z\d/#&.:=?%@~_]+)*)?\u0007|(?:(?:\d{1,4}(?:[;:]\d{0,4})*)?[\dA-PR-TZcf-nq-uy=><~]))/g;

const URL_PATTERN =
  /https:\/\/(?:www\.)?(?:microsoft\.com\/(?:devicelogin|link)|aka\.ms\/devicelogin|login\.microsoft\.com\/device|login\.microsoftonline\.com\/[^\s"'<>]+)(?=[\s"'<>),.;]|$)/i;

const CODE_PATTERNS = [
  /(?:enter|use)\s+(?:the\s+)?(?:user\s+|device\s+)?code\s*[:=]?\s*["']?([A-Z0-9][A-Z0-9-]{5,20})/i,
  /(?:user|device)\s+code\s*[:=]\s*["']?([A-Z0-9][A-Z0-9-]{5,20})/i,
  /code\s+["']?([A-Z0-9][A-Z0-9-]{5,20})["']?\s+(?:at|on|to authenticate)/i,
];

function normalizeCode(code: string): string {
  return code.replace(/[^A-Z0-9-]/gi, "").toUpperCase();
}

export class DeviceCodeParser {
  private buffer = "";
  private details: DeviceCodeDetails | undefined;

  push(chunk: string): DeviceCodeDetails | undefined {
    if (this.details) {
      return this.details;
    }

    this.buffer = `${this.buffer}${chunk.replace(ANSI_ESCAPE_PATTERN, "")}`.slice(
      -32_768,
    );
    const url = this.buffer.match(URL_PATTERN)?.[0];
    const code = CODE_PATTERNS.map(
      (pattern) => this.buffer.match(pattern)?.[1],
    ).find(Boolean);

    if (!url || !code) {
      return undefined;
    }

    this.details = {
      verificationUrl: url.replace(/[),.;]+$/, ""),
      userCode: normalizeCode(code),
    };
    return this.details;
  }
}
