export const BROWSER_CAPTURE_HELPER_FILE = "capture-browser.sh";
export const BROWSER_CAPTURE_URL_FILE = "authorize-url.txt";

export const BROWSER_CAPTURE_HELPER = `#!/bin/sh
set -eu
umask 077
if [ "$#" -ne 1 ]; then
  exit 64
fi
: "\${AUTH_AUTOMATION_URL_FILE:?}"
case "$1" in
  https://*) ;;
  *) exit 65 ;;
esac
capture_tmp="\${AUTH_AUTOMATION_URL_FILE}.tmp.$$"
trap 'rm -f "$capture_tmp"' EXIT HUP INT TERM
printf '%s\\n' "$1" > "$capture_tmp"
mv -f "$capture_tmp" "$AUTH_AUTOMATION_URL_FILE"
trap - EXIT HUP INT TERM
`;

export function parseCapturedUrlFile(content: string): string {
  if (content.length > 32_768) {
    throw new Error("Captured browser authorization data is too large.");
  }
  const normalized = content.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n").filter((line) => line.length > 0);
  if (lines.length !== 1 || lines[0] !== lines[0].trim()) {
    throw new Error(
      "Captured browser authorization data must contain exactly one URL.",
    );
  }
  return lines[0];
}
