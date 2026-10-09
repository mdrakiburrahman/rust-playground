# Azure auth automation

`auth-automation` is a TypeScript library and CLI that starts Azure CLI
device-code login, reads the verification URL and short-lived user code from
the running process, and uses Playwright for only the safe browser steps.

The current default tenant is
`72f988bf-86f1-41af-91ab-2d7cd011db47`. Override it with
`--tenant <tenant-guid-or-domain>`.

## Windows: current Azure CLI

From Windows PowerShell, `--target current` selects the Windows process host
and uses installed Microsoft Edge. `--target windows` is an explicit alias for
the same native-Windows Azure CLI and is accepted only when the tool itself is
running on Windows:

```powershell
npx nx run auth-automation:status -- --target current
npx nx run auth-automation:login -- --target current
npx nx run auth-automation:login -- --target current --account user@example.com
npx nx run auth-automation:status -- --target windows
```

## Windows: Azure CLI in WSL

From Windows PowerShell, target Azure CLI inside a named WSL distribution while
the browser continues to use installed Windows Microsoft Edge:

```powershell
npx nx run auth-automation:status -- --target wsl --wsl-distro Ubuntu-24.04
npx nx run auth-automation:login -- --target wsl --wsl-distro Ubuntu-24.04
npx nx run auth-automation:login -- --target wsl --wsl-distro Ubuntu-24.04 --account user@example.com
```

`--target wsl` is valid only on Windows and dispatches through `wsl.exe`.
An internal shell wrapper reports the numeric Linux Azure CLI PID before
replacing itself with `az`, so abort handling can terminate and confirm that
exact Linux process tree.

## WSL-native Azure CLI and browser

Inside WSL/Linux, install the pinned Playwright Chromium build, use WSLg (or
another graphical display), and target the current host:

```bash
npx nx run auth-automation:browser-install
npx nx run auth-automation:status -- --target current
npx nx run auth-automation:login -- --target current
npx nx run auth-automation:login -- --target current --account user@example.com
```

Inside WSL/Linux, only `--target current` is supported. `--target windows` is
rejected before any process starts because a POSIX-hosted tool cannot securely
terminate and confirm the Windows `cmd.exe`/Azure CLI process tree.

## Status and cleanup

`status` runs `az account show` against the selected target and requires the
reported tenant to match `--tenant`. It does not start a browser.

The normal Edge or Chromium profile is never used. The tool creates and closes
only its own persistent context:

- Windows Edge:
  `%LOCALAPPDATA%\rust-playground\auth-automation\msedge-profile`
- WSL/Linux Chromium:
  `${XDG_CACHE_HOME:-$HOME/.cache}/rust-playground/auth-automation/chromium-profile`

The profile can retain cookies and known account tiles between runs. It is
outside the repository and must not be copied into source control. Close the
auth-automation browser window before cleanup.

Windows cleanup:

```powershell
Remove-Item -Recurse -Force "$env:LOCALAPPDATA\rust-playground\auth-automation\msedge-profile"
```

WSL/Linux cleanup:

```bash
rm -rf "${XDG_CACHE_HOME:-$HOME/.cache}/rust-playground/auth-automation/chromium-profile"
```

Verify profile cleanup directly:

```powershell
Test-Path "$env:LOCALAPPDATA\rust-playground\auth-automation\msedge-profile"
```

```bash
test ! -d "${XDG_CACHE_HOME:-$HOME/.cache}/rust-playground/auth-automation/chromium-profile"
```

Profile cleanup does not sign Azure CLI out. Run the matching `status` command
to check CLI authentication, or use `az logout` against the selected CLI target
when sign-out is intended.

## Security boundary

The browser automation may enter only the short-lived device code, select an
existing account tile named by `--account`, and activate accessible controls
named `Next`, `Continue`, `Accept`, or `Consent`.

It never types or stores a username, password, MFA code, authenticator
response, recovery code, or other identity challenge. Password and
MFA/identity challenge pages fail immediately with a clear diagnostic. There
is no option that enables password or MFA automation.

Azure CLI output is parsed as it streams; the tool does not wait for `az login`
to exit before opening the browser. Raw login output is not echoed, device
codes and common token/password fields are redacted from diagnostics, and
`az account show` must confirm the requested tenant after login completes.
Browser actions and the Azure CLI process have bounded timeouts.

Timeout and safety aborts wait for confirmed launched-process exit. On Windows,
the tool enumerates descendants of the launched wrapper PID and invokes
`Stop-Process -Id` on those numeric PIDs only; it never terminates processes by
name. For a WSL target, it first terminates the reported Linux Azure CLI PID
tree and then confirms the Windows `wsl.exe` wrapper tree has exited.

## Development targets

```bash
npx nx run auth-automation:typecheck
npx nx run auth-automation:test
npx nx run auth-automation:verify
```

Automated tests use injected process, browser, filesystem, and timer adapters;
they do not launch Azure CLI or a real browser. A live login smoke test remains
an explicit manual operation.
