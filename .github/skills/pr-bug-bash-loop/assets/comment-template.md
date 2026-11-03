# Bug bash - fresh devcontainer

<!-- bug-bash:runbook:start -->
Run these blocks **in order** on `<branch>` in a freshly provisioned devcontainer.
<Only the prerequisites actually required by these commands.>

## 1. Setup

```bash
<working-directory and setup commands>
```

## 2. Run

```bash
<documented public entrypoint>
```

**Expect:** <short, observable success conditions and exit code>.

**Report:** `<output location>`.

## 3. Cleanup

<State automatic cleanup, or give the exact cleanup command. Put any destructive
warning immediately beside its command.>
<!-- bug-bash:runbook:end -->

**Verification:** <not yet verified / one clean pass / two consecutive clean passes>
on `<full SHA>`. Runbook: `<SHA-256>`. Fresh bootstraps: `<IDs>`.
<One short coverage caveat only if needed.>
