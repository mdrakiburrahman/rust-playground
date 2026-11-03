---
name: pr-bug-bash-loop
description: Write brief, executable PR bug-bash comments and prove them with repeated fresh-devcontainer rehearsals. Use when asked to verify PR setup instructions, run a clean-room bug bash, or rebuild and retry until the published steps work.
compatibility: Requires GitHub access and the repository's supported devcontainer provisioning tools. For repo-bootstrapper and privy devboxes, load the remote-devbox skill.
---

# PR bug-bash loop

Produce a runnable PR comment, not a long testing narrative. Finish only after
two consecutive fresh-environment passes on the same commit and instructions.
Use a different pass count only when the user explicitly requests it.

## Safety and scope

- Resolve the intended repository, PR, branch, environment and experiment before
  changing anything. Read repository guidance and the documented setup commands.
- Establish permission for the reset boundary. Never infer permission to erase a
  shared machine, unrelated work, cloud resources or the local controller checkout.
- For repo-bootstrapper/privy, load `remote-devbox` and follow its register, lease,
  bootstrap, work and release contract. Reuse an owned lease; never bypass a lost
  lease or use another session's box. Keep one repo per lease.
- Preserve pushed work and copy evidence outside the reset boundary before every
  reset. Stop if unexpected unpushed work cannot safely be preserved.
- Do not print secrets, source private environment files, publish raw private logs
  or put credentials in command history. Do not merge the PR.

## Workflow

### 1. Identify the candidate

Record the live PR head SHA, branch and supported fresh-environment procedure.
Check the PR's live CI results for that exact SHA. Pending or failed checks are
not green; explain intentionally skipped or inapplicable checks rather than
silently counting them as passing.

Keep a small YAML receipt in session artifacts outside the devbox. Track the
repository, PR URL, head SHA, comment ID, executable-runbook hash, pass count,
bootstrap IDs, container identities, commands, exit codes and evidence paths.
Do not hardcode session IDs or environment names in this skill.

### 2. Start fresh and run the complete setup

On a leased disposable repo-bootstrapper devbox, use `Full` on the intended
branch, not `Up` or a restart. Else use the repository's authorized equivalent
that clears previous experiment state and build outputs. Record reset evidence,
verify bootstrap success, and confirm the checkout matches the green PR head.

Run the documented user entrypoint, including dependency installation and builds.
Do not bypass it with an internal script, manually carried-over binary or image,
prestarted stack, or undocumented setup command. Standard provisioning and
documented image downloads are allowed.

Capture command output and actual exit codes without masking pipeline failures.
Check the requested behavior, data assertions, report and cleanup, not just
process exit. Record what the user should see after each command.

### 3. Publish the first clean pass

After the complete candidate instructions work without intervention, create one
top-level PR comment using [the template](assets/comment-template.md). Keep it
brief: prerequisites, ordered copy-paste blocks, expected results, report path
and cleanup. Include destructive warnings beside the affected command.

Capture the comment ID. Hash the exact text between the runbook markers,
including prerequisites and expected results. The receipt outside those markers
may change without invalidating the executable instructions.

Post only measured facts. Mark one clean pass as one pass, not final verification.

### 4. Rebuild and follow the posted comment

Preserve evidence, perform another full reset, and verify the same head SHA.
Fetch the live comment and execute its instructions verbatim, in order.
Do not skip or replace commands, add hidden prerequisites or fix the environment
mid-run and still call the result a clean pass.

Repeat the behavioral assertions and cleanup. Record a distinct bootstrap and
container identity. Recheck the PR head and runbook hash before counting the pass.
Continue until the requested number of consecutive clean passes is reached.

### 5. On any failure, repair and restart the count

Preserve diagnostics, identify the root cause, and fix code and/or instructions.
Keep the repository's setup documentation consistent with the final comment.
Use relevant existing checks and focused regression coverage for actual bugs.
Never suppress errors or weaken assertions merely to pass.

Commit and push authorized repository fixes. Edit the same PR comment rather
than leaving competing runbooks. Mark changed instructions unverified.
Reset the pass count to zero after any failure, code change, executable-runbook
change or undocumented intervention. Require green CI on the current head,
reset again, and restart from the first clean pass.

A diagnostic retry in a repaired environment is not a qualifying fresh pass.
Persistent external outages, authentication or permission failures, and lost
leases are blockers: preserve evidence and report them rather than retrying
indefinitely, changing scope or claiming success.

### 6. Close the evidence loop

Confirm the live PR head and runbook hash still match both successful receipts,
and CI is green. Edit the same comment with a short verification footer: tested
SHA, runbook hash, fresh-bootstrap IDs and outcome. Keep detailed logs private.
Verify cleanup and pushed work, then release the lease.

Report the comment link and measured coverage. Two clean passes demonstrate
reproducibility in the tested environment, not the absence of every possible bug.
