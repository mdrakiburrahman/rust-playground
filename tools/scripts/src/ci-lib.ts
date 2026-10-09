import {
  appendFileSync,
  mkdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import path from 'node:path';

import {
  CommandError,
  type CommandRunner,
  runChecked,
} from './process.js';

export const CI_LABEL_NAME = 'rust-playground.ci-run';

const canonicalContainerIdPattern = /^[a-f0-9]{64}$/u;
const composeProjectFormat =
  '{{ index .Config.Labels "com.docker.compose.project" }}';
const composeProjectPattern = /^[a-z0-9][a-z0-9_-]*$/u;
const containerIdFormat = '{{ .Id }}';
const containerIdPattern = /^[a-f0-9]{12,64}$/u;
const containerWorkspace = '/workspaces/rust-playground';
const ciLabelFormat = `{{ index .Config.Labels "${CI_LABEL_NAME}" }}`;
const ciLabelValuePattern = /^\d+-\d+-(?:source|published)$/u;

export type DevcontainerKind = 'published' | 'source';

export interface CiFileSystem {
  appendFile(filePath: string, contents: string): void;
  ensurePrivateDirectory(directoryPath: string): void;
  removeFile(filePath: string): void;
  writePrivateFile(filePath: string, contents: string): void;
}

export const systemFileSystem: CiFileSystem = {
  appendFile(filePath, contents) {
    appendFileSync(filePath, contents, 'utf8');
  },
  ensurePrivateDirectory(directoryPath) {
    mkdirSync(directoryPath, { mode: 0o700, recursive: true });
  },
  removeFile(filePath) {
    rmSync(filePath, { force: true });
  },
  writePrivateFile(filePath, contents) {
    writeFileSync(filePath, contents, { encoding: 'utf8', mode: 0o600 });
  },
};

export interface PrepareOptions {
  readonly fileSystem: CiFileSystem;
  readonly home: string;
  readonly workspace: string;
}

export function prepareCiEnvironment(options: PrepareOptions): void {
  const azureDirectory = path.join(options.home, '.azure');
  const githubDirectory = path.join(options.home, '.config', 'gh');

  options.fileSystem.ensurePrivateDirectory(azureDirectory);
  options.fileSystem.ensurePrivateDirectory(githubDirectory);
  options.fileSystem.writePrivateFile(
    ciEnvironmentFile(options.workspace),
    [
      `HOST_AZURE_DIR=${composeEnvironmentValue(azureDirectory)}`,
      `HOST_GH_CONFIG_DIR=${composeEnvironmentValue(githubDirectory)}`,
      '',
    ].join('\n'),
  );
}

export interface StartOptions {
  readonly githubOutput: string;
  readonly kind: DevcontainerKind;
  readonly runAttempt: string;
  readonly runId: string;
  readonly workspace: string;
}

export function startDevcontainer(
  options: StartOptions,
  runner: CommandRunner,
  fileSystem: CiFileSystem,
  writeOutput: (output: string) => void = (output) =>
    process.stdout.write(output),
): void {
  const ciLabel = createCiLabel(
    options.runId,
    options.runAttempt,
    options.kind,
  );
  appendStepOutputs(fileSystem, options.githubOutput, {
    ci_label: ciLabel,
    start_kind: options.kind,
  });

  const upArgs = createDevcontainerUpArgs(options, ciLabel);
  const upResult = runner.run('npx', upArgs, {
    cwd: options.workspace,
    stdout: 'capture',
  });
  if (upResult.stdout) {
    writeOutput(formatCapturedOutput(upResult.stdout));
  }

  let recoveryError: unknown;
  try {
    const outputContainerId = extractContainerId(upResult.stdout);
    const discoveredContainerIds =
      outputContainerId === undefined
        ? discoverContainerIds(ciLabel, runner)
        : [];

    if (discoveredContainerIds.length > 1) {
      throw new Error(
        `CI label ${ciLabel} matched multiple containers; refusing to choose one.`,
      );
    }

    const candidate = outputContainerId ?? discoveredContainerIds[0];
    if (candidate !== undefined) {
      const inspected = inspectStartedContainer(candidate, ciLabel, runner);
      appendStepOutputs(fileSystem, options.githubOutput, {
        container_id: inspected.containerId,
        compose_project: inspected.composeProject,
      });
    } else if (upResult.exitCode === 0) {
      throw new Error(
        'devcontainer up succeeded without returning a container ID.',
      );
    }
  } catch (error) {
    recoveryError = error;
  }

  if (upResult.exitCode !== 0) {
    throw new CommandError('npx', upArgs, upResult.exitCode);
  }
  if (recoveryError !== undefined) {
    throw recoveryError;
  }
}

export interface VerifyOptions {
  readonly containerId: string;
}

export function verifyDevcontainer(
  options: VerifyOptions,
  runner: CommandRunner,
): void {
  const containerId = requireCanonicalContainerId(options.containerId);
  runChecked(runner, 'docker', [
    'exec',
    '--user',
    'vscode',
    '--workdir',
    containerWorkspace,
    containerId,
    'bash',
    '-lc',
    'npx nx run-many -t verify --all --parallel=1 && npx nx run tools-scripts:clean-worktree',
  ]);
}

export interface CleanupOptions {
  readonly ciLabels?: readonly string[];
  readonly composeProject?: string;
  readonly containerId?: string;
  readonly workspace: string;
}

export function cleanupDevcontainer(
  options: CleanupOptions,
  runner: CommandRunner,
  fileSystem: CiFileSystem,
): void {
  try {
    const recordedProject = normalizeComposeProject(
      options.composeProject ?? '',
    );
    if (recordedProject !== undefined) {
      removeComposeProjects(
        new Set([recordedProject]),
        options.workspace,
        runner,
      );
      return;
    }

    const expectedLabels = normalizeCiLabels(options.ciLabels ?? []);
    const candidateIds = new Set<string>();
    const recordedContainerId = normalizeOptional(options.containerId);
    if (recordedContainerId !== undefined) {
      candidateIds.add(requireCanonicalContainerId(recordedContainerId));
    }
    for (const label of expectedLabels) {
      for (const containerId of discoverContainerIds(label, runner)) {
        candidateIds.add(containerId);
      }
    }

    const projects = new Set<string>();
    const standaloneContainers = new Set<string>();
    for (const candidateId of candidateIds) {
      const containerId = tryInspectCanonicalContainerId(candidateId, runner);
      if (containerId === undefined) {
        continue;
      }

      if (expectedLabels.length > 0) {
        const actualLabel = inspectCiLabel(containerId, runner);
        if (!expectedLabels.includes(actualLabel)) {
          throw new Error(
            `Container ${containerId} does not have an expected CI run label.`,
          );
        }
      }

      const project = inspectComposeProject(containerId, runner);
      if (project === undefined) {
        standaloneContainers.add(containerId);
      } else {
        projects.add(project);
      }
    }

    removeComposeProjects(projects, options.workspace, runner);
    for (const containerId of standaloneContainers) {
      runChecked(runner, 'docker', ['rm', '--force', containerId]);
    }
  } finally {
    fileSystem.removeFile(ciEnvironmentFile(options.workspace));
  }
}

export function createCiLabel(
  runId: string,
  runAttempt: string,
  kind: DevcontainerKind = 'source',
): string {
  const normalizedRunId = requireRunNumber('GITHUB_RUN_ID', runId);
  const normalizedRunAttempt = requireRunNumber(
    'GITHUB_RUN_ATTEMPT',
    runAttempt,
  );
  return `${CI_LABEL_NAME}=${normalizedRunId}-${normalizedRunAttempt}-${kind}`;
}

export function extractContainerId(output: string): string | undefined {
  try {
    const parsed = JSON.parse(output) as unknown;
    if (
      typeof parsed === 'object' &&
      parsed !== null &&
      'containerId' in parsed &&
      typeof parsed.containerId === 'string'
    ) {
      return normalizeContainerReference(parsed.containerId);
    }
  } catch {
    return undefined;
  }
  return undefined;
}

export function normalizeComposeProject(
  value: string,
): string | undefined {
  const normalized = normalizeOptional(value);
  if (normalized === undefined || normalized === '<no value>') {
    return undefined;
  }
  if (!composeProjectPattern.test(normalized)) {
    throw new Error(`Invalid Docker Compose project name: ${normalized}`);
  }
  return normalized;
}

function createDevcontainerUpArgs(
  options: StartOptions,
  ciLabel: string,
): string[] {
  const configFile =
    options.kind === 'source'
      ? path.join('source', 'devcontainer.json')
      : 'devcontainer.json';
  const args = [
    '--no-install',
    'devcontainer',
    'up',
    '--workspace-folder',
    options.workspace,
    '--config',
    path.join(options.workspace, '.devcontainer', configFile),
  ];
  if (options.kind === 'source') {
    args.push('--frozen-lockfile');
  }
  args.push('--id-label', ciLabel);
  return args;
}

function inspectStartedContainer(
  candidateId: string,
  ciLabel: string,
  runner: CommandRunner,
): { readonly composeProject: string; readonly containerId: string } {
  const containerId = inspectCanonicalContainerId(candidateId, runner);
  const actualLabel = inspectCiLabel(containerId, runner);
  if (actualLabel !== ciLabel) {
    throw new Error(
      `Container ${containerId} has CI label ${actualLabel || '<no value>'}; expected ${ciLabel}.`,
    );
  }

  const composeProject = inspectComposeProject(containerId, runner);
  if (composeProject === undefined) {
    throw new Error(
      `Devcontainer ${containerId} has no valid Compose project label.`,
    );
  }
  return { composeProject, containerId };
}

function discoverContainerIds(
  ciLabel: string,
  runner: CommandRunner,
): string[] {
  normalizeCiLabel(ciLabel);
  const output = runChecked(
    runner,
    'docker',
    ['ps', '-aq', '--no-trunc', '--filter', `label=${ciLabel}`],
    { stderr: 'capture', stdout: 'capture' },
  ).stdout;
  return [
    ...new Set(
      nonEmptyLines(output).map((containerId) =>
        requireCanonicalContainerId(containerId),
      ),
    ),
  ];
}

function inspectCanonicalContainerId(
  containerId: string,
  runner: CommandRunner,
): string {
  const candidate = normalizeContainerReference(containerId);
  if (candidate === undefined) {
    throw new Error(`Invalid Docker container ID: ${containerId}`);
  }
  return requireCanonicalContainerId(
    runChecked(
      runner,
      'docker',
      ['inspect', '--format', containerIdFormat, candidate],
      { stderr: 'capture', stdout: 'capture' },
    ).stdout,
  );
}

function tryInspectCanonicalContainerId(
  containerId: string,
  runner: CommandRunner,
): string | undefined {
  const result = runner.run(
    'docker',
    ['inspect', '--format', containerIdFormat, containerId],
    { stderr: 'ignore', stdout: 'capture' },
  );
  if (result.exitCode !== 0) {
    return undefined;
  }
  return requireCanonicalContainerId(result.stdout);
}

function inspectCiLabel(
  containerId: string,
  runner: CommandRunner,
): string {
  const value = normalizeOptional(
    runChecked(
      runner,
      'docker',
      ['inspect', '--format', ciLabelFormat, containerId],
      { stderr: 'capture', stdout: 'capture' },
    ).stdout,
  );
  return value === undefined || value === '<no value>'
    ? ''
    : normalizeCiLabel(`${CI_LABEL_NAME}=${value}`);
}

function inspectComposeProject(
  containerId: string,
  runner: CommandRunner,
): string | undefined {
  return normalizeComposeProject(
    runChecked(
      runner,
      'docker',
      ['inspect', '--format', composeProjectFormat, containerId],
      { stderr: 'capture', stdout: 'capture' },
    ).stdout,
  );
}

function removeComposeProjects(
  projects: ReadonlySet<string>,
  workspace: string,
  runner: CommandRunner,
): void {
  const devcontainerDirectory = path.join(workspace, '.devcontainer');
  const composeFile = path.join(devcontainerDirectory, 'docker-compose.yml');
  for (const project of projects) {
    runChecked(runner, 'docker', [
      'compose',
      '--project-name',
      project,
      '--project-directory',
      devcontainerDirectory,
      '--file',
      composeFile,
      'down',
      '--volumes',
      '--remove-orphans',
    ]);
  }
}

function normalizeCiLabels(labels: readonly string[]): string[] {
  return [...new Set(labels.map((label) => normalizeCiLabel(label)))];
}

function normalizeCiLabel(label: string): string {
  const normalized = label.trim();
  const prefix = `${CI_LABEL_NAME}=`;
  const value = normalized.startsWith(prefix)
    ? normalized.slice(prefix.length)
    : '';
  if (!ciLabelValuePattern.test(value)) {
    throw new Error(`Invalid rust-playground CI run label: ${label}`);
  }
  return `${prefix}${value}`;
}

function requireCanonicalContainerId(value: string): string {
  const normalized = value.trim();
  if (!canonicalContainerIdPattern.test(normalized)) {
    throw new Error(`Invalid canonical Docker container ID: ${value}`);
  }
  return normalized;
}

function normalizeContainerReference(
  value: string,
): string | undefined {
  const normalized = normalizeOptional(value);
  return normalized !== undefined && containerIdPattern.test(normalized)
    ? normalized
    : undefined;
}

function requireRunNumber(name: string, value: string): string {
  const normalized = value.trim();
  if (!/^\d+$/u.test(normalized)) {
    throw new Error(`${name} must contain only decimal digits.`);
  }
  return normalized;
}

function appendStepOutputs(
  fileSystem: CiFileSystem,
  githubOutput: string,
  outputs: Readonly<Record<string, string>>,
): void {
  const contents = Object.entries(outputs)
    .map(([name, value]) => {
      if (/[\r\n]/u.test(value)) {
        throw new Error(`GitHub step output ${name} contains a newline.`);
      }
      return `${name}=${value}\n`;
    })
    .join('');
  fileSystem.appendFile(githubOutput, contents);
}

function ciEnvironmentFile(workspace: string): string {
  return path.join(workspace, '.devcontainer', '.env');
}

function composeEnvironmentValue(value: string): string {
  if (/[\0\r\n]/u.test(value)) {
    throw new Error('Host credential directory contains an unsupported character.');
  }

  const normalized = value.replaceAll('\\', '/');
  if (/^[A-Za-z0-9_./:-]+$/u.test(normalized)) {
    return normalized;
  }
  return `"${normalized
    .replaceAll('\\', '\\\\')
    .replaceAll('"', '\\"')
    .replaceAll('$', '$$')}"`;
}

function nonEmptyLines(value: string): string[] {
  return value
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean);
}

function normalizeOptional(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
}

function formatCapturedOutput(output: string): string {
  return `${output.replace(/[\r\n]+$/u, '')}\n`;
}
