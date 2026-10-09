import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import * as path from 'node:path';
import type { ExecutorContext } from '@nx/devkit';
import type {
  DockerBuildExecutorSchema,
  DockerBuildOutputMode,
} from './schema.js';

const dockerTagPattern = /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/u;
const dockerTagMaximumLength = 128;
const gitBranchTagPrefix = 'branch-';
const gitBranchHashLength = 16;
const gitShaPattern = /^[0-9a-f]{7,64}$/iu;
const platformPattern =
  /^[a-z0-9][a-z0-9._-]*\/[a-z0-9][a-z0-9._-]*(?:\/[a-z0-9][a-z0-9._-]*)?$/u;
const remoteManifestMissingPatterns = [
  /\bmanifest unknown\b/iu,
  /\bno such manifest\b/iu,
  /\b(?:GET|HEAD) request to [^\r\n]*\/manifests\/[^\r\n]*:\s*404 Not Found\b/iu,
] as const;
const remoteManifestFailurePattern =
  /\b(?:401|403|408|429|5\d\d)\b|\b(?:authentication required|bad gateway|connection refused|connection reset|denied|failed to authorize|forbidden|gateway timeout|i\/o timeout|internal server error|service unavailable|temporarily unavailable|timed out|timeout|tls handshake|too many requests|unauthorized)\b/iu;

const gitShaEnvironmentVariables = [
  'REGISTRY_SHA',
  'GITHUB_SHA',
  'BUILD_SOURCEVERSION',
  'CI_COMMIT_SHA',
  'GIT_COMMIT',
] as const;

const gitBranchEnvironmentVariables = [
  'REGISTRY_BRANCH',
  'GITHUB_HEAD_REF',
  'GITHUB_REF_NAME',
  'BUILD_SOURCEBRANCH',
  'BUILD_SOURCEBRANCHNAME',
  'CI_COMMIT_REF_NAME',
  'CI_COMMIT_BRANCH',
  'BRANCH_NAME',
  'GIT_BRANCH',
] as const;

export interface CommandOptions {
  readonly captureOutput?: boolean;
  readonly cwd?: string;
}

export interface CommandResult {
  readonly error?: Error;
  readonly signal?: NodeJS.Signals | null;
  readonly status: number | null;
  readonly stderr?: string;
  readonly stdout?: string;
}

export interface CommandRunner {
  run(
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ): CommandResult;
}

export interface ExecutorLogger {
  error(message: string): void;
  info(message: string): void;
}

export interface ManifestWriter {
  write(filePath: string, contents: string): void;
}

export interface DockerBuildExecutorDependencies {
  readonly commandRunner: CommandRunner;
  readonly environment: NodeJS.ProcessEnv;
  readonly logger: ExecutorLogger;
  readonly manifestWriter: ManifestWriter;
}

export interface DockerBuildPlan {
  readonly args: readonly string[];
  readonly imageReferences: readonly string[];
  readonly immutableImageReferences: readonly string[];
  readonly manifestFile?: string;
  readonly requireCleanWorktree: boolean;
}

export class SpawnCommandRunner implements CommandRunner {
  run(
    command: string,
    args: readonly string[],
    options: CommandOptions = {},
  ): CommandResult {
    const result = spawnSync(command, [...args], {
      cwd: options.cwd,
      encoding: 'utf8',
      shell: false,
      stdio: options.captureOutput
        ? ['ignore', 'pipe', 'pipe']
        : ['ignore', 'inherit', 'inherit'],
      windowsHide: true,
    });

    return {
      error: result.error,
      signal: result.signal,
      status: result.status,
      stderr: result.stderr ?? undefined,
      stdout: result.stdout ?? undefined,
    };
  }
}

const consoleLogger: ExecutorLogger = {
  error: (message) => console.error(message),
  info: (message) => console.log(message),
};

const fileManifestWriter: ManifestWriter = {
  write(filePath, contents) {
    mkdirSync(path.dirname(filePath), { recursive: true });
    writeFileSync(filePath, contents, 'utf8');
  },
};

export default async function runDockerBuildExecutor(
  options: DockerBuildExecutorSchema,
  context: ExecutorContext,
): Promise<{ success: boolean }> {
  return runDockerBuildExecutorWithDependencies(options, context);
}

export async function runDockerBuildExecutorWithDependencies(
  options: DockerBuildExecutorSchema,
  context: ExecutorContext,
  dependencyOverrides: Partial<DockerBuildExecutorDependencies> = {},
): Promise<{ success: boolean }> {
  const dependencies: DockerBuildExecutorDependencies = {
    commandRunner: dependencyOverrides.commandRunner ?? new SpawnCommandRunner(),
    environment: dependencyOverrides.environment ?? process.env,
    logger: dependencyOverrides.logger ?? consoleLogger,
    manifestWriter: dependencyOverrides.manifestWriter ?? fileManifestWriter,
  };

  try {
    const plan = createDockerBuildPlan(options, context, dependencies);
    const workspaceRoot = path.resolve(context.root);

    if (plan.requireCleanWorktree) {
      requireCleanGitWorktree(dependencies.commandRunner, workspaceRoot);
    }

    const buildImageReferences = selectBuildImageReferences(
      plan,
      dependencies,
      workspaceRoot,
      context.isVerbose,
    );

    if (buildImageReferences.length > 0) {
      const buildArguments = replaceDockerBuildImageReferences(
        plan,
        buildImageReferences,
      );

      if (context.isVerbose) {
        dependencies.logger.info(
          `Running ${formatCommand(
            'docker',
            redactBuildArguments(buildArguments),
          )}`,
        );
      }

      const result = dependencies.commandRunner.run(
        'docker',
        buildArguments,
        {
          cwd: workspaceRoot,
        },
      );
      const failure = commandFailure('docker buildx build', result);
      if (failure) {
        dependencies.logger.error(failure);
        return { success: false };
      }
    } else {
      dependencies.logger.info(
        'All selected image tags already exist as immutable remote manifests; skipping docker buildx build.',
      );
    }

    if (plan.manifestFile) {
      dependencies.manifestWriter.write(
        plan.manifestFile,
        `${plan.imageReferences.join('\n')}\n`,
      );
    }

    return { success: true };
  } catch (error: unknown) {
    dependencies.logger.error(errorMessage(error));
    return { success: false };
  }
}

export function createDockerBuildPlan(
  options: DockerBuildExecutorSchema,
  context: ExecutorContext,
  dependencies: Pick<
    DockerBuildExecutorDependencies,
    'commandRunner' | 'environment'
  >,
): DockerBuildPlan {
  const output = validateOutputMode(options.output);
  const rawTags = validateStringArray(options.tags, 'tags', true);
  const rawBuildArgs =
    options.buildArgs === undefined
      ? []
      : validateStringArray(options.buildArgs, 'buildArgs', false);
  const rawPlatforms =
    options.platforms === undefined
      ? []
      : validateStringArray(options.platforms, 'platforms', true);
  const rawImmutableTags =
    options.immutableTags === undefined
      ? []
      : validateStringArray(options.immutableTags, 'immutableTags', true);
  const requireCleanWorktree =
    options.requireCleanWorktree === undefined
      ? false
      : validateBoolean(
          options.requireCleanWorktree,
          'requireCleanWorktree',
        );
  const expandTokens = createTokenExpander(context, dependencies);

  const file = path.normalize(
    validateNonBlank(
      expandTokens(validateString(options.file, 'file'), 'file'),
      'file',
    ),
  );
  const buildContext =
    options.context === undefined
      ? path.dirname(file)
      : normalizeBuildContext(
          expandTokens(validateString(options.context, 'context'), 'context'),
        );
  const image = validateImage(
    expandTokens(validateString(options.image, 'image'), 'image'),
  );
  const tags = rawTags.map((tag, index) =>
    validateDockerTag(expandTokens(tag, `tags[${index}]`)),
  );
  rejectDuplicates(tags, 'tags after token expansion');
  const immutableTags = rawImmutableTags.map((tag, index) =>
    validateDockerTag(
      expandTokens(tag, `immutableTags[${index}]`),
    ),
  );
  rejectDuplicates(
    immutableTags,
    'immutableTags after token expansion',
  );

  if (immutableTags.length > 0 && output !== 'push') {
    throw new Error(
      'immutableTags can only be used with output "push".',
    );
  }
  const selectedTags = new Set(tags);
  for (const immutableTag of immutableTags) {
    if (!selectedTags.has(immutableTag)) {
      throw new Error(
        `immutableTags contains "${immutableTag}", which is not present in tags after token expansion.`,
      );
    }
  }

  const buildArgs = rawBuildArgs.map((argument, index) =>
    validateNonBlank(
      expandTokens(argument, `buildArgs[${index}]`),
      `buildArgs[${index}]`,
    ),
  );
  const platforms = rawPlatforms.map((platform, index) =>
    validatePlatform(
      expandTokens(platform, `platforms[${index}]`),
      `platforms[${index}]`,
    ),
  );
  rejectDuplicates(platforms, 'platforms after token expansion');

  if (output === 'load' && platforms.length > 1) {
    throw new Error(
      'Docker load output supports at most one platform; use output "push" for multi-platform builds.',
    );
  }

  const imageReferences = tags.map((tag) => `${image}:${tag}`);
  const immutableImageReferences = immutableTags.map(
    (tag) => `${image}:${tag}`,
  );
  const args: string[] = ['buildx', 'build', '--file', file];

  for (const imageReference of imageReferences) {
    args.push('--tag', imageReference);
  }
  for (const buildArgument of buildArgs) {
    args.push('--build-arg', buildArgument);
  }
  if (platforms.length > 0) {
    args.push('--platform', platforms.join(','));
  }

  args.push(output === 'load' ? '--load' : '--push', buildContext);

  const manifestFile =
    options.manifestFile === undefined
      ? undefined
      : resolveManifestFile(
          expandTokens(
            validateString(options.manifestFile, 'manifestFile'),
            'manifestFile',
          ),
          context.root,
        );

  return {
    args,
    imageReferences,
    immutableImageReferences,
    requireCleanWorktree,
    ...(manifestFile ? { manifestFile } : {}),
  };
}

function requireCleanGitWorktree(
  commandRunner: CommandRunner,
  workspaceRoot: string,
): void {
  const args = [
    'status',
    '--porcelain=v1',
    '--untracked-files=all',
  ] as const;
  const result = commandRunner.run('git', args, {
    captureOutput: true,
    cwd: workspaceRoot,
  });
  const failure = commandFailure(`git ${args.join(' ')}`, result);
  if (failure) {
    throw new Error(`Unable to verify a clean Git worktree: ${failure}`);
  }

  const changes = result.stdout?.trim();
  if (changes) {
    throw new Error(
      `Git worktree must be clean before publication; tracked or untracked changes were found:\n${changes}`,
    );
  }
}

function selectBuildImageReferences(
  plan: DockerBuildPlan,
  dependencies: Pick<
    DockerBuildExecutorDependencies,
    'commandRunner' | 'logger'
  >,
  workspaceRoot: string,
  isVerbose: boolean,
): string[] {
  const immutableImageReferences = new Set(
    plan.immutableImageReferences,
  );
  const selectedImageReferences: string[] = [];

  for (const imageReference of plan.imageReferences) {
    if (!immutableImageReferences.has(imageReference)) {
      selectedImageReferences.push(imageReference);
      continue;
    }

    if (
      remoteManifestExists(
        imageReference,
        dependencies,
        workspaceRoot,
        isVerbose,
      )
    ) {
      dependencies.logger.info(
        `Immutable image tag already exists; omitting it from this build: ${imageReference}`,
      );
      continue;
    }

    selectedImageReferences.push(imageReference);
  }

  return selectedImageReferences;
}

function remoteManifestExists(
  imageReference: string,
  dependencies: Pick<
    DockerBuildExecutorDependencies,
    'commandRunner' | 'logger'
  >,
  workspaceRoot: string,
  isVerbose: boolean,
): boolean {
  const args = [
    'buildx',
    'imagetools',
    'inspect',
    imageReference,
  ] as const;

  if (isVerbose) {
    dependencies.logger.info(
      `Running ${formatCommand('docker', args)}`,
    );
  }

  const result = dependencies.commandRunner.run('docker', args, {
    captureOutput: true,
    cwd: workspaceRoot,
  });
  if (
    result.error === undefined &&
    result.signal == null &&
    result.status === 0
  ) {
    return true;
  }
  if (isMissingRemoteManifest(result, imageReference)) {
    return false;
  }

  const failure =
    commandFailure('docker buildx imagetools inspect', result) ??
    'docker buildx imagetools inspect failed.';
  throw new Error(
    `Unable to inspect immutable image tag "${imageReference}": ${failure}`,
  );
}

function isMissingRemoteManifest(
  result: CommandResult,
  imageReference: string,
): boolean {
  if (
    result.error !== undefined ||
    result.signal != null ||
    result.status === null ||
    result.status === 0
  ) {
    return false;
  }

  const output = [result.stderr, result.stdout]
    .filter((value): value is string => value !== undefined)
    .join('\n');
  if (!output || remoteManifestFailurePattern.test(output)) {
    return false;
  }

  const exactBuildxNotFoundPattern = new RegExp(
    `(?:^|\\r?\\n)ERROR:\\s+${escapeRegularExpression(
      imageReference,
    )}:\\s+not found\\s*(?:\\r?\\n|$)`,
    'iu',
  );
  const referenceManifestNotFoundPattern = new RegExp(
    `\\bmanifest for ["']?${escapeRegularExpression(
      imageReference,
    )}["']? (?:was )?not found\\b`,
    'iu',
  );
  if (
    exactBuildxNotFoundPattern.test(output) ||
    referenceManifestNotFoundPattern.test(output)
  ) {
    return true;
  }

  return remoteManifestMissingPatterns.some((pattern) =>
    pattern.test(output),
  );
}

function escapeRegularExpression(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
}

function replaceDockerBuildImageReferences(
  plan: DockerBuildPlan,
  imageReferences: readonly string[],
): string[] {
  const tagArgumentsStart = 4;
  const tagArgumentsEnd =
    tagArgumentsStart + plan.imageReferences.length * 2;
  const tagArguments = imageReferences.flatMap((imageReference) => [
    '--tag',
    imageReference,
  ]);

  return [
    ...plan.args.slice(0, tagArgumentsStart),
    ...tagArguments,
    ...plan.args.slice(tagArgumentsEnd),
  ];
}

export function validateDockerTag(tag: string): string {
  const normalized = tag.trim();
  if (!dockerTagPattern.test(normalized)) {
    throw new Error(
      `Invalid Docker tag "${tag}"; use at most 128 letters, digits, periods, underscores, or hyphens and start with a letter, digit, or underscore.`,
    );
  }
  return normalized;
}

export function sanitizeGitBranch(branch: string): string {
  return sanitizeGitBranchToLength(branch, dockerTagMaximumLength);
}

export function createGitBranchTag(branch: string): string {
  const branchValue = sanitizeGitBranchToLength(
    branch,
    dockerTagMaximumLength - gitBranchTagPrefix.length,
  );
  return validateDockerTag(`${gitBranchTagPrefix}${branchValue}`);
}

function sanitizeGitBranchToLength(
  branch: string,
  maximumLength: number,
): string {
  const normalized = normalizeGitBranchIdentity(branch);
  const sanitized = normalized
    .normalize('NFKD')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/gu, '-')
    .replace(/[._-]{2,}/gu, '-')
    .replace(/^[._-]+|[._-]+$/gu, '');

  if (!sanitized || sanitized === 'head') {
    throw new Error(
      `Git branch "${branch}" does not contain a usable Docker tag value.`,
    );
  }

  if (sanitized.length <= maximumLength) {
    return validateDockerTag(sanitized);
  }

  const hash = createHash('sha256')
    .update(normalized.normalize('NFKD'), 'utf8')
    .digest('hex')
    .slice(0, gitBranchHashLength);
  const readableLength =
    maximumLength - hash.length - 1;
  const readablePrefix = sanitized
    .slice(0, readableLength)
    .replace(/[._-]+$/gu, '');
  return validateDockerTag(`${readablePrefix}-${hash}`);
}

function normalizeGitBranchIdentity(branch: string): string {
  return branch
    .trim()
    .replace(/^refs\/heads\//iu, '')
    .replace(/^refs\/remotes\/[^/]+\//iu, '')
    .replace(/^origin\//iu, '');
}

function createTokenExpander(
  context: ExecutorContext,
  dependencies: Pick<
    DockerBuildExecutorDependencies,
    'commandRunner' | 'environment'
  >,
): (value: string, optionName: string) => string {
  const workspaceRoot = path.resolve(context.root);
  let projectRoot: string | undefined;
  let gitSha: string | undefined;
  let rawGitBranch: string | undefined;
  let gitBranch: string | undefined;
  let gitBranchTag: string | undefined;

  return (value, optionName) => {
    let expanded = value.replaceAll('{absWorkspaceRoot}', workspaceRoot);

    if (expanded.includes('{absProjectRoot}')) {
      projectRoot ??= resolveProjectRoot(context);
      expanded = expanded.replaceAll('{absProjectRoot}', projectRoot);
    }
    if (expanded.includes('{projectName}')) {
      if (!context.projectName) {
        throw new Error(
          `Cannot expand {projectName} in ${optionName}: the executor context has no project name.`,
        );
      }
      expanded = expanded.replaceAll('{projectName}', context.projectName);
    }

    expanded = expanded.replace(
      /\{env:([^{}]+)\}/gu,
      (_token, environmentVariable: string) => {
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(environmentVariable)) {
          throw new Error(
            `Invalid environment token "{env:${environmentVariable}}" in ${optionName}.`,
          );
        }
        const environmentValue =
          dependencies.environment[environmentVariable];
        if (environmentValue === undefined || environmentValue.length === 0) {
          throw new Error(
            `Environment variable "${environmentVariable}" required by ${optionName} is not set.`,
          );
        }
        return environmentValue;
      },
    );

    if (
      expanded.includes('{gitSha}') ||
      expanded.includes('{gitShortSha}')
    ) {
      gitSha ??= resolveGitSha(
        workspaceRoot,
        dependencies.environment,
        dependencies.commandRunner,
      );
      expanded = expanded
        .replaceAll('{gitSha}', gitSha)
        .replaceAll('{gitShortSha}', gitSha.slice(0, 12));
    }
    if (
      expanded.includes('{gitBranch}') ||
      expanded.includes('{gitBranchTag}')
    ) {
      rawGitBranch ??= resolveGitBranchIdentity(
        workspaceRoot,
        dependencies.environment,
        dependencies.commandRunner,
      );
    }
    if (expanded.includes('{gitBranchTag}')) {
      gitBranchTag ??= createGitBranchTag(rawGitBranch ?? '');
      expanded = expanded.replaceAll(
        '{gitBranchTag}',
        gitBranchTag,
      );
    }
    if (expanded.includes('{gitBranch}')) {
      gitBranch ??= sanitizeGitBranch(rawGitBranch ?? '');
      expanded = expanded.replaceAll('{gitBranch}', gitBranch);
    }

    return expanded;
  };
}

function resolveProjectRoot(context: ExecutorContext): string {
  if (!context.projectName) {
    throw new Error(
      'Cannot expand {absProjectRoot}: the executor context has no project name.',
    );
  }
  const project =
    context.projectsConfigurations?.projects[context.projectName];
  if (!project) {
    throw new Error(
      `Cannot expand {absProjectRoot}: project "${context.projectName}" is absent from the executor context.`,
    );
  }
  return path.resolve(context.root, project.root);
}

function resolveGitSha(
  workspaceRoot: string,
  environment: NodeJS.ProcessEnv,
  commandRunner: CommandRunner,
): string {
  const rawSha =
    firstEnvironmentValue(environment, gitShaEnvironmentVariables) ??
    runGitCommand(
      commandRunner,
      workspaceRoot,
      ['rev-parse', 'HEAD'],
      'Git SHA',
    );
  const normalized = rawSha.trim().toLowerCase();

  if (!gitShaPattern.test(normalized)) {
    throw new Error(
      `Git SHA "${rawSha.trim()}" is invalid; expected 7 to 64 hexadecimal characters.`,
    );
  }
  return normalized;
}

function resolveGitBranchIdentity(
  workspaceRoot: string,
  environment: NodeJS.ProcessEnv,
  commandRunner: CommandRunner,
): string {
  const rawBranch =
    firstEnvironmentValue(environment, gitBranchEnvironmentVariables) ??
    runGitCommand(
      commandRunner,
      workspaceRoot,
      ['branch', '--show-current'],
      'Git branch',
    );
  return rawBranch;
}

function firstEnvironmentValue(
  environment: NodeJS.ProcessEnv,
  variableNames: readonly string[],
): string | undefined {
  for (const variableName of variableNames) {
    const value = environment[variableName]?.trim();
    if (value) {
      return value;
    }
  }
  return undefined;
}

function runGitCommand(
  commandRunner: CommandRunner,
  workspaceRoot: string,
  args: readonly string[],
  description: string,
): string {
  const result = commandRunner.run('git', args, {
    captureOutput: true,
    cwd: workspaceRoot,
  });
  const failure = commandFailure(`git ${args.join(' ')}`, result);
  if (failure) {
    throw new Error(`Unable to resolve ${description}: ${failure}`);
  }
  const value = result.stdout?.trim();
  if (!value) {
    throw new Error(`Unable to resolve ${description}: Git returned no value.`);
  }
  return value;
}

function validateString(value: unknown, optionName: string): string {
  if (typeof value !== 'string') {
    throw new Error(`${optionName} must be a string.`);
  }
  return value;
}

function validateNonBlank(value: string, optionName: string): string {
  if (!value.trim()) {
    throw new Error(`${optionName} must not be empty.`);
  }
  if (value.includes('\0')) {
    throw new Error(`${optionName} must not contain a null character.`);
  }
  return value;
}

function validateBoolean(value: unknown, optionName: string): boolean {
  if (typeof value !== 'boolean') {
    throw new Error(`${optionName} must be a boolean.`);
  }
  return value;
}

function validateStringArray(
  value: unknown,
  optionName: string,
  requireItems: boolean,
): string[] {
  if (!Array.isArray(value)) {
    throw new Error(`${optionName} must be an array of strings.`);
  }
  if (requireItems && value.length === 0) {
    throw new Error(`${optionName} must contain at least one value.`);
  }
  return value.map((item, index) =>
    validateString(item, `${optionName}[${index}]`),
  );
}

function validateOutputMode(value: unknown): DockerBuildOutputMode {
  if (value !== 'load' && value !== 'push') {
    throw new Error('output must be either "load" or "push".');
  }
  return value;
}

function validateImage(image: string): string {
  const normalized = validateNonBlank(image, 'image').trim();
  if (/[\s{}]/u.test(normalized)) {
    throw new Error(
      `Invalid Docker image "${image}"; image repositories cannot contain whitespace or unresolved tokens.`,
    );
  }
  if (normalized.includes('@')) {
    throw new Error(
      `Invalid Docker image "${image}"; provide a repository without a digest.`,
    );
  }
  if (normalized.endsWith('/')) {
    throw new Error(
      `Invalid Docker image "${image}"; the repository name is missing.`,
    );
  }

  const lastPathComponent =
    normalized.slice(normalized.lastIndexOf('/') + 1);
  if (lastPathComponent.includes(':')) {
    throw new Error(
      `Invalid Docker image "${image}"; provide tags through the tags option instead of image.`,
    );
  }
  return normalized;
}

function validatePlatform(platform: string, optionName: string): string {
  const normalized = platform.trim();
  if (!platformPattern.test(normalized)) {
    throw new Error(
      `${optionName} must use lowercase os/architecture[/variant] syntax; received "${platform}".`,
    );
  }
  return normalized;
}

function normalizeBuildContext(buildContext: string): string {
  const normalized = validateNonBlank(buildContext, 'context');
  return normalized === '-' || /^[A-Za-z][A-Za-z0-9+.-]*:\/\//u.test(normalized)
    ? normalized
    : path.normalize(normalized);
}

function rejectDuplicates(values: readonly string[], optionName: string): void {
  const seen = new Set<string>();
  for (const value of values) {
    if (seen.has(value)) {
      throw new Error(`${optionName} contains duplicate value "${value}".`);
    }
    seen.add(value);
  }
}

function resolveManifestFile(filePath: string, workspaceRoot: string): string {
  const normalized = validateNonBlank(filePath, 'manifestFile');
  return path.isAbsolute(normalized)
    ? path.normalize(normalized)
    : path.resolve(workspaceRoot, normalized);
}

function commandFailure(
  description: string,
  result: CommandResult,
): string | undefined {
  if (result.error) {
    return `Failed to start ${description}: ${result.error.message}`;
  }
  if (result.status === null) {
    return result.signal
      ? `${description} was terminated by signal ${result.signal}.`
      : `${description} ended without an exit status.`;
  }
  if (result.status !== 0) {
    const stderr = result.stderr?.trim();
    return `${description} exited with status ${result.status}${
      stderr ? `: ${stderr}` : '.'
    }`;
  }
  return undefined;
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function redactBuildArguments(args: readonly string[]): string[] {
  return args.map((argument, index) => {
    if (args[index - 1] !== '--build-arg') {
      return argument;
    }
    const separator = argument.indexOf('=');
    return separator < 0
      ? argument
      : `${argument.slice(0, separator)}=<redacted>`;
  });
}

function formatCommand(command: string, args: readonly string[]): string {
  return [command, ...args].map((part) => JSON.stringify(part)).join(' ');
}
