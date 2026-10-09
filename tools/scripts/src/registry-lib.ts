import type {
  CommandOptions,
  CommandResult,
  CommandRunner,
} from './process.js';

export const GHCR_HOST = 'ghcr.io';
export const DEFAULT_BRANCH = 'main';

const packagePageSize = 100;
const containerSegmentPattern =
  /^[a-z0-9]+(?:(?:[._]|__|[-]+)[a-z0-9]+)*$/u;
const ownerPattern = /^[a-z0-9](?:[a-z0-9-]{0,37}[a-z0-9])?$/u;
const registryTagPattern = /^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$/u;

export type RegistryExecutionEnvironment = 'ci' | 'local';
export type RegistryCredentialSource =
  | 'GHCR_TOKEN'
  | 'GITHUB_TOKEN'
  | 'gh-auth-token';

export class RegistryCommandError extends Error {
  readonly exitCode: number;

  constructor(message: string, exitCode = 1) {
    super(message);
    this.name = 'RegistryCommandError';
    this.exitCode = exitCode;
  }
}

export interface RegistryImageInput {
  readonly image: string;
  readonly owner: string;
  readonly repository: string;
}

export interface RegistryImage {
  readonly image: string;
  readonly owner: string;
  readonly packageName: string;
  readonly reference: string;
  readonly repository: string;
}

export interface RegistryLoginOptions {
  readonly environment: RegistryExecutionEnvironment;
  readonly env: Readonly<NodeJS.ProcessEnv>;
  readonly owner: string;
}

export interface RegistryLoginResult {
  readonly credentialSource: RegistryCredentialSource;
  readonly owner: string;
  readonly registry: typeof GHCR_HOST;
  readonly status: 'authenticated';
}

export interface RegistryTagDescriptor {
  readonly immutable: boolean;
  readonly kind: 'branch' | 'git-sha' | 'latest';
  readonly value: string;
}

export interface RegistryTagMetadata {
  readonly branch: string;
  readonly branchTag: string;
  readonly defaultBranch: string;
  readonly gitSha: string;
  readonly isDefaultBranch: boolean;
  readonly publishLatest: boolean;
  readonly shaTag: string;
  readonly tags: readonly RegistryTagDescriptor[];
  readonly values: readonly string[];
}

export interface ManifestVerificationResult {
  readonly reference: string;
  readonly status: 'verified';
}

export interface PackagePublicResult {
  readonly image: string;
  readonly owner: string;
  readonly packageName: string;
  readonly reference: string;
  readonly repository: string;
  readonly status: 'public';
  readonly visibility: 'public';
}

export interface PackageNeedsUiChangeResult {
  readonly currentVisibility: string;
  readonly image: string;
  readonly owner: string;
  readonly packageName: string;
  readonly reference: string;
  readonly repository: string;
  readonly settingsUrl: string;
  readonly status: 'needs-ui-change';
  readonly targetVisibility: 'public';
}

export type PackageVisibilityResult =
  | PackageNeedsUiChangeResult
  | PackagePublicResult;

interface GhcrPackageMetadata {
  readonly name: string;
  readonly owner: string;
  readonly packageType: 'container';
  readonly visibility: string;
}

interface RegistryCredential {
  readonly source: RegistryCredentialSource;
  readonly token: string;
}

export function resolveExecutionEnvironment(
  explicitValue: string | undefined,
  env: Readonly<NodeJS.ProcessEnv>,
): RegistryExecutionEnvironment {
  const value = normalizeOptional(explicitValue ?? env.REGISTRY_ENVIRONMENT);
  if (value !== undefined) {
    if (value === 'ci' || value === 'local') {
      return value;
    }
    throw new RegistryCommandError(
      `Unsupported registry environment "${value}"; expected "ci" or "local".`,
    );
  }

  return env.GITHUB_ACTIONS === 'true' || env.CI === 'true' ? 'ci' : 'local';
}

export function createRegistryImage(input: RegistryImageInput): RegistryImage {
  const owner = normalizeOwner(input.owner);
  const repository = normalizeContainerSegment(
    input.repository,
    'repository',
  );
  const image = normalizeContainerPath(input.image, 'image');
  const packageName = `${repository}/${image}`;

  return {
    image,
    owner,
    packageName,
    reference: `${GHCR_HOST}/${owner}/${packageName}`,
    repository,
  };
}

export function encodePackageName(packageName: string): string {
  return encodeURIComponent(normalizeContainerPath(packageName, 'package name'));
}

export function loginToRegistry(
  options: RegistryLoginOptions,
  runner: CommandRunner,
): RegistryLoginResult {
  const owner = normalizeOwner(options.owner);
  const credential = resolveRegistryCredential(options, runner);
  const loginResult = runner.run(
    'docker',
    [
      'login',
      GHCR_HOST,
      '--username',
      owner,
      '--password-stdin',
    ],
    {
      input: `${credential.token}\n`,
      stderr: 'capture',
      stdout: 'capture',
    },
  );

  if (loginResult.exitCode !== 0) {
    throw new RegistryCommandError(
      `Docker login to ${GHCR_HOST} failed for ${owner} with exit code ${loginResult.exitCode}.`,
      loginResult.exitCode,
    );
  }

  return {
    credentialSource: credential.source,
    owner,
    registry: GHCR_HOST,
    status: 'authenticated',
  };
}

export function sanitizeBranchName(branch: string): string {
  const branchName = normalizeBranchName(branch);
  const sanitized = branchName
    .normalize('NFKD')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/gu, '-')
    .replace(/[._-]{2,}/gu, '-')
    .replace(/^[._-]+|[._-]+$/gu, '');
  const truncated = sanitized.slice(0, 128).replace(/[._-]+$/gu, '');

  if (!truncated) {
    throw new RegistryCommandError(
      `Branch name "${branchName}" does not contain characters usable in a container tag.`,
    );
  }

  return validateRegistryTag(truncated);
}

export function createRegistryTagMetadata(options: {
  readonly branch: string;
  readonly defaultBranch?: string;
  readonly gitSha: string;
}): RegistryTagMetadata {
  const branch = normalizeBranchName(options.branch);
  const defaultBranch = normalizeBranchName(
    options.defaultBranch ?? DEFAULT_BRANCH,
  );
  const branchTag = sanitizeBranchName(branch);
  const gitSha = normalizeGitSha(options.gitSha);
  const shaTag = `sha-${gitSha}`;
  const isDefaultBranch = branch === defaultBranch;
  const tags: RegistryTagDescriptor[] = [
    {
      immutable: false,
      kind: 'branch',
      value: branchTag,
    },
    {
      immutable: true,
      kind: 'git-sha',
      value: shaTag,
    },
  ];

  if (isDefaultBranch) {
    tags.push({
      immutable: false,
      kind: 'latest',
      value: 'latest',
    });
  }

  return {
    branch,
    branchTag,
    defaultBranch,
    gitSha,
    isDefaultBranch,
    publishLatest: isDefaultBranch,
    shaTag,
    tags,
    values: tags.map(({ value }) => value),
  };
}

export function verifyRemoteManifest(
  input: RegistryImageInput & { readonly tag: string },
  runner: CommandRunner,
): ManifestVerificationResult {
  const image = createRegistryImage(input);
  const tag = validateRegistryTag(input.tag);
  const reference = `${image.reference}:${tag}`;
  const result = runner.run(
    'docker',
    ['manifest', 'inspect', reference],
    {
      stderr: 'capture',
      stdout: 'ignore',
    },
  );

  if (result.exitCode !== 0) {
    throw new RegistryCommandError(
      `Remote image manifest was not found or is not accessible: ${reference} (docker manifest inspect exited with code ${result.exitCode}).`,
      result.exitCode,
    );
  }

  return {
    reference,
    status: 'verified',
  };
}

export function inspectPackageVisibility(
  input: RegistryImageInput,
  runner: CommandRunner,
): PackageVisibilityResult {
  const image = createRegistryImage(input);
  const packageMetadata = findUserPackage(image, runner);

  if (packageMetadata.visibility !== 'public') {
    return {
      currentVisibility: packageMetadata.visibility,
      image: image.image,
      owner: image.owner,
      packageName: image.packageName,
      reference: image.reference,
      repository: image.repository,
      settingsUrl: packageSettingsUrl(image),
      status: 'needs-ui-change',
      targetVisibility: 'public',
    };
  }

  verifyPublicPackageEndpoint(image, runner);

  return {
    image: image.image,
    owner: image.owner,
    packageName: image.packageName,
    reference: image.reference,
    repository: image.repository,
    status: 'public',
    visibility: 'public',
  };
}

export function validateRegistryTag(tag: string): string {
  const normalized = tag.trim();
  if (!registryTagPattern.test(normalized)) {
    throw new RegistryCommandError(
      `Invalid container tag "${tag}"; use at most 128 letters, digits, periods, underscores, or hyphens and start with a letter, digit, or underscore.`,
    );
  }
  return normalized;
}

function resolveRegistryCredential(
  options: RegistryLoginOptions,
  runner: CommandRunner,
): RegistryCredential {
  if (options.environment === 'ci') {
    const ghcrToken = normalizeOptional(options.env.GHCR_TOKEN);
    if (ghcrToken !== undefined) {
      return {
        source: 'GHCR_TOKEN',
        token: ghcrToken,
      };
    }

    const githubToken = normalizeOptional(options.env.GITHUB_TOKEN);
    if (githubToken !== undefined) {
      return {
        source: 'GITHUB_TOKEN',
        token: githubToken,
      };
    }

    throw new RegistryCommandError(
      'CI registry login requires GHCR_TOKEN or GITHUB_TOKEN.',
    );
  }

  const result = runner.run('gh', ['auth', 'token'], {
    stderr: 'capture',
    stdout: 'capture',
  });
  if (result.exitCode !== 0) {
    throw new RegistryCommandError(
      `Unable to obtain a local GitHub token from "gh auth token" (exit code ${result.exitCode}).`,
      result.exitCode,
    );
  }

  const token = normalizeOptional(result.stdout);
  if (token === undefined) {
    throw new RegistryCommandError(
      '"gh auth token" returned an empty token.',
    );
  }

  return {
    source: 'gh-auth-token',
    token,
  };
}

function findUserPackage(
  image: RegistryImage,
  runner: CommandRunner,
): GhcrPackageMetadata {
  for (let page = 1; page <= 100; page += 1) {
    const endpoint =
      `users/${encodeURIComponent(image.owner)}/packages` +
      `?package_type=container&per_page=${packagePageSize}&page=${page}`;
    const response = runGhApi(
      runner,
      endpoint,
      `Unable to list GHCR packages for ${image.owner}`,
    );
    const packages = parsePackageList(response.stdout, endpoint);
    const packageMetadata = packages.find(
      ({ name }) => name.toLowerCase() === image.packageName,
    );

    if (packageMetadata !== undefined) {
      if (packageMetadata.owner.toLowerCase() !== image.owner) {
        throw new RegistryCommandError(
          `GHCR package ${image.packageName} is owned by ${packageMetadata.owner}, not ${image.owner}.`,
        );
      }
      return packageMetadata;
    }

    if (packages.length < packagePageSize) {
      break;
    }
  }

  throw new RegistryCommandError(
    `GHCR package ${image.owner}/${image.packageName} was not found or is not visible to the current gh authentication.`,
  );
}

function verifyPublicPackageEndpoint(
  image: RegistryImage,
  runner: CommandRunner,
): void {
  const endpoint =
    `users/${encodeURIComponent(image.owner)}/packages/container/` +
    encodePackageName(image.packageName);
  const response = runGhApi(
    runner,
    endpoint,
    `GHCR package ${image.owner}/${image.packageName} is marked public but could not be verified through the public package endpoint`,
  );
  const packageMetadata = parsePackage(response.stdout, endpoint);

  if (
    packageMetadata.name.toLowerCase() !== image.packageName ||
    packageMetadata.owner.toLowerCase() !== image.owner ||
    packageMetadata.visibility !== 'public'
  ) {
    throw new RegistryCommandError(
      `Public visibility verification returned unexpected metadata for ${image.owner}/${image.packageName}.`,
    );
  }
}

function runGhApi(
  runner: CommandRunner,
  endpoint: string,
  errorMessage: string,
): CommandResult {
  const options: CommandOptions = {
    stderr: 'capture',
    stdout: 'capture',
  };
  const result = runner.run(
    'gh',
    [
      'api',
      endpoint,
      '--method',
      'GET',
      '--header',
      'Accept: application/vnd.github+json',
    ],
    options,
  );

  if (result.exitCode !== 0) {
    throw new RegistryCommandError(
      `${errorMessage} (gh api exited with code ${result.exitCode}).`,
      result.exitCode,
    );
  }

  return result;
}

function parsePackageList(
  value: string,
  endpoint: string,
): GhcrPackageMetadata[] {
  const parsed = parseJson(value, endpoint);
  if (!Array.isArray(parsed)) {
    throw new RegistryCommandError(
      `GitHub package endpoint ${endpoint} returned a non-array response.`,
    );
  }
  return parsed.map((item) => parsePackageValue(item, endpoint));
}

function parsePackage(value: string, endpoint: string): GhcrPackageMetadata {
  return parsePackageValue(parseJson(value, endpoint), endpoint);
}

function parsePackageValue(
  value: unknown,
  endpoint: string,
): GhcrPackageMetadata {
  if (typeof value !== 'object' || value === null) {
    throw new RegistryCommandError(
      `GitHub package endpoint ${endpoint} returned invalid package metadata.`,
    );
  }

  const record = value as Record<string, unknown>;
  const owner = record.owner;
  if (
    typeof record.name !== 'string' ||
    record.package_type !== 'container' ||
    typeof record.visibility !== 'string' ||
    typeof owner !== 'object' ||
    owner === null ||
    typeof (owner as Record<string, unknown>).login !== 'string'
  ) {
    throw new RegistryCommandError(
      `GitHub package endpoint ${endpoint} returned incomplete package metadata.`,
    );
  }

  return {
    name: record.name,
    owner: (owner as Record<string, unknown>).login as string,
    packageType: 'container',
    visibility: record.visibility,
  };
}

function parseJson(value: string, endpoint: string): unknown {
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw new RegistryCommandError(
      `GitHub package endpoint ${endpoint} returned invalid JSON.`,
    );
  }
}

function packageSettingsUrl(image: RegistryImage): string {
  return (
    `https://github.com/users/${encodeURIComponent(image.owner)}` +
    `/packages/container/${encodePackageName(image.packageName)}/settings`
  );
}

function normalizeOwner(value: string): string {
  const owner = value.trim().toLowerCase();
  if (!ownerPattern.test(owner)) {
    throw new RegistryCommandError(`Invalid GitHub owner "${value}".`);
  }
  return owner;
}

function normalizeContainerPath(value: string, label: string): string {
  const normalized = value.trim().toLowerCase();
  const segments = normalized.split('/');
  if (
    normalized.length === 0 ||
    segments.some((segment) => !containerSegmentPattern.test(segment))
  ) {
    throw new RegistryCommandError(
      `Invalid ${label} "${value}"; use lowercase container path segments separated by "/".`,
    );
  }
  return segments.join('/');
}

function normalizeContainerSegment(value: string, label: string): string {
  const normalized = normalizeContainerPath(value, label);
  if (normalized.includes('/')) {
    throw new RegistryCommandError(
      `Invalid ${label} "${value}"; expected a single container path segment.`,
    );
  }
  return normalized;
}

function normalizeBranchName(value: string): string {
  const normalized = value.trim().replace(/^refs\/heads\//u, '');
  if (!normalized) {
    throw new RegistryCommandError('Branch name must not be empty.');
  }
  return normalized;
}

function normalizeGitSha(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (!/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/u.test(normalized)) {
    throw new RegistryCommandError(
      `Git SHA "${value}" must be a full 40- or 64-character hexadecimal object ID.`,
    );
  }
  return normalized;
}

function normalizeOptional(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
}
