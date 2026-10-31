#!/usr/bin/env node

import { appendFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import {
  createProjectGraphAsync,
  readProjectsConfigurationFromProjectGraph,
  type ProjectConfiguration,
  type TargetConfiguration,
} from '@nx/devkit';

export const PRODUCTION_RUNTIME_IMAGE_PROJECTS = [
  'otelcol-rust',
  'arrowcol-rust',
  'arrowcol-dataflow-rust',
  'arrowcol-router-rust',
  'kusto-bridge',
  'delta-maintenance',
  'arrow-tenant-stream',
  'kusto-explorer',
] as const;

export const RETAINED_RUNTIME_IMAGE_PROJECTS = [
  'hello-world',
] as const;

export const PUBLISHED_RUNTIME_IMAGE_PROJECTS = [
  ...PRODUCTION_RUNTIME_IMAGE_PROJECTS,
  ...RETAINED_RUNTIME_IMAGE_PROJECTS,
] as const;

const dockerExecutor = 'rust-playground-build:docker-build';
const imageNamespace =
  'ghcr.io/mdrakiburrahman/rust-playground';
const projectNamePattern = /^[a-z0-9][a-z0-9-]*$/u;

interface DockerTargetOptions {
  readonly buildArgs?: unknown;
  readonly image?: unknown;
  readonly immutableTags?: unknown;
  readonly manifestFile?: unknown;
  readonly output?: unknown;
  readonly platforms?: unknown;
  readonly requireCleanWorktree?: unknown;
  readonly tags?: unknown;
}

export interface ImagePublication {
  readonly image: string;
  readonly manifest: string;
  readonly project: string;
  readonly reference: string;
}

export function createImagePublicationPlan(
  projects: Readonly<Record<string, ProjectConfiguration>>,
): readonly ImagePublication[] {
  const discovered = Object.entries(projects)
    .filter(([, project]) =>
      project.targets?.publish?.executor === dockerExecutor,
    )
    .map(([name]) => name)
    .sort();
  const expected = [...PUBLISHED_RUNTIME_IMAGE_PROJECTS].sort();
  if (!sameValues(discovered, expected)) {
    throw new Error(
      `Published runtime image projects must be exactly ${expected.join(', ')}; discovered ${discovered.join(', ') || 'none'}.`,
    );
  }

  return PUBLISHED_RUNTIME_IMAGE_PROJECTS.map((project) => {
    if (!projectNamePattern.test(project)) {
      throw new Error(`Invalid runtime image project name: ${project}`);
    }
    const configuration = projects[project];
    if (configuration === undefined) {
      throw new Error(`Missing runtime image project: ${project}`);
    }
    if (!configuration.tags?.includes('type:runtime-image')) {
      throw new Error(
        `${project} must be tagged type:runtime-image.`,
      );
    }

    const imageTarget = requireDockerTarget(
      project,
      'image',
      configuration.targets?.image,
    );
    const publishTarget = requireDockerTarget(
      project,
      'publish',
      configuration.targets?.publish,
    );
    const smokeTarget = configuration.targets?.['image-smoke'];
    if (
      smokeTarget?.executor !== 'nx:run-commands' ||
      !dependsOnTarget(smokeTarget, 'image')
    ) {
      throw new Error(
        `${project}:image-smoke must be a run-commands target that depends on image.`,
      );
    }

    const expectedReference = `${imageNamespace}/${project}`;
    const loadManifest =
      `{absWorkspaceRoot}/target/images/${project}-load.txt`;
    const publishManifest =
      `{absWorkspaceRoot}/target/images/${project}-publish.txt`;
    requireEqual(
      imageTarget.image,
      expectedReference,
      `${project}:image repository`,
    );
    requireEqual(
      publishTarget.image,
      expectedReference,
      `${project}:publish repository`,
    );
    requireStringArray(
      imageTarget.tags,
      ['dev'],
      `${project}:image tags`,
    );
    requireStringArray(
      imageTarget.platforms,
      ['linux/amd64'],
      `${project}:image platforms`,
    );
    requireEqual(
      imageTarget.output,
      'load',
      `${project}:image output`,
    );
    requireEqual(
      imageTarget.manifestFile,
      loadManifest,
      `${project}:image manifest`,
    );
    requireStringArray(
      publishTarget.tags,
      ['{gitBranchTag}', 'sha-{gitSha}'],
      `${project}:publish tags`,
    );
    requireStringArray(
      publishTarget.immutableTags,
      ['sha-{gitSha}'],
      `${project}:publish immutable tags`,
    );
    requireEqual(
      publishTarget.requireCleanWorktree,
      true,
      `${project}:publish clean-worktree guard`,
    );
    requireStringArray(
      publishTarget.platforms,
      ['linux/amd64'],
      `${project}:publish platforms`,
    );
    requireEqual(
      publishTarget.output,
      'push',
      `${project}:publish output`,
    );
    requireEqual(
      publishTarget.manifestFile,
      publishManifest,
      `${project}:publish manifest`,
    );
    if (
      (PRODUCTION_RUNTIME_IMAGE_PROJECTS as readonly string[]).includes(
        project,
      )
    ) {
      requireStringArray(
        publishTarget.buildArgs,
        ['VCS_REF={gitSha}'],
        `${project}:publish OCI revision build argument`,
      );
    }
    requireStringArray(
      configuration.targets?.publish?.configurations?.main?.options
        ?.tags ??
        configuration.targets?.publish?.configurations?.main?.tags,
      ['{gitBranchTag}', 'sha-{gitSha}', 'latest'],
      `${project}:publish:main tags`,
    );

    const cache =
      configuration.targets?.image?.configurations?.['ci-verify'];
    if (
      !isNonEmptyStringArray(cache?.cacheFrom) ||
      !isNonEmptyStringArray(cache?.cacheTo) ||
      cache?.tags !== undefined
    ) {
      throw new Error(
        `${project}:image must provide a ci-verify BuildKit cache configuration without replacing tags.`,
      );
    }

    return {
      image: project,
      manifest: `target/images/${project}-publish.txt`,
      project,
      reference: expectedReference,
    };
  });
}

function requireDockerTarget(
  project: string,
  target: string,
  configuration: TargetConfiguration | undefined,
): DockerTargetOptions {
  if (configuration?.executor !== dockerExecutor) {
    throw new Error(
      `${project}:${target} must use ${dockerExecutor}.`,
    );
  }
  return (configuration.options ?? {}) as DockerTargetOptions;
}

function dependsOnTarget(
  configuration: TargetConfiguration,
  target: string,
): boolean {
  return (configuration.dependsOn ?? []).some((dependency) =>
    dependency === target ||
    (
      typeof dependency === 'object' &&
      dependency !== null &&
      dependency.target === target
    ),
  );
}

function requireEqual(
  actual: unknown,
  expected: unknown,
  label: string,
): void {
  if (actual !== expected) {
    throw new Error(
      `${label} must be ${JSON.stringify(expected)}; received ${JSON.stringify(actual)}.`,
    );
  }
}

function requireStringArray(
  actual: unknown,
  expected: readonly string[],
  label: string,
): void {
  if (
    !Array.isArray(actual) ||
    !actual.every((value): value is string => typeof value === 'string') ||
    !sameValues(actual, expected)
  ) {
    throw new Error(
      `${label} must be ${JSON.stringify(expected)}; received ${JSON.stringify(actual)}.`,
    );
  }
}

function isNonEmptyStringArray(
  value: unknown,
): value is readonly string[] {
  return (
    Array.isArray(value) &&
    value.length > 0 &&
    value.every((item) => typeof item === 'string' && item.length > 0)
  );
}

function sameValues(
  left: readonly string[],
  right: readonly string[],
): boolean {
  return (
    left.length === right.length &&
    left.every((value, index) => value === right[index])
  );
}

async function main(): Promise<void> {
  const output = process.env.GITHUB_OUTPUT;
  if (!output) {
    throw new Error('GITHUB_OUTPUT must be set.');
  }
  const graph = await createProjectGraphAsync();
  const projects =
    readProjectsConfigurationFromProjectGraph(graph).projects;
  const include = createImagePublicationPlan(projects);
  appendFileSync(
    output,
    `matrix=${JSON.stringify({ include })}\n`,
  );
  console.log(
    `Image publication plan: ${include.map(({ project }) => project).join(', ')}.`,
  );
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  main().catch((error: unknown) => {
    console.error(
      `image-plan: ${error instanceof Error ? error.message : String(error)}`,
    );
    process.exitCode = 1;
  });
}
