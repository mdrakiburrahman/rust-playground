#!/usr/bin/env node

import { appendFileSync, mkdirSync } from 'node:fs';
import { availableParallelism } from 'node:os';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
  readJsonFile,
  type ProjectGraph,
  type TargetConfiguration,
  type TaskGraph,
} from '@nx/devkit';

import { runChecked, SpawnCommandRunner } from './process.js';

export interface VerificationGraph {
  readonly graph: Pick<ProjectGraph, 'nodes'>;
  readonly tasks: Pick<TaskGraph, 'dependencies' | 'tasks'>;
}

export interface VerificationShard {
  readonly key: string;
  readonly task: string;
}

export interface VerificationBatchShard extends VerificationShard {
  readonly tasks: readonly string[];
}

export interface ImageShard extends VerificationShard {
  readonly image_refs: readonly string[];
}

export interface VerificationPlan {
  readonly images: readonly ImageShard[];
  readonly node: readonly VerificationBatchShard[];
  readonly rust: readonly VerificationBatchShard[];
  readonly materialTasks: readonly string[];
  readonly reader: string;
  readonly integration: string;
}

interface CommandOptions {
  readonly command?: string;
  readonly commands?: readonly (string | { readonly command: string })[];
  readonly image?: string;
  readonly output?: string;
  readonly tags?: readonly string[];
  readonly cacheFrom?: readonly string[];
  readonly cacheTo?: readonly string[];
}

const readerTask = 'loadtest:reader-build';
const integrationTask = 'loadtest:e2e';
const taskPattern = /^[A-Za-z0-9_.@/-]+:[A-Za-z0-9_.-]+(?::[A-Za-z0-9_.-]+)?$/u;
const imagePattern = /^[a-z0-9][a-z0-9./_-]*:[A-Za-z0-9_][A-Za-z0-9_.-]*$/u;

export function requireVerificationTask(task: string): string {
  if (!taskPattern.test(task) || task.startsWith('-') || task.includes('..')) {
    throw new Error(`Invalid Nx verification task: ${task}`);
  }
  return task;
}

export function requireImageReference(image: string): string {
  if (!imagePattern.test(image)) {
    throw new Error(`Invalid Docker image reference: ${image}`);
  }
  return image;
}

export function createVerificationPlan(graph: VerificationGraph): VerificationPlan {
  const tasks = graph.tasks.tasks;
  const configurations = new Map<string, TargetConfiguration<CommandOptions>>();
  for (const [id, task] of Object.entries(tasks)) {
    requireVerificationTask(id);
    const configuration =
      graph.graph.nodes[task.target.project]?.data.targets?.[task.target.target];
    if (configuration === undefined) {
      throw new Error(`Nx task ${id} has no target configuration.`);
    }
    configurations.set(id, configuration);
  }
  const material = new Set(
    [...configurations].filter(([, configuration]) =>
      configuration.executor !== 'nx:run-commands' ||
      configuration.options?.command !== undefined ||
      configuration.options?.commands?.length !== 0,
    ).map(([id]) => id),
  );
  for (const task of [readerTask, integrationTask]) {
    if (!material.has(task)) throw new Error(`Required CI task is missing: ${task}`);
  }

  const closure = (id: string, visiting = new Set<string>()): Set<string> => {
    if (!tasks[id]) throw new Error(`Missing Nx prerequisite: ${id}`);
    if (visiting.has(id)) throw new Error(`Cyclic Nx prerequisites at ${id}`);
    const next = new Set(visiting).add(id);
    const result = new Set(material.has(id) ? [id] : []);
    for (const dependency of graph.tasks.dependencies[id] ?? []) {
      for (const nested of closure(dependency, next)) result.add(nested);
    }
    return result;
  };
  // Validate even empty aggregation nodes; their dependencies must not disappear.
  for (const id of Object.keys(tasks)) closure(id);

  const images: ImageShard[] = [...material]
    .filter((id) => configurations.get(id)?.executor === 'rust-playground-build:docker-build')
    .sort()
    .map((id) => {
      const options = configurations.get(id)?.options;
      if (options?.output !== 'load' || !options.image || !options.tags?.length) {
        throw new Error(`CI image task ${id} must load explicitly tagged images.`);
      }
      if (closure(id).size !== 1) {
        throw new Error(`CI image task ${id} needs an explicit prerequisite handoff.`);
      }
      const cache = configurations.get(id)?.configurations?.['ci-verify'];
      if (!cache?.cacheFrom?.length || !cache.cacheTo?.length || cache.tags !== undefined) {
        throw new Error(`CI image task ${id} must provide a ci-verify cache configuration without changing tags.`);
      }
      return {
        key: shardKey(id),
        task: id,
        image_refs: options.tags.map((tag) => requireImageReference(`${options.image}:${tag}`)),
      };
    });
  if (images.length === 0) throw new Error('CI has no source-built runtime images.');

  const readerDependencies = closure(readerTask);
  const expectedReaderDependencies = new Set([
    readerTask, 'loadtest:lint', 'loadtest:test',
    ...['lint', 'build', 'test', 'generate', 'check', 'compatibility', 'generated-test', 'package-artifact']
      .map((target) => `arrowcol-sdk-gen:${target}`),
  ]);
  if (!sameSet(readerDependencies, expectedReaderDependencies)) {
    throw new Error('The reader build prerequisites must be exactly the loadtest and SDK writer checks.');
  }
  const special = new Set([...readerDependencies, integrationTask, ...images.map(({ task }) => task)]);
  const integrationDependencies = closure(integrationTask);
  integrationDependencies.delete(integrationTask);
  const expectedDependencies = new Set([...readerDependencies, ...images.map(({ task }) => task)]);
  if (!sameSet(integrationDependencies, expectedDependencies)) {
    throw new Error('E2E prerequisites must be exactly the reader and source image artifacts.');
  }

  const remaining = [...material].filter((id) => !special.has(id));
  const prerequisites = new Set<string>();
  for (const id of remaining) {
    for (const prerequisite of closure(id)) {
      if (prerequisite !== id) prerequisites.add(prerequisite);
    }
  }
  const roots = remaining.filter((id) => !prerequisites.has(id)).sort();
  const claimed = new Map([...special].map((id) => [id, readerDependencies.has(id) ? readerTask : id]));
  const nodeRoots: VerificationShard[] = [];
  const rustRoots: VerificationShard[] = [];
  const rootUsesCargo = new Map<string, boolean>();
  for (const id of roots) {
    const covered = closure(id);
    const project = id.split(':')[0];
    if (!project) throw new Error(`Invalid verification root: ${id}`);
    const usesCargo = [...covered].some((task) =>
      commands(configurations.get(task)).some((command) =>
        /(?:^|[\s"';&])(?:cargo|rustc|rustfmt|rustup|cargo-make|makers)(?:\s|$)/u.test(command)),
    );
    for (const prerequisite of covered) {
      const owner = claimed.get(prerequisite);
      if (owner !== undefined) {
        const ownerProject = owner.split(':')[0];
        if (
          ownerProject === project &&
          rootUsesCargo.get(owner) === usesCargo
        ) {
          continue;
        }
        throw new Error(`Nx prerequisite ${prerequisite} is shared by ${owner} and ${id}; add an artifact handoff.`);
      }
      claimed.set(prerequisite, id);
    }
    rootUsesCargo.set(id, usesCargo);
    (usesCargo ? rustRoots : nodeRoots).push({
      key: shardKey(id),
      task: id,
    });
  }
  if (!sameSet(new Set(claimed.keys()), material)) {
    throw new Error('Parallel CI does not cover every material Nx verification task.');
  }
  const batchByProject = (
    shards: readonly VerificationShard[],
    keySuffix: string,
  ): VerificationBatchShard[] => {
    const projects = new Map<string, string[]>();
    for (const { task } of shards) {
      const project = task.split(':')[0];
      if (!project) throw new Error(`Invalid verification task: ${task}`);
      const targets = projects.get(project) ?? [];
      targets.push(task);
      projects.set(project, targets);
    }
    return [...projects].map(([project, tasks]) => {
      const task = tasks[0];
      if (!task) throw new Error(`Empty verification shard: ${project}`);
      return {
        key: `${project}-${keySuffix}`,
        task,
        tasks,
      };
    });
  };
  const rust = batchByProject(rustRoots, 'checks');
  const node = batchByProject(nodeRoots, 'node-checks');
  const keys = [...images, ...node, ...rust].map(({ key }) => key);
  if (new Set(keys).size !== keys.length) throw new Error('CI shard keys collide.');
  if (node.length === 0 || rust.length === 0) throw new Error('CI verification matrices must not be empty.');
  if (Math.max(node.length, rust.length, images.length) > 256) {
    throw new Error('CI exceeds the GitHub matrix limit of 256 jobs.');
  }
  return {
    images,
    node,
    rust,
    materialTasks: [...material].sort(),
    reader: readerTask,
    integration: integrationTask,
  };
}

export function verificationArguments(
  task?: string | readonly string[],
  parallel = availableParallelism(),
): string[] {
  if (!Number.isSafeInteger(parallel) || parallel < 1) {
    throw new Error('Nx parallelism must be a positive integer.');
  }
  let selection: string[];
  if (task === undefined) {
    selection = ['run-many', '-t', 'verify', '--all'];
  } else if (typeof task === 'string') {
    selection = ['run', requireVerificationTask(task)];
  } else {
    if (task.length === 0) throw new Error('An Nx task batch must not be empty.');
    const parsed = task.map((id) => requireVerificationTask(id).split(':'));
    const project = parsed[0]?.[0];
    if (!project || parsed.some((parts) => parts.length !== 2 || parts[0] !== project)) {
      throw new Error('An Nx task batch must contain unconfigured targets from one project.');
    }
    selection = ['run-many', `--projects=${project}`, `--targets=${parsed.map((parts) => parts[1]).join(',')}`];
  }
  return [
    ...selection,
    `--parallel=${parallel}`,
    '--outputStyle=static',
  ];
}

function commands(configuration: TargetConfiguration<CommandOptions> | undefined): string[] {
  const options = configuration?.options;
  return [
    ...(options?.command === undefined ? [] : [options.command]),
    ...(options?.commands ?? []).map((command) => typeof command === 'string' ? command : command.command),
  ];
}

function shardKey(task: string): string {
  return task.replace(/[^A-Za-z0-9_.-]/gu, '-');
}

function sameSet(left: ReadonlySet<string>, right: ReadonlySet<string>): boolean {
  return left.size === right.size && [...left].every((id) => right.has(id));
}

if (process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const workspace = fileURLToPath(new URL('../../../', import.meta.url));
    const directory = path.join(workspace, '.temp', 'ci');
    mkdirSync(directory, { recursive: true });
    const graphFile = path.join(directory, 'verification-graph.json');
    runChecked(new SpawnCommandRunner(), 'npx', [
      '--no-install', 'nx', 'run-many', '-t', 'verify', '--all', `--graph=${graphFile}`,
    ], { cwd: workspace });
    const plan = createVerificationPlan(readJsonFile<VerificationGraph>(graphFile));
    const output = process.env.GITHUB_OUTPUT;
    if (!output) throw new Error('GITHUB_OUTPUT must be set.');
    appendFileSync(output, [
      `node_matrix=${JSON.stringify({ include: plan.node })}`,
      `rust_matrix=${JSON.stringify({ include: plan.rust })}`,
      `image_matrix=${JSON.stringify({ include: plan.images })}`,
      `image_refs=${JSON.stringify(plan.images.flatMap(({ image_refs }) => image_refs))}`,
      `reader=${plan.reader}`,
      `integration=${plan.integration}`,
      '',
    ].join('\n'));
    console.log(`CI: ${plan.materialTasks.length} material checks, ${plan.node.length} Node shards, ${plan.rust.length} Rust shards, ${plan.images.length} image builds, reader, and E2E.`);
  } catch (error) {
    console.error(`ci-plan: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}
