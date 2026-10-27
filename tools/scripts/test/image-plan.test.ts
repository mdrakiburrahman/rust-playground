import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import type { ProjectConfiguration } from '@nx/devkit';
import { parse } from 'yaml';

import {
  createImagePublicationPlan,
  PRODUCTION_RUNTIME_IMAGE_PROJECTS,
  PUBLISHED_RUNTIME_IMAGE_PROJECTS,
  RETAINED_RUNTIME_IMAGE_PROJECTS,
} from '../src/image-plan.js';

const workspace = fileURLToPath(new URL('../../../', import.meta.url));

function projectPath(project: string): string {
  return project === 'hello-world'
    ? 'bin/hello-world/project.json'
    : `projects/${project}/project.json`;
}

function workspaceProjects(): Record<string, ProjectConfiguration> {
  const projects: Record<string, ProjectConfiguration> = {};
  for (const project of PUBLISHED_RUNTIME_IMAGE_PROJECTS) {
    projects[project] = JSON.parse(
      readFileSync(join(workspace, projectPath(project)), 'utf8'),
    ) as ProjectConfiguration;
  }
  projects.devcontainer = JSON.parse(
    readFileSync(
      join(workspace, '.devcontainer/project.json'),
      'utf8',
    ),
  ) as ProjectConfiguration;
  return projects;
}

test('discovers production images and retains the sample publication', () => {
  assert.deepEqual(PRODUCTION_RUNTIME_IMAGE_PROJECTS, [
    'otelcol-rust',
    'arrowcol-rust',
    'arrowcol-dataflow-rust',
    'arrowcol-router-rust',
    'kusto-bridge',
    'kusto-explorer',
  ]);
  assert.deepEqual(RETAINED_RUNTIME_IMAGE_PROJECTS, ['hello-world']);

  const plan = createImagePublicationPlan(workspaceProjects());
  assert.deepEqual(
    plan.map(({ project }) => project),
    [...PUBLISHED_RUNTIME_IMAGE_PROJECTS],
  );
  for (const entry of plan) {
    assert.equal(
      entry.reference,
      `ghcr.io/mdrakiburrahman/rust-playground/${entry.project}`,
    );
    assert.equal(
      entry.manifest,
      `target/images/${entry.project}-publish.txt`,
    );
  }
});

test('fails closed when a source-built publication is missing or unexpected', () => {
  const missing = workspaceProjects();
  delete missing['arrowcol-router-rust'];
  assert.throws(
    () => createImagePublicationPlan(missing),
    /must be exactly/u,
  );

  const unexpected = workspaceProjects();
  unexpected.extra = {
    name: 'extra',
    root: 'extra',
    targets: {
      publish: {
        executor: 'rust-playground-build:docker-build',
      },
    },
  };
  assert.throws(
    () => createImagePublicationPlan(unexpected),
    /discovered .*extra/u,
  );
});

// Collector images compile the otel-arrow protobuf crates and bake read-only configuration files.
const COLLECTOR_IMAGE_PROJECTS = [
  'otelcol-rust',
  'arrowcol-rust',
  'arrowcol-dataflow-rust',
  'arrowcol-router-rust',
];
// Web images compile a wasm UI and serve read-only static assets instead of configuration files.
const WEB_IMAGE_PROJECTS = ['kusto-explorer'];
const SERVICE_IMAGE_PROJECTS = ['kusto-bridge'];

test('every production runtime image is classified for Dockerfile policy', () => {
  assert.deepEqual(
    [...COLLECTOR_IMAGE_PROJECTS, ...WEB_IMAGE_PROJECTS, ...SERVICE_IMAGE_PROJECTS].sort(),
    [...PRODUCTION_RUNTIME_IMAGE_PROJECTS].sort(),
  );
});

test('runtime Dockerfiles pin build and runtime bases and enforce production boundaries', () => {
  for (const project of PRODUCTION_RUNTIME_IMAGE_PROJECTS) {
    const dockerfile = readFileSync(
      join(workspace, `projects/${project}/Dockerfile`),
      'utf8',
    );
    assert.match(
      dockerfile,
      /FROM rust:1\.96\.1-bookworm@sha256:[a-f0-9]{64} AS builder/u,
      project,
    );
    assert.match(
      dockerfile,
      /FROM debian:bookworm-slim@sha256:[a-f0-9]{64} AS runtime/u,
      project,
    );
    for (const from of dockerfile.match(/^FROM [^\n]*/gmu) ?? []) {
      assert.match(from, /^FROM \S+@sha256:[a-f0-9]{64} AS \S+$/u, project);
    }
    assert.match(
      dockerfile,
      /COPY Cargo\.toml Cargo\.lock rust-toolchain\.toml/u,
      project,
    );
    assert.match(dockerfile, /COPY submodules\/otel-arrow/u, project);
    assert.match(dockerfile, /ca-certificates/u, project);
    if (!SERVICE_IMAGE_PROJECTS.includes(project)) {
      assert.match(dockerfile, /ca-certificates curl/u, project);
    }
    assert.match(
      dockerfile,
      /org\.opencontainers\.image\.source/u,
      project,
    );
    assert.match(
      dockerfile,
      /org\.opencontainers\.image\.revision="\$\{VCS_REF\}"/u,
      project,
    );
    assert.match(dockerfile, /USER 10001:10001/u, project);
    assert.match(dockerfile, /HEALTHCHECK/u, project);
    assert.doesNotMatch(dockerfile, /FROM [^\n]*:latest/u, project);

    if (COLLECTOR_IMAGE_PROJECTS.includes(project)) {
      assert.match(dockerfile, /cmake protobuf-compiler/u, project);
      assert.match(dockerfile, /COPY --chmod=0444/u, project);
    } else if (WEB_IMAGE_PROJECTS.includes(project)) {
      assert.match(dockerfile, /rustup target add wasm32-unknown-unknown/u, project);
      assert.match(
        dockerfile,
        /COPY Cargo\.lock [^\n]*\n[^\n]*name = "wasm-bindgen"/u,
        `${project} must derive the wasm-bindgen CLI version from Cargo.lock`,
      );
      assert.match(dockerfile, /find \/out\/www -type d -exec chmod 0555/u, project);
      assert.match(dockerfile, /find \/out\/www -type f -exec chmod 0444/u, project);
      assert.match(dockerfile, /COPY --from=builder --chmod=0555/u, project);
    } else {
      assert.match(dockerfile, /install -D --mode=0555/u, project);
      assert.match(dockerfile, /COPY --from=builder/u, project);
    }
  }
});

test('Explorer context override retains root exclusions before browser-only exclusions', () => {
  const patterns = (file: string): string[] => readFileSync(join(workspace, file), 'utf8')
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith('#'));
  const root = patterns('.dockerignore');
  const explorer = patterns('projects/kusto-explorer/Dockerfile.dockerignore');
  assert.deepEqual(explorer.slice(0, root.length), root);
});

interface PublishWorkflow {
  concurrency?: {
    'cancel-in-progress'?: boolean;
    group?: string;
  };
  jobs: Record<string, {
    needs?: string;
    permissions?: Record<string, string>;
    strategy?: {
      'fail-fast'?: boolean;
      matrix?: string;
    };
    steps?: Array<{
      name?: string;
      run?: string;
    }>;
  }>;
  on: {
    push: {
      paths: string[];
    };
  };
  permissions?: Record<string, string>;
}

test('publish workflow discovers, smokes, publishes, and verifies the matrix with least privilege', () => {
  const workflow = parse(
    readFileSync(
      join(workspace, '.github/workflows/publish-images.yml'),
      'utf8',
    ),
  ) as PublishWorkflow;
  assert.deepEqual(workflow.permissions, { contents: 'read' });
  assert.equal(workflow.concurrency?.['cancel-in-progress'], true);
  assert.equal(workflow.jobs.publish?.needs, 'plan');
  assert.equal(workflow.jobs.publish?.permissions?.contents, 'read');
  assert.equal(workflow.jobs.publish?.permissions?.packages, 'write');
  assert.equal(workflow.jobs.publish?.strategy?.['fail-fast'], false);
  assert.match(
    workflow.jobs.publish?.strategy?.matrix ?? '',
    /fromJSON\(needs\.plan\.outputs\.matrix\)/u,
  );

  const steps = workflow.jobs.publish?.steps ?? [];
  const commands = steps
    .map(({ run }) => run)
    .filter((run): run is string => run !== undefined)
    .join('\n');
  assert.match(commands, /:image:ci-verify/u);
  assert.match(commands, /:image-smoke.*--excludeTaskDependencies/u);
  assert.match(commands, /:publish:main/u);
  assert.match(
    commands,
    /tsx tools\/scripts\/src\/registry\.ts tags/u,
  );
  assert.match(commands, /registry-manifest/u);
  assert.match(commands, /publication manifest mismatch/u);

  const paths = new Set(workflow.on.push.paths);
  for (const project of PRODUCTION_RUNTIME_IMAGE_PROJECTS) {
    assert(paths.has(`projects/${project}/**`), project);
  }
  assert(paths.has('bin/hello-world/**'));
  assert(paths.has('crates/**'));
  assert(paths.has('infra/docker/**'));
  assert(paths.has('submodules/otel-arrow'));
  assert(paths.has('submodules/otel-arrow/**'));
  assert(paths.has('tools/libs/rust-playground-build/**'));
  assert(paths.has('tools/scripts/**'));
});
