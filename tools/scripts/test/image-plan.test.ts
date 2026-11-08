import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync, type SpawnSyncReturns } from 'node:child_process';
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
  if (project === 'delta-maintenance' || project === 'arrow-tenant-stream') {
    return `crates/${project}/project.json`;
  }
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
    'arrowcol-dataflow-rust',
    'arrowcol-router-rust',
    'arrowcol-catalog-rust',
    'delta-maintenance',
    'arrow-tenant-stream',
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

test('every publication and the retained sample build bind to the Git revision', () => {
  const projects = workspaceProjects();
  for (const project of PUBLISHED_RUNTIME_IMAGE_PROJECTS) {
    const dockerfile = readFileSync(join(workspace, projectPath(project).replace(/project\.json$/u, 'Dockerfile')), 'utf8');
    assert.match(dockerfile, /^ARG VCS_REF=local$/mu, project);
    assert.match(dockerfile, /org\.opencontainers\.image\.revision="\$\{VCS_REF\}"/u, project);
    for (const target of project === 'hello-world' ? ['image', 'publish'] : ['publish']) {
      assert.deepEqual(projects[project].targets?.[target]?.options?.buildArgs, ['VCS_REF={gitSha}'], `${project}:${target}`);
      const missing = workspaceProjects();
      delete missing[project].targets![target].options!.buildArgs;
      assert.throws(() => createImagePublicationPlan(missing), /OCI revision build argument/u, `${project}:${target}`);
      const stale = workspaceProjects();
      stale[project].targets![target].options!.buildArgs = ['VCS_REF=local'];
      assert.throws(() => createImagePublicationPlan(stale), /OCI revision build argument/u, `${project}:${target}`);
    }
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

test('all loadtest images retain local build behavior with clean Git revision metadata', () => {
  const expected = [
    'arrowcol-dataflow-rust', 'arrowcol-router-rust',
    'arrowcol-catalog-rust', 'kusto-explorer', 'arrow-tenant-stream', 'delta-maintenance',
  ];
  const loadtest = JSON.parse(readFileSync(join(workspace, 'projects/loadtest/project.json'), 'utf8')) as ProjectConfiguration;
  const images = loadtest.targets?.build?.dependsOn?.filter(
    dependency => typeof dependency === 'object' && dependency.target === 'image',
  );
  assert.deepEqual(images, [{ projects: expected, target: 'image' }]);
  const projects = workspaceProjects();
  for (const project of expected) {
    const target = projects[project].targets?.image;
    assert.equal(target?.executor, 'rust-playground-build:docker-build', project);
    assert.equal(target?.cache, false, project);
    const options = target?.options;
    assert.equal(options?.file, '{absProjectRoot}/Dockerfile', project);
    assert.equal(options?.context, '{absWorkspaceRoot}', project);
    assert.equal(options?.image, `ghcr.io/mdrakiburrahman/rust-playground/${project}`, project);
    assert.deepEqual(options?.tags, ['dev'], project);
    assert.deepEqual(options?.platforms, ['linux/amd64'], project);
    assert.equal(options?.output, 'load', project);
    assert.equal(options?.manifestFile, `{absWorkspaceRoot}/target/images/${project}-load.txt`, project);
    assert.deepEqual(options?.buildArgs, ['VCS_REF={gitSha}'], project);
    assert.equal(options?.requireCleanWorktree, true, project);
    const ci = target?.configurations?.['ci-verify'];
    assert.equal(ci?.buildArgs, undefined, project);
    assert.equal(ci?.requireCleanWorktree, undefined, project);
    const dockerfile = readFileSync(join(workspace, projectPath(project).replace(/project\.json$/u, 'Dockerfile')), 'utf8');
    assert.match(dockerfile, /^ARG VCS_REF=local$/mu, project);
    assert.match(dockerfile, /org\.opencontainers\.image\.revision="\$\{VCS_REF\}"/u, project);
  }
});

// Collector images compile the otel-arrow protobuf crates and bake read-only configuration files.
const COLLECTOR_IMAGE_PROJECTS = [
  'otelcol-rust',
  'arrowcol-dataflow-rust',
  'arrowcol-router-rust',
];
// Web images compile a wasm UI and serve read-only static assets instead of configuration files.
const WEB_IMAGE_PROJECTS = ['kusto-explorer'];
const SERVICE_IMAGE_PROJECTS = ['delta-maintenance', 'arrow-tenant-stream', 'arrowcol-catalog-rust'];

test('every production runtime image is classified for Dockerfile policy', () => {
  assert.deepEqual(
    [...COLLECTOR_IMAGE_PROJECTS, ...WEB_IMAGE_PROJECTS, ...SERVICE_IMAGE_PROJECTS].sort(),
    [...PRODUCTION_RUNTIME_IMAGE_PROJECTS].sort(),
  );
});

test('runtime Dockerfiles pin build and runtime bases and enforce production boundaries', () => {
  for (const project of PRODUCTION_RUNTIME_IMAGE_PROJECTS) {
    const dockerfile = readFileSync(
      join(workspace, projectPath(project).replace(/project\.json$/u, 'Dockerfile')),
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
    if (dockerfile.includes('sh /tmp/install-azure-cli.sh') || dockerfile.includes('sh /usr/local/share/install-azure-cli.sh')) {
      assert.match(
        dockerfile,
        /COPY(?: --chmod=\d+)? infra\/docker\/install-azure-cli\.sh \/(?:tmp|usr\/local\/share)\/install-azure-cli\.sh/u,
        project,
      );
    } else {
      assert.match(dockerfile, /ca-certificates/u, project);
      if (!SERVICE_IMAGE_PROJECTS.includes(project)) {
        assert.match(dockerfile, /ca-certificates curl/u, project);
      }
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
      assert.match(dockerfile, /(?:install (?:-D )?--mode=0555|COPY --from=builder --chmod=0555)/u, project);
      assert.match(dockerfile, /COPY --from=builder/u, project);
    }
  }
});

test('Kernel consumer lint caches include their library dependencies', () => {
  const projects = workspaceProjects();
  for (const project of ['delta-maintenance', 'arrow-tenant-stream', 'arrowcol-catalog-rust']) {
    const config = projects[project];
    assert(config, project);
    assert(config.implicitDependencies?.includes('arrow-deltalake'), project);
    assert.deepEqual(config.targets?.lint?.inputs, ['default', '^default'], project);
  }
});

test('Azure writer images install the shared CLI and reap orphan descendants', () => {
  for (const project of ['arrowcol-dataflow-rust', 'delta-maintenance', 'arrow-tenant-stream', 'arrowcol-catalog-rust']) {
    const dockerfile = readFileSync(
      join(workspace, projectPath(project).replace(/project\.json$/u, 'Dockerfile')),
      'utf8',
    );
    assert.match(dockerfile, /sh \/(?:tmp|usr\/local\/share)\/install-azure-cli\.sh/u, project);
    assert.match(
      dockerfile,
      /COPY(?: --chmod=\d+)? infra\/docker\/install-azure-cli\.sh \/(?:tmp|usr\/local\/share)\/install-azure-cli\.sh/u,
      project,
    );
    assert.match(dockerfile, /ENTRYPOINT \[\s*"\/usr\/bin\/tini"/u, project);
  }
});

test('shared CLI helper installs TLS prerequisites through a signed pinned repository', () => {
  const helper = readFileSync(join(workspace, 'infra/docker/install-azure-cli.sh'), 'utf8');
  assert.match(helper, /apt-get install -y --no-install-recommends ca-certificates curl gpg/u);
  assert.match(helper, /sha256sum --check --strict -/u);
  assert.match(helper, /signed-by=\/usr\/share\/keyrings\/arrowcol-azure-cli\.gpg/u);
  assert.match(helper, /apt-get install -y --no-install-recommends azure-cli=\d+\.\d+\.\d+-1~bookworm/u);
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
    if?: string;
    needs?: string;
    permissions?: Record<string, string>;
    strategy?: {
      'fail-fast'?: boolean;
      matrix?: string;
    };
    steps?: Array<{
      if?: string;
      env?: Record<string, string>;
      name?: string;
      run?: string;
    }>;
  }>;
  on: {
    push: {
      branches: string[];
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
    const source = projectPath(project).startsWith('crates/')
      ? 'crates/**'
      : `projects/${project}/**`;
    assert(paths.has(source), project);
  }
  assert(paths.has('bin/hello-world/**'));
  assert(paths.has('crates/**'));
  assert(paths.has('infra/docker/**'));
  assert(paths.has('submodules/otel-arrow'));
  assert(paths.has('submodules/otel-arrow/**'));
  assert(paths.has('tools/libs/rust-playground-build/**'));
  assert(paths.has('tools/scripts/**'));
});

test('catalog image stays Rust-only with an unprivileged healthchecked runtime', () => {
  const config = workspaceProjects()['arrowcol-catalog-rust'];
  assert(config.implicitDependencies?.includes('arrowcol-catalog'));
  assert.deepEqual(config.targets?.verify?.dependsOn, ['format-check', 'lint', 'build', 'test']);
  const dockerfile = readFileSync(join(workspace, 'projects/arrowcol-catalog-rust/Dockerfile'), 'utf8');
  assert.match(dockerfile, /--package arrowcol-catalog-rust/u);
  assert.match(dockerfile, /EXPOSE 8817/u);
  assert(dockerfile.includes('CMD ["/usr/local/bin/arrowcol-catalog-rust", "--healthcheck"]'));
  assert(dockerfile.includes('ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/arrowcol-catalog-rust"]'));
  assert.doesNotMatch(dockerfile, /nodejs/u);
  assert.match(dockerfile, /ENV AZURE_CONFIG_DIR=\/var\/lib\/arrowcol-catalog-azure/u);
  assert.match(dockerfile, /install -d --owner app --group app --mode 0700 \/var\/lib\/arrowcol-catalog-azure \/data\/catalog \/data\/warehouse/u);
  for (const target of ['image', 'publish']) {
    assert(config.targets?.[target]?.inputs?.includes('{workspaceRoot}/infra/docker/install-azure-cli.sh'), target);
  }
  const smokeCommands = config.targets?.['image-smoke']?.options?.commands as string[];
  assert(smokeCommands.some(command => command.includes('az version') && command.includes('test -z')));
  assert(smokeCommands.some(command => command.includes('docker exec') && command.includes('--healthcheck')));
  const root = JSON.parse(readFileSync(join(workspace, 'project.json'), 'utf8')) as ProjectConfiguration;
  assert(root.implicitDependencies?.includes('arrowcol-catalog-rust'));
  for (const target of ['clean', 'format', 'format-check', 'lint', 'build', 'test']) {
    assert(root.targets?.[target]?.dependsOn?.some(dependency =>
      typeof dependency === 'object' && dependency.target === target &&
      Array.isArray(dependency.projects) && dependency.projects.includes('arrowcol-catalog-rust')
    ), target);
  }
});

test('integration previews push only immutable SHA tags without release aliases or PR credentials', () => {
  const workflow = parse(readFileSync(join(workspace, '.github/workflows/publish-images.yml'), 'utf8')) as PublishWorkflow;
  assert.deepEqual(workflow.on.push.branches, ['main', 'mdrakiburrahman-schema-catalog']);
  assert(!('pull_request' in workflow.on));
  assert(!('pull_request_target' in workflow.on));
  assert.equal(workflow.jobs.publish?.if, "github.event_name == 'push' || github.event_name == 'workflow_dispatch'");
  const steps = workflow.jobs.publish?.steps ?? [];
  const byName = (name: string) => {
    const step = steps.find(step => step.name === name);
    assert(step, name);
    return step;
  };
  const preview = byName('Publish immutable preview SHA only');
  assert.equal(preview.if, "github.ref == 'refs/heads/mdrakiburrahman-schema-catalog'");
  assert.equal(preview.run, 'npx --no-install nx run "$IMAGE_PROJECT:publish" --tags="sha-${REGISTRY_SHA}"');
  assert.doesNotMatch(preview.run ?? '', /latest|stable|:main/u);
  assert.equal(byName('Publish branch and SHA tags').if, "github.ref != 'refs/heads/main' && github.ref != 'refs/heads/mdrakiburrahman-schema-catalog'");
  assert.equal(byName('Publish branch, SHA, and latest tags').if, "github.ref == 'refs/heads/main'");
  assert.equal(byName('Verify latest manifest').if, "github.ref == 'refs/heads/main'");
  assert.equal(byName('Verify branch manifest').if, "github.ref != 'refs/heads/mdrakiburrahman-schema-catalog'");
  const metadata = byName('Resolve branch and immutable tags');
  assert.equal(metadata.env?.REGISTRY_PREVIEW, "${{ github.ref == 'refs/heads/mdrakiburrahman-schema-catalog' }}");
  assert.match(metadata.run ?? '', /if \(process\.env\.REGISTRY_PREVIEW === "true"\) \{\s+tags\.values = \[tags\.shaTag\];\s+writeFileSync/u);
});


test('catalog smoke requires default SIGTERM, exit zero and the publisher close marker', () => {
  const config = workspaceProjects()['arrowcol-catalog-rust'];
  const commands = config.targets?.['image-smoke']?.options?.commands as string[];
  assert(commands.every(command => command.includes('CATALOG_IMAGE')));
  const command = commands.find(command => command.includes('docker stop --time 10'));
  assert(command);
  assert.doesNotMatch(command, /--stop-signal|--rm/u);
  assert(command.includes('docker wait'));
  assert(command.includes('docker rm --force'));
  const mock = [
    'docker() {',
    'case "$1" in',
    'run) echo smoke-container ;;',
    'exec) return 0 ;;',
    'stop) return "$SMOKE_STOP_STATUS" ;;',
    'wait) echo "$SMOKE_EXIT_CODE" ;;',
    'logs) echo "$SMOKE_LOG" ;;',
    'rm) return 0 ;;',
    '*) return 99 ;;',
    'esac',
    '}',
  ].join('\n');
  for (const [name, env, success] of [
    ['graceful close', {}, true],
    ['forced termination', { SMOKE_EXIT_CODE: '137' }, false],
    ['missing publisher marker', { SMOKE_LOG: 'listener stopped' }, false],
    ['stop failure', { SMOKE_STOP_STATUS: '1' }, false],
  ] as const) {
    const result: SpawnSyncReturns<string> = spawnSync('bash', ['-c', mock + '\n' + command], {
      encoding: 'utf8',
      env: { ...process.env, SMOKE_STOP_STATUS: '0', SMOKE_EXIT_CODE: '0', SMOKE_LOG: 'catalog publisher sessions drained and closed', ...env, CATALOG_IMAGE: 'ghcr.io/example/catalog@sha256:' + 'a'.repeat(64) },
      timeout: 5000,
    });
    assert.equal(result.status === 0, success, name + ': ' + result.stderr);
  }
});
