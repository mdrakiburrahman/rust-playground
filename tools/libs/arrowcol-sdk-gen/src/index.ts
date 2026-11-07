import { cpSync, mkdirSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';

import {
  SpawnCommandRunner,
  type CommandRunner,
  runChecked,
} from '../../../scripts/src/process.js';

export const fixtureDdl =
  'crates/arrowcol-sdk-gen/fixtures/telemetry-events.sql';
export const fixtureArtifacts =
  'crates/arrowcol-sdk-gen/.temp/catalog';
export const evolvedFixtureDdl =
  'crates/arrowcol-sdk-gen/fixtures/telemetry-events-v2.sql';
export const evolvedFixtureArtifacts =
  'crates/arrowcol-sdk-gen/.temp/catalog-v2';
export const evolvedGeneratedOutput =
  'crates/arrowcol-sdk-gen/.temp/generated-v2';
export const generatedOutput =
  'crates/arrowcol-sdk-gen/.temp/generated';
export const packageArchive =
  'crates/arrowcol-sdk-gen/.temp/arrowcol-sdk.tar.gz';
export const validationRoot =
  'target/arrowcol-sdk-gen-validation';
export const validationCargoTarget =
  'target/arrowcol-sdk-gen-validation-cargo-target';

export interface WorkflowResult {
  readonly commands: readonly string[];
}

export class SdkGenerationTool {
  readonly #runner: CommandRunner;
  readonly #workspace: string;

  constructor(
    workspace: string = process.cwd(),
    runner: CommandRunner = new SpawnCommandRunner(),
  ) {
    this.#workspace = resolve(workspace);
    this.#runner = runner;
  }

  generateFixture(): WorkflowResult {
    const commands = [
      this.#generator([
        'fixture',
        '--ddl',
        fixtureDdl,
        '--out',
        fixtureArtifacts,
      ]),
      this.#generator([
        'generate',
        '--artifacts',
        fixtureArtifacts,
        '--out',
        generatedOutput,
      ]),
      this.#generator([
        'fixture', '--ddl', evolvedFixtureDdl,
        '--previous', fixtureArtifacts, '--out', evolvedFixtureArtifacts,
      ]),
      this.#generator([
        'generate', '--artifacts', evolvedFixtureArtifacts,
        '--out', evolvedGeneratedOutput,
      ]),
    ];
    return { commands };
  }

  generate(): WorkflowResult {
    return {
      commands: [
        this.#generator([
          'generate',
          '--artifacts',
          fixtureArtifacts,
          '--out',
          generatedOutput,
        ]),
      ],
    };
  }

  check(): WorkflowResult {
    return {
      commands: [
        this.#generator([
          'check',
          '--artifacts',
          fixtureArtifacts,
          '--out',
          generatedOutput,
        ]),
      ],
    };
  }

  compatibility(): WorkflowResult {
    return {
      commands: [
        this.#generator([
          'compatibility',
          '--previous',
          fixtureArtifacts,
          '--current',
          evolvedFixtureArtifacts,
        ]),
        this.#command('npx', [
          '--no-install', 'buf', 'breaking',
          `${evolvedFixtureArtifacts}/schema.proto`,
          '--against', `${fixtureArtifacts}/schema.proto`,
          '--config', 'buf.yaml',
        ]),
      ],
    };
  }

  testGenerated(): WorkflowResult {
    const validation = resolve(this.#workspace, validationRoot);
    rmSync(validation, { force: true, recursive: true });
    mkdirSync(validation, { recursive: true });
    cpSync(resolve(this.#workspace, generatedOutput, 'rust'), resolve(validation, 'rust'), {
      recursive: true,
    });
    cpSync(
      resolve(this.#workspace, generatedOutput, 'typescript'),
      resolve(validation, 'typescript'),
      { recursive: true },
    );
    cpSync(
      resolve(this.#workspace, generatedOutput, 'descriptors'),
      resolve(validation, 'descriptors'),
      { recursive: true },
    );

    const rustManifest = `${validationRoot}/rust/Cargo.toml`;
    const typescriptConfig = `${validationRoot}/typescript/tsconfig.json`;
    const environment = {
      ...process.env,
      CARGO_TARGET_DIR: resolve(this.#workspace, validationCargoTarget),
    };
    const commands = [
      this.#command('cargo', [
        'fmt',
        '--manifest-path',
        rustManifest,
        '--',
        '--check',
      ]),
      this.#command(
        'cargo',
        [
          'clippy',
          '--manifest-path',
          rustManifest,
          '--all-targets',
          '--',
          '-D',
          'warnings',
        ],
        environment,
      ),
      this.#command(
        'cargo',
        ['test', '--manifest-path', rustManifest],
        environment,
      ),
      this.#command(
        'cargo',
        ['doc', '--manifest-path', rustManifest, '--no-deps'],
        { ...environment, RUSTDOCFLAGS: '-D warnings' },
      ),
      this.#command('npx', [
        '--no-install',
        'tsc',
        '--project',
        typescriptConfig,
        '--noEmit',
        '--noUnusedLocals',
        '--noUnusedParameters',
      ]),
      this.#command('npx', [
        '--no-install',
        'tsc',
        '--project',
        typescriptConfig,
        '--noEmit',
      ]),
      this.#command('npx', [
        '--no-install',
        'tsx',
        '--test',
        `${validationRoot}/typescript/test/generated.test.ts`,
      ]),
    ];
    return { commands };
  }

  packageArtifact(): WorkflowResult {
    return {
      commands: [
        this.#generator([
          'package-artifact',
          '--input',
          generatedOutput,
          '--out',
          packageArchive,
        ]),
      ],
    };
  }

  #generator(args: readonly string[]): string {
    return this.#command('cargo', [
      'run',
      '--locked',
      '--quiet',
      '--package',
      'arrowcol-sdk-gen',
      '--',
      ...args,
    ]);
  }

  #command(
    command: string,
    args: readonly string[],
    env: NodeJS.ProcessEnv = process.env,
  ): string {
    runChecked(this.#runner, command, args, {
      cwd: this.#workspace,
      env,
    });
    return [command, ...args].map((part) => JSON.stringify(part)).join(' ');
  }
}

export type { CommandRunner };
