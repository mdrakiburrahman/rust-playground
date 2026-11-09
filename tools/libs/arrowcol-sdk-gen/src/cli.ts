#!/usr/bin/env node

import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { SdkGenerationTool } from './index.js';

type Command =
  | 'generate-fixture'
  | 'generate'
  | 'check'
  | 'compatibility'
  | 'test'
  | 'package-artifact';

export function runCli(
  args: readonly string[],
  tool: SdkGenerationTool = new SdkGenerationTool(),
): readonly string[] {
  if (args[0] === 'generate') {
    return tool.generate(args.slice(1)).commands;
  }
  if (args.length !== 1) {
    throw new Error(
      'Usage: cli.ts <generate-fixture|generate|check|compatibility|test|package-artifact>',
    );
  }
  const command = args[0] as Command;
  switch (command) {
    case 'generate-fixture':
      return tool.generateFixture().commands;
    case 'check':
      return tool.check().commands;
    case 'compatibility':
      return tool.compatibility().commands;
    case 'test':
      return tool.testGenerated().commands;
    case 'package-artifact':
      return tool.packageArtifact().commands;
    default:
      throw new Error(`Unknown arrowcol SDK workflow: ${String(command)}`);
  }
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(resolve(process.argv[1])).href
) {
  try {
    const commands = runCli(process.argv.slice(2));
    console.log(`Completed ${commands.length} SDK workflow command(s).`);
  } catch (error) {
    process.stderr.write(
      `arrowcol-sdk-gen-tool: ${
        error instanceof Error ? error.message : String(error)
      }\n`,
    );
    process.exitCode = 1;
  }
}
