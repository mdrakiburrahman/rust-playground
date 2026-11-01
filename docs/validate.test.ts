import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  headingAnchors, loadCargoPackages, loadNxProjects, markdownLinks, shellWords,
  staticPathTokens, validateCommands, validateLink, validateMarkdown,
} from './validate.js';

function checkout(parent: string): string {
  const root = join(parent, 'checkout');
  const project = (directory: string, name: string, target: string): void => {
    mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, 'project.json'), JSON.stringify({ name, targets: { [target]: {} } }));
  };
  const pkg = (directory: string, name: string): void => {
    mkdirSync(directory, { recursive: true });
    writeFileSync(join(directory, 'Cargo.toml'), `[package]\nname = "${name}"\nversion = "0.1.0"\n`);
  };
  project(root, 'fixture', 'verify');
  project(join(root, '.devcontainer'), 'devcontainer', 'test-source');
  pkg(root, 'fixture-package');
  pkg(join(root, 'crates/child'), 'fixture-child');
  for (const excluded of ['node_modules', 'target', 'submodules', 'onelake', '.temp']) {
    const directory = join(root, excluded, 'ignored');
    project(directory, `ignored-${excluded}`, 'verify');
    if (excluded !== 'node_modules') pkg(directory, `ignored-${excluded}`);
  }
  return root;
}

function withCheckout(action: (root: string) => void): void {
  const directory = mkdtempSync(join(tmpdir(), 'docs-validation-'));
  try {
    for (const ancestor of ['plain', '.temp']) action(checkout(join(directory, ancestor)));
  } finally { rmSync(directory, { recursive: true, force: true }); }
}

test('ancestor .temp preserves inventory and nested runtime exclusions', () => {
  withCheckout((root) => {
    assert.deepEqual(loadNxProjects(root), new Map([
      ['devcontainer', new Set(['test-source'])], ['fixture', new Set(['verify'])],
    ]));
    assert.deepEqual(loadCargoPackages(root), new Set(['fixture-package', 'fixture-child']));
  });
});

test('known targets and packages resolve under both checkout paths', () => {
  withCheckout((root) => {
    const text = '```bash\nnpx nx run devcontainer:test-source\nnpx nx run fixture:verify\ncargo test --package fixture-package\n```\n';
    assert.deepEqual(validateCommands(join(root, 'README.md'), text, loadNxProjects(root), loadCargoPackages(root), root), [2, 1, 0]);
  });
});

test('unknown Nx target still fails', () => {
  withCheckout((root) => assert.throws(() =>
    validateCommands(join(root, 'README.md'), '```bash\nnpx nx run devcontainer:missing\n```\n', loadNxProjects(root), loadCargoPackages(root), root),
  /unknown Nx target devcontainer:missing/));
});

test('unknown Cargo package still fails', () => {
  withCheckout((root) => assert.throws(() =>
    validateCommands(join(root, 'README.md'), '```bash\ncargo test --package missing\n```\n', loadNxProjects(root), loadCargoPackages(root), root),
  /unknown Cargo package missing/));
});

test('run-many validates each requested project and counts all targets', () => {
  withCheckout((root) => {
    const text = '```bash\nnpx --no-install nx run-many --projects fixture,devcontainer --target verify\n```\n';
    const projects = loadNxProjects(root);
    assert.throws(() => validateCommands(join(root, 'README.md'), text, projects, loadCargoPackages(root), root),
      /unknown Nx target devcontainer:verify/);
    projects.get('devcontainer')!.add('verify');
    assert.deepEqual(validateCommands(join(root, 'README.md'), text, projects, loadCargoPackages(root), root), [2, 0, 0]);
  });
});

test('malformed project manifests are errors rather than silently missing inventory', () => {
  withCheckout((root) => {
    writeFileSync(join(root, 'project.json'), '{"name":');
    assert.throws(() => loadNxProjects(root));
    writeFileSync(join(root, 'Cargo.toml'), '[package\nname = "broken"');
    assert.throws(() => loadCargoPackages(root));
  });
});

test('private env notes neither require secrets nor hide missing sources', () => {
  withCheckout((root) => {
    for (const text of ['Never commit `.devcontainer/.env`.\n', '```bash\ncat .devcontainer/.env\n```\n']) {
      assert.deepEqual(validateCommands(join(root, 'README.md'), text, loadNxProjects(root), loadCargoPackages(root), root), [0, 0, 0]);
    }
    for (const source of ['.devcontainer/.env.example', 'projects/missing.yaml']) {
      assert.deepEqual(staticPathTokens(`\`${source}\``), new Set([source]));
      assert.throws(() => validateCommands(join(root, 'README.md'), `\`${source}\``, loadNxProjects(root), loadCargoPackages(root), root), /missing documented path/);
    }
  });
});

test('shell tokenization handles quotes, escapes, comments and malformed examples', () => {
  assert.deepEqual(shellWords('cat "docs/a b.md" docs/c\\ d.md # ignore\nls \'tools/e\''),
    ['cat', 'docs/a b.md', 'docs/c d.md', 'ls', 'tools/e']);
  assert.deepEqual(shellWords('echo "a\\qb" ""'), ['echo', 'a\\qb', '']);
  assert.throws(() => shellWords('cat "docs/a'), /unclosed quote/);
  assert.throws(() => shellWords('cat \\'), /dangling escape/);
});

test('heading anchors handle duplicates, Unicode and fenced examples', () => {
  assert.deepEqual(headingAnchors('# Hello!\n## Hello!\n```\n# Hidden\n```\n## Caf\u00e9\n'), new Set(['hello', 'hello-1', 'caf\u00e9']));
});

test('relative links and anchors validate without checking external URLs or image links', () => {
  assert.deepEqual(markdownLinks('[A](other.md#hello) ![image](missing.png) [Web](https://example.com) [Encoded](a%20b.md)'),
    [['other.md', 'hello'], ['a b.md', undefined]]);
  withCheckout((root) => {
    const source = join(root, 'README.md');
    writeFileSync(source, '# Hello\n');
    validateLink(source, '', 'hello', root);
    assert.throws(() => validateLink(source, '', 'missing', root), /missing anchor/);
    assert.throws(() => validateLink(source, 'missing.md', undefined, root), /missing link target/);
  });
});

test('Markdown formatting and public reference checks still fail closed', () => {
  validateMarkdown('README.md', '# Title\n\n## Subheading\n');
  for (const [text, message] of [
    ['# Title', 'missing final newline'], ['# Title \n', 'trailing whitespace'],
    ['# T\title\n', 'tab character'], ['# Title\n### Skip\n', 'skipped heading level'],
    ['# Title\n```bash\n', 'unclosed fenced code block'],
    ['# Title\nhttps://github.com/o/r/pull/42\n', 'prohibited pull request URL'],
    ['# Title\n/' + 'workspaces/repo\n', 'prohibited absolute workspace path'],
  ]) assert.throws(() => validateMarkdown('README.md', text), new RegExp(message));
});
