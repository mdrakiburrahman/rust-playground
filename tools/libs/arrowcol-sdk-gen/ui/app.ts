import { pipelineSteps, type PipelineRun } from './run-pipeline.js';
interface Registry { id: string; label: string; endpoint: string; mode: string; authority: string; native?: { provider: string; endpoint: string } }
interface PropertyDefinition {
  key: string; option_name: string; optional: boolean; kind: string;
  enum_values: string[]; declared_default: string | null;
}
interface PropertyDefinitions { definitions_sha256: string; options: PropertyDefinition[] }
interface Table {
  table: string; tableId: string; version: number; fingerprint: string; location: string;
  properties: Record<string, string>;
  registrationReceipt?: { provider: string; native_table_id: string };
  schema: { fields: { name: string; nullable: boolean; field_number: number; mapping: { spark: string } }[] };
}
interface Run extends PipelineRun {
  id: string; registry: Registry; status: string; phase: string; registration: string;
  createdAt: string; startedAt?: string; finishedAt?: string;
  pin?: { table: string; version: number };
  error?: string; artifact?: { hostPath: string; sha256: string };
  packages?: { id: string; kind: string; language?: string; hostPath: string; sha256: string }[];
}
const element = <T extends HTMLElement>(id: string) => {
  const result = document.getElementById(id);
  if (!result) throw new Error(`Missing UI element: ${id}`);
  return result as T;
};
const escape = (text: unknown) => String(text ?? '').replace(/[&<>"']/g, (char) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]!);
function packageDownloads(run: Run): string {
  if (!run.packages?.length) return '';
  return `<details class="package-downloads"><summary>Individual contract and SDK packages (${run.packages.length})</summary>
    <p class="small">Apps reference two packages: their language SDK and their schema models.
    Models embed the neutral contract and do not depend on Arrowcol. SDK source is shared across schemas.
    Build native binaries for your OS/architecture; these are source distributions, not published package feeds.</p>
    ${run.packages.map((item) => `<div class="artifact">
      <a href="/api/runs/${run.id}/artifact?package=${encodeURIComponent(item.id)}">Download ${escape(item.kind === 'contract'
        ? 'language-neutral contract' : `${item.language} ${item.kind === 'sdk' ? 'Arrowcol SDK' : 'models + contract'}`)}</a>
      <input aria-label="${escape(item.id)} package path" readonly value="${escape(item.hostPath)}">
      <div class="mono">SHA-256 ${escape(item.sha256)}</div></div>`).join('')}</details>`;
}
let registries: Registry[] = [];
let tables: Table[] = [];
let runs: Run[] = [];
let selected = '';
let token = '';
let propertyDefinitions: PropertyDefinitions;
let lastRunSignature = '';
let tableSequence = 0;
const fabric = document.body.dataset.frontend === 'fabric';
const lakehouse = document.body.dataset.frontend === 'lakehouse';
const closedBranches = new Set<string>();
let lakehousePane = 'schema';
let workspaceFilter = '';
let favoritesOnly = false;
const favorites = new Set<string>();
const registrySelect = element<HTMLSelectElement>('registry');
const nativeChoices = [
  { id: 'native:unity', label: 'Unity Catalog' },
  { id: 'native:hive', label: 'Hive Metastore' },
];
const nativeOptions = () => nativeChoices.map((item) =>
  `<option value="${item.id}">${item.label} (native metadata inspector)</option>`).join('');
const unavailableNativeOptions = () => nativeChoices.filter((item) =>
  !registries.some((registry) => registry.native?.provider === item.id.slice(7)))
  .map((item) => `<option value="${item.id}">${item.label} (enable builds first)</option>`).join('');
const selectedNative = () => nativeChoices.find((item) => item.id === registrySelect.value);
const dialog = element<HTMLDialogElement>('create-dialog');
let inspectedRunId: string | undefined = location.hash.match(/^#run=([a-f0-9-]{36})$/)?.[1];
let inspectedPhase: string | undefined;
let inspectedDocument: string | undefined;
let inspectionSequence = 0;
const runDialog = document.createElement('dialog');
runDialog.className = 'run-inspector';
runDialog.setAttribute('aria-label', 'Workflow pipeline');
runDialog.innerHTML = `<div class="dialog-heading"><div><div class="eyebrow">LIVE NX PIPELINE</div><h2 id="pipeline-title"></h2></div>
  <button type="button" id="close-pipeline" aria-label="Close workflow pipeline">Close</button></div>
  <p id="pipeline-summary" class="small"></p><p id="pipeline-legacy" class="muted small" hidden>Older run: per-step timing was not recorded. Unrecorded steps are not shown as verified successes.</p>
  <div class="run-pipeline-layout"><ol id="pipeline-steps" aria-label="Workflow stages"></ol>
  <section class="pipeline-detail"><button id="follow-pipeline" class="text-button">Follow active step</button>
  <h3 id="pipeline-step-title"></h3><p id="pipeline-description"></p><div id="pipeline-attempts" class="small"></div>
  <label for="pipeline-document">Inspect input / output</label><select id="pipeline-document"></select>
  <p id="pipeline-output-name" class="small muted" role="status"></p><pre id="pipeline-output" aria-label="Step output"></pre>
  <div id="pipeline-artifact"></div></section></div>`;
document.body.append(runDialog);
element('close-pipeline').onclick = () => runDialog.close();
runDialog.onclose = () => {
  inspectedRunId = undefined;
  inspectionSequence++;
  history.replaceState(null, '', location.pathname + location.search);
};
window.addEventListener('hashchange', () => {
  const id = location.hash.match(/^#run=([a-f0-9-]{36})$/)?.[1];
  if (id && runs.some((run) => run.id === id)) openRunPipeline(id);
  else if (id) inspectedRunId = id;
  else if (runDialog.open) runDialog.close();
});
element('follow-pipeline').onclick = () => {
  inspectedPhase = undefined; inspectedDocument = undefined; renderRunPipeline(); handle(refreshRunOutput);
};
element('pipeline-document').onchange = () => {
  inspectedDocument = element<HTMLSelectElement>('pipeline-document').value; handle(refreshRunOutput);
};
function openRunPipeline(id: string): void {
  inspectedRunId = id; inspectedPhase = undefined; inspectedDocument = undefined;
  history.replaceState(null, '', `#run=${id}`);
  showTab('runs');
  renderRunPipeline();
  if (!runDialog.open) runDialog.showModal();
  handle(refreshRunOutput);
}
function renderRunPipeline(): void {
  const run = runs.find((item) => item.id === inspectedRunId);
  if (!run) return;
  const steps = pipelineSteps(run);
  const active = steps.find((step) => step.phase === (inspectedPhase ?? run.phase)) ??
    (run.status === 'queued' ? steps[0] : steps.at(-1)!);
  element('pipeline-title').textContent = run.pin?.table ?? run.request.table ?? 'New table contract';
  element('pipeline-summary').textContent = `${run.registry.label} (${run.registry.endpoint}) | ${run.status} | Registration: ${run.registration} | ${run.id}`;
  element('pipeline-legacy').hidden = !!run.steps && (!run.steps.length || run.steps[0].phase === 'validating');
  const focused = document.activeElement instanceof HTMLElement ? document.activeElement.dataset.phase : undefined;
  element('pipeline-steps').innerHTML = steps.map((step, index) => `<li class="pipeline-node ${escape(step.status)}">
    <button data-phase="${escape(step.phase)}" aria-current="${step.phase === active.phase ? 'step' : 'false'}">
    <span class="step-number">${index + 1}</span><span><strong>${escape(step.label)}</strong><small>${escape(step.status.replaceAll('-', ' '))}${step.startedAt ? ` · ${escape(new Date(step.startedAt).toLocaleTimeString())}` : ''}${step.finishedAt ? ` · ${((Date.parse(step.finishedAt) - Date.parse(step.startedAt!)) / 1000).toFixed(1)}s` : ''}</small></span></button></li>`).join('');
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-phase]')) {
    button.onclick = () => { inspectedPhase = button.dataset.phase; inspectedDocument = undefined; renderRunPipeline(); handle(refreshRunOutput); };
    if (focused === button.dataset.phase) button.focus();
  }
  element('pipeline-step-title').textContent = active.label;
  element('pipeline-description').textContent = active.skip
    ? run.request.mode === 'rebuild' && active.phase === 'promoting'
      ? 'Skipped: this run rebuilds an already registered version. No registration call is made.'
      : 'Skipped: this run uses the direct renderer, not the protoc backend.'
    : active.description;
  element('pipeline-attempts').innerHTML = active.attempts.map((attempt, index) =>
    `<p>Attempt ${index + 1}: ${escape(attempt.status)}${attempt.error ? `<span class="error"> — ${escape(attempt.error)}</span>` : ''}</p>`).join('') +
    (run.error && active.phase === run.phase && !active.attempts.length ? `<p class="error">${escape(run.error)}</p>` : '');
  const documents = [
    ['log', 'Live workflow logs'], ...(run.request.mode === 'create' ? [['ddl', 'Submitted DDL']] : []),
    ['schema', 'Canonical row .proto'], ['options', 'Property options .proto'], ['manifest', 'Schema manifest'],
    ...(run.request.codegen === 'protoc' ? [['compiler', 'Compiler entry .proto']] : []),
    ['sdk', 'Generated SDK manifest'], ['release', 'Release provenance'],
    ...(run.distributionVersion ? [['models', 'Language models manifest'], ['packages', 'Package digests']] : []),
    ...(run.registry.native ? [['registration', 'Native registration receipt']] : []),
  ];
  const select = element<HTMLSelectElement>('pipeline-document');
  select.innerHTML = documents.map(([value, name]) => `<option value="${value}">${name}</option>`).join('');
  select.value = inspectedDocument ?? (active.skip ? 'log' : active.document);
  element('pipeline-artifact').innerHTML = run.artifact
    ? `<a href="/api/runs/${run.id}/artifact">Download SDK + contract</a><p class="small">Complete developer source bundle${run.packages ? '; individual packages below' : ' (legacy generated adapters)'}.</p><p class="mono">${escape(run.artifact.hostPath)}</p><p class="mono">SHA-256 ${escape(run.artifact.sha256)}</p>${packageDownloads(run)}` : '';
}
async function refreshRunOutput(): Promise<void> {
  if (!runDialog.open || !inspectedRunId) return;
  const sequence = ++inspectionSequence;
  const kind = element<HTMLSelectElement>('pipeline-document').value;
  const url = `/api/runs/${inspectedRunId}/${kind === 'log' ? 'log' : `documents/${kind}`}`;
  try {
    const output = await api<{ text?: string; available?: boolean; name?: string; truncated?: boolean }>(url);
    if (sequence !== inspectionSequence || !runDialog.open) return;
    element('pipeline-output-name').textContent = kind === 'log' ? 'Live log tail · refreshes every 2 seconds'
      : `${output.name}${output.truncated ? ' (first 128 KiB)' : ''}`;
    element('pipeline-output').textContent = output.available === false
      ? 'Not produced yet. A failed or skipped stage may never produce this output.' : output.text || 'Waiting for workflow output...';
  } catch (error) {
    if (sequence !== inspectionSequence || !runDialog.open) return;
    element('pipeline-output-name').textContent = 'Could not load step output';
    element('pipeline-output').textContent = error instanceof Error ? error.message : String(error);
  }
}
function notice(text: string): void {
  element('notice').textContent = text;
  element('notice').hidden = !text;
}
const connectionButton = document.createElement('button');
connectionButton.type = 'button';
connectionButton.className = 'text-button';
connectionButton.id = 'inspect-connections';
connectionButton.textContent = 'Inspect connections';
registrySelect.insertAdjacentElement('afterend', connectionButton);
const connectionDialog = document.createElement('dialog');
connectionDialog.setAttribute('aria-label', 'Catalogue connections');
connectionDialog.innerHTML = `<div class="dialog-heading"><h2>Catalogue connections</h2><button id="close-connections">Close</button></div>
  <p class="small">Local and Hosted simulation are independent Rust catalogues with full SDK builds.
  Unity Catalog and Hive are optional independent native authorities. Enable their build connections to create contracts;
  the native metadata inspectors also browse foreign tables without importing them.</p>
  <label for="inspect-connection">Inspect catalogue connection</label><select id="inspect-connection"></select>
  <p id="connection-status" class="small"></p><div id="native-namespace" hidden><div class="identity-fields">
  <label>Native catalogue<input id="native-catalog" placeholder="Leave empty to list catalogues"></label>
  <label>Native schema / database<input id="native-schema" placeholder="Optional"></label>
  <label>Native table<input id="native-table" placeholder="Optional"></label></div>
  <p class="small muted">Read-only metadata inspection. For Hive, use catalogue <code>hive</code>.
  Fill parent names to list schemas/tables or inspect one table. This does not register schemas or query table data.</p></div>
  <button id="inspect-metadata" class="primary">Read metadata</button><pre id="connection-metadata" aria-label="Catalogue metadata"></pre>`;
document.body.append(connectionDialog);
element('close-connections').onclick = () => connectionDialog.close();
interface NativeConnection { provider: string; installed: boolean; running?: boolean; endpoint?: string; container?: string; note?: string }
let nativeConnections: NativeConnection[] = [];
let connectionSequence = 0;
let connectionLoad = 0;
connectionDialog.addEventListener('close', () => { connectionLoad++; connectionSequence++; });
connectionButton.onclick = () => handle(openConnections);
async function openConnections(): Promise<void> {
  const load = ++connectionLoad;
  connectionSequence++;
  if (!connectionDialog.open) connectionDialog.showModal();
  const select = element<HTMLSelectElement>('inspect-connection');
  select.disabled = true;
  element('connection-status').textContent = 'Loading connection status...';
  for (const part of ['catalog', 'schema', 'table']) element<HTMLInputElement>(`native-${part}`).value = '';
  try {
    const connections = await api<{ registries: Registry[]; native: NativeConnection[] }>('/api/connections');
    if (load !== connectionLoad || !connectionDialog.open) return;
    nativeConnections = connections.native;
    select.innerHTML = connections.registries.map((registry) => `<option value="${escape(registry.id)}">${escape(registry.label)} (${registry.native ? 'committed contracts' : 'Rust'})</option>`).join('') +
      nativeOptions();
    select.value = registrySelect.value;
    select.disabled = false;
    await inspectConnection();
  } catch (error) {
    if (load !== connectionLoad || !connectionDialog.open) return;
    select.disabled = false;
    element('connection-status').textContent = 'Could not load connections';
    element('connection-metadata').textContent = error instanceof Error ? error.message : String(error);
  }
}
async function inspectConnection(): Promise<void> {
  const sequence = ++connectionSequence;
  const value = element<HTMLSelectElement>('inspect-connection').value;
  const provider = value.startsWith('native:') ? value.slice(7) : undefined;
  element('native-namespace').hidden = !provider;
  const button = element<HTMLButtonElement>('inspect-metadata');
  button.disabled = true;
  element('connection-metadata').textContent = 'Reading catalogue metadata...';
  try {
    let url: string;
    if (provider) {
      const state = nativeConnections.find((item) => item.provider === provider);
      element('connection-status').textContent = state?.installed
        ? `${state.running ? 'Container running' : 'Stopped'} | ${state.endpoint} | ${state.container} | ${registries.some((registry) => registry.native?.provider === provider) ? 'Native builds enabled; select the authority in the main catalogue dropdown' : 'Enable builds with --action=enable-build, then restart catalog-ui'}`
        : `Not installed. Run: npx --no-install nx run arrowcol-sdk-gen:native-catalogues --action=up --provider=${provider}`;
      const query = new URLSearchParams();
      for (const part of ['catalog', 'schema', 'table']) {
        const name = element<HTMLInputElement>(`native-${part}`).value.trim();
        if (name) query.set(part, name);
      }
      url = `/api/native-catalogues/${provider}/inspect?${query}`;
    } else {
      const registry = registries.find((item) => item.id === value);
      element('connection-status').textContent = `${registry?.endpoint} | Authority ${registry?.authority} | Signed gRPC/Flight, not a browser endpoint. This backend performs the authenticated read.`;
      url = `/api/tables?registry=${encodeURIComponent(value)}`;
    }
    const metadata = await api<unknown>(url);
    if (sequence === connectionSequence) element('connection-metadata').textContent =
      metadata && typeof metadata === 'object' && 'text' in metadata && typeof metadata.text === 'string'
        ? metadata.text : JSON.stringify(metadata, null, 2);
  } catch (error) {
    if (sequence === connectionSequence) element('connection-metadata').textContent = `Inspection failed: ${error instanceof Error ? error.message : String(error)}`;
  } finally { if (sequence === connectionSequence) button.disabled = false; }
}
element('inspect-connection').onchange = () => {
  for (const part of ['catalog', 'schema', 'table']) element<HTMLInputElement>(`native-${part}`).value = '';
  handle(inspectConnection);
};
element('inspect-metadata').onclick = () => handle(inspectConnection);
async function api<T>(url: string, value?: unknown): Promise<T> {
  const response = await fetch(url, value === undefined ? undefined : {
    method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Contract-Token': token },
    body: JSON.stringify(value),
  });
  const result = await response.json();
  if (!response.ok) throw new Error(result.error ?? `HTTP ${response.status}`);
  return result;
}
function handle(action: () => Promise<void>): void {
  void action().catch((error: unknown) => notice(error instanceof Error ? error.message : String(error)));
}
function showTab(name: 'catalog' | 'runs'): void {
  element('catalog-view').hidden = name !== 'catalog';
  element('runs-view').hidden = name !== 'runs';
  element('catalog-tab').classList.toggle('selected', name === 'catalog');
  element('runs-tab').classList.toggle('selected', name === 'runs');
}
function renderTree(): void {
  const native = selectedNative();
  if (native) {
    const message = `<div class="empty"><h2>${native.label}</h2><p class="muted">Use Inspect connections to browse native metadata.</p><p class="muted small">Enable SDK builds in the devcontainer with native-catalogues --action=enable-build --provider=${native.id.slice(7)}, then restart catalog-ui.</p></div>`;
    element('tree').innerHTML = fabric ? message : '<p class="muted small">Native metadata inspection</p>';
    if (fabric) {
      element('workspace-list').replaceChildren();
      element('item-count').textContent = 'Native metadata · inspection only';
      element('scope-title').textContent = native.label;
    } else element('detail').innerHTML = message;
    return;
  }
  if (lakehouse) { renderLakehouseTree(); return; }
  if (fabric) { renderFabricCatalog(); return; }
  const filter = element<HTMLInputElement>('filter').value.toLowerCase();
  const visible = tables.filter((table) => table.table.toLowerCase().includes(filter));
  let html = '';
  let lastCatalog = '';
  let lastSchema = '';
  for (const table of visible) {
    const [catalog, schema, name] = table.table.split('.');
    if (catalog !== lastCatalog) { html += `<div class="tree-catalog">▾ ${escape(catalog)}</div>`; lastSchema = ''; }
    if (schema !== lastSchema) html += `<div class="tree-schema">▾ ${escape(schema)}</div>`;
    html += `<button class="tree-table ${selected === table.table ? 'selected' : ''}" data-table="${escape(table.table)}">▦ &nbsp;${escape(name)}</button>`;
    lastCatalog = catalog;
    lastSchema = schema;
  }
  element('tree').innerHTML = html || '<p class="muted small">No tables in this view.</p>';
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-table]')) {
    button.onclick = () => { selected = button.dataset.table!; renderTree(); renderDetail(); showTab('catalog'); };
  }
}
function compilerOptions(): { codegen?: 'protoc'; protoEntry?: string } {
  return (fabric || lakehouse) && element<HTMLSelectElement>('codegen').value === 'protoc'
    ? { codegen: 'protoc', protoEntry: element<HTMLInputElement>('proto-entry').value } : {};
}
function lakehouseIcon(name: string): string {
  return `<svg class="icon" aria-hidden="true"><use href="#icon-${name}"></use></svg>`;
}
function renderLakehouseTree(): void {
  const search = element<HTMLInputElement>('filter').value.toLowerCase();
  const visible = tables.filter((table) => table.table.toLowerCase().includes(search));
  const branch = (key: string, label: string, icon: string, children: string) =>
    `<details data-branch="${escape(key)}" ${search || !closedBranches.has(key) ? 'open' : ''}><summary>${lakehouseIcon(icon)}<span>${escape(label)}</span></summary>${children}</details>`;
  element('tree').innerHTML = [...new Set(visible.map((table) => table.table.split('.')[0]))].map((catalog) => {
    const members = visible.filter((table) => table.table.split('.')[0] === catalog);
    const schemas = [...new Set(members.map((table) => table.table.split('.')[1]))].map((schema) =>
      branch(`schema:${catalog}.${schema}`, schema, 'schema', members.filter((table) => table.table.split('.')[1] === schema)
        .map((table) => `<button class="tree-table ${selected === table.table ? 'selected' : ''}" data-table="${escape(table.table)}" aria-pressed="${selected === table.table}" title="${escape(table.table)}">${lakehouseIcon('table')}<span>${escape(table.table.split('.')[2])}</span></button>`).join(''))).join('');
    return branch(`catalog:${catalog}`, catalog, 'lakehouse',
      branch(`tables:${catalog}`, 'Tables', 'folder', schemas));
  }).join('') || '<p class="tree-empty">No matching tables. Create a contract or choose another catalogue.</p>';
  for (const details of element('tree').querySelectorAll<HTMLDetailsElement>('details')) {
    details.ontoggle = () => {
      if (element<HTMLInputElement>('filter').value) return;
      if (details.open) closedBranches.delete(details.dataset.branch!);
      else closedBranches.add(details.dataset.branch!);
    };
  }
  for (const button of element('tree').querySelectorAll<HTMLButtonElement>('[data-table]')) {
    button.onclick = () => { selected = button.dataset.table!; renderTree(); renderDetail(); showTab('catalog'); };
  }
}
function showLakehousePane(name: string): void {
  lakehousePane = name;
  for (const pane of ['schema', 'properties', 'contract']) {
    const content = document.getElementById(`lakehouse-${pane}`);
    if (content) content.hidden = pane !== name;
    element(`${pane}-view`).classList.toggle('selected', pane === name);
    element(`${pane}-view`).setAttribute('aria-selected', String(pane === name));
  }
}
function renderLakehouseDetail(table: Table): string {
  return `<section id="lakehouse-schema" role="tabpanel" aria-labelledby="schema-view">
    <div class="grid-heading"><strong>${table.schema.fields.length} columns</strong><span>Committed schema · v${table.version}</span></div>
    <table class="schema-grid"><thead><tr><th scope="col">#</th><th scope="col">Column name</th><th scope="col">Data type</th><th scope="col">Nullable</th><th scope="col">Proto field</th></tr></thead><tbody>
      ${table.schema.fields.map((field, index) => `<tr><td class="row-index">${index + 1}</td><td>${lakehouseIcon('column')}<strong>${escape(field.name)}</strong></td><td>${escape(field.mapping.spark)}</td><td>${field.nullable ? 'Yes' : 'No'}</td><td>${field.field_number}</td></tr>`).join('')}
    </tbody></table><p class="metadata-note">Schema preview only. This explorer does not query table rows.</p></section>
    <section id="lakehouse-properties" role="tabpanel" aria-labelledby="properties-view" hidden><h2>Table properties</h2>
    <div class="property-grid"><span class="muted">Location</span><span class="mono">${escape(table.location)}</span>
    ${Object.entries(table.properties).map(([key, value]) => `<span>${escape(key)}</span><span class="mono">${escape(value)}</span>`).join('')}</div>
    <p class="metadata-note">Recorded backend policy. Creating a contract does not add policy enforcement.</p></section>
    <section id="lakehouse-contract" role="tabpanel" aria-labelledby="contract-view" hidden>
    <div class="table-heading"><div><h2>SDK contract</h2><span class="pill">Registered · v${table.version}</span></div><button id="rebuild" class="primary">Build committed v${table.version}</button></div>
    <div class="property-grid"><span>Table</span><span>${escape(table.table)}</span><span>Table identity</span><span class="mono">${escape(table.tableId)}</span>
    <span>Schema fingerprint</span><span class="mono">${escape(table.fingerprint)}</span>
    ${table.registrationReceipt ? `<span>Native ${escape(table.registrationReceipt.provider)} identity</span><span class="mono">${escape(table.registrationReceipt.native_table_id)}</span>` : ''}</div>
    <p class="metadata-note">Build the committed schema without registering it again. Schema models and the reusable Arrowcol SDK are separate packages. The SDK binds the contract at runtime and calls Arrowcol, not the catalogue. Downloads and host paths appear under Workflow runs.</p></section>`;
}
function favoriteKey(table: string): string {
  return `${registrySelect.value}:${table}`;
}
function renderFabricCatalog(): void {
  const search = element<HTMLInputElement>('filter').value.toLowerCase();
  const schema = element<HTMLSelectElement>('schema-filter').value;
  const workspaceSearch = element<HTMLInputElement>('workspace-filter').value.toLowerCase();
  const catalogs = [...new Set(tables.map((table) => table.table.split('.')[0]))].sort();
  element('workspace-list').innerHTML = catalogs.filter((name) => name.toLowerCase().includes(workspaceSearch))
    .map((name) => `<button class="workspace-button ${name === workspaceFilter ? 'selected' : ''}" data-workspace="${escape(name)}"><span aria-hidden="true">▱</span>${escape(name)}</button>`).join('');
  element('scope-title').textContent = favoritesOnly ? 'Favorites' : workspaceFilter || 'All items';
  element('all-items').classList.toggle('selected', !favoritesOnly);
  element('favorites').classList.toggle('selected', favoritesOnly);
  element('all-workspaces').classList.toggle('selected', !workspaceFilter);
  const visible = tables.filter((table) => {
    const [catalog, database] = table.table.split('.');
    return (!workspaceFilter || catalog === workspaceFilter) && (!schema || database === schema) &&
      table.table.toLowerCase().includes(search) && (!favoritesOnly || favorites.has(favoriteKey(table.table)));
  });
  element('tree').innerHTML = visible.length ? `<table class="catalog-table"><thead><tr>
    <th aria-label="Favorite"></th><th>Name</th><th>Type</th><th>Workspace</th><th>Database</th><th>Version</th><th>Contract</th>
    </tr></thead><tbody>${visible.map((table) => {
      const [catalog, database, name] = table.table.split('.');
      const saved = favorites.has(favoriteKey(table.table));
      return `<tr class="${selected === table.table ? 'selected' : ''}">
        <td><button class="favorite-button ${saved ? 'saved' : ''}" data-favorite="${escape(table.table)}" aria-label="${saved ? 'Unfavorite' : 'Favorite'} ${escape(name)}" aria-pressed="${saved}">${saved ? '★' : '☆'}</button></td>
        <td><span class="item-symbol" aria-hidden="true">▦</span><button class="table-link" data-table="${escape(table.table)}">${escape(name)}</button></td>
        <td>Delta table</td><td>${escape(catalog)}</td><td>${escape(database)}</td><td>v${table.version}</td>
        <td><span class="pill">Registered</span></td></tr>`;
    }).join('')}</tbody></table>` : '<div class="empty"><div class="empty-symbol">▦</div><h2>No tables in this view</h2><p class="muted">Choose another workspace or create a new schema contract.</p></div>';
  element('item-count').textContent = `${visible.length} items found`;
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-table]')) {
    button.onclick = () => { selected = button.dataset.table!; renderTree(); renderDetail(); };
  }
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-workspace]')) {
    button.onclick = () => { workspaceFilter = button.dataset.workspace!; selected = ''; renderTree(); renderDetail(); };
  }
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-favorite]')) {
    button.onclick = () => {
      const key = favoriteKey(button.dataset.favorite!);
      if (favorites.has(key)) favorites.delete(key); else favorites.add(key);
      try { localStorage.setItem('arrowcol-fabric-favorites', JSON.stringify([...favorites])); }
      catch (error) { notice(`Could not persist favorites: ${String(error)}`); }
      renderTree();
    };
  }
}
function renderDetail(): void {
  const table = tables.find((item) => item.table === selected);
  if (lakehouse) {
    const url = new URL(location.href);
    url.searchParams.set('registry', registrySelect.value);
    if (table) url.searchParams.set('table', table.table);
    else url.searchParams.delete('table');
    history.replaceState(null, '', url.pathname + url.search + url.hash);
    element('table-tab-name').textContent = table?.table.split('.')[2] ?? selectedNative()?.label ?? 'Lakehouse explorer';
    element('table-breadcrumb').textContent = table?.table.split('.').join(' / ') ?? 'Select a table in Explorer';
    element('schema-status').textContent = table ? `${table.schema.fields.length} columns · Committed v${table.version} · Metadata only` : 'Local catalogue · Metadata only';
    element<HTMLButtonElement>('build-selected').disabled = !table;
    element('close-detail').hidden = !table;
    for (const pane of ['schema', 'properties', 'contract']) element<HTMLButtonElement>(`${pane}-view`).disabled = !table;
  }
  if (selectedNative()) {
    if (fabric) element('detail-panel').hidden = true;
    return;
  }
  if (fabric) element('detail-panel').hidden = !table;
  if (!table) {
    element('detail').innerHTML = '<div class="empty"><div class="empty-symbol">▦</div><h2>Your catalogue starts here</h2><p class="muted">Create a table and build its neutral contract and Rust/C++/C# models. Consume them with reusable language SDKs.</p><p class="muted small">The local catalogue and hosted simulation have independent persistent state. Typed streams support flat scalars; legacy Rust/TypeScript Arrow builders remain available.</p></div>';
    return;
  }
  const [catalog, schema, name] = table.table.split('.');
  element('detail').innerHTML = lakehouse ? renderLakehouseDetail(table) : `
    <div class="breadcrumb">${escape(catalog)} &nbsp;/&nbsp; ${escape(schema)} &nbsp;/&nbsp; ${escape(name)}</div>
    <div class="table-heading"><div><h2>${escape(name)}</h2><span class="pill">DELTA TABLE</span>${fabric ? `<p class="small"><a href="/lakehouse?${escape(new URLSearchParams({ registry: registrySelect.value, table: table.table }).toString())}">Open in Lakehouse Explorer</a></p>` : ''}</div><button id="rebuild" class="primary">Build committed v${table.version}</button></div>
    <div class="cards">
      <div class="card"><div class="caption">Schema version</div><strong>v${table.version}</strong><p class="muted small">Committed catalogue version</p></div>
      <div class="card"><div class="caption">Table identity</div><div class="mono">${escape(table.tableId)}</div></div>
      <div class="card"><div class="caption">Schema fingerprint</div><div class="mono">${escape(table.fingerprint)}</div></div>
    </div>
    <h3>Columns <span class="muted">(${table.schema.fields.length})</span></h3>
    <table><thead><tr><th>Column name</th><th>Data type</th><th>Nullable</th><th>Protobuf field</th></tr></thead><tbody>
      ${table.schema.fields.map((field) => `<tr><td><strong>${escape(field.name)}</strong></td><td><code>${escape(field.mapping.spark)}</code></td><td>${field.nullable ? 'Yes' : 'No'}</td><td>${field.field_number}</td></tr>`).join('')}
    </tbody></table>
    <h3>Table properties</h3><div class="property-grid">
      <span class="muted">Location</span><span class="mono">${escape(table.location)}</span>
      ${Object.entries(table.properties).map(([key, value]) => `<span class="muted">${escape(key)}</span><span class="mono">${escape(value)}</span>`).join('')}
    </div><p class="small muted">Properties describe the committed backend policy. The contract builder does not introduce policy enforcement or forward arbitrary headers.</p>`;
  const rebuild = () => handle(async () => {
    const run = await api<Run>('/api/runs', { registry: registrySelect.value, mode: 'rebuild', table: table.table, version: table.version, ...compilerOptions() });
    notice(`Queued rebuild ${run.id}. No CREATE statement will be sent.`);
    showTab('runs');
    await refreshRuns();
  });
  element<HTMLButtonElement>('rebuild').onclick = rebuild;
  if (lakehouse) {
    element<HTMLButtonElement>('build-selected').onclick = rebuild;
    showLakehousePane(lakehousePane);
  }
}
async function refreshTables(): Promise<void> {
  const sequence = ++tableSequence;
  const native = selectedNative();
  element<HTMLButtonElement>('add-table').disabled = !!native;
  if (fabric) element<HTMLButtonElement>('create-nav').disabled = !!native;
  if (native) {
    element('registry-note').textContent = `${native.label} · Inspection only · Build connection not enabled`;
    tables = [];
    selected = '';
    if (fabric) element('schema-filter').innerHTML = '<option value="">All databases</option>';
    renderTree();
    renderDetail();
    return;
  }
  const registry = registries.find((item) => item.id === registrySelect.value)!;
  element('registry-note').textContent = registry.native
    ? `${registry.native.endpoint} · Native authority · Collector bridge ${registry.endpoint}`
    : `${registry.endpoint} · ${registry.mode === 'owned' ? 'Persistent independent service' : 'Configured external service'}`;
  const result = await api<{ tables: Table[] }>(`/api/tables?registry=${encodeURIComponent(registry.id)}`);
  if (sequence !== tableSequence) return;
  tables = result.tables;
  if (!tables.some((table) => table.table === selected)) selected = fabric ? '' : tables[0]?.table ?? '';
  if (fabric) {
    const filter = element<HTMLSelectElement>('schema-filter');
    const previous = filter.value;
    const schemas = [...new Set(tables.map((table) => table.table.split('.')[1]))].sort();
    filter.innerHTML = '<option value="">All databases</option>' +
      schemas.map((name) => `<option>${escape(name)}</option>`).join('');
    if (schemas.includes(previous)) filter.value = previous;
  }
  renderTree();
  renderDetail();
}
function renderRuns(): void {
  element('run-count').textContent = String(runs.length);
  element('runs').innerHTML = runs.length ? runs.map((run) => `
    <article class="run" data-run="${escape(run.id)}">
      <div class="run-top"><button class="run-open" data-pipeline="${escape(run.id)}" aria-label="View pipeline for ${escape(run.pin?.table ?? run.id)}">${escape(run.pin?.table ?? 'New table contract')} <span aria-hidden="true">↗</span></button><span class="status ${escape(run.status)}">${escape(run.status)}</span></div>
      <div class="run-meta">${escape(run.registry.label)} &nbsp; / &nbsp; ${escape(run.phase)} &nbsp; / &nbsp; ${escape(new Date(run.createdAt).toLocaleString())}</div>
      <div class="small muted">Registration: ${escape(run.registration)}${run.startedAt ? ` · Started ${escape(new Date(run.startedAt).toLocaleTimeString())}` : ''}${run.finishedAt ? ` · Finished ${escape(new Date(run.finishedAt).toLocaleTimeString())}` : ''}</div>
      ${run.error ? `<p class="error">${run.registration === 'committed' || run.registration === 'existing' ? 'Table registered; artifact build failed. ' : run.registration === 'unknown' ? 'Registration outcome uncertain; inspect the catalogue before rebuilding. ' : ''}${escape(run.error)}</p>` : ''}
      ${run.artifact ? `<div class="artifact"><a href="/api/runs/${run.id}/artifact">Download SDK + contract</a><input aria-label="Artifact path" readonly value="${escape(run.artifact.hostPath)}"><button class="text-button" data-copy="${escape(run.id)}">Copy path</button><div class="mono">SHA-256 ${escape(run.artifact.sha256)}</div></div>${packageDownloads(run)}` : ''}
      <button class="text-button" data-log="${escape(run.id)}">View logs</button><span class="mono muted">${escape(run.id)}</span>
    </article>`).join('') : '<div class="empty"><h2>No builds yet</h2><p class="muted">Create a table or rebuild a committed version to start the Nx workflow.</p></div>';
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-log]')) {
    button.onclick = () => handle(async () => {
      const result = await api<{ text: string }>(`/api/runs/${button.dataset.log}/log`);
      element('logs').textContent = result.text || 'Waiting for workflow output...';
      element('logs').hidden = false;
    });
  }
  for (const button of document.querySelectorAll<HTMLButtonElement>('[data-copy]')) {
    button.onclick = () => handle(async () => {
      const path = runs.find((run) => run.id === button.dataset.copy)!.artifact!.hostPath;
      await navigator.clipboard.writeText(path);
      notice('Artifact path copied.');
    });
  }
  for (const article of document.querySelectorAll<HTMLElement>('[data-run]')) {
    article.onclick = (event) => {
      if (event.target instanceof Element && event.target.closest('a, input, button, details') &&
        !event.target.closest('[data-pipeline]')) return;
      openRunPipeline(article.dataset.run!);
    };
  }
}
async function refreshRuns(): Promise<void> {
  const latest = await api<Run[]>('/api/runs');
  const signature = JSON.stringify(latest);
  if (signature !== lastRunSignature) {
    const previous = runs;
    runs = latest;
    lastRunSignature = signature;
    renderRuns();
    renderRunPipeline();
    if (latest.some((run) => run.registry.id === registrySelect.value &&
      ['committed', 'existing'].includes(run.registration) &&
      previous.find((item) => item.id === run.id)?.registration !== run.registration)) await refreshTables();
  }
  if (inspectedRunId && runs.some((run) => run.id === inspectedRunId) && !runDialog.open) openRunPipeline(inspectedRunId);
  await refreshRunOutput();
}
element('add-table').onclick = () => {
  if (selectedNative()) { notice('Enable this native build connection first, then restart catalog-ui.'); return; }
  const registry = registries.find((item) => item.id === registrySelect.value)!;
  element('create-registry').textContent = `Target: ${registry.label} (${registry.endpoint})`;
  if (registry.native?.provider === 'hive') element<HTMLInputElement>('catalog-name').value = 'hive';
  if (fabric && workspaceFilter) element<HTMLInputElement>('catalog-name').value = workspaceFilter;
  element('column-rows').replaceChildren();
  element('property-rows').replaceChildren();
  addColumn('event_id', 'BIGINT', false);
  addColumn('observed_at', 'TIMESTAMP', false);
  addColumn('payload', 'STRING', true);
  addProperty('delta.appendOnly', 'true');
  for (const definition of propertyDefinitions.options) {
    if (definition.declared_default !== null) addProperty(definition.key, definition.declared_default);
  }
  element('property-source').textContent = `Optional values and defaults come from compiled proto2 options. Unset declared defaults are included in the preview. Ingest tier is metadata only; no runtime tier enforcement. Definitions ${propertyDefinitions.definitions_sha256}`;
  updateDdl();
  element('form-error').textContent = '';
  dialog.showModal();
};
function addColumn(name = '', type = 'STRING', nullable = true): void {
  const row = document.createElement('tr');
  row.innerHTML = `<td><input aria-label="Column name" data-column-name value="${escape(name)}" required pattern="[A-Za-z_][A-Za-z0-9_]*"></td>
    <td><select aria-label="Column type" data-column-type>${['BIGINT', 'INT', 'STRING', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'DOUBLE', 'BOOLEAN', 'BINARY', 'DATE', 'DECIMAL(20,4)'].map((item) => `<option ${type === item ? 'selected' : ''}>${item}</option>`).join('')}</select></td>
    <td><input aria-label="Nullable" data-nullable type="checkbox" ${nullable ? 'checked' : ''}></td>
    <td><button type="button" class="text-button" aria-label="Remove column">Remove</button></td>`;
  row.querySelector('button')!.onclick = () => { row.remove(); updateDdl(); };
  element('column-rows').append(row);
  updateDdl();
}
function addProperty(key = '', value = ''): void {
  if ([...document.querySelectorAll<HTMLInputElement>('[data-key]')].some((input) => key && input.value === key)) {
    element('form-error').textContent = 'That property is already present.';
    return;
  }
  const definition = propertyDefinitions.options.find((item) => item.key === key);
  const selected = value || definition?.declared_default || '';
  const row = document.createElement('div');
  row.className = 'property-editor';
  const valueControl = definition?.kind === 'enum'
    ? `<select aria-label="Property value" data-value>${definition.enum_values.map((item) =>
      `<option value="${escape(item)}" ${selected === item ? 'selected' : ''}>${escape(item)}${item === definition.declared_default ? ' (default)' : ''}</option>`).join('')}</select>`
    : `<input aria-label="Property value" data-value placeholder="true" value="${escape(selected)}" required>`;
  row.innerHTML = `<input aria-label="Property key" data-key placeholder="delta.appendOnly" value="${escape(key)}" ${definition ? 'readonly' : ''} required>
    ${valueControl}
    <button type="button" class="text-button" aria-label="Remove property">Remove</button>`;
  row.querySelector('button')!.onclick = () => { row.remove(); updateDdl(); };
  element('property-rows').append(row);
  updateDdl();
}
function updateDdl(): void {
  const identifier = (value: string) => `\`${value.replace(/`/g, '``')}\``;
  const literal = (value: string) => `'${value.replace(/'/g, "''")}'`;
  const table = ['catalog-name', 'schema-name', 'table-name'].map((id) => identifier(element<HTMLInputElement>(id).value)).join('.');
  const columns = [...document.querySelectorAll<HTMLTableRowElement>('#column-rows tr')].map((row) =>
    `    ${identifier(row.querySelector<HTMLInputElement>('[data-column-name]')!.value)} ${row.querySelector<HTMLSelectElement>('[data-column-type]')!.value}${row.querySelector<HTMLInputElement>('[data-nullable]')!.checked ? '' : ' NOT NULL'}`);
  const explicit = [...document.querySelectorAll<HTMLElement>('.property-editor')].map((row) => ({
    key: row.querySelector<HTMLInputElement>('[data-key]')!.value,
    value: row.querySelector<HTMLInputElement | HTMLSelectElement>('[data-value]')!.value,
  }));
  for (const definition of propertyDefinitions.options) {
    if (definition.declared_default !== null && !explicit.some((item) => item.key === definition.key)) {
      explicit.push({ key: definition.key, value: definition.declared_default });
    }
  }
  const properties = explicit.map((item) => `${literal(item.key)} = ${literal(item.value)}`);
  element<HTMLTextAreaElement>('ddl').value = `CREATE TABLE ${table} (\n${columns.join(',\n')}\n)\nUSING DELTA${properties.length ? `\nTBLPROPERTIES (${properties.join(', ')})` : ''}`;
}
element('create-form').oninput = updateDdl;
element('add-column').onclick = () => addColumn();
element('add-property').onclick = () => {
  const key = element<HTMLSelectElement>('property-kind').value;
  addProperty(key === 'custom' ? '' : key);
};
element('close-dialog').onclick = () => dialog.close();
element<HTMLFormElement>('create-form').onsubmit = (event) => {
  event.preventDefault();
  if (selectedNative()) {
    element('form-error').textContent = 'Enable this native build connection first, then restart catalog-ui.';
    return;
  }
  updateDdl();
  const submit = element<HTMLButtonElement>('create-submit');
  submit.disabled = true;
  void (async () => {
    try {
      const run = await api<Run>('/api/runs', {
        registry: registrySelect.value, mode: 'create', ddl: element<HTMLTextAreaElement>('ddl').value,
        definitionsDigest: propertyDefinitions.definitions_sha256,
        ...compilerOptions(),
      });
      dialog.close();
      notice(`Queued contract build ${run.id}. Follow registration and artifact generation below.`);
      showTab('runs');
      await refreshRuns();
    } catch (error) { element('form-error').textContent = error instanceof Error ? error.message : String(error); }
    finally { submit.disabled = false; }
  })();
};
registrySelect.onchange = () => {
  const value = registrySelect.value;
  closedBranches.clear();
  workspaceFilter = '';
  selected = '';
  tables = [];
  renderTree();
  renderDetail();
  localStorage.setItem('arrowcol-registry', registrySelect.value);
  handle(async () => {
    await refreshTables();
    if (registrySelect.value === value && selectedNative()) {
      showTab('catalog');
      await openConnections();
    }
  });
};
element('refresh').onclick = () => handle(selectedNative() ? openConnections : refreshTables);
element('filter').oninput = renderTree;
element('catalog-tab').onclick = () => showTab('catalog');
element('runs-tab').onclick = () => { showTab('runs'); handle(refreshRuns); };
if (lakehouse) {
  element('close-detail').onclick = () => { selected = ''; renderTree(); renderDetail(); };
  for (const pane of ['schema', 'properties', 'contract']) element(`${pane}-view`).onclick = () => showLakehousePane(pane);
  element('choose-catalogue').onclick = () => registrySelect.focus();
  element('toggle-explorer').onclick = () => {
    const collapsed = element('catalog-view').classList.toggle('explorer-collapsed');
    element('toggle-explorer').setAttribute('aria-expanded', String(!collapsed));
    element('toggle-explorer').setAttribute('aria-label', collapsed ? 'Expand Explorer' : 'Collapse Explorer');
  };
}
if (fabric) {
  element('close-detail').onclick = () => { selected = ''; renderTree(); renderDetail(); };
  element('all-workspaces').onclick = () => { workspaceFilter = ''; selected = ''; renderTree(); renderDetail(); };
  element('all-items').onclick = () => { favoritesOnly = false; showTab('catalog'); renderTree(); };
  element('favorites').onclick = () => { favoritesOnly = true; showTab('catalog'); renderTree(); };
  element('workspace-filter').oninput = renderTree;
  element('schema-filter').onchange = renderTree;
  for (const id of ['home-nav', 'explore-nav', 'workspace-nav']) {
    element(id).onclick = () => { showTab('catalog'); };
  }
  element('monitor-nav').onclick = () => { showTab('runs'); handle(refreshRuns); };
  element('create-nav').onclick = () => element<HTMLButtonElement>('add-table').click();
  try {
    const saved: unknown = JSON.parse(localStorage.getItem('arrowcol-fabric-favorites') ?? '[]');
    if (!Array.isArray(saved) || !saved.every((item) => typeof item === 'string')) throw new Error('Invalid saved favorites');
    for (const item of saved) favorites.add(item);
  } catch (error) { notice(`Could not load favorites: ${String(error)}`); }
}
handle(async () => {
  const [config, definitions] = await Promise.all([
    api<{ registries: Registry[]; token: string }>('/api/config'),
    api<PropertyDefinitions>('/api/properties'),
  ]);
  propertyDefinitions = definitions;
  element<HTMLSelectElement>('property-kind').innerHTML = '<option value="custom">Custom or existing Delta / Arrowcol property</option>' +
    definitions.options.map((definition) => `<option value="${escape(definition.key)}">${escape(definition.option_name)}${definition.optional ? ' (optional)' : ''}${definition.declared_default === null ? '' : ` — default: ${escape(definition.declared_default)}`}</option>`).join('');
  registries = config.registries;
  token = config.token;
  registrySelect.innerHTML = registries.map((registry) => `<option value="${escape(registry.id)}">${escape(registry.label)}</option>`).join('') + unavailableNativeOptions();
  const saved = (lakehouse ? new URLSearchParams(location.search).get('registry') : null) ?? localStorage.getItem('arrowcol-registry');
  if (saved && [...registrySelect.options].some((option) => option.value === saved)) registrySelect.value = saved;
  if (lakehouse) selected = new URLSearchParams(location.search).get('table') ?? '';
  await Promise.all([refreshTables(), refreshRuns()]);
});
let polling = false;
setInterval(() => {
  if (polling) return;
  polling = true;
  void refreshRuns().catch((error: unknown) => notice(`Status refresh failed: ${error instanceof Error ? error.message : String(error)}`))
    .finally(() => { polling = false; });
}, 2000);
