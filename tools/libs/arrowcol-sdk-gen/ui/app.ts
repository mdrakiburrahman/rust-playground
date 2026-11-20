export {};
interface Registry { id: string; label: string; endpoint: string; mode: string; authority: string }
interface PropertyDefinition {
  key: string; option_name: string; optional: boolean; kind: string;
  enum_values: string[]; declared_default: string | null;
}
interface PropertyDefinitions { definitions_sha256: string; options: PropertyDefinition[] }
interface Table {
  table: string; tableId: string; version: number; fingerprint: string; location: string;
  properties: Record<string, string>;
  schema: { fields: { name: string; nullable: boolean; field_number: number; mapping: { spark: string } }[] };
}
interface Run {
  id: string; registry: Registry; status: string; phase: string; registration: string;
  createdAt: string; startedAt?: string; finishedAt?: string;
  pin?: { table: string; version: number };
  error?: string; artifact?: { hostPath: string; sha256: string };
}
const element = <T extends HTMLElement>(id: string) => {
  const result = document.getElementById(id);
  if (!result) throw new Error(`Missing UI element: ${id}`);
  return result as T;
};
const escape = (text: unknown) => String(text ?? '').replace(/[&<>"']/g, (char) =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]!);
let registries: Registry[] = [];
let tables: Table[] = [];
let runs: Run[] = [];
let selected = '';
let token = '';
let propertyDefinitions: PropertyDefinitions;
let lastRunSignature = '';
let tableSequence = 0;
const registrySelect = element<HTMLSelectElement>('registry');
const dialog = element<HTMLDialogElement>('create-dialog');
function notice(text: string): void {
  element('notice').textContent = text;
  element('notice').hidden = !text;
}
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
function renderDetail(): void {
  const table = tables.find((item) => item.table === selected);
  if (!table) {
    element('detail').innerHTML = '<div class="empty"><div class="empty-symbol">▦</div><h2>Your catalogue starts here</h2><p class="muted">Create a table to register a real schema and build its Rust and TypeScript SDKs.</p><p class="muted small">The local catalogue and hosted simulation have independent persistent state.</p></div>';
    return;
  }
  const [catalog, schema, name] = table.table.split('.');
  element('detail').innerHTML = `
    <div class="breadcrumb">${escape(catalog)} &nbsp;/&nbsp; ${escape(schema)} &nbsp;/&nbsp; ${escape(name)}</div>
    <div class="table-heading"><div><h2>${escape(name)}</h2><span class="pill">DELTA TABLE</span></div><button id="rebuild" class="primary">Build committed v${table.version}</button></div>
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
  element<HTMLButtonElement>('rebuild').onclick = () => handle(async () => {
    const run = await api<Run>('/api/runs', { registry: registrySelect.value, mode: 'rebuild', table: table.table, version: table.version });
    notice(`Queued rebuild ${run.id}. No CREATE statement will be sent.`);
    showTab('runs');
    await refreshRuns();
  });
}
async function refreshTables(): Promise<void> {
  const sequence = ++tableSequence;
  const registry = registries.find((item) => item.id === registrySelect.value)!;
  element('registry-note').textContent = `${registry.endpoint} · ${registry.mode === 'owned' ? 'Persistent independent service' : 'Configured external service'}`;
  const result = await api<{ tables: Table[] }>(`/api/tables?registry=${encodeURIComponent(registry.id)}`);
  if (sequence !== tableSequence) return;
  tables = result.tables;
  if (!tables.some((table) => table.table === selected)) selected = tables[0]?.table ?? '';
  renderTree();
  renderDetail();
}
function renderRuns(): void {
  element('run-count').textContent = String(runs.length);
  element('runs').innerHTML = runs.length ? runs.map((run) => `
    <article class="run" data-run="${escape(run.id)}">
      <div class="run-top"><strong>${escape(run.pin?.table ?? 'New table contract')}</strong><span class="status ${escape(run.status)}">${escape(run.status)}</span></div>
      <div class="run-meta">${escape(run.registry.label)} &nbsp; / &nbsp; ${escape(run.phase)} &nbsp; / &nbsp; ${escape(new Date(run.createdAt).toLocaleString())}</div>
      <div class="small muted">Registration: ${escape(run.registration)}${run.startedAt ? ` · Started ${escape(new Date(run.startedAt).toLocaleTimeString())}` : ''}${run.finishedAt ? ` · Finished ${escape(new Date(run.finishedAt).toLocaleTimeString())}` : ''}</div>
      ${run.error ? `<p class="error">${run.registration === 'committed' || run.registration === 'existing' ? 'Table registered; artifact build failed. ' : run.registration === 'unknown' ? 'Registration outcome uncertain; inspect the catalogue before rebuilding. ' : ''}${escape(run.error)}</p>` : ''}
      ${run.artifact ? `<div class="artifact"><a href="/api/runs/${run.id}/artifact">Download SDK + contract</a><input aria-label="Artifact path" readonly value="${escape(run.artifact.hostPath)}"><button class="text-button" data-copy="${escape(run.id)}">Copy path</button><div class="mono">SHA-256 ${escape(run.artifact.sha256)}</div></div>` : ''}
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
}
async function refreshRuns(): Promise<void> {
  const latest = await api<Run[]>('/api/runs');
  const signature = JSON.stringify(latest);
  if (signature !== lastRunSignature) {
    const previous = runs;
    runs = latest;
    lastRunSignature = signature;
    renderRuns();
    if (latest.some((run) => run.registry.id === registrySelect.value &&
      ['committed', 'existing'].includes(run.registration) &&
      previous.find((item) => item.id === run.id)?.registration !== run.registration)) await refreshTables();
  }
}
element('add-table').onclick = () => {
  const registry = registries.find((item) => item.id === registrySelect.value)!;
  element('create-registry').textContent = `Target: ${registry.label} (${registry.endpoint})`;
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
  updateDdl();
  const submit = element<HTMLButtonElement>('create-submit');
  submit.disabled = true;
  void (async () => {
    try {
      const run = await api<Run>('/api/runs', {
        registry: registrySelect.value, mode: 'create', ddl: element<HTMLTextAreaElement>('ddl').value,
        definitionsDigest: propertyDefinitions.definitions_sha256,
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
  selected = '';
  tables = [];
  renderTree();
  renderDetail();
  localStorage.setItem('arrowcol-registry', registrySelect.value);
  handle(refreshTables);
};
element('refresh').onclick = () => handle(refreshTables);
element('filter').oninput = renderTree;
element('catalog-tab').onclick = () => showTab('catalog');
element('runs-tab').onclick = () => { showTab('runs'); handle(refreshRuns); };
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
  registrySelect.innerHTML = registries.map((registry) => `<option value="${escape(registry.id)}">${escape(registry.label)}</option>`).join('');
  const saved = localStorage.getItem('arrowcol-registry');
  if (registries.some((registry) => registry.id === saved)) registrySelect.value = saved!;
  element<HTMLButtonElement>('add-table').disabled = false;
  await Promise.all([refreshTables(), refreshRuns()]);
});
let polling = false;
setInterval(() => {
  if (polling) return;
  polling = true;
  void refreshRuns().catch((error: unknown) => notice(`Status refresh failed: ${error instanceof Error ? error.message : String(error)}`))
    .finally(() => { polling = false; });
}, 2000);
