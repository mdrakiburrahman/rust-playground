import * as monaco from 'monaco-editor/editor';
import 'monaco-editor/features/register.all';
import { getKustoWorker, kustoDefaults } from '@kusto/monaco-kusto/release/esm/monaco.contribution.js';
import { DARK_THEME, LIGHT_THEME, defineThemes } from './themes';
import '../styles/app.css';

type ChangeListener = (tabId: string, text: string) => void;
type CursorListener = (line: number, column: number, selected: number) => void;
type CommandListener = (command: string) => void;

interface TabModel {
  model: monaco.editor.ITextModel;
  viewState: monaco.editor.ICodeEditorViewState | null;
  pending: number | undefined;
}

const UI_MODULE = '/assets/kusto_explorer_ui.js';
const UI_WASM = '/assets/kusto_explorer_ui_bg.wasm';
const CHANGE_DELAY_MS = 200;

(self as unknown as { MonacoEnvironment: monaco.Environment }).MonacoEnvironment = {
  getWorker: (_workerId: string, label: string) =>
    new Worker(label === 'kusto' ? '/assets/kusto.worker.js' : '/assets/editor.worker.js', {
      type: 'module',
      name: label,
    }),
};

defineThemes(monaco);
guardSemanticTokens();
kustoDefaults.setLanguageSettings({
  ...kustoDefaults.languageSettings,
  includeControlCommands: true,
  enableHover: true,
  newlineAfterPipe: true,
  openSuggestionDialogAfterPreviousSuggestionAccepted: true,
  formatter: { indentationSize: 4, pipeOperatorStyle: 'Smart' },
  syntaxErrorAsMarkDown: { enableSyntaxErrorAsMarkDown: true, header: 'Syntax error' },
  completionOptions: { includeExtendedSyntax: false },
});

const tabs = new Map<string, TabModel>();
const changeListeners: ChangeListener[] = [];
const cursorListeners: CursorListener[] = [];
const commandListeners: CommandListener[] = [];
let editor: monaco.editor.IStandaloneCodeEditor | undefined;
let activeTab: string | undefined;
let theme = DARK_THEME;
let fontSize = 14;

// The Kusto semantic tokens provider maps classifications onto the model after a worker round
// trip, so edits made meanwhile would misplace them. Reporting such results as cancelled makes
// the editor keep the current tokens and request fresh ones.
function guardSemanticTokens(): void {
  const register = monaco.languages.registerDocumentSemanticTokensProvider;
  Object.assign(monaco.languages, {
    registerDocumentSemanticTokensProvider(
      selector: monaco.languages.LanguageSelector,
      provider: monaco.languages.DocumentSemanticTokensProvider,
    ): monaco.IDisposable {
      return register(selector, {
        onDidChange: provider.onDidChange,
        getLegend: () => provider.getLegend(),
        releaseDocumentSemanticTokens: (resultId) => provider.releaseDocumentSemanticTokens(resultId),
        async provideDocumentSemanticTokens(model, lastResultId, token) {
          const version = model.getVersionId();
          const result = await provider.provideDocumentSemanticTokens(model, lastResultId, token);
          if (model.isDisposed() || model.getVersionId() !== version) {
            const cancelled = new Error('Canceled');
            cancelled.name = 'Canceled';
            throw cancelled;
          }
          return result;
        },
      });
    },
  });
}

function notifyChange(tabId: string): void {
  const entry = tabs.get(tabId);
  if (!entry) return;
  window.clearTimeout(entry.pending);
  entry.pending = undefined;
  const text = entry.model.getValue();
  for (const listener of changeListeners) listener(tabId, text);
}

function emitCommand(command: string): void {
  for (const listener of commandListeners) listener(command);
}

function emitCursor(): void {
  const selection = editor?.getSelection();
  const model = editor?.getModel();
  if (!selection || !model) return;
  const selected = model.getValueLengthInRange(selection);
  for (const listener of cursorListeners) listener(selection.positionLineNumber, selection.positionColumn, selected);
}

function createEditor(element: HTMLElement): monaco.editor.IStandaloneCodeEditor {
  const created = monaco.editor.create(element, {
    model: activeTab ? tabs.get(activeTab)?.model ?? null : null,
    theme,
    automaticLayout: true,
    fontFamily: "'Cascadia Code', 'Cascadia Mono', 'JetBrains Mono', 'SF Mono', Consolas, 'DejaVu Sans Mono', monospace",
    fontSize,
    minimap: { enabled: false },
    scrollBeyondLastLine: false,
    fixedOverflowWidgets: true,
    lineNumbersMinChars: 3,
    padding: { top: 8, bottom: 8 },
    renderLineHighlight: 'all',
    smoothScrolling: true,
    cursorBlinking: 'smooth',
    tabSize: 4,
    bracketPairColorization: { enabled: true },
    'semanticHighlighting.enabled': true,
    suggest: { showStatusBar: true, preview: true },
    stickyScroll: { enabled: false },
    ariaLabel: 'Query editor',
  });
  created.onDidChangeCursorSelection(() => emitCursor());
  created.addAction({
    id: 'kusto-explorer.run',
    label: 'Run Query',
    contextMenuGroupId: 'navigation',
    contextMenuOrder: 0,
    run: () => emitCommand('run'),
  });
  created.addAction({
    id: 'kusto-explorer.palette',
    label: 'Command Palette…',
    contextMenuGroupId: 'navigation',
    contextMenuOrder: 1,
    run: () => emitCommand('palette'),
  });
  return created;
}

const host = {
  mount(element: HTMLElement): void {
    if (editor && element.contains(editor.getContainerDomNode())) {
      editor.layout();
      return;
    }
    if (editor) {
      if (activeTab) {
        const entry = tabs.get(activeTab);
        if (entry) entry.viewState = editor.saveViewState();
      }
      editor.dispose();
    }
    editor = createEditor(element);
    if (activeTab) host.activate(activeTab);
  },

  openModel(tabId: string, text: string): void {
    if (tabs.has(tabId)) return;
    const uri = monaco.Uri.parse(`inmemory://kusto-explorer/tab-${encodeURIComponent(tabId)}.kql`);
    const model = monaco.editor.getModel(uri) ?? monaco.editor.createModel(text, 'kusto', uri);
    const entry: TabModel = { model, viewState: null, pending: undefined };
    model.onDidChangeContent(() => {
      window.clearTimeout(entry.pending);
      entry.pending = window.setTimeout(() => notifyChange(tabId), CHANGE_DELAY_MS);
    });
    tabs.set(tabId, entry);
  },

  activate(tabId: string): void {
    const entry = tabs.get(tabId);
    if (!entry) return;
    if (editor && activeTab && activeTab !== tabId) {
      const previous = tabs.get(activeTab);
      if (previous) previous.viewState = editor.saveViewState();
    }
    activeTab = tabId;
    if (!editor) return;
    if (editor.getModel() !== entry.model) {
      editor.setModel(entry.model);
      if (entry.viewState) editor.restoreViewState(entry.viewState);
    }
    emitCursor();
  },

  closeModel(tabId: string): void {
    const entry = tabs.get(tabId);
    if (!entry) return;
    window.clearTimeout(entry.pending);
    if (editor?.getModel() === entry.model) editor.setModel(null);
    entry.model.dispose();
    tabs.delete(tabId);
    if (activeTab === tabId) activeTab = undefined;
  },

  getText(tabId: string): string {
    const entry = tabs.get(tabId);
    if (!entry) return '';
    if (entry.pending !== undefined) notifyChange(tabId);
    return entry.model.getValue();
  },

  setText(tabId: string, text: string): void {
    const entry = tabs.get(tabId);
    if (!entry) return;
    entry.model.pushEditOperations([], [{ range: entry.model.getFullModelRange(), text }], () => null);
    notifyChange(tabId);
  },

  insertText(text: string): void {
    const selection = editor?.getSelection();
    if (!editor || !selection) return;
    editor.executeEdits('kusto-explorer', [{ range: selection, text, forceMoveMarkers: true }]);
    editor.pushUndoStop();
    editor.focus();
  },

  selectionText(): string {
    const selection = editor?.getSelection();
    const model = editor?.getModel();
    if (!selection || !model || selection.isEmpty()) return '';
    return model.getValueInRange(selection);
  },

  cursorLine(): number {
    return editor?.getPosition()?.lineNumber ?? 1;
  },

  dismissSuggestions(): void {
    // Closes the completion list and drops completion requests still in flight, so a late
    // IntelliSense answer does not pop up over the editor after a query starts.
    const suggest = editor?.getContribution('editor.contrib.suggestController') as
      | { cancelSuggestWidget?: () => void }
      | null
      | undefined;
    suggest?.cancelSuggestWidget?.();
  },

  format(): void {
    void editor?.getAction('editor.action.formatDocument')?.run();
  },

  async setSchema(schemaJson: string, clusterUri: string, database: string): Promise<void> {
    const model = editor?.getModel() ?? tabs.values().next().value?.model;
    if (!model) return;
    const accessor = await getKustoWorker();
    const worker = await accessor(model.uri);
    await worker.setSchemaFromShowSchema(JSON.parse(schemaJson), clusterUri, database);
  },

  setTheme(dark: boolean): void {
    theme = dark ? DARK_THEME : LIGHT_THEME;
    monaco.editor.setTheme(theme);
  },

  setFontSize(size: number): void {
    fontSize = size;
    editor?.updateOptions({ fontSize });
  },

  focus(): void {
    editor?.focus();
  },

  layout(): void {
    editor?.layout();
  },

  onChange(listener: ChangeListener): void {
    changeListeners.push(listener);
  },

  onCursor(listener: CursorListener): void {
    cursorListeners.push(listener);
  },

  onCommand(listener: CommandListener): void {
    commandListeners.push(listener);
  },
};

(globalThis as unknown as { __kxEditorHost: typeof host }).__kxEditorHost = host;

async function boot(): Promise<void> {
  const ui = (await import(UI_MODULE)) as { default: (options: { module_or_path: string }) => Promise<unknown> };
  await ui.default({ module_or_path: UI_WASM });
}

boot().catch((error: unknown) => {
  const root = document.getElementById('app');
  if (root) {
    root.textContent = `kusto-explorer failed to start: ${error instanceof Error ? error.message : String(error)}`;
    root.className = 'kx-boot-error';
  }
  throw error;
});
