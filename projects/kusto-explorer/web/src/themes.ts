import type * as monaco from 'monaco-editor/editor';

type Palette = Record<string, string>;

const darkTokens: Palette = {
  plainText: 'D4D4D4',
  comment: '6A9955',
  punctuation: 'D4D4D4',
  directive: 'C586C0',
  literal: 'B5CEA8',
  stringLiteral: 'CE9178',
  type: '4EC9B0',
  column: '9CDCFE',
  table: '4EC9B0',
  database: '4EC9B0',
  function: 'DCDCAA',
  parameter: '9CDCFE',
  variable: '9CDCFE',
  identifier: 'D4D4D4',
  clientParameter: '4FC1FF',
  queryParameter: '4FC1FF',
  scalarParameter: '4FC1FF',
  mathOperator: 'D4D4D4',
  queryOperator: 'C586C0',
  command: '569CD6',
  keyword: '569CD6',
  materializedView: '4EC9B0',
  schemaMember: '9CDCFE',
  signatureParameter: '9CDCFE',
  option: 'D7BA7D',
};

const lightTokens: Palette = {
  plainText: '1F1F1F',
  comment: '008000',
  punctuation: '1F1F1F',
  directive: 'AF00DB',
  literal: '098658',
  stringLiteral: 'A31515',
  type: '267F99',
  column: '001080',
  table: '267F99',
  database: '267F99',
  function: '795E26',
  parameter: '001080',
  variable: '001080',
  identifier: '1F1F1F',
  clientParameter: '0070C1',
  queryParameter: '0070C1',
  scalarParameter: '0070C1',
  mathOperator: '1F1F1F',
  queryOperator: 'AF00DB',
  command: '0000FF',
  keyword: '0000FF',
  materializedView: '267F99',
  schemaMember: '001080',
  signatureParameter: '001080',
  option: '795E26',
};

function rules(palette: Palette): monaco.editor.ITokenThemeRule[] {
  return [
    { token: '', foreground: palette.plainText },
    ...Object.entries(palette).map(([token, foreground]) => ({
      token,
      foreground,
      fontStyle: token === 'comment' ? 'italic' : undefined,
    })),
  ];
}

export const DARK_THEME = 'kx-dark';
export const LIGHT_THEME = 'kx-light';

export function defineThemes(api: typeof monaco): void {
  api.editor.defineTheme(DARK_THEME, {
    base: 'vs-dark',
    inherit: true,
    rules: rules(darkTokens),
    colors: {
      'editor.background': '#1f1f1f',
      'editor.foreground': '#d4d4d4',
      'editor.lineHighlightBackground': '#ffffff08',
      'editor.lineHighlightBorder': '#00000000',
      'editor.selectionHighlightBackground': '#3794ff12',
      'editorLineNumber.foreground': '#6e7681',
      'editorLineNumber.activeForeground': '#cccccc',
      'editorGutter.background': '#1f1f1f',
      'editorWidget.background': '#202020',
      'editorWidget.border': '#313131',
      'editorSuggestWidget.background': '#202020',
      'editorSuggestWidget.border': '#313131',
      'editorSuggestWidget.selectedBackground': '#04395e',
      'editorHoverWidget.background': '#202020',
      'editorHoverWidget.border': '#313131',
      'scrollbarSlider.background': '#79797966',
      'focusBorder': '#0078d4',
    },
  });
  api.editor.defineTheme(LIGHT_THEME, {
    base: 'vs',
    inherit: true,
    rules: rules(lightTokens),
    colors: {
      'editor.background': '#ffffff',
      'editor.foreground': '#1f1f1f',
      'editor.lineHighlightBackground': '#0000000a',
      'editor.lineHighlightBorder': '#00000000',
      'editor.selectionHighlightBackground': '#005fb80d',
      'editorLineNumber.foreground': '#8b8b8b',
      'editorLineNumber.activeForeground': '#171184',
      'editorWidget.background': '#f8f8f8',
      'editorWidget.border': '#e5e5e5',
      'editorSuggestWidget.background': '#f8f8f8',
      'editorSuggestWidget.border': '#e5e5e5',
      'editorSuggestWidget.selectedBackground': '#e8e8e8',
      'editorHoverWidget.background': '#f8f8f8',
      'editorHoverWidget.border': '#e5e5e5',
      'focusBorder': '#005fb8',
    },
  });
}
