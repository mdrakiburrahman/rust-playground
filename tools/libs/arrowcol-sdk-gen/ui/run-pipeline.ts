export interface PipelineRun {
  id: string;
  status: string;
  phase: string;
  createdAt: string;
  finishedAt?: string;
  request: { mode: 'create' | 'rebuild'; codegen?: string; protoEntry?: string; table?: string };
  steps?: { phase: string; status: string; startedAt: string; finishedAt?: string; error?: string }[];
}
export function pipelineSteps(run: PipelineRun) {
  const create = run.request.mode === 'create';
  const protoc = run.request.codegen === 'protoc';
  const definitions = [
    { phase: 'submitted', label: create ? 'UI / CLI -> DDL' : 'Rebuild request', description: create
      ? 'The submitted table definition is persisted with the run. No catalogue mutation has happened.'
      : 'Select an existing registered table and exact schema revision. No CREATE will be replayed.', document: create ? 'ddl' : 'log' },
    { phase: 'validating', label: 'Validate definition', description: 'Check compiled option defaults, toolchain identity, target connection and the Rust DDL parser.', document: create ? 'ddl' : 'log' },
    { phase: create ? 'preparing' : 'pinning', label: create ? 'Prepare schema + row proto' : 'Read committed schema',
      description: create ? 'Read-only catalogue preparation freezes IDs, Arrow schema, properties and row .proto. The table is not registered yet.'
        : 'Export the pinned registered schema and its descriptors without changing the table.', document: 'schema' },
    { phase: 'exporting-proto', label: 'Export compiler .proto', description: 'Export the configured entry, property options and the lossless Arrowcol IR/API binding.', document: 'compiler', skip: !protoc },
    { phase: 'validating-proto', label: 'Validate protobuf', description: 'Run protoc without an SDK plugin to parse the entry, resolve imports/options and produce a descriptor set. Semantic IR/API validation follows inside the Arrowcol plugin.', document: 'compiler', skip: !protoc },
    { phase: 'generating', label: protoc ? 'Arrowcol protoc -> SDK' : 'Direct renderer -> SDK',
      description: protoc ? 'Invoke protoc-gen-arrowcol-sdk. It checks the canonical IR, row descriptor and API binding, then emits Rust and TypeScript SDKs.'
        : 'Render both SDKs directly from the same canonical contract. No protoc plugin is invoked.', document: 'sdk' },
    { phase: 'verifying', label: 'Verify generated SDKs', description: 'Compare canonical output, typecheck TypeScript, run generated TypeScript/Rust tests, regenerate and check again.', document: 'log' },
    { phase: 'packaging', label: 'Stage immutable archive', description: 'Package contract, SDKs and provenance deterministically. Record the archive digest; it is not published yet.', document: 'release' },
    { phase: 'finalizing', label: 'Recheck staged contract', description: 'Recheck the target authority, compiled defaults and retained candidate/archive hashes before registration or publication.', document: 'manifest' },
    { phase: 'promoting', label: 'Register in target catalogue', description: 'Promote only the exact verified candidate to the selected authority and validate committed read-back. An uncertain reply is not reported as success.', document: 'manifest', skip: !create },
    { phase: 'publishing', label: 'Publish SDK download', description: 'Publish the verified archive at a create-only content-addressed path and expose its download link.', document: 'release' },
  ];
  const firstRecorded = definitions.findIndex((step) => step.phase === run.steps?.[0]?.phase);
  return definitions.map((step, index) => {
    const attempts = run.steps?.filter((item) => item.phase === step.phase) ?? [];
    const latest = attempts.at(-1);
    const status = step.phase === 'submitted' ? 'succeeded' : step.skip ? 'skipped' : latest?.status ??
      (run.phase === step.phase ? run.status === 'failed' ? 'failed' : 'running'
        : run.steps && !(firstRecorded > index) ? 'pending' : 'not-recorded');
    return { ...step, status, attempts, startedAt: step.phase === 'submitted' ? run.createdAt : latest?.startedAt,
      finishedAt: step.phase === 'submitted' ? run.createdAt : latest?.finishedAt };
  });
}
