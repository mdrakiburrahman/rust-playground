# Repository instructions

## Languages and tooling

- Use Rust for application and data-plane code, and TypeScript for automation,
  infrastructure orchestration, developer tooling, test helpers and validators.
- Extend the existing Node.js/Nx tooling instead of introducing Python scripts
  or a second scripting runtime. Use `npx --no-install tsx` for TypeScript scripts
  and `node:test` through `tsx --test` for their tests.
- Keep Azure infrastructure declarative in Bicep. Implement deployment sequencing,
  REST/KQL setup and recovery logic in TypeScript.
- A new scripting language requires explicit user approval; convenience or standard
  library availability alone is not a reason to introduce one.
- When changing a script's language or entry point, update every caller, Nx target,
  CI check, fixture and documented command, and preserve its safety invariants and
  regression coverage. Run the relevant tests and TypeScript type checks.
