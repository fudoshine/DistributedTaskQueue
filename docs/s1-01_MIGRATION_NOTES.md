- Expansion adds storage fields; application routing behavior remains unchanged.
- Backfill requires operator-provided legacy tenant and execution-contract mappings. Unknown ownership blocks cutover; no default tenant is invented.
- Cutover requires verified backfill, required-field enforcement, and blocking old writers from creating incomplete jobs.
- Production rollback preserves columns and accepted contracts. The destructive down migration is for disposable-test rollback.
- Verified on PostgreSQL 18.3 (Homebrew) using a disposable cluster.
- Migration 6→7, rollback 7→6, and reapply 6→7 passed.
- Existing workflows, jobs, dependencies, and outbox rows were preserved.
- All six new columns had the expected types, allowed NULL, had no
  defaults, and remained NULL for existing rows.