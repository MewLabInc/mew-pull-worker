# Mew pull-worker engineering

Codex and Claude Code are peer engineering lanes in this repository.

## Scope and authority

`index.js` implements a long-running Pub/Sub pull subscriber, event dispatch,
USDA ingestion, place seeding, menu crawling/extraction and enrichment handlers.
`Dockerfile` packages that entrypoint. Backup/patch files are historical helpers,
not alternate current implementations or tests.

**Live production ownership: UNKNOWN.** Backoffice contains overlapping migrated
handlers. Before changing processing responsibility, verify the actual deployed
revision and subscription destination through an authorized task-time check;
neither this tree nor migration notes establish active ownership or retirement.

Code/Git establishes implementation and HEAD; Issues/PRs establish task scope;
tests and runtime evidence prove only their recorded revision/environment.
[Mew Brain](https://github.com/MewLabInc/mew-brain) supplies repository/system/platform
navigation and optional workstream context, not runtime truth or authorization.
Read one relevant Brain entry, then current code. Keep private Brain content and
operational data out of commits.

## Autonomous work and hard boundaries

- On an authorized bounded task, either lane may investigate, edit, test, repair,
  commit, push a task branch and create/update a **draft PR** without repeated
  approval. Preserve unrelated work; do not expand into cleanup or other repos.
- Start a new task branch from current `origin/main` (`codex/` for Codex). Track
  scope in an Issue. For handoffs, continue the existing Issue/PR/branch after
  verifying released ownership and an accessible clean Git checkpoint.
- Leave main pushes, merges, deployments/promotions, production SQL and live
  IAM/cloud configuration to humans. Never create, purge, delete, replay or retarget
  live queues/topics/subscriptions as diagnosis. Ready/force-push actions require
  explicit approval; do not change global Git configuration.
- Do not read, request, print or commit secrets, credentials or `.env` contents.
  Do not acquire service-role keys, export customer rows or inspect raw production
  logs/debug payloads as routine verification. Never weaken RLS or access controls.
- Database/schema/RLS, routing and dependency changes need explicit task scope.
  RPC call sites are not proof of live objects/signatures/grants. Draft necessary
  SQL with verification/negative checks and rollback for human review/execution.
  Ask only for missing authorization, material product choices, sensitive expansion
  or validation that cannot be completed within scope.

## Worker cautions

- Trace `handlers`, `validateEnvelope`, `persistWorkerEvent`, `classifyError` and
  the subscription callback in `index.js`. Duplicate insertion into `worker_events`
  can return without preventing handler execution: do not claim exactly-once effects.
  Changes need evidence for repeated delivery and handler-specific idempotency.
- Preserve `ackOnce`/`nackOnce`, permanent-versus-transient classification, bounded
  concurrency/body size, quotas and USDA retry/dead queue transitions. ACK may mean
  a dropped/permanent failure; neither ACK nor HTTP health proves processing success.
- `init` uses `loadWorkerConfig` for a privileged Supabase client; startup probes the
  crawler and starts pulling messages. Project/topic/subscription and crawler
  defaults are not a safe local sandbox. Extraction GET requests can invoke work.
  Preserve source provenance and confirmed data; AI output is not nutrition truth.
  USDA ingestion is upstream of Mew-owned data, not runtime Resolver authority.
- `deploy.sh` stages all files, commits, pushes and deploys Cloud Run: never run it
  as validation. Queue/control and one-off patch scripts are not test commands.
  `alert-policy.json` and `backlog-policy.json` record intent, not live monitoring.

## Verification and completion

Run `node --check index.js` for non-executing JavaScript syntax validation and
`git diff --check` for whitespace; verify instruction links/paths for docs-only work.
`package.json` defines only `npm start`, which executes the worker, **not a test**.
No test/lint/build script or test suite is currently present. Do not install
dependencies, start the worker or build/deploy a container solely to validate prose.
For logic changes, add scoped deterministic fixtures when needed; syntax alone
does not verify retry/idempotency, database effects or deployed behavior.

Report changed files/behavior, actual checks/results, pending evidence/manual QA,
Issue/branch/commit/PR and any handoff next action. Keep live ownership and schema
facts UNKNOWN until verified; local checks never establish production readiness.
