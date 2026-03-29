const { execSync } = require("node:child_process");

function run(cmd) {
  return execSync(cmd, { encoding: "utf8", stdio: ["pipe", "pipe", "pipe"] }).trim();
}

function parseJsonArray(text) {
  try {
    return JSON.parse(text);
  } catch (err) {
    throw new Error(`Failed to parse JSON output: ${text}`);
  }
}

function sql(query) {
  const escaped = query.replace(/"/g, '\\"').replace(/\n/g, " ");
  const cmd = `supabase db query --json "${escaped}"`;
  return parseJsonArray(run(cmd));
}

function main() {
  const targetRunning = Number(process.argv[2] || 2);
  const countryCode = process.argv[3] || "LU";

  if (!Number.isInteger(targetRunning) || targetRunning <= 0) {
    throw new Error("Usage: node places_control_loop.cjs <target_running> [country_code]");
  }

  const resetRows = sql(`
    select public.reset_stale_place_seed_targets_v1() as reset_count;
  `);
  const resetCount = Number(resetRows[0]?.reset_count || 0);

  const runningRows = sql(`
    select count(*)::int as running_targets
    from public.place_seed_targets
    where status = 'running';
  `);
  const runningTargets = Number(runningRows[0]?.running_targets || 0);

  const pendingRows = sql(`
    select count(*)::int as pending_or_retry_targets
    from public.place_seed_targets t
    join public.place_seed_regions r
      on r.id = t.region_id
    where r.country_code = '${countryCode}'
      and t.status in ('pending', 'retry');
  `);
  const pendingTargets = Number(pendingRows[0]?.pending_or_retry_targets || 0);

  let released = 0;

  if (runningTargets < targetRunning && pendingTargets > 0) {
    run(`node ~/mew-pull-worker/release_places_seed_batch.cjs 1 ${countryCode}`);
    released = 1;
  }

  console.log(JSON.stringify({
    countryCode,
    targetRunning,
    resetCount,
    runningTargets,
    pendingTargets,
    released
  }));
}

try {
  main();
} catch (err) {
  console.error(err.message || String(err));
  process.exit(1);
}
