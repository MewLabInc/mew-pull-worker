import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createUsdaSeedTickHandler, USDA_STREAMS, normalizeUsdaStreamKey } from "../lib/usda.js";
import { createFakeSupabase, jsonResponse } from "../testsupport/fake-supabase.js";

const page = JSON.parse(readFileSync(new URL("./fixtures/usda-list-page.json", import.meta.url)));

function setup({ results = {}, pageBody = page, fetchResp, cfg = { USDA_API_KEY: "test-key-invented" } } = {}) {
  const supabase = createFakeSupabase({
    "usda_seed_queue.select": { count: 0 },
    "usda_catalog_cursor.select": { data: null },
    "rpc.claim_usda_seed_batch_v1": { data: [] },
    ...results,
  });
  const urls = [];
  const fetch = async (url) => { urls.push(url); return fetchResp ?? jsonResponse(pageBody); };
  const published = [];
  const logs = [];
  const handler = createUsdaSeedTickHandler({
    supabase, fetch, workerCfg: cfg,
    jlog: (e, p) => logs.push([e, p]),
    publishFoodEvent: async (env) => { published.push(env); },
  });
  return { supabase, urls, published, logs, handler };
}
const tick = (stream_key, extra = {}) => ({
  envelope: { payload: { ...(stream_key === undefined ? {} : { stream_key }), ...extra } },
  meta: { msgId: "m1" },
});
const ops = (s, table, op) => s.calls.filter((c) => c.kind === "from" && c.table === table && c.op === op);

// AC1
for (const [key, dt] of Object.entries(USDA_STREAMS)) {
  test(`AC1 stream ${key}: list URL carries dataType=${dt}, stable sort, cursor keyed by stream`, async () => {
    const t = setup();
    await t.handler(tick(key));
    assert.equal(t.urls.length, 1);
    const u = new URL(t.urls[0]);
    assert.equal(u.pathname, "/fdc/v1/foods/list");
    assert.equal(u.searchParams.get("dataType"), dt);
    assert.equal(u.searchParams.get("sortBy"), "fdcId");
    assert.equal(u.searchParams.get("sortOrder"), "asc");
    const read = ops(t.supabase, "usda_catalog_cursor", "select")[0];
    assert.deepEqual(read.filters, [["stream_key", key]]);
    const write = ops(t.supabase, "usda_catalog_cursor", "upsert")[0];
    assert.equal(write.args.stream_key, key);
    assert.deepEqual(write.opts, { onConflict: "stream_key" });
  });
}

test("AC1 default stream is foundation", async () => {
  const t = setup();
  await t.handler(tick(undefined));
  assert.equal(new URL(t.urls[0]).searchParams.get("dataType"), "Foundation");
  assert.deepEqual(ops(t.supabase, "usda_catalog_cursor", "select")[0].filters, [["stream_key", "foundation"]]);
});

test("AC1 stream_key accepted by dataType label and case-insensitively", () => {
  assert.equal(normalizeUsdaStreamKey("Survey (FNDDS)"), "fndds");
  assert.equal(normalizeUsdaStreamKey(" BRANDED "), "branded");
  assert.equal(normalizeUsdaStreamKey("nope"), null);
});

test("AC1 non-foundation stream does not touch the foundation cursor", async () => {
  const t = setup();
  await t.handler(tick("branded"));
  const keys = t.supabase.calls.filter((c) => c.table === "usda_catalog_cursor").flatMap((c) => c.filters.map((f) => f[1]).concat(c.args?.stream_key ?? []));
  assert.ok(keys.length > 0);
  assert.ok(keys.every((k) => k === "branded"));
});

test("AC1 invalid stream_key throws ENVELOPE:invalid_usda_stream before any I/O", async () => {
  const t = setup();
  await assert.rejects(t.handler(tick("bogus")), (e) => e.message === "ENVELOPE:invalid_usda_stream" && e.status === 400);
  assert.equal(t.urls.length, 0);
  assert.equal(t.supabase.calls.length, 0);
});

// AC2
test("AC2 short page wraps cursor to 1", async () => {
  const t = setup({ results: { "usda_catalog_cursor.select": { data: { page_number: 7, page_size: 5 } } } });
  await t.handler(tick("sr_legacy"));
  assert.equal(new URL(t.urls[0]).searchParams.get("pageNumber"), "7");
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert")[0].args.page_number, 1);
});

test("AC2 full page advances cursor to page+1 and keeps page_size", async () => {
  const t = setup({ results: { "usda_catalog_cursor.select": { data: { page_number: 7, page_size: 3 } } } });
  await t.handler(tick("sr_legacy"));
  const w = ops(t.supabase, "usda_catalog_cursor", "upsert")[0].args;
  assert.equal(w.page_number, 8);
  assert.equal(w.page_size, 3);
});

test("AC2 empty page wraps to 1; missing cursor row starts at page 1 size 200", async () => {
  const t = setup({ pageBody: [] });
  await t.handler(tick("fndds"));
  const u = new URL(t.urls[0]);
  assert.equal(u.searchParams.get("pageNumber"), "1");
  assert.equal(u.searchParams.get("pageSize"), "200");
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert")[0].args.page_number, 1);
  assert.equal(ops(t.supabase, "usda_seed_queue", "upsert").length, 0, "no enqueue for empty page");
});

test("AC2 accepts { foods: [...] } response shape", async () => {
  const t = setup({ pageBody: { foods: page }, results: { "usda_catalog_cursor.select": { data: { page_number: 2, page_size: 3 } } } });
  await t.handler(tick("foundation"));
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert")[0].args.page_number, 3);
});

// AC3
test("AC3 queue rows carry source_stream_key and usda_data_type (item value or stream fallback)", async () => {
  const t = setup();
  await t.handler(tick("foundation"));
  const up = ops(t.supabase, "usda_seed_queue", "upsert")[0];
  assert.deepEqual(up.opts, { onConflict: "fdc_id", ignoreDuplicates: true });
  assert.deepEqual(up.args, [
    { fdc_id: "900101", status: "queued", source_stream_key: "foundation", usda_data_type: "Foundation" },
    { fdc_id: "900102", status: "queued", source_stream_key: "foundation", usda_data_type: "Foundation" },
    { fdc_id: "900103", status: "queued", source_stream_key: "foundation", usda_data_type: "SR Legacy" },
  ]);
});

test("AC3 fallback uses the stream's mapped type for each stream", async () => {
  for (const [key, dt] of Object.entries(USDA_STREAMS)) {
    const t = setup({ pageBody: [{ fdcId: 900200 }] });
    await t.handler(tick(key));
    const row = ops(t.supabase, "usda_seed_queue", "upsert")[0].args[0];
    assert.equal(row.source_stream_key, key);
    assert.equal(row.usda_data_type, dt);
  }
});

test("AC3 items without fdcId are dropped", async () => {
  const t = setup({ pageBody: [{ dataType: "Foundation" }, { fdcId: 900300 }] });
  await t.handler(tick("foundation"));
  assert.equal(ops(t.supabase, "usda_seed_queue", "upsert")[0].args.length, 1);
});

// order / publish
test("order: queue count -> cursor read -> enqueue -> cursor write -> claim -> publish", async () => {
  const t = setup({ results: { "rpc.claim_usda_seed_batch_v1": { data: [{ fdc_id: "900101" }, { fdc_id: "900102" }] } } });
  await t.handler(tick("foundation", { batch_size: 10 }));
  const seq = t.supabase.calls.map((c) => (c.kind === "rpc" ? `rpc:${c.name}` : `${c.table}.${c.op}`));
  assert.deepEqual(seq, [
    "usda_seed_queue.select", "usda_catalog_cursor.select", "usda_seed_queue.upsert",
    "usda_catalog_cursor.upsert", "rpc:claim_usda_seed_batch_v1",
  ]);
  assert.deepEqual(t.supabase.calls.at(-1).args, { p_batch_size: 10 });
  assert.deepEqual(t.published.map((p) => p.dedupe_key), ["usda_seed_900101", "usda_seed_900102"]);
  assert.equal(t.published[0].event_type, "food.usda.ingest.requested");
});

test("queue already >= batch_size: no USDA fetch, no cursor change, still claims", async () => {
  const t = setup({ results: { "usda_seed_queue.select": { count: 80 } } });
  await t.handler(tick("foundation"));
  assert.equal(t.urls.length, 0);
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert").length, 0);
  assert.equal(t.supabase.calls.filter((c) => c.name === "claim_usda_seed_batch_v1").length, 1);
});

test("queue count uses exact count + head on status=queued", async () => {
  const t = setup();
  await t.handler(tick("foundation"));
  const c = ops(t.supabase, "usda_seed_queue", "select")[0];
  assert.deepEqual(c.opts, { count: "exact", head: true });
  assert.deepEqual(c.filters, [["status", "queued"]]);
});

// negatives
test("neg: cursor read error throws usda_cursor_read_failed, no fetch", async () => {
  const t = setup({ results: { "usda_catalog_cursor.select": { error: { message: "boom" } } } });
  await assert.rejects(t.handler(tick("foundation")), /usda_cursor_read_failed: boom/);
  assert.equal(t.urls.length, 0);
});

test("neg: queue count error throws usda_seed_queue_count_failed", async () => {
  const t = setup({ results: { "usda_seed_queue.select": { error: { message: "x" } } } });
  await assert.rejects(t.handler(tick("foundation")), /usda_seed_queue_count_failed: x/);
});

test("neg: enqueue error throws and cursor is NOT advanced", async () => {
  const t = setup({ results: { "usda_seed_queue.upsert": { error: { message: "e" } } } });
  await assert.rejects(t.handler(tick("foundation")), /usda_catalog_enqueue_failed: e/);
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert").length, 0);
});

test("neg: cursor update error throws usda_cursor_update_failed", async () => {
  const t = setup({ results: { "usda_catalog_cursor.upsert": { error: { message: "u" } } } });
  await assert.rejects(t.handler(tick("foundation")), /usda_cursor_update_failed: u/);
});

test("neg: claim error throws usda_seed_claim_failed, nothing published", async () => {
  const t = setup({ results: { "rpc.claim_usda_seed_batch_v1": { error: { message: "c" } } } });
  await assert.rejects(t.handler(tick("foundation")), /usda_seed_claim_failed: c/);
  assert.equal(t.published.length, 0);
});

test("neg: missing API key -> ENVELOPE:missing_usda_api_key, no cursor write", async () => {
  const t = setup({ cfg: {} });
  await assert.rejects(t.handler(tick("foundation")), (e) => e.message === "ENVELOPE:missing_usda_api_key" && e.status === 400);
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert").length, 0);
});

test("neg: USDA list HTTP 500 throws with status, cursor not advanced", async () => {
  const t = setup({ fetchResp: jsonResponse({}, { status: 500, statusText: "Err" }) });
  await assert.rejects(t.handler(tick("foundation")), (e) => e.status === 500 && /USDA page fetch failed/.test(e.message));
  assert.equal(ops(t.supabase, "usda_catalog_cursor", "upsert").length, 0);
});

test("neg: publish failure after cursor advance propagates (cursor already written)", async () => {
  const supabase = createFakeSupabase({
    "usda_seed_queue.select": { count: 0 },
    "rpc.claim_usda_seed_batch_v1": { data: [{ fdc_id: "900101" }] },
  });
  const h = createUsdaSeedTickHandler({
    supabase, fetch: async () => jsonResponse(page), workerCfg: { USDA_API_KEY: "k" }, jlog() {},
    publishFoodEvent: async () => { throw new Error("pubsub down"); },
  });
  await assert.rejects(h(tick("foundation")), /pubsub down/);
  assert.equal(ops(supabase, "usda_catalog_cursor", "upsert").length, 1);
});

// idempotence documentation
test("repeat delivery: same tick twice re-reads cursor, re-fetches and re-upserts (no tick-level dedupe in lib)", async () => {
  const t = setup();
  const env = tick("foundation");
  await t.handler(env);
  await t.handler(env);
  assert.equal(t.urls.length, 2);
  assert.equal(ops(t.supabase, "usda_seed_queue", "upsert").length, 2);
  // Row-level dedupe exists only via DB: ignoreDuplicates on fdc_id (unverified live).
  for (const u of ops(t.supabase, "usda_seed_queue", "upsert")) assert.equal(u.opts.ignoreDuplicates, true);
});
