import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createUsdaIngestHandler, extractUsdaMacrosPer100g, fetchUsdaFood } from "../lib/usda.js";
import { createFakeSupabase, jsonResponse } from "../testsupport/fake-supabase.js";

const detail = JSON.parse(readFileSync(new URL("./fixtures/usda-food-detail.json", import.meta.url)));
const FOOD_ID = "00000000-0000-4000-8000-000000000001";

function setup({ results = {}, fetchResp, cfg = { USDA_API_KEY: "test-key-invented" }, quota = true, upsertId = FOOD_ID } = {}) {
  const supabase = createFakeSupabase(results);
  const order = []; // unified order across rpcs and supabase
  const rpcCalls = [];
  const evidence = [];
  const urls = [];
  const logs = [];
  const handler = createUsdaIngestHandler({
    supabase,
    fetch: async (url) => { urls.push(url); return fetchResp ?? jsonResponse(detail); },
    workerCfg: cfg,
    jlog: (e, p) => logs.push([e, p]),
    tryConsumeDailyQuota: async (a) => { order.push("quota"); return quota; },
    rpcOrThrow: async (name, args) => {
      order.push(`rpc:${name}`); rpcCalls.push({ name, args });
      return name === "upsert_food_item_from_usda_v1" ? upsertId : null;
    },
    rpcUpsertMacroEvidence: async (a) => { order.push("evidence"); evidence.push(a); },
    quotaKey: "usda_daily",
    dailyLimit: 1000,
  });
  return { supabase, order, rpcCalls, evidence, urls, logs, handler };
}
const env = (payload = { fdc_id: "900001" }, extra = {}) => ({ envelope: { payload, ...extra }, meta: { msgId: "m1" } });
const updates = (s, table) => s.calls.filter((c) => c.table === table && c.op === "update");

// AC4
test("AC4 after upsert returns id, food_items updated with usda_data_type/source_stream_key", async () => {
  const t = setup();
  await t.handler(env());
  const u = updates(t.supabase, "food_items");
  assert.equal(u.length, 1);
  assert.deepEqual(u[0].args, { usda_data_type: "Foundation", source_stream_key: "foundation" });
  assert.deepEqual(u[0].filters, [["id", FOOD_ID], ["source", "usda"], ["source_id", "900001"]]);
  assert.ok(t.order.indexOf("rpc:upsert_food_item_from_usda_v1") < t.order.length);
});

test("AC4 known dataType 'SR Legacy' writes { usda_data_type, source_stream_key: sr_legacy }", async () => {
  const t = setup({ fetchResp: jsonResponse({ ...detail, dataType: "SR Legacy" }) });
  await t.handler(env());
  assert.deepEqual(updates(t.supabase, "food_items")[0].args, { usda_data_type: "SR Legacy", source_stream_key: "sr_legacy" });
});

test("AC4 unknown dataType writes exactly { usda_data_type: raw } with no source_stream_key key", async () => {
  const t = setup({ fetchResp: jsonResponse({ ...detail, dataType: "Invented Type" }) });
  await t.handler(env());
  const u = updates(t.supabase, "food_items");
  assert.equal(u.length, 1);
  assert.deepEqual(u[0].args, { usda_data_type: "Invented Type" });
  assert.equal("source_stream_key" in u[0].args, false);
});

test("AC4 detail without dataType: no food_items update, skip logged, full RPC order still holds", async () => {
  const { dataType, ...noType } = detail;
  const t = setup({ fetchResp: jsonResponse(noType) });
  await t.handler(env());
  assert.equal(t.supabase.calls.filter((c) => c.table === "food_items").length, 0);
  const skip = t.logs.filter(([e]) => e === "usda_metadata_skipped_no_data_type");
  assert.equal(skip.length, 1);
  assert.deepEqual(skip[0][1], { msgId: "m1", fdcId: "900001", food_id: FOOD_ID });
  assert.ok(!t.logs.some(([e]) => e === "usda_metadata_ok"));
  assert.deepEqual(t.order, [
    "quota",
    "rpc:upsert_food_item_from_usda_v1",
    "evidence", "evidence", "evidence", "evidence", "evidence", "evidence",
    "rpc:resolve_food_item_macros_v1_2",
    "rpc:compute_food_item_quality_rollup_v1",
    "rpc:compute_food_item_seo_v2",
    "rpc:apply_food_item_cluster_seo_gate",
  ]);
});

test("AC4 whitespace-only dataType is treated as missing (no update)", async () => {
  const t = setup({ fetchResp: jsonResponse({ ...detail, dataType: "   " }) });
  await t.handler(env());
  assert.equal(t.supabase.calls.filter((c) => c.table === "food_items").length, 0);
});

test("AC4 metadata update error throws usda_metadata_update_failed, no evidence written", async () => {
  const t = setup({ results: { "food_items.update": { error: { message: "m" } } } });
  await assert.rejects(t.handler(env()), /usda_metadata_update_failed: m/);
  assert.equal(t.evidence.length, 0);
});

// AC5
test("AC5 full order: upsert -> evidence x6 -> resolve -> rollup -> seo -> cluster gate", async () => {
  const t = setup();
  await t.handler(env());
  assert.deepEqual(t.order, [
    "quota",
    "rpc:upsert_food_item_from_usda_v1",
    "evidence", "evidence", "evidence", "evidence", "evidence", "evidence",
    "rpc:resolve_food_item_macros_v1_2",
    "rpc:compute_food_item_quality_rollup_v1",
    "rpc:compute_food_item_seo_v2",
    "rpc:apply_food_item_cluster_seo_gate",
  ]);
});

test("AC5 seo compute and cluster gate share the same p_food_item_id", async () => {
  const t = setup();
  await t.handler(env());
  const seo = t.rpcCalls.find((c) => c.name === "compute_food_item_seo_v2");
  const gate = t.rpcCalls.find((c) => c.name === "apply_food_item_cluster_seo_gate");
  assert.deepEqual(seo.args, { p_food_item_id: FOOD_ID });
  assert.deepEqual(gate.args, { p_food_item_id: FOOD_ID });
});

test("AC5 seo failure skips the gate and queue-done update", async () => {
  const supabase = createFakeSupabase();
  const names = [];
  const h = createUsdaIngestHandler({
    supabase, fetch: async () => jsonResponse(detail), workerCfg: { USDA_API_KEY: "k" }, jlog() {},
    tryConsumeDailyQuota: async () => true,
    rpcOrThrow: async (n) => { names.push(n); if (n === "compute_food_item_seo_v2") throw new Error("seo bad"); return FOOD_ID; },
    rpcUpsertMacroEvidence: async () => {}, quotaKey: "q", dailyLimit: 1,
  });
  await assert.rejects(h(env()), /seo bad/);
  assert.ok(!names.includes("apply_food_item_cluster_seo_gate"));
  assert.equal(supabase.calls.filter((c) => c.table === "usda_seed_queue").length, 0);
});

test("evidence payloads: six macros with fixture values, source usda, fdc ref", async () => {
  const t = setup();
  await t.handler(env({ fdc_id: "900001" }, { occurred_at: "2030-01-01T00:00:00.000Z" }));
  assert.deepEqual(
    t.evidence.map((e) => [e.macro_key, e.value_per_100g]),
    [["calories_kcal_per_100g", 111.5], ["protein_g_per_100g", 7.25], ["carbs_g_per_100g", 19.5],
     ["fat_g_per_100g", 0.75], ["fiber_g_per_100g", 6.5], ["sugar_g_per_100g", 1.25]]
  );
  for (const e of t.evidence) {
    assert.equal(e.source, "usda"); assert.equal(e.source_ref, "fdc:900001");
    assert.equal(e.food_item_id, FOOD_ID); assert.equal(e.evidence_confidence, 1.0);
    assert.equal(e.occurred_at, "2030-01-01T00:00:00.000Z");
  }
});

test("missing macro is skipped, not written as 0", async () => {
  const body = { ...detail, foodNutrients: detail.foodNutrients.filter((n) => n.nutrient.id !== 1079) };
  const t = setup({ fetchResp: jsonResponse(body) });
  await t.handler(env());
  assert.equal(t.evidence.length, 5);
  assert.ok(!t.evidence.some((e) => e.macro_key === "fiber_g_per_100g"));
});

test("source_url sent to upsert has no api_key; name falls back when description missing", async () => {
  const t = setup({ fetchResp: jsonResponse({ ...detail, description: "  " }) });
  await t.handler(env());
  const a = t.rpcCalls[0].args;
  assert.equal(a.p_source_url, "https://api.nal.usda.gov/fdc/v1/food/900001");
  assert.equal(a.p_name, "USDA 900001");
  assert.equal(a.p_source_id, "900001");
});

test("queue done update: status leased -> done on success", async () => {
  const t = setup();
  await t.handler(env());
  const u = updates(t.supabase, "usda_seed_queue");
  assert.equal(u.length, 1);
  assert.equal(u[0].args.status, "done");
  assert.deepEqual(u[0].filters, [["fdc_id", "900001"], ["status", "leased"]]);
});

test("queue done update error is swallowed (logged), handler resolves", async () => {
  const t = setup({ results: { "usda_seed_queue.update": { error: { message: "q" } } } });
  await t.handler(env());
  assert.ok(t.logs.some(([e]) => e === "usda_seed_queue_done_update_failed"));
});

test("source_food_id takes precedence over fdc_id", async () => {
  const t = setup();
  await t.handler(env({ source_food_id: "900777", fdc_id: "900001" }));
  assert.ok(t.urls[0].includes("/food/900777?"));
});

// negatives
test("neg: missing id -> ENVELOPE:missing_source_food_id before quota", async () => {
  const t = setup();
  await assert.rejects(t.handler(env({})), (e) => e.message === "ENVELOPE:missing_source_food_id" && e.status === 400);
  assert.deepEqual(t.order, []);
});

test("neg: quota exhausted -> resolves, no fetch, no DB writes", async () => {
  const t = setup({ quota: false });
  await t.handler(env());
  assert.deepEqual(t.order, ["quota"]);
  assert.equal(t.urls.length, 0);
  assert.equal(t.supabase.calls.length, 0);
  assert.ok(t.logs.some(([e]) => e === "quota_exhausted_skip"));
});

test("neg: USDA 404 -> ENVELOPE:usda_not_found, queue row marked dead (leased only)", async () => {
  const t = setup({ fetchResp: jsonResponse({}, { status: 404, statusText: "NF" }) });
  await assert.rejects(t.handler(env()), (e) => e.message === "ENVELOPE:usda_not_found" && e.status === 404);
  const u = updates(t.supabase, "usda_seed_queue");
  assert.equal(u.length, 1);
  assert.equal(u[0].args.status, "dead");
  assert.equal(u[0].args.last_error, "ENVELOPE:usda_not_found");
  assert.deepEqual(u[0].filters, [["fdc_id", "900001"], ["status", "leased"]]);
  assert.equal(t.rpcCalls.length, 0);
});

test("neg: 404 with dead-update error still throws not_found", async () => {
  const t = setup({ fetchResp: jsonResponse({}, { status: 404 }), results: { "usda_seed_queue.update": { error: { message: "d" } } } });
  await assert.rejects(t.handler(env()), /ENVELOPE:usda_not_found/);
});

test("neg: USDA 503 is transient: throws with status, queue row untouched in lib", async () => {
  const t = setup({ fetchResp: jsonResponse({}, { status: 503, statusText: "Unavail" }) });
  await assert.rejects(t.handler(env()), (e) => e.status === 503 && !e.message.startsWith("ENVELOPE:"));
  assert.equal(updates(t.supabase, "usda_seed_queue").length, 0);
});

test("neg: missing API key -> ENVELOPE:missing_usda_api_key (quota already consumed)", async () => {
  const t = setup({ cfg: {} });
  await assert.rejects(t.handler(env()), (e) => e.message === "ENVELOPE:missing_usda_api_key");
  assert.deepEqual(t.order, ["quota"]);
});

test("neg: upsert returns null -> throws, no metadata/evidence", async () => {
  const t = setup({ upsertId: null });
  await assert.rejects(t.handler(env()), /upsert_food_item_from_usda_v1 returned null/);
  assert.equal(updates(t.supabase, "food_items").length, 0);
  assert.equal(t.evidence.length, 0);
});

// idempotence
test("repeat delivery: same ingest twice repeats quota, fetch, RPCs and evidence (no handler-level dedupe)", async () => {
  const t = setup();
  await t.handler(env());
  await t.handler(env());
  assert.equal(t.urls.length, 2);
  assert.equal(t.order.filter((o) => o === "quota").length, 2, "quota consumed per delivery");
  assert.equal(t.evidence.length, 12);
  assert.equal(t.rpcCalls.filter((c) => c.name === "apply_food_item_cluster_seo_gate").length, 2);
  // Second done-update targets status=leased so it is a no-op if row already done (DB-side, unverified).
});

// pure helpers
test("extractUsdaMacrosPer100g: name fallback when ids missing; empty input all null", () => {
  const out = extractUsdaMacrosPer100g({ foodNutrients: [{ nutrientName: "Protein", unitName: "G", amount: 3 }] });
  assert.equal(out.protein_g_per_100g, 3);
  assert.equal(out.calories_kcal_per_100g, null);
  assert.equal(Object.values(extractUsdaMacrosPer100g({})).every((v) => v === null), true);
});

test("fetchUsdaFood: URL has api_key, encodes id", async () => {
  let seen;
  await fetchUsdaFood({ workerCfg: { USDA_API_KEY: "k y" }, fetch: async (u) => { seen = u; return jsonResponse({}); } }, "9/1");
  assert.equal(seen, "https://api.nal.usda.gov/fdc/v1/food/9%2F1?api_key=k%20y");
});
