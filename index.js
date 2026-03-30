// index.js

import express from "express";
import { PubSub } from "@google-cloud/pubsub";
import { createClient } from "@supabase/supabase-js";

const BUILD_ID = process.env.BUILD_ID || "no-build-id";
const PROJECT_ID =
  process.env.GOOGLE_CLOUD_PROJECT ||
  process.env.GCP_PROJECT ||
  process.env.PUBSUB_PROJECT ||
  "mew-prod-488113";

const SUB_NAME = process.env.PUBSUB_SUB_FOOD || "food-ingest-sub";
const SUB_PATH = SUB_NAME.startsWith("projects/")
  ? SUB_NAME
  : `projects/${PROJECT_ID}/subscriptions/${SUB_NAME}`;

// Publish topic (used by FOOD_ITEM_UPDATED to trigger enrichment)
const TOPIC_NAME = process.env.PUBSUB_TOPIC_FOOD || "food-ingest";
const TOPIC_PATH = TOPIC_NAME.startsWith("projects/")
  ? TOPIC_NAME
  : `projects/${PROJECT_ID}/topics/${TOPIC_NAME}`;

const MAX_MESSAGES = Number(process.env.MAX_MESSAGES || "5");
const MAX_BODY_BYTES = Number(process.env.MAX_BODY_BYTES || String(256 * 1024)); // 256KB
const CRAWLER_EXTRACT_BASE_URL = process.env.CRAWLER_EXTRACT_BASE_URL || "http://136.113.236.245:8000";

// ---- daily quota controls ----
// NOTE: There are two try_consume_daily_quota overloads in your DB history.
// This worker supports BOTH safely via tryConsumeDailyQuota() below.
const USDA_DAILY_LIMIT = Number(process.env.USDA_DAILY_LIMIT || "2000");
const USDA_QUOTA_KEY = process.env.USDA_QUOTA_KEY || "usda_ingest";

const WEBHUNTER_DAILY_LIMIT = Number(process.env.WEBHUNTER_DAILY_LIMIT || "200");
const WEBHUNTER_QUOTA_KEY = process.env.WEBHUNTER_QUOTA_KEY || "webhunter_enrich";
const WEBHUNTER_STUB_RECOMPUTE = (process.env.WEBHUNTER_STUB_RECOMPUTE || "false").toLowerCase() === "true";
const GEOAPIFY_DAILY_LIMIT = Number(process.env.GEOAPIFY_DAILY_LIMIT || "5000");
const GEOAPIFY_MAX_TARGET_OFFSET = Number(process.env.GEOAPIFY_MAX_TARGET_OFFSET || "2000");

let IS_READY = false;
let supabase = null;
let subscription = null;
let workerCfg = null;
let pubsubClient = null;

const stats = {
  startedAt: new Date().toISOString(),
  buildId: BUILD_ID,
  projectId: PROJECT_ID,
  subPath: SUB_PATH,
  recv: 0,
  acked: 0,
  nacked: 0,
  dropped: 0, // acked but not processed (invalid/permanent)
  lastMsgId: null,
  lastErr: null,
  lastDbErrCode: null,
  initOk: false,
};

function jlog(event, data = {}) {
  const payload = {
    event,
    ts: new Date().toISOString(),
    build_id: BUILD_ID,
    service: process.env.K_SERVICE,
    revision: process.env.K_REVISION,
    ...data,
  };
  console.log(
    Object.entries(payload)
      .map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`)
      .join(";")
  );
}

function setErr(err) {
  stats.lastErr = err?.message || String(err);
  stats.lastDbErrCode = err?.code || err?.status || null;
}

process.on("unhandledRejection", (e) => {
  setErr(e);
  jlog("unhandledRejection", { err: stats.lastErr });
});
process.on("uncaughtException", (e) => {
  setErr(e);
  jlog("uncaughtException", { err: stats.lastErr });
});

const app = express();
app.get("/", (_req, res) => res.status(200).send("ok"));
app.get("/healthz", (_req, res) => res.status(200).send("ok"));
app.get("/health", (_req, res) => res.status(200).send("ok"));
app.get("/healthzz", (_req, res) => res.status(200).send("ok"));
app.get("/readyz", (_req, res) =>
  res.status(IS_READY ? 200 : 503).send(IS_READY ? "ready" : "not-ready")
);
app.get("/debug", (_req, res) => res.status(200).json(stats));

function loadWorkerConfig() {
  const raw = process.env.MEW_WORKER_CONFIG;
  if (!raw) throw new Error("Missing env: MEW_WORKER_CONFIG");

  let cfg;
  try {
    cfg = JSON.parse(raw);
  } catch {
    throw new Error("MEW_WORKER_CONFIG is not valid JSON");
  }

  if (!cfg.SUPABASE_URL) throw new Error("MEW_WORKER_CONFIG missing SUPABASE_URL");
  if (!cfg.SUPABASE_SERVICE_ROLE_KEY) {
    throw new Error("MEW_WORKER_CONFIG missing SUPABASE_SERVICE_ROLE_KEY");
  }

  // USDA_API_KEY is optional at config level, but required for the USDA handler to run.
  return cfg;
}

function isUniqueViolation(err) {
  return err && (err.code === "23505" || err?.details?.includes?.("23505"));
}

function mustString(x) {
  return typeof x === "string" && x.trim().length > 0 ? x.trim() : null;
}

function numOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

async function fetchUsdaFood(fdcId) {
  const apiKey = workerCfg?.USDA_API_KEY;
  if (!apiKey) {
    const e = new Error("ENVELOPE:missing_usda_api_key");
    e.status = 400;
    throw e;
  }

  const url = `https://api.nal.usda.gov/fdc/v1/food/${encodeURIComponent(
    fdcId
  )}?api_key=${encodeURIComponent(apiKey)}`;

  const resp = await fetch(url, { method: "GET" });

  if (!resp.ok) {
    if (resp.status === 404) {
      const e = new Error("ENVELOPE:usda_not_found");
      e.status = 404;
      throw e;
    }
    const txt = await resp.text().catch(() => "");
    const e = new Error(
      `USDA fetch failed: ${resp.status} ${resp.statusText} ${txt}`.slice(0, 400)
    );
    e.status = resp.status;
    throw e;
  }

  const data = await resp.json();
  return { data, url };
}

async function fetchUsdaFoodsPage(pageNumber, pageSize = 200, dataType = null) {
  const apiKey = workerCfg?.USDA_API_KEY;
  if (!apiKey) {
    const e = new Error("ENVELOPE:missing_usda_api_key");
    e.status = 400;
    throw e;
  }

  let url =
    `https://api.nal.usda.gov/fdc/v1/foods/list` +
    `?api_key=${encodeURIComponent(apiKey)}` +
    `&pageNumber=${encodeURIComponent(pageNumber)}` +
    `&pageSize=${encodeURIComponent(pageSize)}`;

  if (dataType) {
    url += `&dataType=${encodeURIComponent(dataType)}`;
  }

  const resp = await fetch(url, { method: "GET" });

  if (!resp.ok) {
    const txt = await resp.text().catch(() => "");
    const e = new Error(
      `USDA page fetch failed: ${resp.status} ${resp.statusText} ${txt}`.slice(0, 400)
    );
    e.status = resp.status;
    throw e;
  }

  const data = await resp.json();
  return { data, url };
}

// ---- concurrency guard (simple semaphore) ----
function createSemaphore(limit) {
  let active = 0;
  const queue = [];
  const acquire = () =>
    new Promise((resolve) => {
      if (active < limit) {
        active += 1;
        resolve(() => release());
      } else {
        queue.push(resolve);
      }
    });
  const release = () => {
    active = Math.max(0, active - 1);
    const next = queue.shift();
    if (next) {
      active += 1;
      next(() => release());
    }
  };
  return {
    acquire,
    get active() {
      return active;
    },
    get queued() {
      return queue.length;
    },
  };
}

const sem = createSemaphore(MAX_MESSAGES);

// ---- envelope validation ----
function validateEnvelope(env) {
  if (!env || typeof env !== "object") return { ok: false, reason: "not_object" };
  if (!env.source || typeof env.source !== "string") return { ok: false, reason: "missing_source" };
  if (!env.event_type || typeof env.event_type !== "string")
    return { ok: false, reason: "missing_event_type" };

  const dedupeOptionalEvents = new Set(["food.usda.seed.tick", "places.seed.control.tick"]);
  if (
    !dedupeOptionalEvents.has(env.event_type) &&
    (!env.dedupe_key || typeof env.dedupe_key !== "string")
  ) {
    return { ok: false, reason: "missing_dedupe_key" };
  }

  if (env.payload !== undefined && typeof env.payload !== "object")
    return { ok: false, reason: "payload_not_object" };
  return { ok: true };
}

// ---- error classification ----
function classifyError(err) {
  const msg = (err?.message || String(err)).toLowerCase();
  const code = err?.code || err?.status || null;

  // Permanent auth/permission/config errors
  // Pub/Sub publish permission errors should be permanent (avoid retry storms)
  if (msg.includes("permission_denied") || msg.includes("not authorized")) return { kind: "permanent", class: "auth" };

  if (code === 401 || code === 403) return { kind: "permanent", class: "auth" };
  if (msg.includes("invalid api key") || msg.includes("jwt")) return { kind: "permanent", class: "auth" };

  // Permanent envelope / validation errors
  if (msg.startsWith("envelope:")) return { kind: "permanent", class: "envelope" };
  if (msg.startsWith("unknown_event_type:")) return { kind: "permanent", class: "unknown_event" };

  // Unique constraint is not an error for us
  if (isUniqueViolation(err)) return { kind: "duplicate", class: "duplicate" };

  // Transient conditions
  if (code === 429) return { kind: "transient", class: "rate_limited" };
  if (code && Number(code) >= 500) return { kind: "transient", class: "remote_5xx" };
  if (
    msg.includes("timeout") ||
    msg.includes("econnreset") ||
    msg.includes("etimedout") ||
    msg.includes("enotfound") ||
    msg.includes("eai_again") ||
    msg.includes("socket hang up") ||
    msg.includes("fetch failed")
  ) {
    return { kind: "transient", class: "network" };
  }

  // Default: transient (conservative)
  return { kind: "transient", class: "unknown" };
}

// ---- DB persistence ----
async function persistWorkerEvent(envelope, pubsubMeta, status = "received", last_error = null) {
  const row = {
    source: envelope.source,
    dedupe_key: envelope.dedupe_key,
    event_type: envelope.event_type ?? null,
    entity_type: envelope.entity_type ?? null,
    entity_id: envelope.entity_id ?? null,
    occurred_at: envelope.occurred_at ?? null,
    actor_user_id: envelope.actor_user_id ?? null,
    trace_id: envelope.trace_id ?? null,
    status,
    last_error,
    payload: {
      envelope,
      _pubsub: pubsubMeta,
      _build: BUILD_ID,
    },
  };

  const { error } = await supabase.from("worker_events").insert(row);

  if (!error) return;

  // Idempotency: dedupe_key unique violation → treat as success
  if (isUniqueViolation(error)) {
    jlog("db_duplicate_ack", {
      dedupe_key: envelope.dedupe_key,
      event_type: envelope.event_type ?? null,
    });
    return;
  }

  throw error;
}

async function updateWorkerEventStatus(dedupe_key, patch) {
  if (!dedupe_key) return;
  const { error } = await supabase
    .from("worker_events")
    .update(patch)
    .eq("dedupe_key", dedupe_key);
  if (error) throw error;
}

// ---- USDA nutrients parsing ----
const USDA_NUTRIENT_ID = {
  calories_kcal: 1008, // Energy (kcal)
  protein_g: 1003,
  carbs_g: 1005, // Carbohydrate, by difference
  fat_g: 1004, // Total lipid (fat)
  fiber_g: 1079, // Fiber, total dietary
  sugar_g: 2000, // Sugars, total including NLEA
};

function extractUsdaMacrosPer100g(usdaFoodJson) {
  const out = {
    calories_kcal_per_100g: null,
    protein_g_per_100g: null,
    carbs_g_per_100g: null,
    fat_g_per_100g: null,
    fiber_g_per_100g: null,
    sugar_g_per_100g: null,
  };

  const arr = Array.isArray(usdaFoodJson?.foodNutrients) ? usdaFoodJson.foodNutrients : [];
  if (!arr.length) return out;

  const getAmount = (n) => numOrNull(n?.amount);

  // Pass 1: nutrientId exact matches
  for (const n of arr) {
    const id = n?.nutrient?.id ?? n?.nutrientId ?? null;
    const amt = getAmount(n);
    if (amt === null || id === null) continue;

    if (id === USDA_NUTRIENT_ID.calories_kcal) out.calories_kcal_per_100g = amt;
    if (id === USDA_NUTRIENT_ID.protein_g) out.protein_g_per_100g = amt;
    if (id === USDA_NUTRIENT_ID.carbs_g) out.carbs_g_per_100g = amt;
    if (id === USDA_NUTRIENT_ID.fat_g) out.fat_g_per_100g = amt;
    if (id === USDA_NUTRIENT_ID.fiber_g) out.fiber_g_per_100g = amt;
    if (id === USDA_NUTRIENT_ID.sugar_g) out.sugar_g_per_100g = amt;
  }

  // Pass 2: name fallbacks if IDs missing
  const needNameFallback =
    out.calories_kcal_per_100g === null ||
    out.protein_g_per_100g === null ||
    out.carbs_g_per_100g === null ||
    out.fat_g_per_100g === null ||
    out.fiber_g_per_100g === null ||
    out.sugar_g_per_100g === null;

  if (needNameFallback) {
    for (const n of arr) {
      const nameRaw = n?.nutrient?.name ?? n?.nutrientName ?? "";
      const name = String(nameRaw).toLowerCase();
      const unitRaw = n?.nutrient?.unitName ?? n?.unitName ?? "";
      const unit = String(unitRaw).toLowerCase();
      const amt = getAmount(n);
      if (amt === null) continue;

      if (out.calories_kcal_per_100g === null) {
        if ((name.includes("energy") || name.includes("calories")) && unit === "kcal") {
          out.calories_kcal_per_100g = amt;
        }
      }
      if (out.protein_g_per_100g === null) {
        if (name === "protein" && unit === "g") out.protein_g_per_100g = amt;
      }
      if (out.fat_g_per_100g === null) {
        if ((name.includes("total lipid") || name.includes("fat")) && unit === "g")
          out.fat_g_per_100g = amt;
      }
      if (out.carbs_g_per_100g === null) {
        if (name.includes("carbohydrate") && unit === "g") out.carbs_g_per_100g = amt;
      }
      if (out.fiber_g_per_100g === null) {
        if (name.includes("fiber") && unit === "g") out.fiber_g_per_100g = amt;
      }
      if (out.sugar_g_per_100g === null) {
        if (name.includes("sugars") && unit === "g") out.sugar_g_per_100g = amt;
      }
    }
  }

  return out;
}

// ---- RPC: upsert_macro_evidence_v1 ----
async function rpcUpsertMacroEvidence({
  food_item_id,
  macro_key,
  value_per_100g,
  source,
  source_ref,
  evidence_confidence,
  occurred_at,
}) {
  const { error } = await supabase.rpc("upsert_macro_evidence_v1", {
    p_food_item_id: food_item_id,
    p_macro_key: macro_key,
    p_value_per_100g: value_per_100g,
    p_source: source,
    p_source_ref: source_ref,
    p_evidence_confidence: evidence_confidence,
    p_occurred_at: occurred_at,
  });

  if (error) {
    const e = new Error(`ENVELOPE:macro_rpc_failed:${error.message}`);
    e.status = 400;
    throw e;
  }

  jlog("db_macro_evidence_upsert_ok", { macro_key });
}

// ---- Generic RPC helper ----
async function rpcOrThrow(name, args) {
  const { data, error } = await supabase.rpc(name, args);
  if (error) {
    const e = new Error(`ENVELOPE:rpc_failed:${name}:${error.message || String(error)}`);
    e.status = 400;
    e.code = error.code;
    e.details = error.details;
    throw e;
  }
  return data;
}

// ---- Quota helper: supports both DB overloads safely ----
async function tryConsumeDailyQuota({ quota_key, limit, amount = 1 }) {
  // Attempt boolean overload first
  try {
    const allowedBool = await rpcOrThrow("try_consume_daily_quota", {
      p_key: quota_key,
      p_limit: limit,
      p_amount: amount,
    });
    return !!allowedBool;
  } catch (e1) {
    const m1 = (e1?.message || "").toLowerCase();

    const looksLikeSignatureMismatch =
      m1.includes("function") &&
      (m1.includes("does not exist") || m1.includes("not unique") || m1.includes("no function matches"));
    if (!looksLikeSignatureMismatch) throw e1;

    // Fall back to TABLE overload
    const { data: rows, error } = await supabase.rpc("try_consume_daily_quota", {
      p_quota_key: quota_key,
      p_limit: limit,
    });
    if (error) {
      const e2 = new Error(
        `ENVELOPE:rpc_failed:try_consume_daily_quota:${error.message || String(error)}`
      );
      e2.status = 400;
      e2.code = error.code;
      e2.details = error.details;
      throw e2;
    }
    const row = Array.isArray(rows) ? rows[0] : null;
    return !!row?.allowed;
  }
}

// ---- handlers registry ----
async function notImplementedHandler(ctx, handler_name) {
  const { envelope, meta } = ctx || {};
  jlog("handler_skip", {
    handler_name,
    event_type: envelope?.event_type ?? null,
    dedupe_key: envelope?.dedupe_key ?? null,
    source: envelope?.source ?? null,
    msgId: meta?.msgId ?? null,
    reason: "not_implemented",
  });
}

// Publish helper (used by FOOD_ITEM_UPDATED)
async function publishFoodEvent(envelope) {
  if (!pubsubClient) {
    const e = new Error("ENVELOPE:pubsub_not_initialized");
    e.status = 500;
    throw e;
  }

  // Publish by TOPIC NAME (more reliable than full path across client versions)
  const topic = pubsubClient.topic(TOPIC_NAME);
  const messageId = await topic.publishMessage({
    data: Buffer.from(JSON.stringify(envelope)),
  });

  jlog("pubsub_publish_ok", {
    topic: TOPIC_NAME,
    messageId,
    event_type: envelope?.event_type ?? null,
    dedupe_key: envelope?.dedupe_key ?? null,
  });

  return messageId;
}

function asTrimmedString(value) {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s.length > 0 ? s : null;
}

function asNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function upperCountryCode(value) {
  const s = asTrimmedString(value);
  return s ? s.toUpperCase() : null;
}

function pickFirstCategory(categories) {
  if (!Array.isArray(categories) || categories.length === 0) return null;

  const cleaned = categories
    .map((v) => asTrimmedString(v))
    .filter(Boolean);

  if (cleaned.length === 0) return null;

  const cateringDot = cleaned.find((c) => c.startsWith("catering."));
  if (cateringDot) return cateringDot;

  const catering = cleaned.find((c) => c === "catering");
  if (catering) return catering;

  return cleaned[0] ?? null;
}

function buildAddressFromParts(street, housenumber) {
  const s = asTrimmedString(street);
  const h = asTrimmedString(housenumber);

  if (s && h) return `${h} ${s}`;
  if (s) return s;
  if (h) return h;
  return null;
}

function selectGeoapifyDetailsFeature(detailsResponse) {
  const features = Array.isArray(detailsResponse?.features)
    ? detailsResponse.features
    : [];

  for (const feature of features) {
    if (feature?.properties?.feature_type === "details") {
      return feature;
    }
  }

  return null;
}

function buildNormalizedGeoapifySeedPayload(placeFeature, detailsFeature) {
  const place = placeFeature?.properties ?? {};
  const details = detailsFeature?.properties ?? {};

  const address =
    buildAddressFromParts(details.street, details.housenumber) ??
    asTrimmedString(details.address_line2) ??
    buildAddressFromParts(place.street, place.housenumber) ??
    asTrimmedString(place.address_line2) ??
    null;

  return {
    name:
      asTrimmedString(details.name) ??
      asTrimmedString(place.name) ??
      null,

    address,

    city:
      asTrimmedString(details.city) ??
      asTrimmedString(place.city) ??
      null,

    postcode:
      asTrimmedString(details.postcode) ??
      asTrimmedString(place.postcode) ??
      null,

    country_code:
      upperCountryCode(details.country_code) ??
      upperCountryCode(place.country_code) ??
      null,

    lat:
      asNumber(details.lat) ??
      asNumber(place.lat) ??
      null,

    lon:
      asNumber(details.lon) ??
      asNumber(place.lon) ??
      null,

    category:
      pickFirstCategory(details.categories) ??
      pickFirstCategory(place.categories) ??
      null,

    website:
      asTrimmedString(details.website) ??
      asTrimmedString(place.website) ??
      null,

    phone:
      asTrimmedString(details.contact?.phone) ??
      asTrimmedString(place.contact?.phone) ??
      null,

    formatted:
      asTrimmedString(details.formatted) ??
      asTrimmedString(place.formatted) ??
      null,

    opening_hours:
      asTrimmedString(details.opening_hours) ??
      asTrimmedString(place.opening_hours) ??
      null,

    cuisine:
      asTrimmedString(details.catering?.cuisine) ??
      asTrimmedString(place.catering?.cuisine) ??
      null,
  };
}

async function fetchGeoapifyPlaces(target) {
  const apiKey = workerCfg?.GEOAPIFY_API_KEY;
  if (!apiKey) {
    const e = new Error("ENVELOPE:missing_geoapify_api_key");
    e.status = 400;
    throw e;
  }

  const pageSize = Number(target?.page_size || 50);
  const offset = Number(target?.last_offset || 0);

  if (target?.target_type !== "rect") {
    const e = new Error(`ENVELOPE:unsupported_place_seed_target_type:${target?.target_type || "unknown"}`);
    e.status = 400;
    throw e;
  }

  const url = new URL("https://api.geoapify.com/v2/places");
  url.searchParams.set(
    "filter",
    `rect:${target.min_lon},${target.min_lat},${target.max_lon},${target.max_lat}`
  );
  url.searchParams.set("categories", target.categories || "catering");
  url.searchParams.set("limit", String(pageSize));
  url.searchParams.set("offset", String(offset));
  url.searchParams.set("apiKey", apiKey);

  const resp = await fetch(url.toString(), { method: "GET" });

  if (!resp.ok) {
    const txt = await resp.text().catch(() => "");
    const e = new Error(
      `Geoapify places fetch failed: ${resp.status} ${resp.statusText} ${txt}`.slice(0, 500)
    );
    e.status = resp.status;
    throw e;
  }

  return await resp.json();
}

async function fetchGeoapifyPlaceDetails(placeId) {
  const apiKey = workerCfg?.GEOAPIFY_API_KEY;
  if (!apiKey) {
    const e = new Error("ENVELOPE:missing_geoapify_api_key");
    e.status = 400;
    throw e;
  }

  const id = asTrimmedString(placeId);
  if (!id) {
    const e = new Error("ENVELOPE:missing_geoapify_place_id");
    e.status = 400;
    throw e;
  }

  const url = new URL("https://api.geoapify.com/v2/place-details");
  url.searchParams.set("id", id);
  url.searchParams.set("apiKey", apiKey);

  const resp = await fetch(url.toString(), { method: "GET" });

  if (!resp.ok) {
    const txt = await resp.text().catch(() => "");
    const e = new Error(
      `Geoapify details fetch failed: ${resp.status} ${resp.statusText} ${txt}`.slice(0, 500)
    );
    e.status = resp.status;
    throw e;
  }

  return await resp.json();
}

async function enqueueGeoapifyPlaceSeed(normalized, sourcePlaceId, priority = 100) {
  const { data, error } = await supabase.rpc("enqueue_geoapify_place_seed_v1", {
    p_source_place_id: sourcePlaceId,
    p_name: normalized.name,
    p_address: normalized.address,
    p_city: normalized.city,
    p_postcode: normalized.postcode,
    p_country_code: normalized.country_code,
    p_lat: normalized.lat,
    p_lon: normalized.lon,
    p_category: normalized.category,
    p_website: normalized.website,
    p_phone: normalized.phone,
    p_formatted: normalized.formatted,
    p_opening_hours: normalized.opening_hours,
    p_cuisine: normalized.cuisine,
    p_priority: priority,
  });

  if (error) {
    const e = new Error(`ENVELOPE:rpc_failed:enqueue_geoapify_place_seed_v1:${error.message || String(error)}`);
    e.status = 400;
    e.code = error.code;
    e.details = error.details;
    throw e;
  }

  return data;
}

async function claimPlaceSeedTarget() {
  const { data, error } = await supabase.rpc("claim_place_seed_targets_v1", {
    p_batch_size: 1,
  });

  if (error) {
    const e = new Error(`ENVELOPE:rpc_failed:claim_place_seed_targets_v1:${error.message || String(error)}`);
    e.status = 400;
    e.code = error.code;
    e.details = error.details;
    throw e;
  }

  if (!Array.isArray(data) || data.length === 0) return null;
  return data[0];
}

async function markPlaceSeedTargetProgress(targetId, nextOffset) {
  return await rpcOrThrow("mark_place_seed_target_progress_v1", {
    p_target_id: targetId,
    p_next_offset: nextOffset,
  });
}

async function markPlaceSeedTargetDone(targetId) {
  return await rpcOrThrow("mark_place_seed_target_done_v1", {
    p_target_id: targetId,
  });
}

async function markPlaceSeedTargetFailed(targetId, errorText) {
  return await rpcOrThrow("mark_place_seed_target_failed_v1", {
    p_target_id: targetId,
    p_error: String(errorText || "unknown error").slice(0, 1000),
  });
}
async function claimPlaceMenuCrawlBatch() {
  const nowIso = new Date().toISOString();

  const { data: candidates, error: selectErr } = await supabase
    .from("place_menu_crawl_batches")
    .select(
      "id, source_id, batch_name, status, priority_score, attempts, next_run_at, leased_at, lease_expires_at, processed_at, created_at, updated_at"
    )
    .eq("status", "queued")
    .lte("next_run_at", nowIso)
    .order("priority_score", { ascending: false, nullsFirst: false })
    .order("created_at", { ascending: true })
    .limit(1);

  if (selectErr) {
    const e = new Error(
      `ENVELOPE:select_place_menu_crawl_batch_failed:${selectErr.message || String(selectErr)}`
    );
    e.status = 400;
    throw e;
  }

  const candidate = Array.isArray(candidates) ? candidates[0] : null;
  if (!candidate) return null;

  const { data: claimedRows, error: claimErr } = await supabase
    .from("place_menu_crawl_batches")
    .update({
      status: "running",
      leased_at: nowIso,
      lease_expires_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
      updated_at: nowIso,
      attempts: Number(candidate.attempts || 0) + 1,
    })
    .eq("id", candidate.id)
    .eq("status", "queued")
    .select("*")
    .limit(1);

  if (claimErr) {
    const e = new Error(
      `ENVELOPE:claim_place_menu_crawl_batch_update_failed:${claimErr.message || String(claimErr)}`
    );
    e.status = 400;
    throw e;
  }

  const claimed = Array.isArray(claimedRows) ? claimedRows[0] : null;
  if (!claimed) return null;

  return claimed;
}

async function getPlaceMenuSourceById(sourceId) {
  const { data, error } = await supabase
    .from("place_menu_sources")
    .select("id, place_id, source_type, source_url, source_domain, registrable_domain, discovery_method, crawl_status, crawl_attempts, last_crawl_at, menu_items_found, confidence, created_at, updated_at, menu_signal_score")
    .eq("id", sourceId)
    .single();

  if (error) {
    const e = new Error(`ENVELOPE:get_place_menu_source_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }

  return data;
}

async function getPlaceMenuCrawlResultById(crawlResultId) {
  const { data, error } = await supabase
    .from("place_menu_crawl_results")
    .select(`
      id,
      crawl_batch_id,
      source_id,
      place_id,
      source_url,
      final_url,
      http_status,
      fetch_status,
      content_type,
      page_title,
      raw_html,
      raw_text,
      detected_menu_url,
      detected_language,
      error_text,
      created_at
    `)
    .eq("id", crawlResultId)
    .single();

  if (error) {
    const e = new Error(`ENVELOPE:get_place_menu_crawl_result_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }

  return data;
}

function stripHtmlToText(html) {
  const raw = String(html || "");
  return raw
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 50000);
}

function collapseDuplicatePathSegments(urlObj) {
  const parts = urlObj.pathname.split("/").filter(Boolean);
  const deduped = [];

  for (let i = 0; i < parts.length; i++) {
    if (i > 0 && parts[i].toLowerCase() === parts[i - 1].toLowerCase()) {
      continue;
    }
    deduped.push(parts[i]);
  }

  urlObj.pathname = "/" + deduped.join("/");
  return urlObj.toString();
}

function detectMenuUrlFromHtml(html, baseUrl) {
  const raw = String(html || "");

  const blockedExtPattern =
    /\.(css|js|json|png|jpg|jpeg|gif|svg|webp|ico|woff|woff2|ttf|eot|map|xml)(\?|#|$)/i;

  const hrefPattern = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;

  const MENU_KEYWORDS = [
    "menu",
    "menus",
    "food",
    "food-menu",
    "drinks",
    "drink-menu",
    "dining",
    "our-menu",
    "restaurant-menu",
    "view-menu",
    "carte",
    "la-carte",
    "carta",
    "speisekarte",
    "karte",
    "menue",
    "getraenkekarte",
    "speisen",
    "menukort",
    "mad",
    "spisekort",
    "vinkort",
    "ementa",
    "pratos",
    "refeicoes",
    "meniu",
    "maistas",
    "patiekalai",
    "jidelni-listek",
    "listek",
    "jidlo",
    "napojovy-listek",
    "nabidka"
  ];

  const ORDER_KEYWORDS = [
    "order",
    "orderfood",
    "order-food",
    "order-online",
    "online-ordering",
    "fastorder",
    "homeorder",
    "order-now",
    "place-order",
    "checkout",
    "commande",
    "commander",
    "commande-en-ligne",
    "commander-en-ligne",
    "panier",
    "passer-commande",
    "pedir",
    "pedido",
    "pedidos",
    "pedidos-en-linea",
    "pide-aqui",
    "hacer-pedido",
    "ordenar",
    "orden",
    "bestellen",
    "bestellung",
    "online-bestellen",
    "onlinebestellen",
    "vorbestellen",
    "jetzt-bestellen",
    "bestil",
    "bestilling",
    "koeb",
    "online-bestilling",
    "bestil-online",
    "bestil-mad",
    "encomenda",
    "encomendar",
    "fazer-pedido",
    "encomendas",
    "uzsakymas",
    "uzsakyti",
    "uzsakymai",
    "pirkti",
    "objednat",
    "objednavka",
    "objednavky",
    "online-objednavka"
  ];

  const DELIVERY_KEYWORDS = [
    "delivery",
    "deliver",
    "home-delivery",
    "delivery-menu",
    "food-delivery",
    "order-delivery",
    "livraison",
    "livrer",
    "livraison-a-domicile",
    "livraison-repas",
    "domicilio",
    "entrega",
    "reparto",
    "enviar",
    "envio",
    "lieferung",
    "liefern",
    "lieferservice",
    "levering",
    "udbringning",
    "entregas",
    "ao-domicilio",
    "pristatymas",
    "i-namus",
    "rozvoz",
    "dovoz",
    "doruceni"
  ];

  const TAKEAWAY_KEYWORDS = [
    "takeaway",
    "take-away",
    "pickup",
    "pick-up",
    "collection",
    "clickandcollect",
    "click-and-collect",
    "togo",
    "to-go",
    "emporter",
    "a-emporter",
    "vente-a-emporter",
    "retrait",
    "para-llevar",
    "llevar",
    "recoger",
    "retiro",
    "abholen",
    "abholung",
    "mitnehmen",
    "zum-mitnehmen",
    "afhentning",
    "recolha",
    "issinesti",
    "sebou",
    "s-sebou"
  ];

  const NEGATIVE_KEYWORDS = [
    "christmas",
    "xmas",
    "event",
    "events",
    "blog",
    "news",
    "article",
    "post",
    "gallery",
    "contact",
    "about",
    "booking",
    "reservation",
    "reserve",
    "gift",
    "voucher",
    "careers",
    "jobs",
    "privacy",
    "terms",
    "members",
    "member",
    "login",
    "register",
    "account",
    "profile",
    "franchise",
    "press",
    "media",
    "faq"
  ];

  const CHOOSER_KEYWORDS = [
    "menu.html",
    "order-type",
    "how-would-you-like-to-order",
    "choose",
    "choose-order",
    "choose-your-order",
    "select-order",
    "start-order"
  ];

  let base;
  try {
    base = new URL(baseUrl);
  } catch {
    return null;
  }

  const scoreIncludes = (text, words, weight) => {
    let score = 0;
    for (const word of words) {
      if (text.includes(word)) score += weight;
    }
    return score;
  };

  const countSegments = (pathname) =>
    String(pathname || "").split("/").filter(Boolean).length;

  const candidates = [];

  let match;
  while ((match = hrefPattern.exec(raw)) !== null) {
    const href = String(match[1] || "").trim();
    const anchorHtml = String(match[2] || "");
    const anchorText = anchorHtml.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim();

    if (!href) continue;
    if (href.startsWith("#")) continue;
    if (/^(mailto:|tel:|javascript:)/i.test(href)) continue;

    let resolved;
    try {
      resolved = new URL(href, base.toString());
    } catch {
      continue;
    }

   if (!/^https?:$/i.test(resolved.protocol)) continue;

let urlStr = resolved.toString();
urlStr = collapseDuplicatePathSegments(new URL(urlStr));

if (blockedExtPattern.test(urlStr)) continue;

const normalizedUrl = new URL(urlStr);

const sameHost = normalizedUrl.hostname === base.hostname;
const pathLower = normalizedUrl.pathname.toLowerCase();
const fullLower = `${urlStr} ${anchorText}`.toLowerCase();
const segmentCount = countSegments(pathLower);

    let score = 0;

    if (sameHost) score += 5;

    score += scoreIncludes(pathLower, MENU_KEYWORDS, 8);
    score += scoreIncludes(pathLower, ORDER_KEYWORDS, 5);
    score += scoreIncludes(pathLower, DELIVERY_KEYWORDS, 4);
    score += scoreIncludes(pathLower, TAKEAWAY_KEYWORDS, 4);

    score += scoreIncludes(fullLower, MENU_KEYWORDS, 2);
    score += scoreIncludes(fullLower, ORDER_KEYWORDS, 2);
    score += scoreIncludes(fullLower, DELIVERY_KEYWORDS, 1);
    score += scoreIncludes(fullLower, TAKEAWAY_KEYWORDS, 1);

    if (/(^|\/)(menu|menus)(\/|$)/i.test(pathLower)) score += 10;
    if (/(^|\/)(collection|pickup|takeaway|delivery|home-delivery)(\/|$)/i.test(pathLower)) score += 8;
    if (/\/menu\/[^/]+(\.html)?$/i.test(pathLower)) score += 8;
    if (/\/menu\/(collection|pickup|takeaway|delivery|home-delivery)(\.html)?$/i.test(pathLower)) score += 12;
    if (/\/(order|order-online|orderfood|fastorder|homeorder)\/[^/]+(\.html)?$/i.test(pathLower)) score += 6;
    if (/pdf(\?|#|$)/i.test(urlStr)) score += 3;

    if (segmentCount >= 2) score += 2;
    if (segmentCount >= 3) score += 3;

    for (const word of NEGATIVE_KEYWORDS) {
      if (fullLower.includes(word)) score -= 8;
    }

    for (const word of CHOOSER_KEYWORDS) {
      if (fullLower.includes(word)) score -= 6;
    }

    if (/\/$/.test(pathLower) && segmentCount <= 1) score -= 4;
    if (/\/restaurant\/?$/.test(pathLower)) score -= 6;
    if (/\/home\/?$/.test(pathLower)) score -= 8;
    if (/\/members?\/?$/.test(pathLower)) score -= 10;
    if (/\/contact\/?$/.test(pathLower)) score -= 10;
    if (/\/reservations?\/?$/.test(pathLower)) score -= 10;

    if (score <= 0) continue;

    candidates.push({
      url: urlStr,
      score,
      path: pathLower
    });
  }

  if (!candidates.length) return null;

  candidates.sort((a, b) =>
    b.score - a.score ||
    b.path.length - a.path.length ||
    a.url.localeCompare(b.url)
  );

  return candidates[0].url;
}

function buildMenuCandidates(sourceUrl) {
  const src = asTrimmedString(sourceUrl);
  if (!src) return [];

  let url;
  try {
    url = new URL(src);
  } catch {
    return [];
  }

  const out = [];
  const seen = new Set();

  const add = (candidate) => {
    try {
      const normalized = new URL(candidate, url.origin).toString();
      if (seen.has(normalized)) return;
      seen.add(normalized);
      out.push(normalized);
    } catch {
      // ignore bad candidate
    }
  };

  const cleanPath = String(url.pathname || "/")
    .replace(/\/{2,}/g, "/")
    .trim();

  const pathParts = cleanPath.split("/").filter(Boolean);

  const looksLikeFile =
    pathParts.length > 0 &&
    /\.[a-z0-9]{2,8}$/i.test(pathParts[pathParts.length - 1]);

  const dirParts = looksLikeFile ? pathParts.slice(0, -1) : pathParts;

  const fullBase = "/" + dirParts.join("/");
  const parentBase =
    dirParts.length > 1 ? "/" + dirParts.slice(0, -1).join("/") : "";
  const rootBase = "";

  const bases = [];
  if (fullBase && fullBase !== "/") bases.push(fullBase);
  if (parentBase && parentBase !== fullBase && parentBase !== "/") bases.push(parentBase);
  bases.push(rootBase);

  const MENU_KEYWORDS = [
    // English
    "menu",
    "menus",
    "food",
    "food-menu",
    "drinks",
    "drink-menu",
    "dining",
    "eat",
    "our-menu",
    "restaurant-menu",
    "view-menu",

    // French
    "carte",
    "la-carte",
    "nos-menus",
    "notre-carte",
    "plats",
    "boissons",
    "la-carte-des-vins",

    // Spanish
    "carta",
    "nuestro-menu",
    "nuestra-carta",
    "platillos",
    "comida",

    // German
    "speisekarte",
    "karte",
    "menue",
    "getraenkekarte",
    "speisen",
    "unsere-karte",

    // Danish
    "menukort",
    "mad",
    "vores-menu",
    "drikkevarer",
    "spisekort",
    "vinkort",

    // Portuguese
    "ementa",
    "nossa-ementa",
    "pratos",
    "refeicoes",
    "bebidas",

    // Lithuanian
    "meniu",
    "maistas",
    "patiekalai",
    "gerimai",
    "musu-meniu",

    // Czech
    "jidelni-listek",
    "listek",
    "jidlo",
    "napojovy-listek",
    "nabidka"
  ];

  const ORDER_KEYWORDS = [
    // English
    "order",
    "orderfood",
    "order-food",
    "order-online",
    "online-ordering",
    "fastorder",
    "homeorder",
    "order-now",
    "place-order",
    "checkout",

    // French
    "commande",
    "commander",
    "commande-en-ligne",
    "commander-en-ligne",
    "panier",
    "passer-commande",

    // Spanish
    "pedir",
    "pedido",
    "pedidos",
    "pedidos-en-linea",
    "pide-aqui",
    "hacer-pedido",
    "ordenar",
    "orden",

    // German
    "bestellen",
    "bestellung",
    "online-bestellen",
    "onlinebestellen",
    "vorbestellen",
    "jetzt-bestellen",

    // Danish
    "bestil",
    "bestilling",
    "koeb",
    "online-bestilling",
    "bestil-online",
    "bestil-mad",

    // Portuguese
    "encomenda",
    "encomendar",
    "fazer-pedido",
    "encomendas",

    // Lithuanian
    "uzsakymas",
    "uzsakyti",
    "uzsakymai",
    "pirkti",

    // Czech
    "objednat",
    "objednavka",
    "objednavky",
    "online-objednavka"
  ];

  const DELIVERY_KEYWORDS = [
    // English
    "delivery",
    "deliver",
    "home-delivery",
    "delivery-menu",
    "food-delivery",
    "order-delivery",

    // French
    "livraison",
    "livrer",
    "livraison-a-domicile",
    "livraison-repas",

    // Spanish
    "domicilio",
    "entrega",
    "reparto",
    "enviar",
    "envio",

    // German
    "lieferung",
    "liefern",
    "lieferservice",

    // Danish
    "levering",
    "udbringning",

    // Portuguese
    "entrega",
    "entregas",
    "ao-domicilio",

    // Lithuanian
    "pristatymas",
    "i-namus",

    // Czech
    "rozvoz",
    "dovoz",
    "doruceni"
  ];

  const TAKEAWAY_KEYWORDS = [
    // English
    "takeaway",
    "take-away",
    "pickup",
    "pick-up",
    "collection",
    "clickandcollect",
    "click-and-collect",
    "togo",
    "to-go",

    // French
    "emporter",
    "a-emporter",
    "vente-a-emporter",
    "retrait",

    // Spanish
    "para-llevar",
    "llevar",
    "recoger",
    "retiro",

    // German
    "abholen",
    "abholung",
    "mitnehmen",
    "zum-mitnehmen",

    // Danish
    "afhentning",

    // Portuguese
    "recolha",

    // Lithuanian
    "issinesti",

    // Czech
    "sebou",
    "s-sebou"
  ];

  const DEEP_MENU_LEAFS = [
    "collection",
    "home-delivery",
    "delivery",
    "pickup",
    "takeaway",
    "order",
    "order-online",
    "food-delivery",
    "click-and-collect",
    "clickandcollect",
    "abholen",
    "afhentning",
    "recolha",
    "sebou",
    "issinesti"
  ];

  const joinPath = (base, slug) => {
    const b = base && base !== "/" ? base.replace(/\/+$/, "") : "";
    const s = String(slug || "").replace(/^\/+/, "");
    return `${b}/${s}`.replace(/\/{2,}/g, "/");
  };

  const addWithForms = (path) => {
    const p = String(path || "").replace(/\/{2,}/g, "/");
    add(p);
    add(`${p}/`);
    add(`${p}.html`);
  };

  const addVariantGroup = (base, variants) => {
    for (const v of variants) {
      addWithForms(joinPath(base, v));
    }
  };

  const addNestedVariants = (base) => {
    for (const menuSlug of MENU_KEYWORDS) {
      const menuBase = joinPath(base, menuSlug);
      addWithForms(menuBase);

      for (const leaf of DEEP_MENU_LEAFS) {
        addWithForms(joinPath(menuBase, leaf));
      }

      for (const orderSlug of ORDER_KEYWORDS) {
        addWithForms(joinPath(menuBase, orderSlug));
      }

      for (const deliverySlug of DELIVERY_KEYWORDS) {
        addWithForms(joinPath(menuBase, deliverySlug));
      }

      for (const takeawaySlug of TAKEAWAY_KEYWORDS) {
        addWithForms(joinPath(menuBase, takeawaySlug));
      }
    }
  };

  const addOrderNestedVariants = (base) => {
    for (const orderSlug of ORDER_KEYWORDS) {
      const orderBase = joinPath(base, orderSlug);
      addWithForms(orderBase);

      for (const menuSlug of MENU_KEYWORDS) {
        addWithForms(joinPath(orderBase, menuSlug));
      }

      for (const deliverySlug of DELIVERY_KEYWORDS) {
        addWithForms(joinPath(orderBase, deliverySlug));
      }

      for (const takeawaySlug of TAKEAWAY_KEYWORDS) {
        addWithForms(joinPath(orderBase, takeawaySlug));
      }
    }
  };

  // 1) preserve current path context first
  for (const base of bases) {
    if (base) {
      add(base);
      add(`${base}/`);
    }

    addVariantGroup(base, MENU_KEYWORDS);
    addVariantGroup(base, ORDER_KEYWORDS);
    addVariantGroup(base, DELIVERY_KEYWORDS);
    addVariantGroup(base, TAKEAWAY_KEYWORDS);

    addNestedVariants(base);
    addOrderNestedVariants(base);
  }

  // 2) explicit Levante-style /menu/... hits at root
  add("/menu");
  add("/menu/");
  add("/menu.html");
  add("/menu/home-delivery.html");
  add("/menu/home-delivery/");
  add("/menu/collection.html");
  add("/menu/collection/");
  add("/menu/delivery.html");
  add("/menu/delivery/");
  add("/menu/pickup.html");
  add("/menu/pickup/");
  add("/menu/takeaway.html");
  add("/menu/takeaway/");

  // 3) common root-level direct hits
  add("/order");
  add("/order-online");
  add("/orderfood");
  add("/fastorder");
  add("/homeorder");
  add("/delivery");
  add("/takeaway");
  add("/collection");
  add("/pickup");
  add("/click-and-collect");
  add("/clickandcollect");

  // 4) keep original source URL too
  add(url.toString());

  return out.slice(0, 140);
}

async function callCrawlerExtract(sourceUrl) {
  const url = asTrimmedString(sourceUrl);
  if (!url) {
    const e = new Error("ENVELOPE:missing_source_url");
    e.status = 400;
    throw e;
  }

  const endpoint = `${CRAWLER_EXTRACT_BASE_URL}/extract?url=${encodeURIComponent(url)}`;

  const resp = await fetch(endpoint, {
    method: "GET",
    headers: {
      "accept": "application/json",
    },
  });

  if (!resp.ok) {
    const txt = await resp.text().catch(() => "");
    const e = new Error(
      `ENVELOPE:crawler_extract_http_${resp.status}:${txt}`.slice(0, 500)
    );
    e.status = resp.status;
    throw e;
  }

  const data = await resp.json();

  if (!data || data.ok !== true) {
    const e = new Error(
      `ENVELOPE:crawler_extract_failed:${data?.error || "unknown_error"}`
    );
    e.status = 502;
    throw e;
  }

  return {
    final_url: asTrimmedString(data.final_url) || url,
    http_status: Number(data.status || 200),
    content_type: "text/html",
    raw_html: String(data.html_preview || ""),
    raw_text: stripHtmlToText(String(data.html_preview || "")),
    page_title: (() => {
      const m = String(data.html_preview || "").match(/<title[^>]*>([\s\S]*?)<\/title>/i);
      return m ? m[1].replace(/\s+/g, " ").trim().slice(0, 500) : null;
    })(),
    links: [],
  };
}

async function classifyMenuLinksWithLLM(links) {
  const apiKey = workerCfg?.OPENAI_API_KEY;

  if (!apiKey) {
    return { menu: [], order: [], delivery: [], ignore: [] };
  }

  const prompt = `
You are classifying restaurant website links.

Return JSON only.

Classify each URL into one of:
- menu
- order
- delivery
- ignore

Rules:
- menu = actual food listing or menu page
- order = ordering flow or restaurant order page
- delivery = delivery platform or delivery-specific page
- ignore = booking, contact, login, careers, privacy, terms, blog, members

Links:
${JSON.stringify(links, null, 2)}

Return format:
{
  "menu": ["url1"],
  "order": ["url2"],
  "delivery": ["url3"],
  "ignore": ["url4"]
}
`;

  try {
    const resp = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: "gpt-4o-mini",
        messages: [{ role: "user", content: prompt }],
        temperature: 0,
      }),
    });

    if (!resp.ok) {
      return { menu: [], order: [], delivery: [], ignore: [] };
    }

    const data = await resp.json();
    const content = data?.choices?.[0]?.message?.content || "{}";

    try {
      return JSON.parse(content);
    } catch {
      return { menu: [], order: [], delivery: [], ignore: [] };
    }
  } catch {
    return { menu: [], order: [], delivery: [], ignore: [] };
  }
}

async function fetchMenuSourcePage(sourceUrl) {
  const url = asTrimmedString(sourceUrl);
  if (!url) {
    const e = new Error("ENVELOPE:missing_source_url");
    e.status = 400;
    throw e;
  }

  const resp = await fetch(url, {
    method: "GET",
    redirect: "follow",
    headers: {
      "user-agent": "MewWebHunter/1.0 (+menu crawl)",
      "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7",
      "accept-language": "en;q=0.9",
    },
  });

  const finalUrl = resp.url || url;
  const contentType = resp.headers.get("content-type") || null;
  const html = await resp.text().catch(() => "");

  return {
    source_url: url,
    final_url: finalUrl,
    http_status: resp.status,
    ok: resp.ok,
    content_type: contentType,
    raw_html: html,
    raw_text: stripHtmlToText(html),
    page_title: (() => {
      const m = String(html || "").match(/<title[^>]*>([\s\S]*?)<\/title>/i);
      return m ? m[1].replace(/\s+/g, " ").trim().slice(0, 500) : null;
    })(),
    detected_menu_url: detectMenuUrlFromHtml(html, finalUrl),
  };
}

async function insertPlaceMenuCrawlResult({
  crawl_batch_id,
  source_id,
  place_id,
  source_url,
  final_url,
  http_status,
  fetch_status,
  content_type,
  page_title,
  raw_html,
  raw_text,
  detected_menu_url,
  detected_language,
  error_text,
}) {
  const { data, error } = await supabase
    .from("place_menu_crawl_results")
    .insert({
      crawl_batch_id,
      source_id,
      place_id,
      source_url,
      final_url,
      http_status,
      fetch_status,
      content_type,
      page_title,
      raw_html,
      raw_text,
      detected_menu_url,
      detected_language,
      error_text,
    })
    .select("id")
    .single();

  if (error) {
    const e = new Error(`ENVELOPE:insert_place_menu_crawl_result_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }

  return data;
}

async function markPlaceMenuCrawlBatchDone(batchId) {
  const nowIso = new Date().toISOString();

  const { error } = await supabase
    .from("place_menu_crawl_batches")
    .update({
      status: "done",
      processed_at: nowIso,
      lease_expires_at: null,
      updated_at: nowIso,
      last_error: null,
    })
    .eq("id", batchId);

  if (error) {
    const e = new Error(`ENVELOPE:mark_place_menu_crawl_batch_done_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }
}

async function markPlaceMenuCrawlBatchFailed(batchId, errorText) {
  const nowIso = new Date().toISOString();
  const nextRunAt = new Date(Date.now() + 15 * 60 * 1000).toISOString();

  const { error } = await supabase
    .from("place_menu_crawl_batches")
    .update({
      status: "queued",
      next_run_at: nextRunAt,
      lease_expires_at: null,
      updated_at: nowIso,
      last_error: String(errorText || "unknown error").slice(0, 1000),
    })
    .eq("id", batchId);

  if (error) {
    const e = new Error(`ENVELOPE:mark_place_menu_crawl_batch_failed_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }
}

async function updatePlaceMenuSourceAfterCrawl(sourceId, patch = {}) {
  const { error } = await supabase
    .from("place_menu_sources")
    .update({
      ...patch,
      updated_at: new Date().toISOString(),
    })
    .eq("id", sourceId);

  if (error) {
    const e = new Error(`ENVELOPE:update_place_menu_source_after_crawl_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }

}

function getNested(obj, path, fallback = null) {
  let cur = obj;
  for (const key of path) {
    if (!cur || typeof cur !== "object" || !(key in cur)) return fallback;
    cur = cur[key];
  }
  return cur ?? fallback;
}

function cleanLine(line) {
  return String(line || "")
    .replace(/\u00a0/g, " ")
    .replace(/[ \t]+/g, " ")
    .trim();
}

function htmlToLines(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, "\n")
    .replace(/<style[\s\S]*?<\/style>/gi, "\n")
    .replace(/<\/(p|div|section|article|li|ul|ol|h1|h2|h3|h4|h5|h6|br|tr)>/gi, "\n")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .split("\n")
    .map(cleanLine)
    .filter(Boolean)
    .slice(0, 4000);
}

function textToLines(text) {
  return String(text || "")
    .split(/\r?\n/)
    .map(cleanLine)
    .filter(Boolean)
    .slice(0, 4000);
}

function looksLikePrice(line) {
  return /(?:^|\s)(€|\$|£)\s?\d{1,3}(?:[.,]\d{2})?(?:\s|$)|(?:^|\s)\d{1,3}(?:[.,]\d{2})\s?(€|\$|£)(?:\s|$)/.test(line);
}

function extractPrice(line) {
  const m =
    line.match(/(€|\$|£)\s?(\d{1,3}(?:[.,]\d{2})?)/) ||
    line.match(/(\d{1,3}(?:[.,]\d{2})?)\s?(€|\$|£)/);

  if (!m) return { price_amount: null, price_currency: null };

  if (m[1] && /[€$£]/.test(m[1])) {
    return {
      price_currency: m[1],
      price_amount: Number(String(m[2]).replace(",", ".")),
    };
  }

  return {
    price_currency: m[2] || null,
    price_amount: Number(String(m[1]).replace(",", ".")),
  };
}

function normalizeCurrencySymbol(symbol) {
  if (symbol === "€") return "EUR";
  if (symbol === "$") return "USD";
  if (symbol === "£") return "GBP";
  return null;
}

function isProbablySectionHeader(line) {
  if (!line) return false;

  if (looksLikePrice(line)) return false;
  if (line.length > 60) return false;

  const upperRatio =
    line.replace(/[^A-Z]/g, "").length /
    Math.max(1, line.replace(/[^A-Za-z]/g, "").length);

  return (
    /^[A-Z0-9 &/+\-!']+$/.test(line) ||
    upperRatio > 0.7 ||
    /^(starters|small plates|mains|main courses|burgers|pizza|pizzas|pasta|salads|desserts|drinks|beverages|sides|kids|lunch|dinner|breakfast|brunch|wraps|kebabs|grill|platters|sushi|curries|noodles|meze|hot meze|cold meze|open|closed|limited time savings!?)$/i.test(line)
  );
}

function isProbablyItemLine(line) {
  if (!line) return false;

  const low = line.toLowerCase();

  if (line.length < 2) return false;
  if (line.length > 120) return false;

  // obvious non-food UI/system text
  if (
    /home|members|reservations|contact us|book a table|order online|order for|delivery|collection|postcode|login|sign up|register|password|reset|cancel|contact name|contact number|contact email|email address|notes\/requirements|please select your address|my address is not listed|all rights reserved|welcome to/i.test(line)
  ) {
    return false;
  }

  // weak one-word junk labels
  if (
    /^(book|order|date|time|people|cancel|home|members|reservations)$/i.test(line)
  ) {
    return false;
  }

  // title-like or priced food candidates only
  return (
    looksLikePrice(line) ||
    /^[A-Z][A-Za-z0-9 '&/,+().-]{2,100}$/.test(line)
  );
}

function extractNameFromPricedLine(line) {
  return cleanLine(
    String(line)
      .replace(/(€|\$|£)\s?\d{1,3}(?:[.,]\d{2})?/g, " ")
      .replace(/\d{1,3}(?:[.,]\d{2})?\s?(€|\$|£)/g, " ")
      .replace(/[·•]/g, " ")
  );
}

function buildBasicMenuFromLines(lines) {
  const sections = [];
  let currentSection = {
    name: "Uncategorized",
    normalized_name: null,
    items: [],
  };

  const blockedSectionPattern =
    /^(open|closed|home|members|reservations|contact us|limited time savings!?|welcome)$/i;

  const pushSectionIfNeeded = () => {
    if (currentSection.items.length > 0 || sections.length === 0) {
      sections.push(currentSection);
    }
  };

  for (let i = 0; i < lines.length; i += 1) {
    const line = cleanLine(lines[i]);
    if (!line) continue;

    if (isProbablySectionHeader(line)) {
      if (currentSection.items.length > 0) {
        pushSectionIfNeeded();
        currentSection = {
          name: line,
          normalized_name: null,
          items: [],
        };
      } else {
        currentSection.name = line;
      }
      continue;
    }

    // do not collect items under obviously bad sections
    if (blockedSectionPattern.test(String(currentSection.name || "").trim())) {
      continue;
    }

    if (isProbablyItemLine(line)) {
      const priceBits = extractPrice(line);
      const nextLine = cleanLine(lines[i + 1] || "");

      const item = {
        name: looksLikePrice(line) ? extractNameFromPricedLine(line) : line,
        description: null,
        price: priceBits.price_amount,
        currency: normalizeCurrencySymbol(priceBits.price_currency),
      };

      if (
        nextLine &&
        !isProbablySectionHeader(nextLine) &&
        !isProbablyItemLine(nextLine) &&
        nextLine.length <= 240
      ) {
        item.description = nextLine;
        i += 1;
      }

      if (item.name) {
        currentSection.items.push(item);
      }
    }
  }

  pushSectionIfNeeded();

  return sections.filter((s) => s.items.length > 0);
}

function shouldSkipExtractedMenuItem(item, sectionName = null) {
  const name = String(item?.name || "").trim();
  const section = String(sectionName || "").trim();

  if (!name) return true;

  const lowName = name.toLowerCase();
  const lowSection = section.toLowerCase();

  const exactBadNames = new Set([
    "home",
    "members",
    "reservations",
    "contact us",
    "book a table",
    "order",
    "order online",
    "order for home delivery",
    "order for collection",
    "make a reservation",
    "welcome",
  ]);

  if (exactBadNames.has(lowName)) return true;

  const badNamePatterns = [
    /t&c|t&cs|terms/i,
    /postcode/i,
    /delivery charge/i,
    /serve your area/i,
    /limited time/i,
    /call us/i,
    /contact us/i,
    /book a table/i,
    /reservation/i,
    /order online/i,
    /home delivery/i,
    /collection/i,
    /click here/i,
    /sign up/i,
    /log in/i,
    /login/i,
    /welcome to /i,
  ];

  if (badNamePatterns.some((re) => re.test(name))) return true;

  const badSectionPatterns = [
    /^open$/i,
    /limited time/i,
    /reservations?/i,
    /contact/i,
    /members?/i,
    /delivery/i,
    /collection/i,
    /offers?/i,
    /promotions?/i,
  ];

  if (badSectionPatterns.some((re) => re.test(section))) return true;

  const hasPrice = item?.price !== null && item?.price !== undefined;
  const menuLikeSection = /starters|small plates|mains|main courses|burgers|pizza|pizzas|pasta|salads|desserts|drinks|beverages|sides|kids|lunch|dinner|breakfast|brunch|wraps|kebabs|grill|platters|sushi|curries|noodles/i.test(section);

  const foodLikeName =
    /^[A-Z][A-Za-z0-9 '&/,+().-]{2,80}$/.test(name) &&
    !/welcome|contact|reservation|delivery|postcode|member|login|sign up|book/i.test(name);

  if (!(hasPrice || menuLikeSection || foodLikeName)) return true;

  return false;
}

async function insertPlaceMenuItemsFromExtraction(extraction) {
  const sections = extraction?.raw?.menu?.sections || [];
  if (!Array.isArray(sections) || sections.length === 0) {
    console.log({
      skipped: 0,
      inserted: 0,
      source_url: extraction?.source_url || extraction?.raw?.source?.url || null,
    });
    return { inserted: 0, skipped: 0 };
  }

  const rows = [];
  let skipped = 0;

  for (const section of sections) {
    const sectionName = section?.name || null;

    for (const item of section.items || []) {
      if (!item?.name) {
        skipped += 1;
        continue;
      }

      if (shouldSkipExtractedMenuItem(item, sectionName)) {
        skipped += 1;
        continue;
      }

      rows.push({
        place_id: extraction.place_id,
        source: "webhunter",
        source_ref: extraction.id,
        name: item.name,
        description: item.description || null,
        section_name: sectionName,
        normalized_section: null,
        price_amount: item.price ?? null,
        price_currency: item.currency || null,
        item_confidence: extraction?.raw?.confidence?.overall ?? null,
        source_priority: 100,
        calculator_ready: false,
        calculator_status: "pending",
        macro_estimate_status: "none",
      });
    }
  }

  if (rows.length > 0) {
    const { error } = await supabase.from("place_menu_items").insert(rows);
    if (error) {
      const e = new Error(`ENVELOPE:insert_place_menu_items_failed:${error.message || String(error)}`);
      e.status = 400;
      throw e;
    }
  }

  console.log({
    skipped,
    inserted: rows.length,
    source_url: extraction?.source_url || extraction?.raw?.source?.url || null,
  });

  return { inserted: rows.length, skipped };
}

function summarizeExtraction(sections) {
  const itemCount = sections.reduce((sum, s) => sum + s.items.length, 0);

  let pricedCount = 0;
  for (const s of sections) {
    for (const item of s.items) {
      if (item.price !== null && item.price !== undefined) pricedCount += 1;
    }
  }

  return {
    itemCount,
    sectionCount: sections.length,
    pricedCount,
  };
}

async function claimStagedMenuExtraction() {
  const { data: rows, error } = await supabase
    .from("menu_extractions")
    .select("id, place_id, candidate_source_id, source_url, provider, raw, status, created_at, input_kind, acquired_via")
    .eq("status", "staged")
    .order("created_at", { ascending: true })
    .limit(1);

  if (error) {
    const e = new Error(`ENVELOPE:claim_menu_extraction_select_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }

  const row = Array.isArray(rows) ? rows[0] : null;
  if (!row) return null;

  const claimPatch = {
    status: "needs_review",
    extracted_at: new Date().toISOString(),
    extractor: "webhunter_v1",
    extractor_version: "v1_basic_html",
    parse_method: "basic_html_pdf",
  };

  const { data: claimedRows, error: claimErr } = await supabase
    .from("menu_extractions")
    .update(claimPatch)
    .eq("id", row.id)
    .eq("status", "staged")
    .select("id, place_id, candidate_source_id, source_url, provider, raw, status, created_at, extracted_at, input_kind, acquired_via")
    .limit(1);

  if (claimErr) {
    const e = new Error(`ENVELOPE:claim_menu_extraction_update_failed:${claimErr.message || String(claimErr)}`);
    e.status = 400;
    throw e;
  }

  const claimed = Array.isArray(claimedRows) ? claimedRows[0] : null;
  return claimed || null;
}

async function updateMenuExtractionRow(extractionId, patch) {
  const { error } = await supabase
    .from("menu_extractions")
    .update(patch)
    .eq("id", extractionId);

  if (error) {
    const e = new Error(`ENVELOPE:update_menu_extraction_failed:${error.message || String(error)}`);
    e.status = 400;
    throw e;
  }
  }
function buildSidecarContactPoints({
  html = "",
  baseUrl = "",
  sourceUrl = "",
  sourceDomain = null,
  provider = "webhunter_sidecar_v1",
}) {
  
  const results = [];
  const seen = new Set();

  const addPoint = ({
    contact_type,
    contact_value,
    confidence = 0.7,
    is_primary = false,
  }) => {
    const type = String(contact_type || "").trim();
    const value = String(contact_value || "").trim();
    if (!type || !value) return;

    const key = `${type}::${value.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);

    results.push({
      contact_type: type,
      contact_value: value,
      provider,
      source_url: sourceUrl || baseUrl || null,
      source_domain: sourceDomain || null,
      confidence,
      is_primary,
    });
  };

  const rawHtml = String(html || "");
  const rawText = stripHtmlToText(rawHtml);

// Emails
const emailMatches = rawHtml.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
for (const email of emailMatches) {
  const lowerEmail = String(email).toLowerCase();

  // Filter junk (Wix / Sentry / tracking)
  if (
    lowerEmail.includes("sentry") ||
    lowerEmail.includes("wixpress")
  ) {
    continue;
  }

  addPoint({
    contact_type: "email",
    contact_value: lowerEmail,
    confidence: 0.95,
    is_primary: true,
  });
}

// Phones
const phoneMatches =
  rawText.match(/(?:\+?\d[\d\s().-]{6,}\d)/g) || [];
for (const phone of phoneMatches) {
  const cleaned = phone.replace(/\s+/g, " ").trim();
  const digitsOnly = cleaned.replace(/\D/g, "");

  // Require real phone length (avoid IDs / garbage)
  if (digitsOnly.length < 9) continue;

  addPoint({
    contact_type: "phone",
    contact_value: cleaned,
    confidence: 0.85,
    is_primary: true,
  });
}

  // Anchor extraction
  const hrefRegex = /<a\b[^>]*href=["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let match;

  const resolveUrl = (href) => {
    try {
      return new URL(href, baseUrl).toString();
    } catch {
      return null;
    }
  };

  while ((match = hrefRegex.exec(rawHtml)) !== null) {
    const href = String(match[1] || "").trim();
    const anchorHtml = String(match[2] || "");
    const anchorText = anchorHtml.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().toLowerCase();

    if (!href) continue;
    if (/^(mailto:|tel:|javascript:|#)/i.test(href)) continue;

    const resolved = resolveUrl(href);
    if (!resolved) continue;

    const lower = `${resolved} ${anchorText}`.toLowerCase();

    if (/(instagram|facebook|tiktok|youtube|linkedin|x\.com|twitter\.com)/i.test(lower)) {
      addPoint({
        contact_type: "social_url",
        contact_value: resolved,
        confidence: 0.9,
      });
      continue;
    }

    if (/(ubereats|deliveroo|wolt|glovo|justeat|delivery|home delivery)/i.test(lower)) {
      addPoint({
        contact_type: "delivery_url",
        contact_value: resolved,
        confidence: 0.85,
      });
      continue;
    }

    if (/(order|takeaway|pickup|collection)/i.test(lower)) {
      addPoint({
        contact_type: "order_url",
        contact_value: resolved,
        confidence: 0.8,
      });
      continue;
    }

 if (
  resolved !== baseUrl &&
  /(reservation|reserve|booking|book a table|book now)/i.test(lower)
) {
  addPoint({
    contact_type: "booking_url",
    contact_value: resolved,
    confidence: 0.85,
  });
  continue;
}

if (
  resolved !== baseUrl &&
  /(contact|find us|get in touch|location)/i.test(lower)
) {
  addPoint({
    contact_type: "contact_url",
    contact_value: resolved,
    confidence: 0.75,
  });
  continue;
}
  }

  return results;
}
 

async function insertPlaceContactPoints(placeId, points = []) {
  if (!placeId) {
    const e = new Error("ENVELOPE:missing_place_id");
    e.status = 400;
    throw e;
  }

  if (!Array.isArray(points) || points.length === 0) {
    return { inserted: 0 };
  }

  const rows = points
    .filter((p) => p?.contact_type && p?.contact_value)
    .map((p) => ({
      place_id: placeId,
      contact_type: p.contact_type,
      contact_value: p.contact_value,
      provider: p.provider || "webhunter_sidecar_v1",
      source_url: p.source_url || null,
      source_domain: p.source_domain || null,
      confidence: p.confidence ?? null,
      is_primary: p.is_primary === true,
      updated_at: new Date().toISOString(),
    }));

  if (rows.length === 0) {
    return { inserted: 0 };
  }

  const { error } = await supabase
    .from("place_contact_points")
    .upsert(rows, {
      onConflict: "place_id,contact_type,contact_value",
      ignoreDuplicates: true,
    });

  if (error) {
    const e = new Error(
      `ENVELOPE:insert_place_contact_points_failed:${error.message || String(error)}`
    );
    e.status = 400;
    throw e;
  }

  return { inserted: rows.length };
}

const handlers = {
  // ---- SIDE CAR: PLACE CONTACT ENRICHMENT ----
  // Extracts phone, email, booking/order/delivery links from crawled HTML
  // Triggered after place.menu.crawl.requested completes
  // Writes into: place_contact_points (deduped via unique index)
  // Purpose:
  // - Fast enrichment of high-value user actions (call, book, order)
  // - Independent of menu parsing (runs even if menu extraction fails)
  // Notes:
  // - Uses latest successful crawl result when available
  // - Falls back to live fetch if no cached HTML
  // - Idempotent via (place_id, contact_type, contact_value)
  // - Designed for backfill + continuous ingestion
  "place.sidecar.extract.requested": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;

    const placeId =
      mustString(envelope?.payload?.place_id) ||
      mustString(envelope?.entity_id);

    if (!placeId) {
      const e = new Error("ENVELOPE:missing_place_id");
      e.status = 400;
      throw e;
    }

    const { data: sourceRows, error: sourceErr } = await supabase
      .from("place_menu_sources")
      .select("id, place_id, source_url, source_domain, source_type, created_at")
      .eq("place_id", placeId)
      .eq("source_type", "website")
      .order("created_at", { ascending: true })
      .limit(1);

    if (sourceErr) {
      const e = new Error(`ENVELOPE:get_sidecar_source_failed:${sourceErr.message || String(sourceErr)}`);
      e.status = 400;
      throw e;
    }

    const source = Array.isArray(sourceRows) ? sourceRows[0] : null;
    if (!source?.source_url) {
      jlog("place_sidecar_no_source", { msgId, place_id: placeId });
      return;
    }

    const { data: crawlRows, error: crawlErr } = await supabase
      .from("place_menu_crawl_results")
      .select("id, source_url, final_url, raw_html, raw_text, content_type, created_at")
      .eq("place_id", placeId)
      .eq("source_id", source.id)
      .eq("fetch_status", "success")
      .order("created_at", { ascending: false })
      .limit(1);

    if (crawlErr) {
      const e = new Error(`ENVELOPE:get_sidecar_crawl_result_failed:${crawlErr.message || String(crawlErr)}`);
      e.status = 400;
      throw e;
    }

    let crawl = Array.isArray(crawlRows) ? crawlRows[0] : null;
    let html = crawl?.raw_html || "";
    let finalUrl = crawl?.final_url || source.source_url;

    if (!html) {
      const fetched = await fetchMenuSourcePage(source.source_url);
      html = fetched.raw_html || "";
      finalUrl = fetched.final_url || source.source_url;
    }

    if (!html) {
      jlog("place_sidecar_no_html", {
        msgId,
        place_id: placeId,
        source_url: source.source_url,
      });
      return;
    }

    const provider = "webhunter_sidecar_v1";

    const points = buildSidecarContactPoints({
      html,
      baseUrl: finalUrl,
      sourceUrl: finalUrl,
      sourceDomain: source.source_domain || null,
      provider,
    });

    const result = await insertPlaceContactPoints(placeId, points);

    jlog("place_sidecar_done", {
      msgId,
      place_id: placeId,
      source_url: source.source_url,
      final_url: finalUrl,
      contact_point_count: points.length,
      inserted_count: result.inserted,
    });
  },


  // USDA seed queue tick (scheduler → top up queue from USDA catalog if low → claim seed rows → publish ingest requests)


  "food.usda.seed.tick": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;
    const batch_size = envelope?.payload?.batch_size ?? 80;

    jlog("usda_seed_tick_start", { msgId, batch_size });

    const { count: queuedCount, error: qCountErr } = await supabase
      .from("usda_seed_queue")
      .select("fdc_id", { count: "exact", head: true })
      .eq("status", "queued");

    if (qCountErr) throw new Error("usda_seed_queue_count_failed: " + qCountErr.message);

    jlog("usda_seed_queue_count", { msgId, queued: queuedCount ?? null });

    if ((queuedCount ?? 0) < batch_size) {
      const { data: cursorRow, error: cursorErr } = await supabase
        .from("usda_catalog_cursor")
        .select("stream_key,page_number,page_size")
        .eq("stream_key", "foundation")
        .single();

      if (cursorErr) throw new Error("usda_cursor_read_failed: " + cursorErr.message);

      const pageNumber = Number(cursorRow.page_number || 1);
      const pageSize = Number(cursorRow.page_size || 200);

      jlog("usda_catalog_fetch_start", { msgId, pageNumber, pageSize });

      const resp = await fetchUsdaFoodsPage(pageNumber, pageSize);
      const foods = Array.isArray(resp?.data) ? resp.data : (Array.isArray(resp?.data?.foods) ? resp.data.foods : []);

      const rowsToInsert = foods
        .map((f) => mustString((f?.fdcId ?? f?.fdc_id)?.toString()))
        .filter(Boolean)
        .map((fdc_id) => ({ fdc_id }));

      if (rowsToInsert.length > 0) {
        const { error: insertErr } = await supabase
          .from("usda_seed_queue")
          .upsert(rowsToInsert, { onConflict: "fdc_id", ignoreDuplicates: true });

        if (insertErr) {
          throw new Error("usda_catalog_enqueue_failed: " + insertErr.message);
        }
      }

      const { error: cursorUpdateErr } = await supabase
        .from("usda_catalog_cursor")
        .update({
          page_number: pageNumber + 1,
          updated_at: new Date().toISOString(),
        })
        .eq("stream_key", "foundation");

      if (cursorUpdateErr) throw new Error("usda_cursor_update_failed: " + cursorUpdateErr.message);

      jlog("usda_catalog_fetch_done", {
        msgId,
        pageNumber,
        fetched: foods.length,
        enqueued: rowsToInsert.length,
      });
    }

    const { data: rows, error } = await supabase.rpc("claim_usda_seed_batch_v1", {
      p_batch_size: batch_size,
    });

    if (error) throw new Error("usda_seed_claim_failed: " + error.message);

    const claimed = Array.isArray(rows) ? rows.length : 0;
    jlog("usda_seed_claimed", { msgId, claimed });

    for (const r of rows || []) {
      const fdc_id = r.fdc_id;
      const dedupe_key = `usda_seed_${fdc_id}`;

      const outEnv = {
        source: "usda_seed",
        dedupe_key,
        event_type: "food.usda.ingest.requested",
        payload: { fdc_id },
      };

      await publishFoodEvent(outEnv);
      jlog("usda_seed_published", { msgId, fdc_id, dedupe_key });
    }
  },

"menu.extract.requested": async ({ envelope, meta }) => {
  const msgId = meta?.msgId;

  const extraction = await claimStagedMenuExtraction();

  if (!extraction) {
    jlog("menu_extract_noop", { msgId });
    return;
  }

  const sourceType = extraction?.input_kind || null;
  const acquiredVia = extraction?.acquired_via || null;
  const extractionSourceUrl = extraction?.source_url || null;

  let provider = "webhunter_html";

  if (acquiredVia === "apify") {
    provider = "apify";
  } else if (sourceType === "website" && extractionSourceUrl) {
    provider = "crawl4ai";
  }

  jlog("menu_extract_provider_routed", {
    msgId,
    extraction_id: extraction?.id || null,
    provider,
    source_type: sourceType,
    acquired_via: acquiredVia,
    source_url: extractionSourceUrl,
  });

  const url = extraction.source_url || "";
  const crawlResultId = getNested(extraction.raw, ["source", "crawl_result_id"], null);

  let crawlResult = null;
  let parseMethod = "basic_html_pdf";
  let extractorVersion = "v1_basic_html";

  if (crawlResultId) {
    try {
      crawlResult = await getPlaceMenuCrawlResultById(crawlResultId);
    } catch (err) {
      jlog("menu_extract_crawl_result_load_failed", {
        msgId,
        extraction_id: extraction.id,
        crawl_result_id: crawlResultId,
        err: err?.message || String(err),
      });
    }
  }

  const contentType =
    crawlResult?.content_type ||
    getNested(extraction.raw, ["meta", "content_type"], null) ||
    null;

  const effectiveUrl =
    url ||
    crawlResult?.detected_menu_url ||
    crawlResult?.final_url ||
    crawlResult?.source_url ||
    null;

  const raw = {
    source: {
      url: effectiveUrl,
      crawl_result_id: crawlResult?.id || crawlResultId || null,
      source_url: crawlResult?.source_url || null,
      final_url: crawlResult?.final_url || null,
    },
    meta: {
      detected_at: new Date().toISOString(),
      parser_version: extractorVersion,
      document_type: null,
      page_title: crawlResult?.page_title || null,
      content_type: contentType,
      fetch_status: crawlResult?.fetch_status || null,
      needs_pdf_parse: false,
      fetch_error: null,
    },
    menu: {
      sections: [],
    },
    confidence: {
      overall: 0,
    },
  };

  const isPdf =
    /application\/pdf/i.test(String(contentType || "")) ||
    /\.pdf(\?|#|$)/i.test(String(effectiveUrl || ""));

  if (isPdf) {
    raw.meta.document_type = "pdf";
    raw.meta.needs_pdf_parse = true;
    raw.confidence.overall = 0.1;
    parseMethod = "pdf_pending";
    extractorVersion = "v1_pdf_pending";
  } else {
    raw.meta.document_type = "html";

    let lines = [];

    if (crawlResult?.raw_html) {
      lines = htmlToLines(crawlResult.raw_html);
    } else if (crawlResult?.raw_text) {
      lines = textToLines(crawlResult.raw_text);
    } else {
  try {
    let fetched = null;

    const candidateUrls = buildMenuCandidates(effectiveUrl);

    const candidateLinks = candidateUrls.map((u) => ({
      url: u,
      text: u.split("/").pop() || u,
    }));

    const classified = await classifyMenuLinksWithLLM(candidateLinks);

    const bestUrls = [
      ...(classified.menu || []),
      ...(classified.order || []),
    ].slice(0, 3);

    const urlsToTry = bestUrls.length > 0 ? bestUrls : candidateUrls.slice(0, 3);

    let bestFetch = null;

jlog("menu_candidates", {
  msgId,
  extraction_id: extraction.id,
  urls: urlsToTry,
});

    for (const candidateUrl of urlsToTry) {
      try {
        let result = null;

        if (provider === "crawl4ai") {
          try {
            result = await callCrawlerExtract(candidateUrl);
            jlog("menu_extract_crawler_success", {
              msgId,
              extraction_id: extraction.id,
              url: candidateUrl,
              final_url: result?.final_url || null,
              http_status: result?.http_status || null,
              content_type: result?.content_type || null,
              raw_html_len: result?.raw_html?.length || 0,
              raw_text_len: result?.raw_text?.length || 0,
              preview: String(result?.raw_text || "").slice(0, 300),
            });
          } catch (err) {
            jlog("menu_extract_crawler_failed_fallback", {
              msgId,
              extraction_id: extraction.id,
              url: candidateUrl,
              err: err?.message || String(err),
            });

            result = await fetchMenuSourcePage(candidateUrl);
          }
        } else {
          result = await fetchMenuSourcePage(candidateUrl);
        }

        if (!result) continue;

        const text = result.raw_text || result.raw_html || "";
        const score = (text.match(/£|\$|€/g) || []).length;

        if (!bestFetch || score > bestFetch.score) {
          bestFetch = { ...result, score, url: candidateUrl };
        }
      } catch (err) {
        // ignore this candidate and continue
      }
    }

jlog("menu_best_fetch_selected", {
  msgId,
  extraction_id: extraction.id,
  best_url: bestFetch?.url || null,
  final_url: bestFetch?.final_url || null,
  score: bestFetch?.score || 0,
});

    fetched = bestFetch;

    if (!fetched) {
      throw new Error("ENVELOPE:no_candidate_fetch_succeeded");
    }

    raw.meta.page_title = raw.meta.page_title || fetched.page_title || null;
    raw.meta.content_type = raw.meta.content_type || fetched.content_type || null;
    lines = fetched.raw_html
      ? htmlToLines(fetched.raw_html)
      : textToLines(fetched.raw_text);
  } catch (err) {
    raw.meta.fetch_error = err?.message || String(err);
  }
}

    const sections = buildBasicMenuFromLines(lines);
    const summary = summarizeExtraction(sections);

    raw.menu.sections = sections;
    raw.meta.section_count = summary.sectionCount;
    raw.meta.priced_item_count = summary.pricedCount;

    if (summary.itemCount >= 20) raw.confidence.overall = 0.7;
    else if (summary.itemCount >= 8) raw.confidence.overall = 0.55;
    else if (summary.itemCount >= 3) raw.confidence.overall = 0.4;
    else raw.confidence.overall = 0.2;

    extractorVersion = "v1_basic_html";
    parseMethod = "basic_html_sections";
  }

  const insertStats = await insertPlaceMenuItemsFromExtraction({
    ...extraction,
    raw,
  });

  const itemCount = Array.isArray(raw.menu.sections)
    ? raw.menu.sections.reduce((sum, section) => sum + (section.items?.length || 0), 0)
    : 0;

  await updateMenuExtractionRow(extraction.id, {
    raw,
    item_count: itemCount,
    extractor: "webhunter_v1",
    extractor_version: extractorVersion,
    parse_method: parseMethod,
    extraction_confidence: raw.confidence.overall,
    status: "needs_review",
    extracted_at: new Date().toISOString(),
  });

  jlog("menu_extract_done", {
    msgId,
    extraction_id: extraction.id,
    url: effectiveUrl,
    crawl_result_id: crawlResult?.id || null,
    item_count: itemCount,
    inserted_item_count: insertStats.inserted,
    skipped_item_count: insertStats.skipped,
    parse_method: parseMethod,
    confidence: raw.confidence.overall,
  });
},

  "test.event": async (ctx) => notImplementedHandler(ctx, "test.event"),

  // NEW: handle DB-triggered updates → enqueue enrichment if gaps exist
  "FOOD_ITEM_UPDATED": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;

    const food_item_id =
      mustString(envelope?.entity_id) ||
      mustString(envelope?.payload?.food_item_id) ||
      mustString(envelope?.payload?.foodId);

    if (!food_item_id) {
      const e = new Error("ENVELOPE:missing_food_item_id");
      e.status = 400;
      throw e;
    }

    // Check missing macro view
    const { data: rows, error: gapsErr } = await supabase
      .from("food_items_missing_macros")
      .select("food_item_id,missing_calories,missing_protein,missing_carbs,missing_fat")
      .eq("food_item_id", food_item_id)
      .limit(1);

    if (gapsErr) throw gapsErr;

    const row = Array.isArray(rows) ? rows[0] : null;

    const gap_keys = [];
    if (row?.missing_calories) gap_keys.push("calories_kcal_per_100g");
    if (row?.missing_protein) gap_keys.push("protein_g_per_100g");
    if (row?.missing_carbs) gap_keys.push("carbs_g_per_100g");
    if (row?.missing_fat) gap_keys.push("fat_g_per_100g");

    if (gap_keys.length === 0) {
      jlog("food_item_updated_no_gaps_skip", { msgId, food_item_id });
      return;
    }

    // Prevent spam: 10-minute bucket dedupe key (idempotency via worker_events)
    const bucket = Math.floor(Date.now() / (10 * 60 * 1000));
    const dedupe_key = `webhunter_enrich:${food_item_id}:${bucket}`;

    const outEnv = {
      source: "worker",
      dedupe_key,
      event_type: "food.webhunter.enrich.requested",
      entity_type: "food_item",
      entity_id: food_item_id,
      occurred_at: new Date().toISOString(),
      trace_id: envelope?.trace_id ?? null,
      payload: { food_item_id, gap_keys },
    };

    jlog("food_item_updated_publish_webhunter", {
      msgId,
      food_item_id,
      gap_count: gap_keys.length,
      gap_keys,
      dedupe_key,
      topic: TOPIC_PATH,
    });

    await publishFoodEvent(outEnv);
  },

  "FOOD_ITEM_SEO_UPDATED": async ({ envelope, meta }) => {
    jlog("food_item_seo_updated_skip", {
      msgId: meta?.msgId ?? null,
      entity_id: envelope?.entity_id ?? null,
      reason: "no_followup_required",
    });
  },

  "food.usda.ingest": async ({ envelope, meta }) => {
    const fdcId =
      mustString(envelope?.payload?.source_food_id) ||
      mustString(envelope?.payload?.fdc_id);

    if (!fdcId) {
      const e = new Error("ENVELOPE:missing_source_food_id");
      e.status = 400;
      throw e;
    }

    const allowed = await tryConsumeDailyQuota({
      quota_key: USDA_QUOTA_KEY,
      limit: USDA_DAILY_LIMIT,
      amount: 1,
    });

    if (!allowed) {
      jlog("quota_exhausted_skip", {
        msgId: meta?.msgId,
        quota_key: USDA_QUOTA_KEY,
        daily_limit: USDA_DAILY_LIMIT,
        fdcId,
      });
      return;
    }

    jlog("usda_fetch_start", { msgId: meta?.msgId, fdcId });

      let usda;
      try {
        const resp = await fetchUsdaFood(fdcId);
        usda = resp.data;
      } catch (e) {
        if ((e?.message || "") === "ENVELOPE:usda_not_found") {
          const { error: qDeadErr } = await supabase
            .from("usda_seed_queue")
            .update({
              status: "dead",
              last_error: "ENVELOPE:usda_not_found",
              lease_expires_at: null,
              updated_at: new Date().toISOString(),
            })
            .eq("fdc_id", fdcId)
            .eq("status", "leased");

          if (qDeadErr) {
            jlog("usda_seed_queue_dead_update_failed", {
              msgId: meta?.msgId,
              fdcId,
              err: qDeadErr.message,
            });
          } else {
            jlog("usda_seed_queue_dead", { msgId: meta?.msgId, fdcId });
          }
        }
        throw e;
      }

    const name = mustString(usda?.description) || `USDA ${fdcId}`;

    // clean ref url (no api_key)
    const sourceUrl = `https://api.nal.usda.gov/fdc/v1/food/${encodeURIComponent(fdcId)}`;

    const foodId = await rpcOrThrow("upsert_food_item_from_usda_v1", {
      p_source_id: fdcId,
      p_name: name,
      p_source_url: sourceUrl,
    });

    if (!foodId) throw new Error("upsert_food_item_from_usda_v1 returned null");

    jlog("db_food_upsert_ok", { msgId: meta?.msgId, fdcId, food_id: foodId });

    const macros = extractUsdaMacrosPer100g(usda);

    const source = "usda";
    const source_ref = `fdc:${fdcId}`;
    const occurred_at = envelope?.occurred_at || meta?.publishTime || new Date().toISOString();
    const evidence_confidence = 1.0;

    const macroWrites = [
      { key: "calories_kcal_per_100g", val: macros.calories_kcal_per_100g },
      { key: "protein_g_per_100g", val: macros.protein_g_per_100g },
      { key: "carbs_g_per_100g", val: macros.carbs_g_per_100g },
      { key: "fat_g_per_100g", val: macros.fat_g_per_100g },
      { key: "fiber_g_per_100g", val: macros.fiber_g_per_100g },
      { key: "sugar_g_per_100g", val: macros.sugar_g_per_100g },
    ];

    for (const w of macroWrites) {
      if (w.val === null) {
        jlog("macro_missing_in_usda", { msgId: meta?.msgId, fdcId, macro_key: w.key });
        continue;
      }

      jlog("db_macro_evidence_upsert_start", {
        msgId: meta?.msgId,
        fdcId,
        food_id: foodId,
        macro_key: w.key,
        value_per_100g: w.val,
      });

      await rpcUpsertMacroEvidence({
        food_item_id: foodId,
        macro_key: w.key,
        value_per_100g: w.val,
        source,
        source_ref,
        evidence_confidence,
        occurred_at,
      });
    }

    jlog("macro_evidence_upsert_done", { msgId: meta?.msgId, fdcId, food_id: foodId });

    jlog("phase2_start", { msgId: meta?.msgId, fdcId, food_id: foodId });

    await rpcOrThrow("resolve_food_item_macros_v1_2", { p_food_item_id: foodId });
    jlog("resolve_macros_ok", { msgId: meta?.msgId, fdcId, food_id: foodId });

    await rpcOrThrow("compute_food_item_quality_rollup_v1", { p_food_item_id: foodId });
    jlog("quality_rollup_ok", { msgId: meta?.msgId, fdcId, food_id: foodId });

    await rpcOrThrow("compute_food_item_seo_v2", { p_food_item_id: foodId });
    jlog("seo_compute_ok", { msgId: meta?.msgId, fdcId, food_id: foodId });
      // Mark seed queue item done when USDA ingest succeeds
      const { error: qDoneErr } = await supabase
        .from("usda_seed_queue")
        .update({
          status: "done",
          processed_at: new Date().toISOString(),
          last_error: null,
          lease_expires_at: null,
          updated_at: new Date().toISOString(),
        })
        .eq("fdc_id", fdcId)
        .eq("status", "leased");

      if (qDoneErr) {
        jlog("usda_seed_queue_done_update_failed", {
          msgId: meta?.msgId,
          fdcId,
          err: qDoneErr.message,
        });
      } else {
        jlog("usda_seed_queue_done", { msgId: meta?.msgId, fdcId });
      }
  },

  "food.usda.ingest.requested": async (ctx) => handlers["food.usda.ingest"](ctx),

  // Webhunter enrichment (plumbing-only pass)
  "food.webhunter.enrich": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;

    const food_item_id =
      mustString(envelope?.payload?.food_item_id) ||
      mustString(envelope?.payload?.foodId) ||
      mustString(envelope?.entity_id);

    if (!food_item_id) {
      const e = new Error("ENVELOPE:missing_food_item_id");
      e.status = 400;
      throw e;
    }

    const allowed = await tryConsumeDailyQuota({
      quota_key: WEBHUNTER_QUOTA_KEY,
      limit: WEBHUNTER_DAILY_LIMIT,
      amount: 1,
    });

    if (!allowed) {
      jlog("webhunter_quota_exhausted_skip", {
        msgId,
        quota_key: WEBHUNTER_QUOTA_KEY,
        daily_limit: WEBHUNTER_DAILY_LIMIT,
        food_item_id,
      });
      return;
    }

    // ✅ Use existing view: food_items_missing_macros
    const { data: rows, error: gapsErr } = await supabase
      .from("food_items_missing_macros")
      .select("food_item_id,slug,name,missing_calories,missing_protein,missing_carbs,missing_fat")
      .eq("food_item_id", food_item_id)
      .limit(1);

    if (gapsErr) throw gapsErr;

    const row = Array.isArray(rows) ? rows[0] : null;

    // Derive gap keys
    const gap_keys = [];
    if (row?.missing_calories) gap_keys.push("calories_kcal_per_100g");
    if (row?.missing_protein) gap_keys.push("protein_g_per_100g");
    if (row?.missing_carbs) gap_keys.push("carbs_g_per_100g");
    if (row?.missing_fat) gap_keys.push("fat_g_per_100g");

    jlog("webhunter_gaps_loaded", {
      msgId,
      food_item_id,
      slug: row?.slug ?? null,
      name: row?.name ?? null,
      gap_count: gap_keys.length,
      gap_keys: gap_keys.slice(0, 20),
    });

    // Senior-engineer guard: if there are no gaps, do nothing further.
    // (This handler is plumbing-only until webhunter actually writes evidence rows.)
    if (gap_keys.length === 0) {
      jlog("webhunter_no_gaps_skip", { msgId, food_item_id });
      return;
    }

    // Temporary Webhunter evidence placeholder (production-safe stub)
    // This does NOT write evidence yet — it just proves the handler path runs at scale.
    jlog("webhunter_stub_active", {
      msgId,
      food_item_id,
      gap_count: gap_keys.length,
      gap_keys: gap_keys.slice(0, 20),
    });

    // NOTE: real webhunting will happen later (write evidence rows with source='webhunter')
    // Production guard: do NOT burn compute/quota on recompute unless explicitly enabled.
    if (!WEBHUNTER_STUB_RECOMPUTE) {
      jlog("webhunter_stub_disabled_skip", { msgId, food_item_id, gap_count: gap_keys.length });
      return;
    }

    await rpcOrThrow("resolve_food_item_macros_v1_2", { p_food_item_id: food_item_id });
    await rpcOrThrow("compute_food_item_quality_rollup_v1", { p_food_item_id: food_item_id });
    await rpcOrThrow("compute_food_item_seo_v2", { p_food_item_id: food_item_id });

    jlog("webhunter_recompute_ok", { msgId, food_item_id });
  },

  "food.webhunter.enrich.requested": async (ctx) => handlers["food.webhunter.enrich"](ctx),
  "place.menu.crawl.requested": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;

    const batch = await claimPlaceMenuCrawlBatch();

    if (!batch) {
      jlog("place_menu_crawl_no_batches", {
        msgId,
        event_type: envelope?.event_type ?? null,
      });
      return;
    }

    jlog("place_menu_crawl_batch_claimed", {
      msgId,
      batch_id: batch.id,
      source_id: batch.source_id,
      batch_name: batch.batch_name,
      priority_score: batch.priority_score,
      attempts: batch.attempts,
    });

    try {
      const source = await getPlaceMenuSourceById(batch.source_id);

      jlog("place_menu_crawl_source_loaded", {
        msgId,
        batch_id: batch.id,
        source_id: source.id,
        place_id: source.place_id,
        source_type: source.source_type,
        source_url: source.source_url,
        source_domain: source.source_domain,
        registrable_domain: source.registrable_domain,
      });

      const fetched = await fetchMenuSourcePage(source.source_url);

      const fetchStatus = fetched.ok ? "success" : "failed";

      const crawlResult = await insertPlaceMenuCrawlResult({
        crawl_batch_id: batch.id,
        source_id: source.id,
        place_id: source.place_id,
        source_url: fetched.source_url,
        final_url: fetched.final_url,
        http_status: fetched.http_status,
        fetch_status: fetchStatus,
        content_type: fetched.content_type,
        page_title: fetched.page_title,
        raw_html: fetched.raw_html,
        raw_text: fetched.raw_text,
        detected_menu_url: fetched.detected_menu_url,
        detected_language: null,
        error_text: fetched.ok ? null : `http_status_${fetched.http_status}`,
      });

      await updatePlaceMenuSourceAfterCrawl(source.id, {
        crawl_status: fetched.ok ? "done" : "failed",
        crawl_attempts: Number(source.crawl_attempts || 0) + 1,
        last_crawl_at: new Date().toISOString(),
      });

// NEW: stub extraction insert

const detected = fetched.detected_menu_url || "";

const isBadDetected =
  !detected ||
  /\.(css|js|png|jpg|jpeg|gif|svg|webp|ico)(\?|$)/i.test(detected) ||
  /(christmas|event|blog|news|press|contact|about|privacy|terms)/i.test(detected);

const chosenUrl = isBadDetected
  ? (fetched.final_url || fetched.source_url)
  : detected;

await supabase.from("menu_extractions").insert({
  place_id: source.place_id,
  candidate_source_id: null,
  source_url: chosenUrl,
  provider: "webhunter",
  extracted_restaurant_name: null,
  completeness_grade: "stub",
  item_count: 0,
  raw: {
    source: {
      place_id: source.place_id,
      source_id: source.id,
      crawl_batch_id: batch.id,
      crawl_result_id: crawlResult?.id ?? null,
      url: chosenUrl
    },
    meta: {
      detected_at: new Date().toISOString(),
      parser_version: "v1_stub",
      page_title: fetched.page_title,
      content_type: fetched.content_type
    },
    menu: {
      sections: []
    },
    confidence: {
      overall: 0
    }
  },
  match_score: null,
  match_reasons: {},
  status: "staged",
  extractor_version: "v1_stub",
  extraction_confidence: 0,
  source_trust_weight: null,
  language_code: null,
  translated_language_code: null,
  translation_provider: null,
  translation_confidence: null,
  menu_hash: null,
  auto_approved_at: null,
  auto_approval_reason: null,
  approved_place_menu_id: null,
  extractor: "webhunter_stub",
  parse_method: "crawl_stub"
});

// insert menu_extractions (already done above)

// SIDE CAR follow-up:
await publishFoodEvent({
  source: "place_menu_crawl",
  dedupe_key: `place_sidecar_extract:${source.place_id}:${crawlResult?.id ?? batch.id}`,
  event_type: "place.sidecar.extract.requested",
  entity_type: "place",
  entity_id: source.place_id,
  payload: {
    place_id: source.place_id,
    source_id: source.id,
    crawl_result_id: crawlResult?.id ?? null,
  },
});

// exististing 
      await markPlaceMenuCrawlBatchDone(batch.id);

      jlog("place_menu_crawl_done", {
        msgId,
        batch_id: batch.id,
        source_id: source.id,
        place_id: source.place_id,
        crawl_result_id: crawlResult?.id ?? null,
        http_status: fetched.http_status,
        content_type: fetched.content_type,
        detected_menu_url: fetched.detected_menu_url,
        page_title: fetched.page_title,
        raw_text_len: fetched.raw_text?.length ?? 0,
      });
    } catch (err) {
      await markPlaceMenuCrawlBatchFailed(batch.id, err?.message || String(err));

      jlog("place_menu_crawl_failed", {
        msgId,
        batch_id: batch.id,
        err: err?.message || String(err),
      });

      throw err;
    }
  },

  "places.seed.control.tick": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;
    const countryCode = String(envelope?.payload?.country_code || "LU").toUpperCase();
    const targetRunning = Number(envelope?.payload?.target_running || 2);

    const resetCount = await rpcOrThrow("reset_stale_place_seed_targets_v1", {});

    const { count: runningCount, error: runningErr } = await supabase
      .from("place_seed_targets")
      .select("id", { count: "exact", head: true })
      .eq("status", "running");

    if (runningErr) throw runningErr;

    const { data: pendingRows, error: pendingErr } = await supabase
      .from("place_seed_targets")
      .select("id, region_id")
      .in("status", ["pending", "retry"]);

    if (pendingErr) throw pendingErr;

    let pendingCount = 0;

    if (Array.isArray(pendingRows) && pendingRows.length > 0) {
      const regionIds = [...new Set(
        pendingRows
          .map((row) => row?.region_id)
          .filter(Boolean)
      )];

      let allowedRegionIds = new Set();

      if (regionIds.length > 0) {
        const { data: regionRows, error: regionErr } = await supabase
          .from("place_seed_regions")
          .select("id, country_code")
          .in("id", regionIds)
          .eq("country_code", countryCode);

        if (regionErr) throw regionErr;

        allowedRegionIds = new Set((regionRows || []).map((row) => row.id));
      }

      pendingCount = pendingRows.filter((row) => allowedRegionIds.has(row.region_id)).length;
    }

    jlog("places_seed_control_status", {
      msgId,
      country_code: countryCode,
      target_running: targetRunning,
      reset_count: resetCount ?? 0,
      running_count: runningCount ?? 0,
      pending_count: pendingCount ?? 0,
    });

    if ((runningCount || 0) < targetRunning && (pendingCount || 0) > 0) {
      const controlEnv = {
        source: "places_seed_control",
        dedupe_key: `places_seed_control_release:${countryCode}:${msgId}`,
        event_type: "places.seed.city",
        payload: { country_code: countryCode },
      };

      await publishFoodEvent(controlEnv);

      jlog("places_seed_control_released", {
        msgId,
        country_code: countryCode,
      });
    }
  },

  "places.seed.process.requested": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;
    const batchSize = Number(envelope?.payload?.batch_size || 10);

    const rows = await rpcOrThrow("process_places_seed_batch_v1", {
      p_batch_size: batchSize,
    });

    const processedCount = Array.isArray(rows) ? rows.length : 0;

    jlog("places_seed_process_batch_done", {
      msgId,
      batch_size: batchSize,
      processed_count: processedCount,
    });

    const { count: queuedCount, error: qErr } = await supabase
      .from("places_seed_queue")
      .select("source_place_id", { count: "exact", head: true })
      .eq("status", "queued");

    if (qErr) throw qErr;

    jlog("places_seed_queue_remaining", {
      msgId,
      queued_count: queuedCount ?? null,
    });

    if ((queuedCount || 0) > 0 && processedCount > 0) {
      const nextEnv = {
        source: "places_seed",
        dedupe_key: `places_seed_process_followup:${msgId}:${queuedCount}`,
        event_type: "places.seed.process.requested",
        payload: { batch_size: batchSize },
      };

      await publishFoodEvent(nextEnv);

      jlog("places_seed_process_republished", {
        msgId,
        queued_count: queuedCount,
        batch_size: batchSize,
      });
    }
  },

  "places.seed.city": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;

    const target = await claimPlaceSeedTarget();

    if (!target) {
      jlog("places_seed_no_targets", {
        msgId,
        event_type: envelope?.event_type ?? null,
      });
      return;
    }

    jlog("places_seed_target_claimed", {
      msgId,
      target_id: target.id,
      source: target.source,
      target_type: target.target_type,
      categories: target.categories,
      last_offset: target.last_offset,
      page_size: target.page_size,
    });

    try {
      const allowed = await rpcOrThrow("places_seed_daily_allowed_v1", {
      p_source: "geoapify",
      p_daily_limit: GEOAPIFY_DAILY_LIMIT,
    });

    if (Number(target?.last_offset || 0) >= GEOAPIFY_MAX_TARGET_OFFSET) {
      jlog("geoapify_target_offset_limit_reached", {
        target_id: target.id,
        last_offset: Number(target?.last_offset || 0),
        max_target_offset: GEOAPIFY_MAX_TARGET_OFFSET,
      });
      await markPlaceSeedTargetDone(target.id);
      return;
    }

    if (!allowed) {
      jlog("geoapify_daily_limit_reached", {
        target_id: target.id,
        limit: GEOAPIFY_DAILY_LIMIT,
      });
      return;
    }

    const placesData = await fetchGeoapifyPlaces(target);
      const features = Array.isArray(placesData?.features) ? placesData.features : [];
      const pageSize = Number(target?.page_size || 50);

      jlog("places_seed_places_fetched", {
        msgId,
        target_id: target.id,
        feature_count: features.length,
        page_size: pageSize,
        offset: target.last_offset,
      });

      for (const placeFeature of features) {
        const sourcePlaceId = asTrimmedString(placeFeature?.properties?.place_id);

        if (!sourcePlaceId) {
          jlog("places_seed_skip_missing_place_id", {
            msgId,
            target_id: target.id,
          });
          continue;
        }

        const detailsResponse = await fetchGeoapifyPlaceDetails(sourcePlaceId);
        const detailsFeature = selectGeoapifyDetailsFeature(detailsResponse);
        const normalized = buildNormalizedGeoapifySeedPayload(placeFeature, detailsFeature);

        await enqueueGeoapifyPlaceSeed(
          normalized,
          sourcePlaceId,
          Number(target?.priority || 100)
        );

        jlog("places_seed_enqueued", {
          msgId,
          target_id: target.id,
          source_place_id: sourcePlaceId,
          name: normalized.name,
          category: normalized.category,
          city: normalized.city,
          country_code: normalized.country_code,
        });
      }

      const drainEnv = {
        source: "places_seed",
        dedupe_key: `places_seed_process:${target.id}:${Number(target.last_offset || 0)}`,
        event_type: "places.seed.process.requested",
        payload: { target_id: target.id, batch_size: 10 },
      };

      await publishFoodEvent(drainEnv);

      if (features.length === pageSize) {
        const nextOffset = Number(target.last_offset || 0) + pageSize;
        await markPlaceSeedTargetProgress(target.id, nextOffset);

        jlog("places_seed_target_progress", {
          msgId,
          target_id: target.id,
          next_offset: nextOffset,
        });

        const resumeEnv = {
          source: "places_seed",
          dedupe_key: `places_seed_city_resume:${target.id}:${nextOffset}`,
          event_type: "places.seed.city",
          payload: { target_id: target.id },
        };

        await publishFoodEvent(resumeEnv);
      } else {
        await markPlaceSeedTargetDone(target.id);

        jlog("places_seed_target_done", {
          msgId,
          target_id: target.id,
          final_offset: target.last_offset,
          feature_count: features.length,
        });
      }
    } catch (err) {
      await markPlaceSeedTargetFailed(target.id, err?.message || String(err));

      jlog("places_seed_target_failed", {
        msgId,
        target_id: target.id,
        err: err?.message || String(err),
      });

      throw err;
    }
  },
};

function getHandler(eventType) {
  return handlers[eventType] || null;
}

// ---- ack/nack safety ----
function ackOnce(m) {
  if (m.__mew_done) return false;
  m.__mew_done = true;
  m.ack();
  return true;
}
function nackOnce(m) {
  if (m.__mew_done) return false;
  m.__mew_done = true;
  m.nack();
  return true;
}

async function init() {
  // 1) Supabase
  workerCfg = loadWorkerConfig();
  supabase = createClient(workerCfg.SUPABASE_URL, workerCfg.SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });

  // 2) Pub/Sub
  pubsubClient = new PubSub({ projectId: PROJECT_ID });
  subscription = pubsubClient.subscription(SUB_PATH, {
    flowControl: { maxMessages: MAX_MESSAGES, allowExcessMessages: false },
  });

  subscription.on("error", (err) => {
    setErr(err);
    jlog("pubsub_error_listener", { err: stats.lastErr });
  });

  subscription.on("message", async (m) => {
    const release = await sem.acquire();
    let msgId = m?.id;
    try {
      msgId = m.id;
      const attributes = m.attributes || {};
      stats.recv += 1;
      stats.lastMsgId = msgId;

      jlog("pubsub_recv", {
        msgId,
        attributes,
        sem_active: sem.active,
        sem_queued: sem.queued,
      });

      const raw = m.data ? m.data.toString("utf8") : "";
      if (!raw) {
        jlog("pubsub_invalid_envelope", { msgId, reason: "empty_body" });
        stats.dropped += 1;
        if (ackOnce(m)) {
          stats.acked += 1;
          jlog("pubsub_acked", { msgId, reason: "empty_body" });
        }
        return;
      }

      if (Buffer.byteLength(raw, "utf8") > MAX_BODY_BYTES) {
        jlog("pubsub_invalid_envelope", { msgId, reason: "body_too_large", max: MAX_BODY_BYTES });
        stats.dropped += 1;
        if (ackOnce(m)) {
          stats.acked += 1;
          jlog("pubsub_acked", { msgId, reason: "body_too_large" });
        }
        return;
      }

      let envelope;
      try {
        envelope = JSON.parse(raw);
      } catch {
        jlog("pubsub_invalid_envelope", { msgId, reason: "invalid_json" });
        stats.dropped += 1;
        if (ackOnce(m)) {
          stats.acked += 1;
          jlog("pubsub_acked", { msgId, reason: "invalid_json" });
        }
        return;
      }

      const v = validateEnvelope(envelope);
      if (!v.ok) {
        const errMsg = `ENVELOPE:${v.reason}`;
        jlog("pubsub_invalid_envelope", { msgId, reason: v.reason });
        stats.dropped += 1;
        if (ackOnce(m)) {
          stats.acked += 1;
          jlog("pubsub_acked", { msgId, reason: errMsg });
        }
        return;
      }

      if (!envelope.dedupe_key && envelope.event_type === "food.usda.seed.tick") {
        envelope.dedupe_key = `food.usda.seed.tick:${msgId}`;
        jlog("dedupe_key_derived", { msgId, dedupe_key: envelope.dedupe_key, event_type: envelope.event_type });
      }

      if ((!envelope.dedupe_key || envelope.dedupe_key === "places_seed_control_tick_scheduler") &&
          envelope.event_type === "places.seed.control.tick") {
        envelope.dedupe_key = `places.seed.control.tick:${msgId}`;
        jlog("dedupe_key_derived", { msgId, dedupe_key: envelope.dedupe_key, event_type: envelope.event_type });
      }

      const handler = getHandler(envelope.event_type);
      if (!handler) {
        const errMsg = `unknown_event_type:${envelope.event_type}`;
        jlog("unknown_event_type", { msgId, event_type: envelope.event_type });

        // Persist as failed so you can inspect later, but ACK to avoid poison retries
        try {
          const meta = {
            msgId,
            attributes,
            publishTime: m.publishTime ? new Date(m.publishTime).toISOString() : null,
            sub: SUB_PATH,
          };
          await persistWorkerEvent(envelope, meta, "failed", errMsg);
          jlog("db_insert_ok", {
            msgId,
            dedupe_key: envelope.dedupe_key,
            source: envelope.source,
          });
        } catch (e) {
          setErr(e);
          jlog("persist_failed_unknown_event", { msgId, err: stats.lastErr });
          stats.nacked += 1;
          nackOnce(m);
          return;
        }

        stats.dropped += 1;
        if (ackOnce(m)) stats.acked += 1;
        jlog("pubsub_acked", { msgId, reason: "unknown_event_type" });
        return;
      }

      const meta = {
        msgId,
        attributes,
        publishTime: m.publishTime ? new Date(m.publishTime).toISOString() : null,
        sub: SUB_PATH,
      };

      // Phase 0: persist received first
      try {
        await persistWorkerEvent(envelope, meta, "received", null);
        jlog("db_insert_ok", { msgId, dedupe_key: envelope.dedupe_key, source: envelope.source });
      } catch (dbErr) {
        const cls = classifyError(dbErr);
        setErr(dbErr);

        if (cls.kind === "duplicate") {
          jlog("db_duplicate_ack", { msgId, dedupe_key: envelope.dedupe_key });
          if (ackOnce(m)) {
            stats.acked += 1;
            jlog("pubsub_acked", { msgId, reason: "duplicate_dedupe_key" });
          }
          return;
        }

        jlog("db_insert_failed", { msgId, err: stats.lastErr, error_class: cls.class });

        if (cls.kind === "permanent") {
          stats.dropped += 1;
          if (ackOnce(m)) {
            stats.acked += 1;
            jlog("pubsub_acked", { msgId, reason: "db_permanent" });
          }
          return;
        }

        stats.nacked += 1;
        nackOnce(m);
        return;
      }

      // Mark processing (best-effort)
      try {
        await updateWorkerEventStatus(envelope.dedupe_key, {
          status: "processing",
          last_error: null,
        });
      } catch (e) {
        setErr(e);
        jlog("db_status_update_failed", {
          msgId,
          dedupe_key: envelope.dedupe_key,
          err: stats.lastErr,
        });
      }

      // Run handler
      jlog("handler_start", { msgId, event_type: envelope.event_type });

      try {
        await handler({ envelope, meta });
        jlog("handler_ok", { msgId, event_type: envelope.event_type });

        // Mark succeeded (best-effort; should not change ack behavior if update fails)
        try {
          await updateWorkerEventStatus(envelope.dedupe_key, {
            status: "succeeded",
            last_error: null,
          });
        } catch (e) {
          setErr(e);
          jlog("db_status_update_failed", {
            msgId,
            dedupe_key: envelope.dedupe_key,
            err: stats.lastErr,
          });
        }

        if (ackOnce(m)) stats.acked += 1;
        jlog("pubsub_acked", { msgId });
      } catch (err) {
        const cls = classifyError(err);
        setErr(err);

        jlog("handler_fail", {
          msgId,
          event_type: envelope.event_type,
          err: stats.lastErr,
          error_class: cls.class,
        });

        // Persist failure status (best-effort)
        try {
          await updateWorkerEventStatus(envelope.dedupe_key, {
            status: cls.kind === "permanent" ? "failed" : "retrying",
            last_error: stats.lastErr,
          });
        } catch (e) {
          setErr(e);
          jlog("db_status_update_failed", {
            msgId,
            dedupe_key: envelope.dedupe_key,
            err: stats.lastErr,
          });
        }
          // Update USDA seed queue on handler failure
          if (envelope.event_type === "food.usda.ingest.requested") {
            const fdcId =
              mustString(envelope?.payload?.source_food_id) ||
              mustString(envelope?.payload?.fdc_id);

            if (fdcId) {
              try {
                if (cls.kind === "transient") {
                  const nextRetryAt = new Date(Date.now() + 15 * 60 * 1000).toISOString();
                  const { error: qRetryErr } = await supabase
                    .from("usda_seed_queue")
                    .update({
                      status: "retry",
                      last_error: stats.lastErr,
                      next_run_at: nextRetryAt,
                      lease_expires_at: null,
                      updated_at: new Date().toISOString(),
                    })
                    .eq("fdc_id", fdcId)
                    .eq("status", "leased");

                  if (qRetryErr) {
                    jlog("usda_seed_queue_retry_update_failed", {
                      msgId,
                      fdcId,
                      err: qRetryErr.message,
                    });
                  } else {
                    jlog("usda_seed_queue_retry", { msgId, fdcId, next_retry_at: nextRetryAt });
                  }
                }
              } catch (qErr) {
                jlog("usda_seed_queue_retry_handler_crash", {
                  msgId,
                  fdcId,
                  err: qErr?.message || String(qErr),
                });
              }
            }
          }

          if (envelope.event_type === "food.usda.ingest.requested" && cls.kind === "permanent") {
            const fdcId =
              mustString(envelope?.payload?.source_food_id) ||
              mustString(envelope?.payload?.fdc_id);

            if (fdcId) {
              try {
                const { error: qDeadErr } = await supabase
                  .from("usda_seed_queue")
                  .update({
                    status: "dead",
                    last_error: stats.lastErr,
                    lease_expires_at: null,
                    updated_at: new Date().toISOString(),
                  })
                  .eq("fdc_id", fdcId)
                  .eq("status", "leased");

                if (qDeadErr) {
                  jlog("usda_seed_queue_dead_update_failed", {
                    msgId,
                    fdcId,
                    err: qDeadErr.message,
                  });
                } else {
                  jlog("usda_seed_queue_dead", { msgId, fdcId, reason: "permanent_error" });
                }
              } catch (qErr) {
                jlog("usda_seed_queue_dead_handler_crash", {
                  msgId,
                  fdcId,
                  err: qErr?.message || String(qErr),
                });
              }
            }
          }
        if (cls.kind === "permanent") {
          stats.dropped += 1;
          if (ackOnce(m)) {
            stats.acked += 1;
            jlog("pubsub_acked", { msgId, reason: "handler_permanent" });
          }
          return;
        }

        stats.nacked += 1;
        nackOnce(m);
      }
    } catch (e) {
      setErr(e);
      stats.nacked += 1;
      jlog("pubsub_handler_crash", { msgId: m?.id, err: stats.lastErr });
      try {
        nackOnce(m);
      } catch {}
    } finally {
      release();
    }
  });

  stats.initOk = true;
  IS_READY = true;

  jlog("ready", {
    sub: SUB_PATH,
    topic: TOPIC_PATH,
    maxMessages: MAX_MESSAGES,
    usda_daily_limit: USDA_DAILY_LIMIT,
    usda_quota_key: USDA_QUOTA_KEY,
    webhunter_daily_limit: WEBHUNTER_DAILY_LIMIT,
    webhunter_quota_key: WEBHUNTER_QUOTA_KEY,
  });
}

// start server FIRST so Cloud Run is satisfied
const port = process.env.PORT || 8080;
app.listen(port, () => {
  jlog("http_listening", { port });
  Promise.resolve()
    .then(() => init())
    .catch((e) => {
      setErr(e);
      IS_READY = false;
      jlog("init_failed", { err: stats.lastErr });
    });
});
