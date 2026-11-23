// lib/usda.js
// Pure USDA helpers and handler factories. No side effects on import:
// no env reads, no clients, no network. All collaborators are passed in.

function mustString(x) {
  return typeof x === "string" && x.trim().length > 0 ? x.trim() : null;
}

function numOrNull(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : null;
}

// USDA FDC catalog streams: stream_key -> FDC API dataType.
// Same mapping as Mew-backoffice lib/worker/food/usda.ts (USDA_STREAMS).
export const USDA_STREAMS = {
  branded: "Branded",
  foundation: "Foundation",
  fndds: "Survey (FNDDS)",
  sr_legacy: "SR Legacy",
};

export function normalizeUsdaStreamKey(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return (
    Object.keys(USDA_STREAMS).find(
      (key) => key === normalized || USDA_STREAMS[key].toLowerCase() === normalized
    ) ?? null
  );
}

export async function fetchUsdaFood({ workerCfg, fetch }, fdcId) {
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

export async function fetchUsdaFoodsPage({ workerCfg, fetch }, pageNumber, pageSize = 200, dataType = null) {
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

  // Stable ordering so a cursor page always means the same slice of the stream.
  url += `&sortBy=fdcId&sortOrder=asc`;

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

// ---- USDA nutrients parsing ----
export const USDA_NUTRIENT_ID = {
  calories_kcal: 1008, // Energy (kcal)
  protein_g: 1003,
  carbs_g: 1005, // Carbohydrate, by difference
  fat_g: 1004, // Total lipid (fat)
  fiber_g: 1079, // Fiber, total dietary
  sugar_g: 2000, // Sugars, total including NLEA
};

export function extractUsdaMacrosPer100g(usdaFoodJson) {
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

// Handler for food.usda.seed.tick. deps: { supabase, fetch, workerCfg, jlog, publishFoodEvent }
export function createUsdaSeedTickHandler({ supabase, fetch, workerCfg, jlog, publishFoodEvent }) {
  const fetchUsdaFoodsPageBound = (pageNumber, pageSize, dataType) =>
    fetchUsdaFoodsPage({ workerCfg, fetch }, pageNumber, pageSize, dataType);
  return async function usdaSeedTick({ envelope, meta }) {
    const msgId = meta?.msgId;
    const batch_size = envelope?.payload?.batch_size ?? 80;

    // Each stream pages its own FDC dataType with its own cursor row. Before this,
    // the "foundation" cursor paged the unfiltered catalog (every dataType,
    // including all Branded revisions) and no row recorded its dataType.
    const stream_key = normalizeUsdaStreamKey(envelope?.payload?.stream_key ?? "foundation");
    if (!stream_key) {
      const e = new Error("ENVELOPE:invalid_usda_stream");
      e.status = 400;
      throw e;
    }
    const usda_data_type = USDA_STREAMS[stream_key];

    jlog("usda_seed_tick_start", { msgId, batch_size, stream_key });

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
        .eq("stream_key", stream_key)
        .maybeSingle();

      if (cursorErr) throw new Error("usda_cursor_read_failed: " + cursorErr.message);

      const pageNumber = Number(cursorRow?.page_number || 1);
      const pageSize = Number(cursorRow?.page_size || 200);

      jlog("usda_catalog_fetch_start", { msgId, stream_key, pageNumber, pageSize });

      const resp = await fetchUsdaFoodsPageBound(pageNumber, pageSize, usda_data_type);
      const foods = Array.isArray(resp?.data) ? resp.data : (Array.isArray(resp?.data?.foods) ? resp.data.foods : []);

      const rowsToInsert = foods
        .map((f) => ({
          fdc_id: mustString((f?.fdcId ?? f?.fdc_id)?.toString()),
          data_type: mustString(f?.dataType),
        }))
        .filter((f) => f.fdc_id)
        .map((f) => ({
          fdc_id: f.fdc_id,
          status: "queued",
          source_stream_key: stream_key,
          usda_data_type: f.data_type ?? usda_data_type,
        }));

      if (rowsToInsert.length > 0) {
        const { error: insertErr } = await supabase
          .from("usda_seed_queue")
          .upsert(rowsToInsert, { onConflict: "fdc_id", ignoreDuplicates: true });

        if (insertErr) {
          throw new Error("usda_catalog_enqueue_failed: " + insertErr.message);
        }
      }

      // Short/empty page = end of stream: wrap to page 1 instead of paging forever.
      const nextPage = foods.length < pageSize ? 1 : pageNumber + 1;
      const { error: cursorUpdateErr } = await supabase
        .from("usda_catalog_cursor")
        .upsert(
          {
            stream_key,
            page_number: nextPage,
            page_size: pageSize,
            updated_at: new Date().toISOString(),
          },
          { onConflict: "stream_key" }
        );

      if (cursorUpdateErr) throw new Error("usda_cursor_update_failed: " + cursorUpdateErr.message);

      jlog("usda_catalog_fetch_done", {
        msgId,
        stream_key,
        pageNumber,
        nextPage,
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
  };
}

// Handler for food.usda.ingest. deps: { supabase, fetch, workerCfg, jlog, tryConsumeDailyQuota,
// rpcOrThrow, rpcUpsertMacroEvidence, quotaKey, dailyLimit }
export function createUsdaIngestHandler({
  supabase,
  fetch,
  workerCfg,
  jlog,
  tryConsumeDailyQuota,
  rpcOrThrow,
  rpcUpsertMacroEvidence,
  quotaKey: USDA_QUOTA_KEY,
  dailyLimit: USDA_DAILY_LIMIT,
}) {
  const fetchUsdaFoodBound = (fdcId) => fetchUsdaFood({ workerCfg, fetch }, fdcId);
  return async function usdaIngest({ envelope, meta }) {
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
        const resp = await fetchUsdaFoodBound(fdcId);
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

    // Record which FDC dataset this row came from. Canonical election ranks
    // duplicates by usda_data_type (food_item_data_type_rank); without it every
    // row ranks as "missing" and datasets cannot be told apart.
    const detailDataType = mustString(usda?.dataType) || mustString(usda?.datatype);
    const { error: metadataErr } = await supabase
      .from("food_items")
      .update({
        source_stream_key: normalizeUsdaStreamKey(detailDataType),
        usda_data_type: detailDataType,
      })
      .eq("id", foodId)
      .eq("source", "usda")
      .eq("source_id", fdcId);

    if (metadataErr) throw new Error("usda_metadata_update_failed: " + metadataErr.message);
    jlog("usda_metadata_ok", { msgId: meta?.msgId, fdcId, food_id: foodId, usda_data_type: detailDataType });

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

    // compute_food_item_seo_v2 resets canonicality; without the cluster gate a
    // refresh re-promotes an already-suppressed duplicate into the sitemap.
    await rpcOrThrow("apply_food_item_cluster_seo_gate", { p_food_item_id: foodId });
    jlog("seo_cluster_gate_ok", { msgId: meta?.msgId, fdcId, food_id: foodId });
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
  };
}
