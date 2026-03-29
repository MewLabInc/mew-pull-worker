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
