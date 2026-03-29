from pathlib import Path

p = Path("index.js")
s = p.read_text()

publish_anchor = """      if (features.length === pageSize) {
        const nextOffset = Number(target.last_offset || 0) + pageSize;
        await markPlaceSeedTargetProgress(target.id, nextOffset);

        jlog("places_seed_target_progress", {
          msgId,
          target_id: target.id,
          next_offset: nextOffset,
        });
      } else {
        await markPlaceSeedTargetDone(target.id);

        jlog("places_seed_target_done", {
          msgId,
          target_id: target.id,
          final_offset: target.last_offset,
          feature_count: features.length,
        });
      }
"""

publish_replacement = """      const drainEnv = {
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
      } else {
        await markPlaceSeedTargetDone(target.id);

        jlog("places_seed_target_done", {
          msgId,
          target_id: target.id,
          final_offset: target.last_offset,
          feature_count: features.length,
        });
      }
"""

if publish_anchor not in s:
    raise SystemExit("Could not find places.seed.city completion block")

s = s.replace(publish_anchor, publish_replacement, 1)

handlers_anchor = '''  "places.seed.city": async ({ envelope, meta }) => {
'''
if handlers_anchor not in s:
    raise SystemExit('Could not find places.seed.city handler')

insert_after = '''  "places.seed.city": async ({ envelope, meta }) => {
'''
process_handler = '''
  "places.seed.process.requested": async ({ envelope, meta }) => {
    const msgId = meta?.msgId;
    const batchSize = Number(envelope?.payload?.batch_size || 10);

    const rows = await rpcOrThrow("process_places_seed_batch_v1", {
      p_limit: batchSize,
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

'''

marker = '  "places.seed.city": async ({ envelope, meta }) => {'
idx = s.find(marker)
if idx == -1:
    raise SystemExit("Could not find places.seed.city marker")

# insert new handler immediately before places.seed.city
s = s[:idx] + process_handler + s[idx:]

p.write_text(s)
print("[OK] patched index.js with places queue drain handler")
