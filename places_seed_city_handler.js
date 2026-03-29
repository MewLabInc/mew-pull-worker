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
