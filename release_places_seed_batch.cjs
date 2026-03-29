const { PubSub } = require("@google-cloud/pubsub");

const TOPIC_NAME = process.env.TOPIC_NAME || "food-ingest";
const pubsub = new PubSub();

async function main() {
  const count = Number(process.argv[2] || 1);
  const countryCode = process.argv[3] || "LU";

  if (!Number.isInteger(count) || count <= 0) {
    throw new Error("Usage: node release_places_seed_batch.js <positive_integer> [country_code]");
  }

  const topic = pubsub.topic(TOPIC_NAME);

  for (let i = 1; i <= count; i++) {
    const stamp = new Date().toISOString().replace(/[-:.TZ]/g, "");
    const dedupeKey = `places_seed_city_batch_release_${countryCode}_${stamp}_${i}`;

    const envelope = {
      event_type: "places.seed.city",
      source: "manual",
      dedupe_key: dedupeKey,
      trace_id: `release-batch-${countryCode}-${count}-${i}`,
      payload: { country_code: countryCode },
    };

    const messageId = await topic.publishMessage({
      data: Buffer.from(JSON.stringify(envelope)),
    });

    console.log(JSON.stringify({ i, countryCode, messageId, dedupeKey }));
  }
}

main().catch((err) => {
  console.error(err.message || String(err));
  process.exit(1);
});
