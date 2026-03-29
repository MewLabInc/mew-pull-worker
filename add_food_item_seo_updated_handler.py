from pathlib import Path

p = Path("index.js")
s = p.read_text()

anchor = '''  "food.usda.ingest": async ({ envelope, meta }) => {
'''
insert = '''  "FOOD_ITEM_SEO_UPDATED": async ({ envelope, meta }) => {
    jlog("food_item_seo_updated_skip", {
      msgId: meta?.msgId ?? null,
      entity_id: envelope?.entity_id ?? null,
      reason: "no_followup_required",
    });
  },

'''

if '"FOOD_ITEM_SEO_UPDATED": async ({ envelope, meta }) => {' in s:
    raise SystemExit("FOOD_ITEM_SEO_UPDATED handler already exists")

if anchor not in s:
    raise SystemExit("Anchor not found: food.usda.ingest handler")

s = s.replace(anchor, insert + anchor, 1)
p.write_text(s)
print("[OK] inserted FOOD_ITEM_SEO_UPDATED handler")
