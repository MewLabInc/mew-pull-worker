from pathlib import Path

p = Path("index.js")
s = p.read_text()

old = '''    const rows = await rpcOrThrow("process_places_seed_batch_v1", {
      p_limit: batchSize,
    });
'''

new = '''    const rows = await rpcOrThrow("process_places_seed_batch_v1", {
      p_batch_size: batchSize,
    });
'''

if old not in s:
    raise SystemExit("Target RPC block not found")

s = s.replace(old, new, 1)
p.write_text(s)
print("[OK] replaced p_limit with p_batch_size")
