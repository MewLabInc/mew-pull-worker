// Recording fake of the supabase-js query builder surface used by lib/usda.js.
// Synthetic only. Every call is appended to `calls` in order.
// results: { "table.op": {data,count,error} | fn(call) , "rpc.name": ... }
export function createFakeSupabase(results = {}) {
  const calls = [];
  const resolveResult = (key, call) => {
    const r = results[key];
    const v = typeof r === "function" ? r(call) : r;
    return { data: null, count: null, error: null, ...(v || {}) };
  };
  function from(table) {
    const call = { kind: "from", table, op: "select", args: null, opts: null, filters: [] };
    calls.push(call);
    const b = {
      select(cols, opts) { call.op = call.op === "select" ? "select" : call.op; call.cols = cols; call.opts = opts ?? null; return b; },
      upsert(rows, opts) { call.op = "upsert"; call.args = rows; call.opts = opts ?? null; return b; },
      insert(rows, opts) { call.op = "insert"; call.args = rows; call.opts = opts ?? null; return b; },
      update(patch) { call.op = "update"; call.args = patch; return b; },
      eq(col, val) { call.filters.push([col, val]); return b; },
      maybeSingle() { call.single = true; return b; },
      then(res, rej) {
        return Promise.resolve(resolveResult(`${table}.${call.op}`, call)).then(res, rej);
      },
    };
    return b;
  }
  async function rpc(name, args) {
    const call = { kind: "rpc", name, args };
    calls.push(call);
    return resolveResult(`rpc.${name}`, call);
  }
  return { from, rpc, calls };
}

export function jsonResponse(body, { status = 200, statusText = "OK" } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText,
    json: async () => body,
    text: async () => JSON.stringify(body),
  };
}
