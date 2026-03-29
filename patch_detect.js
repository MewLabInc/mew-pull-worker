const fs = require("fs");

const path = "./index.js";
let text = fs.readFileSync(path, "utf8");

const start = text.indexOf("function detectMenuUrlFromHtml(html, baseUrl) {");
if (start === -1) {
  console.error("START NOT FOUND");
  process.exit(1);
}

const end = text.indexOf("\n}\n\nasync function fetchMenuSourcePage(sourceUrl) {", start);
if (end === -1) {
  console.error("END NOT FOUND");
  process.exit(1);
}

const newFunc = `
function detectMenuUrlFromHtml(html, baseUrl) {
  const raw = String(html || "");

  const blockedExtPattern =
    /\\.(css|js|json|png|jpg|jpeg|gif|svg|webp|ico|woff|woff2|ttf|eot|map|xml)(\\?|#|$)/i;

  const menuWordPattern =
    /\\b(menu|food|drink|dinner|lunch|breakfast|brunch|supper|wine|cocktail|beverage|dessert|order)\\b/i;

  const hrefPattern = /<a\\b[^>]*href=["']([^"']+)["'][^>]*>([\\s\\S]*?)<\\/a>/gi;

  let base;
  try {
    base = new URL(baseUrl);
  } catch {
    return null;
  }

  const candidates = [];

  let match;
  while ((match = hrefPattern.exec(raw)) !== null) {
    const href = String(match[1] || "").trim();
    const anchorHtml = String(match[2] || "");
    const anchorText = anchorHtml.replace(/<[^>]+>/g, " ").replace(/\\s+/g, " ").trim();

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

    const urlStr = resolved.toString();
    if (blockedExtPattern.test(urlStr)) continue;

    const sameHost = resolved.hostname === base.hostname;

    const pathLower = resolved.pathname.toLowerCase();
    const fullLower = (urlStr + " " + anchorText).toLowerCase();

    let score = 0;

    if (sameHost) score += 5;
    if (menuWordPattern.test(fullLower)) score += 5;

    if (/(^|\\/)(menu|menus)(\\/|$)/i.test(pathLower)) score += 8;
    if (/(^|\\/)(food|drink|drinks|dining|eat|order)(\\/|$)/i.test(pathLower)) score += 4;
    if (/pdf(\\?|#|$)/i.test(urlStr)) score += 3;

    if (score <= 0) continue;

    candidates.push({ url: urlStr, score });
  }

  if (!candidates.length) return null;

  candidates.sort((a, b) => b.score - a.score || a.url.localeCompare(b.url));
  return candidates[0].url;
}
`;

const updated = text.slice(0, start) + newFunc + text.slice(end);
fs.writeFileSync(path, updated);

console.log("[OK] patch applied");
