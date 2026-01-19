import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { XMLParser } from "fast-xml-parser";

// ─────────────────────────────────────────────
// ENV
// ─────────────────────────────────────────────
const {
  SYSTEM2_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
} = process.env;

if (!SYSTEM2_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing environment variables");
}

// ─────────────────────────────────────────────
// SUPABASE
// ─────────────────────────────────────────────
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  { auth: { autoRefreshToken: false, persistSession: false } }
);

// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────
const AFFILIATE_ID = "26142";
const START_DATE = new Date("2026-01-10"); // first known System2 data
const WINDOW_DAYS = 28;
const ROW_LIMIT = 500;

const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

// snapshot date = yesterday (stable totals)
const today = new Date();
today.setDate(today.getDate() - 1);
const SNAPSHOT_DATE = today.toISOString().split("T")[0];

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function toISO(d) {
  return d.toISOString().split("T")[0];
}

function addDays(d, days) {
  const x = new Date(d);
  x.setDate(x.getDate() + days);
  return x;
}

function minDate(a, b) {
  return a < b ? a : b;
}

function normalizeText(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "number") return String(v);
  if (typeof v === "object") {
    if (typeof v["#text"] === "string") return v["#text"].trim();
    const values = Object.values(v);
    if (values.length === 1) return normalizeText(values[0]);
  }
  return "";
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function run() {
  const totals = new Map();
  const seenSubIds = new Set(); // debug visibility

  const parser = new XMLParser({
    ignoreAttributes: false,
    trimValues: true,
    parseTagValue: true,
  });

  let cursor = new Date(START_DATE);

  while (cursor <= today) {
    const windowStart = new Date(cursor);
    const windowEnd = minDate(
      addDays(cursor, WINDOW_DAYS - 1),
      today
    );

    console.log(
      `Fetching System2: ${toISO(windowStart)} → ${toISO(windowEnd)}`
    );

    let startAt = 1;

    while (true) {
      const url =
        "https://mymonetise.co.uk/affiliates/api/Reports/SubAffiliateSummary" +
        `?api_key=${SYSTEM2_API_KEY}` +
        `&affiliate_id=${AFFILIATE_ID}` +
        `&start_date=${encodeURIComponent(toISO(windowStart) + " 00:00:00")}` +
        `&end_date=${encodeURIComponent(toISO(windowEnd) + " 23:59:59")}` +
        `&start_at_row=${startAt}` +
        `&row_limit=${ROW_LIMIT}` +
        `&format=xml`;

      const res = await fetch(url);
      if (!res.ok) throw new Error(await res.text());

      const xml = await res.text();
      const parsed = parser.parse(xml);

      const root = parsed?.sub_affiliate_summary_response;
      let rows = root?.data?.subaffiliate;

      if (!rows) break;
      if (!Array.isArray(rows)) rows = [rows];
      if (!rows.length) break;

      for (const r of rows) {
        const subId = normalizeText(r.sub_id);

        // debug: print first 25 unique sub_ids
        if (subId && !seenSubIds.has(subId) && seenSubIds.size < 25) {
          console.log("SEEN sub_id:", subId);
          seenSubIds.add(subId);
        }

        if (!SPARK_ID_REGEX.test(subId)) continue;

        const revenue = Number(r.revenue ?? 0);
        const clicks = Number(r.clicks ?? 0);
        const conversions = Number(r.conversions ?? 0);

        const prev = totals.get(subId) || {
          revenue: 0,
          clicks: 0,
          conversions: 0,
        };

        totals.set(subId, {
          revenue: prev.revenue + revenue,
          clicks: prev.clicks + clicks,
          conversions: prev.conversions + conversions,
        });
      }

      if (rows.length < ROW_LIMIT) break;
      startAt += ROW_LIMIT;
    }

    cursor = addDays(windowEnd, 1);
  }

  if (!totals.size) {
    console.log("❌ No valid SPK rows found");
    return;
  }

  const rowsToUpsert = Array.from(totals.entries()).map(
    ([sparkId, v]) => ({
      cake_affiliate_id: sparkId,
      date: SNAPSHOT_DATE,
      system2_revenue: v.revenue,
      clicks: v.clicks,
      conversions: v.conversions,
    })
  );

  const { error } = await supabase
    .from("cake_earnings_daily")
    .upsert(rowsToUpsert, {
      onConflict: "cake_affiliate_id,date",
    });

  if (error) throw error;

  console.log(`✔ Synced ${rowsToUpsert.length} SPK System2 rows`);
}

// ─────────────────────────────────────────────
// RUN
// ─────────────────────────────────────────────
run()
  .then(() => process.exit(0))
  .catch(err => {
    console.error("❌ Sync failed:", err);
    process.exit(1);
  });
