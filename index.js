import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import { XMLParser } from "fast-xml-parser";
import { DateTime } from "luxon";

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
const TIME_ZONE = "America/New_York";
const START_DATE = DateTime.fromISO("2026-01-10", { zone: TIME_ZONE }).startOf("day"); // first known System2 data
const WINDOW_DAYS = 28;
const ROW_LIMIT = 500;

const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

// snapshot date = current EST day (API is GMT)
const nowEst = DateTime.now().setZone(TIME_ZONE);
const END_DATE = nowEst.endOf("day");
const SNAPSHOT_DATE = nowEst.toISODate();

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
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

  const parser = new XMLParser({
    ignoreAttributes: false,
    trimValues: true,
    parseTagValue: true,
  });

  let cursor = START_DATE;

  while (cursor <= END_DATE) {
    const windowStart = cursor.startOf("day");
    const windowEnd = DateTime.min(cursor.plus({ days: WINDOW_DAYS - 1 }).endOf("day"), END_DATE);

    const windowStartUtc = windowStart.toUTC();
    const windowEndUtc = windowEnd.toUTC();

    console.log(`Fetching System2 (EST days, GMT request): ${windowStart.toISODate()} → ${windowEnd.toISODate()}`);

    let startAt = 1;

    while (true) {
      const url =
        "https://mymonetise.co.uk/affiliates/api/Reports/SubAffiliateSummary" +
        `?api_key=${SYSTEM2_API_KEY}` +
        `&affiliate_id=${AFFILIATE_ID}` +
        `&start_date=${encodeURIComponent(windowStartUtc.toFormat("yyyy-LL-dd HH:mm:ss"))}` +
        `&end_date=${encodeURIComponent(windowEndUtc.toFormat("yyyy-LL-dd HH:mm:ss"))}` +
        `&start_at_row=${startAt}` +
        `&row_limit=${ROW_LIMIT}` +
        `&format=xml`;

      const res = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; SPRKNetworkBot/1.0)",
          Accept: "application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
      });

      if (!res.ok) throw new Error(await res.text());

      const xml = await res.text();
      const parsed = parser.parse(xml);

      let rows = parsed?.sub_affiliate_summary_response?.data?.subaffiliate;

      if (!rows) break;
      if (!Array.isArray(rows)) rows = [rows];
      if (!rows.length) break;

      for (const r of rows) {
        const subId = normalizeText(r.sub_id);
        if (!SPARK_ID_REGEX.test(subId)) continue;

        const revenue = Number(r.revenue ?? 0);
        const clicks = Number(r.clicks ?? 0);
        const conversions = Number(r.conversions ?? 0);

        const prev = totals.get(subId) || { revenue: 0, clicks: 0, conversions: 0 };

        totals.set(subId, {
          revenue: prev.revenue + revenue,
          clicks: prev.clicks + clicks,
          conversions: prev.conversions + conversions,
        });
      }

      if (rows.length < ROW_LIMIT) break;
      startAt += ROW_LIMIT;
    }

    cursor = windowEnd.plus({ days: 1 }).startOf("day");
  }

  if (!totals.size) {
    console.log("❌ No valid SPK rows found");
    return;
  }

  // Build ONE row per SPK for the snapshot date.
  // Upsert will update existing row instead of creating duplicates.
  const rowsToUpsert = Array.from(totals.entries()).map(([sparkId, v]) => ({
    cake_affiliate_id: sparkId,
    date: SNAPSHOT_DATE,
    system2_revenue: v.revenue,
    clicks: v.clicks,
    conversions: v.conversions,
  }));

  // Check which Spark codes already exist; only create new rows for the missing ones.
  const sparkIds = Array.from(totals.keys());
  const { data: existingRows, error: fetchExistingError } = await supabase
    .from("cake_earnings_daily")
    .select("cake_affiliate_id")
    .in("cake_affiliate_id", sparkIds);

  if (fetchExistingError) throw fetchExistingError;

  const existingSet = new Set((existingRows || []).map((r) => r.cake_affiliate_id));
  const existingRowsToUpsert = rowsToUpsert.filter((r) => existingSet.has(r.cake_affiliate_id));
  const newRowsToInsert = rowsToUpsert.filter((r) => !existingSet.has(r.cake_affiliate_id));

  if (existingRowsToUpsert.length) {
    const { error } = await supabase
      .from("cake_earnings_daily")
      .upsert(existingRowsToUpsert, {
        onConflict: "cake_affiliate_id,date",
      });

    if (error) throw error;
  }

  if (newRowsToInsert.length) {
    const { error } = await supabase
      .from("cake_earnings_daily")
      .upsert(newRowsToInsert, {
        onConflict: "cake_affiliate_id,date",
      });

    if (error) throw error;
  }

  console.log(
    `✔ Upserted ${existingRowsToUpsert.length} existing SPK rows, inserted ${newRowsToInsert.length} new SPK rows into cake_earnings_daily for ${SNAPSHOT_DATE}`
  );
}

// ─────────────────────────────────────────────
// RUN
// ─────────────────────────────────────────────
run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("❌ Sync failed:", err);
    process.exit(1);
  });
