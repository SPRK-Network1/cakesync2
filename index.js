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
const START_DATE = new Date("2025-12-01");
const WINDOW_DAYS = 28;
const SNAPSHOT_DATE = "2026-01-04";
const ROW_LIMIT = 500;

const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

// yesterday only (completed data)
const today = new Date();
today.setDate(today.getDate() - 1);

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

// SAFELY extract sub_id text from XML parser output
function extractSubId(raw) {
  if (!raw) return "";
  if (typeof raw === "string") return raw.trim();
  if (typeof raw === "object" && raw["#text"]) {
    return String(raw["#text"]).trim();
  }
  return "";
}

// ─────────────────────────────────────────────
// MAIN
// ─────────────────────────────────────────────
async function run() {
  const totals = new Map();
  const parser = new XMLParser();

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
        `&row_limit=${ROW_LIMIT}`;

      const res = await fetch(url);
      if (!res.ok) {
        throw new Error(await res.text());
      }

      const xml = await res.text();
      const parsed = parser.parse(xml);

      const subs =
        parsed?.sub_affiliate_summary_response?.data?.subaffiliate ?? [];

      const rows = Array.isArray(subs) ? subs : [subs];
      if (!rows.length) break;

      for (const r of rows) {
        const subId = extractSubId(r.sub_id);

        if (!subId || !SPARK_ID_REGEX.test(subId)) continue;

        const prev = totals.get(subId) || {
          clicks: 0,
          conversions: 0,
          revenue: 0,
        };

        totals.set(subId, {
          clicks: prev.clicks + Number(r.clicks ?? 0),
          conversions: prev.conversions + Number(r.conversions ?? 0),
          revenue: prev.revenue + Number(r.revenue ?? 0),
        });
      }

      // pagination end condition
      if (rows.length < ROW_LIMIT) break;
      startAt += ROW_LIMIT;
    }

    cursor = addDays(windowEnd, 1);
  }

  if (!totals.size) {
    console.log("No valid SPK rows found");
    return;
  }

  const rows = Array.from(totals.entries()).map(
    ([sparkId, v]) => ({
      cake_affiliate_id: sparkId,
      date: SNAPSHOT_DATE,
      system2_revenue: v.revenue,
    })
  );

  const { error } = await supabase
    .from("cake_earnings_daily")
    .upsert(rows, {
      onConflict: "cake_affiliate_id,date",
    });

  if (error) throw error;

  console.log(`✔ Synced ${rows.length} SPK System2 rows`);
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
