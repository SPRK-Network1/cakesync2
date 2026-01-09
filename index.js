import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ─────────────────────────────────────────────
// ENV VARS (SET THESE IN RENDER)
// ─────────────────────────────────────────────
const {
  CAKE_API_KEY,
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
} = process.env;

if (!CAKE_API_KEY || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Missing environment variables");
}

// ─────────────────────────────────────────────
// SUPABASE CLIENT
// ─────────────────────────────────────────────
const supabase = createClient(
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY
);

// ─────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────
const AFFILIATE_ID = "208330";
const SNAPSHOT_DATE = "2026-01-04"; // lifetime row
const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

// ─────────────────────────────────────────────
// 🧠 CAKE-SAFE ROLLING 29-DAY WINDOW
// - end = yesterday (completed day)
// - start = end − 29 days
// ─────────────────────────────────────────────
const now = new Date();

// yesterday (CAKE considers completed)
const end = new Date(now.getTime() - 24 * 60 * 60 * 1000);

// 29-day window (CAKE max)
const start = new Date(end.getTime() - 29 * 24 * 60 * 60 * 1000);

const START_DATE = start.toISOString().split("T")[0];
const END_DATE = end.toISOString().split("T")[0];

// ─────────────────────────────────────────────
// CAKE REQUEST (WINDOW-SAFE)
// ─────────────────────────────────────────────
const url =
  "https://login.affluentco.com/affiliates/api/Reports/SubAffiliateSummary" +
  `?api_key=${CAKE_API_KEY}` +
  `&affiliate_id=${AFFILIATE_ID}` +
  `&start_date=${START_DATE}` +
  `&end_date=${END_DATE}` +
  `&format=json`;

console.log("Fetching CAKE:", url);

const res = await fetch(url, {
  headers: {
    "Accept": "application/json",
    "User-Agent": "render-cron",
  },
});

if (!res.ok) {
  const text = await res.text();
  throw new Error(`CAKE HTTP ${res.status}: ${text}`);
}

const json = await res.json();

if (!Array.isArray(json.data)) {
  throw new Error("Unexpected CAKE response shape");
}

// ─────────────────────────────────────────────
// TRANSFORM → LIFETIME ROWS
// (Overwrite same snapshot each run)
// ─────────────────────────────────────────────
const rows = json.data
  .filter(
    r =>
      r.sub_id &&
      SPARK_ID_REGEX.test(String(r.sub_id))
  )
  .map(r => ({
    cake_affiliate_id: String(r.sub_id),
    date: SNAPSHOT_DATE,
    clicks: Number(r.clicks ?? 0),
    conversions: Number(r.conversions ?? 0),
    revenue: Number(r.revenue ?? 0),
    payout: Number(r.events ?? 0),
  }));

if (rows.length === 0) {
  console.log("No SPK rows found. Done.");
  process.exit(0);
}

// ─────────────────────────────────────────────
// UPSERT (user_id PRESERVED)
// ─────────────────────────────────────────────
const { error } = await supabase
  .from("cake_earnings_daily")
  .upsert(rows, {
    onConflict: "cake_affiliate_id,date",
  });

if (error) {
  throw error;
}

console.log(
  `✔ Synced ${rows.length} SPK lifetime rows ` +
  `(window ${START_DATE} → ${END_DATE})`
);
