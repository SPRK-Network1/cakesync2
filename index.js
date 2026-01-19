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
const START_DATE = "2026-01-10";
const END_DATE = new Date().toISOString().split("T")[0];
const ROW_LIMIT = 500;

const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;
const SNAPSHOT_DATE = END_DATE;

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
  const parser = new XMLParser({
    ignoreAttributes: false,
    trimValues: true,
    parseTagValue: true,
  });

  let startAt = 1;
  const seenSubIds = new Set();
  const rowsToUpsert = [];

  while (true) {
    const url =
      "https://mymonetise.co.uk/affiliates/api/Reports/SubAffiliateSummary" +
      `?api_key=${SYSTEM2_API_KEY}` +
      `&affiliate_id=${AFFILIATE_ID}` +
      `&start_date=${encodeURIComponent(START_DATE + " 00:00:00")}` +
      `&end_date=${encodeURIComponent(END_DATE + " 23:59:59")}` +
      `&start_at_row=${startAt}` +
      `&row_limit=${ROW_LIMIT}`;

    console.log("REQUEST URL:", url);

    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(await res.text());
    }

    const xml = await res.text();
    const parsed = parser.parse(xml);

    // ───────── DEBUG STRUCTURE ─────────
    console.log("TOP LEVEL KEYS:", Object.keys(parsed));

    const root = parsed.sub_affiliate_summary_response;
    console.log("ROOT KEYS:", root ? Object.keys(root) : null);

    console.log("SUCCESS:", root?.success);
    console.log("ROW COUNT:", root?.row_count);

    console.log("DATA NODE RAW:", root?.data);

    let rows = root?.data?.subaffiliate;

    if (!rows) {
      console.log("❌ No subaffiliate node present");
      break;
    }

    if (!Array.isArray(rows)) rows = [rows];
    if (!rows.length) {
      console.log("❌ subaffiliate array is empty");
      break;
    }

    for (const r of rows) {
      const subId = normalizeText(r.sub_id);

      if (subId && !seenSubIds.has(subId) && seenSubIds.size < 25) {
        console.log("SEEN sub_id:", subId);
        seenSubIds.add(subId);
      }

      if (!SPARK_ID_REGEX.test(subId)) continue;

      rowsToUpsert.push({
        cake_affiliate_id: subId,
        date: SNAPSHOT_DATE,
        system2_revenue: Number(r.revenue ?? 0),
        clicks: Number(r.clicks ?? 0),
        conversions: Number(r.conversions ?? 0),
      });
    }

    if (rows.length < ROW_LIMIT) break;
    startAt += ROW_LIMIT;
  }

  if (!rowsToUpsert.length) {
    console.log("❌ No valid SPK rows found");
    return;
  }

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
