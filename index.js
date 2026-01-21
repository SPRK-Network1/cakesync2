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
const START_DATE = new Date("2026-01-10");
const WINDOW_DAYS = 28;
const ROW_LIMIT = 500;

const SPARK_ID_REGEX = /^SPK-[A-Z0-9]{4}-[A-Z0-9]{4}$/i;

// yesterday (stable totals)
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

  let cursor = new Date(START_DATE);

  // ───── Fetch & aggregate System2 ─────
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

      const res = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; SPRKNetworkBot/1.0)",
          Accept: "application/xml,text/xml;q=0.9,*/*;q=0.8",
        },
      });

      if (!res.ok) throw new Error(await res.text());

      const xml = await res.text();
      const parsed = parser.parse(xml);

      let rows =
        parsed?.sub_affiliate_summary_response?.data?.subaffiliate;

      if (!rows) break;
      if (!Array.isArray(rows)) rows = [rows];

      for (const r of rows) {
        const spkCode = normalizeText(r.sub_id);
        if (!SPARK_ID_REGEX.test(spkCode)) continue;

        const revenue = Number(r.revenue ?? 0);
        const clicks = Number(r.clicks ?? 0);
        const conversions = Number(r.conversions ?? 0);

        const prev = totals.get(spkCode) || {
          revenue: 0,
          clicks: 0,
          conversions: 0,
        };

        totals.set(spkCode, {
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
    console.log("❌ No valid SPKs found");
    return;
  }

  // ───── Merge into canonical sparks table ─────
  for (const [spkCode, v] of totals.entries()) {
    // 1️⃣ Fetch existing spark
    const { data: existing, error: fetchError } = await supabase
      .from("sparks")
      .select(
        "id, system2_revenue, system2_clicks, system2_conversions"
      )
      .eq("spk_code", spkCode)
      .maybeSingle();

    if (fetchError) throw fetchError;

    // 2️⃣ Update if exists
    if (existing) {
      const { error: updateError } = await supabase
        .from("sparks")
        .update({
          system2_revenue:
            Number(existing.system2_revenue || 0) + v.revenue,
          system2_clicks:
            Number(existing.system2_clicks || 0) + v.clicks,
          system2_conversions:
            Number(existing.system2_conversions || 0) + v.conversions,
        })
        .eq("id", existing.id);

      if (updateError) throw updateError;
    } 
    // 3️⃣ Insert if missing
    else {
      const { error: insertError } = await supabase
        .from("sparks")
        .insert({
          spk_code: spkCode,
          system2_revenue: v.revenue,
          system2_clicks: v.clicks,
          system2_conversions: v.conversions,
        });

      // Ignore duplicate race conditions
      if (insertError && insertError.code !== "23505") {
        throw insertError;
      }
    }
  }

  console.log(`✔ Updated ${totals.size} SPKs with System2 data`);
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
