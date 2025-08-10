// server.js
// Reads Google Sheets and joins School Profile + full School Performance on "School ID"

const express = require("express");
const cors = require("cors");
const NodeCache = require("node-cache");
const { google } = require("googleapis");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// ---------- Config (env) ----------
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const API_KEY = process.env.GOOGLE_API_KEY;
const SERVICE_ACCOUNT_KEY = process.env.GOOGLE_SERVICE_ACCOUNT_KEY
  ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY)
  : null;

// Required sheet ranges
const PROFILE_RANGE = "School Profile!A:AI";         // row 1 = headers, row 2+ = data
const PERF_HEADERS_RANGE = "School Performance!1:3"; // rows 1-3 = headers
const PERF_DATA_RANGE = "School Performance!4:100000"; // row 4+ = data

// Optional “key metrics” subrange for charts (AB:AU) — kept as a convenience
const PERF_KEY_RANGE_A1 = { start: "AB", end: "AU" };

// Cache + stats
const cache = new NodeCache({ stdTTL: 60 });
const stats = {
  totalRequests: 0,
  cacheHits: 0,
  cacheMisses: 0,
  apiCalls: 0,
  errors: 0,
  lastApiCall: null,
};

// ---------- Helpers ----------
function increment(name) {
  if (stats[name] !== undefined) stats[name]++;
}

function a1ToIndex(colLabel) {
  let n = 0;
  for (const ch of colLabel.toUpperCase()) n = n * 26 + (ch.charCodeAt(0) - 64);
  return n - 1; // 0-based
}
const AB_IDX = a1ToIndex(PERF_KEY_RANGE_A1.start);
const AU_IDX = a1ToIndex(PERF_KEY_RANGE_A1.end);

function isSchoolIdHeader(h) {
  const s = String(h || "").trim().toLowerCase();
  // works for "School ID", "schoolid", or any header containing both words
  return s === "school id" || s === "schoolid" || (s.includes("school") && s.includes("id"));
}

function findSchoolIdIndex(headers) {
  for (let i = 0; i < headers.length; i++) if (isSchoolIdHeader(headers[i])) return i;
  return -1;
}

function rowsToObjects(rows, headerRow = []) {
  return rows.map((r) => {
    const obj = {};
    for (let i = 0; i < headerRow.length; i++) {
      const key = headerRow[i] || `col_${i}`;
      obj[key] = r[i] ?? "";
    }
    return obj;
  });
}

function buildPerfFlatHeaders(h1, h2, h3) {
  const width = Math.max(h1.length, h2.length, h3.length);
  const headers = [];
  for (let i = 0; i < width; i++) {
    const cat = h1[i] ?? "";
    const field = h2[i] ?? "";
    const year = h3[i] ?? "";
    const label = `${cat} | ${field} | ${year}`.trim();
    headers.push(label.length ? label : `col_${i}`);
  }
  return headers;
}

function filterToKeyCols(rows, headers) {
  const schoolIdx = findSchoolIdIndex(headers);
  const idxs = [];
  if (schoolIdx >= 0) idxs.push(schoolIdx);
  for (let i = AB_IDX; i <= AU_IDX && i < headers.length; i++) idxs.push(i);

  const outHeaders = idxs.map((i) => headers[i]);
  const outRows = rows.map((r) => idxs.map((i) => r[i] ?? ""));
  return { headers: outHeaders, rows: outRows };
}

function toKeyedBySchoolId(objs, headerRow) {
  const schoolIdIdx = findSchoolIdIndex(headerRow);
  const map = new Map();
  if (schoolIdIdx < 0) return map;
  const keyName = headerRow[schoolIdIdx];
  for (const r of objs) {
    const id = r[keyName];
    if (id !== undefined && id !== "") map.set(String(id), r);
  }
  return map;
}

// Google Sheets client
async function getSheetsClient() {
  if (!SPREADSHEET_ID) throw new Error("SPREADSHEET_ID env var is required");

  if (SERVICE_ACCOUNT_KEY?.client_email && SERVICE_ACCOUNT_KEY?.private_key) {
    const auth = new google.auth.JWT({
      email: SERVICE_ACCOUNT_KEY.client_email,
      key: SERVICE_ACCOUNT_KEY.private_key.replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    });
    await auth.authorize();
    return google.sheets({ version: "v4", auth });
  }

  if (!API_KEY) {
    throw new Error("Either GOOGLE_SERVICE_ACCOUNT_KEY or GOOGLE_API_KEY must be set");
  }
  return google.sheets({ version: "v4", auth: API_KEY });
}

async function getSheetValues(sheets, range) {
  increment("apiCalls");
  stats.lastApiCall = new Date().toISOString();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range,
    majorDimension: "ROWS",
  });
  return res.data.values || [];
}

// ---------- Data fetchers (cached) ----------
async function getProfile() {
  const cacheKey = "profile_full";
  stats.totalRequests++;
  const cached = cache.get(cacheKey);
  if (cached) { stats.cacheHits++; return cached; }
  stats.cacheMisses++;

  const sheets = await getSheetsClient();
  const values = await getSheetValues(sheets, PROFILE_RANGE);
  if (!values.length) return { headers: [], rows: [], data: [] };

  const header = values[0];
  const dataRows = values.slice(1);
  const data = rowsToObjects(dataRows, header);

  const result = { headers: header, rows: dataRows, data };
  cache.set(cacheKey, result);
  return result;
}

async function getPerformanceFull() {
  const cacheKey = "performance_full";
  stats.totalRequests++;
  const cached = cache.get(cacheKey);
  if (cached) { stats.cacheHits++; return cached; }
  stats.cacheMisses++;

  const sheets = await getSheetsClient();
  const headerRows = await getSheetValues(sheets, PERF_HEADERS_RANGE); // 1–3
  const dataRows = await getSheetValues(sheets, PERF_DATA_RANGE);      // 4+

  const h1 = headerRows[0] || [];
  const h2 = headerRows[1] || [];
  const h3 = headerRows[2] || [];
  const flatHeaders = buildPerfFlatHeaders(h1, h2, h3);

  const data = rowsToObjects(dataRows, flatHeaders);
  const result = { headers: flatHeaders, rows: dataRows, data };
  cache.set(cacheKey, result);
  return result;
}

async function getPerformanceKey() {
  const cacheKey = "performance_key";
  stats.totalRequests++;
  const cached = cache.get(cacheKey);
  if (cached) { stats.cacheHits++; return cached; }
  stats.cacheMisses++;

  const { headers: flatHeaders, rows: fullRows } = await getPerformanceFull();
  // Convert back to arrays for slicing (we already have arrays in .rows)
  const { headers, rows } = filterToKeyCols(fullRows, flatHeaders);
  const data = rowsToObjects(rows, headers);

  const result = { headers, rows, data };
  cache.set(cacheKey, result);
  return result;
}

async function getJoinedFull() {
  const cacheKey = "joined_full";
  stats.totalRequests++;
  const cached = cache.get(cacheKey);
  if (cached) { stats.cacheHits++; return cached; }
  stats.cacheMisses++;

  const [profile, perf] = await Promise.all([getProfile(), getPerformanceFull()]);

  const pMap = toKeyedBySchoolId(profile.data, profile.headers);
  const qMap = toKeyedBySchoolId(perf.data, perf.headers);

  const schoolIdHeaderProfile = profile.headers.find(isSchoolIdHeader);
  const schoolIdHeaderPerf = perf.headers.find(isSchoolIdHeader);

  const allIds = new Set([...pMap.keys(), ...qMap.keys()]);
  const merged = [];

  for (const id of allIds) {
    const p = pMap.get(id) || {};
    const q = qMap.get(id) || {};
    const row = { ...p, ...q };

    // ensure a consistent "School ID" field
    if (!row["School ID"]) {
      if (schoolIdHeaderProfile && p[schoolIdHeaderProfile] != null) row["School ID"] = p[schoolIdHeaderProfile];
      else if (schoolIdHeaderPerf && q[schoolIdHeaderPerf] != null) row["School ID"] = q[schoolIdHeaderPerf];
      else row["School ID"] = id;
    }

    merged.push(row);
  }

  cache.set(cacheKey, merged);
  return merged;
}

// ---------- Routes ----------
app.get("/health", (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});

app.get("/stats", (_req, res) => {
  res.json(stats);
});

app.get("/api/profile", async (_req, res) => {
  try {
    const data = await getProfile();
    res.json(data);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// FULL performance (all columns)
app.get("/api/performance", async (_req, res) => {
  try {
    const data = await getPerformanceFull();
    res.json(data);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Optional: AB:AU + School ID subset for charts
app.get("/api/performance/key", async (_req, res) => {
  try {
    const data = await getPerformanceKey();
    res.json(data);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Joined FULL dataset (profile ⨝ performance)
app.get("/api/schools", async (_req, res) => {
  try {
    const data = await getJoinedFull();
    res.json({ count: data.length, data });
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// ---------- Start ----------
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});
