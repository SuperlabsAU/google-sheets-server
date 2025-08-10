// server.js
// School Profile + School Performance (3-row header) reader with inner-join on "School ID"

const express = require("express");
const cors = require("cors");
const NodeCache = require("node-cache");
const { google } = require("googleapis");

// ---- Server setup
const app = express();
const PORT = process.env.PORT || 3000;
app.use(cors());
app.use(express.json());

// ---- Env
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const API_KEY = process.env.GOOGLE_API_KEY || "";
const SERVICE_ACCOUNT_KEY = process.env.GOOGLE_SERVICE_ACCOUNT_KEY
  ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY)
  : null;

// ---- Sheet ranges
const PROFILE_RANGE = "School Profile!A:AI";        // row 1 headers, row 2+ data
const PERF_HEADERS_RANGE = "School Performance!1:3"; // 3-row header
const PERF_DATA_RANGE = "School Performance!4:100000"; // data starts row 4

// Optional: convenience key-range (not required for inner-join/full)
const PERF_KEY_RANGE_A1 = { start: "AB", end: "AU" };

// ---- Cache + stats
const cache = new NodeCache({ stdTTL: 60 });
const stats = {
  totalRequests: 0,
  cacheHits: 0,
  cacheMisses: 0,
  apiCalls: 0,
  errors: 0,
  lastApiCall: null,
};

// ---- Helpers
function increment(name) {
  if (stats[name] !== undefined) stats[name]++;
}
function a1ToIndex(colLabel) {
  let n = 0;
  for (const ch of colLabel.toUpperCase()) n = n * 26 + (ch.charCodeAt(0) - 64);
  return n - 1;
}
const AB_IDX = a1ToIndex(PERF_KEY_RANGE_A1.start);
const AU_IDX = a1ToIndex(PERF_KEY_RANGE_A1.end);

function isSchoolIdHeader(h) {
  const s = String(h || "").trim().toLowerCase();
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
function toKeyedBySchoolId(objs, headerRow) {
  const idx = findSchoolIdIndex(headerRow);
  const map = new Map();
  if (idx < 0) return map;
  const keyName = headerRow[idx];
  for (const r of objs) {
    const id = r[keyName];
    if (id !== undefined && id !== "") map.set(String(id), r);
  }
  return map;
}

// ---- Google Sheets client
let API_MODE = "service"; // "service" | "apikey"
async function getSheetsClient() {
  if (!SPREADSHEET_ID) throw new Error("SPREADSHEET_ID env var is required.");

  // Prefer Service Account (server-to-server)
  if (SERVICE_ACCOUNT_KEY?.client_email && SERVICE_ACCOUNT_KEY?.private_key) {
    const auth = new google.auth.JWT({
      email: SERVICE_ACCOUNT_KEY.client_email,
      key: SERVICE_ACCOUNT_KEY.private_key.replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    });
    await auth.authorize();
    API_MODE = "service";
    return google.sheets({ version: "v4", auth });
  }

  // Fallback: API key (works for public/readable sheets)
  if (!API_KEY) throw new Error("Either GOOGLE_SERVICE_ACCOUNT_KEY or GOOGLE_API_KEY must be set.");
  API_MODE = "apikey";
  return google.sheets({ version: "v4" /* auth not required for API key mode */ });
}

// Single-range read using spreadsheets.values.get (ValueRange). Docs: values.get.  [oai_citation:2‡Google for Developers](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values/get?utm_source=chatgpt.com)
async function getSheetValues(sheets, range) {
  increment("apiCalls");
  stats.lastApiCall = new Date().toISOString();
  const params = {
    spreadsheetId: SPREADSHEET_ID,
    range,
    majorDimension: "ROWS",
  };
  // When using API key mode, attach the key (supported by googleapis client).  [oai_citation:3‡Google Cloud](https://googleapis.dev/nodejs/googleapis/latest/sheets/index.html?utm_source=chatgpt.com) [oai_citation:4‡GitHub](https://github.com/googleapis/google-api-nodejs-client/blob/main/src/apis/sheets/v4.ts?utm_source=chatgpt.com)
  if (API_MODE === "apikey") params.key = API_KEY;

  const res = await sheets.spreadsheets.values.get(params);
  return res.data.values || [];
}

// ---- Fetchers (cached)
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
  const headerRows = await getSheetValues(sheets, PERF_HEADERS_RANGE); // rows 1–3
  const dataRows = await getSheetValues(sheets, PERF_DATA_RANGE);      // row 4+

  const h1 = headerRows[0] || [];
  const h2 = headerRows[1] || [];
  const h3 = headerRows[2] || [];
  const flatHeaders = buildPerfFlatHeaders(h1, h2, h3);

  const data = rowsToObjects(dataRows, flatHeaders);
  const result = { headers: flatHeaders, rows: dataRows, data };
  cache.set(cacheKey, result);
  return result;
}

// Optional: AB:AU convenience subset
function filterToKeyCols(rows, headers) {
  const schoolIdx = findSchoolIdIndex(headers);
  const idxs = [];
  if (schoolIdx >= 0) idxs.push(schoolIdx);
  for (let i = AB_IDX; i <= AU_IDX && i < headers.length; i++) idxs.push(i);
  const outHeaders = idxs.map((i) => headers[i]);
  const outRows = rows.map((r) => idxs.map((i) => r[i] ?? ""));
  return { headers: outHeaders, rows: outRows };
}
async function getPerformanceKey() {
  const cacheKey = "performance_key";
  stats.totalRequests++;
  const cached = cache.get(cacheKey);
  if (cached) { stats.cacheHits++; return cached; }
  stats.cacheMisses++;

  const { headers: flatHeaders, rows: fullRows } = await getPerformanceFull();
  const { headers, rows } = filterToKeyCols(fullRows, flatHeaders);
  const data = rowsToObjects(rows, headers);
  const result = { headers, rows, data };
  cache.set(cacheKey, result);
  return result;
}

// Inner-join: only IDs present in BOTH sheets
async function getJoinedFull() {
  const cacheKey = "joined_full_matched";
  stats.totalRequests++;
  const cached = cache.get(cacheKey);
  if (cached) { stats.cacheHits++; return cached; }
  stats.cacheMisses++;

  const [profile, perf] = await Promise.all([getProfile(), getPerformanceFull()]);
  const pMap = toKeyedBySchoolId(profile.data, profile.headers);
  const qMap = toKeyedBySchoolId(perf.data, perf.headers);
  const schoolIdHeaderProfile = profile.headers.find(isSchoolIdHeader);
  const schoolIdHeaderPerf = perf.headers.find(isSchoolIdHeader);

  const ids = [...pMap.keys()].filter((id) => qMap.has(id));
  const merged = [];
  for (const id of ids) {
    const p = pMap.get(id) || {};
    const q = qMap.get(id) || {};
    const row = { ...p, ...q };
    if (!row["School ID"]) {
      if (schoolIdHeaderProfile && p[schoolIdHeaderProfile] != null) row["School ID"] = p[schoolIdHeaderProfile];
      else if (schoolIdHeaderPerf && q[schoolIdHeaderPerf] != null) row["School ID"] = q[schoolIdHeaderPerf];
      else row["School ID"] = id;
    }
    merged.push(row);
  }

  const result = { count: merged.length, data: merged };
  cache.set(cacheKey, result);
  return result;
}

// ---- Routes
app.get("/health", (_req, res) => {
  res.json({ ok: true, time: new Date().toISOString() });
});
app.get("/stats", (_req, res) => res.json(stats));

// Debug: confirm perf headers exist
app.get("/api/debug/perf-headers", async (_req, res) => {
  try {
    const perf = await getPerformanceFull();
    res.json({ headerCount: perf.headers.length, sample: perf.headers.slice(0, 25) });
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Full profile
app.get("/api/profile", async (_req, res) => {
  try {
    const data = await getProfile();
    res.json(data);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Full performance (all flattened columns)
app.get("/api/performance", async (_req, res) => {
  try {
    const data = await getPerformanceFull();
    res.json(data);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Optional key subset (AB:AU + School ID)
app.get("/api/performance/key", async (_req, res) => {
  try {
    const data = await getPerformanceKey();
    res.json(data);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Joined dataset: default inner-join; pass ?onlyMatched=false for outer-join
app.get("/api/schools", async (req, res) => {
  try {
    const onlyMatched = (req.query.onlyMatched ?? "true").toString().toLowerCase() !== "false";
    if (onlyMatched) {
      const result = await getJoinedFull();
      return res.json(result);
    }

    // Outer-join fallback (if explicitly requested)
    const [profile, perf] = await Promise.all([getProfile(), getPerformanceFull()]);
    const pMap = toKeyedBySchoolId(profile.data, profile.headers);
    const qMap = toKeyedBySchoolId(perf.data, perf.headers);
    const allIds = new Set([...pMap.keys(), ...qMap.keys()]);
    const schoolIdHeaderProfile = profile.headers.find(isSchoolIdHeader);
    const schoolIdHeaderPerf = perf.headers.find(isSchoolIdHeader);
    const out = [];
    for (const id of allIds) {
      const p = pMap.get(id) || {};
      const q = qMap.get(id) || {};
      const row = { ...p, ...q };
      if (!row["School ID"]) {
        if (schoolIdHeaderProfile && p[schoolIdHeaderProfile] != null) row["School ID"] = p[schoolIdHeaderProfile];
        else if (schoolIdHeaderPerf && q[schoolIdHeaderPerf] != null) row["School ID"] = q[schoolIdHeaderPerf];
        else row["School ID"] = id;
      }
      out.push(row);
    }
    res.json({ count: out.length, data: out });
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// ---- Start
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
});

/*
Notes:
- Share the Google Sheet with your Service Account’s client_email to allow access, or use API key for publicly readable sheets.
- This server reads ranges with spreadsheets.values.get (ValueRange). See official docs.   [oai_citation:5‡Google for Developers](https://developers.google.com/workspace/sheets/api/reference/rest/v4/spreadsheets.values/get?utm_source=chatgpt.com)
- googleapis Node client supports JWT service accounts and API keys.   [oai_citation:6‡Google Cloud](https://googleapis.dev/nodejs/googleapis/latest/sheets/index.html?utm_source=chatgpt.com)
*/
