// server.js
// School Profile + School Performance (3-row header) reader with manual snapshot + disk persistence + inner-join on "School ID"

const express = require("express");
const cors = require("cors");
const NodeCache = require("node-cache");
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");

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
const REFRESH_TOKEN = process.env.REFRESH_TOKEN || ""; // optional token to protect /admin/refresh
const SNAPSHOT_FILE = process.env.SNAPSHOT_FILE || path.resolve(process.cwd(), "snapshot.json");

// ---- Sheet ranges
const PROFILE_RANGE = "School Profile!A:AI";          // row 1 headers, row 2+ data
const PERF_HEADERS_RANGE = "School Performance!1:3";  // 3-row header
const PERF_DATA_RANGE = "School Performance!4:100000";// data starts row 4

// Optional: convenience subrange labels (not required for join)
const PERF_KEY_RANGE_A1 = { start: "AB", end: "AU" };

// ---- Stats + cache (disable TTL; snapshot handles persistence)
const cache = new NodeCache({ stdTTL: 0, checkperiod: 0 });
const stats = {
  totalRequests: 0,
  cacheHits: 0,
  cacheMisses: 0,
  apiCalls: 0,
  errors: 0,
  lastApiCall: null,
};

// ---- Manual-refresh snapshot (in-memory)
const SNAPSHOT = {
  profile: null,        // { headers, rows, data }
  performance: null,    // { headers, rows, data }
  joinedMatched: null,  // { count, data }
  lastUpdated: null,    // ISO string
  lastReason: null
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

// ---- Disk persistence helpers
function snapshotToSerializable() {
  return {
    profile: SNAPSHOT.profile,
    performance: SNAPSHOT.performance,
    joinedMatched: SNAPSHOT.joinedMatched,
    lastUpdated: SNAPSHOT.lastUpdated,
    lastReason: SNAPSHOT.lastReason,
    stats: {
      apiCalls: stats.apiCalls,
      totalRequests: stats.totalRequests,
      cacheHits: stats.cacheHits,
      cacheMisses: stats.cacheMisses,
      lastApiCall: stats.lastApiCall,
    },
  };
}
function saveSnapshotToDisk() {
  try {
    const tmp = SNAPSHOT_FILE + ".tmp";
    fs.writeFileSync(tmp, JSON.stringify(snapshotToSerializable()), "utf8");
    fs.renameSync(tmp, SNAPSHOT_FILE); // atomic-ish replace
    return { ok: true, file: SNAPSHOT_FILE };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}
function loadSnapshotFromDisk() {
  try {
    if (!fs.existsSync(SNAPSHOT_FILE)) return { ok: false, error: "file not found" };
    const raw = fs.readFileSync(SNAPSHOT_FILE, "utf8");
    const j = JSON.parse(raw);
    SNAPSHOT.profile = j.profile || null;
    SNAPSHOT.performance = j.performance || null;
    SNAPSHOT.joinedMatched = j.joinedMatched || null;
    SNAPSHOT.lastUpdated = j.lastUpdated || null;
    SNAPSHOT.lastReason = j.lastReason || null;
    if (j.stats) Object.assign(stats, j.stats);
    return { ok: true, file: SNAPSHOT_FILE };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}
function snapshotFileInfo() {
  try {
    if (!fs.existsSync(SNAPSHOT_FILE)) return { exists: false };
    const st = fs.statSync(SNAPSHOT_FILE);
    return { exists: true, file: SNAPSHOT_FILE, bytes: st.size, mtime: st.mtime.toISOString() };
  } catch (e) {
    return { exists: false, error: e?.message || String(e) };
  }
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
  if (!API_KEY) throw new Error("Either GOOGLE_SERVICE_ACCOUNT_KEY or GOOGLE_API_KEY must be set");
  API_MODE = "apikey";
  return google.sheets({ version: "v4" });
}
async function getSheetValues(sheets, range) {
  increment("apiCalls");
  stats.lastApiCall = new Date().toISOString();
  const params = {
    spreadsheetId: SPREADSHEET_ID,
    range,
    majorDimension: "ROWS",
  };
  if (API_MODE === "apikey") params.key = API_KEY; // supported by googleapis client
  const res = await sheets.spreadsheets.values.get(params);
  return res.data.values || [];
}

// ---- Snapshot-aware getters (fetch only when snapshot is empty)
async function getProfile() {
  stats.totalRequests++;
  if (SNAPSHOT.profile) { stats.cacheHits++; return SNAPSHOT.profile; }
  stats.cacheMisses++;

  const sheets = await getSheetsClient();
  const values = await getSheetValues(sheets, PROFILE_RANGE);
  if (!values.length) return { headers: [], rows: [], data: [] };

  const header = values[0];
  const dataRows = values.slice(1);
  const data = rowsToObjects(dataRows, header);
  const result = { headers: header, rows: dataRows, data };
  SNAPSHOT.profile = result;
  return result;
}
async function getPerformanceFull() {
  stats.totalRequests++;
  if (SNAPSHOT.performance) { stats.cacheHits++; return SNAPSHOT.performance; }
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
  SNAPSHOT.performance = result;
  return result;
}
async function getJoinedFull() {
  stats.totalRequests++;
  if (SNAPSHOT.joinedMatched) { stats.cacheHits++; return SNAPSHOT.joinedMatched; }
  stats.cacheMisses++;

  const [profile, perf] = await Promise.all([getProfile(), getPerformanceFull()]);

  const pMap = toKeyedBySchoolId(profile.data, profile.headers);
  const qMap = toKeyedBySchoolId(perf.data, perf.headers);
  const schoolIdHeaderProfile = profile.headers.find(isSchoolIdHeader);
  const schoolIdHeaderPerf = perf.headers.find(isSchoolIdHeader);

  // INNER JOIN: only IDs present in BOTH
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
  SNAPSHOT.joinedMatched = result;
  return result;
}

// ---- Manual refresh: pull everything and store snapshot (also save to disk)
async function refreshAll(reason = "manual") {
  const started = Date.now();
  const sheets = await getSheetsClient();

  // Profile
  const values = await getSheetValues(sheets, PROFILE_RANGE);
  const header = values[0] || [];
  const dataRows = (values.length > 1) ? values.slice(1) : [];
  const profile = { headers: header, rows: dataRows, data: rowsToObjects(dataRows, header) };

  // Performance
  const headerRows = await getSheetValues(sheets, PERF_HEADERS_RANGE);
  const perfRows = await getSheetValues(sheets, PERF_DATA_RANGE);
  const h1 = headerRows[0] || [];
  const h2 = headerRows[1] || [];
  const h3 = headerRows[2] || [];
  const flat = buildPerfFlatHeaders(h1, h2, h3);
  const performance = { headers: flat, rows: perfRows, data: rowsToObjects(perfRows, flat) };

  // Joined (inner)
  const pMap = toKeyedBySchoolId(profile.data, profile.headers);
  const qMap = toKeyedBySchoolId(performance.data, performance.headers);
  const schoolIdHeaderProfile = profile.headers.find(isSchoolIdHeader);
  const schoolIdHeaderPerf = performance.headers.find(isSchoolIdHeader);
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

  // Commit snapshot atomically
  SNAPSHOT.profile = profile;
  SNAPSHOT.performance = performance;
  SNAPSHOT.joinedMatched = { count: merged.length, data: merged };
  SNAPSHOT.lastUpdated = new Date().toISOString();
  SNAPSHOT.lastReason = reason;

  // Save snapshot to disk
  const saveRes = saveSnapshotToDisk();

  const finished = Date.now();
  return {
    ok: true,
    lastUpdated: SNAPSHOT.lastUpdated,
    lastReason: reason,
    durationMs: finished - started,
    counts: {
      profileHeaders: profile.headers.length,
      profileRows: profile.data.length,
      performanceHeaders: performance.headers.length,
      performanceRows: performance.data.length,
      joinedRows: merged.length,
    },
    snapshotFile: snapshotFileInfo(),
    saveResult: saveRes,
  };
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

// Admin: run refresh (GET/POST) — optional token via REFRESH_TOKEN
app.all("/admin/refresh", async (req, res) => {
  try {
    if (REFRESH_TOKEN && (req.query.token !== REFRESH_TOKEN && req.headers["x-refresh-token"] !== REFRESH_TOKEN)) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const meta = await refreshAll("manual_endpoint");
    res.json(meta);
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Admin: snapshot status (+ file info)
app.get("/admin/status", (_req, res) => {
  res.json({
    lastUpdated: SNAPSHOT.lastUpdated,
    lastReason: SNAPSHOT.lastReason,
    counts: {
      profile: SNAPSHOT.profile?.data?.length || 0,
      performance: SNAPSHOT.performance?.data?.length || 0,
      joined: SNAPSHOT.joinedMatched?.count || 0
    },
    stats,
    snapshotFile: snapshotFileInfo()
  });
});

// Admin: force save snapshot to disk
app.post("/admin/save", (_req, res) => {
  const r = saveSnapshotToDisk();
  res.json({ action: "save", ...r, snapshotFile: snapshotFileInfo() });
});

// Admin: reload snapshot from disk
app.post("/admin/load", (_req, res) => {
  const r = loadSnapshotFromDisk();
  res.json({ action: "load", ...r, snapshotFile: snapshotFileInfo() });
});

// Full profile
app.get("/api/profile", async (_req, res) => {
  try {
    const data = await getProfile(); // from snapshot or first-load
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
function filterToKeyCols(rows, headers) {
  const schoolIdx = findSchoolIdIndex(headers);
  const idxs = [];
  if (schoolIdx >= 0) idxs.push(schoolIdx);
  for (let i = AB_IDX; i <= AU_IDX && i < headers.length; i++) idxs.push(i);
  const outHeaders = idxs.map((i) => headers[i]);
  const outRows = rows.map((r) => idxs.map((i) => r[i] ?? ""));
  return { headers: outHeaders, rows: outRows };
}
app.get("/api/performance/key", async (_req, res) => {
  try {
    const { headers: flatHeaders, rows: fullRows } = await getPerformanceFull();
    const { headers, rows } = filterToKeyCols(fullRows, flatHeaders);
    const data = rowsToObjects(rows, headers);
    res.json({ headers, rows, data });
  } catch (err) {
    increment("errors");
    res.status(500).json({ error: String(err?.message || err) });
  }
});

// Joined dataset: default inner-join; pass ?onlyMatched=false for outer-join (computed from snapshot)
app.get("/api/schools", async (req, res) => {
  try {
    const onlyMatched = (req.query.onlyMatched ?? "true").toString().toLowerCase() !== "false";
    if (onlyMatched) {
      const result = await getJoinedFull();
      return res.json(result);
    }
    // Outer-join fallback using snapshot
    const profile = await getProfile();
    const perf = await getPerformanceFull();
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

// ---- Start (try load from disk; else pre-warm from Sheets)
app.listen(PORT, () => {
  console.log(`Server listening on :${PORT}`);
  (async () => {
    try {
      const loadRes = loadSnapshotFromDisk();
      if (loadRes.ok) {
        console.log("Snapshot loaded from disk:", snapshotFileInfo());
      } else {
        const meta = await refreshAll("startup");
        console.log("Snapshot preloaded from Sheets:", meta);
      }
    } catch (e) {
      console.warn("Startup snapshot init failed:", e?.message || e);
    }
  })();
});

/*
ENV REQUIRED:
- SPREADSHEET_ID
- GOOGLE_SERVICE_ACCOUNT_KEY (JSON string)  OR  GOOGLE_API_KEY

OPTIONAL:
- REFRESH_TOKEN       (secret for /admin/refresh)
- SNAPSHOT_FILE       (default: ./snapshot.json — point this to a Persistent Disk on Render, e.g. /data/snapshot.json)

NOTES:
- Snapshot is persisted to disk and reloaded on boot (fast wake-ups on sleeping free tier).
- If the file is missing (first boot), the server fetches from Sheets and writes snapshot.json.
- Endpoints:
  * GET/POST /admin/refresh[?token=...]  -> pulls sheets, updates snapshot, saves to disk
  * GET       /admin/status              -> snapshot counts, stats, and snapshot file info
  * POST      /admin/save                -> force write current snapshot to disk
  * POST      /admin/load                -> reload snapshot from disk
  * GET       /api/profile, /api/performance, /api/performance/key
  * GET       /api/schools               -> inner join by default; ?onlyMatched=false for outer
  * GET       /api/debug/perf-headers
*/
