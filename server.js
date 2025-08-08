// server.js - Node.js server with caching for Google Sheets data
const express = require('express');
const cors = require('cors');
const { google } = require('googleapis');
const NodeCache = require('node-cache');

const app = express();
const PORT = process.env.PORT || 3000;

// Enable CORS for web clients
app.use(cors());
app.use(express.json());

// Initialize cache with 60 second TTL
const cache = new NodeCache({ stdTTL: 60 });

// Google Sheets configuration
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const API_KEY = process.env.GOOGLE_API_KEY;

// Optional: Use Service Account for better security
const SERVICE_ACCOUNT_KEY = process.env.GOOGLE_SERVICE_ACCOUNT_KEY 
  ? JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY) 
  : null;

// Statistics tracking
let stats = {
  totalRequests: 0,
  cacheHits: 0,
  cacheMisses: 0,
  apiCalls: 0,
  errors: 0,
  lastApiCall: null
};

// Initialize Google Sheets client
function getGoogleSheetsClient() {
  if (SERVICE_ACCOUNT_KEY) {
    // Preferred: Use Service Account
    const auth = new google.auth.GoogleAuth({
      credentials: SERVICE_ACCOUNT_KEY,
      scopes: ['https://www.googleapis.com/auth/spreadsheets.readonly'],
    });
    return google.sheets({ version: 'v4', auth });
  } else {
    // Fallback: Use API Key
    return google.sheets({ version: 'v4', auth: API_KEY });
  }
}

// Fetch data from Google Sheets
async function fetchFromGoogleSheets(sheetName = 'Sheet2', range = 'A:C') {
  try {
    const sheets = getGoogleSheetsClient();
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetName}!${range}`,
    });

    stats.apiCalls++;
    stats.lastApiCall = new Date();

    return response.data.values || [];
  } catch (error) {
    stats.errors++;
    throw error;
  }
}

// Main endpoint to get sheet data
app.get('/api/sheets/:sheetName', async (req, res) => {
  stats.totalRequests++;
  
  const { sheetName } = req.params;
  const range = req.query.range || 'A:C';
  const cacheKey = `${sheetName}-${range}`;

  try {
    // Check cache first
    const cachedData = cache.get(cacheKey);
    
    if (cachedData) {
      stats.cacheHits++;
      return res.json({
        data: cachedData,
        cached: true,
        cacheExpiry: cache.getTtl(cacheKey),
        stats: {
          cacheHitRate: ((stats.cacheHits / stats.totalRequests) * 100).toFixed(2) + '%'
        }
      });
    }

    // Cache miss - fetch from Google Sheets
    stats.cacheMisses++;
    const data = await fetchFromGoogleSheets(sheetName, range);
    
    // Store in cache
    cache.set(cacheKey, data);

    res.json({
      data: data,
      cached: false,
      cacheExpiry: cache.getTtl(cacheKey),
      stats: {
        cacheHitRate: ((stats.cacheHits / stats.totalRequests) * 100).toFixed(2) + '%'
      }
    });

  } catch (error) {
    console.error('Error fetching data:', error);
    res.status(500).json({
      error: 'Failed to fetch data',
      message: error.message
    });
  }
});

// Endpoint to get server statistics
app.get('/api/stats', (req, res) => {
  res.json({
    ...stats,
    cacheSize: cache.keys().length,
    uptime: process.uptime(),
    cacheHitRate: stats.totalRequests > 0 
      ? ((stats.cacheHits / stats.totalRequests) * 100).toFixed(2) + '%'
      : '0%'
  });
});

// Endpoint to manually clear cache
app.post('/api/cache/clear', (req, res) => {
  cache.flushAll();
  res.json({ message: 'Cache cleared successfully' });
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date() });
});

// Serve a simple demo page
app.get('/', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Google Sheets API Server</title>
      <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .endpoint { background: #f4f4f4; padding: 10px; margin: 10px 0; border-radius: 5px; }
        code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
      </style>
    </head>
    <body>
      <h1>Google Sheets API Server with Caching</h1>
      <p>This server provides cached access to Google Sheets data, reducing API calls and improving performance.</p>
      
      <h2>Available Endpoints:</h2>
      <div class="endpoint">
        <strong>GET /api/sheets/:sheetName</strong><br>
        Fetch data from a specific sheet (default range: A:C)<br>
        Query params: <code>?range=A:Z</code>
      </div>
      
      <div class="endpoint">
        <strong>GET /api/stats</strong><br>
        View server statistics and cache performance
      </div>
      
      <div class="endpoint">
        <strong>POST /api/cache/clear</strong><br>
        Manually clear the cache
      </div>
      
      <h2>Configuration:</h2>
      <p>Set these environment variables:</p>
      <ul>
        <li><code>SPREADSHEET_ID</code> - Your Google Sheets ID</li>
        <li><code>GOOGLE_API_KEY</code> - Your Google API Key (or use Service Account)</li>
        <li><code>GOOGLE_SERVICE_ACCOUNT_KEY</code> - Service Account JSON (recommended)</li>
        <li><code>PORT</code> - Server port (default: 3000)</li>
      </ul>
    </body>
    </html>
  `);
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Cache TTL: 60 seconds`);
  console.log(`Using ${SERVICE_ACCOUNT_KEY ? 'Service Account' : 'API Key'} authentication`);
});

// --- Package.json ---
/*
{
  "name": "google-sheets-cache-server",
  "version": "1.0.0",
  "description": "Cached Google Sheets API server",
  "main": "server.js",
  "scripts": {
    "start": "node server.js",
    "dev": "nodemon server.js"
  },
  "dependencies": {
    "express": "^4.18.2",
    "cors": "^2.8.5",
    "googleapis": "^118.0.0",
    "node-cache": "^5.1.2",
    "dotenv": "^16.0.3"
  },
  "devDependencies": {
    "nodemon": "^2.0.22"
  }
}
*/

// --- .env.example ---
/*
SPREADSHEET_ID=your-spreadsheet-id-here
GOOGLE_API_KEY=your-api-key-here

# Optional: Use Service Account instead (more secure)
# GOOGLE_SERVICE_ACCOUNT_KEY={"type":"service_account","project_id":"...","private_key":"...","client_email":"..."}

PORT=3000
*/

// --- client.html - Updated client to use the server ---
/*
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Google Sheets Table Viewer (Server Version)</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1000px;
            margin: 50px auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 { color: #333; margin-bottom: 20px; }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 12px 24px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
            margin-right: 10px;
        }
        button:hover { background-color: #45a049; }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f8f9fa;
            font-weight: bold;
        }
        tr:hover { background-color: #f5f5f5; }
        .stats {
            margin-top: 20px;
            padding: 15px;
            background-color: #e3f2fd;
            border-radius: 4px;
            font-size: 14px;
        }
        .cache-hit { color: #4CAF50; font-weight: bold; }
        .cache-miss { color: #FF9800; font-weight: bold; }
        .error { color: #f44336; padding: 15px; background-color: #ffebee; border-radius: 4px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>Google Sheets Table Viewer (Cached Server Version)</h1>
        
        <button onclick="fetchData()">Fetch Data</button>
        <button onclick="fetchStats()">View Stats</button>
        <button onclick="clearCache()">Clear Cache</button>
        
        <div id="stats"></div>
        <div id="result"></div>
    </div>

    <script>
        const SERVER_URL = 'http://localhost:3000'; // Change this to your server URL

        async function fetchData() {
            const resultDiv = document.getElementById('result');
            const statsDiv = document.getElementById('stats');
            
            try {
                const response = await fetch(`${SERVER_URL}/api/sheets/Sheet2`);
                const result = await response.json();
                
                if (response.ok) {
                    displayTable(result.data);
                    
                    // Show cache status
                    statsDiv.innerHTML = `
                        <div class="stats">
                            <strong>Cache Status:</strong> 
                            <span class="${result.cached ? 'cache-hit' : 'cache-miss'}">
                                ${result.cached ? 'Cache Hit' : 'Cache Miss (Fresh from Google Sheets)'}
                            </span><br>
                            <strong>Cache Hit Rate:</strong> ${result.stats.cacheHitRate}<br>
                            <strong>Cache Expires:</strong> ${new Date(result.cacheExpiry).toLocaleTimeString()}
                        </div>
                    `;
                } else {
                    throw new Error(result.message || 'Failed to fetch data');
                }
            } catch (error) {
                resultDiv.innerHTML = `<div class="error">Error: ${error.message}</div>`;
            }
        }

        async function fetchStats() {
            const resultDiv = document.getElementById('result');
            
            try {
                const response = await fetch(`${SERVER_URL}/api/stats`);
                const stats = await response.json();
                
                resultDiv.innerHTML = `
                    <div class="stats">
                        <h3>Server Statistics</h3>
                        <strong>Total Requests:</strong> ${stats.totalRequests}<br>
                        <strong>Cache Hits:</strong> ${stats.cacheHits}<br>
                        <strong>Cache Misses:</strong> ${stats.cacheMisses}<br>
                        <strong>API Calls to Google:</strong> ${stats.apiCalls}<br>
                        <strong>Cache Hit Rate:</strong> ${stats.cacheHitRate}<br>
                        <strong>Errors:</strong> ${stats.errors}<br>
                        <strong>Last API Call:</strong> ${stats.lastApiCall ? new Date(stats.lastApiCall).toLocaleString() : 'Never'}<br>
                        <strong>Server Uptime:</strong> ${Math.floor(stats.uptime / 60)} minutes
                    </div>
                `;
            } catch (error) {
                resultDiv.innerHTML = `<div class="error">Error: ${error.message}</div>`;
            }
        }

        async function clearCache() {
            try {
                const response = await fetch(`${SERVER_URL}/api/cache/clear`, { method: 'POST' });
                const result = await response.json();
                alert(result.message);
            } catch (error) {
                alert('Error clearing cache: ' + error.message);
            }
        }

        function displayTable(values) {
            const resultDiv = document.getElementById('result');
            
            if (!values || values.length === 0) {
                resultDiv.innerHTML = '<p>No data found</p>';
                return;
            }
            
            const headers = values[0] || ['ID', 'Name', 'Value'];
            const rows = values.slice(1);
            
            let html = '<table><thead><tr>';
            headers.forEach(header => {
                html += `<th>${escapeHtml(header || '')}</th>`;
            });
            html += '</tr></thead><tbody>';
            
            rows.forEach(row => {
                html += '<tr>';
                for (let i = 0; i < headers.length; i++) {
                    html += `<td>${escapeHtml(row[i] || '')}</td>`;
                }
                html += '</tr>';
            });
            
            html += '</tbody></table>';
            html += `<p style="margin-top: 10px; color: #666;">Total records: ${rows.length}</p>`;
            
            resultDiv.innerHTML = html;
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        // Auto-refresh every 30 seconds
        setInterval(fetchData, 30000);
    </script>
</body>
</html>
*/