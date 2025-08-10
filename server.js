// server.js - Complete Node.js server with caching and school data endpoints
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

// Replace the existing /api/schools/performance endpoint with this fixed version

app.get('/api/schools/performance', async (req, res) => {
  stats.totalRequests++;
  
  const { year, schoolId, region } = req.query;
  const cacheKey = `schools-performance-${JSON.stringify(req.query)}`;

  try {
    // Check cache first
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      stats.cacheHits++;
      return res.json({
        data: cachedData,
        cached: true,
        cacheExpiry: cache.getTtl(cacheKey)
      });
    }

    stats.cacheMisses++;

    // Fetch both sheets
    const [profileData, performanceData] = await Promise.all([
      fetchFromGoogleSheets('School Profile', 'A:E'), // AGE ID, Name, Suburb, Region, Calendar Year
      fetchFromGoogleSheets('School Performance', 'A:Z') // Complex structure with headers in row 3
    ]);

    console.log('Profile data rows:', profileData.length);
    console.log('Performance data rows:', performanceData.length);

    // Process profile data
    const profileHeaders = profileData[0];
    const profiles = profileData.slice(1).map(row => ({
      ageId: String(row[0] || '').trim(),  // Convert to string and trim
      name: row[1],
      suburb: row[2],
      region: row[3],
      calendarYear: parseInt(row[4])
    }));

    // Process performance data - FIXED STRUCTURE
    // Row 0: Category headers
    // Row 1: Metric descriptions  
    // Row 2: Column headers including ID, School, Locality, and years
    // Row 3+: Actual data
    
    const headerRow = performanceData[2]; // This contains ID, School, Locality, 2021, 2022, etc.
    console.log('Header row:', headerRow);
    
    // Find year columns - they're numbers in the header row
    const yearColumns = {};
    headerRow.forEach((header, index) => {
      if (header && !isNaN(header) && header >= 2021 && header <= 2024) {
        yearColumns[header] = index;
        console.log(`Found year ${header} at column ${index}`);
      }
    });
    
    console.log('Year columns found:', yearColumns);

    // Process performance records (starting from row 4, which is index 3)
    const performances = performanceData.slice(3).map(row => {
      const record = { 
        id: String(row[0] || '').trim(), // Convert to string and trim
        school: row[1],
        locality: row[2]
      };
      
      // Extract performance for each year from the first set of year columns
      Object.entries(yearColumns).forEach(([year, colIndex]) => {
        // Only get the first occurrence of each year (columns 3-6)
        if (colIndex >= 3 && colIndex <= 6) {
          record[`performance_${year}`] = row[colIndex];
        }
      });
      
      return record;
    });

    console.log('Sample performance record:', performances[0]);
    console.log('Total performance records:', performances.length);

    // Join the data
    const joinedData = [];
    
    profiles.forEach(profile => {
      const perfRecord = performances.find(p => p.id === profile.ageId);
      
      if (perfRecord) {
        const schoolData = {
          ...profile,
          performances: {}
        };
        
        // Add all year performances
        [2021, 2022, 2023, 2024].forEach(year => {
          schoolData.performances[year] = perfRecord[`performance_${year}`];
        });
        
        joinedData.push(schoolData);
      }
    });

    console.log('Joined data count:', joinedData.length);

    // Apply filters if provided
    let filteredData = joinedData;
    
    if (year) {
      filteredData = filteredData.filter(school => 
        school.calendarYear === parseInt(year) || 
        (school.performances[year] !== null && school.performances[year] !== undefined)
      );
    }
    
    if (schoolId) {
      filteredData = filteredData.filter(school => school.ageId === schoolId);
    }
    
    if (region) {
      filteredData = filteredData.filter(school => 
        school.region && school.region.toLowerCase().includes(region.toLowerCase())
      );
    }

    // Group by school for summary
    const schoolSummary = {};
    filteredData.forEach(record => {
      if (!schoolSummary[record.ageId]) {
        schoolSummary[record.ageId] = {
          ageId: record.ageId,
          name: record.name,
          profiles: [],
          performances: record.performances,
          averagePerformance: 0
        };
      }
      
      schoolSummary[record.ageId].profiles.push({
        year: record.calendarYear,
        suburb: record.suburb,
        region: record.region
      });
    });

    // Calculate average performance for each school
    Object.values(schoolSummary).forEach(school => {
      const scores = Object.values(school.performances)
        .filter(v => v !== null && v !== undefined)
        .map(v => parseFloat(v));
      
      if (scores.length > 0) {
        school.averagePerformance = scores.reduce((a, b) => a + b, 0) / scores.length;
      }
    });

    const result = {
      schools: Object.values(schoolSummary),
      totalSchools: Object.keys(schoolSummary).length,
      years: [2021, 2022, 2023, 2024],
      metadata: {
        profileCount: profiles.length,
        performanceCount: performances.length,
        joinedCount: filteredData.length,
        yearColumnsFound: Object.keys(yearColumns)
      }
    };

    // Store in cache
    cache.set(cacheKey, result);

    res.json({
      data: result,
      cached: false,
      cacheExpiry: cache.getTtl(cacheKey)
    });

  } catch (error) {
    console.error('Error in school performance endpoint:', error);
    res.status(500).json({
      error: 'Failed to fetch school performance data',
      message: error.message
    });
  }
});

// Endpoint for individual school history
app.get('/api/schools/:schoolId/history', async (req, res) => {
  const { schoolId } = req.params;
  const cacheKey = `school-history-${schoolId}`;

  try {
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      return res.json({
        data: cachedData,
        cached: true
      });
    }

    // Fetch both sheets
    const [profileData, performanceData] = await Promise.all([
      fetchFromGoogleSheets('School Profile', 'A:E'),
      fetchFromGoogleSheets('School Performance', 'A:Z')
    ]);

    // Find all profile records for this school
    const profileHeaders = profileData[0];
    const allProfiles = profileData.slice(1)
      .filter(row => row[0] === schoolId)
      .map(row => ({
        ageId: row[0],
        name: row[1],
        suburb: row[2],
        region: row[3],
        calendarYear: parseInt(row[4])
      }))
      .sort((a, b) => a.calendarYear - b.calendarYear);

    // Get performance data
    const yearRow = performanceData[2];
    const performanceRow = performanceData.find(row => row[0] === schoolId);
    
    const performances = {};
    if (performanceRow) {
      yearRow.forEach((year, index) => {
        if (year && !isNaN(year)) {
          performances[year] = performanceRow[index] || null;
        }
      });
    }

    const result = {
      schoolId,
      name: allProfiles[0]?.name || 'Unknown',
      profileHistory: allProfiles,
      performances,
      summary: {
        yearsActive: allProfiles.map(p => p.calendarYear),
        regionsServed: [...new Set(allProfiles.map(p => p.region))],
        suburbsServed: [...new Set(allProfiles.map(p => p.suburb))]
      }
    };

    cache.set(cacheKey, result);

    res.json({
      data: result,
      cached: false
    });

  } catch (error) {
    console.error('Error fetching school history:', error);
    res.status(500).json({
      error: 'Failed to fetch school history',
      message: error.message
    });
  }
});

// Endpoint for comparative analysis
app.get('/api/schools/compare', async (req, res) => {
  const { schoolIds, years } = req.query;
  const ids = schoolIds ? schoolIds.split(',') : [];
  const yearFilter = years ? years.split(',').map(y => parseInt(y)) : [2021, 2022, 2023, 2024];
  
  const cacheKey = `compare-${schoolIds}-${years}`;

  try {
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      return res.json({
        data: cachedData,
        cached: true
      });
    }

    const [profileData, performanceData] = await Promise.all([
      fetchFromGoogleSheets('School Profile', 'A:E'),
      fetchFromGoogleSheets('School Performance', 'A:Z')
    ]);

    // Process and filter schools
    const yearRow = performanceData[2];
    const yearIndices = {};
    yearRow.forEach((year, index) => {
      if (year && yearFilter.includes(parseInt(year))) {
        yearIndices[year] = index;
      }
    });

    const comparisonData = ids.map(schoolId => {
      // Get latest profile
      const schoolProfiles = profileData.slice(1).filter(row => row[0] === schoolId);
      const latestProfile = schoolProfiles[schoolProfiles.length - 1] || {};
      
      // Get performance data
      const perfRow = performanceData.find(row => row[0] === schoolId);
      const performances = {};
      
      if (perfRow) {
        Object.entries(yearIndices).forEach(([year, index]) => {
          performances[year] = perfRow[index] || null;
        });
      }

      return {
        schoolId,
        name: latestProfile[1] || 'Unknown',
        region: latestProfile[3] || 'Unknown',
        performances,
        trend: calculateTrend(performances)
      };
    });

    const result = {
      comparison: comparisonData,
      years: yearFilter,
      summary: {
        highestPerformer: findHighestPerformer(comparisonData),
        mostImproved: findMostImproved(comparisonData),
        averageByYear: calculateAverageByYear(comparisonData, yearFilter)
      }
    };

    cache.set(cacheKey, result);

    res.json({
      data: result,
      cached: false
    });

  } catch (error) {
    console.error('Error in comparison:', error);
    res.status(500).json({
      error: 'Failed to compare schools',
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

// Helper functions
function calculateTrend(performances) {
  const values = Object.entries(performances)
    .filter(([year, value]) => value !== null)
    .sort(([yearA], [yearB]) => parseInt(yearA) - parseInt(yearB))
    .map(([year, value]) => parseFloat(value));

  if (values.length < 2) return 'insufficient_data';
  
  const firstValue = values[0];
  const lastValue = values[values.length - 1];
  const change = ((lastValue - firstValue) / firstValue) * 100;
  
  if (change > 5) return 'improving';
  if (change < -5) return 'declining';
  return 'stable';
}

function findHighestPerformer(schools) {
  let highest = null;
  let highestAvg = -Infinity;
  
  schools.forEach(school => {
    const scores = Object.values(school.performances)
      .filter(v => v !== null)
      .map(v => parseFloat(v));
    
    if (scores.length > 0) {
      const avg = scores.reduce((a, b) => a + b, 0) / scores.length;
      if (avg > highestAvg) {
        highestAvg = avg;
        highest = { name: school.name, average: avg.toFixed(2) };
      }
    }
  });
  
  return highest;
}

function findMostImproved(schools) {
  let mostImproved = null;
  let highestImprovement = -Infinity;
  
  schools.forEach(school => {
    const years = Object.keys(school.performances).sort();
    if (years.length >= 2) {
      const firstYear = years[0];
      const lastYear = years[years.length - 1];
      
      if (school.performances[firstYear] !== null && school.performances[lastYear] !== null) {
        const improvement = parseFloat(school.performances[lastYear]) - parseFloat(school.performances[firstYear]);
        
        if (improvement > highestImprovement) {
          highestImprovement = improvement;
          mostImproved = {
            name: school.name,
            improvement: improvement.toFixed(2),
            from: firstYear,
            to: lastYear
          };
        }
      }
    }
  });
  
  return mostImproved;
}

function calculateAverageByYear(schools, years) {
  const averages = {};
  
  years.forEach(year => {
    const scores = schools
      .map(school => school.performances[year])
      .filter(score => score !== null)
      .map(score => parseFloat(score));
    
    if (scores.length > 0) {
      averages[year] = (scores.reduce((a, b) => a + b, 0) / scores.length).toFixed(2);
    } else {
      averages[year] = null;
    }
  });
  
  return averages;
}

// Add this debug endpoint to your server.js to diagnose the join issue

app.get('/api/debug/join-issue', async (req, res) => {
  try {
    // Fetch both sheets
    const [profileData, performanceData] = await Promise.all([
      fetchFromGoogleSheets('School Profile', 'A:B'), // Just ID and Name
      fetchFromGoogleSheets('School Performance', 'A:C') // Just ID, School, Locality
    ]);

    // Get sample profile IDs
    const profileSample = profileData.slice(1, 6).map(row => ({
      raw: row[0],
      type: typeof row[0],
      trimmed: String(row[0] || '').trim(),
      name: row[1]
    }));

    // Get sample performance IDs
    const performanceSample = performanceData.slice(3, 8).map(row => ({
      raw: row[0],
      type: typeof row[0],
      trimmed: String(row[0] || '').trim(),
      school: row[1]
    }));

    // Try to find matches
    const matches = [];
    profileSample.forEach(profile => {
      const match = performanceSample.find(perf => 
        String(perf.raw).trim() === String(profile.raw).trim()
      );
      matches.push({
        profileId: profile.trimmed,
        profileName: profile.name,
        foundMatch: !!match,
        matchedWith: match ? match.school : 'NO MATCH'
      });
    });

    res.json({
      debug: {
        profileHeaders: profileData[0],
        performanceHeaders: performanceData[2], // Row 3 has the headers
        profileSample: profileSample,
        performanceSample: performanceSample,
        matches: matches,
        issue: matches.every(m => !m.foundMatch) ? 
          'No IDs are matching! Check if School AGE ID matches ID column' : 
          'Some matches found'
      }
    });

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Also add a simpler endpoint to just compare the first few IDs
app.get('/api/debug/compare-ids', async (req, res) => {
  try {
    const [profileData, performanceData] = await Promise.all([
      fetchFromGoogleSheets('School Profile', 'A1:A10'),
      fetchFromGoogleSheets('School Performance', 'A1:A10')
    ]);

    // Get the column headers
    const profileHeader = profileData[0][0]; // Should be "School AGE ID"
    const performanceHeader = performanceData[2][0]; // Should be "ID"

    // Get actual IDs
    const profileIDs = profileData.slice(1, 6).map(row => row[0]);
    const performanceIDs = performanceData.slice(3, 8).map(row => row[0]);

    res.json({
      headers: {
        profile: profileHeader,
        performance: performanceHeader
      },
      firstFiveIDs: {
        profile: profileIDs,
        performance: performanceIDs
      },
      analysis: {
        profileIDsAreNumbers: profileIDs.every(id => typeof id === 'number'),
        performanceIDsAreNumbers: performanceIDs.every(id => typeof id === 'number'),
        sampleMatch: `Profile ID "${profileIDs[0]}" ${
          performanceIDs.includes(profileIDs[0]) ? 'MATCHES' : 'DOES NOT MATCH'
        } performance IDs`
      }
    });

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Serve a simple demo page
app.get('/', (req, res) => {
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>School Data API Server</title>
      <style>
        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
        .endpoint { background: #f4f4f4; padding: 10px; margin: 10px 0; border-radius: 5px; }
        code { background: #e0e0e0; padding: 2px 5px; border-radius: 3px; }
      </style>
    </head>
    <body>
      <h1>School Data API Server with Caching</h1>
      <p>This server provides cached access to school profile and performance data.</p>
      
      <h2>Available Endpoints:</h2>
      <div class="endpoint">
        <strong>GET /api/sheets/:sheetName</strong><br>
        Fetch data from a specific sheet<br>
        Query params: <code>?range=A:Z</code>
      </div>
      
      <div class="endpoint">
        <strong>GET /api/schools/performance</strong><br>
        Get joined school profile and performance data<br>
        Query params: <code>?year=2023&region=North&schoolId=12345</code>
      </div>
      
      <div class="endpoint">
        <strong>GET /api/schools/:schoolId/history</strong><br>
        Get complete history for a specific school
      </div>
      
      <div class="endpoint">
        <strong>GET /api/schools/compare</strong><br>
        Compare multiple schools<br>
        Query params: <code>?schoolIds=123,456,789&years=2022,2023,2024</code>
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
