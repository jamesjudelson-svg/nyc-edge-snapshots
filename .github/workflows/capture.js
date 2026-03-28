// capture.js
// Runs nightly at midnight EDT via GitHub Actions
// Fetches NWS + Open-Meteo forecasts and saves to Supabase

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const NWS_POINT = 'https://api.weather.gov/points/40.781,-73.967';

// ── Helpers ──────────────────────────────────────────────────────────────────

function cToF(c) { return Math.round(c * 9/5 + 32); }

function windDegToDir(deg) {
  if (deg == null) return '—';
  const dirs = ['N','NNE','NE','ENE','E','ESE','SE','SSE','S','SSW','SW','WSW','W','WNW','NW','NNW'];
  return dirs[Math.round(deg / 22.5) % 16];
}

function windRegime(dir) {
  if (!dir || dir === '—') return 'OTHER';
  if (['SW','WSW','W','WNW'].includes(dir)) return 'SW/W';
  if (['N','NNE','NNW','NE','ENE','E','ESE'].includes(dir)) return 'N/NE';
  return 'S/SE';
}

// Get tomorrow's date in Eastern time as YYYY-MM-DD
// This runs at midnight EDT so "tomorrow" = the day we're forecasting
function getTomorrowEastern() {
  const now = new Date();
  // Convert to Eastern time
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  // Add one day (since we run at midnight EDT, tomorrow = the forecast day)
  eastern.setDate(eastern.getDate() + 1);
  return eastern.toISOString().slice(0, 10);
}

// Get today's date in Eastern time (the date whose LST window starts at 1AM EDT tonight)
function getTodayEastern() {
  const now = new Date();
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  return eastern.toISOString().slice(0, 10);
}

// LST window: 1AM EDT today through 12:59AM EDT tomorrow (during DST)
// The forecast date is "today" Eastern — the day whose CLI will resolve tomorrow night
function getLSTWindow(dateStr) {
  // Start: 1:00AM EDT on dateStr
  const start = new Date(`${dateStr}T01:00:00`);
  // End: 12:59AM EDT on dateStr+1
  const end = new Date(`${dateStr}T00:59:59`);
  end.setDate(end.getDate() + 1);
  return { start, end };
}

// ── Fetch NWS ─────────────────────────────────────────────────────────────────

async function fetchNWS(forecastDate) {
  console.log('Fetching NWS forecast...');
  
  // Get forecast URL from points API
  const pointsRes = await fetch(NWS_POINT, {
    headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' }
  });
  if (!pointsRes.ok) throw new Error(`NWS points API failed: ${pointsRes.status}`);
  const pointsData = await pointsRes.json();
  const forecastHourlyUrl = pointsData.properties.forecastHourly;

  // Fetch hourly forecast
  const forecastRes = await fetch(forecastHourlyUrl, {
    headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' }
  });
  if (!forecastRes.ok) throw new Error(`NWS hourly forecast failed: ${forecastRes.status}`);
  const forecastData = await forecastRes.json();

  const periods = forecastData.properties.periods;
  const { start, end } = getLSTWindow(forecastDate);

  // Filter to LST window for the forecast date
  const windowPeriods = periods.filter(p => {
    const t = new Date(p.startTime);
    return t >= start && t <= end;
  });

  if (!windowPeriods.length) {
    console.warn('No NWS periods found in LST window, using all periods for date');
  }

  const targetPeriods = windowPeriods.length ? windowPeriods : 
    periods.filter(p => new Date(p.startTime).toISOString().slice(0,10) === forecastDate);

  let nwsHigh = null, nwsLow = null;
  const hourly = [];

  targetPeriods.forEach(p => {
    const temp = p.temperature; // Already in F for US
    if (nwsHigh === null || temp > nwsHigh) nwsHigh = temp;
    if (nwsLow === null || temp < nwsLow) nwsLow = temp;
    hourly.push({
      time: p.startTime,
      temp,
      wind_dir: p.windDirection,
      wind_speed: p.windSpeed,
      short_forecast: p.shortForecast
    });
  });

  // Dominant afternoon wind direction (1PM-5PM EDT)
  const afternoonPeriods = targetPeriods.filter(p => {
    const h = new Date(p.startTime).getHours();
    return h >= 13 && h <= 17;
  });
  const windDirs = afternoonPeriods.map(p => p.windDirection).filter(Boolean);
  const domDir = windDirs.length ? 
    windDirs.sort((a,b) => windDirs.filter(v=>v===a).length - windDirs.filter(v=>v===b).length).pop() 
    : '—';

  console.log(`NWS: high=${nwsHigh}°F low=${nwsLow}°F wind=${domDir}`);
  return { nwsHigh, nwsLow, domDir, hourly };
}

// ── Fetch Open-Meteo ──────────────────────────────────────────────────────────

async function fetchModel(forecastDate) {
  console.log('Fetching Open-Meteo model...');
  
  const url = [
    'https://api.open-meteo.com/v1/gfs',
    '?latitude=40.7812&longitude=-73.9665',
    '&hourly=temperature_2m,windspeed_10m,winddirection_10m,cloudcover,dewpoint_2m,precipitation_probability',
    '&temperature_unit=fahrenheit',
    '&windspeed_unit=mph',
    '&forecast_days=3',
    '&timezone=America%2FNew_York'
  ].join('');

  const res = await fetch(url);
  if (!res.ok) throw new Error(`Open-Meteo failed: ${res.status}`);
  const data = await res.json();

  const { start, end } = getLSTWindow(forecastDate);
  // Also include hour 0 of next day
  const nextDate = new Date(forecastDate + 'T12:00:00');
  nextDate.setDate(nextDate.getDate() + 1);
  const nextDateStr = nextDate.toISOString().slice(0, 10);

  let modelHigh = null, modelLow = null;
  const hourly = [];

  data.hourly.time.forEach((t, i) => {
    const tDate = t.slice(0, 10);
    const tHour = parseInt(t.slice(11, 13));
    // LST window: hours 1-23 of forecastDate + hour 0 of nextDate
    const inWindow = (tDate === forecastDate && tHour >= 1) || 
                     (tDate === nextDateStr && tHour === 0);
    
    if (inWindow) {
      const temp = Math.round(data.hourly.temperature_2m[i]);
      if (modelHigh === null || temp > modelHigh) modelHigh = temp;
      if (modelLow === null || temp < modelLow) modelLow = temp;
      hourly.push({
        time: t,
        temp,
        wind_dir: windDegToDir(data.hourly.winddirection_10m[i]),
        wind_speed: data.hourly.windspeed_10m[i],
        cloud: data.hourly.cloudcover[i],
        dew: Math.round(data.hourly.dewpoint_2m[i]),
        pop: data.hourly.precipitation_probability[i]
      });
    }
  });

  // Dominant afternoon wind (1PM-5PM)
  const afHours = data.hourly.time
    .map((t, i) => ({ t, i, hour: parseInt(t.slice(11,13)), date: t.slice(0,10) }))
    .filter(({date, hour}) => date === forecastDate && hour >= 13 && hour <= 17);
  
  const afDirs = afHours.map(({i}) => windDegToDir(data.hourly.winddirection_10m[i])).filter(d => d !== '—');
  const domDir = afDirs.length ?
    afDirs.sort((a,b) => afDirs.filter(v=>v===a).length - afDirs.filter(v=>v===b).length).pop()
    : '—';

  console.log(`Model: high=${modelHigh}°F low=${modelLow}°F wind=${domDir}`);
  return { modelHigh, modelLow, domDir, hourly };
}

// ── Save to Supabase ──────────────────────────────────────────────────────────

async function saveSnapshot(forecastDate, nwsData, modelData) {
  const regime = windRegime(nwsData.domDir);
  
  const snapshot = {
    date: forecastDate,
    captured_at: new Date().toISOString(),
    nws_high: nwsData.nwsHigh,
    nws_low: nwsData.nwsLow,
    model_high: modelData.modelHigh,
    model_low: modelData.modelLow,
    wind_regime: regime,
    dominant_dir: nwsData.domDir,
    nws_hourly: nwsData.hourly,
    model_hourly: modelData.hourly
  };

  console.log('Saving snapshot to Supabase...');
  console.log(JSON.stringify(snapshot, null, 2));

  const res = await fetch(`${SUPABASE_URL}/rest/v1/snapshots`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'apikey': SUPABASE_SERVICE_KEY,
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify(snapshot)
  });

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Supabase save failed: ${res.status} - ${err}`);
  }

  console.log(`✅ Snapshot saved for ${forecastDate}`);
  return snapshot;
}

// ── Score Yesterday ───────────────────────────────────────────────────────────

async function scoreYesterday() {
  // Get yesterday's date in Eastern time
  const now = new Date();
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  eastern.setDate(eastern.getDate() - 1);
  const yesterday = eastern.toISOString().slice(0, 10);

  console.log(`Attempting to score ${yesterday}...`);

  // Check if already scored
  const checkRes = await fetch(
    `${SUPABASE_URL}/rest/v1/scored_days?date=eq.${yesterday}`,
    {
      headers: {
        'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
        'apikey': SUPABASE_SERVICE_KEY
      }
    }
  );
  const existing = await checkRes.json();
  if (existing.length > 0) {
    console.log(`${yesterday} already scored, skipping`);
    return;
  }

  // Get yesterday's snapshot
  const snapRes = await fetch(
    `${SUPABASE_URL}/rest/v1/snapshots?date=eq.${yesterday}`,
    {
      headers: {
        'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
        'apikey': SUPABASE_SERVICE_KEY
      }
    }
  );
  const snaps = await snapRes.json();
  if (!snaps.length) {
    console.log(`No snapshot found for ${yesterday}, cannot score`);
    return;
  }
  const snap = snaps[0];

  // Fetch CLI report
  const CLI_URL = 'https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC';
  let cliText = null;
  try {
    const cliRes = await fetch(CLI_URL, {
      headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' }
    });
    cliText = await cliRes.text();
  } catch(e) {
    console.log('CLI fetch failed:', e.message);
    return;
  }

  // Parse CLI
  const dateMatch = cliText.match(/CLIMATE SUMMARY FOR (\w+ \d+ \d+)/);
  const maxMatch = cliText.match(/MAXIMUM\s+(\d+)\s/);
  const minMatch = cliText.match(/MINIMUM\s+(\d+)\s/);

  if (!dateMatch || !maxMatch || !minMatch) {
    console.log('Could not parse CLI report');
    return;
  }

  const cliDate = new Date(dateMatch[1] + ' 12:00:00 EDT').toISOString().slice(0, 10);
  if (cliDate !== yesterday) {
    console.log(`CLI date ${cliDate} doesn't match yesterday ${yesterday}`);
    return;
  }

  const actualHigh = parseInt(maxMatch[1]);
  const actualLow = parseInt(minMatch[1]);

  const scored = {
    date: yesterday,
    nws_high: snap.nws_high,
    nws_low: snap.nws_low,
    model_high: snap.model_high,
    model_low: snap.model_low,
    wind_regime: snap.wind_regime,
    actual_high: actualHigh,
    actual_low: actualLow,
    nws_high_err: snap.nws_high - actualHigh,
    model_high_err: snap.model_high - actualHigh,
    nws_low_err: snap.nws_low - actualLow,
    model_low_err: snap.model_low - actualLow,
    winner_high: Math.abs(snap.model_high - actualHigh) < Math.abs(snap.nws_high - actualHigh) ? 'model' :
                 Math.abs(snap.nws_high - actualHigh) < Math.abs(snap.model_high - actualHigh) ? 'nws' : 'tie',
    winner_low: Math.abs(snap.model_low - actualLow) < Math.abs(snap.nws_low - actualLow) ? 'model' :
                Math.abs(snap.nws_low - actualLow) < Math.abs(snap.model_low - actualLow) ? 'nws' : 'tie',
    scored_at: new Date().toISOString()
  };

  const saveRes = await fetch(`${SUPABASE_URL}/rest/v1/scored_days`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'apikey': SUPABASE_SERVICE_KEY,
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify(scored)
  });

  if (!saveRes.ok) {
    const err = await saveRes.text();
    throw new Error(`Supabase score save failed: ${saveRes.status} - ${err}`);
  }

  console.log(`✅ Scored ${yesterday}: actual high=${actualHigh}°F low=${actualLow}°F`);
  console.log(`   Model high err: ${scored.model_high_err}°F | NWS high err: ${scored.nws_high_err}°F`);
  console.log(`   Winner high: ${scored.winner_high} | Winner low: ${scored.winner_low}`);
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log('=== NYC Edge Midnight Capture ===');
  console.log(`Time: ${new Date().toISOString()}`);

  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables');
  }

  // The forecast date = today in Eastern time
  // (script runs at midnight EDT, so "today" = the day whose LST window just started)
  const forecastDate = getTodayEastern();
  console.log(`Forecast date: ${forecastDate}`);

  try {
    // 1. Score yesterday (if CLI is available)
    await scoreYesterday();
  } catch(e) {
    console.error('Scoring failed:', e.message);
    // Don't abort — continue to snapshot
  }

  try {
    // 2. Capture tonight's snapshot
    const nwsData = await fetchNWS(forecastDate);
    const modelData = await fetchModel(forecastDate);
    await saveSnapshot(forecastDate, nwsData, modelData);
  } catch(e) {
    console.error('Snapshot failed:', e.message);
    process.exit(1);
  }

  console.log('=== Done ===');
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
