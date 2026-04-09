// capture.js
// Two jobs only:
//   05:00 UTC (1AM EDT) — take today's snapshot
//   07:00 UTC (3AM EDT) — score yesterday via CLI (with retry loop)

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const NWS_POINT = 'https://api.weather.gov/points/40.781,-73.967';
const CLI_URL = 'https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC';

const CLI_MAX_RETRIES = 24;          // 24 x 5min = 2 hours
const CLI_RETRY_INTERVAL_MS = 5 * 60 * 1000;

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

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

function getTodayEastern() {
  const now = new Date();
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const y = eastern.getFullYear();
  const m = String(eastern.getMonth() + 1).padStart(2, '0');
  const d = String(eastern.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function getYesterdayEastern() {
  const now = new Date();
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  eastern.setDate(eastern.getDate() - 1);
  const y = eastern.getFullYear();
  const m = String(eastern.getMonth() + 1).padStart(2, '0');
  const d = String(eastern.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function getLSTWindow(dateStr) {
  const start = new Date(`${dateStr}T01:00:00`);
  const end = new Date(`${dateStr}T00:59:59`);
  end.setDate(end.getDate() + 1);
  return { start, end };
}

function detectJobType() {
  if (process.env.JOB_TYPE) {
    return process.env.JOB_TYPE.toLowerCase().trim();
  }
  const utcHour = new Date().getUTCHours();
  const utcMin = new Date().getUTCMinutes();
  const utcDecimal = utcHour + utcMin / 60;
  if (utcDecimal >= 4.5 && utcDecimal < 6.5) return 'snapshot'; // 1AM EDT = 05:00 UTC
  if (utcDecimal >= 6.5 && utcDecimal < 9.0) return 'score';    // 3AM EDT = 07:00 UTC
  return 'unknown';
}

// ── CLI ───────────────────────────────────────────────────────────────────────

function parseCLI(text, expectedDate) {
  const dateMatch = text.match(/CLIMATE SUMMARY FOR (\w+ \d+ \d+)/);
  const maxMatch = text.match(/MAXIMUM\s+(\d+)\s/);
  const minMatch = text.match(/MINIMUM\s+(\d+)\s/);
  if (!dateMatch || !maxMatch || !minMatch) return null;
  const cliDate = new Date(dateMatch[1] + ' 12:00:00 EDT').toISOString().slice(0, 10);
  if (cliDate !== expectedDate) return null;
  return { date: cliDate, high: parseInt(maxMatch[1]), low: parseInt(minMatch[1]) };
}

async function fetchCLIWithRetry(expectedDate) {
  console.log(`Fetching CLI for ${expectedDate} (retrying up to ${CLI_MAX_RETRIES}x every 5min)...`);
  for (let attempt = 1; attempt <= CLI_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(CLI_URL, {
        headers: { 'User-Agent': 'nyc-edge-snapshots/1.0', 'Cache-Control': 'no-cache' }
      });
      if (res.ok) {
        const text = await res.text();
        const parsed = parseCLI(text, expectedDate);
        if (parsed) {
          console.log(`✅ CLI found on attempt ${attempt}: high=${parsed.high}°F low=${parsed.low}°F`);
          return parsed;
        }
        const dateMatch = text.match(/CLIMATE SUMMARY FOR (\w+ \d+ \d+)/);
        const cliDate = dateMatch ? new Date(dateMatch[1] + ' 12:00:00 EDT').toISOString().slice(0, 10) : 'unknown';
        console.log(`CLI shows ${cliDate}, need ${expectedDate} (attempt ${attempt}/${CLI_MAX_RETRIES})`);
      } else {
        console.warn(`CLI returned ${res.status} (attempt ${attempt}/${CLI_MAX_RETRIES})`);
      }
    } catch(e) {
      console.warn(`CLI fetch failed: ${e.message} (attempt ${attempt}/${CLI_MAX_RETRIES})`);
    }
    if (attempt < CLI_MAX_RETRIES) await sleep(CLI_RETRY_INTERVAL_MS);
  }
  console.error(`❌ CLI not available for ${expectedDate} after ${CLI_MAX_RETRIES} attempts`);
  return null;
}

// ── Score Job ─────────────────────────────────────────────────────────────────

async function runScoreJob() {
  const yesterday = getYesterdayEastern();
  console.log(`=== Score Job — scoring ${yesterday} ===`);

  // Already scored?
  const checkRes = await fetch(
    `${SUPABASE_URL}/rest/v1/scored_days?date=eq.${yesterday}`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
  );
  const existing = await checkRes.json();
  if (existing.length > 0) {
    console.log(`${yesterday} already scored, skipping`);
    return;
  }

  // Get snapshot
  const snapRes = await fetch(
    `${SUPABASE_URL}/rest/v1/snapshots?date=eq.${yesterday}`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
  );
  const snaps = await snapRes.json();
  if (!snaps.length) {
    console.error(`No snapshot found for ${yesterday} — cannot score`);
    return;
  }
  const snap = snaps[0];

  // Fetch CLI with retry
  const cli = await fetchCLIWithRetry(yesterday);
  if (!cli) return;

  // Save to cli_reports
  await fetch(`${SUPABASE_URL}/rest/v1/cli_reports`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'apikey': SUPABASE_SERVICE_KEY,
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify({
      date: yesterday,
      captured_at: new Date().toISOString(),
      actual_high: cli.high,
      actual_low: cli.low
    })
  });
  console.log(`✅ CLI saved: high=${cli.high}°F low=${cli.low}°F`);

  // Score
  const scored = {
    date: yesterday,
    nws_high: snap.nws_high,
    nws_low: snap.nws_low,
    model_high: snap.model_high,
    model_low: snap.model_low,
    wind_regime: snap.wind_regime,
    actual_high: cli.high,
    actual_low: cli.low,
    nws_high_err: snap.nws_high - cli.high,
    model_high_err: snap.model_high - cli.high,
    nws_low_err: snap.nws_low - cli.low,
    model_low_err: snap.model_low - cli.low,
    winner_high: Math.abs(snap.model_high - cli.high) < Math.abs(snap.nws_high - cli.high) ? 'model' :
                 Math.abs(snap.nws_high - cli.high) < Math.abs(snap.model_high - cli.high) ? 'nws' : 'tie',
    winner_low: Math.abs(snap.model_low - cli.low) < Math.abs(snap.nws_low - cli.low) ? 'model' :
                Math.abs(snap.nws_low - cli.low) < Math.abs(snap.model_low - cli.low) ? 'nws' : 'tie',
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

  if (!saveRes.ok) throw new Error(`Score save failed: ${saveRes.status} - ${await saveRes.text()}`);

  console.log(`✅ Scored ${yesterday}: high=${cli.high}°F low=${cli.low}°F`);
  console.log(`   Model high err: ${scored.model_high_err}°F | NWS high err: ${scored.nws_high_err}°F`);
  console.log(`   Winner high: ${scored.winner_high} | Winner low: ${scored.winner_low}`);
}

// ── NWS ───────────────────────────────────────────────────────────────────────

async function fetchNWS(forecastDate) {
  console.log('Fetching NWS forecast...');
  const pointsRes = await fetch(NWS_POINT, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' } });
  if (!pointsRes.ok) throw new Error(`NWS points failed: ${pointsRes.status}`);
  const pointsData = await pointsRes.json();
  const forecastRes = await fetch(pointsData.properties.forecastHourly, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' } });
  if (!forecastRes.ok) throw new Error(`NWS hourly failed: ${forecastRes.status}`);
  const forecastData = await forecastRes.json();
  const periods = forecastData.properties.periods;
  const { start, end } = getLSTWindow(forecastDate);
  const windowPeriods = periods.filter(p => { const t = new Date(p.startTime); return t >= start && t <= end; });
  const targetPeriods = windowPeriods.length ? windowPeriods :
    periods.filter(p => new Date(p.startTime).toISOString().slice(0,10) === forecastDate);
  let nwsHigh = null, nwsLow = null;
  const hourly = [];
  targetPeriods.forEach(p => {
    const temp = p.temperature;
    if (nwsHigh === null || temp > nwsHigh) nwsHigh = temp;
    if (nwsLow === null || temp < nwsLow) nwsLow = temp;
    hourly.push({ time: p.startTime, temp, wind_dir: p.windDirection, wind_speed: p.windSpeed, short_forecast: p.shortForecast });
  });
  const afternoonPeriods = targetPeriods.filter(p => { const h = new Date(p.startTime).getHours(); return h >= 13 && h <= 17; });
  const windDirs = afternoonPeriods.map(p => p.windDirection).filter(Boolean);
  const domDir = windDirs.length ?
    windDirs.sort((a,b) => windDirs.filter(v=>v===a).length - windDirs.filter(v=>v===b).length).pop() : '—';
  console.log(`NWS: high=${nwsHigh}°F low=${nwsLow}°F wind=${domDir}`);
  return { nwsHigh, nwsLow, domDir, hourly };
}

// ── Open-Meteo ────────────────────────────────────────────────────────────────

async function fetchModel(forecastDate) {
  console.log('Fetching Open-Meteo model...');
  const url = [
    'https://api.open-meteo.com/v1/gfs',
    '?latitude=40.7812&longitude=-73.9665',
    '&hourly=temperature_2m,windspeed_10m,winddirection_10m,cloudcover,dewpoint_2m,precipitation_probability',
    '&daily=temperature_2m_max,temperature_2m_min',
    '&temperature_unit=fahrenheit',
    '&windspeed_unit=mph',
    '&forecast_days=5',
    '&timezone=America%2FNew_York'
  ].join('');
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Open-Meteo returned ${res.status}`);
      const data = await res.json();
      const nextDate = new Date(forecastDate + 'T12:00:00');
      nextDate.setDate(nextDate.getDate() + 1);
      const nextDateStr = nextDate.toISOString().slice(0, 10);
      const dateIdx = data.daily.time.indexOf(forecastDate);
      const modelHigh = dateIdx >= 0 ? Math.round(data.daily.temperature_2m_max[dateIdx]) : null;
      const modelLow = dateIdx >= 0 ? Math.round(data.daily.temperature_2m_min[dateIdx]) : null;
      const hourly = [];
      data.hourly.time.forEach((t, i) => {
        const tDate = t.slice(0, 10);
        const tHour = parseInt(t.slice(11, 13));
        const inWindow = (tDate === forecastDate && tHour >= 1) || (tDate === nextDateStr && tHour === 0);
        if (inWindow) {
          hourly.push({ time: t, temp: Math.round(data.hourly.temperature_2m[i]),
            wind_dir: windDegToDir(data.hourly.winddirection_10m[i]),
            wind_speed: data.hourly.windspeed_10m[i], cloud: data.hourly.cloudcover[i],
            dew: Math.round(data.hourly.dewpoint_2m[i]), pop: data.hourly.precipitation_probability[i] });
        }
      });
      const afHours = data.hourly.time
        .map((t, i) => ({ t, i, hour: parseInt(t.slice(11,13)), date: t.slice(0,10) }))
        .filter(({date, hour}) => date === forecastDate && hour >= 13 && hour <= 17);
      const afDirs = afHours.map(({i}) => windDegToDir(data.hourly.winddirection_10m[i])).filter(d => d !== '—');
      const domDir = afDirs.length ?
        afDirs.sort((a,b) => afDirs.filter(v=>v===a).length - afDirs.filter(v=>v===b).length).pop() : '—';
      console.log(`Model: high=${modelHigh}°F low=${modelLow}°F wind=${domDir}`);
      return { modelHigh, modelLow, domDir, hourly };
    } catch(e) {
      console.warn(`Open-Meteo attempt ${attempt}/3 failed: ${e.message}`);
      if (attempt < 3) await sleep(30000);
      else throw new Error(`Open-Meteo failed after 3 attempts: ${e.message}`);
    }
  }
}

// ── Snapshot Job ──────────────────────────────────────────────────────────────

async function runSnapshotJob() {
  const forecastDate = getTodayEastern();
  console.log(`=== Snapshot Job — ${forecastDate} ===`);
  const nwsData = await fetchNWS(forecastDate);
  const modelData = await fetchModel(forecastDate);
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
    if (res.status === 409) {
      console.log(`Snapshot for ${forecastDate} already exists — skipping`);
    } else {
      throw new Error(`Snapshot save failed: ${res.status} - ${err}`);
    }
  } else {
    console.log(`✅ Snapshot saved for ${forecastDate}`);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Time: ${new Date().toISOString()}`);
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');

  const jobType = detectJobType();
  console.log(`Job type: ${jobType}`);

  if (jobType === 'snapshot') {
    await runSnapshotJob();
  } else if (jobType === 'score') {
    await runScoreJob();
  } else {
    console.log(`Unknown job type: ${jobType} — nothing to do`);
  }

  console.log('=== Done ===');
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
