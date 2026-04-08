// capture.js
// Runs via GitHub Actions on multiple schedules:
//   05:00 UTC (1AM EDT)  — snapshot + score yesterday (with CLI retry) + 1AM DSM
//   11:10 UTC (7:10AM EDT)  — DSM check
//   17:10 UTC (1:10PM EDT)  — DSM check
//   20:10 UTC (4:10PM EDT)  — DSM check
//   23:10 UTC (7:10PM EDT)  — DSM check

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const NWS_POINT = 'https://api.weather.gov/points/40.781,-73.967';
const DSM_URLS = [
  'https://tgftp.nws.noaa.gov/data/raw/cd/cdus41.kokx.dsm.txt',
  'https://forecast.weather.gov/product.php?site=NWS&product=DSM&issuedby=NYC',
  'https://mesonet.agron.iastate.edu/wx/afos/p.php?pil=DSMOKX&fmt=text'
];

const DSM_MAX_RETRIES = 12;
const DSM_RETRY_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

// CLI retry config — CLI typically posts between 1AM and 3AM EDT
// We retry for up to 2 hours before giving up
const CLI_MAX_RETRIES = 24;
const CLI_RETRY_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes

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

// Job type: use JOB_TYPE env var if set, otherwise detect from UTC hour
function detectJobType() {
  if (process.env.JOB_TYPE) {
    const jt = process.env.JOB_TYPE.toLowerCase().trim();
    console.log(`Job type from env: ${jt}`);
    return jt;
  }
  const utcHour = new Date().getUTCHours();
  const utcMin = new Date().getUTCMinutes();
  const utcDecimal = utcHour + utcMin / 60;
  if (utcDecimal >= 4.5 && utcDecimal < 8.0) return 'snapshot'; // 1AM EDT window — handles both 1AM and any delayed 3AM
  return 'dsm';
}

// ── CLI ───────────────────────────────────────────────────────────────────────

const CLI_URL = 'https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC';

function parseCLI(text, expectedDate) {
  const dateMatch = text.match(/CLIMATE SUMMARY FOR (\w+ \d+ \d+)/);
  const maxMatch = text.match(/MAXIMUM\s+(\d+)\s/);
  const minMatch = text.match(/MINIMUM\s+(\d+)\s/);
  if (!dateMatch || !maxMatch || !minMatch) return null;
  const cliDate = new Date(dateMatch[1] + ' 12:00:00 EDT').toISOString().slice(0, 10);
  if (cliDate !== expectedDate) return null;
  return { date: cliDate, high: parseInt(maxMatch[1]), low: parseInt(minMatch[1]) };
}

// Fetch CLI with retry loop — retries every 5 min for up to 2 hours
async function fetchCLIWithRetry(expectedDate) {
  console.log(`Fetching CLI for ${expectedDate} (will retry up to ${CLI_MAX_RETRIES}x)...`);
  for (let attempt = 1; attempt <= CLI_MAX_RETRIES; attempt++) {
    try {
      const res = await fetch(CLI_URL, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0', 'Cache-Control': 'no-cache' } });
      if (!res.ok) {
        console.warn(`CLI fetch returned ${res.status}, retrying...`);
      } else {
        const text = await res.text();
        const parsed = parseCLI(text, expectedDate);
        if (parsed) {
          console.log(`✅ CLI found: high=${parsed.high}°F low=${parsed.low}°F`);
          return parsed;
        } else {
          // Check what date the CLI has
          const dateMatch = text.match(/CLIMATE SUMMARY FOR (\w+ \d+ \d+)/);
          const cliDate = dateMatch ? new Date(dateMatch[1] + ' 12:00:00 EDT').toISOString().slice(0, 10) : 'unknown';
          console.log(`CLI date is ${cliDate}, waiting for ${expectedDate} (attempt ${attempt}/${CLI_MAX_RETRIES})`);
        }
      }
    } catch(e) {
      console.warn(`CLI fetch attempt ${attempt} failed: ${e.message}`);
    }
    if (attempt < CLI_MAX_RETRIES) {
      console.log(`Retrying CLI in 5 minutes...`);
      await sleep(CLI_RETRY_INTERVAL_MS);
    }
  }
  console.error(`❌ CLI not available for ${expectedDate} after ${CLI_MAX_RETRIES} attempts`);
  return null;
}

// ── Score Yesterday ───────────────────────────────────────────────────────────

async function scoreYesterday(withRetry = false) {
  const yesterday = getYesterdayEastern();
  console.log(`Attempting to score ${yesterday}...`);

  // Check if already scored
  const checkRes = await fetch(
    `${SUPABASE_URL}/rest/v1/scored_days?date=eq.${yesterday}`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
  );
  const existing = await checkRes.json();
  if (existing.length > 0) {
    console.log(`${yesterday} already scored, skipping`);
    return true;
  }

  // Get yesterday's snapshot
  const snapRes = await fetch(
    `${SUPABASE_URL}/rest/v1/snapshots?date=eq.${yesterday}`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
  );
  const snaps = await snapRes.json();
  if (!snaps.length) {
    console.log(`No snapshot found for ${yesterday}, cannot score`);
    return false;
  }
  const snap = snaps[0];

  // Fetch CLI — with or without retry depending on caller
  const cli = withRetry
    ? await fetchCLIWithRetry(yesterday)
    : await (async () => {
        try {
          const res = await fetch(CLI_URL, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0', 'Cache-Control': 'no-cache' } });
          const text = await res.text();
          return parseCLI(text, yesterday);
        } catch(e) { return null; }
      })();

  if (!cli) {
    console.log(`CLI not available for ${yesterday} — scoring skipped`);
    return false;
  }

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
  console.log(`✅ CLI saved for ${yesterday}: high=${cli.high}°F low=${cli.low}°F`);

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
  return true;
}

// ── DSM ───────────────────────────────────────────────────────────────────────

function parseDSMText(text) {
  if (!text) return null;
  let raw = text;
  if (text.includes('<html') || text.includes('<HTML')) {
    const preMatch = text.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
    if (preMatch) {
      raw = preMatch[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>');
    } else {
      const dsmIdx = text.indexOf('DSMNYC');
      if (dsmIdx === -1) return null;
      raw = text.slice(dsmIdx - 100, dsmIdx + 500);
    }
  }
  if (!raw.includes('DSMNYC')) return null;
  const headerMatch = raw.match(/CXUS41 KOKX (\d{2})(\d{2})(\d{2})/);
  if (!headerMatch) return null;
  const issuanceUTCHour = parseInt(headerMatch[2]);
  const issuanceUTCMin = parseInt(headerMatch[3]);
  const dataMatch = raw.match(/KNYC DS (\d{4}) (\d{2})\/(\d{2})\s+(\d+)(\d{4})\/\s*(\d+)(\d{4})/);
  if (!dataMatch) return null;
  const high = parseInt(dataMatch[4]);
  const highTimeLST = dataMatch[5];
  const low = parseInt(dataMatch[6]);
  const lowTimeLST = dataMatch[7];
  const highH = parseInt(highTimeLST.slice(0, 2)) + 1;
  const highM = highTimeLST.slice(2, 4);
  const lowH = parseInt(lowTimeLST.slice(0, 2)) + 1;
  const lowM = lowTimeLST.slice(2, 4);
  const highTimeStr = `${String(highH).padStart(2, '0')}:${highM} EDT`;
  const lowTimeStr = `${String(lowH).padStart(2, '0')}:${lowM} EDT`;
  const issuanceKey = `${String(issuanceUTCHour).padStart(2, '0')}${String(issuanceUTCMin).padStart(2, '0')}`;
  return { high, low, highTimeStr, lowTimeStr, issuanceKey, issuanceUTCHour, issuanceUTCMin };
}

async function getLastDSMIssuanceKey(date) {
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/dsm_reports?date=eq.${date}&order=captured_at.desc&limit=1`,
      { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
    );
    if (!res.ok) return null;
    const rows = await res.json();
    return rows.length ? rows[0].issuance_key : null;
  } catch(e) {
    console.warn('Could not read last DSM from Supabase:', e.message);
    return null;
  }
}

async function saveDSM(date, parsed) {
  const record = {
    date,
    captured_at: new Date().toISOString(),
    high: parsed.high,
    low: parsed.low,
    high_time_str: parsed.highTimeStr,
    low_time_str: parsed.lowTimeStr,
    issuance_key: parsed.issuanceKey
  };
  const res = await fetch(`${SUPABASE_URL}/rest/v1/dsm_reports`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'apikey': SUPABASE_SERVICE_KEY,
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify(record)
  });
  if (!res.ok) throw new Error(`DSM save failed: ${res.status} - ${await res.text()}`);
  console.log(`✅ DSM saved: high=${parsed.high}°F @ ${parsed.highTimeStr}, low=${parsed.low}°F @ ${parsed.lowTimeStr}`);
  return record;
}

async function runDSMJob() {
  const date = getTodayEastern();
  console.log(`=== DSM Check Job — ${date} ===`);
  const lastKey = await getLastDSMIssuanceKey(date);
  console.log(`Last known DSM issuance key: ${lastKey || 'none'}`);
  let attempt = 0;
  while (attempt < DSM_MAX_RETRIES) {
    attempt++;
    console.log(`Attempt ${attempt}/${DSM_MAX_RETRIES} — fetching DSM...`);
    try {
      let text = null;
      for (const url of DSM_URLS) {
        try {
          const res = await fetch(url, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0', 'Cache-Control': 'no-cache' } });
          if (!res.ok) { console.warn(`  ${url} returned ${res.status}`); continue; }
          const t = await res.text();
          if (t && t.includes('DSMNYC')) { console.log(`  Got DSM from: ${url}`); text = t; break; }
        } catch(urlErr) { console.warn(`  ${url} failed: ${urlErr.message}`); }
      }
      if (!text) { console.warn('All DSM URLs failed, retrying in 5 minutes...'); await sleep(DSM_RETRY_INTERVAL_MS); continue; }
      const parsed = parseDSMText(text);
      if (!parsed) { console.warn('Could not parse DSM, retrying in 5 minutes...'); await sleep(DSM_RETRY_INTERVAL_MS); continue; }
      console.log(`DSM issuance key: ${parsed.issuanceKey} (last: ${lastKey || 'none'})`);
      if (parsed.issuanceKey === lastKey) { console.log('DSM not updated yet, retrying in 5 minutes...'); await sleep(DSM_RETRY_INTERVAL_MS); continue; }
      await saveDSM(date, parsed);
      console.log('DSM job complete.');
      return;
    } catch(e) {
      console.warn(`Attempt ${attempt} failed: ${e.message}`);
      if (attempt < DSM_MAX_RETRIES) { await sleep(DSM_RETRY_INTERVAL_MS); }
    }
  }
  console.error(`❌ DSM job exhausted ${DSM_MAX_RETRIES} retries.`);
}

// ── NWS ───────────────────────────────────────────────────────────────────────

async function fetchNWS(forecastDate) {
  console.log('Fetching NWS forecast...');
  const pointsRes = await fetch(NWS_POINT, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' } });
  if (!pointsRes.ok) throw new Error(`NWS points API failed: ${pointsRes.status}`);
  const pointsData = await pointsRes.json();
  const forecastRes = await fetch(pointsData.properties.forecastHourly, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' } });
  if (!forecastRes.ok) throw new Error(`NWS hourly forecast failed: ${forecastRes.status}`);
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
      if (attempt < 3) { await sleep(30000); } else { throw new Error(`Open-Meteo failed: ${e.message}`); }
    }
  }
}

// ── Save Snapshot ─────────────────────────────────────────────────────────────

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
      console.log(`Snapshot for ${forecastDate} already exists — skipping save, continuing to scoring`);
    } else {
      throw new Error(`Supabase save failed: ${res.status} - ${err}`);
    }
  } else {
    console.log(`✅ Snapshot saved for ${forecastDate}`);
  }
  return snapshot;
}

// ── Snapshot Job (1AM EDT) ────────────────────────────────────────────────────
// Does everything: score yesterday (with CLI retry), take today's snapshot, check 1AM DSM

async function runSnapshotJob() {
  const forecastDate = getTodayEastern();
  console.log(`=== Snapshot Job — ${forecastDate} ===`);

  // Step 1: Take today's snapshot FIRST — locks in 1AM forecast values
  // before the CLI retry loop potentially delays us by 2 hours
  console.log('--- Step 1: Take snapshot ---');
  try {
    const nwsData = await fetchNWS(forecastDate);
    const modelData = await fetchModel(forecastDate);
    await saveSnapshot(forecastDate, nwsData, modelData);
  } catch(e) {
    console.error('Snapshot failed (non-fatal for scoring):', e.message);
  }

  // Step 2: Score yesterday with CLI retry loop
  // CLI typically posts between 1AM and 3AM EDT — we wait up to 2 hours
  console.log('--- Step 2: Score yesterday ---');
  try {
    const scored = await scoreYesterday(true); // true = use retry loop
    if (!scored) console.log('Scoring did not complete — CLI may not have posted in time');
  } catch(e) {
    console.error('Scoring error (non-fatal):', e.message);
  }

  // Step 3: Check 1AM DSM (quick, no retry — just grab whatever is there)
  console.log('--- Step 3: 1AM DSM check ---');
  try {
    const lastKey = await getLastDSMIssuanceKey(forecastDate);
    for (const url of DSM_URLS) {
      try {
        const res = await fetch(url, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0', 'Cache-Control': 'no-cache' } });
        if (!res.ok) continue;
        const text = await res.text();
        if (!text.includes('DSMNYC')) continue;
        const parsed = parseDSMText(text);
        if (!parsed) continue;
        if (parsed.issuanceKey === lastKey) { console.log('No new DSM at 1AM — DSM jobs will pick it up later'); break; }
        await saveDSM(forecastDate, parsed);
        break;
      } catch(e) { continue; }
    }
  } catch(e) {
    console.warn('1AM DSM check failed (non-fatal):', e.message);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Time: ${new Date().toISOString()}`);
  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');

  const jobType = detectJobType();
  console.log(`Job type detected: ${jobType}`);

  if (jobType === 'snapshot') {
    await runSnapshotJob();
  } else {
    await runDSMJob();
  }

  console.log('=== Done ===');
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
