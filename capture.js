// capture.js
// Runs via GitHub Actions on multiple schedules:
//   05:00 UTC (1AM EDT)  — snapshot + score yesterday
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

// ── Helpers ──────────────────────────────────────────────────────────────────

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

function getLSTWindow(dateStr) {
  const start = new Date(`${dateStr}T01:00:00`);
  const end = new Date(`${dateStr}T00:59:59`);
  end.setDate(end.getDate() + 1);
  return { start, end };
}

// Detect which job this is based on current UTC hour
// Returns 'snapshot', 'score', or 'dsm'
function detectJobType() {
  const utcHour = new Date().getUTCHours();
  const utcMin = new Date().getUTCMinutes();
  const utcDecimal = utcHour + utcMin / 60;
  // Snapshot cron: 0 5 * * * (05:00 UTC = 1AM EDT), allow up to 90min late
  if (utcDecimal >= 4.5 && utcDecimal < 6.5) return 'snapshot';
  // Score cron: 0 7 * * * (07:00 UTC = 3AM EDT), allow up to 90min late
  if (utcDecimal >= 6.5 && utcDecimal < 9.0) return 'score';
  return 'dsm';
}

// ── DSM ───────────────────────────────────────────────────────────────────────

function parseDSMText(text) {
  if (!text) return null;

  // forecast.weather.gov returns HTML — extract the pre tag content
  // tgftp returns raw text directly
  let raw = text;
  if (text.includes('<html') || text.includes('<HTML')) {
    // Extract content from <pre> tag which contains the raw product text
    const preMatch = text.match(/<pre[^>]*>([\s\S]*?)<\/pre>/i);
    if (preMatch) {
      raw = preMatch[1].replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>');
    } else {
      // Try to find DSMNYC directly in the HTML
      const dsmIdx = text.indexOf('DSMNYC');
      if (dsmIdx === -1) return null;
      raw = text.slice(dsmIdx - 100, dsmIdx + 500);
    }
  }

  if (!raw.includes('DSMNYC')) return null;

  // Parse issuance timestamp from header e.g. "CXUS41 KOKX 011500"
  const headerMatch = raw.match(/CXUS41 KOKX (\d{2})(\d{2})(\d{2})/);
  if (!headerMatch) return null;
  const issuanceUTCHour = parseInt(headerMatch[2]);
  const issuanceUTCMin = parseInt(headerMatch[3]);

  // Parse high/low from DSM data line
  const dataMatch = raw.match(/KNYC DS (\d{4}) (\d{2})\/(\d{2})\s+(\d+)(\d{4})\/\s*(\d+)(\d{4})/);
  if (!dataMatch) return null;

  const high = parseInt(dataMatch[4]);
  const highTimeLST = dataMatch[5]; // HHMM in LST
  const low = parseInt(dataMatch[6]);
  const lowTimeLST = dataMatch[7];

  // Convert LST times to EDT (LST + 1hr during DST)
  const highH = parseInt(highTimeLST.slice(0, 2)) + 1;
  const highM = highTimeLST.slice(2, 4);
  const lowH = parseInt(lowTimeLST.slice(0, 2)) + 1;
  const lowM = lowTimeLST.slice(2, 4);

  const highTimeStr = `${String(highH).padStart(2, '0')}:${highM} EDT`;
  const lowTimeStr = `${String(lowH).padStart(2, '0')}:${lowM} EDT`;

  // Build issuance UTC string for comparison (HHMM as zero-padded string)
  const issuanceKey = `${String(issuanceUTCHour).padStart(2, '0')}${String(issuanceUTCMin).padStart(2, '0')}`;

  return { high, low, highTimeStr, lowTimeStr, issuanceKey, issuanceUTCHour, issuanceUTCMin };
}

async function getLastDSMIssuanceKey(date) {
  // Read most recent DSM entry for today from Supabase
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

  if (!res.ok) {
    const err = await res.text();
    throw new Error(`DSM save failed: ${res.status} - ${err}`);
  }

  console.log(`✅ DSM saved: high=${parsed.high}°F @ ${parsed.highTimeStr}, low=${parsed.low}°F @ ${parsed.lowTimeStr} (issuance key: ${parsed.issuanceKey})`);
  return record;
}

async function runDSMJob() {
  const date = getTodayEastern();
  console.log(`=== DSM Check Job — ${date} ===`);

  // Get the last issuance key we already have for today
  const lastKey = await getLastDSMIssuanceKey(date);
  console.log(`Last known DSM issuance key: ${lastKey || 'none'}`);

  let attempt = 0;

  while (attempt < DSM_MAX_RETRIES) {
    attempt++;
    console.log(`Attempt ${attempt}/${DSM_MAX_RETRIES} — fetching DSM...`);

    try {
      let text = null;

      // Try each DSM URL in order until one works
      for (const url of DSM_URLS) {
        try {
          const res = await fetch(url, {
            headers: { 'User-Agent': 'nyc-edge-snapshots/1.0', 'Cache-Control': 'no-cache' }
          });
          if (!res.ok) {
            console.warn(`  ${url} returned ${res.status}`);
            continue;
          }
          const t = await res.text();
          if (t && t.includes('DSMNYC')) {
            console.log(`  Got DSM from: ${url}`);
            text = t;
            break;
          }
        } catch(urlErr) {
          console.warn(`  ${url} failed: ${urlErr.message}`);
        }
      }

      if (!text) {
        console.warn('All DSM URLs failed this attempt, retrying in 5 minutes...');
        await sleep(DSM_RETRY_INTERVAL_MS);
        continue;
      }
      const parsed = parseDSMText(text);

      if (!parsed) {
        console.warn('Could not parse DSM text, retrying in 5 minutes...');
        await sleep(DSM_RETRY_INTERVAL_MS);
        continue;
      }

      console.log(`DSM issuance key: ${parsed.issuanceKey} (last known: ${lastKey || 'none'})`);

      if (parsed.issuanceKey === lastKey) {
        console.log('DSM not updated yet, retrying in 5 minutes...');
        await sleep(DSM_RETRY_INTERVAL_MS);
        continue;
      }

      // New DSM — save it
      await saveDSM(date, parsed);

      // Send GFS notification on the 7AM DSM job — this is when the 06Z run
      // is confirmed available, the most actionable pre-market update
      const utcHour = new Date().getUTCHours();
      if (utcHour >= 11 && utcHour <= 12) {
        await sendGFSNotification(date);
      }

      console.log('DSM job complete.');
      return;

    } catch(e) {
      console.warn(`Attempt ${attempt} failed: ${e.message}`);
      if (attempt < DSM_MAX_RETRIES) {
        console.log('Retrying in 5 minutes...');
        await sleep(DSM_RETRY_INTERVAL_MS);
      }
    }
  }

  console.error(`❌ DSM job exhausted ${DSM_MAX_RETRIES} retries without finding a new issuance. NWS may be delayed or down.`);
}

// ── Email Notification ────────────────────────────────────────────────────────

async function sendGFSNotification(forecastDate) {
  const GMAIL_USER = process.env.GMAIL_USER;
  const GMAIL_PASS = process.env.GMAIL_APP_PASSWORD;
  const NOTIFY_EMAIL = process.env.NOTIFY_EMAIL;

  if (!GMAIL_USER || !GMAIL_PASS || !NOTIFY_EMAIL) {
    console.log('Email credentials not configured, skipping notification');
    return;
  }

  // Get latest forecast data from Supabase to include in email
  let snapInfo = '';
  try {
    const res = await fetch(
      `${SUPABASE_URL}/rest/v1/snapshots?order=date.desc&limit=1`,
      { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
    );
    if (res.ok) {
      const rows = await res.json();
      if (rows.length) {
        const s = rows[0];
        const div = s.model_high != null && s.nws_high != null ? s.model_high - s.nws_high : null;
        snapInfo = `
Forecast for ${s.date}:
  NWS High: ${s.nws_high}°F
  Model High: ${s.model_high}°F
  Divergence: ${div != null ? (div > 0 ? '+' : '') + div + '°F' : '—'}
  Regime: ${s.wind_regime || '—'}`;
      }
    }
  } catch(e) {
    console.warn('Could not fetch snapshot for email:', e.message);
  }

  // Determine which GFS run just became available
  const utcHour = new Date().getUTCHours();
  const runZ = Math.floor((utcHour - 2) / 6) * 6; // approximate
  const runLabel = `${String(Math.max(0, runZ)).padStart(2, '0')}Z`;

  const subject = `GFS ${runLabel} Now Live — NYC Edge`;
  const body = `GFS ${runLabel} run is now fully available on Open-Meteo.${snapInfo}

Act now if you have a position to take.

-- NYC Edge Pipeline`;

  // Send via Gmail SMTP using nodemailer
  try {
    const nodemailer = await import('nodemailer');
    const transporter = nodemailer.default.createTransport({
      service: 'gmail',
      auth: { user: GMAIL_USER, pass: GMAIL_PASS }
    });
    await transporter.sendMail({
      from: GMAIL_USER,
      to: NOTIFY_EMAIL,
      subject,
      text: body
    });
    console.log(`✅ GFS notification email sent to ${NOTIFY_EMAIL}`);
  } catch(e) {
    console.warn('Email send failed:', e.message);
  }
}

async function fetchNWS(forecastDate) {
  console.log('Fetching NWS forecast...');

  const pointsRes = await fetch(NWS_POINT, {
    headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' }
  });
  if (!pointsRes.ok) throw new Error(`NWS points API failed: ${pointsRes.status}`);
  const pointsData = await pointsRes.json();
  const forecastHourlyUrl = pointsData.properties.forecastHourly;

  const forecastRes = await fetch(forecastHourlyUrl, {
    headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' }
  });
  if (!forecastRes.ok) throw new Error(`NWS hourly forecast failed: ${forecastRes.status}`);
  const forecastData = await forecastRes.json();

  const periods = forecastData.properties.periods;
  const { start, end } = getLSTWindow(forecastDate);

  const windowPeriods = periods.filter(p => {
    const t = new Date(p.startTime);
    return t >= start && t <= end;
  });

  const targetPeriods = windowPeriods.length ? windowPeriods :
    periods.filter(p => new Date(p.startTime).toISOString().slice(0,10) === forecastDate);

  let nwsHigh = null, nwsLow = null;
  const hourly = [];

  targetPeriods.forEach(p => {
    const temp = p.temperature;
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
    '&daily=temperature_2m_max,temperature_2m_min',
    '&temperature_unit=fahrenheit',
    '&windspeed_unit=mph',
    '&forecast_days=5',
    '&timezone=America%2FNew_York'
  ].join('');

  // Retry up to 3 times with 30s delay — Open-Meteo can be flaky at 1AM
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`Open-Meteo returned ${res.status}`);
      const data = await res.json();

      const nextDate = new Date(forecastDate + 'T12:00:00');
      nextDate.setDate(nextDate.getDate() + 1);
      const nextDateStr = nextDate.toISOString().slice(0, 10);

      // Use daily max/min as model high/low (captures intra-hour peaks)
      const dateIdx = data.daily.time.indexOf(forecastDate);
      const modelHigh = dateIdx >= 0 ? Math.round(data.daily.temperature_2m_max[dateIdx]) : null;
      const modelLow = dateIdx >= 0 ? Math.round(data.daily.temperature_2m_min[dateIdx]) : null;

      const hourly = [];
      data.hourly.time.forEach((t, i) => {
        const tDate = t.slice(0, 10);
        const tHour = parseInt(t.slice(11, 13));
        const inWindow = (tDate === forecastDate && tHour >= 1) ||
                         (tDate === nextDateStr && tHour === 0);
        if (inWindow) {
          hourly.push({
            time: t,
            temp: Math.round(data.hourly.temperature_2m[i]),
            wind_dir: windDegToDir(data.hourly.winddirection_10m[i]),
            wind_speed: data.hourly.windspeed_10m[i],
            cloud: data.hourly.cloudcover[i],
            dew: Math.round(data.hourly.dewpoint_2m[i]),
            pop: data.hourly.precipitation_probability[i]
          });
        }
      });

      const afHours = data.hourly.time
        .map((t, i) => ({ t, i, hour: parseInt(t.slice(11,13)), date: t.slice(0,10) }))
        .filter(({date, hour}) => date === forecastDate && hour >= 13 && hour <= 17);

      const afDirs = afHours.map(({i}) => windDegToDir(data.hourly.winddirection_10m[i])).filter(d => d !== '—');
      const domDir = afDirs.length ?
        afDirs.sort((a,b) => afDirs.filter(v=>v===a).length - afDirs.filter(v=>v===b).length).pop()
        : '—';

      console.log(`Model: high=${modelHigh}°F low=${modelLow}°F wind=${domDir}`);
      return { modelHigh, modelLow, domDir, hourly };

    } catch(e) {
      console.warn(`Open-Meteo attempt ${attempt}/3 failed: ${e.message}`);
      if (attempt < 3) {
        console.log('Retrying in 30 seconds...');
        await sleep(30000);
      } else {
        throw new Error(`Open-Meteo failed after 3 attempts: ${e.message}`);
      }
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
    throw new Error(`Supabase save failed: ${res.status} - ${err}`);
  }

  console.log(`✅ Snapshot saved for ${forecastDate}`);
  return snapshot;
}

// ── Score Yesterday ───────────────────────────────────────────────────────────

async function scoreYesterday() {
  const now = new Date();
  const eastern = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  eastern.setDate(eastern.getDate() - 1);
  const y = eastern.getFullYear();
  const m = String(eastern.getMonth() + 1).padStart(2, '0');
  const d = String(eastern.getDate()).padStart(2, '0');
  const yesterday = `${y}-${m}-${d}`;

  console.log(`Attempting to score ${yesterday}...`);

  // Check if already scored
  const checkRes = await fetch(
    `${SUPABASE_URL}/rest/v1/scored_days?date=eq.${yesterday}`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
  );
  const existing = await checkRes.json();
  if (existing.length > 0) {
    console.log(`${yesterday} already scored, skipping`);
    return;
  }

  // Get yesterday's snapshot
  const snapRes = await fetch(
    `${SUPABASE_URL}/rest/v1/snapshots?date=eq.${yesterday}`,
    { headers: { 'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`, 'apikey': SUPABASE_SERVICE_KEY } }
  );
  const snaps = await snapRes.json();
  if (!snaps.length) {
    console.log(`No snapshot found for ${yesterday}, cannot score`);
    return;
  }
  const snap = snaps[0];

  // Fetch CLI — the official ground truth Kalshi resolves against
  const CLI_URL = 'https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC';
  let cliText = null;
  try {
    const cliRes = await fetch(CLI_URL, { headers: { 'User-Agent': 'nyc-edge-snapshots/1.0' } });
    cliText = await cliRes.text();
  } catch(e) {
    console.log('CLI fetch failed:', e.message);
    return;
  }

  const dateMatch = cliText.match(/CLIMATE SUMMARY FOR (\w+ \d+ \d+)/);
  const maxMatch = cliText.match(/MAXIMUM\s+(\d+)\s/);
  const minMatch = cliText.match(/MINIMUM\s+(\d+)\s/);

  if (!dateMatch || !maxMatch || !minMatch) {
    console.log('Could not parse CLI report — may not be posted yet');
    return;
  }

  const cliDate = new Date(dateMatch[1] + ' 12:00:00 EDT').toISOString().slice(0, 10);
  if (cliDate !== yesterday) {
    console.log(`CLI date ${cliDate} doesn't match yesterday ${yesterday} — not posted yet`);
    return;
  }

  const actualHigh = parseInt(maxMatch[1]);
  const actualLow = parseInt(minMatch[1]);

  console.log(`CLI confirmed: high=${actualHigh}°F low=${actualLow}°F — this is the official Kalshi resolution value`);

  // Save to cli_reports — permanent official record
  const cliRecord = {
    date: yesterday,
    captured_at: new Date().toISOString(),
    actual_high: actualHigh,
    actual_low: actualLow
  };
  const cliSaveRes = await fetch(`${SUPABASE_URL}/rest/v1/cli_reports`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${SUPABASE_SERVICE_KEY}`,
      'apikey': SUPABASE_SERVICE_KEY,
      'Prefer': 'resolution=merge-duplicates'
    },
    body: JSON.stringify(cliRecord)
  });
  if (!cliSaveRes.ok) {
    console.warn('cli_reports save failed:', await cliSaveRes.text());
  } else {
    console.log(`✅ CLI report saved to cli_reports for ${yesterday}`);
  }

  // Score against snapshot
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
    throw new Error(`Score save failed: ${saveRes.status} - ${err}`);
  }

  console.log(`✅ Scored ${yesterday}: actual high=${actualHigh}°F low=${actualLow}°F`);
  console.log(`   Model high err: ${scored.model_high_err}°F | NWS high err: ${scored.nws_high_err}°F`);
  console.log(`   Winner high: ${scored.winner_high} | Winner low: ${scored.winner_low}`);
}

// ── Score Job (3AM EDT) ───────────────────────────────────────────────────────

async function runScoreJob() {
  console.log('=== Score Job (3AM — CLI final verdict) ===');
  try {
    await scoreYesterday();
  } catch(e) {
    // Log but don't exit with code 1 — a scoring miss is not fatal
    console.warn('Scoring failed (non-fatal):', e.message);
  }
}

// ── Snapshot Job ──────────────────────────────────────────────────────────────

async function runSnapshotJob() {
  const forecastDate = getTodayEastern();
  console.log(`=== Snapshot Job — ${forecastDate} ===`);

  // Attempt scoring — CLI may not be posted yet at 1AM, that's ok
  // The 3AM score job will catch it reliably
  try {
    await scoreYesterday();
  } catch(e) {
    console.error('Scoring failed:', e.message);
  }

  try {
    const nwsData = await fetchNWS(forecastDate);
    const modelData = await fetchModel(forecastDate);
    await saveSnapshot(forecastDate, nwsData, modelData);
  } catch(e) {
    console.error('Snapshot failed:', e.message);
    process.exit(1);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log(`Time: ${new Date().toISOString()}`);

  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_KEY');
  }

  const jobType = detectJobType();
  console.log(`Job type detected: ${jobType}`);

  if (jobType === 'snapshot') {
    await runSnapshotJob();
  } else if (jobType === 'score') {
    await runScoreJob();
  } else {
    await runDSMJob();
  }

  console.log('=== Done ===');
}

main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
