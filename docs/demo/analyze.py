import json, math
import pandas as pd

MIN_SAMPLES, Z, REBASE = 30, 3.0, 10

def sev(z):
    m = abs(z)
    return 'CRITICAL' if m >= 15 else 'HIGH' if m >= 10 else 'MEDIUM' if m >= 5 else 'LOW'

rows = []
for line in open('readings.jsonl'):
    key, js = line.strip().split('|', 1)
    r = json.loads(js)
    rows.append((r['sensorId'], pd.Timestamp(r['timestamp']), r['value'], r['sequenceNumber']))
readings = pd.DataFrame(rows, columns=['sensor', 'ts', 'value', 'seq']).sort_values(['sensor', 'seq']).reset_index(drop=True)

def replay(pop_std):
    out, trace = [], []
    for sensor, g in readings.groupby('sensor'):
        n, mean, m2, streak = 0, 0.0, 0.0, 0
        for _, r in g.iterrows():
            v = r.value
            sd = 0.0
            if n > 1:
                sd = math.sqrt(m2 / (n if pop_std else n - 1))
            scored = n >= MIN_SAMPLES and sd > 0
            z = (v - mean) / sd if sd > 0 else float('nan')
            anomaly = scored and abs(z) > Z
            trace.append((sensor, r.ts, r.seq, v, mean, sd, z if scored else float('nan'), scored, bool(anomaly), streak))
            if not anomaly:
                n += 1; d = v - mean; mean += d / n; m2 += d * (v - mean); streak = 0
            else:
                streak += 1
                out.append((sensor, r.ts, v, mean, sd, z, streak, sev(z)))
                if streak >= REBASE:
                    n, mean, m2, streak = 1, v, 0.0, 0
    return pd.DataFrame(out, columns=['sensor_id', 'ts', 'value', 'baseline_mean', 'baseline_std_dev', 'z_score', 'consecutive', 'severity']), \
           pd.DataFrame(trace, columns=['sensor', 'ts', 'seq', 'value', 'mean', 'sd', 'z', 'scored', 'anomaly', 'streak'])

alerts = pd.read_csv('alerts.csv')
alerts['ts'] = pd.to_datetime(alerts['reading_timestamp'], utc=True).dt.tz_convert('UTC')
for pop in (False, True):
    rep, trace = replay(pop)
    a = alerts.sort_values(['sensor_id', 'ts']).reset_index(drop=True)
    b = rep.sort_values(['sensor_id', 'ts']).reset_index(drop=True)
    same_keys = len(a) == len(b) and (a.sensor_id == b.sensor_id).all() and (a.ts.values == b.ts.values).all()
    maxdz = (a.z_score - b.z_score).abs().max() if same_keys else None
    print(f'population std={pop}: replay alerts={len(b)} vs stored={len(a)}  same keys={same_keys}  max |dz|={maxdz}')
    if same_keys and maxdz < 1e-9:
        best = (rep, trace); break
rep, trace = best
trace.to_csv('trace.csv', index=False)

gt = pd.read_csv('ground-truth.csv')
gt['from'] = pd.to_datetime(gt['from'], utc=True); gt['to'] = pd.to_datetime(gt['to'], utc=True)
res = []
inwin = pd.Series(False, index=alerts.index)
for i, f in gt.iterrows():
    m = (alerts.sensor_id == f.sensor_id) & (alerts.ts >= f['from']) & (alerts.ts <= f['to'])
    inwin |= m
    hit = alerts[m]
    delay = (hit.ts.min() - f['from']).total_seconds() if len(hit) else None
    peak = None
    if len(hit):
        order = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        peak = max(hit.severity, key=order.index)
    res.append(dict(sensor=f.sensor_id, type=f.fault_type, sigmas=f.magnitude_sigmas, duration_s=(f['to'] - f['from']).total_seconds() + 0.5,
                    detected=len(hit) > 0, delay_s=delay, alerts=len(hit), peak=peak))
res = pd.DataFrame(res)
print(res.to_string())
fp = alerts[~inwin]
print('\nalerts total', len(alerts), 'in fault windows', int(inwin.sum()), 'outside windows (false positives)', len(fp))
print(fp[['sensor_id', 'reading_timestamp', 'z_score', 'severity']].to_string())

# false-positive rate: scored readings outside any fault window
tr = trace.copy()
tr['in_fault'] = False
for _, f in gt.iterrows():
    tr.loc[(tr.sensor == f.sensor_id) & (tr.ts >= f['from']) & (tr.ts <= f['to']), 'in_fault'] = True
quiet_scored = tr[tr.scored & ~tr.in_fault]
print('\nscored readings total', int(tr.scored.sum()), ' quiet scored readings', len(quiet_scored),
      ' FP', len(fp), ' FP rate %.3f%%' % (100 * len(fp) / len(quiet_scored)), ' expected for |z|>3 normal: 0.270%')
print('unscored readings (warm-up or post-rebaseline):', int((~tr.scored).sum()))
res.to_csv('scorecard.csv', index=False)
fp.to_csv('false_positives.csv', index=False)
