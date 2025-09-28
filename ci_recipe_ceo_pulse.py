import pandas as pd, io, json, base64, matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 1) Load CSVs from uploaded files
companies = pd.read_csv('companies.csv')
deals = pd.read_csv('deals.csv', parse_dates=['createdate','closedate','hs_lastactivitydate'])
contacts = pd.read_csv('contacts.csv')
activities = pd.read_csv('activities.csv', parse_dates=['date'])

# 2) Window
now = pd.Timestamp.utcnow()
week_ago = now - pd.Timedelta(days=7)
month_ago = now - pd.Timedelta(days=30)

# 3) KPIs (weekly defaults; adapt if TIMEFRAME == 'month')
window_start = week_ago
new_deals = deals[deals['createdate'] >= window_start]
pipeline_created = float(new_deals['amount'].sum())
late_stages = deals[deals['dealstage'].isin(['deciderboughtin','contractsent','presentationscheduled','closedwon'])]
forecast = float(late_stages[late_stages['closedate'].isna()]['amount'].sum())
new_contacts = contacts[contacts['lifecyclestage'].str.lower().isin(['mql','sql'])]  # synthetic heuristic
sql_rate = round(len(new_deals) / max(1,len(new_contacts)), 2)
won = deals[(~deals['closedate'].isna()) & (deals['dealstage'] == 'closedwon')]
if len(won):
    cycle = (won['closedate'] - won['createdate']).dt.days.mean()
    win_rate = round(len(won) / max(1, len(won) + len(deals[deals['dealstage']=='closedlost'])), 2)
else:
    cycle, win_rate = 0, 0.0

kpis = [
    {"id":"pipeline_created","label":"Pipeline Created (wk)","value":round(pipeline_created,0),"delta":None},
    {"id":"coverage","label":"Coverage (rough)","value":round(forecast/1000000,2),"delta":None},
    {"id":"new_mqls","label":"New MQLs (wk)","value":int((contacts['lifecyclestage']=='MQL').sum()),"delta":None},
    {"id":"sql_rate","label":"SQL Rate (wk)","value":sql_rate,"delta":None},
    {"id":"win_rate","label":"Win Rate (L4W)","value":win_rate,"delta":None},
    {"id":"avg_cycle","label":"Avg Deal Cycle (d)","value":round(cycle,1),"delta":None}
]

# 4) Expect Perplexity result injected by the assistant as a list of dicts "news"
# Fallback to empty list if not available
try:
    news  # noqa
except NameError:
    news = []  # the assistant should set this variable after the Action call

import re
def tag_item(title):
    t = title.lower()
    tags=[]
    if any(x in t for x in ['earnings','results','guidance']): tags.append('earnings')
    if any(x in t for x in ['partnership','alliance','deal','joint venture','jv']): tags.append('partnership')
    if any(x in t for x in ['price','pricing']): tags.append('pricing')
    if any(x in t for x in ['strike','union','walkout','labor']): tags.append('labor')
    if any(x in t for x in ['outage','incident','breach','downtime']): tags.append('outage')
    if any(x in t for x in ['fine','antitrust','investigation','regulator']): tags.append('regulatory')
    if any(x in t for x in ['ceo','cfo','exec','appointment','resigns']): tags.append('exec')
    if any(x in t for x in ['esg','sustainability','saf','emissions']): tags.append('esg')
    if any(x in t for x in ['acquisition','merger','m&a']): tags.append('m&a')
    return tags or ['other']

from urllib.parse import urlparse
from datetime import datetime as dt
def score_item(d):
    # recency
    try:
        days = (now - pd.to_datetime(d.get('date') or d.get('last_updated'))).days
    except Exception:
        days = 30
    rec = 1.0 if days<=2 else 0.7 if days<=7 else 0.4 if days<=30 else 0.2
    # source tier
    host = urlparse(d.get('url','')).hostname or ''
    tier = 1.0 if any(x in host for x in ['lufthansa','investor','gov','europa.eu']) else \
           0.8 if any(x in host for x in ['reuters','ft','bloomberg','wsj']) else \
           0.5
    # focus (simple)
    rel = 1.0
    return round(0.5*rec + 0.3*tier + 0.2*rel, 2)

signals=[]
for r in news:
    title = r.get('title') or ''
    u = r.get('url') or ''
    host = urlparse(u).hostname or ''
    tags = tag_item(title)
    signals.append({
        "date": str(r.get('date') or r.get('last_updated') or '')[:10],
        "title": title, "url": u, "tags": tags, "score": score_item(r), "source": host
    })

# 5) Charts (matplotlib only; single-figure images)
def fig_to_b64():
    buf = io.BytesIO()
    plt.savefig(buf, bbox_inches='tight')
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close()
    return "data:image/png;base64," + b64

# Tags bar
import collections
cnt = collections.Counter([t for s in signals for t in s['tags']])
plt.figure()
plt.bar(list(cnt.keys()), list(cnt.values()))
plt.title("Signals by Tag")
plt.xticks(rotation=45, ha='right')
tags_b64 = fig_to_b64()

# Pipeline by stage bar
stg = deals['dealstage'].value_counts().sort_values(ascending=False)
plt.figure()
plt.bar(stg.index.tolist(), stg.values.tolist())
plt.title("Pipeline by Stage (count)")
plt.xticks(rotation=45, ha='right')
pipe_b64 = fig_to_b64()

# 6) Select CRM snapshot rows (top by amount; open only)
open_deals = deals[deals['closedate'].isna()].copy()
open_deals['age_days'] = (now - open_deals['createdate']).dt.days
crm_rows = open_deals.sort_values('amount', ascending=False)[['dealname','dealstage','amount','age_days','owner']].head(8)
crm = [
    {"deal":row.dealname,"stage":row.dealstage,"amount":float(row.amount),"age_days":int(row.age_days),"owner":row.owner}
    for _,row in crm_rows.iterrows()
]

DATA_JSON = {
    "timeframe": "week",
    "kpis": kpis,
    "signals": sorted(signals, key=lambda x:(-x['score'], x['date']))[:12],
    "crm": crm,
    "chart_images": {"tags_bar": tags_b64, "pipeline_stage": pipe_b64}
}

# 7) Return artifacts for the assistant to inject into the HTML
DATA_JSON
