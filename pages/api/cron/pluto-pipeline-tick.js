
// CRON 10 — Pluto Pipeline Memory Tick
// Pluto: long-term memory — records pipeline state across all planets
// Reads current state of each planet's KV output →
// writes to Pluto write ledger → flushes to Uranus at 500 entries
// Authority: Joshua Lopez — DCGP.AI — USPTO 19/555,951
'use strict';
const https = require('https');
function now() { return new Date().toISOString(); }
function send(res, s, p) {
  res.statusCode = s;
  res.setHeader('Content-Type','application/json; charset=utf-8');
  res.setHeader('Cache-Control','no-store');
  res.setHeader('Access-Control-Allow-Origin','*');
  res.end(JSON.stringify(p, null, 2));
}
function kvReq(method, path, body) {
  const base = process.env.KV_REST_API_URL, tok = process.env.KV_REST_API_TOKEN;
  if (!base || !tok) return Promise.resolve(null);
  return new Promise(resolve => {
    try {
      const full = new URL(base.replace(/\/$/,'') + path);
      const data = body ? JSON.stringify(body) : null;
      const req = https.request({
        hostname: full.hostname, path: full.pathname + full.search, method,
        headers: { Authorization: 'Bearer ' + tok, ...(data ? { 'Content-Type':'application/json','Content-Length':Buffer.byteLength(data) } : {}) }
      }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve(JSON.parse(d))}catch{resolve(null)} }); });
      req.on('error',()=>resolve(null));
      req.setTimeout(6000,()=>{req.destroy();resolve(null)});
      if(data)req.write(data); req.end();
    } catch{resolve(null)}
  });
}
const kvGet = k => kvReq('GET','/get/'+encodeURIComponent(k)).then(r=>r?.result?JSON.parse(r.result):null).catch(()=>null);
const kvSet = (k,v,ex) => kvReq('POST','/set/'+encodeURIComponent(k)+(ex?'?ex='+ex:''),v).catch(()=>null);
function postInternal(path, body, ms=12000) {
  const base = 'https://aura115.ai';
  return new Promise(resolve => {
    try {
      const data = JSON.stringify(body);
      const req = https.request({
        hostname: 'aura115.ai', path, method: 'POST',
        headers: { 'Content-Type':'application/json','Content-Length':Buffer.byteLength(data),
          'x-aura-operator-key': process.env.AURA_OPERATOR_KEY || 'Honor_is_the_Reward_of_Virtue' }
      }, res => { let d=''; res.on('data',c=>d+=c); res.on('end',()=>{ try{resolve({ok:res.statusCode===200,status:res.statusCode,body:JSON.parse(d)})}catch{resolve({ok:false})} }); });
      req.on('error',()=>resolve({ok:false}));
      req.setTimeout(ms,()=>{req.destroy();resolve({ok:false,error:'timeout'})});
      req.write(data); req.end();
    } catch{resolve({ok:false})}
  });
}

module.exports = async function handler(req, res) {
  const ts = now();
  const planet_states = {};
  const planet_keys = {
    venus:   'aura115:venus:admitted:v1',
    saturn:  'aura115:saturn:pipeline:v1',
    neptune: 'aura115:neptune:cleared:v1',
    moon:    'aura115:moon:projected:v1',
    earth:   'aura115:earth:delegation-log:v1',
    mars:    'aura115:mars:action-log:v1'
  };
  for (const [planet, key] of Object.entries(planet_keys)) {
    const state = await kvGet(key);
    if (state) planet_states[planet] = { count: state.count || (state.entries||[]).length || (state.delegated||[]).length, updated_at: state.updated_at };
  }
  const writes = [];
  for (const [planet, state] of Object.entries(planet_states)) {
    const result = await postInternal('/api/pluto-memory', {
      planet_id: planet, event_type: 'pipeline_tick',
      lcvs_state: state, source_route: '/api/cron/pluto-pipeline-tick',
      operator_id: 'Joshua Lopez'
    });
    if (result.ok) writes.push(planet);
  }
  return send(res, 200, { ok:true, cron:'pluto-pipeline-tick', ts,
    planets_recorded:writes, planet_states,
    authority:'Joshua Lopez — DCGP.AI — USPTO 19/555,951' });
};
