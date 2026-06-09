const http  = require('http');
const https = require('https');
const WebSocket = require('ws');

const PORT = process.env.PORT || 3001;
const GMGN_KEY = 'gmgn_4e8e56886662c19d00376f0bdf022764';

const ALLOWED_HOSTS = [
  'gmgn.ai',
  'api6.axiom.trade',
  'api7.axiom.trade',
  'solana-rpc.publicnode.com',
  'api.dexscreener.com',
  'api.geckoterminal.com',
  'io.dexscreener.com'
];

let geckoQueue = Promise.resolve();
const GECKO_DELAY = 6500;

function makeRequest(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve({ status: res.statusCode, data }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.setTimeout(15000, () => { req.destroy(); reject(new Error('timeout')); });
    if (body) req.write(body);
    req.end();
  });
}

const server = http.createServer(async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Accept');

  // Scanner routes take priority
  if (req.url.startsWith('/scanner')) { scannerRoute(req, res, req.url); return; }

  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // Health check
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', proxy: 'solana-proxy' }));
    return;
  }

  const params = new URLSearchParams(req.url.slice(2));
  const target = params.get('url');
  if (!target) { res.writeHead(400); res.end('missing url'); return; }

  let parsed;
  try { parsed = new URL(target); }
  catch (e) { res.writeHead(400); res.end('bad url'); return; }

  if (!ALLOWED_HOSTS.some(h => parsed.hostname.endsWith(h))) {
    res.writeHead(403); res.end('host not allowed: ' + parsed.hostname); return;
  }

  const isGecko = parsed.hostname.includes('geckoterminal');
  const isGmgn = parsed.hostname.includes('gmgn');
  const isAxiom = parsed.hostname.includes('axiom');

  let body = '';
  req.on('data', chunk => body += chunk);
  req.on('end', async () => {
    const options = {
      hostname: parsed.hostname,
      path: parsed.pathname + parsed.search,
      method: req.method,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Referer': isGecko ? 'https://www.geckoterminal.com/' : isAxiom ? 'https://axiom.trade/' : 'https://gmgn.ai/',
        'Origin': isGecko ? 'https://www.geckoterminal.com' : isAxiom ? 'https://axiom.trade' : 'https://gmgn.ai',
        'Accept-Language': 'en-US,en;q=0.9',
        ...(isGmgn ? { 'X-API-KEY': GMGN_KEY } : {})
      }
    };

    const doRequest = async () => {
      let result = await makeRequest(options, body || null);
      console.log(`${result.status} ${parsed.hostname}${parsed.pathname}`);
      if (result.status === 429 && isGecko) {
        console.log('429 — waiting 12s...');
        await new Promise(r => setTimeout(r, 12000));
        result = await makeRequest(options, body || null);
        console.log(`retry -> ${result.status}`);
      }
      res.writeHead(result.status, {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      });
      res.end(result.data);
    };

    try {
      if (isGecko) {
        let done;
        const p = new Promise(r => done = r);
        geckoQueue = geckoQueue.then(() => new Promise(resolve => {
          setTimeout(async () => {
            try { await doRequest(); } catch (e) { res.writeHead(500); res.end(e.message); }
            done(); resolve();
          }, GECKO_DELAY);
        }));
        await p;
      } else {
        await doRequest();
      }
    } catch (e) { res.writeHead(500); res.end(e.message); }
  });
});

server.listen(PORT, '0.0.0.0', () => console.log(`Solana proxy running on port ${PORT}`));

// ═══════════════════════════════════════════════════════════════
//  RUGGER SCANNER — exact same logic as local, two independent instances
//  DEX routes:  /scanner/dex/status|start|stop|loop
//  GMGN routes: /scanner/gmgn/status|start|stop|loop
// ═══════════════════════════════════════════════════════════════
const WALLETS_URL = 'https://track-app-production-9425.up.railway.app/wallets';
const FEE = 0.002;
const SOL_PRICE = 130;
const SUP = 1e9;
const CLIM = 1000;

function mkScan(defaultConfig) {
  return {
    running: false, loop: false, stopFlag: false,
    status: 'idle', progress: 0, phase: '', scanCount: 0,
    stats: {mints:0,rugs:0,wallets:0,passed:0},
    results: [], log: [],
    config: {...defaultConfig}
  };
}

const scanDEX = mkScan({
  volTimeframe: '1h',   // '5m'|'1h'|'6h'|'24h'
  minVol: 15000,
  collectSecs: 30,
  rugDrop: 70,
  rugCandles: 8,
  firstBuyers: 10,
  tokensAnalyze: 30,
  minTrades: 20,
  minROC: 100,
  minGrade: 5,
});

const scanGMGN = mkScan({
  period: '24h',        // '1h'|'6h'|'24h'
  minVol: 15000,
  rugDrop: 70,
  rugCandles: 8,
  firstBuyers: 10,
  tokensAnalyze: 30,
  minTrades: 20,
  minROC: 100,
  minGrade: 5,
});

function scanLog(s,msg){
  const line=`[${new Date().toTimeString().slice(0,8)}] ${msg}`;
  s.log.push(line);
  if(s.log.length>200)s.log=s.log.slice(-200);
  console.log(`[SCAN-${s===scanDEX?'DEX':'GMGN'}]`,msg);
}

const GMGN_HEADERS={
  'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Accept':'application/json','Accept-Language':'en-US,en;q=0.9',
  'DNT':'1','Referer':'https://gmgn.ai/','Origin':'https://gmgn.ai',
  'X-API-KEY':GMGN_KEY,
};
function sleep(ms){return new Promise(r=>setTimeout(r,ms));}
async function pg(url,retries=2){
  const parsed=new URL(url);
  const opts={hostname:parsed.hostname,path:parsed.pathname+parsed.search,method:'GET',headers:GMGN_HEADERS};
  for(let i=0;i<=retries;i++){
    try{
      const r=await makeRequest(opts,null);
      if(r.status===429){await sleep(3000*(i+1));continue;}
      return JSON.parse(r.data);
    }catch(e){if(i===retries)return null;await sleep(1500);}
  }
  return null;
}

// ── collectGMGNMints — two sort orders for breadth (mirrors WS) ──
async function collectGMGNMints(minVol,period){
  const base='https://gmgn.ai/defi/quotation/v1/pairs/sol/new_pairs/1h?limit=100&launchpad_platforms[]=Pump.fun&filters[]=not_honeypot&min_volume='+minVol+'&period='+period;
  const [d1,d2]=await Promise.all([
    pg(base+'&orderby=open_timestamp&direction=desc'),
    pg(base+'&orderby=volume&direction=desc'),
  ]);
  const mints=new Set();
  for(const d of[d1,d2])for(const p of(d?.data?.pairs||[]))if(p.base_address?.endsWith('pump'))mints.add(p.base_address);
  return[...mints];
}

// ── DEX mints — exact same logic as local collectMints() ─────
// Browser uses native WebSocket + binaryType='arraybuffer'
// Server uses ws npm package — binary messages arrive as Buffer (same indexing)
function collectDEXMints(minVol,volTimeframe,secs){
  return new Promise(resolve=>{
    const mints=new Set();
    const B='123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
    const tfMap={'5m':['h1','m5'],'1h':['h24','h1'],'6h':['h24','h6'],'24h':['h24','h24']};
    const[pathTf,filterTf]=tfMap[volTimeframe||'1h']||['h24','h1'];
    const url='wss://io.dexscreener.com/dex/screener/v7/pairs/'+pathTf+'/1?rankBy[key]=pairAge&rankBy[order]=asc&filters[dexIds][0]=pumpfun&filters[volume]['+filterTf+'][min]='+minVol+'&filters[excludedDexIds][]&filters[chainIds][0]=solana';
    let done=false;
    const finish=()=>{if(!done){done=true;try{ws.terminate();}catch(e){}resolve([...mints]);}};
    const ws=new WebSocket(url,{
      headers:{
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Origin':'https://dexscreener.com',
        'Referer':'https://dexscreener.com/'
      }
    });
    ws.on('message',data=>{
      const buf=Buffer.isBuffer(data)?data:Buffer.from(data);
      for(let i=0;i<buf.length-1;i++){
        const byt=buf[i];
        if(byt>=32&&byt<=50&&i+1+byt<=buf.length){
          const str=buf.slice(i+1,i+1+byt).toString('utf8');
          if([...str].every(c=>B.includes(c))&&str.endsWith('pump'))mints.add(str);
        }
      }
    });
    ws.on('error',e=>{console.log('[DEX-WS] error:',e.message);finish();});
    ws.on('close',()=>finish());
    setTimeout(finish,(secs||30)*1000);
  });
}

// ── Exact copy of local checkRug ─────────────────────────────
async function checkRug(mint,thresh,rugCandles){
  try{
    const d=await pg('https://gmgn.ai/api/v1/token_mcap_candles/sol/'+mint+'?resolution=1s&limit=1000');
    const c=(d?.data?.list||[]).map(x=>({h:+x.high,l:+x.low,t:x.time})).sort((a,b)=>a.t-b.t);
    if(c.length<5)return null;
    const hs=c.map(x=>x.h);const pk=Math.max(...hs);const pi=hs.indexOf(pk);
    const win=c.slice(pi+1,pi+1+rugCandles);if(win.length<2)return null;
    const lowestLow=Math.min(...win.map(x=>x.l));
    const drop=(pk-lowestLow)/pk*100;if(drop<thresh)return null;
    return{mint};
  }catch(e){return null;}
}

// ── Exact copy of local getFirstBuyers ───────────────────────
async function getFirstBuyers(mint,n){
  try{
    const id=await pg('https://gmgn.ai/api/v1/token_info/sol/'+mint);
    const ots=id?.data?.open_timestamp;if(!ots)return[];
    const td=await pg('https://gmgn.ai/vas/api/v1/token_trades/sol/'+mint+'?limit=50&from='+ots+'&to='+(ots+60));
    const tr=td?.data?.history||[];const buyers=[];const seen=new Set();
    for(const t of tr){if(t.event==='buy'&&t.maker&&!seen.has(t.maker)){seen.add(t.maker);buyers.push({wallet:t.maker,position:buyers.length+1});if(buyers.length>=n)break;}}
    return buyers;
  }catch(e){return[];}
}

// ── Exact copy of local getTokenInfo ─────────────────────────
async function getTokenInfo(mint){
  const d=await pg('https://gmgn.ai/api/v1/token_info/sol/'+mint);
  if(!d?.data)throw new Error('no info');
  return{openTs:d.data.open_timestamp};
}

// ── Exact copy of local getFirst1sMC ─────────────────────────
async function getFirst1sMC(mint,openTs){
  let mc=null;
  try{
    const d=await pg('https://gmgn.ai/vas/api/v1/token_trades/sol/'+mint+'?limit=50&from='+openTs+'&to='+(openTs+1)+'&maker=');
    const t=d?.data?.history||[];
    if(t.length){const ts=[...t].sort((a,b)=>a.timestamp-b.timestamp);mc=+ts[ts.length-1].price_usd*SUP;}
    else{
      const d2=await pg('https://gmgn.ai/vas/api/v1/token_trades/sol/'+mint+'?limit=50&from='+openTs+'&to='+(openTs+5)+'&maker=');
      const t2=d2?.data?.history||[];
      if(t2.length){const ts2=[...t2].sort((a,b)=>a.timestamp-b.timestamp);mc=+ts2[ts2.length-1].price_usd*SUP;}
    }
  }catch(e){}
  return mc;
}

// ── Exact copy of local getWalletBuyMC ───────────────────────
async function getWalletBuyMC(mint,buyTs){
  try{
    const d=await pg('https://gmgn.ai/vas/api/v1/token_trades/sol/'+mint+'?limit=20&from='+(buyTs-5)+'&to='+(buyTs+5)+'&maker=');
    const t=d?.data?.history||[];if(!t.length)return null;
    const ts3=[...t].sort((a,b)=>Math.abs(a.timestamp-buyTs)-Math.abs(b.timestamp-buyTs));
    const p=+ts3[0].price_usd*SUP;return p>0?p:null;
  }catch(e){return null;}
}

// ── Exact copy of local getCandles ───────────────────────────
async function getCandles(mint){
  const d=await pg('https://gmgn.ai/api/v1/token_mcap_candles/sol/'+mint+'?pool_type=tpool&resolution=1s&limit='+CLIM);
  const lst=d?.data?.list||[];if(!lst.length)return null;
  return lst.map(c=>({t:c.time,h:+c.high,l:+c.low,c:+c.close})).sort((a,b)=>a.t-b.t);
}

// ── Exact copy of local getWalletTokens (stopFn replaces _stop global) ──
async function getWalletTokens(wallet,needed,stopFn){
  const map={};let cursor=null,page=1;const max=Math.ceil(needed/20)*3+2;
  while(page<=max){
    if(stopFn&&stopFn())break;
    let url='https://gmgn.ai/api/v1/wallet_activity/sol?wallet='+wallet+'&limit=20&type=buy';
    if(cursor)url+='&cursor='+encodeURIComponent(cursor);
    const d=await pg(url);const acts=d?.data?.activities||[];
    acts.forEach(a=>{const mint=a.token?.address;if(!mint||a.event_type!=='buy')return;
      if(!map[mint])map[mint]={symbol:a.token?.symbol||mint.slice(0,8),timestamp:a.timestamp,buyTimestamp:a.timestamp};
      else if(a.timestamp<map[mint].buyTimestamp)map[mint].buyTimestamp=a.timestamp;});
    const next=d?.data?.next;if(!next||acts.length===0)break;
    cursor=next;page++;if(Object.keys(map).length>=needed*1.5)break;
  }
  return map;
}

// ── Exact copy of local calcOptTP ────────────────────────────
function calcOptTP(valid){
  let bestTP=50,bestNet=-Infinity;
  for(let tp=50;tp<=1000;tp+=10){
    const p=valid.filter(r=>r.peakPct>=tp).length*(tp/100);
    const ll=valid.filter(r=>r.peakPct<tp).reduce((acc,r)=>acc+Math.abs(r.bottomPct)/100,0);
    if(p-ll>bestNet){bestNet=p-ll;bestTP=tp;}
  }
  return bestTP;
}

// ── Exact copy of local calcGrade ────────────────────────────
function calcGrade(valid){
  const pcts=valid.map(r=>r.peakPct);const avg=pcts.reduce((a,b)=>a+b,0)/pcts.length;
  const a100=valid.filter(r=>r.peakPct>=100).length;const a50=valid.filter(r=>r.peakPct>=50).length;
  const std=Math.sqrt(pcts.map(x=>Math.pow(x-avg,2)).reduce((a,b)=>a+b,0)/pcts.length);
  const cv=std/Math.abs(avg);let g=5;
  if(avg>200)g+=2;else if(avg>100)g+=1;
  if(a100/valid.length>0.6)g+=2;else if(a100/valid.length>0.4)g+=1;
  if(cv<0.5)g+=1;else if(cv>1.2)g-=1;
  if(a50/valid.length>0.7)g+=1;
  return Math.min(10,Math.max(1,Math.round(g)));
}

// ── Exact copy of local analyzeWallet ────────────────────────
async function analyzeWallet(wallet,tokensN,minROC,solPrice,stopFn){
  try{
    const tokenMap=await getWalletTokens(wallet,tokensN,stopFn);
    const tokens=Object.entries(tokenMap).sort((a,b)=>b[1].timestamp-a[1].timestamp).slice(0,tokensN);
    if(tokens.length<5)return null;
    const results=[];
    for(const [mint,info] of tokens){
      if(stopFn&&stopFn())break;
      try{
        const ti=await getTokenInfo(mint);
        const tradesMC=await getFirst1sMC(mint,ti.openTs);
        const candles=await getCandles(mint);
        if(!candles)continue;
        const candleMC=candles[0].c;
        const first1sMC=(tradesMC&&candleMC)?Math.min(tradesMC,candleMC):(tradesMC||candleMC);
        const buyMC=await getWalletBuyMC(mint,info.buyTimestamp);
        const entryMC=(buyMC&&buyMC>first1sMC)?buyMC:first1sMC;if(!entryMC)continue;
        const buyTs=info.buyTimestamp;
        let ac=candles.filter(c=>c.t/1000>buyTs);
        if(!ac.length)ac=candles.filter(c=>c.t/1000>=buyTs);
        if(!ac.length)continue;
        const peakMC=Math.max(...ac.map(c=>c.h));
        const peakIdx=ac.findIndex(c=>c.h===peakMC);
        const afterPeak=ac.slice(peakIdx);
        const bottomMC=afterPeak.length?Math.min(...afterPeak.map(c=>c.l)):peakMC;
        results.push({peakPct:((peakMC-entryMC)/entryMC)*100,bottomPct:((bottomMC-entryMC)/entryMC)*100});
      }catch(e){}
    }
    const valid=results.filter(r=>r.peakPct!=null);if(valid.length<3)return null;
    const optTP=calcOptTP(valid);const fee=FEE*solPrice;
    const hits=valid.filter(r=>r.peakPct>=optTP);const misses=valid.filter(r=>r.peakPct<optTP);
    const net=hits.length*100*(optTP/100)-misses.reduce((acc,r)=>acc+100*(Math.abs(r.bottomPct)/100),0)-valid.length*fee;
    const roc=(net/100)*100;const winRate=valid.length?(hits.length/valid.length*100):0;
    if(roc<minROC)return null;
    const grade=calcGrade(valid);
    return{wallet,tokensAnalyzed:valid.length,optTP,winRate,roc,grade,verdictLabel:grade>=8?'COPY':grade>=5?'CAUTION':'WATCH'};
  }catch(e){return null;}
}

function saveWallet(w,source){
  const body=JSON.stringify({address:w.wallet,grade:w.grade,roc:w.roc,winRate:w.winRate,
    trades:w.tokensAnalyzed,source,addedAt:Date.now()});
  const opts={hostname:'track-app-production-9425.up.railway.app',path:'/wallets',
    method:'POST',headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(body)}};
  try{const req=https.request(opts);req.write(body);req.end();}catch(e){}
}

// ── Generic runner works for both instances ───────────────────
async function runScan(s,source){
  const cfg={...s.config};
  s.stats={mints:0,rugs:0,wallets:0,passed:0};
  s.results=[];s.progress=0;s.status='scanning';
  const stopFn=()=>s.stopFlag;
  scanLog(s,`▶ Scan #${s.scanCount+1} started — mode=${source}`);

  s.phase='Collecting mints...';
  let mints=[];
  if(source==='dex'){
    scanLog(s,`Fetching DEX mints via WS (tf=${cfg.volTimeframe||'1h'}, min $${cfg.minVol}, ${cfg.collectSecs||30}s)...`);
    mints=await collectDEXMints(cfg.minVol,cfg.volTimeframe,cfg.collectSecs);
  }else{
    scanLog(s,`Fetching GMGN mints (period=${cfg.period}, min $${cfg.minVol})...`);
    mints=await collectGMGNMints(cfg.minVol,cfg.period);
  }
  s.stats.mints=mints.length;s.progress=10;
  scanLog(s,`Phase 1: ${mints.length} mints`);
  if(!mints.length||s.stopFlag)return;

  const rugs=[];
  for(let i=0;i<mints.length;i++){
    if(s.stopFlag)break;
    const r=await checkRug(mints[i],cfg.rugDrop,cfg.rugCandles);
    if(r)rugs.push(r);
    s.progress=10+Math.round(i/mints.length*30);
    s.phase=`Phase 2/4 — ${i+1}/${mints.length} checked, ${rugs.length} rugs`;
  }
  s.stats.rugs=rugs.length;s.progress=40;
  scanLog(s,`Phase 2: ${rugs.length} rugs`);
  if(!rugs.length||s.stopFlag)return;

  const w2r={};const walletPositions={};
  for(let i=0;i<rugs.length;i++){
    if(s.stopFlag)break;
    const buyers=await getFirstBuyers(rugs[i].mint,cfg.firstBuyers);
    for(const b of buyers){if(!w2r[b.wallet])w2r[b.wallet]=[];w2r[b.wallet].push(rugs[i].mint);walletPositions[b.wallet+'_'+rugs[i].mint]=b.position;}
    s.progress=40+Math.round(i/rugs.length*20);
    s.phase=`Phase 3/4 — ${i+1}/${rugs.length} rugs processed`;
  }
  const wallets=Object.keys(w2r);
  s.stats.wallets=wallets.length;s.progress=60;
  scanLog(s,`Phase 3: ${wallets.length} wallets`);
  if(!wallets.length||s.stopFlag)return;

  let passed=0;
  for(let i=0;i<wallets.length;i++){
    if(s.stopFlag)break;
    s.phase=`Phase 4/4 — ${i+1}/${wallets.length} wallets, ${passed} passed`;
    s.progress=60+Math.round(i/wallets.length*40);
    const res=await analyzeWallet(wallets[i],cfg.tokensAnalyze,cfg.minROC,SOL_PRICE,stopFn);
    if(!res||res.grade<cfg.minGrade||res.tokensAnalyzed<cfg.minTrades)continue;
    passed++;
    s.results.unshift(res);
    if(s.results.length>100)s.results=s.results.slice(0,100);
    s.stats.passed=passed;
    saveWallet(res,source+'-server');
    scanLog(s,`✅ ${wallets[i].slice(0,8)}... grade=${res.grade}/10 WR=${res.winRate.toFixed(1)}% ROC=${res.roc.toFixed(0)}% TP=+${res.optTP}%`);
  }
  s.scanCount++;s.progress=100;
  s.phase=`Done — ${passed} passed (scan #${s.scanCount})`;
  scanLog(s,`✅ Scan complete — ${passed} passed`);
}

async function loopRunner(s,source){
  while(true){
    try{await runScan(s,source);}catch(e){scanLog(s,`Scan error: ${e.message}`);}
    s.running=false;
    if(s.stopFlag||!s.loop)break;
    scanLog(s,'🔁 Loop — restarting in 15s...');
    s.status='loop-wait';s.progress=0;s.phase='Restarting in 15s...';
    for(let i=0;i<15;i++){if(s.stopFlag)break;await sleep(1000);}
    if(s.stopFlag)break;
    s.running=true;s.stopFlag=false;
  }
  s.running=false;s.status='idle';s.stopFlag=false;
  scanLog(s,'⏹ Scanner stopped');
}

// ── HTTP route handler for one scanner instance ───────────────
function handleScannerInstance(s,source,req,res,action){
  res.setHeader('Access-Control-Allow-Origin','*');
  res.setHeader('Content-Type','application/json');
  if(req.method==='OPTIONS'){res.writeHead(204);res.end();return;}

  if(action==='status'&&req.method==='GET'){
    res.writeHead(200);
    res.end(JSON.stringify({running:s.running,loop:s.loop,status:s.status,
      progress:s.progress,phase:s.phase,scanCount:s.scanCount,
      stats:s.stats,results:s.results.slice(0,50),log:s.log.slice(-60),config:s.config}));
    return;
  }
  if(action==='start'&&req.method==='POST'){
    let body='';
    req.on('data',d=>body+=d);
    req.on('end',()=>{
      if(s.running){res.writeHead(200);res.end(JSON.stringify({ok:false,error:'Already running'}));return;}
      try{const cfg=JSON.parse(body||'{}');Object.keys(cfg).forEach(k=>{if(k in s.config)s.config[k]=cfg[k];});}catch(e){}
      s.running=true;s.stopFlag=false;s.status='scanning';
      loopRunner(s,source);
      res.writeHead(200);res.end(JSON.stringify({ok:true}));
    });
    return;
  }
  if(action==='stop'&&req.method==='POST'){
    s.stopFlag=true;s.loop=false;s.status='idle';s.running=false;
    scanLog(s,'⏹ Stop requested');
    res.writeHead(200);res.end(JSON.stringify({ok:true}));
    return;
  }
  if(action==='loop'&&req.method==='POST'){
    let body='';
    req.on('data',d=>body+=d);
    req.on('end',()=>{
      try{const d=JSON.parse(body||'{}');s.loop=d.loop!==undefined?!!d.loop:d.enabled!==undefined?!!d.enabled:!s.loop;}catch(e){s.loop=!s.loop;}
      scanLog(s,`🔁 Loop ${s.loop?'enabled':'disabled'}`);
      res.writeHead(200);res.end(JSON.stringify({ok:true,loop:s.loop}));
    });
    return;
  }
  res.writeHead(404);res.end('not found');
}

// ── Route dispatcher ──────────────────────────────────────────
// /scanner/dex/status|start|stop|loop  → DEX instance
// /scanner/gmgn/status|start|stop|loop → GMGN instance
// /scanner/status|start|stop|loop      → legacy, maps to DEX
function scannerRoute(req,res,url){
  const dexM=url.match(/^\/scanner\/dex\/(status|start|stop|loop)/);
  if(dexM){handleScannerInstance(scanDEX,'dex',req,res,dexM[1]);return true;}
  const gmgnM=url.match(/^\/scanner\/gmgn\/(status|start|stop|loop)/);
  if(gmgnM){handleScannerInstance(scanGMGN,'gmgn',req,res,gmgnM[1]);return true;}
  // legacy routes → DEX
  const legM=url.match(/^\/scanner\/(status|start|stop|loop)/);
  if(legM){handleScannerInstance(scanDEX,'dex',req,res,legM[1]);return true;}
  // /scanner with no action → status of both
  if(url==='/scanner'||url==='/scanner/'){
    res.setHeader('Access-Control-Allow-Origin','*');
    res.setHeader('Content-Type','application/json');
    res.writeHead(200);
    res.end(JSON.stringify({dex:{running:scanDEX.running,loop:scanDEX.loop,status:scanDEX.status,stats:scanDEX.stats},gmgn:{running:scanGMGN.running,loop:scanGMGN.loop,status:scanGMGN.status,stats:scanGMGN.stats}}));
    return true;
  }
  return false;
}
