const express = require('express');
const { TikTokLiveConnection: WebcastPushConnection } = require('tiktok-live-connector');
const app = express();
const username = (process.env.TIKTOK_USERNAME || 'denilsonnunesss').replace(/^@/, '');
const clients = new Set();
let connection, connected = false, connecting = false, lastError = '', retry;
function send(payload) { const line = `data: ${JSON.stringify(payload)}\n\n`; for (const res of clients) res.write(line); }
function status() { return { type:'status', connected, username, error:lastError }; }
app.get('/events',(req,res)=>{res.setHeader('Content-Type','text/event-stream');res.setHeader('Cache-Control','no-cache, no-transform');res.setHeader('Connection','keep-alive');res.flushHeaders();clients.add(res);res.write(`data: ${JSON.stringify(status())}\n\n`);req.on('close',()=>clients.delete(res));});
app.get('/health',(req,res)=>res.json(status()));
app.use(express.static('public'));

app.get('/', (req, res) => {
  res.sendFile('index.html', { root: __dirname });
});
  
function scheduleRetry(){clearTimeout(retry);retry=setTimeout(connect,30000);}
async function connect(){if(connecting)return;connecting=true;try{
  connection = new WebcastPushConnection(username, { processInitialData: false });
 connection.on('gift', data=>{
  // TikTok streaks produce intermediate events; only count the final count once.
  if(data.giftType === 1 && !data.repeatEnd)return;
  const name=String(data.nickname||data.uniqueId||'Participante').slice(0,60);
  const giftName=String(data.giftName||data.gift?.name||'').trim();
  const normalized=giftName.toLocaleLowerCase('pt-BR');
  let side=null;
  if(normalized==='rose'||normalized==='rosa')side='F';
  if(normalized==='tiktok')side='L';
  if(!side)return;
  const count=Math.max(1,Math.min(10000,Number(data.repeatCount)||1));
  send({type:'gift',side,count,name,giftName});
 });
 connection.on('disconnected',()=>{connected=false;send(status());scheduleRetry();});
 connection.on('error',e=>{lastError=String(e.message||e);connected=false;send(status());scheduleRetry();});
 await connection.connect();connected=true;lastError='';send(status());
 }catch(e){connected=false;lastError=String(e.message||e);send(status());scheduleRetry();}finally{connecting=false;}}
const port=process.env.PORT||3000;
app.listen(port,()=>{console.log(`Listening on ${port}`);connect();});
