const express = require('express');
const { TikTokLiveConnection } = require('tiktok-live-connector');
const app = express();
const port = process.env.PORT || 3000;
const username = (process.env.TIKTOK_USERNAME || 'denilsonnunesss').replace(/^@/, '');
const clients = new Set();
let connected = false, connecting = false, lastError = '', connection, retryTimer;
const status = () => ({type:'status', connected, username, error:lastError});
function send(obj) { const data = `data: ${JSON.stringify(obj)}\n\n`; for(const c of clients) { try {c.write(data)} catch {clients.delete(c)} } }
app.get('/health', (req,res) => res.json(status()));
app.get('/events', (req,res) => {
 res.setHeader('Content-Type','text/event-stream');
 res.setHeader('Cache-Control','no-cache, no-transform');
 res.setHeader('Connection','keep-alive');
 res.flushHeaders(); clients.add(res); res.write(`data: ${JSON.stringify(status())}\n\n`);
 req.on('close',()=>clients.delete(res));
});
app.use(express.static(__dirname));
function retry() { clearTimeout(retryTimer); retryTimer=setTimeout(connect,30000); }
async function connect() {
 if(connecting || connected) return;
 connecting=true;
 try {
  connection = new TikTokLiveConnection(username, {processInitialData:false});
  connection.on('gift', d => {
   // Streak gifts: count only at the end to prevent double counting.
   if(d.giftType === 1 && !d.repeatEnd) return;
   const giftName = String(d.giftName || d.gift?.name || '').trim().toLowerCase();
   const side = giftName === 'rose' || giftName === 'rosa' ? 'F' : giftName === 'tiktok' || giftName === 'tik tok' ? 'L' : null;
   if(!side) return;
   const count = Math.max(1,Math.min(10000,Number(d.repeatCount)||1));
   send({type:'gift', side, count, name:String(d.nickname||d.uniqueId||'Participante').slice(0,60), giftName});
  });
  connection.on('chat', d => send({type:'comment', nickname:String(d.nickname||d.uniqueId||'Participante').slice(0,60), comment:String(d.comment||'').slice(0,300)}));
  connection.on('disconnected', () => {connected=false;send(status());retry();});
  connection.on('error', e => {lastError=String(e?.message||e);connected=false;send(status());retry();});
  await connection.connect(); connected=true; lastError=''; send(status());
  console.log('Conexão TikTok estabelecida:',username);
 } catch(e) {connected=false;lastError=String(e?.message||e);console.error(lastError);send(status());retry();}
 finally {connecting=false;}
}
app.listen(port,()=>{console.log('Servidor iniciado',port);connect()});
setInterval(()=>{for(const c of clients)try{c.write(': ping\n\n')}catch{clients.delete(c)}},20000);
