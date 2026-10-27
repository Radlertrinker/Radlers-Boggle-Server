// Radlers Boggle – Vermittlungsserver
// Der Server rechnet nichts. Er reicht nur Nachrichten zwischen Moderator und Spielern weiter.
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8787;
// Erlaubte Seite(n), z. B. https://DEINNAME.github.io (mehrere mit Komma). Leer = jede Seite erlaubt.
const ALLOWED = (process.env.ALLOWED_ORIGIN || '').split(',').map(s => s.trim()).filter(Boolean);
const MAX_CLIENTS = 400;     // Schutz vor Überlastung
const GRACE_MS = 10000;      // so lange bleibt eine abgerissene Verbindung reserviert (Funkloch, kurzer Wechsel)

const peers = new Map();     // id -> { ws, links:Set("cid|andereId"), timer }
const tx = (ws, m) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(m)); };

const server = http.createServer((req, res) => {
  const online = [...peers.values()].filter(p => p.ws).length;
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end('Radlers Boggle Server läuft. Verbundene Geräte: ' + online);
});

const wss = new WebSocketServer({
  server,
  maxPayload: 1024 * 1024,
  verifyClient: info => !ALLOWED.length || ALLOWED.includes(info.origin)
});

function drop(id) {
  const p = peers.get(id);
  if (!p) return;
  clearTimeout(p.timer);
  for (const l of p.links) {
    const [cid, other] = l.split('|');
    const o = peers.get(other);
    if (o) { o.links.delete(cid + '|' + id); tx(o.ws, { k: 'close', from: id, cid }); }
  }
  peers.delete(id);
}

wss.on('connection', ws => {
  ws.alive = true; ws.id = null; ws.count = 0;
  const rate = setInterval(() => { ws.count = 0; }, 1000);
  ws.on('pong', () => { ws.alive = true; });

  ws.on('message', raw => {
    if (++ws.count > 300) { ws.close(); return; }          // Schutz vor Überflutung
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.k === 'reg') {                                    // Anmeldung mit eigener Kennung
      const id = String(m.id || '');
      if (!/^[a-z0-9]{4,24}$/i.test(id)) { tx(ws, { k: 'reg', ok: false }); return; }
      let p = peers.get(id);
      if (p && p.ws && p.ws !== ws) { tx(ws, { k: 'reg', ok: false }); return; }   // Kennung vergeben
      if (!p) {
        if (peers.size >= MAX_CLIENTS) { tx(ws, { k: 'reg', ok: false }); return; }
        p = { ws: null, links: new Set(), timer: null };
        peers.set(id, p);
      }
      clearTimeout(p.timer); p.ws = ws; ws.id = id;
      tx(ws, { k: 'reg', ok: true, id });
      return;
    }
    if (m.k === 'ping') { tx(ws, { k: 'pong' }); return; }
    if (!ws.id) return;
    if (!['open', 'ack', 'data', 'close'].includes(m.k)) return;
    if (typeof m.to !== 'string' || typeof m.cid !== 'string' || m.cid.length > 40) return;

    const me = peers.get(ws.id), target = peers.get(m.to);
    if (!me) return;
    if (m.k === 'open') {
      if (!target || !target.ws) { tx(ws, { k: 'gone', from: m.to, cid: m.cid }); return; }
      me.links.add(m.cid + '|' + m.to); target.links.add(m.cid + '|' + ws.id);
    }
    if (m.k === 'close') { me.links.delete(m.cid + '|' + m.to); if (target) target.links.delete(m.cid + '|' + ws.id); }
    if (target && target.ws) tx(target.ws, { k: m.k, from: ws.id, cid: m.cid, d: m.d });
    else if (m.k === 'data') tx(ws, { k: 'gone', from: m.to, cid: m.cid });
  });

  ws.on('close', () => {
    clearInterval(rate);
    if (!ws.id) return;
    const p = peers.get(ws.id);
    if (!p || p.ws !== ws) return;
    p.ws = null;
    p.timer = setTimeout(() => drop(ws.id), GRACE_MS);   // kurze Aussetzer überbrücken
  });
});

// tote Verbindungen aufräumen und Verbindung wach halten
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.alive) { ws.terminate(); return; }
    ws.alive = false; ws.ping();
  });
}, 20000);

server.listen(PORT, () => console.log('Radlers Boggle Server läuft auf Port ' + PORT));
