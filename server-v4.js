'use strict';
// Звонилка 3.0: сервер. Сообщения, голосовые, видео-кружочки, звонки (WebRTC), уведомления.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
let webpush = null;
try { webpush = require('web-push'); } catch (e) { console.warn('web-push не установлен: уведомления при закрытом приложении отключены'); }

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DB_FILE = path.join(DATA_DIR, 'data.json');
const MEDIA_DIR = path.join(DATA_DIR, 'media');
const PUBLIC = fs.existsSync(path.join(__dirname, 'public')) ? path.join(__dirname, 'public') : __dirname;
const FLAT = PUBLIC === __dirname;
const RING_MS = 45000;
const ADMIN_NUM = '+' + String(process.env.ADMIN_NUM || '+79063200457').replace(/\D/g, '');
const EMOJI = ['👍', '❤️', '😂', '😮', '😢', '🙏'];
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch (e) {}

// ---------- хранилище ----------
let db = { users: {}, messages: [], groups: {}, vapid: null };
try { db = Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8'))); } catch (e) {}
let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    const tmp = DB_FILE + '.tmp';
    try { fs.writeFileSync(tmp, JSON.stringify(db)); fs.renameSync(tmp, DB_FILE); } catch (e) { console.error('save:', e.message); }
  }, 400);
}
const uid = () => crypto.randomBytes(8).toString('hex');
const byId = new Map(db.messages.map(m => [m.id, m]));
const mediaIndex = new Map();
db.messages.forEach(m => { if (m.media && m.file) mediaIndex.set(m.media, { file: m.file, mime: m.mime || 'application/octet-stream', att: m.type === 'file', fname: m.fname }); });
const DEFAULT_PREFS = () => ({ pinned: [], muted: [], blocked: [], hideSeen: false, cleared: {}, groupRead: {} });
const hash = (x) => crypto.createHash('sha256').update(String(x)).digest('hex');
const isGid = (x) => /^g_[0-9a-f]{12}$/.test(String(x || ''));
const transfers = new Map();
Object.values(db.users).forEach(u => { u.prefs = Object.assign(DEFAULT_PREFS(), u.prefs || {}); });

if (webpush) {
  if (!db.vapid) { db.vapid = webpush.generateVAPIDKeys(); save(); }
  webpush.setVapidDetails(process.env.VAPID_SUBJECT || 'mailto:push@zvonilka.local', db.vapid.publicKey, db.vapid.privateKey);
}

// ---------- сессии ----------
const sessions = new Map(); // номер -> Set<ws>
const tokens = new Map();   // токен загрузки -> номер
const calls = new Map();
const recentCids = new Map();
const isOnline = (n) => !!(sessions.get(n) && sessions.get(n).size);
const isVisible = (n) => { const s = sessions.get(n); if (!s) return false; for (const w of s) if (w.visible) return true; return false; };
const wsSend = (w, o) => { if (w && w.readyState === 1) w.send(JSON.stringify(o)); };
const sendTo = (n, o, except) => { const s = sessions.get(n); if (!s) return; const str = JSON.stringify(o); for (const w of s) if (w !== except && w.readyState === 1) w.send(str); };
const normalize = (s) => { const d = String(s || '').replace(/\D/g, ''); return d.length >= 8 && d.length <= 15 ? '+' + d : null; };
function pub(n, viewer) {
  const u = db.users[n]; if (!u) return null;
  const hide = viewer !== n && (u.prefs.hideSeen || (viewer && u.prefs.blocked.includes(viewer)));
  const o = { num: n, name: u.name, bio: u.bio || '', av: u.avatarV || 0, online: hide ? false : isOnline(n), lastSeen: hide ? -1 : (u.lastSeen || 0), ver: !!u.verified || n === ADMIN_NUM };
  if (viewer === n && n === ADMIN_NUM) o.admin = true;
  return o;
}
const watchers = (n) => Object.keys(db.users).filter(k => (db.users[k].contacts || []).includes(n));
const presence = (n) => watchers(n).forEach(w => sendTo(w, { t: 'presence', user: pub(n, w) }));
function addContact(a, b) {
  const u = db.users[a]; if (!u) return;
  u.contacts = u.contacts || [];
  if (!u.contacts.includes(b)) { u.contacts.push(b); save(); }
}
const snippet = (m) => m.deleted ? 'Сообщение удалено' : m.type === 'voice' ? 'Голосовое сообщение' : m.type === 'circle' ? 'Видеосообщение' : m.type === 'image' ? 'Фото' : m.type === 'video' ? 'Видео' : m.type === 'file' ? 'Файл: ' + (m.fname || '') : m.type === 'call' ? 'Звонок' : String(m.text || '').slice(0, 80);

// ---------- push ----------
function pushTo(n, payload, ttl) {
  const u = db.users[n]; if (!webpush || !u || !u.subs || !u.subs.length) return;
  const body = JSON.stringify(payload);
  u.subs.slice().forEach(sub => {
    webpush.sendNotification(sub, body, { TTL: ttl || 3600, urgency: 'high' }).catch(err => {
      if (err && (err.statusCode === 404 || err.statusCode === 410)) { u.subs = u.subs.filter(s => s.endpoint !== sub.endpoint); save(); }
    });
  });
}

// ---------- сообщения ----------
function addMessage(m) { db.messages.push(m); byId.set(m.id, m); addContact(m.from, m.to); addContact(m.to, m.from); save(); }
function setStatus(ids, status, notifyUser) {
  if (!ids.length) return;
  ids.forEach(id => { const m = byId.get(id); if (m) m.status = status; });
  save(); sendTo(notifyUser, { t: 'status', ids, status });
}
const chatKinds = ['text', 'voice', 'circle', 'image', 'video', 'file'];
const gPub = (g) => ({ id: g.id, name: g.name, owner: g.owner, admins: g.admins || [], members: g.members, created: g.created });
const groupsOf = (n) => Object.values(db.groups).filter(g => g.members.includes(n));
const inGroup = (n, id) => !!(db.groups[id] && db.groups[id].members.includes(n));
const sendGroup = (g, o, except) => g.members.forEach(n => sendTo(n, o, except));
const usFor = (g, viewer) => g.members.map(n => pub(n, viewer)).filter(Boolean);
function sendGroupInfo(g, only) { (only || g.members).forEach(n => sendTo(n, { t: 'group', g: gPub(g), us: usFor(g, n) })); }
function sysMsg(g, text) {
  const m = { id: uid(), from: 'sys', to: g.id, type: 'sys', ts: Date.now(), status: 'read', text };
  db.messages.push(m); byId.set(m.id, m); save(); sendGroup(g, { t: 'msg', m });
}
function deliverGroup(me, gid, fields, cid) {
  const g = db.groups[gid]; if (!g || !g.members.includes(me)) return null;
  const key = cid ? me + '|' + cid : null;
  if (key && recentCids.has(key)) { const old = byId.get(recentCids.get(key)); if (old) { sendTo(me, { t: 'msg', m: old, cid }); return old; } }
  const msg = Object.assign({ id: uid(), from: me, to: gid, ts: Date.now(), status: 'delivered' }, fields);
  if (cid) msg.cid = cid;
  if (fields.reply) { const r = byId.get(fields.reply); msg.reply = r && r.to === gid ? { id: r.id, from: r.from, text: snippet(r) } : undefined; if (!msg.reply) delete msg.reply; }
  db.messages.push(msg); byId.set(msg.id, msg); save();
  if (key) { recentCids.set(key, msg.id); setTimeout(() => recentCids.delete(key), 600000); }
  sendTo(me, { t: 'msg', m: msg, cid });
  g.members.forEach(n => {
    if (n === me) return; const t = db.users[n]; if (!t) return;
    sendTo(n, { t: 'msg', m: msg, us: [pub(me, n)] });
    if (!isVisible(n) && !t.prefs.muted.includes(gid) && !t.prefs.blocked.includes(me)) pushTo(n, { type: 'msg', from: gid, name: g.name + ': ' + db.users[me].name, text: snippet(msg) }, 86400);
  });
  return msg;
}
function deliver(me, to, fields, cid) {
  const key = cid ? me + '|' + cid : null;
  if (key && recentCids.has(key)) {
    const old = byId.get(recentCids.get(key));
    if (old) { sendTo(me, { t: 'msg', m: old, cid, us: [pub(to, me)] }); return old; }
  }
  const target = db.users[to];
  const blocked = target.prefs.blocked.includes(me);
  const msg = Object.assign({ id: uid(), from: me, to, ts: Date.now(), status: (!blocked && isOnline(to)) ? 'delivered' : 'sent' }, fields);
  if (cid) msg.cid = cid;
  if (fields.reply) {
    const r = byId.get(fields.reply);
    msg.reply = r && ((r.from === me && r.to === to) || (r.from === to && r.to === me)) ? { id: r.id, from: r.from, text: snippet(r) } : undefined;
    if (!msg.reply) delete msg.reply;
  }
  addMessage(msg);
  if (key) { recentCids.set(key, msg.id); setTimeout(() => recentCids.delete(key), 600000); }
  sendTo(me, { t: 'msg', m: msg, cid, us: [pub(to, me)] });
  if (!blocked) {
    sendTo(to, { t: 'msg', m: msg, us: [pub(me, to)] });
    if (!isVisible(to) && !target.prefs.muted.includes(me)) pushTo(to, { type: 'msg', from: me, name: db.users[me].name, text: snippet(msg) }, 86400);
  }
  return msg;
}
const broadcastUpd = (m) => { if (isGid(m.to)) { const g = db.groups[m.to]; if (g) sendGroup(g, { t: 'upd', m }); return; } sendTo(m.from, { t: 'upd', m }); sendTo(m.to, { t: 'upd', m }); };
function dropMedia(m) {
  if (m.media) { const e = mediaIndex.get(m.media); if (e) fs.unlink(path.join(MEDIA_DIR, e.file), () => {}); mediaIndex.delete(m.media); }
  delete m.media; delete m.file; delete m.wave;
}

// ---------- звонки ----------
function busy(n) { for (const c of calls.values()) if ((c.from === n || c.to === n) && c.state === 'active') return true; return false; }
function endCall(call, result) {
  if (!calls.has(call.id)) return;
  clearTimeout(call.timer); calls.delete(call.id);
  const dur = call.state === 'active' ? Math.max(1, Math.round((Date.now() - call.startedAt) / 1000)) : 0;
  const res = call.state === 'active' ? 'done' : result;
  const note = { t: 'ended', id: call.id, result: res, dur };
  clearTimeout(call.grace); wsSend(call.callerWs, note); if (!call.callerWs) sendTo(call.from, note);
  if (call.calleeWs) wsSend(call.calleeWs, note); else sendTo(call.to, note);
  const m = { id: uid(), type: 'call', from: call.from, to: call.to, ts: Date.now(), status: 'read', call: { result: res, dur } };
  addMessage(m);
  sendTo(call.from, { t: 'msg', m, us: [pub(call.to, call.from)] }); sendTo(call.to, { t: 'msg', m, us: [pub(call.from, call.to)] });
  if (res === 'missed' && !isVisible(call.to)) pushTo(call.to, { type: 'missed', from: call.from, name: db.users[call.from].name }, 86400);
}
const callOf = (ws, id) => { const c = calls.get(id); return c && (c.from === ws.num || c.to === ws.num) ? c : null; };

// ---------- HTTP ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css', '.svg': 'image/svg+xml', '.png': 'image/png', '.json': 'application/json', '.webmanifest': 'application/manifest+json' };
function serveMedia(req, res, id) {
  const e = mediaIndex.get(id); if (!e) { res.writeHead(404); return res.end(); }
  const fp = path.join(MEDIA_DIR, e.file);
  fs.stat(fp, (err, st) => {
    if (err) { res.writeHead(404); return res.end(); }
    const size = st.size, head = { 'Content-Type': e.att ? 'application/octet-stream' : e.mime, 'Accept-Ranges': 'bytes', 'Cache-Control': 'private, max-age=86400', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': 'sandbox' };
    if (e.att) head['Content-Disposition'] = "attachment; filename*=UTF-8''" + encodeURIComponent(e.fname || 'file');
    const r = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
    if (r) {
      let start = r[1] === '' ? size - parseInt(r[2], 10) : parseInt(r[1], 10), end = r[1] !== '' && r[2] !== '' ? parseInt(r[2], 10) : size - 1;
      if (!(start >= 0) || start > end || start >= size) { res.writeHead(416, { 'Content-Range': 'bytes */' + size }); return res.end(); }
      end = Math.min(end, size - 1);
      res.writeHead(206, Object.assign(head, { 'Content-Range': `bytes ${start}-${end}/${size}`, 'Content-Length': end - start + 1 }));
      return fs.createReadStream(fp, { start, end }).pipe(res);
    }
    res.writeHead(200, Object.assign(head, { 'Content-Length': size }));
    fs.createReadStream(fp).pipe(res);
  });
}
const UP = {
  voice: { max: 3 * 1024 * 1024, ok: (m) => /^audio\//.test(m) },
  circle: { max: 14 * 1024 * 1024, ok: (m) => /^video\//.test(m) },
  image: { max: 10 * 1024 * 1024, ok: (m) => /^image\/(jpeg|png|webp|gif)$/.test(m.split(';')[0]) },
  video: { max: 40 * 1024 * 1024, ok: (m) => /^video\/(mp4|webm|quicktime|x-matroska|3gpp)$/.test(m.split(';')[0]) },
  file: { max: 25 * 1024 * 1024, ok: () => true }
};
function uploadMedia(req, res, q) {
  const json = (code, o) => { res.writeHead(code, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(o)); };
  const me = tokens.get(q.get('tok')); const rawTo = q.get('to'), kind = q.get('kind');
  if (!me) return json(401, { error: 'auth' });
  const grp = isGid(rawTo), to = grp ? rawTo : normalize(rawTo), spec = UP[kind];
  if (!spec || !to || to === me || (grp ? !inGroup(me, to) : !db.users[to])) return json(400, { error: 'bad' });
  const mime = String(req.headers['content-type'] || '').toLowerCase();
  if (!spec.ok(mime)) return json(400, { error: 'mime' });
  const max = spec.max, chunks = []; let size = 0, dead = false;
  req.on('data', (c) => { size += c.length; if (size > max) { dead = true; json(413, { error: 'big' }); req.destroy(); return; } chunks.push(c); });
  req.on('end', () => {
    if (dead) return;
    const id = uid(), m0 = mime.split(';')[0];
    const ext = kind === 'file' ? 'bin' : /mp4/.test(m0) ? 'mp4' : /quicktime/.test(m0) ? 'mov' : /matroska/.test(m0) ? 'mkv' : /3gpp/.test(m0) ? '3gp' : /ogg/.test(m0) ? 'ogg' : /png/.test(m0) ? 'png' : /gif/.test(m0) ? 'gif' : /webp/.test(m0) ? 'webp' : /jpeg/.test(m0) ? 'jpg' : 'webm', file = id + '.' + ext;
    try { fs.writeFileSync(path.join(MEDIA_DIR, file), Buffer.concat(chunks)); } catch (e) { return json(500, { error: 'disk' }); }
    const fname = String(q.get('fname') || '').replace(/[\\/:*?"<>|\x00-\x1f]/g, '_').slice(0, 80);
    mediaIndex.set(id, { file, mime: kind === 'file' ? 'application/octet-stream' : mime, att: kind === 'file', fname });
    const fields = { type: kind, media: id, file, mime: kind === 'file' ? 'application/octet-stream' : mime, size };
    if (kind === 'voice' || kind === 'circle' || kind === 'video') fields.dur = Math.max(0, Math.min(kind === 'voice' ? 600 : kind === 'circle' ? 60 : 3600, parseFloat(q.get('dur')) || 0));
    if (kind === 'voice') { const wave = String(q.get('wave') || '').split(',').map(Number).filter(Number.isFinite).slice(0, 64).map(v => Math.max(0, Math.min(31, Math.round(v)))); if (wave.length) fields.wave = wave; }
    if (fname && (kind === 'file' || kind === 'image' || kind === 'video')) fields.fname = fname;
    const w = parseInt(q.get('w'), 10), h = parseInt(q.get('h'), 10); if (w > 0 && h > 0 && w < 20000 && h < 20000) { fields.w = w; fields.h = h; }
    if (q.get('reply')) fields.reply = q.get('reply');
    const m = grp ? deliverGroup(me, to, fields, q.get('cid') || null) : deliver(me, to, fields, q.get('cid') || null);
    json(200, { ok: !!m, id: m && m.id });
  });
  req.on('error', () => {});
}
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range');
  if (req.method === 'OPTIONS') { res.writeHead(204); return res.end(); }
  const parsed = new URL(req.url, 'http://x');
  let url; try { url = decodeURIComponent(parsed.pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (req.method === 'POST' && url === '/api/media') return uploadMedia(req, res, parsed.searchParams);
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/api/config') {
    const ice = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];
    if (process.env.TURN_URL) ice.push({ urls: process.env.TURN_URL.split(','), username: process.env.TURN_USER || '', credential: process.env.TURN_PASS || '' });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ vapid: db.vapid ? db.vapid.publicKey : null, ice }));
  }
  if (url === '/api/find') {
    const q = parsed.searchParams, me = normalize(q.get('me')), raw = String(q.get('q') || '').trim();
    const j = (code, o) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o)); };
    if (!me || !db.users[me]) return j(401, { error: 'auth' });
    const n = normalize(raw);
    if (n && /^[+\d\s()-]+$/.test(raw)) {
      if (n === me) return j(200, { self: true, users: [] });
      const p = pub(n, me); if (p) { addContact(me, n); addContact(n, me); }
      return j(200, { exact: p || null, users: [] });
    }
    const lq = raw.toLowerCase().slice(0, 40), out = [];
    if (lq.length >= 2) for (const k of Object.keys(db.users)) { if (k === me || db.users[k].prefs.blocked.includes(me)) continue; if (String(db.users[k].name || '').toLowerCase().includes(lq)) { out.push(pub(k, me)); if (out.length >= 20) break; } }
    return j(200, { users: out });
  }
  const mm = /^\/media\/([0-9a-f]{16})$/.exec(url);
  if (mm) return serveMedia(req, res, mm[1]);
  const am = /^\/avatar\/(\d{8,15})\.jpg$/.exec(url);
  if (am) {
    const u = db.users['+' + am[1]];
    if (!u || !u.avatar) { res.writeHead(404); return res.end(); }
    res.writeHead(200, { 'Content-Type': 'image/jpeg', 'Cache-Control': 'public, max-age=31536000, immutable' });
    return res.end(Buffer.from(u.avatar, 'base64'));
  }
  const pick = () => { for (const n of ['app4.html', 'app.html', 'index.html']) if (fs.existsSync(path.join(PUBLIC, n))) return n; return 'index.html'; };
  const file = path.join(PUBLIC, url === '/' ? pick() : url);
  if (!file.startsWith(PUBLIC + path.sep) || (FLAT && !/^((index|app\d*)\.html|sw\.js|manifest\.webmanifest|icon[-\w]*\.(png|svg))$/.test(path.basename(file)))) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(data);
  });
});

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 256 * 1024 });
wss.on('connection', (ws) => {
  ws.num = null; ws.visible = true; ws.alive = true; ws.token = null;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (raw) => { let m; try { m = JSON.parse(raw); } catch (e) { return; } try { handle(ws, m); } catch (e) { console.error('handle', e); } });
  ws.on('close', () => onClose(ws));
});

function handle(ws, m) {
  if (m.t === 'ping') return wsSend(ws, { t: 'pong' });
  if (m.t === 'auth') return onAuth(ws, m);
  if (!ws.num) return;
  const me = ws.num, u = db.users[me];
  switch (m.t) {
    case 'vis': ws.visible = !!m.v; break;
    case 'profile': {
      const name = String(m.name || '').trim().slice(0, 40);
      if (name) u.name = name;
      if (typeof m.bio === 'string') u.bio = m.bio.trim().slice(0, 80);
      save(); sendTo(me, { t: 'me', user: pub(me, me) }); presence(me); break;
    }
    case 'avatar': {
      if (m.data === null) { u.avatar = ''; u.avatarV = 0; }
      else {
        const mt = /^data:image\/jpeg;base64,([A-Za-z0-9+\/=]+)$/.exec(String(m.data || ''));
        if (!mt || mt[1].length > 150000) return wsSend(ws, { t: 'err' });
        u.avatar = mt[1]; u.avatarV = Date.now();
      }
      save(); sendTo(me, { t: 'me', user: pub(me, me) }); presence(me); break;
    }
    case 'pref': {
      const k = m.key, v = m.value;
      if (['pinned', 'muted'].includes(k) && Array.isArray(v)) u.prefs[k] = [...new Set(v.map(x => isGid(x) ? x : normalize(x)).filter(x => x && x !== me))].slice(0, 500);
      else if (k === 'blocked' && Array.isArray(v)) u.prefs[k] = [...new Set(v.map(normalize).filter(x => x && x !== me))].slice(0, 500);
      else if (k === 'groupRead' && v && typeof v === 'object') { const o = {}; Object.keys(v).forEach(kk => { if (isGid(kk) && Number.isFinite(v[kk])) o[kk] = v[kk]; }); u.prefs.groupRead = o; }
      else if (k === 'hideSeen') u.prefs.hideSeen = !!v;
      else if (k === 'cleared' && v && typeof v === 'object') { const o = {}; Object.keys(v).forEach(kk => { const n = isGid(kk) ? kk : normalize(kk); if (n && Number.isFinite(v[kk])) o[n] = v[kk]; }); u.prefs.cleared = o; }
      else return;
      save(); sendTo(me, { t: 'prefs', prefs: u.prefs }, ws);
      if (k === 'hideSeen' || k === 'blocked') presence(me);
      break;
    }
    case 'testpush': {
      const n = (u.subs || []).length;
      if (!webpush) return wsSend(ws, { t: 'testres', ok: false, why: 'На сервере не установлен модуль уведомлений' });
      if (!n) return wsSend(ws, { t: 'testres', ok: false, why: 'Этот телефон не подписан на уведомления' });
      pushTo(me, { type: 'msg', from: me, name: 'Звонилка', text: 'Проверка: уведомления работают' }, 60); wsSend(ws, { t: 'testres', ok: true }); break;
    }
    case 'find': {
      const raw = String(m.q || '').trim(), q = raw.toLowerCase().slice(0, 40), out = [];
      if (q.length >= 2) for (const k of Object.keys(db.users)) { if (k === me) continue; const x = db.users[k]; if (x.prefs.blocked.includes(me)) continue; if (String(x.name || '').toLowerCase().includes(q)) { out.push(pub(k, me)); if (out.length >= 20) break; } }
      wsSend(ws, { t: 'found', q: raw, users: out }); break;
    }
    case 'lookup': {
      const n = normalize(m.num);
      if (!n) return wsSend(ws, { t: 'user', num: m.num, user: null });
      if (n === me) return wsSend(ws, { t: 'user', num: n, user: null, self: true });
      const p = pub(n, me);
      if (p) { addContact(me, n); addContact(n, me); }
      wsSend(ws, { t: 'user', num: n, user: p });
      break;
    }
    case 'msg': {
      const text = String(m.text || '').trim().slice(0, 4000), cid = m.cid ? String(m.cid).slice(0, 40) : null;
      if (isGid(m.to)) { if (!text || !deliverGroup(me, m.to, { type: 'text', text, reply: m.reply ? String(m.reply) : undefined }, cid)) wsSend(ws, { t: 'err', cid: m.cid }); break; }
      const to = normalize(m.to);
      if (!to || !text || !db.users[to] || to === me) return wsSend(ws, { t: 'err', cid: m.cid });
      deliver(me, to, { type: 'text', text, reply: m.reply ? String(m.reply) : undefined }, cid);
      break;
    }
    case 'edit': {
      const x = byId.get(m.id), text = String(m.text || '').trim().slice(0, 4000);
      if (!x || x.from !== me || x.type !== 'text' || x.deleted || !text) return;
      x.text = text; x.edited = true; save(); broadcastUpd(x); break;
    }
    case 'del': {
      const x = byId.get(m.id); if (!x || x.from !== me || x.type === 'call' || x.deleted) return;
      dropMedia(x); x.deleted = true; delete x.text; delete x.reactions; save(); broadcastUpd(x); break;
    }
    case 'react': {
      const x = byId.get(m.id); if (!x || (x.from !== me && x.to !== me && !inGroup(me, x.to)) || x.deleted || x.type === 'call' || x.type === 'sys') return;
      x.reactions = x.reactions || {};
      if (m.emoji && EMOJI.includes(m.emoji) && x.reactions[me] !== m.emoji) x.reactions[me] = m.emoji; else delete x.reactions[me];
      if (!Object.keys(x.reactions).length) delete x.reactions;
      save(); broadcastUpd(x); break;
    }
    case 'read': {
      const peer = normalize(m.peer); if (!peer) return;
      const ids = db.messages.filter(x => chatKinds.includes(x.type) && x.from === peer && x.to === me && x.status !== 'read').map(x => x.id);
      if (ids.length) { setStatus(ids, 'read', peer); sendTo(me, { t: 'status', ids, status: 'read' }); }
      break;
    }
    case 'typing': { const to = normalize(m.to); if (to && !(db.users[to] && db.users[to].prefs.blocked.includes(me))) sendTo(to, { t: 'typing', from: me }); break; }
    case 'sub': {
      if (!m.sub || !m.sub.endpoint) return;
      u.subs = (u.subs || []).filter(s => s.endpoint !== m.sub.endpoint); u.subs.push(m.sub); if (u.subs.length > 8) u.subs.shift(); save(); break;
    }
    case 'unsub': u.subs = (u.subs || []).filter(s => s.endpoint !== m.endpoint); save(); break;

    // ----- безопасность и админ -----
    case 'xfer': {
      const code = String(crypto.randomInt(100000, 1000000)); transfers.set(me, { code, exp: Date.now() + 300000, tries: 0 });
      wsSend(ws, { t: 'xfercode', code, ttl: 300 }); break;
    }
    case 'admin': {
      if (me !== ADMIN_NUM) return;
      const n = normalize(m.num), x = n && db.users[n];
      if (!x) return wsSend(ws, { t: 'adminres', text: 'Такого номера нет в Звонилке' });
      if (m.op === 'verify') { x.verified = !!m.on; save(); presence(n); sendTo(n, { t: 'me', user: pub(n, n) }); wsSend(ws, { t: 'adminres', text: (m.on ? 'Галочка выдана: ' : 'Галочка снята: ') + n }); }
      else if (m.op === 'reset') { delete x.secret; transfers.delete(n); const s2 = sessions.get(n); if (s2) [...s2].forEach(w => { wsSend(w, { t: 'kicked' }); try { w.close(); } catch (e) {} }); save(); wsSend(ws, { t: 'adminres', text: 'Привязка сброшена: ' + n + '. Следующий вход займёт номер' }); }
      break;
    }

    // ----- группы -----
    case 'gcreate': {
      const name = String(m.name || '').trim().slice(0, 40); if (!name) return;
      const mem = [...new Set((Array.isArray(m.members) ? m.members : []).map(normalize).filter(x => x && x !== me && db.users[x] && !db.users[x].prefs.blocked.includes(me)))].slice(0, 199);
      if (!mem.length) return wsSend(ws, { t: 'err' });
      const g = { id: 'g_' + crypto.randomBytes(6).toString('hex'), name, owner: me, admins: [], members: [me, ...mem], created: Date.now() };
      db.groups[g.id] = g; save(); sendGroupInfo(g); sysMsg(g, u.name + ' создал(а) группу «' + name + '»'); break;
    }
    case 'gadd': {
      const g = db.groups[m.id], n = normalize(m.num); if (!g || !n || !db.users[n] || g.members.includes(n)) return;
      if (g.owner !== me && !(g.admins || []).includes(me)) return; if (g.members.length >= 200 || db.users[n].prefs.blocked.includes(me)) return;
      g.members.push(n); save(); sendGroupInfo(g); sysMsg(g, u.name + ' добавил(а) ' + db.users[n].name); break;
    }
    case 'gkick': {
      const g = db.groups[m.id], n = normalize(m.num); if (!g || !n || !g.members.includes(n) || n === g.owner) return;
      if (g.owner !== me && !(g.admins || []).includes(me)) return;
      g.members = g.members.filter(x => x !== n); g.admins = (g.admins || []).filter(x => x !== n); save();
      sendTo(n, { t: 'groupgone', id: g.id }); sendGroupInfo(g); sysMsg(g, u.name + ' удалил(а) ' + db.users[n].name); break;
    }
    case 'gleave': {
      const g = db.groups[m.id]; if (!g || !g.members.includes(me)) return;
      g.members = g.members.filter(x => x !== me); g.admins = (g.admins || []).filter(x => x !== me);
      sendTo(me, { t: 'groupgone', id: g.id });
      if (!g.members.length) { delete db.groups[g.id]; db.messages = db.messages.filter(x => x.to !== g.id); } else { if (g.owner === me) g.owner = g.members[0]; sendGroupInfo(g); sysMsg(g, u.name + ' покинул(а) группу'); }
      save(); break;
    }
    case 'gname': {
      const g = db.groups[m.id], name = String(m.name || '').trim().slice(0, 40); if (!g || !name || !g.members.includes(me)) return;
      if (g.owner !== me && !(g.admins || []).includes(me)) return; g.name = name; save(); sendGroupInfo(g); sysMsg(g, u.name + ' переименовал(а) группу в «' + name + '»'); break;
    }

    // ----- звонки -----
    case 'call': {
      const to = normalize(m.to);
      if (!to || !db.users[to] || to === me) return wsSend(ws, { t: 'callfail', reason: 'nouser' });
      if (busy(me) || busy(to) || db.users[to].prefs.blocked.includes(me)) return wsSend(ws, { t: 'callfail', reason: 'busy' });
      const call = { id: uid(), from: me, to, state: 'ringing', ts: Date.now(), callerWs: ws, calleeWs: null };
      call.timer = setTimeout(() => endCall(call, 'missed'), RING_MS);
      calls.set(call.id, call);
      wsSend(ws, { t: 'calling', id: call.id });
      sendTo(to, { t: 'incoming', id: call.id, from: me, name: u.name });
      if (!isVisible(to)) pushTo(to, { type: 'call', id: call.id, from: me, name: u.name }, 45);
      break;
    }
    case 'accept': {
      const c = callOf(ws, m.id);
      if (!c || c.to !== me || c.state !== 'ringing') return wsSend(ws, { t: 'ended', id: m.id, result: 'gone' });
      if (!c.callerWs || c.callerWs.readyState !== 1) return endCall(c, 'missed');
      clearTimeout(c.timer); c.state = 'active'; c.startedAt = Date.now(); c.calleeWs = ws;
      wsSend(c.callerWs, { t: 'accepted', id: c.id });
      sendTo(me, { t: 'taken', id: c.id }, ws);
      break;
    }
    case 'decline': { const c = callOf(ws, m.id); if (c && c.to === me && c.state === 'ringing') endCall(c, 'declined'); break; }
    case 'hangup': {
      const c = callOf(ws, m.id); if (!c) return;
      if (c.state === 'ringing') endCall(c, c.from === me ? 'missed' : 'declined'); else endCall(c, 'done');
      break;
    }
    case 'sig': {
      const c = callOf(ws, m.id); if (!c || c.state !== 'active') return;
      const other = ws === c.callerWs ? c.calleeWs : c.callerWs;
      if (other) wsSend(other, { t: 'sig', id: c.id, data: m.data }); else sendTo(ws === c.callerWs ? c.to : c.from, { t: 'sig', id: c.id, data: m.data });
      break;
    }
  }
}

function onAuth(ws, m) {
  const num = normalize(m.num);
  if (!num) return wsSend(ws, { t: 'autherr', text: 'Неверный номер' });
  const sec = String(m.sec || '');
  if (sec.length < 16) return wsSend(ws, { t: 'autherr', text: 'Обновите страницу приложения и попробуйте снова' });
  const name = String(m.name || '').trim().slice(0, 40);
  let u = db.users[num];
  if (!u) u = db.users[num] = { name: name || num, createdAt: Date.now(), lastSeen: Date.now(), contacts: [], subs: [], prefs: DEFAULT_PREFS(), secret: hash(sec) };
  else if (!u.secret) u.secret = hash(sec);
  else if (u.secret !== hash(sec)) {
    const tr = transfers.get(num), code = String(m.code || '').trim();
    if (code && tr && tr.exp > Date.now() && ++tr.tries <= 5 && tr.code === code) {
      transfers.delete(num); u.secret = hash(sec);
      const old = sessions.get(num); if (old) [...old].forEach(w => { wsSend(w, { t: 'kicked' }); try { w.close(); } catch (e) {} });
    } else {
      if (tr && tr.tries > 5) transfers.delete(num);
      return wsSend(ws, { t: 'autherr', code: 'taken', text: code ? 'Код неверный или устарел' : 'Этот номер уже занят на другом устройстве. Если это ваш номер, откройте Звонилку на старом устройстве: Настройки → «Перенести на другое устройство» и введите код ниже.' });
    }
  }
  if (name && m.rename) u.name = name;
  ws.num = num; ws.visible = m.visible !== false; ws.token = uid() + uid(); tokens.set(ws.token, num);
  if (!sessions.has(num)) sessions.set(num, new Set());
  sessions.get(num).add(ws);
  for (const c of calls.values()) if (c.state === 'active') { if (c.from === num && !c.callerWs) c.callerWs = ws; else if (c.to === num && !c.calleeWs) c.calleeWs = ws; if (c.callerWs && c.calleeWs) clearTimeout(c.grace); }
  u.lastSeen = Date.now(); save();

  const bySender = {};
  db.messages.forEach(x => { if (chatKinds.includes(x.type) && x.to === num && x.status === 'sent' && !(db.users[x.from] && db.users[num].prefs.blocked.includes(x.from))) (bySender[x.from] = bySender[x.from] || []).push(x.id); });
  Object.keys(bySender).forEach(s => setStatus(bySender[s], 'delivered', s));

  const mine = db.messages.filter(x => (isGid(x.to) ? inGroup(num, x.to) : ((x.from === num || x.to === num) && !(x.to === num && u.prefs.blocked.includes(x.from))))).slice(-2000);
  const contacts = {}, myGroups = groupsOf(num);
  (u.contacts || []).forEach(c => { const p = pub(c, num); if (p) contacts[c] = p; });
  myGroups.forEach(g => g.members.forEach(c => { if (c !== num) { const p = pub(c, num); if (p) contacts[c] = contacts[c] || p; } }));
  wsSend(ws, { t: 'hello', me: pub(num, num), contacts, msgs: mine, prefs: u.prefs, groups: myGroups.map(gPub), token: ws.token });
  for (const c of calls.values()) if (c.to === num && c.state === 'ringing') wsSend(ws, { t: 'incoming', id: c.id, from: c.from, name: (db.users[c.from] || {}).name });
  presence(num);
}

function onClose(ws) {
  if (ws.token) tokens.delete(ws.token);
  const num = ws.num; if (!num) return;
  const s = sessions.get(num);
  if (s) { s.delete(ws); if (!s.size) sessions.delete(num); }
  for (const c of [...calls.values()]) {
    if (c.state === 'active' && (c.callerWs === ws || c.calleeWs === ws)) {
      if (c.callerWs === ws) c.callerWs = null; else c.calleeWs = null;
      clearTimeout(c.grace); c.grace = setTimeout(() => { if (calls.has(c.id) && (!c.callerWs || !c.calleeWs)) endCall(c, 'done'); }, 90000);
    } else if (c.callerWs === ws) endCall(c, 'missed');
    else if (c.calleeWs === ws) endCall(c, 'done');
  }
  if (!isOnline(num)) { db.users[num].lastSeen = Date.now(); save(); presence(num); }
}

setInterval(() => { wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; try { ws.ping(); } catch (e) {} }); }, 25000);
server.listen(PORT, () => console.log('Звонилка 3.0 запущена на порту ' + PORT));
