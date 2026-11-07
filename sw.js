// Звонилка: service worker (уведомления и открытие без сети)
const CACHE = 'zv-v3';
const ICON = '/icon-192.png';
self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(['/', '/manifest.webmanifest', ICON])).then(() => self.skipWaiting()));
});
self.addEventListener('activate', (e) => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k)))).then(() => self.clients.claim()));
});
self.addEventListener('fetch', (e) => {
  const u = new URL(e.request.url);
  if (e.request.method !== 'GET' || u.origin !== location.origin || u.pathname.startsWith('/api') || u.pathname.startsWith('/media') || u.pathname.startsWith('/ws')) return;
  e.respondWith(fetch(e.request).then(r => { const cp = r.clone(); caches.open(CACHE).then(c => c.put(e.request, cp)); return r; })
    .catch(() => caches.match(e.request).then(r => r || caches.match('/'))));
});
self.addEventListener('push', (e) => {
  let d = {}; try { d = e.data.json(); } catch (_) {}
  let title = 'Звонилка', o = { icon: ICON, badge: ICON, data: d };
  if (d.type === 'call') {
    title = d.name || 'Входящий звонок';
    Object.assign(o, { body: 'Входящий звонок', tag: 'call', renotify: true, requireInteraction: true, vibrate: [500, 250, 500, 250, 500, 250, 500], actions: [{ action: 'open', title: 'Ответить' }] });
  } else if (d.type === 'missed') {
    title = 'Пропущенный звонок';
    Object.assign(o, { body: d.name || '', tag: 'call', renotify: true, vibrate: [200] });
  } else {
    title = d.name || 'Новое сообщение';
    Object.assign(o, { body: d.text || '', tag: 'msg-' + d.from, renotify: true, vibrate: [120, 80, 120] });
  }
  e.waitUntil(self.registration.showNotification(title, o));
});
self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  const d = e.notification.data || {};
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
    for (const c of list) { if ('focus' in c) { c.postMessage({ t: 'open', from: d.from }); return c.focus(); } }
    return self.clients.openWindow('/?from=' + encodeURIComponent(d.from || ''));
  }));
});
