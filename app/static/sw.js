/* BQA-ONE / Energix360 - Service Worker v14.0-PnP
   - App shell cache-first
   - GLP APIs network-first
   - HTML navegaciones NETWORK-FIRST (Solución Usuario Fantasma)
   - MODO OFFLINE GARANTIZADO PLUG & PLAY
*/

// 1. CAMBIO DE VERSIÓN PARA FORZAR ACTUALIZACIÓN Y PURGA DE CACHÉ OBSOLETA
const CACHE_STATIC = "bqa-one-shell-v14.0";
const CACHE_DYNAMIC = "bqa-one-dyn-v14.0";

// ===== MODO OFFLINE GARANTIZADO =====
let FORCE_OFFLINE = false;

self.addEventListener("message", (event) => {
  const data = event.data || {};
  
  if (data.type === "GLP_FORCE_OFFLINE") {
    FORCE_OFFLINE = !!data.value;
    console.log("[SW v14] FORCE_OFFLINE =", FORCE_OFFLINE);

    if (!FORCE_OFFLINE) {
      flushOfflineQueue();
    }

    try {
      if (event.source && typeof event.source.postMessage === "function") {
        event.source.postMessage({
          type: "GLP_FORCE_OFFLINE_ACK",
          value: FORCE_OFFLINE
        });
      }
    } catch (e) {}
  }
});

const APP_SHELL = [
  "/",                              
  "/login_energix360_offline.html", 
  "/B_operador_glp_offline.html",
  "/static/manifest.json",
  "/static/BQA_ONE_192.png",
  "/static/BQA_ONE_512.png",
  "/static/logo_energix360.png",
  "/static/glp_offline_styles.css",
  "/static/glp_offline_script.js",
  "/static/js/html5-qrcode.min.js"
];

const MAX_DYNAMIC_ITEMS = 60;

async function limitCacheSize(cacheName, maxItems) {
  const cache = await caches.open(cacheName);
  const keys = await cache.keys();
  if (keys.length > maxItems) {
    await cache.delete(keys[0]);
    return limitCacheSize(cacheName, maxItems);
  }
}

// INSTALL
self.addEventListener("install", (event) => {
  console.log("[SW v14] install");
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_STATIC);
      for (const url of APP_SHELL) {
        try {
          await cache.add(url);
        } catch (err) {
          console.warn("[SW v14] NO se pudo cachear", url, err);
        }
      }
    })()
  );
  self.skipWaiting();
});

// ACTIVATE
self.addEventListener("activate", (event) => {
  console.log("[SW v14] activate");
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => ![CACHE_STATIC, CACHE_DYNAMIC].includes(k))
          .map((k) => {
            console.log("[SW v14] borrando cache vieja:", k);
            return caches.delete(k);
          })
      )
    )
  );
  self.clients.claim();
});

// FETCH
self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  if (url.origin !== location.origin) return;

  // Bloqueo de mutaciones en modo offline forzado
  if (FORCE_OFFLINE && req.method !== "GET") {
    event.respondWith(
      new Response("offline-forced", {
        status: 503,
        headers: { "Content-Type": "text/plain" }
      })
    );
    return;
  }

  // Nunca cachear POST/PUT/DELETE
  if (req.method !== "GET") {
    event.respondWith(fetch(req));
    return;
  }

  const accept = req.headers.get("accept") || "";
  const isHTML = req.mode === "navigate" || accept.includes("text/html");
  const path = url.pathname || "";

  // Redirección de compatibilidad en caché para rutas HTML renombradas a Plug & Play
  if (path === "/glp_offline.html" || path === "/890707006_offline.html" || path === "/offline.html") {
    event.respondWith(
      caches.match("/B_operador_glp_offline.html").then((res) => {
        return res || fetch("/B_operador_glp_offline.html");
      })
    );
    return;
  }

  // Redirección de compatibilidad para icono legacy icon-192.png
  if (path === "/static/icon-192.png") {
    event.respondWith(
      caches.match("/static/BQA_ONE_192.png").then((res) => {
        return res || fetch("/static/BQA_ONE_192.png");
      })
    );
    return;
  }

  // ===== GLP APIs (JSON bajo /glp/) → NETWORK-FIRST =====
  if (path.startsWith("/glp/") && !isHTML) {
    event.respondWith(
      (async () => {
        try {
          const res = await fetch(req);
          const copy = res.clone();
          caches.open(CACHE_DYNAMIC).then((cache) => {
            cache.put(req, copy);
            limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
          });
          return res;
        } catch (err) {
          const cached = await caches.match(req);
          if (cached) return cached;
          return new Response(JSON.stringify({ success: false, offline: true }), {
            status: 503, headers: { "Content-Type": "application/json" }
          });
        }
      })()
    );
    return;
  }

  // ===== HTML / Navegación: SOLUCIÓN CRÍTICA (NETWORK-FIRST) =====
  if (isHTML) {
    event.respondWith(
      (async () => {
        try {
          // 1. INTENTO DE RED (Para obtener usuario real y salt de login)
          const res = await fetch(req);
          const copy = res.clone();

          // Guardamos copia nueva en background para futuro offline
          caches.open(CACHE_DYNAMIC).then((cache) => {
            cache.put(req, copy);
            limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
          });

          return res;

        } catch (err) {
          console.warn("[SW v14] Sin red, buscando fallback para HTML:", path);

          // 2. FALLBACK A CACHÉ (Solo si no hay red)
          let cached = await caches.match(req);
          if (!cached) {
            cached = await caches.match(path);
          }
          if (cached) return cached;

          // 3. FALLBACKS GENÉRICOS (Pantallas Offline P&P)
          if (path === "/") {
            const rootCached = await caches.match("/");
            if (rootCached) return rootCached;
          }

          const offlineGlpCached = await caches.match("/B_operador_glp_offline.html");
          if (offlineGlpCached) return offlineGlpCached;

          const loginOfflineCached = await caches.match("/login_energix360_offline.html");
          if (loginOfflineCached) return loginOfflineCached;

          return new Response(
            "<!DOCTYPE html><html><body><h2>Sin conexión</h2><p>No se pudo contactar al servidor y no hay copia guardada.</p></body></html>",
            { status: 503, headers: { "Content-Type": "text/html" } }
          );
        }
      })()
    );
    return;
  }

  // ===== ESTÁTICOS (JS/CSS/IMG) → CACHE-FIRST =====
  event.respondWith(
    (async () => {
      const cached = await caches.match(req);
      if (cached) return cached;
      try {
        const res = await fetch(req);
        const copy = res.clone();
        caches.open(CACHE_DYNAMIC).then((cache) => {
          cache.put(req, copy);
          limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
        });
        return res;
      } catch (err) {
        return new Response("", { status: 504 });
      }
    })()
  );
});

// BACKGROUND SYNC
self.addEventListener("sync", (event) => {
  if (event.tag === "sync-glp-queue") {
    event.waitUntil(
      (async () => {
        const clients = await self.clients.matchAll();
        for (const client of clients) {
          client.postMessage({ type: "BQA_GLPSYNC", action: "flushQueue" });
        }
      })()
    );
  }
});