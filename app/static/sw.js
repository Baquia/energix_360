/* BQA-ONE / Energix360 - Service Worker v11-FIX
   - App shell cache-first
   - GLP APIs network-first
   - HTML navegaciones NETWORK-FIRST (Solución Usuario Fantasma)
   - MODO OFFLINE GARANTIZADO
*/

// 1. CAMBIO DE VERSIÓN PARA FORZAR ACTUALIZACIÓN Y LIMPIAR FANTASMAS
const CACHE_STATIC = "bqa-one-shell-v12";
const CACHE_DYNAMIC = "bqa-one-dyn-v12-FIX";

// ===== MODO OFFLINE GARANTIZADO =====
let FORCE_OFFLINE = false;

self.addEventListener("message", (event) => {
  const data = event.data || {};
  
  if (data.type === "GLP_FORCE_OFFLINE") {
    FORCE_OFFLINE = !!data.value;
    console.log("[SW v11] FORCE_OFFLINE =", FORCE_OFFLINE);

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
  "/offline.html",
  "/890707006_offline.html",  
  "/glp_offline.html",
  "/static/manifest.json",
  "/static/BQA_ONE_192.png",
  "/static/BQA_ONE_512.png",
  "/static/logo_energix360.png",
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
  console.log("[SW v11] install");
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_STATIC);
      for (const url of APP_SHELL) {
        try {
          await cache.add(url);
        } catch (err) {
          console.warn("[SW v11] NO se pudo cachear", url, err);
        }
      }
    })()
  );
  self.skipWaiting();
});

// ACTIVATE
self.addEventListener("activate", (event) => {
  console.log("[SW v11] activate");
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => ![CACHE_STATIC, CACHE_DYNAMIC].includes(k))
          .map((k) => {
            console.log("[SW v11] borrando cache vieja:", k);
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
  // CAMBIO: Ahora intentamos red PRIMERO. Si falla, usamos caché.
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
          console.warn("[SW v11] Sin red, buscando fallback para HTML:", path);

          // 2. FALLBACK A CACHÉ (Solo si no hay red)
          let cached = await caches.match(req);
          if (!cached) {
            cached = await caches.match(path);
          }
          if (cached) return cached;

          // 3. FALLBACKS GENÉRICOS (Offline screens)
          if (path === "/") {
            const rootCached = await caches.match("/");
            if (rootCached) return rootCached;
          }

          // Intentar devolver la página offline específica de la empresa si es posible deducirla
          // o la genérica
          const offlineCached = await caches.match("/offline.html");
          if (offlineCached) return offlineCached;

          return new Response(
            "<!DOCTYPE html><html><body><h2>Sin conexión</h2><p>No se pudo contactar al servidor y no hay copia guardada.</p></body></html>",
            { status: 503, headers: { "Content-Type": "text/html" } }
          );
        }
      })()
    );
    return;
  }

  // ===== ESTÁTICOS (JS/CSS/IMG) → CACHE-FIRST (Esto se mantiene igual) =====
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