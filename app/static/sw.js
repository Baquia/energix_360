/* BQA-ONE / Energix360 - Service Worker v5 (GLP network-first + BG Sync) */

const CACHE_STATIC  = "bqa-one-shell-v5";
const CACHE_DYNAMIC = "bqa-one-dyn-v5";

// App Shell mínimo y público (NO incluye "/" para evitar 302/errores)
const APP_SHELL = [
  "/offline.html",
  "/login_energix360.html",
  "/890707006.html",
  "/glp.html",

  "/static/manifest.json",
  "/static/BQA_ONE_192.png",
  "/static/BQA_ONE_512.png",
  "/static/logo_energix360.png",
  "/static/js/html5-qrcode.min.js"
];

// Limitar tamaño del caché dinámico para no inflar el navegador
const MAX_DYNAMIC_ITEMS = 60;
async function limitCacheSize(cacheName, maxItems) {
  const cache = await caches.open(cacheName);
  const keys = await cache.keys();
  if (keys.length > maxItems) {
    await cache.delete(keys[0]);
    return limitCacheSize(cacheName, maxItems);
  }
}

// INSTALL (robusto: no falla si un recurso no cachea)
self.addEventListener("install", (event) => {
  event.waitUntil((async () => {
    const cache = await caches.open(CACHE_STATIC);

    for (const url of APP_SHELL) {
      try {
        await cache.add(url);
      } catch (err) {
        // Importante: NO romper instalación por un 404/redirect aislado
        console.warn("SW install: no se pudo cachear", url, err);
      }
    }
  })());

  self.skipWaiting();
});

// ACTIVATE
self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => ![CACHE_STATIC, CACHE_DYNAMIC].includes(k))
          .map((k) => caches.delete(k))
      )
    )
  );
  self.clients.claim();
});

// FETCH
self.addEventListener("fetch", (event) => {
  const req = event.request;
  const url = new URL(req.url);

  // Solo mismo origen
  if (url.origin !== location.origin) return;

  // Nunca cachear POST/PUT/DELETE (APIs, login POST, etc.)
  if (req.method !== "GET") {
    event.respondWith(fetch(req));
    return;
  }

  const accept = req.headers.get("accept") || "";
  const isHTML = req.mode === "navigate" || accept.includes("text/html");
  const path = url.pathname || "";

  // ===== GLP APIs (JSON bajo /glp/) → NETWORK-FIRST =====
  // OJO: NO afecta glp.html, solo rutas tipo /glp/obtener_tanques, etc.
  if (path.startsWith("/glp/") && !isHTML) {
    event.respondWith((async () => {
      try {
        // Intento online primero
        const res = await fetch(req);
        const copy = res.clone();
        // Guardamos respuesta en caché dinámico por si sirve como fallback
        caches.open(CACHE_DYNAMIC).then((cache) => {
          cache.put(req, copy);
          limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
        });
        return res;
      } catch (err) {
        // Sin red: intentamos devolver una respuesta cacheada
        const cached = await caches.match(req);
        if (cached) return cached;

        // Si no hay nada cacheado, devolvemos un JSON de error controlado
        return new Response(
          JSON.stringify({
            success: false,
            offline: true,
            message: "Sin conexión y sin copia cacheada de la API GLP."
          }),
          {
            status: 503,
            headers: { "Content-Type": "application/json" }
          }
        );
      }
    })());
    return;
  }

  // ===== HTML / Navegación =====
  if (isHTML) {
    event.respondWith(
      fetch(req)
        .then((res) => {
          // Online: guarda copia en dinámico
          const copy = res.clone();
          caches.open(CACHE_DYNAMIC).then((cache) => {
            cache.put(req, copy);
            limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
          });
          return res;
        })
        .catch(async () => {
          // Offline:
          // 1) si la página ya fue visitada, ábrela desde caché
          const cachedPage = await caches.match(req);
          if (cachedPage) return cachedPage;

          // 2) fallback a offline.html
          return caches.match("/offline.html");
        })
    );
    return;
  }

  // ===== ESTÁTICOS (JS/CSS/IMG/FONTS) ===== cache-first
  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;

      return fetch(req)
        .then((res) => {
          const copy = res.clone();
          caches.open(CACHE_DYNAMIC).then((cache) => {
            cache.put(req, copy);
            limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
          });
          return res;
        })
        .catch(() => cached);
    })
  );
});

// BACKGROUND SYNC: avisar a los clientes que deben vaciar la cola GLP
self.addEventListener("sync", (event) => {
  if (event.tag === "sync-glp-queue") {
    event.waitUntil((async () => {
      const clients = await self.clients.matchAll();
      for (const client of clients) {
        // El front debe escuchar este mensaje y llamar a flushOfflineQueue()
        client.postMessage({
          type: "BQA_GLPSYNC",
          action: "flushQueue"
        });
      }
    })());
  }
});
