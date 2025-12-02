/* BQA-ONE / Energix360 - Service Worker v7
   - App shell cache-first
   - GLP APIs network-first
   - HTML navegaciones network-first con fallback SIEMPRE a algo (sin ERR_FAILED)
*/

const CACHE_STATIC = "bqa-one-shell-v7";
const CACHE_DYNAMIC = "bqa-one-dyn-v7";

// App Shell mínimo y público
const APP_SHELL = [
  "/",                // raíz -> login
  "/login_energix360.html",
  "/login_energix360_offline.html",   // 👈 NUEVO
  "/offline.html",
  "/890707006.html",
  "/890707006_offline.html",          // 👈 NUEVO
  "/glp.html",
  "/glp_offline.html", 
  "/offline.html",
               // 👈 NUEVO

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

// INSTALL (no revienta si algo no cachea)
self.addEventListener("install", (event) => {
  console.log("[SW v7] install");
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_STATIC);
      for (const url of APP_SHELL) {
        try {
          await cache.add(url);
          console.log("[SW v7] cacheado en APP_SHELL:", url);
        } catch (err) {
          console.warn("[SW v7] NO se pudo cachear", url, err);
        }
      }
    })()
  );
  self.skipWaiting();
});

// ACTIVATE
self.addEventListener("activate", (event) => {
  console.log("[SW v7] activate");
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => ![CACHE_STATIC, CACHE_DYNAMIC].includes(k))
          .map((k) => {
            console.log("[SW v7] borrando cache vieja:", k);
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

  // Solo mismo origen
  if (url.origin !== location.origin) return;

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
      })()
    );
    return;
  }

  // ===== HTML / Navegación (login, 890707006, glp, dashboard, etc.) =====
  if (isHTML) {
    event.respondWith(
      (async () => {
        try {
          // 1) CACHE-FIRST: si ya hay una copia en caché, la usamos SIEMPRE
          let cached = await caches.match(req);
          if (!cached) {
            // Intento por path plano: /890707006.html, /glp.html, /dashboard/gas, etc.
            cached = await caches.match(path);
          }
          if (cached) {
            return cached;
          }

          // 2) Si no hay copia en cache, intentamos ir a la red (si hay internet)
          const res = await fetch(req);
          const copy = res.clone();

          caches.open(CACHE_DYNAMIC).then((cache) => {
            cache.put(req, copy);
            limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
          });

          return res;
        } catch (err) {
          console.warn("[SW v7] HTML offline FALLBACK para", path, "error:", err);

          // 3) OFFLINE / ERROR: devolvemos siempre algo

          // 3.1) Buscar por path plano
          let cached = await caches.match(path);
          if (cached) return cached;

          // 3.2) Si pidieron raíz "/", intentar raíz cacheada
          if (path === "/") {
            const rootCached = await caches.match("/");
            if (rootCached) return rootCached;
          }

          // 3.3) Fallback a offline.html
          const offlineCached = await caches.match("/offline.html");
          if (offlineCached) return offlineCached;

          // 3.4) Último recurso: HTML simple (para evitar ERR_FAILED)
          return new Response(
            "<!DOCTYPE html><html><head><meta charset='UTF-8'><title>Sin conexión</title></head>" +
            "<body style='font-family:sans-serif; padding:16px;'>" +
            "<h2>Sin conexión a internet</h2>" +
            "<p>No se encontró una copia guardada de esta página y no hay señal disponible.</p>" +
            "<p>Cuando tengas internet, abre de nuevo la aplicación para que se actualice la información.</p>" +
            "</body></html>",
            { status: 503, headers: { "Content-Type": "text/html" } }
          );
        }
      })()
    );
    return;
  }


  // ===== ESTÁTICOS (JS/CSS/IMG/FONTS) → cache-first =====
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
        // Si también falla aquí, devolvemos lo que tengamos cacheado (aunque sea null)
        return cached || new Response("", { status: 504 });
      }
    })()
  );
});

// BACKGROUND SYNC: avisar a los clientes que deben vaciar la cola GLP
self.addEventListener("sync", (event) => {
  if (event.tag === "sync-glp-queue") {
    event.waitUntil(
      (async () => {
        const clients = await self.clients.matchAll();
        for (const client of clients) {
          client.postMessage({
            type: "BQA_GLPSYNC",
            action: "flushQueue"
          });
        }
      })()
    );
  }
});
