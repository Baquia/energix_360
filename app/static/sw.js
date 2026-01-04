/* BQA-ONE / Energix360 - Service Worker v10
   - App shell cache-first
   - GLP APIs network-first
   - HTML navegaciones cache-first con fallback SIEMPRE a algo (sin ERR_FAILED)
   - MODO OFFLINE GARANTIZADO (opción 2): cuando FORCE_OFFLINE=true, BLOQUEA mutaciones (POST/PUT/DELETE)
*/

// 1. CAMBIO DE VERSIÓN PARA FORZAR ACTUALIZACIÓN
const CACHE_STATIC = "bqa-one-shell-v10.3";
const CACHE_DYNAMIC = "bqa-one-dyn-v10.3";

// ===== MODO OFFLINE GARANTIZADO =====
// Se controla desde el frontend vía postMessage({type:"GLP_FORCE_OFFLINE", value:true/false})
let FORCE_OFFLINE = false;

self.addEventListener("message", (event) => {
  const data = event.data || {};
  
  if (data.type === "GLP_FORCE_OFFLINE") {
    FORCE_OFFLINE = !!data.value;
    console.log("[SW v10] FORCE_OFFLINE =", FORCE_OFFLINE);

    // Sincronizar la cola cuando el sistema vuelva a online
    if (!FORCE_OFFLINE) {
      flushOfflineQueue();
    }

    // Confirmación de mensaje al frontend
    try {
      if (event.source && typeof event.source.postMessage === "function") {
        event.source.postMessage({
          type: "GLP_FORCE_OFFLINE_ACK",
          value: FORCE_OFFLINE
        });
      }
    } catch (e) {
      // sin acción
    }
  }
});

// App Shell mínimo y público
// NOTA: Se eliminaron las páginas protegidas (/glp.html, /890707006.html)
// para evitar fallos de instalación si el usuario no está logueado.
// Esas páginas se cachearán dinámicamente apenas el usuario entre a ellas.
const APP_SHELL = [
  "/",                              // raíz -> login
  "/login_energix360_offline.html", 
  "/offline.html",
  // "/890707006.html",       <-- REMOVIDO DEL SHELL (se cachea dinámicamente)
  "/890707006_offline.html",  
  // "/glp.html",             <-- REMOVIDO DEL SHELL (se cachea dinámicamente)
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

// INSTALL (no revienta si algo no cachea)
self.addEventListener("install", (event) => {
  console.log("[SW v10] install");
  event.waitUntil(
    (async () => {
      const cache = await caches.open(CACHE_STATIC);
      for (const url of APP_SHELL) {
        try {
          await cache.add(url);
          console.log("[SW v10] cacheado en APP_SHELL:", url);
        } catch (err) {
          console.warn("[SW v10] NO se pudo cachear", url, err);
        }
      }
    })()
  );
  self.skipWaiting();
});

// ACTIVATE
self.addEventListener("activate", (event) => {
  console.log("[SW v10] activate");
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((k) => ![CACHE_STATIC, CACHE_DYNAMIC].includes(k))
          .map((k) => {
            console.log("[SW v10] borrando cache vieja:", k);
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

  // ===== CANDADO: MODO OFFLINE GARANTIZADO =====
  // Si el frontend decidió OFFLINE, NUNCA permitimos mutaciones hacia el servidor.
  if (FORCE_OFFLINE && req.method !== "GET") {
    event.respondWith(
      new Response("offline-forced", {
        status: 503,
        headers: { "Content-Type": "text/plain" }
      })
    );
    return;
  }

  // Nunca cachear POST/PUT/DELETE (si no está FORCE_OFFLINE, se permite ir a red normal)
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
            cache.put(req, copy); // <--- AQUÍ SE GUARDA GLP.HTML AUTOMÁTICAMENTE
            limitCacheSize(CACHE_DYNAMIC, MAX_DYNAMIC_ITEMS);
          });

          return res;
        } catch (err) {
          console.warn("[SW v10] HTML offline FALLBACK para", path, "error:", err);

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