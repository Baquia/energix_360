// Variable global para controlar el candado
let pedidoPendienteGlobal = null;

/* ==========================================================
NUEVO FLUJO ATÓMICO: CONSUMO + NEGOCIACIÓN (OFFLINE FIRST)
========================================================== */

function enviarConsumo() {
  // NUEVA LÍNEA: Actualizar la caché local antes de enviar y evaluar el copiloto
  actualizarCacheUltimoNivel(sedeQR, items, "consumo");

  // 1. Evaluación Local Inmediata (Copiloto Offline)
  let sumaCap = 0;
  let sumaNivelCap = 0;
  let algunTanqueCritico = false; // NUEVO: Detector individual

  items.forEach(tk => {
    let cap = parseFloat(tk.capacidad || 0);
    let niv = parseFloat(tk.nivel || 0);
    sumaCap += cap;
    sumaNivelCap += (niv * cap);

    // NUEVO: Si algún tanque individual toca el 30%, encendemos la alarma
    if (niv <= 30.0) {
      algunTanqueCritico = true;
    }
  });

  let nivelPromedio = sumaCap > 0 ? (sumaNivelCap / sumaCap) : 0;

  // 2. ¿Nivel crítico? Abrimos el "Peaje" ANTES de enviar
  // Ahora se dispara por promedio bajo O por un tanque bajo
  if (nivelPromedio <= 30.0 || algunTanqueCritico) {
    let sugerencia = Math.round(80 - nivelPromedio);
    if (sugerencia < 10) sugerencia = 10;
    if (sugerencia > 85) sugerencia = 85;

    abrirNegociacionLocal(sugerencia);
  } else {
    // Si el nivel está bien, enviamos el consumo directo
    ejecutarEnvioAtomico(null);
  }
}

function abrirNegociacionLocal(sugerencia) {
  const modal = document.getElementById("modalNegociacion");
  document.getElementById("neg_nivel").value = sugerencia;
  document.getElementById("neg_dias_act").innerText = "--";

  currentDiasExtra = 0;
  document.getElementById("neg_dias_extra").innerText = "0";

  // Botón: SÍ PEDIR GAS
  const btnSi = document.querySelector(".btn-solicitar");
  btnSi.onclick = function () {
    let nivelSolicitado = document.getElementById("neg_nivel").value;
    modal.style.display = "none";
    // Empacamos la solicitud
    ejecutarEnvioAtomico({
      nivel: Number(nivelSolicitado),
      dias_extra: currentDiasExtra
    });
  };

  // Botón: NO PEDIR GAS (Ignorar advertencia)
  const btnSaltar = modal.querySelector("button[onclick='cerrarNegociacion()']");
  btnSaltar.onclick = function () {
    modal.style.display = "none";
    // Empacamos sin solicitud
    ejecutarEnvioAtomico(null);
  };

  modal.style.display = "flex";
}

function ejecutarEnvioAtomico(solicitudGas) {
  // 3. Empaquetado final de datos (El Súper-Paquete)
  const payload = {
    op_id: generarOpId(),
    sede: sedeQR,
    tanques: items
  };

  // Si el usuario negoció gas, inyectamos la orden en el mismo paquete
  if (solicitudGas) {
    payload.solicitud_gas = solicitudGas;
  }

  // 4. Viaje Seguro por la tubería Offline
  sendWithOffline(
    "/glp/registrar_consumo",
    payload,
    mostrarResumenOperacion // Pasamos directo a mostrar el resultado final
  );
}

// ==========================================
// CONFIGURAR CÁMARA VS GALERÍA
// ==========================================
function configurarModoFoto(permitirGaleria) {
  const input = document.getElementById("inputFoto");
  if (!input) return;

  if (permitirGaleria) {
    // Para TANQUEO: Quitamos el atributo 'capture' para que el celular
    // pregunte "¿Cámara o Archivos?" permitiendo subir desde galería.
    input.removeAttribute("capture");
  } else {
    // Para los demás: Forzamos la cámara trasera por seguridad y auditoría.
    input.setAttribute("capture", "environment");
  }
}

/* ==========================================
LÓGICA DEL MODAL DE CONFIRMACIÓN
========================================== */
let accionPendiente = null; // Aquí guardaremos la función a ejecutar (ej: iniciarCalefaccion)

function solicitarConfirmacion(mensaje, funcionCallback) {
  const modal = document.getElementById("modalConfirmacion");
  const txtMensaje = document.getElementById("modalMensaje");
  const btnSi = document.getElementById("btnModalConfirmar");

  // 1. Ponemos el mensaje personalizado
  txtMensaje.textContent = mensaje;

  // 2. Guardamos la función que queremos ejecutar
  accionPendiente = funcionCallback;

  // 3. Configuramos el botón SI para que ejecute esa función
  btnSi.onclick = function () {
    if (typeof accionPendiente === "function") {
      accionPendiente(); // Ejecuta la acción original (ej: iniciarCalefaccion)
    }
    cerrarModalConfirmacion();
  };

  // 4. Mostramos el modal (usamos flex para centrar)
  modal.style.display = "flex";
}

function cerrarModalConfirmacion() {
  const modal = document.getElementById("modalConfirmacion");
  modal.style.display = "none";
  accionPendiente = null; // Limpiamos la memoria
}

function generarOpId() {
  // Navegadores modernos
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  // Fallback simple
  return "op_" + Date.now() + "_" + Math.random().toString(16).slice(2);
}


/* ======================================
VARIABLES PARA REGISTRAR TANQUEO NUEVO
=======================================*/

let registrarTanqueoData = {
  sede: "",
  tanques: [],
  op_id: generarOpId()
};

// Variable global para guardar la respuesta temporalmente
let respuestaConsumoPendiente = null;

// Este arreglo se llenará con los tanques activos de la sede (TK-1, TK-2, etc)
let tanquesDisponibles = [];

// Índice del tanque que estamos procesando
let tanqueActualIndex = 0;

// =======================================================
// PROTECCIÓN BASE CONTRA RECARGA, CIERRE Y NAVEGACIÓN FUERA
// =======================================================

let GLP_OPERACION_EN_CURSO = false;

// Evita recargar/cerrar la página mientras hay una operación en curso
function bloquearSalida(e) {
  if (GLP_OPERACION_EN_CURSO) {
    e.preventDefault();
    e.returnValue = "";
    return "";
  }
}

// Activar protección
function activarProteccionOperacion() {
  GLP_OPERACION_EN_CURSO = true;
  window.addEventListener("beforeunload", bloquearSalida);
}

// Desactivar protección
function desactivarProteccionOperacion() {
  GLP_OPERACION_EN_CURSO = false;
  window.removeEventListener("beforeunload", bloquearSalida);
}

// =======================================================
// BLOQUEO DEL BOTÓN ATRÁS DEL NAVEGADOR
// =======================================================

function bloquearBotonAtras() {
  // Insertamos una entrada ficticia en el historial
  history.pushState(null, "", location.href);

  window.onpopstate = function () {
    if (GLP_OPERACION_EN_CURSO) {
      // Evitar retroceder
      history.pushState(null, "", location.href);
      alert("No puedes retroceder mientras realizas una operación GLP.");
    } else {
      // Si no hay operación, permitimos navegación normal
      history.go(-1);
    }
  };
}

// Activamos el bloqueo del botón atrás al cargar la página
window.addEventListener("load", bloquearBotonAtras);

/* ============================
Normalización + lector de QR
============================ */
function normalizarQR(decodedText) {
  let sede = "", empresa = "", tanques_qr = null;

  // 1) JSON
  try {
    const obj = JSON.parse(decodedText);
    if (obj && (obj.sede || obj.empresa)) {
      sede = (obj.sede || "").trim();
      empresa = (obj.empresa || "").trim();

      // NUEVO: tanques embebidos en el QR
      if (Array.isArray(obj.tanques)) {
        tanques_qr = obj.tanques.map(t => ({
          numero: String(t.numero || "").toLowerCase().trim(),
          capacidad: Number(t.capacidad || 0)
        })).filter(t => t.numero);
      }
    }
  } catch (_) { }

  // 2) URL
  if (!sede) {
    try {
      const u = new URL(decodedText);
      sede = (u.searchParams.get("sede") || "").trim();
      empresa = (u.searchParams.get("empresa") || "").trim();
    } catch (_) { }
  }

  // 3) "SEDE | EMPRESA"
  if (!sede && decodedText.includes("|")) {
    const [s, e] = decodedText.split("|", 2);
    sede = (s || "").trim();
    empresa = (e || "").trim();
  }

  // 4) Texto plano
  if (!sede) { sede = (decodedText || "").trim(); }

  // Limpiezas
  sede = sede.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
  empresa = empresa.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();

  return { sede, empresa, tanques: tanques_qr };
}

const FORCE_OFFLINE_KEY = "glp_force_offline"; // "1" o "0"

function isForceOffline() {
  return localStorage.getItem(FORCE_OFFLINE_KEY) === "1";
}

function setForceOffline(v) {
  localStorage.setItem(FORCE_OFFLINE_KEY, v ? "1" : "0");

  // informar al Service Worker (para bloquear POST reales)
  if (navigator.serviceWorker && navigator.serviceWorker.controller) {
    navigator.serviceWorker.controller.postMessage({ type: "GLP_FORCE_OFFLINE", value: !!v });
  }

  // opcional: pinta un banner visible “MODO OFFLINE ACTIVO”
  try { renderForceOfflineBanner(); } catch (e) { console.warn(e); }
}

/* ============================
MONITOR operatividad_pwa (AUTO)
============================ */

const OPERATIVIDAD = {
  CHECK_EVERY_MS: 8000,          // cada 8s
  TIMEOUT_MS: 4500,              // timeout del ping (4.5s)
  FAIL_THRESHOLD: 2,             // 2 fallos seguidos -> entrar OFFLINE
  SUCCESS_THRESHOLD: 3,          // 3 éxitos seguidos -> salir OFFLINE + sync
  ENDPOINT: "/glp/context"       // ping liviano (GET)
};

let _opw = {
  timer: null,
  inFlight: false,
  fails: 0,
  oks: 0
};

async function pingOperatividadPWA() {
  // CORRECCIÓN CRÍTICA: Se eliminó la línea "if (isForceOffline()) return false;"
  // Ahora el sistema SIEMPRE verificará si hay señal para poder despertar.

  const ctrl = new AbortController();
  // Damos 6 segundos de espera (ideal para zonas con señal inestable)
  const timeout = setTimeout(() => ctrl.abort(), 6000);

  try {
    // Usamos timestamp para evitar que el navegador use memoria vieja
    const pingUrl = "/glp/context?ping=" + Date.now();

    const r = await fetch(pingUrl, {
      method: "GET",
      cache: "no-store",
      credentials: "include",
      signal: ctrl.signal,
      headers: { "Accept": "application/json" }
    });

    clearTimeout(timeout);

    if (!r.ok) return false;

    const ct = (r.headers.get("content-type") || "").toLowerCase();
    if (!ct.includes("application/json")) return false;

    // ¡Éxito! El servidor respondió. Confirmado que hay internet.
    await r.json();
    return true;

  } catch (e) {
    clearTimeout(timeout);
    return false; // Solo aquí confirmamos que no hay señal real
  }
}

async function evaluarYAplicarOperatividadPWA() {
  const isOnline = await pingOperatividadPWA();  // Retorna true si HAY internet

  if (!isOnline) {
    // Si NO hay operatividad (offline)
    if (!isForceOffline()) {
      setForceOffline(true);  // Activa el modo offline en el frontend y en el SW
      console.log("[Operatividad] Entrando en modo OFFLINE...");

      // Enviar el estado al Service Worker
      if (navigator.serviceWorker.controller) {
        navigator.serviceWorker.controller.postMessage({
          type: "GLP_FORCE_OFFLINE",
          value: true
        });
      }
    }
  } else {
    // Si SÍ hay operatividad (online)
    if (isForceOffline()) {
      setForceOffline(false);  // Desactiva el modo offline
      console.log("[Operatividad] Volviendo a modo ONLINE...");

      // Enviar el estado al Service Worker
      if (navigator.serviceWorker.controller) {
        navigator.serviceWorker.controller.postMessage({
          type: "GLP_FORCE_OFFLINE",
          value: false
        });
      }

      flushOfflineQueue();  // Sincroniza la cola de operaciones
    }
  }
}

function iniciarMonitorOperatividadPWA() {
  if (_opw.timer) return;

  // chequeo inmediato
  evaluarYAplicarOperatividadPWA();

  _opw.timer = setInterval(evaluarYAplicarOperatividadPWA, OPERATIVIDAD.CHECK_EVERY_MS);

  window.addEventListener("online", () => evaluarYAplicarOperatividadPWA());
  window.addEventListener("offline", () => evaluarYAplicarOperatividadPWA());
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) evaluarYAplicarOperatividadPWA();
  });
}

// al cargar la página, empuja el estado actual al SW
window.addEventListener("load", () => {
  setForceOffline(isForceOffline());
  iniciarMonitorOperatividadPWA();
});

// ===================================================
//   UTILIDADES OFFLINE: cola + cache tanques
// ===================================================

// --- Cola offline para envíos POST ---
// ===================================================
//   COLA OFFLINE EN INDEXEDDB (soporta fotos grandes)
// ===================================================
const DB_NAME = "glp_offline_db";
const STORE = "queue";

function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, 1);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE, { keyPath: "id", autoIncrement: true });
      }
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function idbAdd(item) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readwrite");
    tx.objectStore(STORE).add(item);
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}

async function idbGetAll() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readonly");
    const req = tx.objectStore(STORE).getAll();
    req.onsuccess = () => resolve(req.result || []);
    req.onerror = () => reject(req.error);
  });
}

async function idbClear() {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readwrite");
    tx.objectStore(STORE).clear();
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}

// --- API compatible con tu lógica actual ---
async function enqueueOffline(item) {
  await idbAdd({ ...item, ts: Date.now() });
}

async function getQueue() {
  try { return await idbGetAll(); }
  catch { return []; }
}

async function setQueue(q) {
  await idbClear();
  for (const it of q) await idbAdd(it);
}

// 1. AÑADE ESTA PEQUEÑA FUNCIÓN (junto a getQueue y setQueue)
// Es necesaria porque tu código llama a removeFromQueue(item.id) y no existía en IndexedDB
async function removeFromQueue(id) {
  const db = await openDB();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(STORE, "readwrite");
    tx.objectStore(STORE).delete(id);
    tx.oncomplete = () => resolve(true);
    tx.onerror = () => reject(tx.error);
  });
}

let GLP_LOGIN_WARNING_SHOWN = false;

async function flushOfflineQueue() {
  if (!navigator.onLine) return;
  if (IS_SYNCING) return;

  const queue = await getQueue();
  if (queue.length === 0) return;

  IS_SYNCING = true;
  console.log(`[Sync] Intentando sincronizar ${queue.length} operaciones...`);

  for (const item of queue) {
    try {
      let response;
      
      // CASO 1: Confirmación de Arribo de Pollitos (Nueva funcionalidad)
      if (item.operacion === 'confirmar_arribo') {
        response = await fetch("/glp/confirmar_arribo_pollito", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(item)
        });
      } 
      // CASO 2: Operaciones estándar (Consumo, Tanqueo, etc.)
      else {
        // CORRECCIÓN QUIRÚRGICA: Lee la URL y el PAYLOAD exactos que guardó sendWithOffline
        response = await fetch(item.url || "/glp/registrar_operacion_glp", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(item.payload || item)
        });
      }

      const resData = await response.json();

      if (resData.success) {
        console.log(`[Sync] Éxito en operación: ${item.operacion} (ID local: ${item.id})`);
        
        // Si fue un arribo, nos aseguramos de limpiar el estado de espera en el dispositivo
        if (item.operacion === 'confirmar_arribo') {
            localStorage.setItem(`estado_lote_${item.sede}`, JSON.stringify({ esperando_pollito: false }));
            if (document.getElementById('bloque-arribo-pollito')) {
                document.getElementById('bloque-arribo-pollito').style.display = 'none';
            }
        }

        // Eliminar de la cola IndexedDB
        await removeFromQueue(item.id);
      } else {
        console.error(`[Sync] El servidor rechazó la operación: ${resData.message}`);
        // Si el error es crítico (ej. lote cerrado), podrías decidir borrarlo o dejarlo para revisión
      }
    } catch (err) {
      console.error("[Sync] Error de red o servidor:", err);
      // Detenemos el bucle para reintentar en el próximo ciclo de conexión
      break; 
    }
  }

  IS_SYNCING = false;
  
  // Actualizar la UI de advertencia de sincronización si existe
  const remaining = await getQueue();
  const warning = document.getElementById("syncWarning");
  if (warning) {
    if (remaining.length > 0) {
      warning.style.display = "block";
      warning.innerHTML = `⚠️ Tienes ${remaining.length} operaciones pendientes por subir.`;
      warning.className = "alert-warning-sync";
    } else {
      warning.style.display = "none";
      // Si todo se subió, recargamos niveles oficiales para estar al día
      if (typeof sedeQR !== 'undefined') {
          sincronizarNivelesServidor(sedeQR);
      }
    }
  }
}

// 3. TU FUNCIÓN INTACTA
function programarFlushOffline() {
  // 1. Cuando el navegador detecta que volvieron los datos/wifi
  window.addEventListener("online", () => {
    console.log("[GLP offline] Señal detectada. Iniciando secuencia de sincronización...");

    // ESTRATEGIA DE REINTENTOS ESCALONADOS (2.5s, 6s, 12s)
    // Ayuda a superar micro-cortes o inestabilidad inicial
    setTimeout(() => {
      console.log("🔄 Sincronizando (Intento 1)...");
      flushOfflineQueue();
      // Forzamos también la revisión del estado general (ping)
      if (typeof evaluarYAplicarOperatividadPWA === 'function') {
        evaluarYAplicarOperatividadPWA();
      }
    }, 2500);

    setTimeout(() => {
      console.log("🔄 Sincronizando (Intento 2)...");
      flushOfflineQueue();
    }, 6000);

    setTimeout(() => {
      flushOfflineQueue();
    }, 12000);
  });

  // 2. Al abrir la app (EVENTO LOAD) - AQUÍ ESTÁ LA MEJORA VISUAL
  window.addEventListener("load", async () => {
    try {
      // A. Verificar visualmente si hay pendientes para mostrar el aviso rojo
      if (typeof getQueue === 'function') {
        const q = await getQueue();
        if (q.length > 0) {
          console.log("⚠️ Se detectaron operaciones pendientes al iniciar.");
          if (typeof mostrarAvisoSyncPendiente === 'function') {
            mostrarAvisoSyncPendiente(); // Muestra el botón rojo
          }
        }
      }
    } catch (e) {
      console.error("Error verificando cola al inicio:", e);
    }

    // B. Intentar sincronizar automáticamente tras 1.5s
    setTimeout(flushOfflineQueue, 1500);
  });

  // 3. Monitor constante (Cada 10 segundos)
  // Sigue intentando eternamente mientras haya internet
  setInterval(() => {
    if (navigator.onLine) flushOfflineQueue();
  }, 10000);
}
function programarFlushOffline() {
  // 1. Cuando el navegador detecta que volvieron los datos/wifi
  window.addEventListener("online", () => {
    console.log("[GLP offline] Señal detectada. Iniciando secuencia de sincronización...");

    // ESTRATEGIA DE REINTENTOS ESCALONADOS (2.5s, 6s, 12s)
    // Ayuda a superar micro-cortes o inestabilidad inicial
    setTimeout(() => {
      console.log("🔄 Sincronizando (Intento 1)...");
      flushOfflineQueue();
      // Forzamos también la revisión del estado general (ping)
      if (typeof evaluarYAplicarOperatividadPWA === 'function') {
        evaluarYAplicarOperatividadPWA();
      }
    }, 2500);

    setTimeout(() => {
      console.log("🔄 Sincronizando (Intento 2)...");
      flushOfflineQueue();
    }, 6000);

    setTimeout(() => {
      flushOfflineQueue();
    }, 12000);
  });

  // 2. Al abrir la app (EVENTO LOAD) - AQUÍ ESTÁ LA MEJORA VISUAL
  window.addEventListener("load", async () => {
    try {
      // A. Verificar visualmente si hay pendientes para mostrar el aviso rojo
      if (typeof getQueue === 'function') {
        const q = await getQueue();
        if (q.length > 0) {
          console.log("⚠️ Se detectaron operaciones pendientes al iniciar.");
          if (typeof mostrarAvisoSyncPendiente === 'function') {
            mostrarAvisoSyncPendiente(); // Muestra el botón rojo
          }
        }
      }
    } catch (e) {
      console.error("Error verificando cola al inicio:", e);
    }

    // B. Intentar sincronizar automáticamente tras 1.5s
    setTimeout(flushOfflineQueue, 1500);
  });

  // 3. Monitor constante (Cada 10 segundos)
  // Sigue intentando eternamente mientras haya internet
  setInterval(() => {
    if (navigator.onLine) flushOfflineQueue();
  }, 10000);
}

// Activamos la programación del flush al cargar el script
programarFlushOffline();

// --- Cache local de tanques por sede ---
const TANQUES_CACHE_PREFIX = "tanques_cache_";

function cacheTanquesLocal(sede, tanques) {
  localStorage.setItem(TANQUES_CACHE_PREFIX + sede, JSON.stringify({
    tanques,
    ts: Date.now()
  }));
}

function getTanquesLocal(sede) {
  try {
    return JSON.parse(localStorage.getItem(TANQUES_CACHE_PREFIX + sede) || "null");
  } catch {
    return null;
  }
}

// ===================================================
//   OBTENER TANQUES CON FALLBACK: BD -> CACHE -> QR
// ===================================================
async function obtenerTanquesConFallback(sede) {
  try {
    // 1. Intentar obtener datos oficiales del servidor si hay internet
    if (navigator.onLine) {
      try {
        const response = await fetch("/glp/obtener_tanques", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sede: sede })
        });
        const data = await response.json();

        if (data.success && data.tanques && data.tanques.length > 0) {
          console.log("✅ Datos obtenidos del servidor.");
          // Actualizamos la caché local con lo más reciente del servidor
          cacheTanquesLocal(sede, data.tanques);
          
          // --- NUEVO: REGISTRAMOS SI HAY PEDIDO PENDIENTE ---
          if (data.hay_pedido_pendiente) {
              pedidoPendienteGlobal = data.codigo_pedido_pendiente;
              console.log("🔒 Candado Activo: Hay un pedido pendiente de tanqueo:", pedidoPendienteGlobal);
          } else {
              pedidoPendienteGlobal = null;
          }

          return {
            tanques: data.tanques,
            fuente: "servidor",
            lote_activo: data.lote_activo || false,
            info_lote: data.info_lote || ""
          };
        }
      } catch (e) {
        console.warn("Fallo fetch servidor, intentando fallback...", e);
      }
    }

    // 2. FALLBACK: Si no hay internet o falló el servidor, revisar QR embebido
    if (typeof sedeQRData !== 'undefined' && sedeQRData && sedeQRData.tanques) {
      console.log("🔍 Fusionando datos: QR (Hardware) + Caché Local (Niveles)");
      
      const cachedData = getTanquesLocal(sede);
      const nivelesEnCache = (cachedData && Array.isArray(cachedData.tanques)) ? cachedData.tanques : [];

      const tanquesFusionados = sedeQRData.tanques.map(tqr => {
        let nivelRecuperado = null;
        const coincidencia = nivelesEnCache.find(c => 
          String(c.numero).toLowerCase().trim() === String(tqr.numero).toLowerCase().trim()
        );

        if (coincidencia) {
          nivelRecuperado = coincidencia.ultimo_nivel;
        }

        return {
          numero: tqr.numero,          
          capacidad: tqr.capacidad,     
          etiqueta: tqr.numero,         
          ultimo_nivel: nivelRecuperado 
        };
      });

      cacheTanquesLocal(sede, tanquesFusionados);

      // --- NUEVO: VALIDAMOS CANDADO LOCAL ---
      if (cachedData && cachedData.hay_pedido_pendiente) {
          pedidoPendienteGlobal = cachedData.codigo_pedido_pendiente;
      } else {
          pedidoPendienteGlobal = null;
      }

      return {
        tanques: tanquesFusionados,
        fuente: "qr_fusionado",
        lote_activo: false,
        info_lote: ""
      };
    }

    // 3. ULTIMO RECURSO: Solo lo que haya en caché (si no hay QR ni internet)
    const finalCache = getTanquesLocal(sede);
    if (finalCache) {
      console.log("📦 Usando caché local pura (sin QR).");
      if (finalCache.hay_pedido_pendiente) {
          pedidoPendienteGlobal = finalCache.codigo_pedido_pendiente;
      } else {
          pedidoPendienteGlobal = null;
      }
      return finalCache;
    }

    return null;
  } catch (err) {
    console.error("Error crítico en obtenerTanquesConFallback:", err);
    return null;
  }
}
// ===================================================
//   ENVÍO POST CON OFFLINE-FALLBACK
// ===================================================

// ===================================================
// ACTUALIZAR CACHÉ DE NIVELES (OFFLINE SMART CACHE)
// ===================================================
function actualizarCacheUltimoNivel(sede, itemsGuardados, operacion) {
  const cacheKey = "tanques_cache_" + sede;
  let cacheData = localStorage.getItem(cacheKey);

  if (cacheData) {
    try {
      let datosSede = JSON.parse(cacheData);

      if (datosSede && Array.isArray(datosSede.tanques)) {
        itemsGuardados.forEach(item => {
          // Buscar el tanque en la caché
          let tkCache = datosSede.tanques.find(t => String(t.numero).toLowerCase() === String(item.numero).toLowerCase());

          if (tkCache) {
            // Si es tanqueo, el nuevo "último nivel" es el nivel FINAL
            if (operacion === "tanqueo" && item.nivel_final !== undefined && item.nivel_final !== null) {
              tkCache.ultimo_nivel = Number(item.nivel_final);
            }
            // Si es consumo u otro, el nuevo "último nivel" es el nivel actual
            else if (item.nivel !== undefined && item.nivel !== null) {
              tkCache.ultimo_nivel = Number(item.nivel);
            }
          }
        });
        // Guardar la caché actualizada en la memoria del celular
        localStorage.setItem(cacheKey, JSON.stringify(datosSede));
        console.log("✅ Caché local offline actualizada con los nuevos niveles.");
      }
    } catch (e) {
      console.error("Error actualizando la caché de niveles:", e);
    }
  }
}

// ===================================================
//   ENVÍO POST CON OFFLINE-FALLBACK (VERSIÓN SEMÁFORO ATÓMICO)
// ===================================================
async function sendWithOffline(url, payload, onSuccess) {
  // 1. MODO OFFLINE MANUAL (Si el usuario forzó el modo offline)
  if (typeof isForceOffline === "function" && isForceOffline()) {
    await enqueueOffline({ url, payload });

    // Activamos el aviso rojo visual
    if (typeof mostrarAvisoSyncPendiente === 'function') {
      mostrarAvisoSyncPendiente();
    }

    // Construimos una respuesta simulada para que el Modal de Resumen sepa qué mostrar
    const fakeResponse = {
      success: true,
      offline: true,
      message: "Guardado offline (modo OFFLINE activo). Se enviará automáticamente al recuperar operatividad.",
      resumen: {
        operacion: url.includes("consumo") ? "consumo" : (url.includes("tanqueo") ? "tanqueo" : "operacion"),
        saldo_estimado_kg: "Calculando...",
        kg_consumidos: "Calculando...",
        tanques: payload.tanques,
        requiere_gas: payload.solicitud_gas ? true : false,
        razon_alerta: payload.solicitud_gas ? "El pedido se generará automáticamente apenas recuperes la señal." : ""
      }
    };

    // ENVIAMOS AL MODAL FINAL (Semáforo Naranja)
    if (typeof onSuccess === "function") {
      onSuccess(fakeResponse);
    }

    limpiarEstadoOperacion(); // Limpiamos variables, el modal ya quedó activo
    return fakeResponse;
  }

  // 2. ENVÍO NORMAL (Intentamos conectar con el servidor)
  try {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });

    const ct = (r.headers.get("content-type") || "").toLowerCase();

    if (!ct.includes("application/json")) {
      throw new Error("non-json-response");
    }

    const j = await r.json();

    // Si el servidor responde pero dice FALSE (ej: Error lógico, Lote activo, etc.)
    if (!j.success) {
      alert(j.message || "Operación rechazada.");

      if (typeof mostrarMenu === 'function') {
        mostrarMenu();
      } else {
        limpiarEstadoOperacion();
      }
      return j;
    }

    // ÉXITO REAL ONLINE: Aquí SÍ mostramos el modal verde con datos reales
    if (typeof onSuccess === "function") {
      onSuccess(j);
    }

    limpiarEstadoOperacion();
    return j;

  } catch (e) {
    // 3. FALLO DE RED (No hay internet o servidor caído)
    const msg = String(e || "");

    const isNetworkError =
      !navigator.onLine ||
      msg.includes("Failed to fetch") ||
      msg.includes("NetworkError") ||
      msg.includes("non-json-response") ||
      msg.includes("Unexpected token <") ||
      msg.includes("Load failed");

    if (isNetworkError) {
      console.warn("⚠️ Fallo de red. Iniciando persistencia:", e);

      // A. Guardar en cola local
      await enqueueOffline({ url, payload });

      // B. Mostrar botón rojo de pendientes
      if (typeof mostrarAvisoSyncPendiente === 'function') {
        mostrarAvisoSyncPendiente();
      }

      // Construimos la respuesta simulada para el corte de red
      const fakeResponse = {
        success: true,
        offline: true,
        message: "⚠️ Señal inestable. Operación guardada en el dispositivo.",
        resumen: {
          operacion: url.includes("consumo") ? "consumo" : (url.includes("tanqueo") ? "tanqueo" : "operacion"),
          saldo_estimado_kg: "Calculando...",
          kg_consumidos: "Calculando...",
          tanques: payload.tanques,
          requiere_gas: payload.solicitud_gas ? true : false,
          razon_alerta: payload.solicitud_gas ? "El pedido se enviará al Webmaster apenas recuperes la señal." : ""
        }
      };

      // ENVIAMOS AL MODAL FINAL (Semáforo Naranja)
      if (typeof onSuccess === "function") {
        onSuccess(fakeResponse);
      }

      limpiarEstadoOperacion(); // Limpiamos estado de variables

      // Intentos de resincronización en background
      setTimeout(() => flushOfflineQueue(), 2000);
      setTimeout(() => flushOfflineQueue(), 5000);
      setTimeout(() => flushOfflineQueue(), 10000);

      return fakeResponse;
    }

    // Error desconocido que no es de red (ej: código roto)
    alert("No se pudo completar la operación: " + (e.message || e));
    throw e;
  }
}

function leerQR(titulo) {
  return new Promise((resolve, reject) => {
    const menu = document.getElementById("menuOpciones");
    const convo = document.getElementById("conversacion");
    const lector = document.getElementById("lectorQR");
    const texto = document.getElementById("preguntaTexto");
    
    if (menu) menu.style.display = "none";
    if (convo) convo.style.display = "block";
    if (lector) lector.style.display = "block";
    if (texto) texto.textContent = titulo || "Escanea el QR de la sede";

    lector.innerHTML = "";
    const qr = new Html5Qrcode("lectorQR");
    const config = { fps: 10, qrbox: 250 };
    
    qr.start({ facingMode: "environment" }, config, (decodedText) => {
      qr.stop().then(() => {
        lector.innerHTML = "";
        // 👇 ESTA ES LA LÍNEA QUE FALTABA: Oculta el cuadro negro apenas lee el QR
        if (lector) lector.style.display = "none"; 
        resolve(normalizarQR(decodedText));
      });
    }).catch(err => {
        if (lector) lector.style.display = "none"; // También ocultar si hay error
        reject(err);
    });
  });
}

/* ============================
Utilidades UI
============================ */
function show(el, disp = "block") { if (el) el.style.display = disp; }
function hide(el) { if (el) el.style.display = "none"; }

// === CORRECCIÓN CRÍTICA DE ARQUITECTURA ===
// Obtenemos la variable que inyectamos previamente en el HTML usando window
const EMPRESA_SESION = window.EMPRESA_SESION || "";
// ==========================================

function mostrarAvisoSyncPendiente() {
  const div = document.getElementById("syncWarning");
  if (!div) return;

  div.style.display = "block";
  div.innerHTML = `
    <div id="syncWarning-header">
      <div id="syncWarning-icon">!</div>
      <div>Operaciones pendientes</div>
    </div>
    <div id="syncWarning-body">
      Hay operaciones guardadas en el celular que no han subido al servidor.
    </div>
    
    <button id="btnSyncManual" onclick="ejecutarSyncManual()" style="
        width: 100%;
        margin-top: 8px;
        padding: 10px;
        background-color: #b71c1c;
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: bold;
        cursor: pointer;
        transition: all 0.3s;
    ">
        🔄 INTENTAR ENVIAR AHORA
    </button>
    
    <div style="margin-top:8px; font-size:12px; color:#555;">
       El sistema intentará enviarlas automáticamente, pero puedes usar este botón para forzarlo si tienes internet.
    </div>
  `;
}

async function ejecutarSyncManual() {
  const btn = document.getElementById("btnSyncManual");

  // 1. Feedback visual inmediato (para que sepas que sí hizo click)
  if (btn) {
    btn.innerHTML = "⏳ Conectando con el servidor...";
    btn.disabled = true; // Evitar doble click
    btn.style.opacity = "0.7";
  }

  // --- NUEVO: AVISAR A TELEGRAM DEL INTENTO MANUAL ---
  try {
    await fetch("/glp/notificar_intento_sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sede: (typeof sedeQR !== 'undefined' && sedeQR) ? sedeQR : "Sede Pendiente" })
    });
  } catch (e) { 
    console.warn("No se pudo enviar aviso de pendientes a Telegram"); 
  }
  // ----------------------------------------------------

  // 2. CORRECCIÓN CRÍTICA: Forzamos al sistema a creer que hay internet
  // Esto rompe el bloqueo que hacía que "no pasara nada"
  if (typeof setForceOffline === 'function') {
    setForceOffline(false);
  }

  // 3. Intentamos enviar (esperamos un poquito para que la UI respire)
  await new Promise(r => setTimeout(r, 500));
  await flushOfflineQueue();

  // 4. Si llegamos aquí y el aviso sigue visible, es que falló de nuevo.
  // Restauramos el botón para que puedas intentar otra vez.
  const warningDiv = document.getElementById("syncWarning");
  if (warningDiv && warningDiv.style.display !== 'none' && btn) {
    btn.innerHTML = "⚠️ Falló. Reintentar ahora";
    btn.disabled = false;
    btn.style.opacity = "1";
  }
}

let sedeQR = "", empresaQR = "";
let tanques = []; 
let tanquesQR = [];  
let items = [];   
let idx = 0;
let opActual = "";

/* ============================
Panel de resumen (único)
============================ */
function tituloOperacion(op) {
  switch (op) {
    case "inicio_calefaccion": return "Resumen - Inicio de calefacción";
    case "tanqueo": return "Resumen - Tanqueo de GLP";
    case "consumo": return "Resumen - Registro de consumo";
    case "finalizar_calefaccion": return "Resumen - Finalización de calefacción";
    default: return "Resumen de operación GLP";
  }
}

function mostrarResumenOperacion(res) {
  if (!res || !res.success) {
    alert((res && res.message) ? res.message : "Error desconocido.");
    if (document.getElementById('loadingOverlay')) document.getElementById('loadingOverlay').style.display = 'none';
    return;
  }

  if (document.getElementById('loadingOverlay')) document.getElementById('loadingOverlay').style.display = 'none';

  const resumen = res.resumen || res;
  if (res.esperando_pollito && resumen && resumen.sede) {
    localStorage.setItem(`estado_lote_${resumen.sede}`, JSON.stringify({ esperando_pollito: true }));
}

  // ==========================================
  // EL SEMÁFORO DE CERTIDUMBRE (NUEVO)
  // ==========================================
  const headerDiv = document.querySelector("#modal-resultado-final > div > div:first-child");
  const tituloDiv = headerDiv.querySelector("h2");
  const subtituloDiv = headerDiv.querySelector("p");
  const iconoDiv = headerDiv.querySelector("div");

  if (res.offline) {
    // ESTADO NARANJA: Guardado en el celular, sin internet
    headerDiv.style.background = "#f57c00"; 
    iconoDiv.innerText = "📡";
    tituloDiv.innerText = "Guardado en Celular";
    subtituloDiv.innerText = "PENDIENTE DE ENVÍO POR FALTA DE SEÑAL";
  } else {
    // ESTADO VERDE: Éxito total en el servidor
    headerDiv.style.background = "#015249"; 
    iconoDiv.innerText = "✅";
    tituloDiv.innerText = "Operación Exitosa";
    subtituloDiv.innerText = "REGISTRO GUARDADO EN EL SERVIDOR";
  }

  // Llenar datos básicos
  const opTitulo = (resumen.operacion || 'OPERACIÓN').replace(/_/g, ' ').toUpperCase();
  document.getElementById('bqa-operacion').innerText = opTitulo + (res.offline ? " (OFFLINE)" : "");

  // ==========================================
  // OCULTAR TARJETAS DE SALDO EN MODO OFFLINE
  // ==========================================
  const bloqueSaldos = document.getElementById('bqa-saldo').parentElement.parentElement;

  if (res.offline) {
    bloqueSaldos.style.display = 'none';
  } else {
    bloqueSaldos.style.display = 'flex';
    document.getElementById('bqa-saldo').innerText = resumen.saldo_estimado_kg ? parseFloat(resumen.saldo_estimado_kg).toFixed(2) + ' kg' : '---';
    document.getElementById('bqa-consumo').innerText = resumen.kg_consumidos ? parseFloat(resumen.kg_consumidos).toFixed(2) + ' kg' : '0.00 kg';
  }

  // Mostrar alertas de pedido
  const divPed = document.getElementById('bqa-bloque-pedido');
  const divAlert = document.getElementById('bqa-bloque-alerta');
  divPed.style.display = 'none';
  divAlert.style.display = 'none';

  if (resumen.pedido_automatico && resumen.pedido_automatico.generado) {
    divPed.style.display = 'block';
    document.getElementById('bqa-codigo').innerText = resumen.pedido_automatico.codigo;
    document.getElementById('bqa-proveedor').innerText = resumen.pedido_automatico.proveedor;
  } else if (resumen.requiere_gas) {
    divAlert.style.display = 'block';

    if (res.offline) {
      document.getElementById('bqa-razon').innerHTML = "<b>⚠️ ATENCIÓN:</b> Tu pedido de gas está guardado en el celular. <b>MUEVETE A UNA ZONA CON SEÑAL</b> para que el sistema lo envíe al Webmaster.";
    } else {
      document.getElementById('bqa-razon').innerText = resumen.razon_alerta;
    }
  }

  // Renderizar Tanques
  const tbody = document.getElementById('bqa-tabla-tanques');
  tbody.innerHTML = '';
  if (resumen.tanques && Array.isArray(resumen.tanques)) {
    resumen.tanques.forEach(t => {
      const nivel = parseFloat(t.nivel || 0).toFixed(1);
      const color = nivel <= 30 ? '#d32f2f' : '#333';
      tbody.innerHTML += `
    <tr style="border-bottom: 1px solid #f0f0f0;">
        <td style="padding: 6px 0; color: #555;">${t.numero}</td>
        <td style="padding: 6px 0; text-align: right; font-weight: bold; color: ${color};">${nivel}%</td>
    </tr>`;
    });
  }

  document.getElementById('modal-resultado-final').style.display = 'flex';
}

function cerrarResumen() {
  desactivarProteccionOperacion();

  const panel = document.getElementById("panelResumen");
  const menu = document.getElementById("menuOpciones");
  const convo = document.getElementById("conversacion");
  const pregunta = document.getElementById("preguntaTexto");

  if (pregunta) pregunta.textContent = "";

  if (panel) {
    panel.innerHTML = "";
    panel.style.display = "none";
  }

  if (menu) menu.style.display = "flex";
  if (convo) convo.style.display = "none";

  window.scrollTo({ top: 0, behavior: "instant" });
}

let currentOpId = null;
let currentDiasExtra = 0;

// ==========================================
// FUNCIONES DE NEGOCIACIÓN (FALTANTES)
// ==========================================

function abrirNegociacion(sugerencia, diasAct) {
  const modal = document.getElementById("modalNegociacion");

  document.getElementById("neg_nivel").value = sugerencia;
  document.getElementById("neg_dias_act").innerText = diasAct;

  currentDiasExtra = 0;
  document.getElementById("neg_dias_extra").innerText = "0";

  modal.style.display = "flex";
}

function adjustDias(delta) {
  currentDiasExtra += delta;
  if (currentDiasExtra < 0) currentDiasExtra = 0;
  document.getElementById("neg_dias_extra").innerText = currentDiasExtra;
}

function cerrarNegociacion() {
  document.getElementById("modalNegociacion").style.display = "none";

  if (respuestaConsumoPendiente && respuestaConsumoPendiente.resumen) {
    respuestaConsumoPendiente.resumen.requiere_gas = false;
  }

  mostrarResumenOperacion(respuestaConsumoPendiente);
}

/* ============================
Flujo común: cargar tanques
============================ */
async function cargarTanques(callback, bloquearSiActivo = false) {
  try {
    const res = await obtenerTanquesConFallback(sedeQR);

    if (bloquearSiActivo && res.lote_activo) {
      alert("⛔ YA HAY UN LOTE ACTIVO (" + (res.info_lote || "") + ").\n\nNo puedes iniciar calefacción nuevamente en esta sede sin finalizar el anterior.");
      mostrarMenu();
      return;
    }

    let listaTanques = (Array.isArray(res.tanques) && res.tanques.length > 0) 
                       ? res.tanques 
                       : tanquesQR;

    if (Array.isArray(listaTanques)) {
      listaTanques = listaTanques.filter(t => t !== null && t !== undefined && t.numero);
    }

    if (!listaTanques || listaTanques.length === 0) {
      alert("Error: No se encontraron tanques para esta sede. Verifica el QR o la conexión.");
      mostrarMenu();
      return;
    }

    tanques = listaTanques.map(t => ({
      numero: String(t.numero || t.etiqueta || "").toUpperCase(),
      capacidad: Number(t.capacidad || 250),
      ultimo_nivel: t.ultimo_nivel !== undefined && t.ultimo_nivel !== null ? Number(t.ultimo_nivel) : null
    }));


    if (typeof callback === "function") callback();

  } catch (err) {
    console.error("Error en cargarTanques:", err);
    alert("Error al cargar datos de la sede.");
    mostrarMenu();
  }
}

function mostrarMenu() {
  desactivarProteccionOperacion();

  const pregunta = document.getElementById("preguntaTexto");
  if (pregunta) pregunta.textContent = "";

  const menu = document.getElementById("menuOpciones");
  const convo = document.getElementById("conversacion");
  const panel = document.getElementById("panelResumen");

  if (menu) menu.style.display = "flex";
  if (convo) convo.style.display = "none";
  if (panel) panel.style.display = "none";

  if (typeof limpiarEstadoOperacion === 'function') {
    limpiarEstadoOperacion();
  }
}

// =======================================
//   COMPRESIÓN DE IMAGEN ANTES DE GUARDAR
// =======================================
async function comprimirImagenArchivo(file, maxDim = 1024, quality = 0.7) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onerror = () => reject(reader.error || "Error leyendo la imagen");
    reader.onload = () => {
      const img = new Image();
      img.onerror = () => reject("No se pudo cargar la imagen");
      img.onload = () => {
        let width = img.width;
        let height = img.height;

        if (width > height) {
          if (width > maxDim) {
            height = Math.round(height * (maxDim / width));
            width = maxDim;
          }
        } else {
          if (height > maxDim) {
            width = Math.round(width * (maxDim / height));
            height = maxDim;
          }
        }

        const canvas = document.createElement("canvas");
        canvas.width = width;
        canvas.height = height;
        const ctx = canvas.getContext("2d");
        ctx.drawImage(img, 0, 0, width, height);

        const dataUrl = canvas.toDataURL("image/jpeg", quality);
        resolve(dataUrl);
      };
      img.src = reader.result;
    };
    reader.readAsDataURL(file);
  });
}

/* ============================
Captura nivel + foto (común)
============================ */
function preguntarNivelFoto(onFinish) {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const divSiNo = document.getElementById("preguntaSiNo");
  const bSi = document.getElementById("btnSi");
  const bNo = document.getElementById("btnNo");

  if (idx >= tanques.length) {
    hide(input);
    hide(foto);
    hide(btn);
    hide(divSiNo);

    const msgDiv = document.getElementById("msgUltimoNivel");
    if (msgDiv) msgDiv.innerHTML = "";

    if (typeof onFinish === "function") onFinish();
    return;
  }

  const tk = tanques[idx];
  let nivelActualValue = 0;
  let archivoFoto = null;

  hide(foto);
  hide(divSiNo);
  input.type = "number";
  input.value = "";
  show(input);

  texto.textContent = `Nivel actual del ${tk.numero} (0-100 %)`;
  btn.textContent = "Guardar nivel";
  show(btn);

  let msgDiv = document.getElementById("msgUltimoNivel");
  if (!msgDiv) {
    msgDiv = document.createElement("div");
    msgDiv.id = "msgUltimoNivel";
    msgDiv.className = "mt-2 mb-2 text-center";
    input.parentNode.insertBefore(msgDiv, input.nextSibling);
  }

  if (tk.ultimo_nivel !== null && tk.ultimo_nivel !== undefined) {
    msgDiv.innerHTML = `<small class="text-primary fw-bold" style="font-size:0.85rem;">ℹ️ Último nivel reportado: ${tk.ultimo_nivel}%</small>`;
  } else {
    msgDiv.innerHTML = `<small class="text-warning fw-bold" style="font-size:0.85rem;">⚠️ Último dato no disponible. Recuerda que el nivel actual no puede ser mayor al anterior.</small>`;
  }
  show(msgDiv);

  btn.onclick = () => {
    const n = Number(input.value || 0);
    if (isNaN(n) || n < 0 || n > 100) {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    if (tk.ultimo_nivel !== null && tk.ultimo_nivel !== undefined) {
      if (n > tk.ultimo_nivel) {
        alert(`⚠️ ERROR DE LECTURA\n\nEl nivel ingresado (${n}%) es MAYOR al último reportado (${tk.ultimo_nivel}%).\n\nEn esta operación el nivel solo puede bajar. Verifica el manómetro.`);
        return;
      }
    }

    nivelActualValue = n;

    hide(input);
    hide(btn);
    hide(msgDiv); 
    texto.textContent = `Adjunta la foto de testigo del ${tk.numero}`;
    
    foto.value = null; 
    show(foto);
    btn.textContent = "Subir foto y continuar";
    show(btn);

    foto.onchange = () => {
      archivoFoto = foto.files[0];
    };

    btn.onclick = () => {
      if (!archivoFoto) {
        alert("Debes seleccionar la foto antes de continuar.");
        return;
      }

      hide(foto);
      hide(btn);
      texto.textContent = `¿Confirmas nivel ${nivelActualValue}% y foto para el ${tk.numero}?`;
      show(divSiNo);

      bSi.disabled = false;
      bNo.disabled = false;
      bSi.textContent = "SÍ, continuar";

      bNo.onclick = () => {
        preguntarNivelFoto(onFinish);
      };

      bSi.onclick = async () => {
        if (bSi.disabled) return; 
        bSi.disabled = true;
        bNo.disabled = true;
        const originalText = bSi.textContent;
        bSi.textContent = "Procesando...";

        try {
          const base64 = await comprimirImagenArchivo(archivoFoto);

          items.push({
            numero: tk.numero,
            capacidad: tk.capacidad,
            nivel: nivelActualValue,
            fotoBase64: base64
          });

          idx++;
          hide(divSiNo);
          
          bSi.disabled = false;
          bNo.disabled = false;
          bSi.textContent = originalText;

          preguntarNivelFoto(onFinish);

        } catch (e) {
          bSi.disabled = false;
          bNo.disabled = false;
          bSi.textContent = originalText;
          console.error("Error procesando foto:", e);
          alert("No se pudo procesar la imagen. Intenta de nuevo.");
        }
      };
    };
  };
}

function generarOpId() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  return "op_" + Date.now() + "_" + Math.random().toString(16).slice(2);
}

function limpiarEstadoOperacion() {
  desactivarProteccionOperacion();
  items = [];
  idx = 0;
  tanques = [];
  tanquesDisponibles = [];
  tanqueActualDatos = null;
  tanqueActualIndex = 0;

  registrarTanqueoData = { sede: "", tanques: [] };

  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const siNo = document.getElementById("preguntaSiNo");

  if (input) { input.value = ""; input.onkeyup = null; }
  if (foto) { foto.value = ""; foto.onchange = null; }
  if (btn) { btn.onclick = null; btn.style.display = "none"; }
  if (siNo) { siNo.style.display = "none"; }

  const convo = document.getElementById("conversacion");
  const lector = document.getElementById("lectorQR");
  if (convo) convo.style.display = "none";
  if (lector) lector.style.display = "none";
}

/* ============================
INICIAR CALEFACCIÓN
============================ */
async function iniciarCalefaccion() {
  activarProteccionOperacion();
  opActual = "inicio_calefaccion";

  if (typeof configurarModoFoto === 'function') {
    configurarModoFoto(false);
  }

  try {
    const info = await leerQR("Escanea el QR de la sede para iniciar calefacción");
    sedeQR = info.sede;
    empresaQR = info.empresa;
    tanquesQR = Array.isArray(info.tanques) ? info.tanques : [];

    const texto = document.getElementById("preguntaTexto");
    const input = document.getElementById("respuestaInput");
    const foto = document.getElementById("inputFoto");
    const btn = document.getElementById("botonEnviar");
    const divSiNo = document.getElementById("preguntaSiNo");

    divSiNo.style.display = "none";

    if (input) input.onkeyup = null;
    if (foto) foto.onchange = null;
    if (btn) btn.onclick = null;

    items = [];
    idx = 0;

    function despuesDeAvicola() {
      cargarTanques(() => {
        preguntarNivelFoto(enviarInicio);
      }, true);
    }

    if (EMPRESA_SESION === "Pollos GAR SAS") {
      let pasoAvicola = 1;

      const hacerPreguntaAvicola = () => {
        show(input);
        hide(foto);
        hide(divSiNo);
        show(btn);

        if (pasoAvicola === 1) {
          input.type = "date";
          input.value = "";
          texto.textContent = "¿Cuál es la fecha real estimada de llegada del pollito? (Día 1)";
          btn.textContent = "Siguiente";

          btn.onclick = () => {
            const fechaSel = input.value;
            if (!fechaSel) {
              alert("Por favor, selecciona una fecha válida.");
              return;
            }
            input.dataset.fecha_llegada = fechaSel;
            pasoAvicola = 2; 
            hacerPreguntaAvicola();
          };
        } else if (pasoAvicola === 2) {
          input.type = "number";
          input.value = "";
          texto.textContent = "¿Con cuántos pollitos inicias el lote?";
          btn.textContent = "Confirmar";

          btn.onclick = () => {
            const val = input.value;
            const pollitos = Number(val || 0);

            if (isNaN(pollitos) || pollitos <= 0) {
              alert("Ingresa un número válido de pollitos.");
              return;
            }

            hide(input);
            hide(btn);
            const fLlegada = input.dataset.fecha_llegada;
            texto.textContent = `¿Estás seguro que inicias con ${pollitos} aves el día ${fLlegada}?`;

            const bSi = document.getElementById("btnSi");
            const bNo = document.getElementById("btnNo");
            show(divSiNo);

            bNo.onclick = () => {
              hide(divSiNo);
              pasoAvicola = 1;
              hacerPreguntaAvicola();
            };

            bSi.onclick = () => {
              hide(divSiNo);
              input.dataset.pollitos = String(pollitos);
              despuesDeAvicola(); 
            };
          };
        }
      };

      hacerPreguntaAvicola(); 

    } else {
      hide(input);
      hide(btn);
      hide(foto);
      despuesDeAvicola();
    }
  } catch (e) {
    alert("No fue posible leer el QR: " + e);
    mostrarMenu();
  }
}

function enviarInicio() {
  const inputRef = document.getElementById("respuestaInput");
  const pollitos = Number(inputRef.dataset.pollitos || 0) || null;
  const fechaLlegada = inputRef.dataset.fecha_llegada || null;

  const payload = {
    op_id: generarOpId(),
    sede: sedeQR,
    pollitos: (EMPRESA_SESION === "Pollos GAR SAS" ? pollitos : null),
    fecha_llegada_pollitos: (EMPRESA_SESION === "Pollos GAR SAS" ? fechaLlegada : null),
    tanques: items
  };

  sendWithOffline("/glp/registrar_inicio", payload, mostrarResumenOperacion);
}

// ============================================
// NUEVO FLUJO DE REGISTRAR TANQUEO
// ============================================

async function cargarTanquesTanqueo(callback) {
  try {
    const res = await obtenerTanquesConFallback(sedeQR);

    let tanquesDisponiblesTmp = (Array.isArray(res.tanques) ? res.tanques : []).filter(t => t !== null && t !== undefined && t.numero);

    if (tanquesDisponiblesTmp.length === 0) {
      alert("No se encontraron tanques para esta sede.");
      mostrarMenu();
      return;
    }

    tanquesDisponibles = tanquesDisponiblesTmp.map(t => ({
      numero: String(t.numero || t.etiqueta || "").toLowerCase(),
      capacidad: Number(t.capacidad || 0),
      ultimo_nivel: t.ultimo_nivel !== undefined && t.ultimo_nivel !== null ? Number(t.ultimo_nivel) : null
    }));

    
    if (typeof callback === "function") callback();

  } catch (err) {
    console.error("Error en cargarTanquesTanqueo:", err);
    mostrarMenu();
  }
}

async function registrarTanqueo() {
  activarProteccionOperacion();
  opActual = "tanqueo"; 

  if (typeof configurarModoFoto === 'function') {
    configurarModoFoto(true);
  }

  try {
    registrarTanqueoData = {
      op_id: generarOpId(),
      sede: "",
      tanques: []
    };

    tanqueActualIndex = 0;

    const info = await leerQR("Escanea el QR de la sede para registrar el tanqueo");
    sedeQR = info.sede;
    empresaQR = info.empresa;
    tanquesQR = Array.isArray(info.tanques) ? info.tanques : [];

    registrarTanqueoData.sede = sedeQR;

    const input = document.getElementById("respuestaInput");
    const foto = document.getElementById("inputFoto");
    const btn = document.getElementById("botonEnviar");

    if (input) {
      input.onkeyup = null;
      input.dataset.pollitos = "";
    }
    if (foto) {
      foto.onchange = null;
    }
    if (btn) {
      btn.onclick = null;
    }

    cargarTanquesTanqueo(() => {
      tanqueActualIndex = 0;
      preguntarSiTanquearTanqueActual();
    });
  } catch (e) {
    console.error(e);
    alert("No fue posible leer el QR: " + e);
    mostrarMenu();
  }
}

function preguntarSiTanquearTanqueActual() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const panelSiNo = document.getElementById("preguntaSiNo");
  const btnSi = document.getElementById("btnSi");
  const btnNo = document.getElementById("btnNo");

  if (tanqueActualIndex >= tanquesDisponibles.length) {
    enviarTanqueo();
    return;
  }

  const tk = tanquesDisponibles[tanqueActualIndex];

  texto.textContent = `¿Desea tanquear el tanque ${tk.numero.toUpperCase()}?`;

  hide(input);
  hide(foto);
  hide(btn);

  panelSiNo.style.display = "block";

  btnSi.onclick = () => {
    panelSiNo.style.display = "none";
    prepararDatosTanqueActual(tk);
    pedirNivelInicial();
  };

  btnNo.onclick = () => {
    panelSiNo.style.display = "none";
    tanqueActualIndex++;
    preguntarSiTanquearTanqueActual();
  };
}

let tanqueActualDatos = null;
function prepararDatosTanqueActual(tk) {
  tanqueActualDatos = {
    numero: tk.numero,
    capacidad: tk.capacidad,
    ultimo_nivel: tk.ultimo_nivel, 
    nivel_inicial: 0,
    foto_nivel_inicial: "",
    nivel_final: 0,
    foto_nivel_final: "",
    densidad_suministrada: 0,
    kg_suministrados: 0,
    foto_baucher: ""
  };
}

function pedirNivelInicial() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const divSiNo = document.getElementById("preguntaSiNo");

  show(input);
  hide(foto);
  hide(divSiNo); 

  input.type = "number";
  input.value = "";
  texto.textContent = `Ingresa el NIVEL ACTUAL del tanque ${tanqueActualDatos.numero.toUpperCase()} (0-100 %)`;
  btn.textContent = "Confirmar nivel";
  show(btn);

  let msgDiv = document.getElementById("msgUltimoNivel");
  if (!msgDiv) {
    msgDiv = document.createElement("div");
    msgDiv.id = "msgUltimoNivel";
    msgDiv.className = "mt-2 mb-2 text-center";
    input.parentNode.insertBefore(msgDiv, input.nextSibling);
  }

  if (tanqueActualDatos.ultimo_nivel !== null && tanqueActualDatos.ultimo_nivel !== undefined) {
    msgDiv.innerHTML = `<small class="text-primary fw-bold" style="font-size:0.85rem;">ℹ️ Último nivel reportado: ${tanqueActualDatos.ultimo_nivel}%</small>`;
  } else {
    msgDiv.innerHTML = `<small class="text-warning fw-bold" style="font-size:0.85rem;">⚠️ Último dato no disponible. El nivel inicial no puede ser mayor al anterior.</small>`;
  }
  show(msgDiv);

  btn.onclick = () => {
    const n = Number(input.value || 0);
    if (isNaN(n) || n < 0 || n > 100) {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    if (tanqueActualDatos.ultimo_nivel !== null && tanqueActualDatos.ultimo_nivel !== undefined) {
        if (n > tanqueActualDatos.ultimo_nivel) {
            alert(`⚠️ ERROR DE LECTURA\n\nEl nivel inicial (${n}%) no puede ser MAYOR al último nivel reportado en este tanque (${tanqueActualDatos.ultimo_nivel}%).\n\nVerifica el manómetro e intenta de nuevo.`);
            return; 
        }
    }

    hide(input);
    hide(btn); 
    hide(msgDiv); 
    texto.textContent = `¿Estás seguro que el Nivel Inicial es ${n}%?`;

    const bSi = document.getElementById("btnSi");
    const bNo = document.getElementById("btnNo");
    show(divSiNo); 

    bNo.onclick = () => {
      hide(divSiNo);
      texto.textContent = `Ingresa el NIVEL ACTUAL del tanque ${tanqueActualDatos.numero.toUpperCase()} (0-100 %)`;
      show(input);
      show(btn);
      show(msgDiv); 
      input.focus(); 
    };

    bSi.onclick = () => {
      hide(divSiNo);
      tanqueActualDatos.nivel_inicial = n;
      pedirFotoNivelInicial();
    };
  };
}

function pedirFotoNivelInicial() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  document.getElementById("preguntaSiNo").style.display = "none";

  hide(input);
  texto.textContent = `Toma una FOTO del NIVEL ACTUAL del tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
  foto.onchange = null;
  foto.value = "";
  show(foto);
  btn.textContent = "Subir foto y continuar";
  show(btn);

  btn.onclick = async () => {
    const f = foto.files[0];
    if (!f) {
      alert("Debes seleccionar la foto.");
      return;
    }
    try {
      const base64 = comprimirImagenArchivo(archivoFoto, 800, 0.4);
      tanqueActualDatos.foto_nivel_inicial = String(base64 || "");
      mostrarMensajeProcederTanqueo();
    } catch (err) {
      console.error(err);
      alert("No se pudo procesar la foto. Intenta de nuevo.");
    }
  };
}

function mostrarMensajeProcederTanqueo() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  document.getElementById("preguntaSiNo").style.display = "none";

  hide(input); hide(foto);
  texto.textContent = `Puedes proceder con el TANQUEO del tanque ${tanqueActualDatos.numero.toUpperCase()}. Cuando termines, presiona "Continuar".`;
  btn.textContent = "Continuar";
  show(btn);

  btn.onclick = () => {
    pedirNivelFinal();
  };
}

function pedirNivelFinal() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const divSiNo = document.getElementById("preguntaSiNo");

  show(input);
  hide(foto);
  hide(divSiNo);

  input.type = "number";
  input.value = "";
  texto.textContent = `Ingresa el NIVEL FINAL del tanque ${tanqueActualDatos.numero.toUpperCase()} (0-100 %)`;
  btn.textContent = "Confirmar nivel final";
  show(btn);

  btn.onclick = () => {
    const n = Number(input.value || 0);
    if (isNaN(n) || n < 0 || n > 100) {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    hide(input);
    hide(btn); 
    texto.textContent = `¿Estás seguro que el Nivel Final es ${n}%?`;

    const bSi = document.getElementById("btnSi");
    const bNo = document.getElementById("btnNo");
    show(divSiNo); 

    bNo.onclick = () => {
      hide(divSiNo);
      texto.textContent = `Ingresa el NIVEL FINAL del tanque ${tanqueActualDatos.numero.toUpperCase()} (0-100 %)`;
      show(input);
      show(btn);
      input.focus(); 
    };

    bSi.onclick = () => {
      hide(divSiNo);
      tanqueActualDatos.nivel_final = n;
      pedirFotoNivelFinal();
    };
  };
}

function pedirFotoNivelFinal() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  document.getElementById("preguntaSiNo").style.display = "none";

  hide(input);
  texto.textContent = `Toma una FOTO del NIVEL FINAL del tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
  foto.onchange = null;
  foto.value = "";
  show(foto);
  btn.textContent = "Subir foto y continuar";
  show(btn);

  btn.onclick = async () => {
    const f = foto.files[0];
    if (!f) {
      alert("Debes seleccionar la foto.");
      return;
    }
    try {
      const base64 = await comprimirImagenArchivo(archivoFoto, 800, 0.4);
      tanqueActualDatos.foto_nivel_final = String(base64 || "");
      pedirDensidadSuministrada();
    } catch (err) {
      console.error(err);
      alert("No se pudo procesar la foto. Intenta de nuevo.");
    }
  };
}

function pedirDensidadSuministrada() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const divSiNo = document.getElementById("preguntaSiNo");

  show(input);
  hide(foto);
  hide(divSiNo);

  input.type = "number";
  input.step = "0.01";
  input.value = "";
  texto.textContent = `Ingresa la DENSIDAD SUMINISTRADA (kg/gal) para el tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
  btn.textContent = "Confirmar densidad";
  show(btn);

  btn.onclick = () => {
    const d = Number(input.value || 0);

    if (isNaN(d) || d < 1.8 || d > 2.5) {
      alert("⛔ DATO INVÁLIDO\n\nLa densidad debe estar obligatoriamente entre 1.8 y 2.5 kg/gal.\n\nRevisa el baucher nuevamente.");
      input.value = ""; 
      input.focus();
      return; 
    }

    hide(input);
    hide(btn);
    texto.textContent = `¿Estás seguro que la densidad es ${d} kg/gal?`;

    const bSi = document.getElementById("btnSi");
    const bNo = document.getElementById("btnNo");
    show(divSiNo);

    bNo.onclick = () => {
      hide(divSiNo);
      texto.textContent = `Ingresa la DENSIDAD SUMINISTRADA (kg/gal) para el tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
      show(input);
      show(btn);
      input.focus();
    };

    bSi.onclick = () => {
      hide(divSiNo);
      tanqueActualDatos.densidad_suministrada = d;
      pedirKgSuministrados();
    };
  };
}

function pedirKgSuministrados() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const divSiNo = document.getElementById("preguntaSiNo");

  show(input);
  hide(foto);
  hide(divSiNo); 

  input.type = "number";
  input.step = "0.01";
  input.value = "";
  texto.textContent = `Ingresa los KG SUMINISTRADOS al tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
  btn.textContent = "Confirmar kg";
  show(btn);

  btn.onclick = () => {
    const k = Number(input.value || 0);
    if (isNaN(k) || k <= 0) {
      alert("Ingresa un valor válido de kg (>0).");
      return;
    }

    hide(input);
    hide(btn); 
    texto.textContent = `¿Estás seguro que son ${k} Kilogramos?`;

    const bSi = document.getElementById("btnSi");
    const bNo = document.getElementById("btnNo");
    show(divSiNo); 

    bNo.onclick = () => {
      hide(divSiNo);
      texto.textContent = `Ingresa los KG SUMINISTRADOS al tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
      show(input);
      show(btn);
      input.focus(); 
    };

    bSi.onclick = () => {
      hide(divSiNo);
      tanqueActualDatos.kg_suministrados = k;
      pedirFotoBaucher();
    };
  };
}

function pedirFotoBaucher() {
  const texto = document.getElementById("preguntaTexto");
  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  document.getElementById("preguntaSiNo").style.display = "none";

  hide(input);
  texto.textContent = `Toma una FOTO del BAUCHER DEL MASICO del tanque ${tanqueActualDatos.numero.toUpperCase()}.`;
  foto.onchange = null;
  foto.value = "";
  show(foto);
  btn.textContent = "Subir baucher y continuar";
  show(btn);

  btn.onclick = async () => {
    const f = foto.files[0];
    if (!f) {
      alert("Debes seleccionar la foto.");
      return;
    }

    if (btn.disabled) return; 
    btn.disabled = true;
    const textoOriginal = btn.textContent;
    btn.textContent = "Procesando...";

    try {
      const base64 = await comprimirImagenArchivo(archivoFoto, 800, 0.4);
      tanqueActualDatos.foto_baucher = String(base64 || "");
      registrarTanqueoData.tanques.push(tanqueActualDatos);
      
      btn.disabled = false;
      btn.textContent = textoOriginal;

      tanqueActualIndex++;
      preguntarSiTanquearTanqueActual();
      
    } catch (err) {
      btn.disabled = false;
      btn.textContent = textoOriginal;
      console.error(err);
      alert("No se pudo procesar la foto. Intenta de nuevo.");
    }
  };
}

async function enviarTanqueo() {
  if (!items || items.length === 0) {
    alert("Error: No hay tanques cargados.");
    return;
  }

  let valido = true;
  let faltanFotos = false;

  for (let i = 0; i < items.length; i++) {
    const t = items[i];
    if (t.nivel_final === undefined || t.nivel_final === null || t.nivel_final === "") valido = false;
    if (!t.foto_nivel_final) faltanFotos = true;
  }

  if (!valido) { alert("Debes ingresar nivel final para todos los tanques."); return; }
  if (faltanFotos) { alert("Debes tomar la FOTO DEL NIVEL FINAL para cada tanque."); return; }

  const fileInput = document.getElementById("foto_baucher_input");
  let file = null;
  if (fileInput && fileInput.files && fileInput.files.length > 0) {
    file = fileInput.files[0];
  } else if (!isForceOffline()) {
     // Si está en offline, luego valida. Online = error si no hay.
  }

  if (file) {
    try {
      const base64_baucher = await comprimirImagenArchivo(file, 800, 0.4);
      items.forEach(t => t.foto_baucher = base64_baucher);
    } catch (e) {
      console.error(e);
      alert("Error procesando foto del recibo.");
      return;
    }
  } else if (!isForceOffline()) {
    alert("Debes tomar foto del recibo (vaucher).");
    return;
  }

  const opId = generarOpId("tanq");
  const payload = {
    operacion: 'tanqueo',
    sede: sedeQR,
    tanques: items,
    op_id: opId,
    timestamp: new Date().toISOString()
  };

  mostrarResumenOperacion("Tanqueo GLP");

  const ok = await sendWithOffline("/glp/registrar_tanqueo", payload, opId);
  if (ok) {
    alert("✅ Operación completada (Tanqueo).");
    pedidoPendienteGlobal = null; // --- NUEVO: LIBERAMOS CANDADO AL TANQUEAR
  } else {
    alert("❌ Error: No se pudo registrar y falló el guardado local.");
  }
  
  cerrarResumen();
  limpiarEstadoOperacion();
  mostrarMenu();
}

/* ============================
  REGISTRAR CONSUMO
============================ */
async function registrarConsumo() {
  if (!items || items.length === 0) {
    alert("Error: No hay tanques cargados. Vuelve a escanear.");
    return;
  }

  // --- NUEVO BLINDAJE: BLOQUEA EL CONSUMO ---
  if (pedidoPendienteGlobal) {
      alert(`⛔ OPERACIÓN BLOQUEADA\n\nEl sistema indica que tienes el pedido ${pedidoPendienteGlobal} pendiente de descarga.\n\nDebes ir al menú, presionar "Registrar Tanqueo" y subir las evidencias para liberar el sistema.`);
      mostrarMenu();
      return; 
  }

  let valido = true;
  for (let i = 0; i < items.length; i++) {
    const valStr = items[i].nivel;
    if (valStr === undefined || valStr === null || valStr === "" || isNaN(valStr)) {
      valido = false; break;
    }
    const valNum = Number(valStr);
    if (valNum < 0 || valNum > 100) {
      valido = false; break;
    }
  }

  if (!valido) {
    alert("Debes ingresar un nivel válido (0 a 100) para todos los tanques.");
    return;
  }

  // Verificar fotos en offline
  if (isForceOffline()) {
    let faltanFotos = false;
    for (let i = 0; i < items.length; i++) {
      if (!items[i].foto) {
        faltanFotos = true; break;
      }
    }
    if (faltanFotos) {
      alert("En modo SIN INTERNET, debes capturar TODAS las fotos de nivel actual obligatoriamente.");
      return;
    }
  }

  const opId = generarOpId("cons");
  const payload = {
    operacion: 'consumo',
    sede: sedeQR,
    tanques: items,
    op_id: opId,
    timestamp: new Date().toISOString()
  };

  mostrarResumenOperacion("Consumo Diario");

  // Llamada unificada que sabe rutear (fetch normal o guardar local)
  const ok = await sendWithOffline("/glp/registrar_consumo", payload, opId);
  if (ok) {
    alert("✅ Operación completada (Consumo).");
  } else {
    alert("❌ Error: No se pudo registrar y falló el guardado local.");
  }
  
  cerrarResumen();
  limpiarEstadoOperacion();
  mostrarMenu();
}

/* ============================
   FINALIZAR CALEFACCIÓN
============================ */
async function finalizarCalefaccion() {
  activarProteccionOperacion();
  opActual = "finalizar_calefaccion"; 

  if (typeof configurarModoFoto === 'function') {
    configurarModoFoto(false);
  }

  try {
    const info = await leerQR("Escanea el QR de la sede para finalizar la calefacción");

    sedeQR = info.sede;
    empresaQR = info.empresa;
    tanquesQR = Array.isArray(info.tanques) ? info.tanques : [];
    const texto = document.getElementById("preguntaTexto");
    const input = document.getElementById("respuestaInput");
    const foto = document.getElementById("inputFoto");
    const btn = document.getElementById("botonEnviar");
    document.getElementById("preguntaSiNo").style.display = "none";

    if (input) {
      input.onkeyup = null;
      input.dataset.pollitos = "";
    }
    if (foto) {
      foto.onchange = null;
    }
    if (btn) {
      btn.onclick = null;
    }
    items = []; idx = 0;
    cargarTanques(() => { preguntarNivelFoto(enviarFinalizar); });
  } catch (e) {
    alert("No fue posible leer el QR: " + e);
    mostrarMenu();
  }
}

function enviarFinalizar() {
  actualizarCacheUltimoNivel(sedeQR, items, "finalizar_calefaccion");

  const payload = {
    op_id: generarOpId(),
    sede: sedeQR,
    tanques: items
  };

  sendWithOffline(
    "/glp/finalizar_calefaccion_batch",
    payload,
    mostrarResumenOperacion
  );
}

// Background Sync + mensajes desde el Service Worker para cola GLP
if ("serviceWorker" in navigator && "SyncManager" in window) {
  window.addEventListener("online", () => {
    navigator.serviceWorker.ready
      .then(reg => reg.sync.register("sync-glp-queue"))
      .catch(err => console.warn("No se pudo registrar sync-glp-queue:", err));
  });
}

if ("serviceWorker" in navigator) {
  navigator.serviceWorker.addEventListener("message", (event) => {
    if (
      event.data &&
      event.data.type === "BQA_GLPSYNC" &&
      event.data.action === "flushQueue" &&
      typeof flushOfflineQueue === "function"
    ) {
      flushOfflineQueue();
    }
  });
}

// =======================================================
// PROTECCIÓN DE CERRAR SESIÓN (logout) 100% BLINDADA
// =======================================================
function actualizarBloqueoLogout() {
  const logoutLinks = document.querySelectorAll('a[href="/logout"]');

  logoutLinks.forEach(a => {
    a.onclick = null; 

    a.addEventListener("click", async function (e) {
      e.preventDefault(); 

      if (typeof GLP_OPERACION_EN_CURSO !== 'undefined' && GLP_OPERACION_EN_CURSO) {
        alert("⛔ Debes terminar la operación actual antes de cerrar sesión.");
        return;
      }

      if (!navigator.onLine) {
        alert("📡 Estás sin conexión. El cierre de sesión requiere internet.");
        return;
      }

      try {
        if (typeof getQueue === 'function') {
          const q = await getQueue();
          if (q && q.length > 0) {
            alert("⛔ ACCESO DENEGADO\n\nTienes operaciones guardadas en el celular que aún no se han subido al servidor.\n\nPor favor, espera a que se sincronicen o presiona 'Intentar enviar ahora' antes de salir.");
            return; 
          }
        }
      } catch (err) {
        console.error("Error leyendo cola offline:", err);
      }

      window.location.href = "/logout";
    });
  });
}

async function sincronizarNivelesServidor(sede) {
  // Si no hay internet, no tiene sentido intentar
  if (!navigator.onLine) return;

  console.log(`[Sync-Fondo] Refrescando niveles oficiales para: ${sede}...`);

  try {
    const response = await fetch("/glp/obtener_tanques", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ sede: sede })
    });
    
    const data = await response.json();
    
    if (data.success && Array.isArray(data.tanques)) {
      // Guardamos los niveles oficiales en la caché local
      cacheTanquesLocal(sede, data.tanques);
      console.log("✅ [Sync-Fondo] Caché actualizada con éxito desde el servidor.");
      
      // Si estamos en la pantalla de tanques, podríamos refrescar la UI (opcional)
      // if (typeof dibujarPasoTanques === 'function' && document.getElementById("cont-tanques")) {
      //    dibujarPasoTanques(data.tanques);
      // }
    }
  } catch (e) {
    console.warn("⚠️ [Sync-Fondo] Error al sincronizar niveles en segundo plano:", e);
  }
}
/* ============================
   CONFIRMAR ARRIBO DE POLLITOS
============================ */
async function iniciarConfirmacionArribo() {
    activarProteccionOperacion();
    opActual = "confirmar_arribo";
    
    if (typeof configurarModoFoto === 'function') {
        configurarModoFoto(false);
    }

    try {
        // 🛠️ LA SOLUCIÓN: Pausa de medio segundo (500ms)
        // Le da tiempo al navegador de cerrar la ventana anterior y calcular el tamaño 
        // de la pantalla para que la cámara NO se renderice con tamaño de 0 píxeles.
        await new Promise(r => setTimeout(r, 500));

        // 1. Pedir QR (Obligatorio para saber A QUÉ GRANJA le vamos a registrar los pollitos)
        const info = await leerQR("Escanea el QR de la sede para confirmar el arribo");
        sedeQR = info.sede;

        // 2. Revisar si la granja escaneada realmente está esperando pollitos
        const infoLote = JSON.parse(localStorage.getItem(`estado_lote_${sedeQR}`));

        if (infoLote && infoLote.esperando_pollito) {
            // Si todo está bien, abre el formulario del pollito
            abrirModalArribo();
        } else {
            alert("✅ Esta granja (" + sedeQR + ") no tiene un arribo pendiente o ya fue confirmado.");
            mostrarMenu();
        }
    } catch (e) {
        alert("No fue posible leer el QR: " + e);
        mostrarMenu();
    }
}

function abrirModalArribo() {
    // Ocultar la cámara y textos detrás del modal
    const lector = document.getElementById("lectorQR");
    const convo = document.getElementById("conversacion");
    const texto = document.getElementById("preguntaTexto");
    if (lector) lector.style.display = "none";
    if (convo) convo.style.display = "none";
    if (texto) texto.textContent = "";

    // Mostrar el Modal Flotante
    document.getElementById('modalArribo').style.display = 'flex';
}

function cerrarModalArribo() {
    document.getElementById('modalArribo').style.display = 'none';
    mostrarMenu(); // Regresar al menú si el operador cancela
}

async function ejecutarConfirmarArribo() {
    const poblacion = document.getElementById('poblacion_real').value;
    const fecha = document.getElementById('fecha_arribo_real').value;

    if (!poblacion || !fecha) {
        alert("⚠️ Por favor completa ambos campos.");
        return;
    }

    const dataConfirmacion = {
        operacion: 'confirmar_arribo',
        sede: sedeQR,
        poblacion: poblacion,
        fecha_arribo: fecha,
        timestamp: new Date().toISOString()
    };

    // 1. Guardar en la cola offline (IndexedDB)
    await enqueueOffline(dataConfirmacion);
    
    // 2. Actualizar estado local para que ya NO vuelva a pedir pollitos en esa sede
    localStorage.setItem(`estado_lote_${sedeQR}`, JSON.stringify({esperando_pollito: false}));
    
    // 3. Cerrar modal y avisar éxito
    document.getElementById('modalArribo').style.display = 'none';
    alert("✅ Arribo registrado localmente. Se notificará a Telegram al sincronizar.");
    
    mostrarMenu(); // Volver a la pantalla principal
    
    // Intentar sincronizar si hay red de inmediato
    if (navigator.onLine) {
        if (typeof flushOfflineQueue === "function") flushOfflineQueue();
    }
}

window.addEventListener("load", actualizarBloqueoLogout);
window.addEventListener("online", actualizarBloqueoLogout);
window.addEventListener("offline", actualizarBloqueoLogout);