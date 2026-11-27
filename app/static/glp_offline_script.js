// ==========================================================
// MOTOR OPERATIVO HÍBRIDO BQA-ONE GLP (ONLINE + OFFLINE PWA)
// ==========================================================

// Variable global para controlar el candado de pedido en tránsito
let pedidoPendienteGlobal = null;

// Candado de concurrencia para evitar ejecuciones paralelas en flushOfflineQueue
let IS_SYNCING = false;

// Estado operativo en curso y variables de flujo
let GLP_OPERACION_EN_CURSO = false;
let sedeQR = "";
let empresaQR = "";
let tanques = [];
let tanquesQR = [];
let items = [];
let idx = 0;
let opActual = "";
let currentOpId = null;
let currentDiasExtra = 0;
let respuestaConsumoPendiente = null;
let tanquesDisponibles = [];
let tanqueActualIndex = 0;
let tanqueActualDatos = null;

// Normalización de sesión multi-tenant (insensible a mayúsculas y espacios finales)
const EMPRESA_SESION = String(window.EMPRESA_SESION || "").replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
const ES_EMPRESA_AVICOLA = EMPRESA_SESION.toUpperCase() === "POLLOS GAR SAS";

// Funcionalidad CSRF BQA-ONE (Ley 4)
const getCsrfToken = () => {
  const el = document.getElementById("csrf_token");
  return el ? el.value : "";
};

function generarOpId() {
  if (window.crypto && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return "op_" + Date.now() + "_" + Math.random().toString(16).slice(2);
}

let registrarTanqueoData = {
  sede: "",
  tanques: [],
  op_id: generarOpId()
};

/* ==========================================================
NOTIFICACIONES PUSH LOCALES (WEB NOTIFICATIONS API)
========================================================== */
function solicitarPermisoNotificaciones() {
  if (!("Notification" in window)) {
    return;
  }
  if (Notification.permission !== "granted" && Notification.permission !== "denied") {
    Notification.requestPermission().then(permission => {
      if (permission === "granted") {
        console.log("Permiso de notificaciones concedido.");
      }
    }).catch(() => {});
  }
}

window.addEventListener("load", solicitarPermisoNotificaciones);

function enviarNotificacionPushLocal(titulo, cuerpo) {
  if (!("Notification" in window)) return;

  if (Notification.permission === "granted") {
    if (navigator.serviceWorker && navigator.serviceWorker.ready) {
      navigator.serviceWorker.ready.then(registration => {
        registration.showNotification(titulo, {
          body: cuerpo,
          icon: "/static/icon-192.png",
          vibrate: [200, 100, 200]
        });
      }).catch(() => {});
    } else {
      try {
        new Notification(titulo, {
          body: cuerpo,
          icon: "/static/icon-192.png"
        });
      } catch (e) {}
    }
  }
}

/* ==========================================================
FLUJO ATÓMICO: CONSUMO + NEGOCIACIÓN (OFFLINE FIRST)
========================================================== */
function enviarConsumo() {
  if (!items || items.length === 0) {
    alert("Error: No hay lecturas de tanques cargadas.");
    mostrarMenu();
    return;
  }

  // 1. Evaluación Local Inmediata (Copiloto Offline/Online) SIN mutar caché antes de enviar
  let sumaCap = 0;
  let sumaNivelCap = 0;
  let algunTanqueCritico = false;

  items.forEach(tk => {
    const cap = parseFloat(tk.capacidad || 0);
    const niv = parseFloat(tk.nivel || 0);
    sumaCap += cap;
    sumaNivelCap += (niv * cap);

    if (niv <= 30.0) {
      algunTanqueCritico = true;
    }
  });

  const nivelPromedio = sumaCap > 0 ? (sumaNivelCap / sumaCap) : 0;

  // 2. Peaje de Negociación si promedio <= 30% o algún tanque individual <= 30%
  if (nivelPromedio <= 30.0 || algunTanqueCritico) {
    abrirNegociacionLocal(80);
  } else {
    ejecutarEnvioAtomico(null);
  }
}

function abrirNegociacionLocal(sugerencia) {
  const modal = document.getElementById("modalNegociacion");
  const inputNivel = document.getElementById("neg_nivel");
  const spanDiasAct = document.getElementById("neg_dias_act");
  const spanDiasExtra = document.getElementById("neg_dias_extra");

  if (inputNivel) inputNivel.value = sugerencia;
  if (spanDiasAct) spanDiasAct.innerText = "--";

  currentDiasExtra = 0;
  if (spanDiasExtra) spanDiasExtra.innerText = "0";

  if (modal) {
    modal.style.display = "flex";
  }
}

function confirmarSolicitud() {
  const modal = document.getElementById("modalNegociacion");
  const inputNivel = document.getElementById("neg_nivel");
  const nivelSolicitado = Number(inputNivel ? inputNivel.value : 80);

  if (isNaN(nivelSolicitado) || nivelSolicitado <= 0 || nivelSolicitado > 100) {
    alert("Ingresa un nivel objetivo válido entre 1 y 100%.");
    return;
  }

  if (modal) modal.style.display = "none";

  ejecutarEnvioAtomico({
    nivel: nivelSolicitado,
    dias_extra: currentDiasExtra
  });
}

function omitirSolicitudNegociacion() {
  const modal = document.getElementById("modalNegociacion");
  if (modal) modal.style.display = "none";
  ejecutarEnvioAtomico(null);
}

function ejecutarEnvioAtomico(solicitudGas) {
  const textoConfirmacion = solicitudGas
    ? `registrar el consumo y solicitar llenado de gas al ${solicitudGas.nivel}% para ${sedeQR}`
    : `registrar el consumo sin solicitar pedido de gas para ${sedeQR}`;

  if (!window.confirm(`¿Está seguro de ${textoConfirmacion}?`)) {
    return;
  }

  const sedeDestino = sedeQR;
  const copiaItems = items.map(it => ({ ...it }));

  const payload = {
    op_id: generarOpId(),
    sede: sedeDestino,
    tanques: copiaItems
  };

  if (solicitudGas) {
    payload.solicitud_gas = solicitudGas;
  }

  sendWithOffline(
    "/glp/registrar_consumo",
    payload,
    (res) => {
      if (res && res.offline) {
        actualizarCacheUltimoNivel(sedeDestino, copiaItems, "consumo", {
          hay_pedido_pendiente: !!solicitudGas,
          bloqueado_por_pedido: !!solicitudGas
        });
      } else {
        localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeDestino);
        sincronizarNivelesServidor(sedeDestino);
      }
      mostrarResumenOperacion(res);
    }
  );
}

// ==========================================
// CONFIGURAR CÁMARA VS GALERÍA
// ==========================================
function configurarModoFoto(permitirGaleria) {
  const input = document.getElementById("inputFoto");
  if (!input) return;

  if (permitirGaleria) {
    input.removeAttribute("capture");
  } else {
    input.setAttribute("capture", "environment");
  }
}

// =======================================================
// PROTECCIÓN BASE CONTRA RECARGA, CIERRE Y BOTÓN ATRÁS
// =======================================================
function bloquearSalida(e) {
  if (GLP_OPERACION_EN_CURSO) {
    e.preventDefault();
    e.returnValue = "";
    return "";
  }
}

function activarProteccionOperacion() {
  GLP_OPERACION_EN_CURSO = true;
  window.addEventListener("beforeunload", bloquearSalida);
}

function desactivarProteccionOperacion() {
  GLP_OPERACION_EN_CURSO = false;
  window.removeEventListener("beforeunload", bloquearSalida);
}

function bloquearBotonAtras() {
  history.pushState(null, "", location.href);

  window.onpopstate = function () {
    if (GLP_OPERACION_EN_CURSO) {
      history.pushState(null, "", location.href);
      alert("No puedes retroceder mientras realizas una operación GLP.");
    } else {
      history.go(-1);
    }
  };
}

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

      if (Array.isArray(obj.tanques)) {
        tanques_qr = obj.tanques.map(t => ({
          numero: String(t.numero || "").toLowerCase().trim(),
          capacidad: Number(t.capacidad || 0)
        })).filter(t => t.numero);
      }
    }
  } catch (_) {}

  // 2) URL
  if (!sede) {
    try {
      const u = new URL(decodedText);
      sede = (u.searchParams.get("sede") || "").trim();
      empresa = (u.searchParams.get("empresa") || "").trim();
    } catch (_) {}
  }

  // 3) "SEDE | EMPRESA"
  if (!sede && String(decodedText || "").includes("|")) {
    const partes = String(decodedText).split("|");
    sede = (partes[0] || "").trim();
    empresa = (partes[1] || "").trim();
  }

  // 4) Texto plano
  if (!sede) {
    sede = String(decodedText || "").trim();
  }

  sede = sede.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();
  empresa = empresa.replace(/\u00A0/g, " ").replace(/\s+/g, " ").trim();

  return { sede, empresa, tanques: tanques_qr };
}

/* ============================
MONITOR operatividad_pwa (AUTO)
============================ */
const FORCE_OFFLINE_KEY = "glp_force_offline";

function isForceOffline() {
  return localStorage.getItem(FORCE_OFFLINE_KEY) === "1";
}

function setForceOffline(v) {
  localStorage.setItem(FORCE_OFFLINE_KEY, v ? "1" : "0");

  if (navigator.serviceWorker && navigator.serviceWorker.controller) {
    navigator.serviceWorker.controller.postMessage({
      type: "GLP_FORCE_OFFLINE",
      value: !!v
    });
  }
}

const OPERATIVIDAD = {
  CHECK_EVERY_MS: 8000,
  TIMEOUT_MS: 6000,
  ENDPOINT: "/glp/context"
};

let _opw = {
  timer: null,
  inFlight: false
};

async function pingOperatividadPWA() {
  if (_opw.inFlight) return !isForceOffline();
  _opw.inFlight = true;

  const ctrl = new AbortController();
  const timeout = setTimeout(() => ctrl.abort(), OPERATIVIDAD.TIMEOUT_MS);

  try {
    const pingUrl = OPERATIVIDAD.ENDPOINT + "?ping=" + Date.now();
    const r = await fetch(pingUrl, {
      method: "GET",
      cache: "no-store",
      credentials: "include",
      redirect: "manual",
      signal: ctrl.signal,
      headers: {
        "Accept": "application/json",
        "X-CSRFToken": getCsrfToken()
      }
    });

    clearTimeout(timeout);
    _opw.inFlight = false;

    if (r.type === "opaqueredirect" || r.status === 401 || r.status === 403) {
      return false;
    }

    if (!r.ok) return false;

    const ct = (r.headers.get("content-type") || "").toLowerCase();
    if (!ct.includes("application/json")) return false;

    const j = await r.json();
    return !!(j && j.success);
  } catch (e) {
    clearTimeout(timeout);
    _opw.inFlight = false;
    return false;
  }
}

async function evaluarYAplicarOperatividadPWA() {
  const isOnline = await pingOperatividadPWA();

  if (!isOnline) {
    if (!isForceOffline()) {
      setForceOffline(true);
      console.log("[Operatividad] Entrando en modo OFFLINE...");
    }
  } else {
    if (isForceOffline()) {
      setForceOffline(false);
      console.log("[Operatividad] Volviendo a modo ONLINE...");
      flushOfflineQueue();
    }
  }
}

function iniciarMonitorOperatividadPWA() {
  if (_opw.timer) return;

  evaluarYAplicarOperatividadPWA();
  _opw.timer = setInterval(evaluarYAplicarOperatividadPWA, OPERATIVIDAD.CHECK_EVERY_MS);

  window.addEventListener("online", () => evaluarYAplicarOperatividadPWA());
  window.addEventListener("offline", () => evaluarYAplicarOperatividadPWA());
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) evaluarYAplicarOperatividadPWA();
  });
}

window.addEventListener("load", () => {
  setForceOffline(isForceOffline());
  iniciarMonitorOperatividadPWA();
});

// ===================================================
//   COLA OFFLINE EN INDEXEDDB (SOPORTA FOTOS GRANDES)
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

async function enqueueOffline(item) {
  await idbAdd({ ...item, ts: Date.now() });
}

async function getQueue() {
  try {
    return await idbGetAll();
  } catch {
    return [];
  }
}

async function setQueue(q) {
  await idbClear();
  for (const it of q) {
    await idbAdd(it);
  }
}

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
  if (queue.length === 0) {
    const warningEmpty = document.getElementById("syncWarning");
    if (warningEmpty) warningEmpty.style.display = "none";
    return;
  }

  IS_SYNCING = true;
  console.log(`[Sync] Intentando sincronizar ${queue.length} operaciones...`);

  let sincronizadasExito = 0;
  let rechazadasNegocio = [];

  for (const item of queue) {
    try {
      let targetUrl = item.url || "/glp/registrar_consumo";
      let payloadToSync = item.payload ? { ...item.payload } : { ...item };

      // Compatibilidad retroactiva con confirmaciones de arribo antiguas guardadas en IndexedDB
      if (item.operacion === "confirmar_arribo" && !item.url) {
        targetUrl = "/glp/confirmar_arribo_pollito";
        payloadToSync = { ...item };
      }

      payloadToSync.is_sync = true;

      const response = await fetch(targetUrl, {
        method: "POST",
        credentials: "include",
        redirect: "manual",
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "X-CSRFToken": getCsrfToken()
        },
        body: JSON.stringify(payloadToSync)
      });

      // Si la sesión expiró (redirect a /login o 401/403), NO borrar la cola y avisar una sola vez
      if (response.type === "opaqueredirect" || response.status === 401 || response.status === 403) {
        if (!GLP_LOGIN_WARNING_SHOWN) {
          GLP_LOGIN_WARNING_SHOWN = true;
          alert("⚠️ Tu sesión expiró. Inicia sesión nuevamente con internet para subir las operaciones retenidas.");
        }
        break;
      }

      // Error transitorio de servidor (502/503/504): conservar en cola para el próximo ciclo
      if (response.status >= 500) {
        console.warn(`[Sync] Error temporal HTTP ${response.status}. Se reintentará luego.`);
        break;
      }

      const ct = (response.headers.get("content-type") || "").toLowerCase();
      if (!ct.includes("application/json")) {
        console.warn("[Sync] Respuesta no JSON (posible portal cautivo o redirección). Se reintentará luego.");
        break;
      }

      const resData = await response.json();
      const sedeItem = (payloadToSync && payloadToSync.sede) || item.sede || "";

      if (resData.success || resData.idempotent || (resData.message && resData.message.includes("recuperada"))) {
        console.log(`[Sync] Éxito en operación ID local: ${item.id}`);

        if (targetUrl.includes("confirmar_arribo_pollito") && sedeItem) {
          localStorage.setItem(`estado_lote_${sedeItem}`, JSON.stringify({ esperando_pollito: false }));
        }

        if (sedeItem) {
          localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeItem);
        }

        await removeFromQueue(item.id);
        sincronizadasExito++;
      } else {
        // PROTECCIÓN ANTI POISON-PILL: Si el servidor respondió HTTP 200/400 con rechazo definitivo de negocio
        // (ej. lote ya cerrado o lectura inválida), retirar de la cola para NO martillar infinitamente ni quemar IDs.
        const msgRechazo = resData.message || "Rechazada por regla de negocio";
        console.warn(`[Sync] Operación descartada por rechazo definitivo del servidor (ID local ${item.id}): ${msgRechazo}`);

        if (sedeItem) {
          localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeItem);
          localStorage.removeItem(`estado_lote_${sedeItem}`);
        }

        await removeFromQueue(item.id);
        rechazadasNegocio.push(`• ${sedeItem || "Sede"}: ${msgRechazo}`);
      }
    } catch (err) {
      console.error("[Sync] Error de red durante sincronización:", err);
      break;
    }
  }

  IS_SYNCING = false;

  if (rechazadasNegocio.length > 0) {
    alert(
      "⚠️ ATENCIÓN EN SINCRONIZACIÓN:\n\n" +
      "Las siguientes operaciones retenidas fueron descartadas por el servidor por inconsistencia con el estado actual del lote:\n\n" +
      rechazadasNegocio.join("\n")
    );
  }

  const remaining = await getQueue();
  const warning = document.getElementById("syncWarning");

  if (remaining.length === 0) {
    if (warning) warning.style.display = "none";

    if (sincronizadasExito > 0) {
      GLP_LOGIN_WARNING_SHOWN = false;
      enviarNotificacionPushLocal(
        "✅ Sincronización Completada",
        `Se han subido ${sincronizadasExito} registros retenidos.`
      );

      const modalExito = document.getElementById("modal-sync-exito");
      const msgExito = document.getElementById("msg-sync-exito");
      if (modalExito && msgExito) {
        msgExito.innerText = `Se ejecutaron ${sincronizadasExito} operaciones retenidas correctamente. La caché ha sido actualizada.`;
        modalExito.style.display = "flex";
      } else {
        alert(`✅ SINCRONIZACIÓN EXITOSA\nSe han subido ${sincronizadasExito} registros a la base de datos central.`);
      }

      if (sedeQR) {
        sincronizarNivelesServidor(sedeQR);
      }
    }
  } else {
    mostrarAvisoSyncPendiente();
  }
}

function programarFlushOffline() {
  window.addEventListener("online", async () => {
    console.log("[GLP offline] Señal detectada. Iniciando secuencia de sincronización...");

    try {
      const q = await getQueue();
      if (q.length > 0) {
        enviarNotificacionPushLocal("⚠️ BQA-ONE", "Tienes operaciones pendientes. Abre la app para sincronizar.");
      }
    } catch (e) {
      console.warn("No se pudo verificar la cola al volver online:", e);
    }

    setTimeout(() => {
      flushOfflineQueue();
      evaluarYAplicarOperatividadPWA();
    }, 2500);

    setTimeout(() => {
      flushOfflineQueue();
    }, 6000);

    setTimeout(() => {
      flushOfflineQueue();
    }, 12000);
  });

  window.addEventListener("load", async () => {
    try {
      const q = await getQueue();
      if (q.length > 0) {
        mostrarAvisoSyncPendiente();
      }
    } catch (e) {
      console.error("Error verificando cola al inicio:", e);
    }

    setTimeout(flushOfflineQueue, 1500);
  });

  setInterval(() => {
    if (navigator.onLine && !isForceOffline()) {
      flushOfflineQueue();
    }
  }, 10000);
}

programarFlushOffline();

// ===================================================
//   CACHÉ LOCAL DE TANQUES Y METADATOS DE SEDE
// ===================================================
const TANQUES_CACHE_PREFIX = "tanques_cache_";

function guardarEstadoCompletoSedeCache(sede, data) {
  if (!sede || !data) return;
  const previo = getTanquesLocal(sede) || {};
  const tanquesAGuardar = (Array.isArray(data.tanques) && data.tanques.length > 0)
    ? data.tanques
    : (Array.isArray(previo.tanques) ? previo.tanques : []);

  localStorage.setItem(TANQUES_CACHE_PREFIX + sede, JSON.stringify({
    tanques: tanquesAGuardar,
    bloqueado_por_arribo: Boolean(data.bloqueado_por_arribo),
    motivo_bloqueo: data.motivo_bloqueo || "",
    bloqueado_por_pedido: Boolean(data.bloqueado_por_pedido),
    motivo_bloqueo_pedido: data.motivo_bloqueo_pedido || "",
    hay_pedido_pendiente: Boolean(data.hay_pedido_pendiente),
    codigo_pedido_pendiente: data.codigo_pedido_pendiente || "",
    lote_activo: Boolean(data.lote_activo),
    info_lote: data.info_lote || "",
    ts: Date.now()
  }));
}

function cacheTanquesLocal(sede, tanquesLista) {
  const previo = getTanquesLocal(sede) || {};
  localStorage.setItem(TANQUES_CACHE_PREFIX + sede, JSON.stringify({
    ...previo,
    tanques: tanquesLista,
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
//   OBTENER TANQUES CON FALLBACK: SERVIDOR -> QR+CACHE -> CACHE
// ===================================================
async function obtenerTanquesConFallback(sede) {
  try {
    // 1. Intentar obtener datos oficiales del servidor (Single Source of Truth)
    if (navigator.onLine && !isForceOffline()) {
      try {
        const response = await fetch("/glp/obtener_tanques", {
          method: "POST",
          credentials: "include",
          redirect: "manual",
          headers: {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-CSRFToken": getCsrfToken()
          },
          body: JSON.stringify({ sede: sede }),
          cache: "no-store"
        });

        if (response.ok && (response.headers.get("content-type") || "").toLowerCase().includes("application/json")) {
          const data = await response.json();

          if (data.success) {
            // Si el servidor responde success:true pero tanques viene vacío [] (sedes sin filas en tanques_sedes que usan QR),
            // fusionamos el hardware de tanquesQR SIN destruir lote_activo ni los candados oficiales del servidor.
            let tanquesFinalesServidor = Array.isArray(data.tanques) ? data.tanques : [];
            if (tanquesFinalesServidor.length === 0 && Array.isArray(tanquesQR) && tanquesQR.length > 0) {
              const prevCache = getTanquesLocal(sede);
              const nivelesPrevios = (prevCache && Array.isArray(prevCache.tanques)) ? prevCache.tanques : [];
              tanquesFinalesServidor = tanquesQR.map(tqr => {
                const matchPrev = nivelesPrevios.find(
                  c => String(c.numero).toLowerCase().trim() === String(tqr.numero).toLowerCase().trim()
                );
                return {
                  numero: tqr.numero,
                  capacidad: tqr.capacidad,
                  ultimo_nivel: matchPrev ? matchPrev.ultimo_nivel : null
                };
              });
            }

            const estadoGuardar = {
              ...data,
              tanques: tanquesFinalesServidor
            };
            guardarEstadoCompletoSedeCache(sede, estadoGuardar);

            // Sincronizar bandera local de espera de pollito con la verdad del servidor
            if (data.lote_activo) {
              localStorage.setItem(
                `estado_lote_${sede}`,
                JSON.stringify({ esperando_pollito: Boolean(data.bloqueado_por_arribo) })
              );
            } else {
              localStorage.removeItem(`estado_lote_${sede}`);
            }

            pedidoPendienteGlobal = data.hay_pedido_pendiente ? data.codigo_pedido_pendiente : null;

            return {
              tanques: tanquesFinalesServidor,
              fuente: "servidor",
              lote_activo: Boolean(data.lote_activo),
              info_lote: data.info_lote || "",
              bloqueado_por_arribo: Boolean(data.bloqueado_por_arribo),
              motivo_bloqueo: data.motivo_bloqueo || "",
              bloqueado_por_pedido: Boolean(data.bloqueado_por_pedido),
              motivo_bloqueo_pedido: data.motivo_bloqueo_pedido || ""
            };
          } else {
            // El servidor rechazó explícitamente (ej. sesión inválida)
            alert(data.message || "No fue posible validar la sede en el servidor.");
            return null;
          }
        }
      } catch (e) {
        console.warn("Fallo fetch servidor por red, aplicando fallback offline...", e);
      }
    }

    // 2. FALLBACK NIVEL 2: QR embebido + Caché local de niveles y candados
    if (tanquesQR && tanquesQR.length > 0) {
      const cachedData = getTanquesLocal(sede);
      const nivelesEnCache = (cachedData && Array.isArray(cachedData.tanques)) ? cachedData.tanques : [];

      const tanquesFusionados = tanquesQR.map(tqr => {
        let nivelRecuperado = null;
        const coincidencia = nivelesEnCache.find(c =>
          String(c.numero).toLowerCase().trim() === String(tqr.numero).toLowerCase().trim()
        );
        if (coincidencia && coincidencia.ultimo_nivel !== undefined) {
          nivelRecuperado = coincidencia.ultimo_nivel;
        }
        return {
          numero: tqr.numero,
          capacidad: tqr.capacidad,
          etiqueta: tqr.numero,
          ultimo_nivel: nivelRecuperado
        };
      });

      pedidoPendienteGlobal = (cachedData && cachedData.hay_pedido_pendiente)
        ? cachedData.codigo_pedido_pendiente
        : null;

      return {
        tanques: tanquesFusionados,
        fuente: "qr_fusionado",
        lote_activo: cachedData ? Boolean(cachedData.lote_activo) : false,
        info_lote: cachedData ? (cachedData.info_lote || "") : "",
        bloqueado_por_arribo: cachedData ? Boolean(cachedData.bloqueado_por_arribo) : false,
        motivo_bloqueo: cachedData ? (cachedData.motivo_bloqueo || "") : "",
        bloqueado_por_pedido: cachedData ? Boolean(cachedData.bloqueado_por_pedido) : false,
        motivo_bloqueo_pedido: cachedData ? (cachedData.motivo_bloqueo_pedido || "") : ""
      };
    }

    // 3. FALLBACK NIVEL 3: Caché local pura
    const finalCache = getTanquesLocal(sede);
    if (finalCache && Array.isArray(finalCache.tanques) && finalCache.tanques.length > 0) {
      pedidoPendienteGlobal = finalCache.hay_pedido_pendiente
        ? finalCache.codigo_pedido_pendiente
        : null;

      return {
        tanques: finalCache.tanques,
        fuente: "cache_pura",
        lote_activo: Boolean(finalCache.lote_activo),
        info_lote: finalCache.info_lote || "",
        bloqueado_por_arribo: Boolean(finalCache.bloqueado_por_arribo),
        motivo_bloqueo: finalCache.motivo_bloqueo || "",
        bloqueado_por_pedido: Boolean(finalCache.bloqueado_por_pedido),
        motivo_bloqueo_pedido: finalCache.motivo_bloqueo_pedido || ""
      };
    }

    return null;
  } catch (err) {
    console.error("Error crítico en obtenerTanquesConFallback:", err);
    return null;
  }
}

// ===================================================
// ACTUALIZAR CACHÉ DE NIVELES POST-CONFIRMACIÓN OFFLINE
// ===================================================
function actualizarCacheUltimoNivel(sede, itemsGuardados, operacion, overridesEstado = {}) {
  if (!sede) return;
  const cacheKey = TANQUES_CACHE_PREFIX + sede;
  const cacheRaw = localStorage.getItem(cacheKey);
  let datosSede = null;

  try {
    datosSede = cacheRaw ? JSON.parse(cacheRaw) : { tanques: [] };
  } catch {
    datosSede = { tanques: [] };
  }

  if (!Array.isArray(datosSede.tanques)) {
    datosSede.tanques = [];
  }

  (itemsGuardados || []).forEach(item => {
    let tkCache = datosSede.tanques.find(
      t => String(t.numero).toLowerCase().trim() === String(item.numero).toLowerCase().trim()
    );

    if (!tkCache) {
      tkCache = {
        numero: item.numero,
        capacidad: Number(item.capacidad || 250),
        ultimo_nivel: null
      };
      datosSede.tanques.push(tkCache);
    }

    if (operacion === "tanqueo" && item.nivel_final !== undefined && item.nivel_final !== null) {
      tkCache.ultimo_nivel = Number(item.nivel_final);
    } else if (item.nivel !== undefined && item.nivel !== null) {
      tkCache.ultimo_nivel = Number(item.nivel);
    }
  });

  if (operacion === "inicio_calefaccion") {
    datosSede.lote_activo = true;
    datosSede.bloqueado_por_arribo = true;
    datosSede.motivo_bloqueo = "Debes confirmar la llegada real del pollito antes de registrar consumos, tanqueos o cierres.";
  } else if (operacion === "tanqueo") {
    datosSede.hay_pedido_pendiente = false;
    datosSede.bloqueado_por_pedido = false;
    datosSede.codigo_pedido_pendiente = "";
    datosSede.motivo_bloqueo_pedido = "";
  } else if (operacion === "finalizar_calefaccion") {
    datosSede.lote_activo = false;
    datosSede.bloqueado_por_arribo = false;
    datosSede.bloqueado_por_pedido = false;
    datosSede.hay_pedido_pendiente = false;
  }

  Object.assign(datosSede, overridesEstado, { ts: Date.now() });
  localStorage.setItem(cacheKey, JSON.stringify(datosSede));
}

// ===================================================
//   ENVÍO POST CON OFFLINE-FALLBACK (SEMÁFORO ATÓMICO)
// ===================================================
function _inferirNombreOperacion(url) {
  if (url.includes("registrar_inicio")) return "inicio_calefaccion";
  if (url.includes("confirmar_arribo")) return "confirmar_arribo";
  if (url.includes("consumo")) return "consumo";
  if (url.includes("tanqueo")) return "tanqueo";
  if (url.includes("finalizar_calefaccion")) return "finalizar_calefaccion";
  return "operacion";
}

function _construirRespuestaOffline(url, payload) {
  const nombreOp = _inferirNombreOperacion(url);
  const tksResumen = Array.isArray(payload.tanques)
    ? payload.tanques.map(t => ({
        numero: t.numero,
        nivel: t.nivel !== undefined ? t.nivel : t.nivel_final,
        capacidad: t.capacidad
      }))
    : [];

  return {
    success: true,
    offline: true,
    esperando_pollito: nombreOp === "inicio_calefaccion",
    message: "Guardado offline en el dispositivo.",
    resumen: {
      operacion: nombreOp,
      sede: payload.sede,
      saldo_estimado_kg: "Calculando...",
      kg_consumidos: "Calculando...",
      tanques: tksResumen,
      requiere_gas: Boolean(payload.solicitud_gas),
      razon_alerta: payload.solicitud_gas ? "Pedido retenido offline." : ""
    }
  };
}

async function sendWithOffline(url, payload, onSuccess) {
  const btn = document.getElementById("botonEnviar");
  const textoOriginal = btn ? btn.textContent : "Enviar";

  // 1. MODO OFFLINE DETECTADO PREVIAMENTE POR EL MONITOR PWA
  if (!navigator.onLine || isForceOffline()) {
    await enqueueOffline({ url, payload });
    dispararNotificacionBackground(payload.sede);
    mostrarAvisoSyncPendiente();

    const fakeResponse = _construirRespuestaOffline(url, payload);
    limpiarEstadoOperacion();

    if (typeof onSuccess === "function") {
      onSuccess(fakeResponse);
    } else {
      mostrarModalNaranja();
    }
    return fakeResponse;
  }

  // 2. BUCLE DE REINTENTOS ACTIVOS ONLINE (3 intentos, cada 20 seg)
  const INTENTOS_MAXIMOS = 3;
  const INTERVALO_MS = 20000;

  for (let intento = 1; intento <= INTENTOS_MAXIMOS; intento++) {
    try {
      if (intento > 1) {
        if (btn) {
          btn.textContent = `⏳ Reintentando red... (${intento}/3)`;
          btn.style.backgroundColor = "#ff9800";
          btn.disabled = true;
          btn.style.display = "inline-block";
        }
        await new Promise(r => setTimeout(r, INTERVALO_MS));
      } else if (btn) {
        btn.textContent = "Procesando...";
        btn.disabled = true;
      }

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);

      const r = await fetch(url, {
        method: "POST",
        credentials: "include",
        redirect: "manual",
        headers: {
          "Content-Type": "application/json",
          "Accept": "application/json",
          "X-CSRFToken": getCsrfToken()
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });

      clearTimeout(timeoutId);

      if (r.type === "opaqueredirect" || r.status === 401 || r.status === 403) {
        if (btn) {
          btn.textContent = textoOriginal;
          btn.disabled = false;
          btn.style.backgroundColor = "";
        }
        alert("⚠️ Tu sesión ha expirado. Por favor vuelve a iniciar sesión.");
        mostrarMenu();
        return { success: false, message: "Sesión expirada" };
      }

      const ct = (r.headers.get("content-type") || "").toLowerCase();
      if (!ct.includes("application/json")) {
        throw new Error("non-json-response");
      }

      const j = await r.json();

      if (!j.success) {
        alert(j.message || "Operación rechazada por el servidor.");
        if (btn) {
          btn.textContent = textoOriginal;
          btn.disabled = false;
          btn.style.backgroundColor = "";
        }
        mostrarMenu();
        return j;
      }

      // ÉXITO REAL EN EL SERVIDOR
      limpiarEstadoOperacion();
      if (btn) {
        btn.textContent = textoOriginal;
        btn.disabled = false;
        btn.style.backgroundColor = "";
      }
      if (typeof onSuccess === "function") {
        onSuccess(j);
      }
      return j;

    } catch (e) {
      const msg = String(e || "");
      const isNetworkError =
        !navigator.onLine ||
        msg.includes("Failed to fetch") ||
        msg.includes("NetworkError") ||
        msg.includes("Load failed") ||
        msg.includes("AbortError") ||
        msg.includes("non-json-response");

      if (!isNetworkError) {
        alert("Error crítico del sistema: " + msg);
        if (btn) {
          btn.textContent = textoOriginal;
          btn.disabled = false;
          btn.style.backgroundColor = "";
        }
        throw e;
      }

      if (intento === INTENTOS_MAXIMOS) {
        console.warn("⚠️ Agotados 3 intentos de red. Transfiriendo operación a IndexedDB.");
        break;
      }
    }
  }

  // 3. FALLBACK A MODO OFFLINE TRAS AGOTAR LOS 3 INTENTOS DE RED
  setForceOffline(true);
  await enqueueOffline({ url, payload });
  dispararNotificacionBackground(payload.sede);
  mostrarAvisoSyncPendiente();

  const fakeResponseFallback = _construirRespuestaOffline(url, payload);
  limpiarEstadoOperacion();

  if (btn) {
    btn.textContent = textoOriginal;
    btn.disabled = false;
    btn.style.backgroundColor = "";
  }

  if (typeof onSuccess === "function") {
    onSuccess(fakeResponseFallback);
  } else {
    mostrarModalNaranja();
  }
  return fakeResponseFallback;
}

// ==========================================
// FUNCIONES AUXILIARES OFFLINE Y LECTOR QR
// ==========================================
function mostrarModalNaranja() {
  const modalNaranja = document.getElementById("modal-offline-naranja");
  if (modalNaranja) {
    modalNaranja.style.display = "flex";
  } else {
    alert("📡 GUARDADO EN CELULAR (OFFLINE)\n\nLos datos se han guardado en tu dispositivo.");
    location.reload();
  }
}

function dispararNotificacionBackground(sede) {
  fetch("/glp/notificar_operaciones_pendientes_background", {
    method: "POST",
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      "X-CSRFToken": getCsrfToken()
    },
    body: JSON.stringify({ sede: sede || "Sede en campo" })
  }).catch(() => {});
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
        if (lector) lector.style.display = "none";
        resolve(normalizarQR(decodedText));
      }).catch(() => {
        lector.innerHTML = "";
        if (lector) lector.style.display = "none";
        resolve(normalizarQR(decodedText));
      });
    }).catch(err => {
      if (lector) lector.style.display = "none";
      reject(err);
    });
  });
}

/* ============================
Utilidades UI y Sincronización Manual
============================ */
function show(el, disp = "block") { if (el) el.style.display = disp; }
function hide(el) { if (el) el.style.display = "none"; }

function mostrarAvisoSyncPendiente() {
  const div = document.getElementById("syncWarning");
  if (!div) return;

  getQueue().then(queue => {
    if (!queue || queue.length === 0) {
      div.style.display = "none";
      return;
    }

    div.style.display = "block";
    div.innerHTML = `
      <div style="background: #333; color: white; padding: 20px; border-radius: 12px; text-align: left; margin-bottom: 20px; border-left: 5px solid #d9534f; box-shadow: 0 4px 10px rgba(0,0,0,0.2);">
        <h4 style="margin: 0 0 10px 0; color: #ff9800; font-size: 1.1rem; display: flex; align-items: center; gap: 8px;">
          <span style="font-size: 1.3rem;">⛔</span> OPERACIONES RETENIDAS
        </h4>
        <p style="font-size: 0.9rem; margin: 0 0 15px 0; color: #ddd; line-height: 1.4;">
          Tienes <strong>${queue.length} operaciones pendientes</strong> en la memoria del celular porque trabajaste sin conexión.<br>
          Si ya tienes señal estable, presiona el botón para enviarlas.
        </p>
        <button id="btnSyncManual" onclick="ejecutarSyncManual()" style="background: #2196f3; color: white; border: none; padding: 12px 15px; border-radius: 8px; font-weight: bold; width: 100%; cursor: pointer; text-transform: uppercase; letter-spacing: 1px;">
          🔄 Sincronizar Pendientes
        </button>
      </div>
    `;
  });
}

async function ejecutarSyncManual() {
  const btn = document.getElementById("btnSyncManual");

  if (btn) {
    btn.innerHTML = "⏳ Subiendo datos... Espere";
    btn.disabled = true;
    btn.style.opacity = "0.7";
  }

  try {
    await fetch("/glp/notificar_intento_sync", {
      method: "POST",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        "X-CSRFToken": getCsrfToken()
      },
      body: JSON.stringify({ sede: sedeQR ? sedeQR : "Sede Pendiente" })
    });
  } catch (e) {}

  setForceOffline(false);

  await new Promise(r => setTimeout(r, 400));
  await flushOfflineQueue();

  const remaining = await getQueue();
  if (remaining.length > 0) {
    mostrarAvisoSyncPendiente();
    const btnNuevo = document.getElementById("btnSyncManual");
    if (btnNuevo) {
      btnNuevo.innerHTML = "⚠️ Sin señal estable. Reintentar ahora";
      btnNuevo.disabled = false;
      btnNuevo.style.opacity = "1";
    }
  }
}

/* ============================
Panel de Resumen y Semáforo de Certidumbre
============================ */
function tituloOperacion(op) {
  switch (op) {
    case "inicio_calefaccion": return "Resumen - Inicio de calefacción";
    case "confirmar_arribo": return "Resumen - Arribo de pollitos";
    case "tanqueo": return "Resumen - Tanqueo de GLP";
    case "consumo": return "Resumen - Registro de consumo";
    case "finalizar_calefaccion": return "Resumen - Finalización de calefacción";
    default: return "Resumen de operación GLP";
  }
}

function mostrarResumenOperacion(res) {
  if (!res || !res.success) {
    alert((res && res.message) ? res.message : "Error desconocido.");
    const overlay = document.getElementById("loadingOverlay");
    if (overlay) overlay.style.display = "none";
    return;
  }

  const overlay = document.getElementById("loadingOverlay");
  if (overlay) overlay.style.display = "none";

  // Asegurar que no quede abierto el modal naranja simultáneamente encima
  const modalNaranja = document.getElementById("modal-offline-naranja");
  if (modalNaranja) modalNaranja.style.display = "none";

  const resumen = res.resumen || res;
  if (res.esperando_pollito && resumen && resumen.sede) {
    localStorage.setItem(`estado_lote_${resumen.sede}`, JSON.stringify({ esperando_pollito: true }));
  }

  const headerDiv = document.querySelector("#modal-resultado-final > div > div:first-child");
  if (headerDiv) {
    const tituloDiv = headerDiv.querySelector("h2");
    const subtituloDiv = headerDiv.querySelector("p");
    const iconoDiv = headerDiv.querySelector("div");

    if (res.offline) {
      headerDiv.style.background = "#f57c00";
      if (iconoDiv) iconoDiv.innerText = "📡";
      if (tituloDiv) tituloDiv.innerText = "Guardado en Celular";
      if (subtituloDiv) subtituloDiv.innerText = "PENDIENTE DE ENVÍO POR FALTA DE SEÑAL";
    } else {
      headerDiv.style.background = "#015249";
      if (iconoDiv) iconoDiv.innerText = "✅";
      if (tituloDiv) tituloDiv.innerText = "Operación Exitosa";
      if (subtituloDiv) subtituloDiv.innerText = "REGISTRO GUARDADO EN EL SERVIDOR";
    }
  }

  const opTitulo = String(resumen.operacion || "OPERACIÓN").replace(/_/g, " ").toUpperCase();
  const elOp = document.getElementById("bqa-operacion");
  if (elOp) elOp.innerText = opTitulo + (res.offline ? " (OFFLINE)" : "");

  const elSaldo = document.getElementById("bqa-saldo");
  const elConsumo = document.getElementById("bqa-consumo");
  const bloqueSaldos = elSaldo ? elSaldo.parentElement.parentElement : null;

  if (bloqueSaldos) {
    if (res.offline || resumen.operacion === "confirmar_arribo") {
      bloqueSaldos.style.display = "none";
    } else {
      bloqueSaldos.style.display = "flex";
      elSaldo.innerText = (resumen.saldo_estimado_kg !== undefined && resumen.saldo_estimado_kg !== null && !isNaN(resumen.saldo_estimado_kg))
        ? parseFloat(resumen.saldo_estimado_kg).toFixed(2) + " kg"
        : "---";
      elConsumo.innerText = (resumen.kg_consumidos !== undefined && resumen.kg_consumidos !== null && !isNaN(resumen.kg_consumidos))
        ? parseFloat(resumen.kg_consumidos).toFixed(2) + " kg"
        : "0.00 kg";
    }
  }

  const divPed = document.getElementById("bqa-bloque-pedido");
  const divAlert = document.getElementById("bqa-bloque-alerta");
  if (divPed) divPed.style.display = "none";
  if (divAlert) divAlert.style.display = "none";

  if (resumen.pedido_automatico && resumen.pedido_automatico.generado) {
    if (divPed) divPed.style.display = "block";
    const elCod = document.getElementById("bqa-codigo");
    const elProv = document.getElementById("bqa-proveedor");
    if (elCod) elCod.innerText = resumen.pedido_automatico.codigo || "---";
    if (elProv) elProv.innerText = resumen.pedido_automatico.proveedor || "";
  } else if (resumen.codigo_pedido && !res.offline && resumen.operacion === "consumo") {
    if (divPed) divPed.style.display = "block";
    const elCod = document.getElementById("bqa-codigo");
    const elProv = document.getElementById("bqa-proveedor");
    if (elCod) elCod.innerText = resumen.codigo_pedido;
    if (elProv) elProv.innerText = resumen.proveedor || "Pendiente de aprobación";
  } else if (resumen.requiere_gas) {
    if (divAlert) divAlert.style.display = "block";
    const elRazon = document.getElementById("bqa-razon");
    if (elRazon) {
      if (res.offline) {
        elRazon.innerHTML = "<b>⚠️ ATENCIÓN:</b> Tu pedido de gas está guardado en el celular. <b>MUÉVETE A UNA ZONA CON SEÑAL</b> para que el sistema lo envíe al Webmaster.";
      } else {
        elRazon.innerText = resumen.razon_alerta || "";
      }
    }
  }

  const tbody = document.getElementById("bqa-tabla-tanques");
  if (tbody) {
    tbody.innerHTML = "";
    if (resumen.tanques && Array.isArray(resumen.tanques) && resumen.tanques.length > 0) {
      resumen.tanques.forEach(t => {
        const nivelNum = parseFloat(t.nivel !== undefined ? t.nivel : (t.nivel_final || 0));
        const nivel = isNaN(nivelNum) ? "0.0" : nivelNum.toFixed(1);
        const color = nivelNum <= 30 ? "#d32f2f" : "#333";
        tbody.innerHTML += `
          <tr style="border-bottom: 1px solid #f0f0f0;">
            <td style="padding: 6px 0; color: #555;">${String(t.numero || "").toUpperCase()}</td>
            <td style="padding: 6px 0; text-align: right; font-weight: bold; color: ${color};">${nivel}%</td>
          </tr>`;
      });
    } else if (resumen.operacion === "confirmar_arribo") {
      tbody.innerHTML = `
        <tr>
          <td style="padding: 6px 0; color: #555;">Población alojada</td>
          <td style="padding: 6px 0; text-align: right; font-weight: bold; color: #015249;">${Number(resumen.pollitos || 0).toLocaleString()} aves</td>
        </tr>`;
    }
  }

  const modalRes = document.getElementById("modal-resultado-final");
  if (modalRes) modalRes.style.display = "flex";
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

function abrirNegociacion(sugerencia, diasAct) {
  const modal = document.getElementById("modalNegociacion");
  const inNivel = document.getElementById("neg_nivel");
  const spAct = document.getElementById("neg_dias_act");
  const spExtra = document.getElementById("neg_dias_extra");

  if (inNivel) inNivel.value = sugerencia;
  if (spAct) spAct.innerText = diasAct;

  currentDiasExtra = 0;
  if (spExtra) spExtra.innerText = "0";
  if (modal) modal.style.display = "flex";
}

function adjustDias(delta) {
  currentDiasExtra += delta;
  if (currentDiasExtra < 0) currentDiasExtra = 0;
  const spExtra = document.getElementById("neg_dias_extra");
  if (spExtra) spExtra.innerText = String(currentDiasExtra);
}

function cerrarNegociacion() {
  const modal = document.getElementById("modalNegociacion");
  if (modal) modal.style.display = "none";

  if (opActual === "consumo" && items && items.length > 0) {
    ejecutarEnvioAtomico(null);
    return;
  }

  if (respuestaConsumoPendiente) {
    if (respuestaConsumoPendiente.resumen) {
      respuestaConsumoPendiente.resumen.requiere_gas = false;
    }
    mostrarResumenOperacion(respuestaConsumoPendiente);
  }
}

/* ============================
Flujo común: cargar tanques y validar Candados
============================ */
async function cargarTanques(callback, bloquearSiActivo = false) {
  try {
    const res = await obtenerTanquesConFallback(sedeQR);

    if (!res) {
      alert("Error: No se encontraron tanques para esta sede o no hay conexión ni caché disponible.");
      mostrarMenu();
      return;
    }

    if (bloquearSiActivo && res.lote_activo) {
      if (res.fuente !== "servidor") {
        alert(
          "⛔ YA HAY UN LOTE ACTIVO (" + (res.info_lote || sedeQR) + ").\n\n" +
          "(Aviso: Esta información proviene de la memoria del celular porque estás trabajando sin conexión. " +
          "Si el lote ya fue cerrado, conéctate a internet e inténtalo de nuevo)."
        );
      } else {
        alert(
          "⛔ YA HAY UN LOTE ACTIVO (" + (res.info_lote || sedeQR) + ").\n\n" +
          "No puedes iniciar calefacción nuevamente en esta sede sin finalizar el lote anterior."
        );
      }
      mostrarMenu();
      return;
    }

    if (!bloquearSiActivo && res.fuente === "servidor" && !res.lote_activo) {
      alert("⛔ NO HAY LOTE ACTIVO EN ESTA SEDE.\n\nPrimero debes ejecutar 'Iniciar calefacción'.");
      mostrarMenu();
      return;
    }

    // CANDADO 1: Arribo de pollitos pendiente (solo para operaciones posteriores al inicio)
    if (!bloquearSiActivo && res.bloqueado_por_arribo) {
      alert(`⛔ OPERACIÓN BLOQUEADA\n\n${res.motivo_bloqueo}`);
      mostrarMenu();
      return;
    }

    // CANDADO 2: Pedido de gas en tránsito sin tanquear
    if (!bloquearSiActivo && res.bloqueado_por_pedido) {
      alert(`${res.motivo_bloqueo_pedido}`);
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
      alert("Error: No se encontraron tanques configurados para esta sede. Verifica el QR o la conexión.");
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

  limpiarEstadoOperacion();
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
  input.step = "0.1";
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
    const rawVal = String(input.value || "").trim();
    if (rawVal === "") {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    const n = Number(rawVal);
    if (isNaN(n) || n < 0 || n > 100) {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    // --- PEAJE DE SEGURIDAD FÍSICO ---
    if (tk.ultimo_nivel !== null && tk.ultimo_nivel !== undefined) {
      if (n > tk.ultimo_nivel && opActual !== "inicio_calefaccion") {
        alert(`⚠️ ERROR DE LECTURA\n\nEl nivel ingresado (${n}%) es MAYOR al último reportado (${tk.ultimo_nivel}%).\n\nEn esta operación el nivel solo puede bajar. Verifica el manómetro.`);
        return;
      }
    }
    nivelActualValue = n;

    hide(input);
    hide(btn);
    hide(msgDiv);
    texto.textContent = `Adjunta la foto de testigo del ${tk.numero}`;

    foto.value = "";
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
      show(divSiNo, "flex");

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
            fotoBase64: base64,
            testigo: base64
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

function limpiarEstadoOperacion() {
  desactivarProteccionOperacion();
  items = [];
  idx = 0;
  tanques = [];
  tanquesDisponibles = [];
  tanqueActualDatos = null;
  tanqueActualIndex = 0;

  registrarTanqueoData = { sede: "", tanques: [], op_id: generarOpId() };

  const input = document.getElementById("respuestaInput");
  const foto = document.getElementById("inputFoto");
  const btn = document.getElementById("botonEnviar");
  const siNo = document.getElementById("preguntaSiNo");
  const msgDiv = document.getElementById("msgUltimoNivel");

  if (input) {
    input.value = "";
    input.onkeyup = null;
    input.dataset.pollitos = "";
    input.dataset.fecha_llegada = "";
    input.style.display = "none";
  }
  if (foto) {
    foto.value = "";
    foto.onchange = null;
    foto.style.display = "none";
  }
  if (btn) {
    btn.onclick = null;
    btn.disabled = false;
    btn.style.backgroundColor = "";
    btn.style.display = "none";
  }
  if (siNo) {
    siNo.style.display = "none";
  }
  if (msgDiv) {
    msgDiv.innerHTML = "";
    msgDiv.style.display = "none";
  }

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

  configurarModoFoto(false);

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

    if (divSiNo) divSiNo.style.display = "none";
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

    if (ES_EMPRESA_AVICOLA) {
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
          input.step = "1";
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
            show(divSiNo, "flex");

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
  if (!window.confirm(`¿Está seguro de iniciar la calefacción en ${sedeQR}?`)) {
    return;
  }

  const inputRef = document.getElementById("respuestaInput");
  const pollitos = inputRef ? (Number(inputRef.dataset.pollitos || 0) || null) : null;
  const fechaLlegada = inputRef ? (inputRef.dataset.fecha_llegada || null) : null;
  const sedeDestino = sedeQR;
  const copiaItems = items.map(it => ({ ...it }));

  const payload = {
    op_id: generarOpId(),
    sede: sedeDestino,
    pollitos: ES_EMPRESA_AVICOLA ? pollitos : null,
    fecha_llegada_pollitos: ES_EMPRESA_AVICOLA ? fechaLlegada : null,
    tanques: copiaItems
  };

  sendWithOffline(
    "/glp/registrar_inicio",
    payload,
    (res) => {
      localStorage.setItem(`estado_lote_${sedeDestino}`, JSON.stringify({ esperando_pollito: true }));
      if (res && res.offline) {
        const hayTanqueBajo = copiaItems.some(t => Number(t.nivel || 0) < 70);
        actualizarCacheUltimoNivel(sedeDestino, copiaItems, "inicio_calefaccion", {
          hay_pedido_pendiente: hayTanqueBajo,
          bloqueado_por_pedido: hayTanqueBajo
        });
      } else {
        localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeDestino);
        sincronizarNivelesServidor(sedeDestino);
      }
      mostrarResumenOperacion(res);
    }
  );
}

// ============================================
// FLUJO DE REGISTRAR TANQUEO
// ============================================
async function cargarTanquesTanqueo(callback) {
  try {
    const res = await obtenerTanquesConFallback(sedeQR);

    if (!res) {
      alert("No se encontraron datos para esta sede o no hay conexión ni caché disponible.");
      mostrarMenu();
      return;
    }

    if (res.fuente === "servidor" && !res.lote_activo) {
      alert("⛔ NO HAY LOTE ACTIVO EN ESTA SEDE.\n\nPrimero debes ejecutar 'Iniciar calefacción'.");
      mostrarMenu();
      return;
    }

    // CANDADO 1: Arribo de pollitos pendiente (SÍ bloquea Tanqueo; el Candado 2 de pedido NO bloquea Tanqueo)
    if (res.bloqueado_por_arribo) {
      alert(`⛔ OPERACIÓN BLOQUEADA\n\n${res.motivo_bloqueo}`);
      mostrarMenu();
      return;
    }

    let baseTanques = (Array.isArray(res.tanques) && res.tanques.length > 0)
      ? res.tanques
      : tanquesQR;

    let tanquesDisponiblesTmp = (Array.isArray(baseTanques) ? baseTanques : []).filter(
      t => t !== null && t !== undefined && t.numero
    );

    if (tanquesDisponiblesTmp.length === 0) {
      alert("No se encontraron tanques configurados para esta sede.");
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

  configurarModoFoto(true);

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
    // Si el operador respondió "No" a todos los tanques, cancelar limpiamente y volver al menú sin colgar la UI
    if (!registrarTanqueoData.tanques || registrarTanqueoData.tanques.length === 0) {
      alert("No seleccionaste ningún tanque para tanquear. Regresando al menú principal.");
      mostrarMenu();
      return;
    }
    enviarTanqueo();
    return;
  }

  const tk = tanquesDisponibles[tanqueActualIndex];
  texto.textContent = `¿Desea tanquear el tanque ${tk.numero.toUpperCase()}?`;

  hide(input);
  hide(foto);
  hide(btn);

  if (panelSiNo) panelSiNo.style.display = "flex";
  if (btnSi) btnSi.textContent = "Sí";
  if (btnNo) btnNo.textContent = "No";

  btnSi.onclick = () => {
    if (panelSiNo) panelSiNo.style.display = "none";
    prepararDatosTanqueActual(tk);
    pedirNivelInicial();
  };

  btnNo.onclick = () => {
    if (panelSiNo) panelSiNo.style.display = "none";
    tanqueActualIndex++;
    preguntarSiTanquearTanqueActual();
  };
}

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
  input.step = "0.1";
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
    const rawVal = String(input.value || "").trim();
    if (rawVal === "") {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    const n = Number(rawVal);
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
    show(divSiNo, "flex");

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
  const divSiNo = document.getElementById("preguntaSiNo");
  if (divSiNo) divSiNo.style.display = "none";

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
      const base64 = await comprimirImagenArchivo(f, 800, 0.4);
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
  const divSiNo = document.getElementById("preguntaSiNo");
  if (divSiNo) divSiNo.style.display = "none";

  hide(input);
  hide(foto);
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
  input.step = "0.1";
  input.value = "";
  texto.textContent = `Ingresa el NIVEL FINAL del tanque ${tanqueActualDatos.numero.toUpperCase()} (0-100 %)`;
  btn.textContent = "Confirmar nivel final";
  show(btn);

  btn.onclick = () => {
    const rawVal = String(input.value || "").trim();
    if (rawVal === "") {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    const n = Number(rawVal);
    if (isNaN(n) || n < 0 || n > 100) {
      alert("Ingresa un nivel válido entre 0 y 100.");
      return;
    }

    if (n <= tanqueActualDatos.nivel_inicial) {
      alert(`⚠️ ERROR LÓGICO DE TANQUEO\n\nEl nivel final (${n}%) debe ser MAYOR al nivel inicial (${tanqueActualDatos.nivel_inicial}%).`);
      return;
    }

    hide(input);
    hide(btn);
    texto.textContent = `¿Estás seguro que el Nivel Final es ${n}%?`;

    const bSi = document.getElementById("btnSi");
    const bNo = document.getElementById("btnNo");
    show(divSiNo, "flex");

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
  const divSiNo = document.getElementById("preguntaSiNo");
  if (divSiNo) divSiNo.style.display = "none";

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
      const base64 = await comprimirImagenArchivo(f, 800, 0.4);
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
    show(divSiNo, "flex");

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
    show(divSiNo, "flex");

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
  const divSiNo = document.getElementById("preguntaSiNo");
  if (divSiNo) divSiNo.style.display = "none";

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
      const base64 = await comprimirImagenArchivo(f, 800, 0.4);
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

function enviarTanqueo() {
  const tanquesRecolectados = registrarTanqueoData.tanques || [];
  if (tanquesRecolectados.length === 0) {
    alert("No seleccionaste tanques para registrar.");
    mostrarMenu();
    return;
  }

  let valido = true;
  let faltanFotos = false;

  for (let i = 0; i < tanquesRecolectados.length; i++) {
    const t = tanquesRecolectados[i];
    if (t.nivel_final === undefined || t.nivel_final === null || t.nivel_final === "") valido = false;
    if (!t.foto_nivel_final) faltanFotos = true;
  }

  if (!valido) {
    alert("Debes ingresar nivel final para todos los tanques.");
    return;
  }
  if (faltanFotos) {
    alert("Debes tomar la FOTO DEL NIVEL FINAL para cada tanque.");
    return;
  }

  if (!window.confirm(`¿Está seguro de registrar este tanqueo de gas en ${sedeQR}?`)) {
    return;
  }

  const sedeDestino = sedeQR;
  const copiaTanques = tanquesRecolectados.map(it => ({ ...it }));
  const opId = registrarTanqueoData.op_id || generarOpId();

  const payload = {
    operacion: "tanqueo",
    sede: sedeDestino,
    tanques: copiaTanques,
    op_id: opId,
    timestamp: new Date().toISOString()
  };

  sendWithOffline(
    "/glp/registrar_tanqueo",
    payload,
    (res) => {
      pedidoPendienteGlobal = null;
      if (res && res.offline) {
        actualizarCacheUltimoNivel(sedeDestino, copiaTanques, "tanqueo");
      } else {
        localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeDestino);
        sincronizarNivelesServidor(sedeDestino);
      }
      mostrarResumenOperacion(res);
    }
  );
}

/* ============================
   INICIAR CONSUMO (PUERTA DE ENTRADA)
============================ */
async function iniciarConsumo() {
  activarProteccionOperacion();
  opActual = "consumo";

  configurarModoFoto(false);

  try {
    const info = await leerQR("Escanea el QR de la sede para registrar el consumo");
    sedeQR = info.sede;
    empresaQR = info.empresa;
    tanquesQR = Array.isArray(info.tanques) ? info.tanques : [];

    const input = document.getElementById("respuestaInput");
    const foto = document.getElementById("inputFoto");
    const btn = document.getElementById("botonEnviar");
    const divSiNo = document.getElementById("preguntaSiNo");

    if (divSiNo) divSiNo.style.display = "none";
    if (input) {
      input.onkeyup = null;
      input.dataset.pollitos = "";
    }
    if (foto) foto.onchange = null;
    if (btn) btn.onclick = null;

    items = [];
    idx = 0;

    cargarTanques(() => {
      preguntarNivelFoto(enviarConsumo);
    });
  } catch (e) {
    alert("No fue posible leer el QR: " + e);
    mostrarMenu();
  }
}

/* ============================
   FINALIZAR CALEFACCIÓN
============================ */
async function finalizarCalefaccion() {
  activarProteccionOperacion();
  opActual = "finalizar_calefaccion";

  configurarModoFoto(false);

  try {
    const info = await leerQR("Escanea el QR de la sede para finalizar la calefacción");

    sedeQR = info.sede;
    empresaQR = info.empresa;
    tanquesQR = Array.isArray(info.tanques) ? info.tanques : [];

    const input = document.getElementById("respuestaInput");
    const foto = document.getElementById("inputFoto");
    const btn = document.getElementById("botonEnviar");
    const divSiNo = document.getElementById("preguntaSiNo");
    if (divSiNo) divSiNo.style.display = "none";

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
    items = [];
    idx = 0;

    cargarTanques(() => {
      preguntarNivelFoto(enviarFinalizar);
    });
  } catch (e) {
    alert("No fue posible leer el QR: " + e);
    mostrarMenu();
  }
}

function enviarFinalizar() {
  if (!window.confirm(`¿Está seguro de FINALIZAR la calefacción y cerrar el lote en ${sedeQR}?`)) {
    return;
  }

  const sedeDestino = sedeQR;
  const copiaItems = items.map(it => ({ ...it }));

  const payload = {
    op_id: generarOpId(),
    sede: sedeDestino,
    tanques: copiaItems
  };

  sendWithOffline(
    "/glp/finalizar_calefaccion_batch",
    payload,
    (res) => {
      localStorage.removeItem(`estado_lote_${sedeDestino}`);
      if (res && res.offline) {
        actualizarCacheUltimoNivel(sedeDestino, copiaItems, "finalizar_calefaccion");
      } else {
        localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeDestino);
        sincronizarNivelesServidor(sedeDestino);
      }
      mostrarResumenOperacion(res);
    }
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
    if (a.dataset.boundLogout === "1") return;
    a.dataset.boundLogout = "1";

    a.addEventListener("click", async function (e) {
      e.preventDefault();

      if (GLP_OPERACION_EN_CURSO) {
        alert("⛔ Debes terminar la operación actual antes de cerrar sesión.");
        return;
      }

      if (!navigator.onLine || isForceOffline()) {
        alert("📡 Estás sin conexión. El cierre de sesión requiere internet.");
        return;
      }

      try {
        const q = await getQueue();
        if (q && q.length > 0) {
          alert("⛔ ACCESO DENEGADO\n\nTienes operaciones guardadas en el celular que aún no se han subido al servidor.\n\nPor favor, espera a que se sincronicen o presiona 'Sincronizar Pendientes' antes de salir.");
          return;
        }
      } catch (err) {
        console.error("Error leyendo cola offline:", err);
      }

      window.location.href = "/logout";
    });
  });
}

async function sincronizarNivelesServidor(sede) {
  if (!sede || !navigator.onLine || isForceOffline()) return;

  try {
    const response = await fetch("/glp/obtener_tanques", {
      method: "POST",
      credentials: "include",
      redirect: "manual",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-CSRFToken": getCsrfToken()
      },
      body: JSON.stringify({ sede: sede })
    });

    if (!response.ok) return;
    const ct = (response.headers.get("content-type") || "").toLowerCase();
    if (!ct.includes("application/json")) return;

    const data = await response.json();
    if (data.success) {
      guardarEstadoCompletoSedeCache(sede, data);
    }
  } catch (e) {
    console.warn("⚠️ [Sync-Fondo] Error al sincronizar niveles en segundo plano:", e);
  }
}

/* ============================
   CONFIRMAR ARRIBO DE POLLITOS (ONLINE FIRST + FALLBACK OFFLINE)
============================ */
async function iniciarConfirmacionArribo() {
  activarProteccionOperacion();
  opActual = "confirmar_arribo";

  configurarModoFoto(false);

  try {
    await new Promise(r => setTimeout(r, 300));

    const info = await leerQR("Escanea el QR de la sede para confirmar el arribo");
    sedeQR = info.sede;
    empresaQR = info.empresa;
    tanquesQR = Array.isArray(info.tanques) ? info.tanques : [];

    const texto = document.getElementById("preguntaTexto");
    if (texto) texto.textContent = "Verificando estado de la granja...";

    const res = await obtenerTanquesConFallback(sedeQR);
    if (texto) texto.textContent = "";

    let esperandoLocal = false;
    try {
      const infoLoteLocal = JSON.parse(localStorage.getItem(`estado_lote_${sedeQR}`) || "null");
      esperandoLocal = Boolean(infoLoteLocal && infoLoteLocal.esperando_pollito);
    } catch (_) {}

    // Si estamos online y el servidor dice que NO hay lote activo, impedir confirmar sobre vacío
    if (res && res.fuente === "servidor" && !res.lote_activo) {
      alert(`⛔ La granja (${sedeQR}) no tiene un lote activo actualmente.`);
      mostrarMenu();
      return;
    }

    if ((res && res.bloqueado_por_arribo) || (res && res.fuente !== "servidor" && esperandoLocal)) {
      abrirModalArribo();
    } else {
      alert("✅ Esta granja (" + sedeQR + ") no tiene un arribo pendiente o ya fue confirmado.");
      mostrarMenu();
    }
  } catch (e) {
    alert("Operación cancelada o no fue posible validar el QR.");
    mostrarMenu();
  }
}

function abrirModalArribo() {
  const lector = document.getElementById("lectorQR");
  const convo = document.getElementById("conversacion");
  const texto = document.getElementById("preguntaTexto");
  const inPob = document.getElementById("poblacion_real");
  const inFec = document.getElementById("fecha_arribo_real");

  if (lector) lector.style.display = "none";
  if (convo) convo.style.display = "none";
  if (texto) texto.textContent = "";
  if (inPob) inPob.value = "";
  if (inFec && !inFec.value) {
    const ahora = new Date();
    const offsetMs = ahora.getTimezoneOffset() * 60000;
    inFec.value = new Date(ahora.getTime() - offsetMs).toISOString().slice(0, 16);
  }

  const modal = document.getElementById("modalArribo");
  if (modal) modal.style.display = "flex";
}

function cerrarModalArribo() {
  const modal = document.getElementById("modalArribo");
  if (modal) modal.style.display = "none";
  mostrarMenu();
}

async function ejecutarConfirmarArribo() {
  const inPob = document.getElementById("poblacion_real");
  const inFec = document.getElementById("fecha_arribo_real");
  const poblacion = Number(inPob ? inPob.value : 0);
  const fecha = inFec ? String(inFec.value || "").trim() : "";

  if (!poblacion || isNaN(poblacion) || poblacion <= 0 || !fecha) {
    alert("⚠️ Por favor ingresa una población válida (> 0) y la fecha/hora de arribo.");
    return;
  }

  if (!window.confirm(`¿Está seguro de confirmar el arribo de ${poblacion} aves en ${sedeQR}? Esta acción activa el reloj del lote.`)) {
    return;
  }

  const modal = document.getElementById("modalArribo");
  if (modal) modal.style.display = "none";

  const sedeDestino = sedeQR;
  const payload = {
    op_id: generarOpId(),
    operacion: "confirmar_arribo",
    sede: sedeDestino,
    poblacion: poblacion,
    fecha_arribo: fecha,
    timestamp: new Date().toISOString()
  };

  await sendWithOffline(
    "/glp/confirmar_arribo_pollito",
    payload,
    (res) => {
      localStorage.setItem(`estado_lote_${sedeDestino}`, JSON.stringify({ esperando_pollito: false }));
      if (res && res.offline) {
        actualizarCacheUltimoNivel(sedeDestino, [], "confirmar_arribo", {
          bloqueado_por_arribo: false,
          motivo_bloqueo: ""
        });
      } else {
        localStorage.removeItem(TANQUES_CACHE_PREFIX + sedeDestino);
        sincronizarNivelesServidor(sedeDestino);
      }
      mostrarResumenOperacion(res);
    }
  );
}

// ============================================
// ANÁLISIS DE LOTE EN TIEMPO REAL
// ============================================
let chartAnalisis = null;

async function iniciarAnalisisLote() {
  if (!navigator.onLine || isForceOffline()) {
    alert("📡 MODO OFFLINE DETECTADO\n\nPara generar la gráfica y el análisis histórico de la granja, necesitas tener conexión a internet.");
    return;
  }

  try {
    const info = await leerQR("Escanea el QR para generar el análisis");
    sedeQR = info.sede;

    const texto = document.getElementById("preguntaTexto");
    if (texto) texto.textContent = "Analizando historial en el servidor...";

    const response = await fetch("/glp/consultar_analisis_lote", {
      method: "POST",
      credentials: "include",
      redirect: "manual",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-CSRFToken": getCsrfToken()
      },
      body: JSON.stringify({ sede: sedeQR })
    });

    if (response.type === "opaqueredirect" || response.status === 401 || response.status === 403) {
      if (texto) texto.textContent = "";
      alert("⚠️ Tu sesión expiró. Inicia sesión nuevamente.");
      mostrarMenu();
      return;
    }

    const data = await response.json();
    if (texto) texto.textContent = "";

    if (!data.success) {
      alert("⚠️ " + data.message);
      mostrarMenu();
      return;
    }

    renderizarPanelAnalisis(data);

  } catch (e) {
    console.error(e);
    alert("Operación cancelada o error al consultar el análisis.");
    mostrarMenu();
  }
}

function renderizarPanelAnalisis(data) {
  const menu = document.getElementById("menuOpciones");
  const convo = document.getElementById("conversacion");
  if (menu) menu.style.display = "none";
  if (convo) convo.style.display = "none";

  document.querySelectorAll(".btn-salir").forEach(el => {
    el.style.visibility = "hidden";
  });

  document.getElementById("analisis_sede_nombre").innerText = "📍 " + data.kpis.sede;
  document.getElementById("analisis_lote_nombre").innerText = "Lote: " + data.kpis.lote;

  document.getElementById("kpi_dias").innerText = data.kpis.dias_operacion;
  document.getElementById("kpi_poblacion").innerText = (data.kpis.poblacion || 0).toLocaleString();
  document.getElementById("kpi_consumo").innerText = data.kpis.total_consumo_kg + " kg";
  document.getElementById("kpi_eficiencia").innerText = data.kpis.eficiencia_actual;

  const formatMoney = (val) => new Intl.NumberFormat("es-CO", {
    style: "currency",
    currency: "COP",
    maximumFractionDigits: 0
  }).format(val || 0);

  document.getElementById("kpi_facturado").innerText = formatMoney(data.kpis.total_facturado);
  document.getElementById("kpi_costo_ave").innerText = formatMoney(data.kpis.costo_por_ave);

  const panel = document.getElementById("panelAnalisis");
  if (panel) panel.style.display = "block";

  if (chartAnalisis) {
    chartAnalisis.destroy();
  }

  const canvas = document.getElementById("graficaAnalisis");
  if (canvas) {
    const ctx = canvas.getContext("2d");
    chartAnalisis = new Chart(ctx, {
      type: "line",
      data: {
        labels: data.grafica.labels,
        datasets: [
          {
            label: "Kg/Ave Acumulado",
            data: data.grafica.eficiencia,
            borderColor: "#f57c00",
            backgroundColor: "rgba(245, 124, 0, 0.1)",
            borderWidth: 3,
            pointRadius: 4,
            pointBackgroundColor: "#d84315",
            fill: true,
            tension: 0.3
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            ticks: { font: { size: 10 } }
          },
          y: {
            beginAtZero: true,
            title: { display: true, text: "Kg / Ave", font: { size: 11, weight: "bold" } }
          }
        },
        plugins: {
          legend: {
            position: "top",
            labels: { boxWidth: 15, font: { size: 11, weight: "bold" } }
          }
        }
      }
    });
  }
}

function cerrarAnalisis() {
  const panel = document.getElementById("panelAnalisis");
  if (panel) panel.style.display = "none";

  document.querySelectorAll(".btn-salir").forEach(el => {
    el.style.visibility = "visible";
  });

  if (chartAnalisis) {
    chartAnalisis.destroy();
    chartAnalisis = null;
  }
  mostrarMenu();
}

window.addEventListener("load", actualizarBloqueoLogout);
window.addEventListener("online", actualizarBloqueoLogout);
window.addEventListener("offline", actualizarBloqueoLogout);