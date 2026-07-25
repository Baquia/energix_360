// app/static/js/visitas_offline.js

// ===================================================
// VARIABLES GLOBALES Y CONFIGURACIÓN
// ===================================================
const CACHE_PREFIX = "cg_cache_" + (window.EMPRESA_SESION || "default") + "_";
const DB_NAME = "cg_visitas_offline_db";
const STORE = "queue";
let IS_SYNCING = false;
let visitaActual = {}; // Almacena temporalmente los datos del formulario

// ===================================================
// BASE DE DATOS LOCAL (IndexedDB)
// ===================================================
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

async function removeFromQueue(id) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(STORE, "readwrite");
        tx.objectStore(STORE).delete(id);
        tx.oncomplete = () => resolve(true);
        tx.onerror = () => reject(tx.error);
    });
}

// ===================================================
// AUTO-SYNC Y MONITOREO DE RED
// ===================================================
window.addEventListener("online", () => {
    document.getElementById("badgeOperatividad").innerHTML = '<span style="font-size: 14px;">📡</span>ESTADO: ONLINE';
    document.getElementById("badgeOperatividad").style.color = "#10b981";
    document.getElementById("badgeOperatividad").style.backgroundColor = "#d1fae5";
    document.getElementById("badgeOperatividad").style.borderColor = "#a7f3d0";
    setTimeout(flushOfflineQueue, 2000);
});

window.addEventListener("offline", () => {
    document.getElementById("badgeOperatividad").innerHTML = '<span style="font-size: 14px;">📡</span>ESTADO: OFFLINE';
    document.getElementById("badgeOperatividad").style.color = "#e65100";
    document.getElementById("badgeOperatividad").style.backgroundColor = "#fff3e0";
    document.getElementById("badgeOperatividad").style.borderColor = "#ffe0b2";
});

window.addEventListener("load", async () => {
    if (navigator.onLine) {
        document.getElementById("badgeOperatividad").innerHTML = '<span style="font-size: 14px;">📡</span>ESTADO: ONLINE';
        document.getElementById("badgeOperatividad").style.color = "#10b981";
        document.getElementById("badgeOperatividad").style.backgroundColor = "#d1fae5";
    } else {
        document.getElementById("badgeOperatividad").innerHTML = '<span style="font-size: 14px;">📡</span>ESTADO: OFFLINE';
    }
    await checkPendientes();
    setTimeout(flushOfflineQueue, 1500);
});

async function checkPendientes() {
    const queue = await idbGetAll();
    const warning = document.getElementById("syncWarning");
    if (queue.length > 0) {
        warning.style.display = "block";
        warning.innerHTML = `⚠️ Tienes ${queue.length} visitas pendientes por subir al servidor.`;
        warning.style.padding = "10px";
        warning.style.backgroundColor = "#fff3e0";
        warning.style.color = "#e65100";
        warning.style.borderRadius = "8px";
        warning.style.marginBottom = "15px";
    } else {
        warning.style.display = "none";
    }
}

async function flushOfflineQueue() {
    if (!navigator.onLine || IS_SYNCING) return;
    const queue = await idbGetAll();
    if (queue.length === 0) return;

    IS_SYNCING = true;
    for (const item of queue) {
        try {
            const response = await fetch("/mecanico_glp/sincronizar_visita", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(item.payload)
            });
            const resData = await response.json();
            if (resData.success) {
                await removeFromQueue(item.id);
            }
        } catch (err) {
            console.error("Error sincronizando visita:", err);
            break;
        }
    }
    IS_SYNCING = false;
    await checkPendientes();
}

// ===================================================
// FLUJO DESCARGA DE AGENDA (PULL)
// ===================================================
async function descargarAgenda() {
    if (!navigator.onLine) {
        alert("Necesitas conexión a internet para descargar la agenda.");
        return;
    }
    
    const btnTextoOriginal = "📥 Descargar Agenda (Online)";
    const botones = document.querySelectorAll(".menu-btn");
    botones[0].innerText = "⏳ Descargando...";
    botones[0].disabled = true;

    try {
        const response = await fetch("/mecanico_glp/sync_down", { method: "POST" });
        const data = await response.json();
        
        if (data.success) {
            localStorage.setItem(CACHE_PREFIX + "equipos", JSON.stringify(data.equipos));
            localStorage.setItem(CACHE_PREFIX + "mantenimientos", JSON.stringify(data.mantenimientos));
            alert("✅ Agenda y equipos descargados correctamente al dispositivo.");
        } else {
            alert("❌ Error: " + data.message);
        }
    } catch (e) {
        alert("❌ Error de red al intentar descargar la agenda.");
    } finally {
        botones[0].innerText = btnTextoOriginal;
        botones[0].disabled = false;
    }
}

function verAgendaLocal() {
    const mantenimientosStr = localStorage.getItem(CACHE_PREFIX + "mantenimientos");
    if (!mantenimientosStr) {
        alert("No hay agenda descargada. Conéctate a internet y presiona 'Descargar Agenda'.");
        return;
    }
    
    const mantenimientos = JSON.parse(mantenimientosStr);
    const lista = document.getElementById("listaAgenda");
    lista.innerHTML = "";
    
    if (mantenimientos.length === 0) {
        lista.innerHTML = "<p style='color:#666; font-size:14px;'>No tienes mantenimientos pendientes asignados.</p>";
    } else {
        mantenimientos.forEach(m => {
            lista.innerHTML += `
            <div class="tarea-card">
                <h4>${m.serial_codigo} - ${m.tipo_mantenimiento.replace('_', ' ').toUpperCase()}</h4>
                <p><strong>Fecha Programada:</strong> ${m.fecha_programada}</p>
                <p><strong>Estado:</strong> ${m.estado.toUpperCase()}</p>
            </div>`;
        });
    }
    
    document.getElementById("menuOpciones").style.display = "none";
    document.getElementById("panelAgenda").style.display = "block";
}

// ===================================================
// FLUJO REGISTRO DE VISITA (OFFLINE)
// ===================================================
function iniciarVisita() {
    const equiposStr = localStorage.getItem(CACHE_PREFIX + "equipos");
    if (!equiposStr) {
        alert("No hay equipos en memoria. Debes descargar la agenda primero (requiere internet).");
        return;
    }
    
    const equipos = JSON.parse(equiposStr);
    if(equipos.length === 0) {
        alert("Tu empresa no tiene equipos activos registrados.");
        return;
    }

    // Inicializar objeto temporal
    visitaActual = {
        op_id: (window.crypto && crypto.randomUUID) ? crypto.randomUUID() : "op_" + Date.now(),
        equipo_id: null,
        mantenimiento_programado_id: null,
        observaciones: "",
        evidencias: []
    };

    // Llenar Select de Equipos
    const selEquipo = document.getElementById("selectEquipo");
    selEquipo.innerHTML = '<option value="">Seleccione el equipo a intervenir...</option>';
    equipos.forEach(eq => {
        selEquipo.innerHTML += `<option value="${eq.id}">${eq.serial_codigo} (${eq.tipo.replace('_',' ')})</option>`;
    });

    document.getElementById("menuOpciones").style.display = "none";
    document.getElementById("conversacion").style.display = "block";
    document.getElementById("preguntaTexto").innerText = "Paso 1: Selecciona el equipo";
    
    selEquipo.style.display = "block";
    
    const btn = document.getElementById("botonEnviar");
    btn.style.display = "block";
    btn.innerText = "Continuar";
    
    btn.onclick = () => {
        if(!selEquipo.value) { alert("Debes seleccionar un equipo."); return; }
        visitaActual.equipo_id = parseInt(selEquipo.value);
        selEquipo.style.display = "none";
        pedirMantenimientoVinculado(visitaActual.equipo_id);
    };
}

function pedirMantenimientoVinculado(equipo_id) {
    const mantStr = localStorage.getItem(CACHE_PREFIX + "mantenimientos");
    let opciones = [];
    if(mantStr) {
        const mantenimientos = JSON.parse(mantStr);
        opciones = mantenimientos.filter(m => m.equipo_id === equipo_id);
    }
    
    const selMant = document.getElementById("selectMantenimiento");
    selMant.innerHTML = '<option value="">Visita Correctiva (Sin programación previa)</option>';
    opciones.forEach(m => {
        selMant.innerHTML += `<option value="${m.id}">${m.tipo_mantenimiento.replace('_',' ')} - Prog: ${m.fecha_programada}</option>`;
    });

    document.getElementById("preguntaTexto").innerText = "Paso 2: ¿La visita corresponde a un mantenimiento programado?";
    selMant.style.display = "block";
    
    const btn = document.getElementById("botonEnviar");
    btn.onclick = () => {
        visitaActual.mantenimiento_programado_id = selMant.value ? parseInt(selMant.value) : null;
        selMant.style.display = "none";
        pedirObservaciones();
    };
}

function pedirObservaciones() {
    const obs = document.getElementById("inputObservaciones");
    document.getElementById("preguntaTexto").innerText = "Paso 3: Escribe las observaciones técnicas";
    obs.style.display = "block";
    obs.value = "";
    
    const btn = document.getElementById("botonEnviar");
    btn.onclick = () => {
        if(obs.value.trim().length < 5) { alert("Describe brevemente el trabajo realizado."); return; }
        visitaActual.observaciones = obs.value.trim();
        obs.style.display = "none";
        pedirEvidencia();
    };
}

function pedirEvidencia() {
    const foto = document.getElementById("inputFoto");
    document.getElementById("preguntaTexto").innerText = "Paso 4: Adjunta una fotografía de evidencia (Opcional)";
    foto.style.display = "block";
    foto.value = null;
    
    const btn = document.getElementById("botonEnviar");
    btn.innerText = "Finalizar y Guardar";
    
    btn.onclick = async () => {
        btn.innerText = "Procesando...";
        btn.disabled = true;
        
        try {
            if(foto.files.length > 0) {
                const b64 = await comprimirImagen(foto.files[0]);
                visitaActual.evidencias.push(b64);
            }
            await empaquetarYGuardarVisita();
        } catch(e) {
            alert("Error procesando imagen.");
            btn.innerText = "Finalizar y Guardar";
            btn.disabled = false;
        }
    };
}

async function empaquetarYGuardarVisita() {
    visitaActual.fecha_visita = new Date().toISOString().slice(0, 19).replace('T', ' ');
    
    // Guardar en cola IndexedDB
    await idbAdd({
        id: Date.now(),
        payload: visitaActual,
        ts: Date.now()
    });

    // Remover mantenimiento de la vista local (para que no vuelva a aparecer como pendiente)
    if(visitaActual.mantenimiento_programado_id) {
        const mantStr = localStorage.getItem(CACHE_PREFIX + "mantenimientos");
        if(mantStr) {
            let mants = JSON.parse(mantStr);
            mants = mants.filter(m => m.id !== visitaActual.mantenimiento_programado_id);
            localStorage.setItem(CACHE_PREFIX + "mantenimientos", JSON.stringify(mants));
        }
    }

    // Mostrar Resumen Final
    document.getElementById("conversacion").style.display = "none";
    document.getElementById("inputFoto").style.display = "none";
    document.getElementById("botonEnviar").style.display = "none";
    document.getElementById("botonEnviar").disabled = false;
    
    // Encontrar nombre equipo para UI
    const eqs = JSON.parse(localStorage.getItem(CACHE_PREFIX + "equipos") || "[]");
    const eqObj = eqs.find(e => e.id === visitaActual.equipo_id);
    
    document.getElementById("resumenEquipo").innerText = eqObj ? eqObj.serial_codigo : visitaActual.equipo_id;
    document.getElementById("resumenMantenimiento").innerText = visitaActual.mantenimiento_programado_id ? "Programado" : "Correctivo";
    document.getElementById("resumenEvidencias").innerText = visitaActual.evidencias.length;
    
    const headerDiv = document.querySelector("#modal-resultado-final > div > div:first-child");
    const tituloDiv = headerDiv.querySelector("h2");
    
    if (navigator.onLine) {
        headerDiv.style.background = "#015249";
        tituloDiv.innerText = "Sincronizando...";
        document.getElementById("bqa-operacion").innerText = "Enviando al servidor";
        setTimeout(flushOfflineQueue, 500);
    } else {
        headerDiv.style.background = "#f57c00";
        tituloDiv.innerText = "Guardado Localmente";
        document.getElementById("bqa-operacion").innerText = "Pendiente de red";
    }
    
    document.getElementById("modal-resultado-final").style.display = "flex";
}

// ===================================================
// UTILIDADES GENERALES
// ===================================================
function mostrarMenu() {
    document.getElementById("panelAgenda").style.display = "none";
    document.getElementById("conversacion").style.display = "none";
    document.getElementById("menuOpciones").style.display = "flex";
    document.getElementById("preguntaTexto").innerText = "";
}

function finalizarVisitaYVolver() {
    document.getElementById("modal-resultado-final").style.display = "none";
    mostrarMenu();
    checkPendientes();
}

function solicitarConfirmacion(mensaje, funcionCallback) {
    document.getElementById("modalMensaje").innerText = mensaje;
    document.getElementById("modalConfirmacion").style.display = "flex";
    
    const btnConf = document.getElementById("btnModalConfirmar");
    btnConf.onclick = () => {
        document.getElementById("modalConfirmacion").style.display = "none";
        funcionCallback();
    };
}

function cerrarModalConfirmacion() {
    document.getElementById("modalConfirmacion").style.display = "none";
}

async function comprimirImagen(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
            const img = new Image();
            img.onload = () => {
                const canvas = document.createElement("canvas");
                let width = img.width, height = img.height;
                const maxDim = 800;
                
                if (width > height && width > maxDim) {
                    height = Math.round(height * (maxDim / width));
                    width = maxDim;
                } else if (height > maxDim) {
                    width = Math.round(width * (maxDim / height));
                    height = maxDim;
                }
                
                canvas.width = width;
                canvas.height = height;
                canvas.getContext("2d").drawImage(img, 0, 0, width, height);
                resolve(canvas.toDataURL("image/jpeg", 0.6));
            };
            img.onerror = reject;
            img.src = reader.result;
        };
        reader.onerror = reject;
        reader.readAsDataURL(file);
    });
}