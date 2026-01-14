from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify, current_app, make_response
from app.utils import login_required_custom
from app import mysql, csrf
import re
import traceback
from datetime import datetime

bp_890707006 = Blueprint('bp_890707006', __name__)

# ==============================================================================
# CONFIGURACIÓN MAESTRA DE ACCESOS
# ==============================================================================
ACCESO_MODULOS = {
    # --- MÓDULO GAS ---
    "gas": {
        "operador_gas":        "glp.html",
        "controlfacturas_gas": "facturas_glp.html",
        "controlador_gas":     "control_glp.html",
        "webmaster":           "control_glp.html",
        "admin":               "control_glp.html"
    },
    # --- MÓDULO MERMAS ---
    "mermas": {
        "operador_mermas":    "mermas.html",
        "controlador_mermas": "controlmermas.html",
        "webmaster":          "controlmermas.html",
        "admin":              "controlmermas.html"
    },
    # --- MÓDULO FLOTA ---
    "flota": {
        "Operador_transporteespecial":    "vehiculos_tespecial.html",
        "Operador_transportecarga":       "vehiculos_tcarga.html",
        "Controlador_transproteespecial": "control_vehiculos_tespecial.html",
        "Controlador_transportecarga":    "control_vehiculos_tcarga.html",
        "webmaster":                      "control_vehiculos_tcarga.html", 
        "admin":                          "control_vehiculos_tcarga.html"
    }
}

# --- RUTAS OFFLINE ---
@bp_890707006.route("/890707006_offline.html")
def panel_pollosgar_offline():
    return render_template("890707006_offline.html")

@bp_890707006.route("/glp_offline.html")
def glp_offline():
    return render_template("glp_offline.html")

# --- PANEL PRINCIPAL ---
@bp_890707006.route('/890707006.html')
@login_required_custom
def panel_pollosgar():
    # LÓGICA LOGO: Obtener NIT
    # 1. Intentar de sesión
    nit = session.get('nit')

    # 2. Si no hay NIT en sesión, buscarlo en la tabla USUARIOS usando el ID del usuario logueado.
    #    NOTA: En tu BD, la columna 'empresa_id' en 'usuarios' ES el NIT.
    if not nit:
        try:
            usuario_id = session.get('usuario_id')
            if usuario_id:
                cur = mysql.connection.cursor()
                cur.execute("SELECT empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
                row = cur.fetchone()
                cur.close()
                
                if row:
                    # Obtenemos el valor de la columna empresa_id (que es el NIT)
                    raw_nit = row.get('empresa_id') if isinstance(row, dict) else row[0]
                    if raw_nit:
                        nit = str(raw_nit).strip()
                        session['nit'] = nit # Guardar en sesión
        except Exception as e:
            print(f"Error recuperando NIT: {e}")
            nit = None

    return render_template(
        '890707006.html', 
        nombre=session.get('nombre'), 
        empresa=session.get('empresa'),
        nit=nit
    )


# ==============================================================================
# ENRUTADOR UNIVERSAL
# ==============================================================================
# ==============================================================================
# ENRUTADOR UNIVERSAL (CORREGIDO)
# ==============================================================================
@bp_890707006.route('/router/<modulo>')
@login_required_custom
def router_universal(modulo):
    usuario_id = session.get('usuario_id')
    if not usuario_id:
        return redirect(url_for('index'))

    # Obtener Perfil actualizado desde BD
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (usuario_id,))
        row = cur.fetchone()
        cur.close()
        
        if row:
            if isinstance(row, dict):
                perfil_db = (row.get('perfil') or '').strip()
            else:
                perfil_db = (row[0] or '').strip()
            session['perfil'] = perfil_db
        else:
            perfil_db = (session.get('perfil') or '').strip()
            
    except Exception:
        perfil_db = (session.get('perfil') or '').strip()

    # Validar acceso
    reglas_modulo = ACCESO_MODULOS.get(modulo)
    
    if not reglas_modulo:
        flash(f"Error: El módulo '{modulo}' no está configurado.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

    archivo_destino = reglas_modulo.get(perfil_db)
    
    # Búsqueda case-insensitive si falla la exacta
    if not archivo_destino:
        perfil_lower = perfil_db.lower()
        for p_key, archivo in reglas_modulo.items():
            if p_key.lower() == perfil_lower:
                archivo_destino = archivo
                break

    if archivo_destino:
        # --- CORRECCIÓN CRÍTICA AQUÍ ---
        # Si el destino es 'facturas_glp.html', NO lo renderizamos directo.
        # Redirigimos a la ruta del Blueprint GLP que carga los datos de la BD.
        if archivo_destino == "facturas_glp.html":
            return redirect(url_for('bp_glp.ver_facturas_glp'))
            
        # Para los demás archivos, renderizamos normal
        return render_template(
            archivo_destino, 
            nombre=session.get('nombre'), 
            empresa=session.get('empresa'),
            perfil=perfil_db,
            nit=session.get('nit') 
        )
    else:
        flash(f"Acceso denegado: Perfil '{perfil_db}' sin permiso para '{modulo}'.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

# =========================================================
# LÓGICA DE FLOTA (PRELOGIN)
# =========================================================
@bp_890707006.route('/dashboard/flota/prelogin', methods=['POST'])
@login_required_custom
def prelogin_flota():
    data = request.get_json(silent=True) or {}
    placa = (data.get("placa") or "").upper().strip()
    
    if not placa:
        return jsonify(success=False, message="Placa no detectada."), 400

    empresa = session.get("empresa")
    if not empresa:
        return jsonify(success=False, message="Sesión inválida."), 403

    cur = mysql.connection.cursor()
    cur.execute("SELECT id, empresa FROM vehiculos WHERE placa = %s LIMIT 1", (placa,))
    v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="Vehículo no encontrado."), 404
    
    v_empresa = v.get("empresa") if isinstance(v, dict) else v[1]
    v_id = v.get("id") if isinstance(v, dict) else v[0]

    if v_empresa != empresa:
        cur.close()
        return jsonify(success=False, message="Vehículo no pertenece a su empresa."), 403

    cur.execute("UPDATE vehiculos SET estatus='Prelogueado' WHERE id=%s", (v_id,))
    mysql.connection.commit()
    cur.close()

    session["placa_prelogueada"] = placa
    
    return jsonify(
        success=True, 
        message="Vehículo prelogueado.", 
        redirect_url=url_for("bp_890707006.router_universal", modulo="flota")
    )