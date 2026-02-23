from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify
from app.utils import login_required_custom
from app import mysql

gestionavicola_bp = Blueprint('gestionavicola_bp', __name__)

# ==============================================================================
# CONFIGURACIÓN MAESTRA DE ACCESOS (GENÉRICO)
# ==============================================================================
ACCESO_MODULOS = {
    "gas": {
        "operador_gas":        "glp.html",
        "supervisor_gas":      "B_supervisorgas.html", # <--- NUEVO ROL
        "controlfacturas_gas": "facturas_glp.html",
        "auditor_gas":         "auditor_glp.html"
    },
    "mermas": {
        "operador_mermas":    "mermas.html",
        "controlador_mermas": "controlmermas.html",
        "webmaster":          "controlmermas.html",
        "admin":              "controlmermas.html"
    },
    "flota": {
        "operador_transporteespecial":    "vehiculos_tespecial.html",
        "operador_transportecarga":       "A_control_logistica.html",
        "controlador_transproteespecial": "control_vehiculos_tespecial.html",
        "controlador_transportecarga":    "control_logistica.html",
        "webmaster":                      "control_vehiculos_tcarga.html", 
        "admin":                          "control_vehiculos_tcarga.html"
    }
}

# --- RUTAS OFFLINE ---
@gestionavicola_bp.route("/gestion_avicola_offline.html")
def panel_avicola_offline():
    return render_template("gestion_avicola_offline.html")

@gestionavicola_bp.route("/glp_offline.html")
def glp_offline():
    return render_template("glp_offline.html")

# --- PANEL PRINCIPAL SaaS ---
@gestionavicola_bp.route('/gestion_avicola.html')
@login_required_custom
def panel_avicola():
    nit = session.get('nit')

    if not nit:
        try:
            usuario_id = session.get('usuario_id')
            if usuario_id:
                cur = mysql.connection.cursor()
                cur.execute("SELECT empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
                row = cur.fetchone()
                cur.close()
                
                if row:
                    raw_nit = row.get('empresa_id') if isinstance(row, dict) else row[0]
                    if raw_nit:
                        nit = str(raw_nit).strip()
                        session['nit'] = nit 
        except Exception as e:
            print(f"Error recuperando NIT: {e}")
            nit = None

    return render_template(
        'A_gestion_avicola.html', 
        nombre=session.get('nombre'), 
        empresa=session.get('empresa'),
        nit=nit
    )

# ==============================================================================
# ENRUTADOR UNIVERSAL CON FEATURE TOGGLING
# ==============================================================================
@gestionavicola_bp.route('/avicola/router/<modulo>')
@login_required_custom
def router_universal(modulo):
    usuario_id = session.get('usuario_id')
    if not usuario_id:
        return redirect(url_for('index'))

    # 1. EL GUARDIA DE SEGURIDAD COMERCIAL: ¿Su empresa pagó por esto?
    modulos_comprados = session.get('modulos_activos', [])
    if modulo not in modulos_comprados:
        flash(f"Tu empresa no tiene contratado el módulo de {modulo.capitalize()}.", "warning")
        return redirect(url_for('gestionavicola_bp.panel_avicola'))

    # 2. Obtener Perfil actualizado desde BD
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (usuario_id,))
        row = cur.fetchone()
        cur.close()
        
        if row:
            perfil_db = (row.get('perfil') if isinstance(row, dict) else row[0] or '').strip()
            session['perfil'] = perfil_db
        else:
            perfil_db = (session.get('perfil') or '').strip()
            
    except Exception:
        perfil_db = (session.get('perfil') or '').strip()

    # 3. Validar acceso según su Rol
    reglas_modulo = ACCESO_MODULOS.get(modulo)
    archivo_destino = reglas_modulo.get(perfil_db.lower())
    
    if not archivo_destino:
        for p_key, archivo in reglas_modulo.items():
            if p_key.lower() == perfil_db.lower():
                archivo_destino = archivo
                break

    if archivo_destino:
        # --- ENRUTAMIENTO ESPECIAL ---
        if archivo_destino == "facturas_glp.html":
            return redirect(url_for('bp_glp.ver_facturas_glp'))
            
        elif archivo_destino == "B_supervisorgas.html":
            return redirect(url_for('bp_supervisorgas.panel_supervisor'))
            
        return render_template(
            archivo_destino, 
            nombre=session.get('nombre'), 
            empresa=session.get('empresa'),
            perfil=perfil_db,
            nit=session.get('nit') 
        )
    else:
        flash(f"Acceso denegado: Tu perfil '{perfil_db}' no tiene permisos para operar '{modulo}'.", "danger")
        return redirect(url_for('gestionavicola_bp.panel_avicola'))

# =========================================================
# LÓGICA DE FLOTA (PRELOGIN)
# =========================================================
@gestionavicola_bp.route('/dashboard/flota/prelogin', methods=['POST'])
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
        redirect_url=url_for("gestionavicola_bp.router_universal", modulo="flota")
    )