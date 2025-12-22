from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify, current_app, make_response
from app.utils import login_required_custom
from app import mysql, csrf
import re
import traceback
from datetime import datetime

bp_890707006 = Blueprint('bp_890707006', __name__)

# CONSTANTES DE LA EMPRESA DUEÑA DEL MÓDULO (POLLOS GAR)
NIT_POLLOS = '890707006'

# CONSTANTES DEL WEBMASTER (SUPER ADMIN)
NIT_WEBMASTER = '901811727'
PERFIL_WEBMASTER = 'webmaster_admin'

# =========================================================
# 1. GRUPO INFRAESTRUCTURA Y GENERAL
# =========================================================

@bp_890707006.route("/890707006_offline.html")
def panel_pollosgar_offline():
    return render_template("890707006_offline.html")

@bp_890707006.route("/glp_offline.html")
def glp_offline():
    return render_template("glp_offline.html")

@bp_890707006.route('/890707006.html')
@login_required_custom
def panel_pollosgar():
    # CAMBIO UX: Obtener el perfil real desde la BD para enviarlo al frontend
    # Esto permite que el botón de Flota se bloquee visualmente si no es el perfil correcto
    usuario_id = session.get('usuario_id')
    perfil_usuario = ''
    
    if usuario_id:
        cur = mysql.connection.cursor()
        cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (usuario_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            perfil_usuario = str(row['perfil']).strip().lower()

    return render_template('890707006.html', 
                           nombre=session.get('nombre'), 
                           empresa=session.get('empresa'),
                           perfil=perfil_usuario)


# =========================================================
# 2. GRUPO MÓDULO GAS (GLP)
# =========================================================

@bp_890707006.route('/dashboard/gas')
@login_required_custom
def router_modulo_gas():
    # 1. Validación de Sesión
    usuario_id = session.get('usuario_id')
    empresa_nombre = session.get('empresa') 

    if not usuario_id or not empresa_nombre:
        flash("Sesión no válida.", "warning")
        return redirect(url_for('index'))

    # 2. Obtener datos frescos
    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id, cedula FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    
    if not usuario:
        cur.close()
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for('index'))
    
    # 3. Normalización
    perfil_raw = usuario.get('perfil') or ''
    perfil = perfil_raw.strip().lower().replace('-', '_')
    usuario_empresa_id = str(usuario.get('empresa_id') or '').strip()
    
    # --- DETECCIÓN DE WEBMASTER ---
    es_webmaster = (usuario_empresa_id == NIT_WEBMASTER and perfil == PERFIL_WEBMASTER)

    # --- CASO A: OPERADOR DE GAS ---
    if perfil == 'gar_operador_gas':
        cur.close()
        response = make_response(render_template(
            'glp.html',
            nombre=usuario['nombre'],
            empresa=empresa_nombre
        ))
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response

    # --- CASO B: CONTROLADOR DE FACTURAS O WEBMASTER ---
    elif perfil == 'gar_controlfacturas_gas' or es_webmaster:
        
        pedidos_pendientes = []
        try:
            query = """
                SELECT p.id, p.codigo AS codigo_pedido, p.fecha_registro AS fecha_generacion, 
                       p.proveedor, p.ubicacion
                FROM pedidos_gas_glp p
                WHERE p.cliente = %s AND p.estatus = 'generado'
                ORDER BY p.fecha_registro DESC
            """
            cur.execute(query, (empresa_nombre,))
            pedidos = cur.fetchall()
            
            for p_row in pedidos:
                p = dict(p_row)
                if p['fecha_generacion']: p['fecha_generacion'] = str(p['fecha_generacion'])
                p['proveedor'] = p.get('proveedor') or 'N/A'
                pedidos_pendientes.append(p)
                
        except Exception as e:
            flash(f"Error cargando datos: {str(e)}", "danger")
            current_app.logger.error(f"Error GLP: {traceback.format_exc()}")
        finally:
            cur.close()

        response = make_response(render_template(
            'facturas_glp.html',
            nombre=usuario['nombre'],
            nit=usuario_empresa_id,
            cedula=usuario.get('cedula'),
            empresa=empresa_nombre,
            pedidos=pedidos_pendientes
        ))
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        return response

    # --- CASO C: DENEGADO ---
    else:
        cur.close()
        flash(f"Acceso denegado: Perfil '{perfil_raw}' sin permisos para Gas.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))


# =========================================================
# 3. GRUPO MÓDULO MERMAS
# =========================================================

@bp_890707006.route('/dashboard/mermas')
@login_required_custom
def acceso_modulo_mermas():
    usuario_id = session.get('usuario_id')
    if not usuario_id: return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("SELECT perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    data = cur.fetchone()
    cur.close()

    if not data: return redirect(url_for('index'))

    perfil = str(data['perfil']).strip().lower()  
    empresa = str(data['empresa_id']).strip()
    
    # --- DETECCIÓN DE WEBMASTER ---
    es_webmaster = (empresa == NIT_WEBMASTER and perfil == PERFIL_WEBMASTER)

    # 1. Modo Supervisor (Controlador o Webmaster)
    if (perfil == 'gar_controlador_mermas' and empresa == NIT_POLLOS) or es_webmaster:
        return render_template('controlmermas.html', nombre=session.get('nombre'), nit=empresa)

    # 2. Modo Operador
    if perfil == 'gar_operador_mermas' and empresa == NIT_POLLOS:
        return render_template('mermas.html', nombre=session.get('nombre'), nit=empresa)

    flash("Acceso no autorizado al módulo de Mermas.", "danger")
    return redirect(url_for('bp_890707006.panel_pollosgar'))


# =========================================================
# 4. GRUPO MÓDULO FLOTA (VEHÍCULOS)
# =========================================================

@csrf.exempt
@bp_890707006.route('/dashboard/flota/prelogin', methods=['POST'])
@login_required_custom
def flota_prelogin_qr():
    try:
        j = request.get_json(force=True, silent=True) or {}
        placa = (j.get("placa") or "").strip().upper()
    except Exception:
        return jsonify(success=False, message="JSON inválido"), 400

    if not placa:
        return jsonify(success=False, message="QR inválido."), 400

    usuario_id = session.get("usuario_id")
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, perfil, empresa_id FROM usuarios WHERE id=%s", (usuario_id,))
    usuario = cur.fetchone()
    
    if not usuario:
        cur.close()
        return jsonify(success=False, message="Usuario no encontrado."), 404

    perfil = (usuario.get("perfil") or "").strip().lower()
    
    # IMPORTANTE: El Webmaster NO entra aquí porque no tiene vehículo físico ni QR
    if perfil != "operarios_vehiculos":
        cur.close()
        return jsonify(success=False, message="No tiene acceso a flota."), 403

    placa = re.sub(r'[^A-Z0-9]', '', placa)
    empresa_id = str(session.get("empresa_id") or "").strip()
    empresa_id_num = re.sub(r'\D', '', empresa_id)
    
    cur.execute("""
        SELECT id FROM vehiculos 
        WHERE UPPER(TRIM(placa))=%s AND id_empresa=%s LIMIT 1
    """, (placa, int(empresa_id_num) if empresa_id_num else 0))
    v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="Vehículo no pertenece a su empresa."), 404

    cur.execute("UPDATE vehiculos SET estatus='Prelogueado' WHERE id=%s", (v["id"],))
    mysql.connection.commit()
    cur.close()

    session["placa_prelogueada"] = placa
    return jsonify(success=True, message="Vehículo prelogueado.", redirect_url=url_for("bp_890707006.acceso_modulo_flota"))

@bp_890707006.route('/dashboard/flota')
@login_required_custom
def acceso_modulo_flota():
    usuario_id = session.get('usuario_id')
    
    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        return redirect(url_for('index'))

    perfil = (usuario.get('perfil') or '').strip().lower()
    if perfil != 'operarios_vehiculos':
        flash("No tiene acceso al módulo de flota.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

    placa = session.get("placa_prelogueada") 
    if not placa:
        flash("Debe escanear primero el QR.", "warning")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

    return render_template('vehiculos.html', nombre=usuario['nombre'], nit=usuario['empresa_id'], placa=placa)