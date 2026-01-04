from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify, current_app, make_response
from app.utils import login_required_custom
from app import mysql, csrf
import re
import traceback
from datetime import datetime

bp_890707006 = Blueprint('bp_890707006', __name__)

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
    return render_template('890707006.html', nombre=session.get('nombre'), empresa=session.get('empresa'))


# =========================================================
# 1. ROUTER PRINCIPAL (El semáforo)
#    Esta ruta NO muestra nada, solo redirige a la URL correcta.
# =========================================================
@bp_890707006.route('/dashboard/gas')
@login_required_custom
def router_modulo_gas():
    usuario_id = session.get('usuario_id')
    empresa_nombre = session.get('empresa') 

    if not usuario_id or not empresa_nombre:
        flash("Sesión no válida.", "warning")
        return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for('index'))
    
    # Normalización del perfil
    perfil_raw = usuario.get('perfil') or ''
    perfil = perfil_raw.strip().lower()

    # --- LÓGICA DE REDIRECCIÓN ---
    # Al usar redirect(), el navegador se ve forzado a cambiar de URL,
    # evitando que el Service Worker confunda las pantallas.

    if 'operador' in perfil and 'gas' in perfil:
        # Redirige a la URL exclusiva de operación
        return redirect(url_for('bp_890707006.vista_operador_glp'))
        
    elif 'control' in perfil or 'facturas' in perfil:
        # Redirige a la URL exclusiva de facturación
        return redirect(url_for('bp_890707006.vista_facturas_glp'))
    
    else:
        flash(f"Su perfil ({perfil_raw}) no tiene acceso habilitado al módulo de Gas.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))


# =========================================================
# 2. VISTA ESPECÍFICA: OPERADOR (glp.html)
#    URL: /dashboard/gas/operacion
# =========================================================
@bp_890707006.route('/dashboard/gas/operacion')
@login_required_custom
def vista_operador_glp():
    usuario_id = session.get('usuario_id')
    
    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    # Renderizamos la pantalla de operación GLP
    response = make_response(render_template(
        'glp.html',
        nombre=usuario['nombre'],
        empresa=session.get('empresa')
    ))
    
    # Cabeceras para asegurar que no se cachee la lógica interna
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response


# =========================================================
# 3. VISTA ESPECÍFICA: FACTURAS (facturas_glp.html)
#    URL: /dashboard/gas/facturacion
# =========================================================
@bp_890707006.route('/dashboard/gas/facturacion')
@login_required_custom
def vista_facturas_glp():
    usuario_id = session.get('usuario_id')
    empresa_nombre = session.get('empresa')
    
    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id, cedula FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    
    # --- Lógica de carga de pedidos pendientes ---
    pedidos_pendientes = []
    try:
        # === CORRECCIÓN AQUÍ ===
        # 1. Usamos p.codigo_pedido (la columna real)
        # 2. Mantenemos el alias de fecha para que coincida con tu HTML
        query = """
            SELECT 
                p.id, 
                p.codigo_pedido,                  
                p.fecha_registro AS fecha_generacion, 
                p.proveedor,       
                p.ubicacion
            FROM pedidos_gas_glp p
            WHERE p.cliente = %s AND p.estatus = 'generado'
            ORDER BY p.fecha_registro DESC
        """
        cur.execute(query, (empresa_nombre,))
        
        # Recuperamos los nombres de las columnas para armar el diccionario
        columns = [col[0] for col in cur.description]
        pedidos = cur.fetchall()
        
        for pedido in pedidos:
            # Creamos el diccionario uniendo columnas y valores
            # (Esto funciona tanto si el cursor devuelve tuplas como diccionarios)
            if isinstance(pedido, dict):
                p = pedido
            else:
                p = dict(zip(columns, pedido))
            
            # Formateo de fecha
            if p.get('fecha_generacion'):
                p['fecha_generacion'] = str(p['fecha_generacion'])
            
            p['proveedor'] = p.get('proveedor') or 'N/A'
            pedidos_pendientes.append(p)
            
    except Exception as e:
        flash(f"Error cargando pedidos: {str(e)}", "danger")
        current_app.logger.error(f"Error Facturas GLP: {traceback.format_exc()}")
    
    cur.close()

    # Renderizamos la pantalla de facturas
    response = make_response(render_template(
        'facturas_glp.html',
        nombre=usuario['nombre'],
        nit=usuario['empresa_id'],
        cedula=usuario.get('cedula'),
        empresa=empresa_nombre,
        pedidos=pedidos_pendientes
    ))
    
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return response
# ================================
# MÓDULO MERMAS
# ================================
@bp_890707006.route('/dashboard/mermas')
@login_required_custom
def acceso_modulo_mermas():
    usuario_id = session.get('usuario_id')
    if not usuario_id:
        return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for('index'))

    perfil = (usuario.get('perfil') or '').strip().lower()

    if perfil == 'controlador_mermas':
        return render_template('controlmermas.html', nombre=usuario['nombre'], nit=usuario['empresa_id'])

    if if perfil == 'controlador_mermas':
        return render_template('mermas.html', nombre=usuario['nombre'], nit=usuario['empresa_id'])

    flash("No tiene acceso al módulo Control de Mermas.", "danger")
    return redirect(url_for('bp_890707006.panel_pollosgar'))


# ================================
# MÓDULO FLOTA
# ================================
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
    if not usuario_id:
        return jsonify(success=False, message="Sesión no válida."), 401

    cur = mysql.connection.cursor()
    cur.execute("SELECT id, nombre, perfil, empresa_id FROM usuarios WHERE id=%s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        return jsonify(success=False, message="Usuario no encontrado."), 404

    perfil = (usuario.get("perfil") or "").strip().lower()
    if perfil != "operarios_vehiculos":
        return jsonify(success=False, message="No tiene acceso a flota."), 403

    # Normalización
    placa = re.sub(r'[^A-Z0-9]', '', placa)
    empresa_id = str(session.get("empresa_id") or "").strip()
    empresa_id = re.sub(r'\D', '', empresa_id)
    
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT id, estatus FROM vehiculos 
        WHERE UPPER(TRIM(placa))=%s AND id_empresa=%s LIMIT 1
    """, (placa, int(empresa_id) if empresa_id else 0))
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
    if not usuario_id:
        return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        flash("Usuario no encontrado.", "danger")
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