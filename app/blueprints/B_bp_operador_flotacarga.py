# app/blueprints/B_bp_operador_flotacarga.py
import math
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash
from app import mysql
from app.utils import login_required_custom
from datetime import datetime
import MySQLdb.cursors

bp_flotacarga = Blueprint('flotacarga', __name__)

# ==============================================================================
# 1. RUTA: DASHBOARD PRINCIPAL DEL OPERADOR
# ==============================================================================
@bp_flotacarga.route('/dashboard_operador')
@login_required_custom
def dashboard_operador():
    return render_template('B_modulo_operador_flotacarga.html',
                           nit=session.get('nit'),
                           empresa=session.get('empresa'),
                           nombre=session.get('nombre'))


# ==============================================================================
# 2. LÓGICA DE PRELOGIN (Migrada quirúrgicamente desde PWA Avícola)
# ==============================================================================
@bp_flotacarga.route('/dashboard/flota/prelogin', methods=['POST'])
@login_required_custom
def prelogin_flota():
    # A. CANDADO COMERCIAL: Verificar que la empresa pagó por este módulo
    modulos_activos = session.get('modulos_activos', [])
    if 'flota' not in modulos_activos:
        return jsonify(success=False, message="Acceso denegado: Tu empresa no tiene contratado el módulo de Flota."), 403

    # B. CAPTURA DE DATOS
    data = request.get_json(silent=True) or {}
    placa = (data.get("placa") or "").upper().strip()
    
    if not placa:
        return jsonify(success=False, message="Placa no detectada."), 400

    empresa = session.get("empresa")
    if not empresa:
        return jsonify(success=False, message="Sesión inválida."), 403

    # C. VERIFICACIÓN EN BASE DE DATOS
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, empresa FROM vehiculos WHERE placa = %s LIMIT 1", (placa,))
    v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="Vehículo no encontrado en el sistema."), 404
    
    v_empresa = v.get("empresa") if isinstance(v, dict) else v[1]
    v_id = v.get("id") if isinstance(v, dict) else v[0]

    # D. CANDADO MULTIEMPRESA: Evitar que modifiquen camiones de otras empresas
    if str(v_empresa).strip() != str(empresa).strip():
        cur.close()
        return jsonify(success=False, message="Este vehículo no pertenece a su empresa."), 403

    # E. ACTUALIZACIÓN DE ESTADO
    cur.execute("UPDATE vehiculos SET estatus='Prelogueado' WHERE id=%s", (v_id,))
    mysql.connection.commit()
    cur.close()

    session["placa_prelogueada"] = placa
    
    # F. ÉXITO -> Redirección al Enrutador Maestro Universal de energix_360.py
    return jsonify(
        success=True, 
        message="Vehículo prelogueado correctamente.", 
        redirect_url="/router/flota"
    )

# LA RUTA VIEJA DE COMBUSTIBLE FUE ELIMINADA DE AQUÍ PARA EVITAR CONFLICTOS.

# ==============================================================================
# 4. MOTOR DE RASTREO Y GEOCERCA
# ==============================================================================
@bp_flotacarga.route('/api/actualizar_ubicacion', methods=['POST'])
def actualizar_ubicacion():
    if 'usuario_id' not in session or 'placa_prelogueada' not in session:
        return jsonify({"status": "error", "message": "Sesión inválida o vehículo no logueado"}), 401

    placa = session.get('placa_prelogueada')
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    latitud = datos.get('lat')
    longitud = datos.get('lng')

    if latitud is None or longitud is None:
        return jsonify({"status": "error", "message": "Faltan coordenadas"}), 400

    cur = mysql.connection.cursor()
    try:
        cur.execute("UPDATE vehiculos SET ultima_latitud = %s, ultima_longitud = %s WHERE placa = %s AND id_empresa = %s", (latitud, longitud, placa, empresa_id))
        mysql.connection.commit()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error"}), 500
    finally:
        cur.close()


def calcular_distancia(lat1, lon1, lat2, lon2):
    R = 6371000 
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    delta_phi = math.radians(float(lat2) - float(lat1))
    delta_lambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(delta_phi/2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


@bp_flotacarga.route('/cron/deslogueo_geocerca', methods=['GET', 'POST'])
def cron_deslogueo_geocerca():
    token = request.args.get('token')
    if token != 'BQA_CRON_2026': return jsonify({"status": "error"}), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT v.placa, v.ultima_latitud, v.ultima_longitud, 
               e.base_latitud, e.base_longitud, e.radio_permitido_metros
        FROM vehiculos v
        JOIN empresas e ON v.id_empresa = e.id
        WHERE v.estatus IN ('Logueado', 'Prelogueado') 
          AND v.ultima_latitud IS NOT NULL AND e.base_latitud IS NOT NULL
    """)
    vehiculos = cur.fetchall()
    vehiculos_a_desloguear = []
    
    for v in vehiculos:
        distancia = calcular_distancia(v['ultima_latitud'], v['ultima_longitud'], v['base_latitud'], v['base_longitud'])
        if distancia <= (v['radio_permitido_metros'] or 200):
            vehiculos_a_desloguear.append(v['placa'])
            
    if vehiculos_a_desloguear:
        format_strings = ','.join(['%s'] * len(vehiculos_a_desloguear))
        cur.execute(f"UPDATE vehiculos SET estatus = 'No logueado' WHERE placa IN ({format_strings})", tuple(vehiculos_a_desloguear))
        mysql.connection.commit()
        
    cur.close()
    return jsonify({"status": "success", "total_deslogueados": len(vehiculos_a_desloguear)})