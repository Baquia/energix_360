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
# 2. LÓGICA DE PRELOGIN (Enlace con el Vehículo)
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
    empresa_id = session.get("empresa_id")
    usuario_id = session.get("usuario_id")
    if not empresa or not usuario_id:
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

    # E. ACTUALIZACIÓN DE ESTADO Y REGISTRO DE SESIÓN DE OPERADOR
    try:
        cur.execute("UPDATE vehiculos SET estatus='Prelogueado' WHERE id=%s", (v_id,))
        
        # Guardar en el historial de sesiones el logueo para el tablero del controlador
        cur.execute("""
            INSERT INTO historial_sesiones_flota (id_empresa, id_usuario, placa_vehiculo, fecha_login)
            VALUES (%s, %s, %s, NOW())
        """, (empresa_id, usuario_id, placa))
        
        mysql.connection.commit()
    except Exception as e:
        mysql.connection.rollback()
        cur.close()
        return jsonify(success=False, message=f"Error al registrar sesión: {str(e)}"), 500
    finally:
        cur.close()

    session["placa_prelogueada"] = placa
    
    # F. ÉXITO -> Redirección al Enrutador Maestro Universal de energix_360.py
    return jsonify(
        success=True, 
        message="Vehículo prelogueado correctamente.", 
        redirect_url="/router/flota"
    )

# ==============================================================================
# 3. RUTAS DE SESIÓN EN VIVO (HEARTBEAT Y LOGOUT)
# ==============================================================================
@bp_flotacarga.route('/api/flota/heartbeat', methods=['POST'])
def heartbeat_flota():
    """Endpoint llamado por la PWA del operador para mantener su estado en línea"""
    if 'usuario_id' not in session:
        return jsonify({"status": "error", "message": "No autenticado"}), 401
        
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (session.get('usuario_id'),))
        mysql.connection.commit()
        cur.close()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@bp_flotacarga.route('/api/flota/logout_manual', methods=['POST'])
def logout_manual_flota():
    """Cierra la sesión operativa, marcando la hora exacta para el dashboard"""
    if 'usuario_id' not in session: 
        return jsonify({"status": "error"}), 401
        
    empresa_id = session.get('empresa_id')
    usuario_id = session.get('usuario_id')
    placa = session.get('placa_prelogueada')
    
    try:
        cur = mysql.connection.cursor()
        
        # 1. Registrar hora de cierre manual
        cur.execute("""
            UPDATE historial_sesiones_flota 
            SET fecha_logout_manual = NOW(), estado_sesion = 'FINALIZADA'
            WHERE id_usuario = %s AND id_empresa = %s AND estado_sesion = 'ACTIVA'
            ORDER BY id DESC LIMIT 1
        """, (usuario_id, empresa_id))
        
        # 2. Liberar el vehículo
        if placa:
            cur.execute("UPDATE vehiculos SET estatus='No logueado' WHERE placa=%s AND id_empresa=%s", (placa, empresa_id))
            
        mysql.connection.commit()
        cur.close()
        
        # Limpiar variable de sesión de la flota
        session.pop('placa_prelogueada', None)
        
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ==============================================================================
# 4. MOTOR DE RASTREO, GEOCERCA Y RUTAS HISTÓRICAS
# ==============================================================================
@bp_flotacarga.route('/api/actualizar_ubicacion', methods=['POST'])
def actualizar_ubicacion():
    if 'usuario_id' not in session or 'placa_prelogueada' not in session:
        return jsonify({"status": "error", "message": "Sesión inválida o vehículo no logueado"}), 401

    placa = session.get('placa_prelogueada')
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    
    # Soporte para sincronización offline (array de ubicaciones) o punto individual
    ubicaciones = datos.get('ubicaciones', [])
    if not ubicaciones:
        latitud = datos.get('lat')
        longitud = datos.get('lng')
        if latitud and longitud:
            ubicaciones = [{'lat': latitud, 'lng': longitud, 'fecha_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]

    if not ubicaciones:
        return jsonify({"status": "error", "message": "Faltan coordenadas"}), 400

    cur = mysql.connection.cursor()
    try:
        # 1. Actualizar última ubicación en tabla vehículos (usando el punto más reciente)
        ultima_lat = ubicaciones[-1]['lat']
        ultima_lng = ubicaciones[-1]['lng']
        cur.execute("UPDATE vehiculos SET ultima_latitud = %s, ultima_longitud = %s WHERE placa = %s AND id_empresa = %s", (ultima_lat, ultima_lng, placa, empresa_id))
        
        # 2. Guardar el recorrido en el historial de rutas
        for ubi in ubicaciones:
            fecha_hora = ubi.get('fecha_hora', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            cur.execute("""
                INSERT INTO vehiculos_historial_rutas (id_empresa, placa, latitud, longitud, fecha_hora, tipo_registro)
                VALUES (%s, %s, %s, %s, %s, 'Automático')
            """, (empresa_id, placa, ubi['lat'], ubi['lng'], fecha_hora))

        mysql.connection.commit()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()


@bp_flotacarga.route('/api/registrar_parada', methods=['POST'])
def registrar_parada():
    """Endpoint para clasificar e insertar paradas manuales o automáticas, compatible con offline sync."""
    if 'usuario_id' not in session or 'placa_prelogueada' not in session:
        return jsonify({"status": "error", "message": "Sesión inválida"}), 401
        
    datos = request.get_json(silent=True) or {}
    empresa_id = session.get('empresa_id')
    placa = session.get('placa_prelogueada')
    usuario_id = session.get('usuario_id')
    
    paradas = datos.get('paradas', [])
    if not paradas and 'lat' in datos:
        paradas = [datos]
        
    if not paradas:
        return jsonify({"status": "error", "message": "Datos de parada vacíos"}), 400
        
    cur = mysql.connection.cursor()
    try:
        ids_insertados = []
        for p in paradas:
            # 1. Guardar en el historial analítico de paradas
            cur.execute("""
                INSERT INTO historial_paradas_flota 
                (id_empresa, placa, usuario_id, fecha, hora_inicio, hora_fin, latitud, longitud, tipo_actividad, nombre_punto, origen_registro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, placa, usuario_id, 
                p.get('fecha', datetime.now().strftime('%Y-%m-%d')),
                p.get('hora_inicio', datetime.now().strftime('%H:%M:%S')),
                p.get('hora_fin'), # Puede ser nulo si la parada sigue activa
                p.get('lat'), p.get('lng'),
                p.get('tipo_actividad', 'Otra'),
                p.get('nombre_punto', 'Punto Desconocido'),
                p.get('origen_registro', 'Manual')
            ))
            nuevo_id = cur.lastrowid
            ids_insertados.append({"temp_id": p.get('temp_id'), "db_id": nuevo_id})
            
            # 2. Insertar también un punto destacado en el historial de rutas
            fecha_hora = f"{p.get('fecha', datetime.now().strftime('%Y-%m-%d'))} {p.get('hora_inicio', datetime.now().strftime('%H:%M:%S'))}"
            cur.execute("""
                INSERT INTO vehiculos_historial_rutas (id_empresa, placa, latitud, longitud, fecha_hora, tipo_registro, nombre_punto)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, placa, p.get('lat'), p.get('lng'), fecha_hora,
                'Parada ' + p.get('origen_registro', 'Manual'),
                p.get('nombre_punto', 'Punto Desconocido')
            ))

        mysql.connection.commit()
        return jsonify({"status": "success", "ids_mapeados": ids_insertados}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()


@bp_flotacarga.route('/api/actualizar_fin_parada', methods=['POST'])
def actualizar_fin_parada():
    """Endpoint para cerrar una parada (hora_fin) cuando el vehículo reanuda la marcha."""
    if 'usuario_id' not in session: 
        return jsonify({"status": "error"}), 401
    
    datos = request.get_json(silent=True) or {}
    paradas = datos.get('paradas', [])
    if not paradas and 'id' in datos:
        paradas = [datos]
        
    cur = mysql.connection.cursor()
    try:
        for p in paradas:
            if p.get('id'):
                cur.execute("""
                    UPDATE historial_paradas_flota 
                    SET hora_fin = %s 
                    WHERE id = %s AND id_empresa = %s
                """, (p.get('hora_fin', datetime.now().strftime('%H:%M:%S')), p.get('id'), session.get('empresa_id')))
        mysql.connection.commit()
        return jsonify({"status":"success"})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status":"error", "message": str(e)}), 500
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
        
        # Opcional: Cerrar la sesión en historial_sesiones_flota si fue forzado por geocerca
        # ...
        
        mysql.connection.commit()
        
    cur.close()
    return jsonify({"status": "success", "total_deslogueados": len(vehiculos_a_desloguear)})