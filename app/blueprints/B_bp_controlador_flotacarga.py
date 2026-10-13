# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: CONTROLADOR_FLOTA | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/B_bp_controlador_flotacarga.py
import json
import math
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash
from app import mysql
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta

bp_gestorflota = Blueprint('gestorflota', __name__, url_prefix='/gestor_flota')
bp_controlador_flotacarga = bp_gestorflota

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS (CORREGIDO - ANTI BUCLE)
# ==============================================================================
def gestor_flota_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'gestor_flotacarga', 
            'controlador_transportecarga', 
            'controlador_flotacarga', 
            'controlador_logistica',
            'controlador_pesv',
            'webmaster'
        ]
        
        # 1. Validación de Perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            if request.is_json:
                return jsonify(success=False, message="Acceso denegado: Se requiere perfil de Controlador de Flota."), 403
            flash('Acceso denegado: Se requiere perfil de Gestor/Controlador de Flota para ingresar a este módulo.', 'danger')
            return redirect('/gestor_flota/dashboard')
            
        # 2. Validación Plug & Play (Inquilino - Anti Bucle)
        if 'webmaster' not in tipo_empresa:
            autorizados = session.get('submodulos_activos', [])
            if not autorizados:
                autorizados = session.get('modulos_activos', [])
                
            if not any(m in autorizados for m in ['controlador_flota', 'flota', 'carga']):
                if request.is_json:
                    return jsonify(success=False, message="Acceso denegado: Tu empresa no tiene activo el submódulo de Controlador de Flota."), 403
                flash('Acceso denegado: Tu empresa no tiene contratado/activo el submódulo de Controlador de Flota.', 'danger')
                return redirect('/gestor_flota/dashboard')
                
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# HERRAMIENTAS MATEMÁTICAS (HAVERSINE)
# =========================================================
def calcular_distancia(lat1, lon1, lat2, lon2):
    R = 6371000 
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    delta_phi = math.radians(float(lat2) - float(lat1))
    delta_lambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(delta_phi/2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# =========================================================
# RUTAS DEL PANEL ADMINISTRATIVO (HUB CENTRAL)
# =========================================================

@bp_gestorflota.route('/dashboard')
#@bp_gestorflota.route('/hub_herramientas')
#@bp_gestorflota.route('/hub_herramientas/')
@login_required_custom
@gestor_flota_required
def dashboard_gestor():
    empresa_id = session.get('empresa_id')
    nit = session.get('nit')
    
    # 0. Crear tablas de monitoreo e inyectar columnas de geolocalización y feature flags (Mantenimiento BD)
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historial_sesiones_flota (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_empresa INT NOT NULL,
                id_usuario INT NOT NULL,
                placa_vehiculo VARCHAR(20),
                fecha_login DATETIME,
                fecha_logout_manual DATETIME,
                latitud DECIMAL(10, 8),
                longitud DECIMAL(11, 8),
                estado_sesion VARCHAR(20) DEFAULT 'ACTIVA',
                INDEX(id_empresa, id_usuario)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitoreo_actividad (
                id_usuario INT PRIMARY KEY,
                ultima_actividad DATETIME
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        mysql.connection.commit()
        
        try:
            cur.execute("ALTER TABLE historial_sesiones_flota ADD COLUMN latitud DECIMAL(10, 8), ADD COLUMN longitud DECIMAL(11, 8)")
            mysql.connection.commit()
        except Exception:
            pass
            
        try:
            cur.execute("ALTER TABLE empresas ADD COLUMN funcionalidades_operador JSON")
            mysql.connection.commit()
        except Exception:
            pass

        cur.close()
    except Exception as e:
        print(f"Aviso tablas monitoreo flota y BD: {e}")

    # 1. Sincronización en caliente de submódulos y funcionalidades activas (Plug & Play Dinámico)
    if nit:
        try:
            cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
            cur.execute("SELECT submodulos_activos, funcionalidades_operador FROM empresas WHERE nit = %s", (nit,))
            row = cur.fetchone()
            cur.close()
            if row:
                # Sincronizar Módulos Macro
                raw_sub = row.get('submodulos_activos')
                if raw_sub:
                    try:
                        session['submodulos_activos'] = json.loads(raw_sub) if isinstance(raw_sub, str) else raw_sub
                    except Exception:
                        session['submodulos_activos'] = []
                else:
                    session['submodulos_activos'] = []
                
                # Sincronizar Feature Flags del Operador
                raw_func = row.get('funcionalidades_operador')
                if raw_func:
                    try:
                        session['funcionalidades_operador'] = json.loads(raw_func) if isinstance(raw_func, str) else raw_func
                    except Exception:
                        session['funcionalidades_operador'] = ['registro_paradas']
                else:
                    session['funcionalidades_operador'] = ['registro_paradas']
        except Exception as e:
            print(f"Error actualizando variables de entorno P&P en sesión: {e}")

    # 2. Consultar operadores logueados, su estado en vivo, vehículo, tiempos y coordenadas
    operadores_en_linea = []
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                u.id as id_operador,
                u.nombre as nombre_operador,
                u.perfil,
                IF(ma.ultima_actividad IS NOT NULL AND TIMESTAMPDIFF(SECOND, ma.ultima_actividad, NOW()) <= 300, 1, 0) as en_linea,
                hs.placa_vehiculo,
                hs.fecha_login,
                hs.fecha_logout_manual,
                hs.latitud,
                hs.longitud
            FROM usuarios u
            INNER JOIN historial_sesiones_flota hs ON hs.id = (
                SELECT MAX(id) FROM historial_sesiones_flota 
                WHERE id_usuario = u.id AND DATE(fecha_login) = CURDATE()
            )
            LEFT JOIN monitoreo_actividad ma ON ma.id_usuario = u.id
            WHERE u.empresa_id = %s AND u.perfil = 'operador_flotacarga' AND hs.estado_sesion = 'ACTIVA'
            ORDER BY u.nombre ASC
        """, (empresa_id,))
        
        operadores_db = cur.fetchall()
        
        # Coordenadas exactas Planta Pollos GAR
        PLANTA_LAT = 4.4134686
        PLANTA_LNG = -75.1797367
        
        # AJUSTE HORA COLOMBIA (UTC-5): Calculamos dinámicamente el desfase del servidor
        now_local = datetime.now()
        now_utc = datetime.utcnow()
        server_offset = round((now_local - now_utc).total_seconds() / 3600)
        adj_hours = -5 - server_offset
        
        for op in operadores_db:
            op_dict = dict(op)
            
            if op_dict['fecha_login']:
                op_dict['fecha_login'] += timedelta(hours=adj_hours)
                op_dict['fecha_login_str'] = op_dict['fecha_login'].strftime('%H:%M:%S')
            else:
                op_dict['fecha_login_str'] = '--:--'
                
            if op_dict['fecha_logout_manual']:
                op_dict['fecha_logout_manual'] += timedelta(hours=adj_hours)
                op_dict['fecha_logout_str'] = op_dict['fecha_logout_manual'].strftime('%H:%M:%S')
            else:
                op_dict['fecha_logout_str'] = '--:--'
                
            op_dict['sitio_logueo'] = "No registrado"
            if op_dict.get('latitud') and op_dict.get('longitud'):
                dist = calcular_distancia(op_dict['latitud'], op_dict['longitud'], PLANTA_LAT, PLANTA_LNG)
                if dist <= 100:
                    op_dict['sitio_logueo'] = "PLANTA DE PROCESO POLLOS GAR"
                else:
                    op_dict['sitio_logueo'] = f"Lat: {op_dict['latitud']}, Lng: {op_dict['longitud']}"
                
            operadores_en_linea.append(op_dict)
            
        cur.close()
    except Exception as e:
        print(f"Error consultando monitoreo de flota: {e}")

    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='dashboard',
        operadores_en_linea=operadores_en_linea,
        submodulos_activos=session.get('submodulos_activos', []),
        funcionalidades_operador=session.get('funcionalidades_operador', ['registro_paradas'])
    )

# =========================================================
# ENDPOINT DE MONITOREO EN TIEMPO REAL (AJAX)
# =========================================================
@bp_gestorflota.route('/api/flota/monitoreo_realtime')
@login_required_custom
@gestor_flota_required
def monitoreo_realtime():
    empresa_id = session.get('empresa_id')
    operadores_en_linea = []
    
    try:
        # Registrar latido del propio controlador
        cur_hb = mysql.connection.cursor()
        cur_hb.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (session.get('usuario_id'),))
        mysql.connection.commit()
        cur_hb.close()

        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Mostrar logueados activos e identificar cortes de señal con tolerancia de 300s (5 min)
        cur.execute("""
            SELECT 
                u.id as id_operador,
                u.nombre as nombre_operador,
                u.perfil,
                IF(ma.ultima_actividad IS NOT NULL AND TIMESTAMPDIFF(SECOND, ma.ultima_actividad, NOW()) <= 300, 1, 0) as en_linea,
                hs.placa_vehiculo,
                hs.fecha_login,
                hs.fecha_logout_manual,
                hs.latitud,
                hs.longitud
            FROM usuarios u
            INNER JOIN historial_sesiones_flota hs ON hs.id = (
                SELECT MAX(id) FROM historial_sesiones_flota 
                WHERE id_usuario = u.id AND DATE(fecha_login) = CURDATE()
            )
            LEFT JOIN monitoreo_actividad ma ON ma.id_usuario = u.id
            WHERE u.empresa_id = %s AND u.perfil = 'operador_flotacarga' AND hs.estado_sesion = 'ACTIVA'
            ORDER BY u.nombre ASC
        """, (empresa_id,))
        
        operadores_db = cur.fetchall()
        
        PLANTA_LAT = 4.4134686
        PLANTA_LNG = -75.1797367
        
        # AJUSTE HORA COLOMBIA (UTC-5) PARA AJAX
        now_local = datetime.now()
        now_utc = datetime.utcnow()
        server_offset = round((now_local - now_utc).total_seconds() / 3600)
        adj_hours = -5 - server_offset
        
        for op in operadores_db:
            op_dict = dict(op)
            
            if op_dict['fecha_login']:
                op_dict['fecha_login'] += timedelta(hours=adj_hours)
                op_dict['fecha_login_str'] = op_dict['fecha_login'].strftime('%H:%M:%S')
            else:
                op_dict['fecha_login_str'] = '--:--'
                
            if op_dict['fecha_logout_manual']:
                op_dict['fecha_logout_manual'] += timedelta(hours=adj_hours)
                op_dict['fecha_logout_str'] = op_dict['fecha_logout_manual'].strftime('%H:%M:%S')
            else:
                op_dict['fecha_logout_str'] = '--:--'
                
            op_dict['sitio_logueo'] = "No registrado"
            if op_dict.get('latitud') and op_dict.get('longitud'):
                dist = calcular_distancia(op_dict['latitud'], op_dict['longitud'], PLANTA_LAT, PLANTA_LNG)
                if dist <= 100:
                    op_dict['sitio_logueo'] = "PLANTA DE PROCESO POLLOS GAR"
                else:
                    op_dict['sitio_logueo'] = f"Lat: {op_dict['latitud']}, Lng: {op_dict['longitud']}"
            
            # Formatear el string del perfil
            op_dict['perfil_str'] = str(op_dict['perfil']).replace('_', ' ').title()
                
            # Limpiar los objetos datetime y decimal nativos
            op_dict.pop('fecha_login', None)
            op_dict.pop('fecha_logout_manual', None)
            op_dict.pop('latitud', None)
            op_dict.pop('longitud', None)
                
            operadores_en_linea.append(op_dict)
            
        cur.close()
        return jsonify({'status': 'success', 'operadores': operadores_en_linea})
    except Exception as e:
        print(f"Error consultando realtime de flota: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

# =========================================================
# ENDPOINT PARA FEATURE FLAGS DEL OPERADOR
# =========================================================
@bp_gestorflota.route('/api/flota/toggle_feature', methods=['POST'])
@login_required_custom
@gestor_flota_required
def toggle_feature_operador():
    nit = session.get('nit')
    data = request.get_json(silent=True) or {}
    feature = data.get('feature')
    estado = data.get('estado') # Booleano (True para encender, False para apagar)
    
    if not nit or not feature:
        return jsonify({'status': 'error', 'message': 'Datos inválidos para configuración.'}), 400
        
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # Bloqueo para evitar colisión si varios administradores modifican a la vez
        cur.execute("SELECT funcionalidades_operador FROM empresas WHERE nit = %s FOR UPDATE", (nit,))
        row = cur.fetchone()
        
        raw_func = row.get('funcionalidades_operador') if row else None
        func_activas = json.loads(raw_func) if isinstance(raw_func, str) else (raw_func or ['registro_paradas'])
        
        if estado and feature not in func_activas:
            func_activas.append(feature)
        elif not estado and feature in func_activas:
            func_activas.remove(feature)
            
        json_str = json.dumps(func_activas)
        cur.execute("UPDATE empresas SET funcionalidades_operador = %s WHERE nit = %s", (json_str, nit))
        mysql.connection.commit()
        cur.close()
        
        # Reflejar inmediatamente en la sesión del controlador
        session.permanent = True
        session['funcionalidades_operador'] = func_activas
        
        return jsonify({'status': 'success', 'funcionalidades': func_activas}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500