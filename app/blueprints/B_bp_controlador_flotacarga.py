# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: CONTROLADOR_FLOTA | CONDICIÓN: OPCIONAL
# app/blueprints/B_bp_controlador_flotacarga.py
import os
import io
import json
import math
import qrcode
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, send_file
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta

# Librerías PDF (QR)
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

halfLetter = (5.5 * inch, 8.5 * inch)

bp_gestorflota = Blueprint('gestorflota', __name__, url_prefix='/gestor_flota')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS (VALIDACIÓN PLUG & PLAY ESTRICTA)
# ==============================================================================
def gestor_flota_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        # 1. Validación de Perfil
        if perfil not in ['gestor_flotacarga', 'controlador_transportecarga', 'webmaster'] and 'webmaster' not in tipo_empresa:
            if request.is_json:
                return jsonify(success=False, message="Acceso denegado: Se requiere perfil de Controlador de Flota."), 403
            flash('Acceso denegado: Se requiere perfil de Gestor/Controlador de Flota para ingresar a este módulo.', 'danger')
            return redirect(url_for('index'))
            
        # 2. Validación Plug & Play (Inquilino)
        if 'webmaster' not in tipo_empresa:
            autorizados = session.get('submodulos_activos', [])
            if not autorizados:
                autorizados = session.get('modulos_activos', [])
                
            if 'controlador_flota' not in autorizados and 'flota' not in autorizados:
                if request.is_json:
                    return jsonify(success=False, message="Acceso denegado: Tu empresa no tiene activo el submódulo de Controlador de Flota."), 403
                flash('Acceso denegado: Tu empresa no tiene contratado/activo el submódulo de Controlador de Flota.', 'danger')
                return redirect(url_for('index'))
                
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
# RUTAS DEL PANEL ADMINISTRATIVO
# =========================================================

@bp_gestorflota.route('/dashboard')
@login_required_custom
@gestor_flota_required
def dashboard_gestor():
    empresa_id = session.get('empresa_id')
    nit = session.get('nit')
    
    # 0. Sincronización en caliente de submódulos activos (Plug & Play Dinámico)
    if nit:
        try:
            cur = mysql.connection.cursor()
            cur.execute("SELECT submodulos_activos FROM empresas WHERE nit = %s", (nit,))
            row = cur.fetchone()
            cur.close()
            if row:
                raw_sub = row[0] if isinstance(row, (tuple, list)) else row.get('submodulos_activos')
                if raw_sub:
                    try:
                        session['submodulos_activos'] = json.loads(raw_sub) if isinstance(raw_sub, str) else raw_sub
                    except Exception:
                        session['submodulos_activos'] = []
                else:
                    session['submodulos_activos'] = []
        except Exception as e:
            print(f"Error actualizando submodulos_activos en sesión: {e}")
            
    # 1. Crear tablas de monitoreo e inyectar columnas de geolocalización
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
        except:
            pass
            
        cur.close()
    except Exception as e:
        print(f"Aviso tablas monitoreo flota: {e}")

    # 2. Consultar operadores logueados, su estado en vivo, vehículo, tiempos y coordenadas
    operadores_en_linea = []
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # ESTADO DINÁMICO: Aumento de tolerancia a 300 segundos (5 min) para soportar pausas de OS en móviles
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
        adj_hours = -5 - server_offset # Objetivo UTC-5 (Colombia)
        
        # 3. Formatear los datos para la vista
        for op in operadores_db:
            op_dict = dict(op)
            
            # Aplicar ajuste horario a logueo y deslogueo
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
                
            # Calcular sitio de logueo por geocerca (100 metros)
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
        operadores_en_linea=operadores_en_linea
    )


# =========================================================
# ENDPOINT DE MONITOREO EN TIEMPO REAL (NUEVO AJAX)
# =========================================================
@bp_gestorflota.route('/api/flota/monitoreo_realtime')
@login_required_custom
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


@bp_gestorflota.route('/vehiculos', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_vehiculos():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'crear':
            placa = str(request.form.get('placa', '')).upper().strip()
            tipo = request.form.get('tipo', '').strip()
            caja_de_carga = request.form.get('caja_de_carga', '').strip()
            referencia = request.form.get('referencia', '').strip()
            peso_vacio = request.form.get('peso_vacio', 0)
            capacidad = request.form.get('capacidad', 0)
            propiedad = request.form.get('propiedad', 'Propio').strip()
            
            if placa and caja_de_carga and tipo:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        INSERT INTO vehiculos (empresa, id_empresa, placa, tipo, caja_de_carga, referencia, peso_vacio, `capacidad (kg)`, propiedad) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (empresa_nombre, empresa_id, placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, propiedad))
                    mysql.connection.commit()
                    flash(f"Vehículo con placa {placa} registrado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al registrar vehículo: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'editar':
            vehiculo_id = request.form.get('vehiculo_id')
            placa = str(request.form.get('placa', '')).upper().strip()
            tipo = request.form.get('tipo', '').strip()
            caja_de_carga = request.form.get('caja_de_carga', '').strip()
            referencia = request.form.get('referencia', '').strip()
            peso_vacio = request.form.get('peso_vacio', 0)
            capacidad = request.form.get('capacidad', 0)
            propiedad = request.form.get('propiedad', 'Propio').strip()
            
            if vehiculo_id and placa and caja_de_carga and tipo:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        UPDATE vehiculos 
                        SET placa = %s, tipo = %s, caja_de_carga = %s, referencia = %s, peso_vacio = %s, `capacidad (kg)` = %s, propiedad = %s
                        WHERE id = %s AND id_empresa = %s
                    """, (placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, propiedad, vehiculo_id, empresa_id))
                    mysql.connection.commit()
                    flash(f"Vehículo {placa} actualizado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al actualizar vehículo: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            vehiculo_id = request.form.get('vehiculo_id')
            cur = mysql.connection.cursor()
            try:
                cur.execute("DELETE FROM vehiculos WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                mysql.connection.commit()
                flash("Vehículo eliminado de la base de datos.", "success")
            except Exception as e:
                flash("Error al eliminar vehículo.", "danger")
            finally:
                cur.close()

        return redirect(url_for('gestorflota.gestion_vehiculos'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM vehiculos WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='vehiculos', 
        vehiculos=vehiculos_db
    )

@bp_gestorflota.route('/rutas', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_rutas():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'crear':
            nombre_ruta = request.form.get('nombre_ruta', '').strip()
            tipo_ruta = request.form.get('tipo_ruta', '').strip()
            
            if nombre_ruta and tipo_ruta:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        INSERT INTO rutas (empresa, id_empresa, nombre_ruta, tipo_ruta) 
                        VALUES (%s, %s, %s, %s)
                    """, (empresa_nombre, empresa_id, nombre_ruta, tipo_ruta))
                    mysql.connection.commit()
                    flash(f"Ruta '{nombre_ruta}' registrada correctamente.", "success")
                except Exception as e:
                    flash(f"Error al registrar la ruta: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'editar':
            ruta_id = request.form.get('ruta_id')
            nombre_ruta = request.form.get('nombre_ruta', '').strip()
            tipo_ruta = request.form.get('tipo_ruta', '').strip()
            
            if ruta_id and nombre_ruta and tipo_ruta:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        UPDATE rutas 
                        SET nombre_ruta = %s, tipo_ruta = %s
                        WHERE id = %s AND id_empresa = %s
                    """, (nombre_ruta, tipo_ruta, ruta_id, empresa_id))
                    mysql.connection.commit()
                    flash(f"Ruta '{nombre_ruta}' actualizada correctamente.", "success")
                except Exception as e:
                    flash(f"Error al actualizar la ruta: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            ruta_id = request.form.get('ruta_id')
            cur = mysql.connection.cursor()
            try:
                cur.execute("DELETE FROM rutas WHERE id = %s AND id_empresa = %s", (ruta_id, empresa_id))
                mysql.connection.commit()
                flash("Ruta eliminada del sistema.", "success")
            except Exception as e:
                flash("Error al eliminar la ruta.", "danger")
            finally:
                cur.close()

        return redirect(url_for('gestorflota.gestion_rutas'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM rutas WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    rutas_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='rutas', 
        rutas=rutas_db
    )

# =========================================================
# MAPA DE RUTAS Y ANALÍTICA DE PARADAS
# =========================================================
@bp_gestorflota.route('/mapa_rutas', methods=['GET'])
@login_required_custom
@gestor_flota_required
def mapa_rutas():
    empresa_id = session.get('empresa_id')
    
    # Filtros recibidos
    fecha_filtro = request.args.get('fecha', datetime.now().strftime('%Y-%m-%d'))
    placa_filtro = request.args.get('placa', '').upper().strip()

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # 1. Obtener lista de vehículos para el formulario de filtros
    cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()

    puntos_ruta = []
    if placa_filtro:
        # 2. Consultar el historial con LEFT JOIN a paradas para obtener la clasificación
        cur.execute("""
            SELECT vhr.latitud, vhr.longitud, vhr.fecha_hora, vhr.tipo_registro, vhr.nombre_punto, hpf.tipo_actividad
            FROM vehiculos_historial_rutas vhr
            LEFT JOIN historial_paradas_flota hpf 
                ON vhr.id_empresa = hpf.id_empresa 
                AND vhr.placa = hpf.placa 
                AND DATE(vhr.fecha_hora) = hpf.fecha 
                AND TIME(vhr.fecha_hora) = hpf.hora_inicio
            WHERE vhr.id_empresa = %s AND vhr.placa = %s AND DATE(vhr.fecha_hora) = %s
            ORDER BY vhr.fecha_hora ASC
        """, (empresa_id, placa_filtro, fecha_filtro))
        
        # 3. Formatear la fecha para que JSON (y JavaScript en el frontend) la pueda procesar
        for row in cur.fetchall():
            if isinstance(row['fecha_hora'], datetime):
                row['fecha_hora'] = row['fecha_hora'].strftime('%Y-%m-%d %H:%M:%S')
            puntos_ruta.append(row)
            
    cur.close()

    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='mapa_rutas',
        vehiculos=vehiculos_db,
        puntos_ruta=puntos_ruta,
        filtros={'fecha': fecha_filtro, 'placa': placa_filtro}
    )

@bp_gestorflota.route('/qrs')
@login_required_custom
@gestor_flota_required
def generacion_qrs():
    empresa_id = session.get('empresa_id')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, placa, tipo, caja_de_carga, `capacidad (kg)`, referencia FROM vehiculos WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='qrs', 
        vehiculos=vehiculos_db
    )

def _generar_pdf_qrs(vehiculos_list, nit_empresa, nombre_empresa):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=halfLetter)
    width, height = halfLetter

    base_dir = os.path.abspath(os.path.dirname(__file__))
    static_dir = os.path.join(base_dir, '..', 'static')
    logo_cliente_path = os.path.join(static_dir, f'logo_{nit_empresa}.PNG')
    logo_app_path = os.path.join(static_dir, 'logo_energix360.png')

    for v in vehiculos_list:
        placa = str(v['placa']).strip().upper()
        
        qr_data = {
            "placa": placa,
            "nit": str(nit_empresa),
            "empresa": nombre_empresa
        }
        qr_json = json.dumps(qr_data)
        
        qr = qrcode.QRCode(version=1, box_size=10, border=2)
        qr.add_data(qr_json)
        qr.make(fit=True)
        img_qr = qr.make_image(fill_color="#015249", back_color="white") 
        
        qr_buffer = io.BytesIO()
        img_qr.save(qr_buffer, format="PNG")
        qr_buffer.seek(0)
        qr_image_reader = ImageReader(qr_buffer)
        
        if os.path.exists(logo_cliente_path):
            try:
                c.drawImage(logo_cliente_path, width/2 - 1.5*inch, height - 1.5*inch, width=3*inch, height=1*inch, preserveAspectRatio=True, anchor='c')
            except Exception:
                pass
        
        c.setFillColorRGB(0.0039, 0.321, 0.286) 
        c.setFont("Helvetica-Bold", 32)
        c.drawCentredString(width/2, height - 2.2*inch, placa)
        
        c.setFillColorRGB(0.2, 0.2, 0.2)
        c.setFont("Helvetica", 14)
        c.drawCentredString(width/2, height - 2.5*inch, f"Propiedad de: {nombre_empresa}")
        
        qr_size = 3.8 * inch
        c.drawImage(qr_image_reader, width/2 - qr_size/2, height/2 - qr_size/2 - 0.3*inch, width=qr_size, height=qr_size)
        
        if os.path.exists(logo_app_path):
            try:
                c.drawImage(logo_app_path, width/2 - 1*inch, 0.5*inch, width=2*inch, height=0.5*inch, preserveAspectRatio=True, anchor='c')
            except Exception:
                pass
                
        c.showPage()
        
    c.save()
    buffer.seek(0)
    return buffer

@bp_gestorflota.route('/qrs/imprimir/<int:vehiculo_id>')
@login_required_custom
@gestor_flota_required
def imprimir_qr_individual(vehiculo_id):
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa FROM vehiculos WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
    vehiculo = cur.fetchone()
    cur.close()

    if not vehiculo:
        flash("Vehículo no encontrado o no tienes permiso.", "danger")
        return redirect(url_for('gestorflota.generacion_qrs'))

    pdf_buffer = _generar_pdf_qrs([vehiculo], empresa_id, empresa_nombre)
    
    return send_file(
        pdf_buffer, 
        as_attachment=False, 
        download_name=f"QR_{vehiculo['placa']}.pdf", 
        mimetype='application/pdf'
    )

@bp_gestorflota.route('/qrs/imprimir_todos')
@login_required_custom
@gestor_flota_required
def imprimir_todos_qrs():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s", (empresa_id,))
    vehiculos_list = cur.fetchall()
    cur.close()

    if not vehiculos_list:
        flash("No hay vehículos registrados para generar códigos QR.", "warning")
        return redirect(url_for('gestorflota.generacion_qrs'))

    pdf_buffer = _generar_pdf_qrs(vehiculos_list, empresa_id, empresa_nombre)
    
    return send_file(
        pdf_buffer, 
        as_attachment=False, 
        download_name=f"Todos_QRs_{empresa_nombre}.pdf", 
        mimetype='application/pdf'
    )

@bp_gestorflota.route('/operadores', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_operadores():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'crear':
            nombre = request.form.get('nombre', '').strip()
            cedula = request.form.get('cedula', '').strip()
            perfil = request.form.get('perfil', '').strip()
            
            if perfil == 'operador_flotacarga':
                password = request.form.get('password', '').strip()
                if not password:
                    flash("El conductor requiere una contraseña de acceso.", "danger")
                    return redirect(url_for('gestorflota.gestion_operadores'))
                hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
            else:
                hashed_pw = bcrypt.generate_password_hash(os.urandom(12).hex()).decode('utf-8')

            if nombre and cedula and perfil:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
                    if cur.fetchone():
                        flash(f"La identificación {cedula} ya está registrada.", "danger")
                    else:
                        cur.execute("""
                            INSERT INTO usuarios (nombre, cedula, password, perfil, empresa, empresa_id) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (nombre, cedula, hashed_pw, perfil, empresa_nombre, empresa_id))
                        mysql.connection.commit()
                        flash(f"Personal registrado exitosamente: {nombre}.", "success")
                except Exception as e:
                    flash(f"Error al registrar: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'editar':
            operador_id = request.form.get('operador_id')
            nombre = request.form.get('nombre', '').strip()
            cedula = request.form.get('cedula', '').strip()
            perfil = request.form.get('perfil', '').strip()
            
            if operador_id and nombre and cedula and perfil:
                cur = mysql.connection.cursor()
                try:
                    if perfil == 'operador_flotacarga':
                        password = request.form.get('password', '').strip()
                        if password:
                            hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
                            cur.execute("""
                                UPDATE usuarios 
                                SET nombre = %s, cedula = %s, perfil = %s, password = %s
                                WHERE id = %s AND empresa_id = %s
                            """, (nombre, cedula, perfil, hashed_pw, operador_id, empresa_id))
                        else:
                            cur.execute("""
                                UPDATE usuarios 
                                SET nombre = %s, cedula = %s, perfil = %s
                                WHERE id = %s AND empresa_id = %s
                            """, (nombre, cedula, perfil, operador_id, empresa_id))
                    else:
                        cur.execute("""
                            UPDATE usuarios 
                            SET nombre = %s, cedula = %s, perfil = %s
                            WHERE id = %s AND empresa_id = %s
                        """, (nombre, cedula, perfil, operador_id, empresa_id))
                        
                    mysql.connection.commit()
                    flash(f"Registro actualizado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al actualizar: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            operador_id = request.form.get('operador_id')
            cur = mysql.connection.cursor()
            try:
                cur.execute("DELETE FROM usuarios WHERE id = %s AND empresa_id = %s", (operador_id, empresa_id))
                mysql.connection.commit()
                flash("Registro eliminado permanentemente.", "success")
            except Exception as e:
                flash("Error al eliminar.", "danger")
            finally:
                cur.close()

        return redirect(url_for('gestorflota.gestion_operadores'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    # Filtro unificado estricto
    cur.execute("""
        SELECT id, nombre, cedula, perfil 
        FROM usuarios 
        WHERE empresa_id = %s AND perfil = 'operador_flotacarga'
        ORDER BY nombre ASC
    """, (empresa_id,))
    operadores_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='operadores', 
        operadores=operadores_db
    )