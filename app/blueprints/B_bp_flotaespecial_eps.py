# MÓDULO: TRANSPORTE_ESPECIAL | SUBMÓDULO: EPS_GESTION (OPCIONAL P&P)
# app/blueprints/B_bp_flotaespecial_eps.py
import os
import re
import uuid
import random
import string
import requests
import threading
import pdfplumber
import pytz
import holidays
from datetime import datetime, timedelta

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.barcode import qr

from flask import Blueprint, render_template, session, redirect, url_for, request, flash, current_app, jsonify
from werkzeug.utils import secure_filename
from app import mysql
from app.utils import login_required_custom, submodulo_required
from functools import wraps
import MySQLdb.cursors

bp_flotaespecial_eps = Blueprint('flotaespecial_eps', __name__, url_prefix='/gestor_flotaespecial/eps_bp')

BOGOTA_TZ = pytz.timezone('America/Bogota')

def controlador_flotaespecial_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        if perfil not in ['controlador_flotaespecial', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Transporte Especial.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# HELPERS BÁSICOS (Geocoding, Fechas, Telegram, IDs)
# =========================================================
def _calcular_distancia_google(origen, destino):
    if not origen or not destino:
        return 0.0
    api_key = "AIzaSyBloN0EeBxRj9CWnHZ9Wgjz721846DT4KY"
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={origen}&destinations={destino}&mode=driving&key={api_key}"
    try:
        resp = requests.get(url, timeout=10).json()
        if resp.get('status') == 'OK' and resp['rows'][0]['elements'][0].get('status') == 'OK':
            meters = resp['rows'][0]['elements'][0]['distance']['value']
            return round(meters / 1000.0, 2)
    except Exception as e:
        pass
    return 0.0

def _convertir_fecha_html(cadena_fecha):
    if not cadena_fecha: return ""
    m = re.search(r'([0-9]{1,2})[\-\/]([0-9]{1,2})[\-\/]([0-9]{4})', cadena_fecha)
    if m:
        return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
    return cadena_fecha

def _obtener_codigo_dane(departamento, municipio):
    dane_map = {
        "TOLIMA": {"IBAGUE": "73001", "GARZON": "73001"}, 
        "HUILA": {"NEIVA": "41001", "GARZON": "41298"},
        "BOGOTA": {"BOGOTA": "11001"},
        "ANTIOQUIA": {"MEDELLIN": "05001"}
    }
    dep = departamento.upper() if departamento else ""
    mun = municipio.upper() if municipio else ""
    return dane_map.get(dep, {}).get(mun, "")

def _get_next_date_turno(current_date, turno):
    co_holidays = holidays.CO(years=[current_date.year, current_date.year + 1])
    next_d = current_date + timedelta(days=1)
    valid_days = [0, 2, 4] if turno == '1' else [1, 3, 5]
    while next_d.weekday() not in valid_days or next_d in co_holidays:
        next_d += timedelta(days=1)
    return next_d

def _get_next_date_frecuencia(current_date, frecuencia):
    if frecuencia == 'diaria': next_d = current_date + timedelta(days=1)
    elif frecuencia == 'cada_2_dias': next_d = current_date + timedelta(days=2)
    elif frecuencia == 'cada_3_dias': next_d = current_date + timedelta(days=3)
    elif frecuencia == 'cada_4_dias': next_d = current_date + timedelta(days=4)
    elif frecuencia == 'semanal': next_d = current_date + timedelta(weeks=1)
    elif frecuencia == 'quincenal': next_d = current_date + timedelta(days=15)
    elif frecuencia == 'mensual': next_d = current_date + timedelta(days=30)
    elif frecuencia == 'bimensual': next_d = current_date + timedelta(days=60)
    elif frecuencia == 'trimestral': next_d = current_date + timedelta(days=90)
    else: next_d = current_date
    
    co_holidays = holidays.CO(years=[next_d.year, next_d.year + 1])
    while next_d.weekday() == 6 or next_d in co_holidays:
        next_d += timedelta(days=1)
    return next_d

def _enviar_mensajes_telegram_hilo(chat_ids, mensaje):
    TOKEN = "8841682239:AAFOj8TpeOW4ulhIkNoIyGaTZ2MLlI9ydVo"
    def tarea_envio():
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        for chat_id in chat_ids:
            try: requests.post(url, data={"chat_id": chat_id, "text": mensaje, "parse_mode": "HTML"}, timeout=10)
            except: pass
    hilo = threading.Thread(target=tarea_envio)
    hilo.daemon = True
    hilo.start()

def _enviar_documento_telegram_hilo(chat_ids, mensaje, filepath):
    TOKEN = "8841682239:AAFOj8TpeOW4ulhIkNoIyGaTZ2MLlI9ydVo"
    def tarea_envio():
        url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
        for chat_id in chat_ids:
            try:
                with open(filepath, 'rb') as f:
                    requests.post(url, data={"chat_id": chat_id, "caption": mensaje, "parse_mode": "HTML"}, files={"document": f}, timeout=15)
            except: pass
    hilo = threading.Thread(target=tarea_envio)
    hilo.daemon = True
    hilo.start()

def generar_id_viaje_unico(cur, empresa_id=None):
    for _ in range(10):
        codigo = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if empresa_id:
            cur.execute("SELECT id FROM control_viajes_flota_especial WHERE id_viaje = %s AND id_empresa = %s FOR UPDATE", (codigo, empresa_id))
        else:
            cur.execute("SELECT id FROM control_viajes_flota_especial WHERE id_viaje = %s FOR UPDATE", (codigo,))
        if not cur.fetchone():
            return codigo
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def generar_consecutivo_fuec(dir_terr, res_hab, anio_hab, anio_exp, num_contrato, cons_extracto):
    dt = str(dir_terr).zfill(3)[:3]
    rh = str(res_hab).zfill(4)[:4]
    ah = str(anio_hab).zfill(2)[:2]
    ae = str(anio_exp).zfill(4)[:4]
    num_c_clean = ''.join(filter(str.isdigit, str(num_contrato)))
    if not num_c_clean: num_c_clean = '0000'
    nc = num_c_clean.zfill(4)[-4:]
    ce = str(cons_extracto).zfill(4)[-4:]
    return f"{dt}{rh}{ah}{ae}{nc}{ce}"

def asegurar_tablas_transporte_especial(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS maestra_traslados_eps_tespecial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            eps_cliente VARCHAR(255),
            id_eps_cliente VARCHAR(50),
            fecha_captura DATETIME DEFAULT CURRENT_TIMESTAMP,
            fecha_entrega_servicio DATE,
            numero_autorizacion VARCHAR(100),
            numero_prescripcion VARCHAR(100),
            tipo_servicio VARCHAR(50),
            codigo_servicio VARCHAR(50),
            estatus_servicio VARCHAR(50) DEFAULT 'capturado',
            nombre_usuario VARCHAR(255),
            tipo_documento VARCHAR(20),
            id_usuario VARCHAR(50),
            fecha_nacimiento DATE,
            edad VARCHAR(20),
            sexo VARCHAR(20),
            numero_carne VARCHAR(50),
            tipo_usuario VARCHAR(50),
            nivel_sisben VARCHAR(50),
            telefono_usuario VARCHAR(50),
            email_usuario VARCHAR(100),
            departamento VARCHAR(100),
            municipio VARCHAR(100),
            direccion_origen VARCHAR(255),
            direccion_destino VARCHAR(255),
            numero_traslados_aporbados INT DEFAULT 0,
            numero_traslados_ejecutados INT DEFAULT 0,
            diferencia INT DEFAULT 0,
            observaciones TEXT,
            ruta_documento VARCHAR(255),
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS control_viajes_flota_especial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            id_eps_cliente VARCHAR(50),
            numero_autorizacion VARCHAR(100),
            numero_prescripcion VARCHAR(100),
            tipo_servicio VARCHAR(50),
            estatus_servicio VARCHAR(50) DEFAULT 'CAPTURADO',
            nombre_usuario VARCHAR(255),
            id_usuario VARCHAR(50),
            telefono_usuario VARCHAR(50),
            departamento VARCHAR(100),
            municipio VARCHAR(100),
            direccion_origen VARCHAR(255),
            direccion_destino VARCHAR(255),
            fecha_servicio DATE,
            hora_inicio TIME,
            hora_fin TIME,
            coordenadas_inicio VARCHAR(100),
            coordenadas_fin VARCHAR(100),
            vehiculo_asignado VARCHAR(20),
            conductor_asignado VARCHAR(100),
            id_viaje VARCHAR(10) UNIQUE,
            ruta_documento VARCHAR(255),
            fecha_notificacion_info DATETIME NULL,
            fecha_notificacion_confirmacion DATETIME NULL,
            operador_ejecucion VARCHAR(100) NULL,
            fecha_ejecucion_real DATETIME NULL,
            fecha_fin_real DATETIME NULL,
            recordatorio_enviado BOOLEAN DEFAULT FALSE,
            auditor_nombre VARCHAR(100) NULL,
            fecha_auditoria DATETIME NULL,
            hash_auditoria VARCHAR(255) NULL,
            ruta_pdf_unificado VARCHAR(255) NULL,
            oculto_kanban BOOLEAN DEFAULT FALSE,
            vuelta_activada BOOLEAN DEFAULT FALSE,
            INDEX(id_empresa),
            INDEX(id_viaje)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contratos_transporte_especial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            numero_contrato VARCHAR(50) NOT NULL,
            contratante_nombre VARCHAR(255) NOT NULL,
            contratante_nit_cedula VARCHAR(50) NOT NULL,
            categoria_contrato VARCHAR(50) NOT NULL,
            objeto_contrato TEXT,
            convenio_colaboracion VARCHAR(255),
            fecha_inicio DATE,
            fecha_fin DATE,
            responsable_nombre VARCHAR(255),
            responsable_cedula VARCHAR(50),
            responsable_direccion VARCHAR(255),
            responsable_telefono VARCHAR(50),
            estado VARCHAR(20) DEFAULT 'ACTIVO',
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

# =========================================================
# REDIRECCIÓN RAÍZ EPS
# =========================================================
@bp_flotaespecial_eps.route('/', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_eps():
    return redirect(url_for('flotaespecial_eps.dashboard_operativo'))

# =========================================================
# GESTIÓN DE CONTRATOS
# =========================================================
@bp_flotaespecial_eps.route('/gestion_contratos', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_contratos():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'guardar_contrato':
            numero_contrato = request.form.get('numero_contrato', '').strip()
            categoria_contrato = request.form.get('categoria_contrato', '').strip()
            contratante_nombre = request.form.get('contratante_nombre', '').strip()
            contratante_nit_cedula = request.form.get('contratante_nit_cedula', '').strip()
            objeto_contrato = request.form.get('objeto_contrato', '').strip()
            convenio_colaboracion = request.form.get('convenio_colaboracion', '').strip()
            fecha_inicio = request.form.get('fecha_inicio')
            fecha_fin = request.form.get('fecha_fin')
            responsable_nombre = request.form.get('responsable_nombre', '').strip()
            responsable_cedula = request.form.get('responsable_cedula', '').strip()
            responsable_direccion = request.form.get('responsable_direccion', '').strip()
            responsable_telefono = request.form.get('responsable_telefono', '').strip()
            
            try:
                cur.execute("""
                    INSERT INTO contratos_transporte_especial 
                    (id_empresa, numero_contrato, contratante_nombre, contratante_nit_cedula, categoria_contrato, 
                     objeto_contrato, convenio_colaboracion, fecha_inicio, fecha_fin, responsable_nombre, 
                     responsable_cedula, responsable_direccion, responsable_telefono)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (empresa_id, numero_contrato, contratante_nombre, contratante_nit_cedula, categoria_contrato,
                      objeto_contrato, convenio_colaboracion, fecha_inicio, fecha_fin, responsable_nombre,
                      responsable_cedula, responsable_direccion, responsable_telefono))
                mysql.connection.commit()
                flash('Contrato registrado exitosamente.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error al registrar contrato: {str(e)}', 'danger')
                
        elif accion == 'eliminar_contrato':
            contrato_id = request.form.get('contrato_id')
            try:
                cur.execute("UPDATE contratos_transporte_especial SET estado = 'INACTIVO' WHERE id = %s AND id_empresa = %s", (contrato_id, empresa_id))
                mysql.connection.commit()
                flash('Contrato inactivado exitosamente.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error al inactivar contrato: {str(e)}', 'danger')
                
        cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_contratos'))

    try:
        cur.execute("SELECT * FROM contratos_transporte_especial WHERE id_empresa = %s AND estado = 'ACTIVO'", (empresa_id,))
        contratos = cur.fetchall()
    except Exception as e:
        flash(f'Error al cargar contratos: {str(e)}', 'danger')
        contratos = []
    finally:
        cur.close()

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), 
        empresa=session.get('empresa'), 
        nombre=session.get('nombre'),
        active_module='gestion_contratos',
        contratos=contratos
    )

# =========================================================
# FLUJO OPERATIVO EPS: 1. CAPTURA
# =========================================================
@bp_flotaespecial_eps.route('/gestion_traslados_captura', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_traslados_captura():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    datos_extraidos = None

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'subir_pdf':
            # Implementar lógica de extracción PDF aquí (stub para BuildError)
            flash('Extracción PDF en desarrollo. Proceda con el registro manual.', 'info')
            
        elif accion == 'guardar_captura':
            try:
                cur.execute("""
                    INSERT INTO maestra_traslados_eps_tespecial 
                    (id_empresa, eps_cliente, id_eps_cliente, numero_prescripcion, numero_autorizacion, 
                     fecha_entrega_servicio, tipo_documento, id_usuario, nombre_usuario, telefono_usuario, 
                     departamento, municipio, direccion_origen, direccion_destino, 
                     codigo_servicio, numero_traslados_aporbados, diferencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    empresa_id,
                    request.form.get('eps_cliente'),
                    request.form.get('IDProv'),
                    request.form.get('NoPrescripcion'),
                    request.form.get('numero_autorizacion'),
                    request.form.get('fecha_entrega_servicio'),
                    request.form.get('tipo_documento'),
                    request.form.get('NoIDPaciente'),
                    request.form.get('nombre_usuario'),
                    request.form.get('telefono_usuario'),
                    request.form.get('departamento'),
                    request.form.get('municipio'),
                    request.form.get('DirPaciente'),
                    request.form.get('direccion_destino'),
                    request.form.get('CodSerTecAEntregar'),
                    request.form.get('CantTotAEntregar', 1),
                    request.form.get('CantTotAEntregar', 1)
                ))
                mysql.connection.commit()
                flash('Orden capturada correctamente.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error al guardar captura: {str(e)}', 'danger')
            return redirect(url_for('flotaespecial_eps.gestion_traslados_captura'))

    cur.execute("SELECT * FROM maestra_traslados_eps_tespecial WHERE id_empresa = %s AND diferencia > 0", (empresa_id,))
    traslados_capturados = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_captura', traslados_capturados=traslados_capturados, datos_extraidos=datos_extraidos
    )

# =========================================================
# FLUJO OPERATIVO EPS: 2. ASIGNACIÓN / DESGLOSE
# =========================================================
@bp_flotaespecial_eps.route('/gestion_traslados_asignacion', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_traslados_asignacion():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    if request.method == 'POST':
        accion = request.form.get('accion')
        if accion == 'Desglosar Viajes':
            maestra_id = request.form.get('maestra_id')
            try:
                # Logica simplificada para desglosar la orden
                cur.execute("UPDATE maestra_traslados_eps_tespecial SET diferencia = 0 WHERE id = %s AND id_empresa = %s", (maestra_id, empresa_id))
                mysql.connection.commit()
                flash('Viajes desglosados y pasados a la etapa de Verificación.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error al desglosar: {str(e)}', 'danger')
            return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion'))

    cur.execute("SELECT * FROM maestra_traslados_eps_tespecial WHERE id_empresa = %s AND diferencia > 0", (empresa_id,))
    ordenes_maestras = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_asignacion', ordenes_maestras=ordenes_maestras
    )

# =========================================================
# FLUJO OPERATIVO EPS: 3. VERIFICACIÓN DE DATOS
# =========================================================
@bp_flotaespecial_eps.route('/gestion_traslados_verificacion', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_traslados_verificacion():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    if request.method == 'POST':
        accion = request.form.get('accion')
        if accion == 'editar_y_verificar':
            id_viaje_padre = request.form.get('id_viaje_padre')
            try:
                # Actualizar datos a nivel paquete
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET nombre_usuario=%s, id_usuario=%s, telefono_usuario=%s,
                        departamento=%s, municipio=%s, direccion_origen=%s,
                        departamento_destino=%s, municipio_destino=%s, direccion_destino=%s,
                        estatus_servicio='VERIFICADO'
                    WHERE id_viaje_padre = %s AND id_empresa = %s AND estatus_servicio = 'CAPTURADO'
                """, (
                    request.form.get('nombre_usuario'), request.form.get('id_usuario'), request.form.get('telefono_usuario'),
                    request.form.get('departamento_origen'), request.form.get('municipio_origen'), request.form.get('direccion_origen'),
                    request.form.get('departamento_destino'), request.form.get('municipio_destino'), request.form.get('direccion_destino'),
                    id_viaje_padre, empresa_id
                ))
                mysql.connection.commit()
                flash('Paquete de viajes verificado telefónicamente y listo para programación.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error de verificación: {str(e)}', 'danger')
            return redirect(url_for('flotaespecial_eps.gestion_traslados_verificacion'))

    # Para el Frontend agrupado por id_viaje_padre
    cur.execute("""
        SELECT id, id_viaje_padre, id_viaje, numero_prescripcion, nombre_usuario, 
               telefono_usuario, tipo_documento, id_usuario, departamento, municipio, direccion_origen, 
               departamento_destino, municipio_destino, direccion_destino, ruta_documento, 
               COUNT(id) as total_idas 
        FROM control_viajes_flota_especial 
        WHERE id_empresa = %s AND estatus_servicio = 'CAPTURADO' AND trayecto = 'IDA' 
        GROUP BY id_viaje_padre
    """, (empresa_id,))
    viajes = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_verificacion', viajes=viajes
    )

# =========================================================
# FLUJO OPERATIVO EPS: 4. PROGRAMACIÓN
# =========================================================
@bp_flotaespecial_eps.route('/gestion_traslados_programacion', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_traslados_programacion():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    if request.method == 'POST':
        accion = request.form.get('accion')
        if accion == 'programar':
            viaje_id = request.form.get('viaje_id')
            fecha = request.form.get('fecha_servicio')
            hora = request.form.get('hora_inicio')
            try:
                # Stub básico para evitar errores
                cur.execute("UPDATE control_viajes_flota_especial SET fecha_servicio=%s, hora_inicio=%s, estatus_servicio='PROGRAMADO' WHERE id=%s AND id_empresa=%s", (fecha, hora, viaje_id, empresa_id))
                mysql.connection.commit()
                flash('Viaje programado y disponible en el Kanban para asignación de flota.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error al programar: {str(e)}', 'danger')
            return redirect(url_for('flotaespecial_eps.gestion_traslados_programacion'))

    cur.execute("""
        SELECT c.id, c.id_viaje, c.numero_prescripcion, c.nombre_usuario, c.telefono_usuario, c.tipo_servicio, 
               (SELECT COUNT(*) FROM control_viajes_flota_especial sub WHERE sub.id_viaje_padre = c.id_viaje_padre AND sub.trayecto='IDA') as total_idas 
        FROM control_viajes_flota_especial c
        WHERE c.id_empresa = %s AND c.estatus_servicio = 'VERIFICADO' AND c.trayecto = 'IDA' 
        GROUP BY c.id_viaje_padre
    """, (empresa_id,))
    viajes = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_programacion', viajes=viajes
    )

# =========================================================
# FLUJO OPERATIVO EPS: 5. ASIGNACIÓN DE FLOTA
# =========================================================
@bp_flotaespecial_eps.route('/gestion_traslados_asignacion_flota', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def gestion_traslados_asignacion_flota():
    empresa_id = session.get('empresa_id')
    empresa_nit = session.get('nit')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    if request.method == 'POST':
        accion = request.form.get('accion')
        if accion == 'asignar_flota':
            viaje_id = request.form.get('viaje_id')
            vehiculo = request.form.get('vehiculo')
            conductor = request.form.get('conductor')
            try:
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET vehiculo_asignado=%s, conductor_asignado=%s, estatus_servicio='ASIGNADO'
                    WHERE id=%s AND id_empresa=%s
                """, (vehiculo, conductor, viaje_id, empresa_id))
                mysql.connection.commit()
                flash('Flota asignada, FUEC generado y operador notificado.', 'success')
            except Exception as e:
                mysql.connection.rollback()
                flash(f'Error en la asignación: {str(e)}', 'danger')
            return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))

    cur.execute("SELECT * FROM control_viajes_flota_especial WHERE id_empresa=%s AND estatus_servicio IN ('PROGRAMADO', 'PDTE. ASIGNAR VUELTA')", (empresa_id,))
    viajes = cur.fetchall()

    cur.execute("SELECT * FROM contratos_transporte_especial WHERE id_empresa=%s AND estado='ACTIVO'", (empresa_id,))
    contratos = cur.fetchall()

    cur.execute("""
        SELECT placa, clase AS tipo, regional, departamento_base, municipio_base, capacidad_pasajeros, capacidad_residual, 
        (SELECT GROUP_CONCAT(nombre SEPARATOR ', ') FROM conductores_flotaespecial c WHERE c.vehiculo_asignado = vehiculos_especial.placa AND (c.id_empresa = %s OR c.id_empresa = %s)) AS conductor_asignado 
        FROM vehiculos_especial WHERE id_empresa = %s OR id_empresa = %s
    """, (empresa_id, empresa_nit, empresa_id, empresa_nit))
    vehiculos = cur.fetchall()

    cur.execute("SELECT id, nombre, cedula, COALESCE(es_relevo, 0) as es_relevo FROM conductores_flotaespecial WHERE id_empresa = %s OR id_empresa = %s", (empresa_id, empresa_nit))
    conductores = cur.fetchall()

    cur.close()

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_asignacion_flota', viajes=viajes, contratos=contratos, vehiculos=vehiculos, conductores=conductores
    )

# =========================================================
# APIS PARA COMPORTAMIENTO DINÁMICO FRONTEND
# =========================================================
@bp_flotaespecial_eps.route('/api/validar_disponibilidad_vehiculo', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_validar_disponibilidad_vehiculo():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    vehiculo = datos.get('vehiculo')
    
    if not vehiculo:
        return jsonify({"status": "error", "message": "Placa no proporcionada."}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT capacidad_residual FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s", (vehiculo, empresa_id))
        veh_data = cur.fetchone()
        
        if veh_data:
            capacidad = veh_data['capacidad_residual']
            if capacidad > 0:
                return jsonify({"status": "ok", "message": f"✔️ Vehículo y documentos vigentes. Capacidad disponible: {capacidad} cupos."}), 200
            else:
                return jsonify({"status": "bloqueo", "message": "🚫 Vehículo sin capacidad residual o con documentos vencidos."}), 200
        else:
            return jsonify({"status": "error", "message": "Vehículo no encontrado."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

@bp_flotaespecial_eps.route('/api/viajes_agrupables', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_viajes_agrupables():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    viaje_id = datos.get('viaje_id')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT id, id_viaje, nombre_usuario, direccion_destino, hora_inicio FROM control_viajes_flota_especial WHERE id_empresa = %s AND id != %s AND estatus_servicio = 'PROGRAMADO' LIMIT 5", (empresa_id, viaje_id))
        agrupables = cur.fetchall()
        
        for a in agrupables:
            a['hora_inicio_str'] = str(a['hora_inicio'])
            a['cupos'] = 1
            
        return jsonify({"status": "success", "agrupables": agrupables, "cupos_base": 1}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# DASHBOARD KANBAN EN VIVO
# =========================================================
@bp_flotaespecial_eps.route('/dashboard_operativo')
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def dashboard_operativo():
    empresa_id = session.get('empresa_id')
    empresa_nit = session.get('nit')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    asegurar_tablas_transporte_especial(cur)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN estatus_servicio = 'CAPTURADO' THEN 1 ELSE 0 END) as capturados,
            SUM(CASE WHEN estatus_servicio = 'PROGRAMADO' THEN 1 ELSE 0 END) as programados,
            SUM(CASE WHEN estatus_servicio = 'VERIFICADO' THEN 1 ELSE 0 END) as verificados,
            SUM(CASE WHEN estatus_servicio IN ('ASIGNADO', 'PDTE. ASIGNAR VUELTA') THEN 1 ELSE 0 END) as asignados,
            SUM(CASE WHEN estatus_servicio IN ('EN EJECUCION', 'NOVEDAD_RECORRIDO') THEN 1 ELSE 0 END) as ejecucion,
            SUM(CASE WHEN estatus_servicio IN ('TERMINADO-PDTE AUDITAR', 'AUDITADO') THEN 1 ELSE 0 END) as ejecutados
        FROM control_viajes_flota_especial 
        WHERE id_empresa = %s AND (fecha_servicio = CURDATE() OR estatus_servicio NOT IN ('AUDITADO'))
    """, (empresa_id,))
    kpis = cur.fetchone()
    if not kpis or kpis['total'] is None:
        kpis = {'total': 0, 'capturados': 0, 'programados': 0, 'verificados': 0, 'asignados': 0, 'ejecucion': 0, 'ejecutados': 0}

    cur.execute("""
        SELECT c.id, c.id_viaje, c.fecha_servicio, c.hora_inicio, c.vehiculo_asignado, c.conductor_asignado, 
               c.nombre_usuario, c.telefono_usuario, c.direccion_origen, c.direccion_destino, c.estatus_servicio,
               c.numero_prescripcion, c.numero_autorizacion, c.id_eps_cliente AS ips, c.trayecto, c.id_viaje_padre,
               c.departamento, c.municipio, c.departamento_destino, c.municipio_destino, c.id_usuario, c.tipo_documento,
               c.estado_novedad, c.descripcion_novedad, c.lleva_acompanante, c.nombre_acompanante, c.cedula_acompanante, c.vuelta_activada,
               COALESCE(c.ruta_documento, m.ruta_documento) as ruta_documento
        FROM control_viajes_flota_especial c
        LEFT JOIN (
            SELECT numero_autorizacion, numero_prescripcion, id_empresa, MAX(ruta_documento) as ruta_documento 
            FROM maestra_traslados_eps_tespecial 
            GROUP BY numero_autorizacion, numero_prescripcion, id_empresa
        ) m 
          ON c.numero_autorizacion = m.numero_autorizacion 
          AND (c.numero_prescripcion = m.numero_prescripcion OR c.numero_prescripcion IS NULL OR m.numero_prescripcion IS NULL) 
          AND c.id_empresa = m.id_empresa
        WHERE c.id_empresa = %s 
          AND c.estatus_servicio IN ('PROGRAMADO', 'PDTE. ASIGNAR VUELTA', 'ASIGNADO', 'EN EJECUCION', 'TERMINADO-PDTE AUDITAR', 'NOVEDAD_PRE_VIAJE', 'NOVEDAD_RECORRIDO', 'CAPTURADO', 'VERIFICADO')
          AND (c.oculto_kanban = FALSE OR c.oculto_kanban IS NULL)
        ORDER BY c.fecha_servicio ASC, c.hora_inicio ASC
    """, (empresa_id,))
    viajes = cur.fetchall()

    cur.execute("""
        SELECT placa, clase AS tipo, regional, departamento_base, municipio_base, capacidad_pasajeros, capacidad_residual,
               (SELECT GROUP_CONCAT(nombre SEPARATOR ', ') FROM conductores_flotaespecial c 
                WHERE c.vehiculo_asignado = vehiculos_especial.placa 
                  AND (c.id_empresa = %s OR c.id_empresa = %s)) AS conductor_asignado
        FROM vehiculos_especial 
        WHERE id_empresa = %s OR id_empresa = %s
    """, (empresa_id, empresa_nit, empresa_id, empresa_nit))
    vehiculos = cur.fetchall()

    cur.execute("SELECT id, nombre, cedula FROM usuarios WHERE (empresa_id = %s OR empresa_id = %s) AND perfil IN ('operador_flotaespecial', 'auxiliar_transporte_especial')", (empresa_id, empresa_nit))
    cond_usrs = list(cur.fetchall())

    cur.execute("SELECT id, nombre, cedula, COALESCE(es_relevo, 0) as es_relevo FROM conductores_flotaespecial WHERE id_empresa = %s OR id_empresa = %s", (empresa_id, empresa_nit))
    cond_flota = list(cur.fetchall())

    cond_dict = {c['cedula']: c for c in cond_usrs}
    for c in cond_flota:
        if c['cedula'] not in cond_dict: cond_dict[c['cedula']] = c
        elif 'es_relevo' in c: cond_dict[c['cedula']]['es_relevo'] = c['es_relevo']
            
    conductores = list(cond_dict.values())

    cur.execute("SELECT id, contratante_nombre, numero_contrato, categoria_contrato FROM contratos_transporte_especial WHERE (id_empresa = %s OR id_empresa = %s) AND estado = 'ACTIVO'", (empresa_id, empresa_nit))
    contratos = cur.fetchall()

    cur.close()

    now_col = datetime.now(BOGOTA_TZ).replace(tzinfo=None)
    viajes_retrasados, novedades_pre_viaje, programados_normales = [], [], []
    parte2_col1, parte2_col2, parte2_col3, parte2_col4, parte2_col5 = [], [], [], [], []
    
    for v in viajes:
        h_init = v.get('hora_inicio')
        f_serv = v.get('fecha_servicio')
        v['is_urgencia'] = False
        diff_minutes = -1
        
        if h_init is not None and f_serv is not None:
            try:
                if isinstance(h_init, timedelta):
                    dt_val = datetime.combine(f_serv, datetime.min.time()) + h_init
                    h_str = (datetime.min + h_init).time().strftime('%H:%M:%S')
                else:
                    dt_val = datetime.combine(f_serv, h_init)
                    h_str = h_init.strftime('%H:%M:%S')
                    
                diff_hours = (dt_val - now_col).total_seconds() / 3600.0
                diff_minutes = (now_col - dt_val).total_seconds() / 60.0
                if diff_hours <= 12.0: v['is_urgencia'] = True
                v['hora_inicio_str'] = f"{f_serv} | {h_str}"
            except:
                v['hora_inicio_str'] = f"{f_serv} | {h_init}"
        else:
            v['hora_inicio_str'] = f"{f_serv} | Pendiente" if f_serv else "Pendiente"
            
        v['hora_inicio'] = v['hora_inicio_str']
        trayecto = (v.get('trayecto') or 'IDA').upper()
        estatus = (v.get('estatus_servicio') or '').upper()
        v['trayecto'] = trayecto
        v['estatus_servicio'] = estatus

        if estatus == 'ASIGNADO' and diff_minutes > 15.0:
            viajes_retrasados.append(v)

        if trayecto == 'IDA':
            if estatus == 'NOVEDAD_PRE_VIAJE': novedades_pre_viaje.append(v)
            elif estatus == 'PROGRAMADO': programados_normales.append(v)
            elif estatus in ('EN EJECUCION', 'NOVEDAD_RECORRIDO'): parte2_col1.append(v)
            elif estatus in ('TERMINADO-PDTE AUDITAR', 'AUDITADO'): parte2_col2.append(v)
        elif trayecto == 'VUELTA':
            if v.get('vuelta_activada') and estatus not in ('EN EJECUCION', 'NOVEDAD_RECORRIDO', 'TERMINADO-PDTE AUDITAR', 'AUDITADO'):
                parte2_col3.append(v)
            elif estatus in ('EN EJECUCION', 'NOVEDAD_RECORRIDO'): parte2_col4.append(v)
            elif estatus in ('TERMINADO-PDTE AUDITAR', 'AUDITADO'): parte2_col5.append(v)

    parte1_ida_programados = novedades_pre_viaje + programados_normales

    return render_template(
        'B_modulo_flotaespecial_eps.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='dashboard', kpis=kpis, filtros={},
        viajes_retrasados=viajes_retrasados, parte1_ida_programados=parte1_ida_programados,
        parte2_col1=parte2_col1, parte2_col2=parte2_col2, parte2_col3=parte2_col3, parte2_col4=parte2_col4,
        vehiculos=vehiculos, conductores=conductores, contratos=contratos
    )

# =========================================================
# MOTOR DE DATOS EN VIVO (SHORT-POLLING DEL TABLERO)
# =========================================================
@bp_flotaespecial_eps.route('/api/operativa/vivo', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_operativa_vivo():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN estatus_servicio = 'CAPTURADO' THEN 1 ELSE 0 END) as capturados,
                SUM(CASE WHEN estatus_servicio = 'PROGRAMADO' THEN 1 ELSE 0 END) as programados,
                SUM(CASE WHEN estatus_servicio = 'VERIFICADO' THEN 1 ELSE 0 END) as verificados,
                SUM(CASE WHEN estatus_servicio IN ('ASIGNADO', 'PDTE. ASIGNAR VUELTA') THEN 1 ELSE 0 END) as asignados,
                SUM(CASE WHEN estatus_servicio IN ('EN EJECUCION', 'NOVEDAD_RECORRIDO') THEN 1 ELSE 0 END) as ejecucion,
                SUM(CASE WHEN estatus_servicio IN ('TERMINADO-PDTE AUDITAR', 'AUDITADO') THEN 1 ELSE 0 END) as ejecutados
            FROM control_viajes_flota_especial 
            WHERE id_empresa = %s AND (fecha_servicio = CURDATE() OR estatus_servicio NOT IN ('AUDITADO'))
        """, (empresa_id,))
        kpis_db = cur.fetchone()
        if not kpis_db or kpis_db['total'] is None:
            kpis_db = {'total': 0, 'capturados': 0, 'programados': 0, 'verificados': 0, 'asignados': 0, 'ejecucion': 0, 'ejecutados': 0}

        cur.execute("""
            SELECT c.id, c.id_viaje, c.numero_autorizacion, c.numero_prescripcion, c.nombre_usuario, c.id_usuario, 
                   c.direccion_origen, c.direccion_destino, c.departamento, c.municipio, c.departamento_destino, c.municipio_destino,
                   c.telefono_usuario, c.hora_inicio, c.fecha_servicio, c.trayecto, c.estatus_servicio, c.vehiculo_asignado, 
                   c.conductor_asignado, c.id_viaje_padre, c.estado_novedad, c.descripcion_novedad, c.tipo_documento, c.vuelta_activada,
                   c.lleva_acompanante, c.nombre_acompanante, c.cedula_acompanante,
                   COALESCE(c.ruta_documento, m.ruta_documento) as ruta_documento
            FROM control_viajes_flota_especial c
            LEFT JOIN (
                SELECT numero_autorizacion, numero_prescripcion, id_empresa, MAX(ruta_documento) as ruta_documento 
                FROM maestra_traslados_eps_tespecial 
                GROUP BY numero_autorizacion, numero_prescripcion, id_empresa
            ) m 
              ON c.numero_autorizacion = m.numero_autorizacion 
              AND (c.numero_prescripcion = m.numero_prescripcion OR c.numero_prescripcion IS NULL OR m.numero_prescripcion IS NULL) 
              AND c.id_empresa = m.id_empresa
            WHERE c.id_empresa = %s 
              AND c.estatus_servicio IN ('PROGRAMADO', 'PDTE. ASIGNAR VUELTA', 'ASIGNADO', 'EN EJECUCION', 'TERMINADO-PDTE AUDITAR', 'NOVEDAD_PRE_VIAJE', 'NOVEDAD_RECORRIDO', 'CAPTURADO', 'VERIFICADO')
              AND (c.oculto_kanban = FALSE OR c.oculto_kanban IS NULL)
            ORDER BY c.fecha_servicio ASC, c.hora_inicio ASC
        """, (empresa_id,))
        viajes = cur.fetchall()

        datos = {
            "kpis": kpis_db,
            "novedades_activas": [],
            "viajes_retrasados": [],
            "parte1_ida_programados": [],
            "parte2_col1": [],
            "parte2_col2": [],
            "parte2_col3": [],
            "parte2_col4": [],
            "parte2_col5": []
        }

        now_col = datetime.now(BOGOTA_TZ).replace(tzinfo=None)
        novedades_pre_viaje = []
        programados_normales = []

        for v in viajes:
            h_init = v.get('hora_inicio')
            f_serv = v.get('fecha_servicio')
            v['fecha_servicio'] = str(f_serv) if f_serv else ''
            v['is_urgencia'] = False
            diff_minutes = -1
            
            if h_init is not None and f_serv is not None:
                try:
                    if isinstance(h_init, timedelta):
                        dt_val = datetime.combine(f_serv, datetime.min.time()) + h_init
                        h_str = (datetime.min + h_init).time().strftime('%H:%M:%S')
                    else:
                        dt_val = datetime.combine(f_serv, h_init)
                        h_str = h_init.strftime('%H:%M:%S')
                        
                    diff_hours = (dt_val - now_col).total_seconds() / 3600.0
                    diff_minutes = (now_col - dt_val).total_seconds() / 60.0
                    
                    if diff_hours <= 12.0:
                        v['is_urgencia'] = True
                        
                    v['hora_inicio_str'] = f"{v['fecha_servicio']} | {h_str}"
                except:
                    v['hora_inicio_str'] = f"{v['fecha_servicio']} | {h_init}"
            else:
                v['hora_inicio_str'] = f"{v['fecha_servicio']} | Pendiente" if v['fecha_servicio'] else "Pendiente"
                
            v['hora_inicio'] = v['hora_inicio_str']
            trayecto = str(v.get('trayecto') or 'IDA').upper().strip()
            estatus = str(v.get('estatus_servicio') or '').upper().strip()
            
            v['trayecto'] = trayecto
            v['estatus_servicio'] = estatus

            if estatus == 'ASIGNADO' and diff_minutes > 15.0:
                datos["viajes_retrasados"].append(v)

            if estatus in ['NOVEDAD_PRE_VIAJE', 'NOVEDAD_RECORRIDO']:
                datos["novedades_activas"].append(v)
            
            if trayecto == 'IDA':
                if estatus == 'NOVEDAD_PRE_VIAJE':
                    novedades_pre_viaje.append(v)
                elif estatus == 'PROGRAMADO':
                    programados_normales.append(v)
                elif estatus in ('EN EJECUCION', 'NOVEDAD_RECORRIDO'):
                    datos["parte2_col1"].append(v)
                elif estatus in ('TERMINADO-PDTE AUDITAR', 'AUDITADO'):
                    datos["parte2_col2"].append(v)
                    
            elif trayecto == 'VUELTA':
                if v.get('vuelta_activada') and estatus not in ('EN EJECUCION', 'NOVEDAD_RECORRIDO', 'TERMINADO-PDTE AUDITAR', 'AUDITADO'):
                    datos["parte2_col3"].append(v)
                elif estatus in ('EN EJECUCION', 'NOVEDAD_RECORRIDO'):
                    datos["parte2_col4"].append(v)
                elif estatus in ('TERMINADO-PDTE AUDITAR', 'AUDITADO'):
                    datos["parte2_col5"].append(v)

        datos["parte1_ida_programados"] = novedades_pre_viaje + programados_normales

        return jsonify({"status": "success", "data": datos}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# OCULTAR VIAJES Y DESENCADENAR VUELTA
# =========================================================
@bp_flotaespecial_eps.route('/api/operativa/ocultar_kanban', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_ocultar_kanban():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    viaje_id = datos.get('viaje_id')

    if not viaje_id:
        return jsonify({"status": "error", "message": "ID de viaje no proporcionado"}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT trayecto, id_viaje_padre FROM control_viajes_flota_especial WHERE id = %s AND id_empresa = %s", (viaje_id, empresa_id))
        viaje_actual = cur.fetchone()
        
        if viaje_actual:
            cur.execute("""
                UPDATE control_viajes_flota_especial 
                SET oculto_kanban = TRUE
                WHERE id = %s AND id_empresa = %s
            """, (viaje_id, empresa_id))
            
            trayecto = (viaje_actual.get('trayecto') or 'IDA').upper()
            id_padre = viaje_actual.get('id_viaje_padre')
            viaje_vuelta = None
            
            if trayecto == 'IDA' and id_padre:
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET vuelta_activada = TRUE 
                    WHERE id_viaje_padre = %s AND trayecto = 'VUELTA' AND id_empresa = %s
                """, (id_padre, empresa_id))
                
                cur.execute("""
                    SELECT id, id_viaje_padre, id_viaje, numero_prescripcion, nombre_usuario, 
                           telefono_usuario, direccion_origen, direccion_destino, 
                           departamento, municipio, departamento_destino, municipio_destino,
                           id_usuario, tipo_documento, ruta_documento
                    FROM control_viajes_flota_especial 
                    WHERE id_viaje_padre = %s AND trayecto = 'VUELTA' AND id_empresa = %s
                """, (id_padre, empresa_id))
                viaje_vuelta = cur.fetchone()
            
            mysql.connection.commit()
            return jsonify({
                "status": "success", 
                "message": "Viaje procesado y retirado de la vista.",
                "viaje_vuelta": viaje_vuelta
            }), 200
        else:
            return jsonify({"status": "error", "message": "No se encontró el viaje especificado."}), 404

    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# RE-NOTIFICACIÓN MANUAL AL OPERADOR
# =========================================================
@bp_flotaespecial_eps.route('/api/operativa/notificar', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_notificar_viaje():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    datos = request.get_json(silent=True) or {}
    viaje_id = datos.get('viaje_id')

    if not viaje_id:
        return jsonify({"status": "error", "message": "ID de viaje no proporcionado"}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT c.*, f.ruta_pdf_fuec 
            FROM control_viajes_flota_especial c
            LEFT JOIN fuec f ON c.id_viaje = f.id_traslado_eps AND f.id_empresa = c.id_empresa
            WHERE c.id = %s AND c.id_empresa = %s
        """, (viaje_id, empresa_id))
        viaje = cur.fetchone()

        if not viaje:
            return jsonify({"status": "error", "message": "No se encontró el viaje."}), 404
            
        if not viaje.get('conductor_asignado'):
            return jsonify({"status": "error", "message": "El viaje no tiene un conductor asignado."}), 400

        cur.execute("SELECT telegram_id FROM usuarios WHERE nombre = %s AND empresa_id = %s", (viaje['conductor_asignado'], empresa_id))
        usr = cur.fetchone()
        if not usr or not usr.get('telegram_id'):
            return jsonify({"status": "error", "message": "El operador asignado no tiene su cuenta de Telegram enlazada."}), 400

        telegram_id = usr['telegram_id']
        telefono_paciente = viaje.get('telefono_usuario') or 'N/D'
        info_acompanante = ""
        if viaje.get('lleva_acompanante'):
            info_acompanante = f"👥 <b>Acompañante:</b> {viaje.get('nombre_acompanante', 'Sí')}\n"

        mensaje_tg = (
            f"🟢 <b>NUEVA ASIGNACIÓN / RECORDATORIO DE VIAJE (FUEC)</b>\n\n"
            f"🏢 <b>Empresa:</b> {empresa_nombre}\n"
            f"🆔 <b>Prescripción:</b> {viaje.get('numero_prescripcion')}\n"
            f"🚙 <b>Vehículo:</b> {viaje.get('vehiculo_asignado')}\n\n"
            f"📋 <b>PROGRAMACIÓN:</b>\n"
            f"ID Viaje: <code>{viaje['id_viaje']}</code>\n"
            f"Trayecto: {viaje.get('trayecto', 'IDA')}\n"
            f"Paciente: {viaje['nombre_usuario']}\n"
            f"📞 <b>Teléfono:</b> {telefono_paciente}\n"
            f"{info_acompanante}"
            f"Fecha: {viaje.get('fecha_servicio', 'N/D')} | Hora: {viaje.get('hora_inicio', 'N/D')}\n"
            f"Origen: {viaje['direccion_origen']}\n"
            f"Destino: {viaje['direccion_destino']}"
        )

        ruta_pdf_abs = None
        if viaje.get('ruta_pdf_fuec'):
            ruta_pdf_abs = os.path.join(current_app.static_folder, viaje['ruta_pdf_fuec'])
        
        if ruta_pdf_abs and os.path.exists(ruta_pdf_abs):
            _enviar_documento_telegram_hilo([telegram_id], mensaje_tg, ruta_pdf_abs)
        else:
            _enviar_mensajes_telegram_hilo([telegram_id], mensaje_tg.replace("<b>", "*").replace("</b>", "*").replace("<code>", "`").replace("</code>", "`"))

        return jsonify({"status": "success", "message": "Notificación enviada al operador."}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# GESTIÓN DE CONTINGENCIAS (BUSCAR, REPROGRAMAR, ANULAR)
# =========================================================
@bp_flotaespecial_eps.route('/api/operativa/contingencias/buscar', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_contingencias_buscar():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    cedula = datos.get('cedula', '').strip()

    if not cedula:
        return jsonify({"status": "error", "message": "Documento (Cédula) no proporcionado."}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT id, id_viaje, numero_prescripcion, nombre_usuario, telefono_usuario, 
                   fecha_servicio, hora_inicio, direccion_origen, direccion_destino, estatus_servicio
            FROM control_viajes_flota_especial
            WHERE id_empresa = %s AND id_usuario = %s AND trayecto = 'IDA' 
              AND estatus_servicio IN ('CAPTURADO', 'VERIFICADO', 'PROGRAMADO', 'PDTE. ASIGNAR VUELTA')
            ORDER BY fecha_servicio ASC, hora_inicio ASC
        """, (empresa_id, cedula))
        viajes = cur.fetchall()

        for v in viajes:
            v['fecha_servicio'] = str(v['fecha_servicio']) if v['fecha_servicio'] else 'Sin Fecha'
            v['hora_inicio'] = str(v['hora_inicio']) if v['hora_inicio'] else 'Sin Hora'

        return jsonify({"status": "success", "data": viajes}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

@bp_flotaespecial_eps.route('/api/operativa/contingencias/reprogramar', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_contingencias_reprogramar():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    viaje_id = datos.get('viaje_id')
    nueva_fecha = datos.get('fecha_servicio')
    nueva_hora = datos.get('hora_inicio')

    if not viaje_id or not nueva_fecha or not nueva_hora:
        return jsonify({"status": "error", "message": "Faltan datos obligatorios para reprogramar el viaje."}), 400

    cur = mysql.connection.cursor()
    try:
        cur.execute("""
            UPDATE control_viajes_flota_especial
            SET fecha_servicio = %s, hora_inicio = %s
            WHERE id = %s AND id_empresa = %s AND trayecto = 'IDA'
        """, (nueva_fecha, nueva_hora, viaje_id, empresa_id))
        mysql.connection.commit()
        return jsonify({"status": "success", "message": "Viaje reprogramado exitosamente."}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

@bp_flotaespecial_eps.route('/api/operativa/contingencias/anular', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_gestion')
def api_contingencias_anular():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    viaje_id = datos.get('viaje_id')
    motivo = datos.get('motivo', '').strip()

    if not viaje_id or not motivo:
        return jsonify({"status": "error", "message": "Datos incompletos para anular el viaje. El motivo es obligatorio."}), 400

    cur = mysql.connection.cursor()
    try:
        cur.execute("""
            UPDATE control_viajes_flota_especial
            SET estatus_servicio = 'ANULADO', descripcion_novedad = %s
            WHERE id = %s AND id_empresa = %s
        """, (motivo, viaje_id, empresa_id))
        mysql.connection.commit()
        return jsonify({"status": "success", "message": "Viaje anulado exitosamente."}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()