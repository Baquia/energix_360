# app/blueprints/B_bp_flotaespecial_vehiculos.py
import os
import io
import json
import re
import uuid
import requests
import threading
import urllib.parse
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file, current_app, jsonify
from werkzeug.utils import secure_filename
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta, date
import base64

# Librerías PDF (Reportes Platypus)
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

bp_flotaespecial_vehiculos = Blueprint('flotaespecial_vehiculos', __name__, url_prefix='/gestor_flotaespecial/vehiculos_bp')

def controlador_flotaespecial_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['controlador_flotaespecial', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Transporte Especial para ingresar a este módulo.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# HELPER: MIGRACIÓN DE TABLAS Y COLUMNAS
# =========================================================
def asegurar_tablas_y_columnas(cur):
    # 1. Vehículos
    columnas_vehiculos = [
        ("vin", "VARCHAR(100)"), ("numero_serie", "VARCHAR(100)"), ("restriccion_movilidad", "VARCHAR(100)"),
        ("blindaje", "VARCHAR(50)"), ("potencia_hp", "VARCHAR(50)"), ("declaracion_importacion", "VARCHAR(100)"),
        ("fecha_importacion", "DATE"), ("puertas", "INT"), ("limitacion_propiedad", "VARCHAR(255)"),
        ("fecha_expedicion_licencia", "DATE"), ("servicio", "VARCHAR(50)"), ("modalidad_servicio", "VARCHAR(100)"),
        ("nivel_servicio", "VARCHAR(100)"), ("radio_accion", "VARCHAR(100)"), ("fecha_expedicion_tarjeta_operacion", "DATE"),
        ("fecha_inicio_tarjeta_operacion", "DATE"), ("ruta_pdf_tarjeta_operacion", "VARCHAR(255)"),
        ("fecha_inicio_rcc_rce", "DATE"), ("ruta_pdf_rcc_rce", "VARCHAR(255)"), ("empresa_transporte", "VARCHAR(150)")
    ]
    for col, tipo in columnas_vehiculos:
        try: cur.execute(f"ALTER TABLE vehiculos_especial ADD COLUMN {col} {tipo}")
        except: pass
        
    # 2. Empresas Terceras
    cur.execute("""
        CREATE TABLE IF NOT EXISTS empresas_transporte_especial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            nombre VARCHAR(150) NOT NULL,
            nit VARCHAR(50) DEFAULT NULL,
            departamento_sede VARCHAR(100) DEFAULT NULL,
            municipio_sede VARCHAR(100) DEFAULT NULL,
            telefono_contacto VARCHAR(50) DEFAULT NULL,
            email_contacto VARCHAR(100) DEFAULT NULL,
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)

    # 3. Conductores (Integración)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS conductores_flotaespecial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            nombre VARCHAR(150) NOT NULL,
            cedula VARCHAR(50) NOT NULL,
            departamento_base VARCHAR(100) DEFAULT NULL,
            municipio_base VARCHAR(100) DEFAULT NULL,
            numero_licencia_conduccion VARCHAR(100) DEFAULT NULL,
            vencimiento_licencia_conduccion DATE DEFAULT NULL,
            eps VARCHAR(100) DEFAULT NULL,
            fondo_pensiones VARCHAR(100) DEFAULT NULL,
            arl VARCHAR(100) DEFAULT NULL,
            ultimo_pago_seguridad_social DATE DEFAULT NULL,
            vencimiento_seguridad_social DATE DEFAULT NULL,
            ruta_pdf_cedula VARCHAR(255) DEFAULT NULL,
            ruta_pdf_licencia VARCHAR(255) DEFAULT NULL,
            ruta_pdf_seguridad_social VARCHAR(255) DEFAULT NULL,
            estatus VARCHAR(50) DEFAULT 'No Logueado',
            ultima_latitud VARCHAR(100) DEFAULT NULL,
            ultima_longitud VARCHAR(100) DEFAULT NULL,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX(id_empresa),
            INDEX(cedula)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)

    try: cur.execute("ALTER TABLE conductores_flotaespecial ADD COLUMN estatus VARCHAR(50) DEFAULT 'No Logueado'")
    except: pass
    try: cur.execute("ALTER TABLE conductores_flotaespecial ADD COLUMN ultima_latitud VARCHAR(100) DEFAULT NULL")
    except: pass
    try: cur.execute("ALTER TABLE conductores_flotaespecial ADD COLUMN ultima_longitud VARCHAR(100) DEFAULT NULL")
    except: pass

    # 4. Historial Auditoría Vencimientos
    cur.execute("""
        CREATE TABLE IF NOT EXISTS historial_verificaciones_flotaespecial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            fecha_verificacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            tipo_entidad VARCHAR(50) NOT NULL COMMENT 'VEHICULO o CONDUCTOR',
            identificador VARCHAR(50) NOT NULL COMMENT 'Placa o Cédula',
            documento_verificado VARCHAR(100) NOT NULL,
            estado_documento VARCHAR(50) NOT NULL,
            observacion TEXT,
            usuario_auditor VARCHAR(100) DEFAULT 'SISTEMA_CRON',
            INDEX(id_empresa),
            INDEX(identificador)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)

# =========================================================
# HELPER: NOTIFICACIONES TELEGRAM
# =========================================================
def _enviar_mensajes_telegram_hilo(chat_ids, mensaje):
    TOKEN = "8841682239:AAFOj8TpeOW4ulhIkNoIyGaTZ2MLlI9ydVo"
    def tarea_envio():
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        for chat_id in set(chat_ids):
            if not chat_id: continue
            data = {"chat_id": chat_id, "text": mensaje, "parse_mode": "Markdown"}
            for _ in range(3):
                try:
                    resp = requests.post(url, data=data, timeout=10)
                    if resp.status_code == 200: break
                except Exception: pass
    hilo = threading.Thread(target=tarea_envio)
    hilo.daemon = True
    hilo.start()

# =========================================================
# HELPER: GUARDAR PDF MANUAL
# =========================================================
def guardar_pdf_manual(file_obj, prefix, subfolder='vehiculos'):
    if file_obj and file_obj.filename.endswith('.pdf'):
        filename = secure_filename(f"{prefix}_{uuid.uuid4().hex[:8]}.pdf")
        ruta_base = os.path.join(current_app.static_folder, 'uploads', 'flotaespecial', subfolder)
        os.makedirs(ruta_base, exist_ok=True)
        file_obj.save(os.path.join(ruta_base, filename))
        return f"uploads/flotaespecial/{subfolder}/{filename}"
    return None

# =========================================================
# RUTAS UNIFICADAS: GESTIÓN DE VEHÍCULOS, CONDUCTORES, TERCEROS Y PREOPERACIONALES
# =========================================================
@bp_flotaespecial_vehiculos.route('/', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_vehiculos():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    active_module = request.args.get('active_module', 'vehiculos')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    asegurar_tablas_y_columnas(cur)
    mysql.connection.commit()

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        # ----------------------------------------------------
        # GESTIÓN DE CONDUCTORES
        # ----------------------------------------------------
        if accion in ['crear_conductor', 'editar_conductor']:
            conductor_id = request.form.get('conductor_id')
            nombre = request.form.get('nombre', '').strip()
            cedula = request.form.get('cedula', '').strip()
            departamento_base = request.form.get('departamento_base', '').strip()
            municipio_base = request.form.get('municipio_base', '').strip()
            numero_licencia = request.form.get('numero_licencia_conduccion', '').strip()
            vencimiento_licencia = request.form.get('vencimiento_licencia_conduccion') or None
            eps = request.form.get('eps', '').strip()
            fondo_pensiones = request.form.get('fondo_pensiones', '').strip()
            arl = request.form.get('arl', '').strip()
            ultimo_pago_ss = request.form.get('ultimo_pago_seguridad_social') or None
            telegram_id = request.form.get('telegram_id', '').strip()

            vencimiento_ss = None
            if ultimo_pago_ss:
                try:
                    fecha_pago = datetime.strptime(ultimo_pago_ss, '%Y-%m-%d')
                    vencimiento_ss = (fecha_pago + timedelta(days=30)).strftime('%Y-%m-%d')
                except ValueError:
                    vencimiento_ss = None

            r_ced = guardar_pdf_manual(request.files.get('file_pdf_cedula'), 'ced', 'conductores')
            r_lic = guardar_pdf_manual(request.files.get('file_pdf_licencia'), 'lic', 'conductores')
            r_ss = guardar_pdf_manual(request.files.get('file_pdf_seguridad_social'), 'ss', 'conductores')

            if nombre and cedula:
                try:
                    if accion == 'crear_conductor':
                        cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
                        if cur.fetchone():
                            flash(f"La cédula {cedula} ya está registrada como usuario.", "danger")
                            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='conductores'))
                        
                        hashed_pw = bcrypt.generate_password_hash(cedula).decode('utf-8')
                        cur.execute("""
                            INSERT INTO usuarios (nombre, cedula, password, tipo_usuario, clase, perfil, empresa, empresa_id, telegram_id) 
                            VALUES (%s, %s, %s, 'cliente', 'op', 'operador_flotaespecial', %s, %s, %s)
                        """, (nombre, cedula, hashed_pw, empresa_nombre, empresa_id, telegram_id or None))
                        
                        cur.execute("""
                            INSERT INTO conductores_flotaespecial 
                            (id_empresa, nombre, cedula, departamento_base, municipio_base, numero_licencia_conduccion, 
                             vencimiento_licencia_conduccion, eps, fondo_pensiones, arl, ultimo_pago_seguridad_social, 
                             vencimiento_seguridad_social, ruta_pdf_cedula, ruta_pdf_licencia, ruta_pdf_seguridad_social, estatus) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'No Logueado')
                        """, (empresa_id, nombre, cedula, departamento_base, municipio_base, numero_licencia,
                              vencimiento_licencia, eps, fondo_pensiones, arl, ultimo_pago_ss,
                              vencimiento_ss, r_ced or '', r_lic or '', r_ss or ''))
                        
                        mysql.connection.commit()
                        flash(f"Conductor {nombre} registrado exitosamente.", "success")
                        
                    elif accion == 'editar_conductor' and conductor_id:
                        cur.execute("""
                            UPDATE conductores_flotaespecial 
                            SET nombre=%s, cedula=%s, departamento_base=%s, municipio_base=%s, numero_licencia_conduccion=%s, 
                                vencimiento_licencia_conduccion=%s, eps=%s, fondo_pensiones=%s, arl=%s, 
                                ultimo_pago_seguridad_social=%s, vencimiento_seguridad_social=%s
                            WHERE id=%s AND id_empresa=%s
                        """, (nombre, cedula, departamento_base, municipio_base, numero_licencia,
                              vencimiento_licencia, eps, fondo_pensiones, arl, ultimo_pago_ss,
                              vencimiento_ss, conductor_id, empresa_id))
                        
                        if r_ced: cur.execute("UPDATE conductores_flotaespecial SET ruta_pdf_cedula=%s WHERE id=%s", (r_ced, conductor_id))
                        if r_lic: cur.execute("UPDATE conductores_flotaespecial SET ruta_pdf_licencia=%s WHERE id=%s", (r_lic, conductor_id))
                        if r_ss: cur.execute("UPDATE conductores_flotaespecial SET ruta_pdf_seguridad_social=%s WHERE id=%s", (r_ss, conductor_id))
                        
                        cur.execute("UPDATE usuarios SET nombre=%s, telegram_id=%s WHERE cedula=%s AND empresa_id=%s", (nombre, telegram_id or None, cedula, empresa_id))
                        mysql.connection.commit()
                        flash(f"Expediente del conductor {nombre} actualizado.", "success")

                except Exception as e:
                    mysql.connection.rollback()
                    flash(f"Error en base de datos: {str(e)}", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='conductores'))

        elif accion == 'eliminar_conductor':
            conductor_id = request.form.get('conductor_id')
            cedula_eliminar = request.form.get('cedula')
            try:
                cur.execute("DELETE FROM conductores_flotaespecial WHERE id = %s AND id_empresa = %s", (conductor_id, empresa_id))
                cur.execute("DELETE FROM usuarios WHERE cedula = %s AND empresa_id = %s", (cedula_eliminar, empresa_id))
                mysql.connection.commit()
                flash("Conductor eliminado permanentemente.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash("Error al eliminar conductor.", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='conductores'))

        # ----------------------------------------------------
        # GESTIÓN DE EMPRESAS TERCERAS
        # ----------------------------------------------------
        elif accion == 'crear_tercero':
            nombre = request.form.get('nombre', '').strip()
            nit = request.form.get('nit', '').strip()
            dep = request.form.get('departamento_sede', '').strip()
            mun = request.form.get('municipio_sede', '').strip()
            tel = request.form.get('telefono_contacto', '').strip()
            email = request.form.get('email_contacto', '').strip()
            try:
                cur.execute("""
                    INSERT INTO empresas_transporte_especial 
                    (id_empresa, nombre, nit, departamento_sede, municipio_sede, telefono_contacto, email_contacto)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (empresa_id, nombre, nit, dep, mun, tel, email))
                mysql.connection.commit()
                flash("Empresa tercera registrada exitosamente.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al registrar tercero: {str(e)}", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='terceros'))

        elif accion == 'editar_tercero':
            t_id = request.form.get('tercero_id')
            nombre = request.form.get('nombre', '').strip()
            nit = request.form.get('nit', '').strip()
            dep = request.form.get('departamento_sede', '').strip()
            mun = request.form.get('municipio_sede', '').strip()
            tel = request.form.get('telefono_contacto', '').strip()
            email = request.form.get('email_contacto', '').strip()
            try:
                cur.execute("""
                    UPDATE empresas_transporte_especial 
                    SET nombre=%s, nit=%s, departamento_sede=%s, municipio_sede=%s, telefono_contacto=%s, email_contacto=%s
                    WHERE id=%s AND id_empresa=%s
                """, (nombre, nit, dep, mun, tel, email, t_id, empresa_id))
                mysql.connection.commit()
                flash("Información de empresa tercera actualizada.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al actualizar tercero: {str(e)}", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='terceros'))
            
        elif accion == 'eliminar_tercero':
            t_id = request.form.get('tercero_id')
            try:
                cur.execute("DELETE FROM empresas_transporte_especial WHERE id=%s AND id_empresa=%s", (t_id, empresa_id))
                mysql.connection.commit()
                flash("Empresa tercera eliminada.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al eliminar tercero.", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='terceros'))

        # ----------------------------------------------------
        # CRUD DE VEHÍCULOS
        # ----------------------------------------------------
        elif accion in ['crear', 'editar']:
            v_id = request.form.get('vehiculo_id')
            placa = str(request.form.get('placa', '')).upper().strip()
            
            clase = request.form.get('clase', '').strip()
            carroceria = request.form.get('carroceria', '').strip()
            marca = request.form.get('marca', '').strip()
            linea = request.form.get('linea', '').strip()
            modelo = request.form.get('modelo', '').strip()
            color = request.form.get('color', '').strip()
            combustible = request.form.get('combustible', '').strip()
            cilindraje = request.form.get('cilindraje', '').strip()
            capacidad = request.form.get('capacidad') or 0
            potencia_hp = request.form.get('potencia_hp', '').strip()
            puertas = request.form.get('puertas') or 0
            
            vin = request.form.get('vin', '').strip()
            chasis = request.form.get('chasis', '').strip()
            motor = request.form.get('motor', '').strip()
            numero_serie = request.form.get('numero_serie', '').strip()
            
            servicio = request.form.get('servicio', '').strip()
            organismo_transito = request.form.get('organismo_transito', '').strip()
            fecha_matricula = request.form.get('fecha_matricula') or None
            fecha_expedicion_licencia = request.form.get('fecha_expedicion_licencia') or None
            limitacion_propiedad = request.form.get('limitacion_propiedad', '').strip()
            declaracion_importacion = request.form.get('declaracion_importacion', '').strip()
            fecha_importacion = request.form.get('fecha_importacion') or None
            restriccion_movilidad = request.form.get('restriccion_movilidad', '').strip()
            blindaje = request.form.get('blindaje', '').strip()
            
            tipo_vinculacion = request.form.get('tipo_vinculacion', 'Propio').strip()
            empresa_vinculadora = request.form.get('empresa_vinculadora', '').strip()
            
            if tipo_vinculacion == 'Propio':
                empresa_transporte = session.get('empresa')
                empresa_vinculadora = session.get('empresa')
                nit_empresa_vinculadora = session.get('nit')
            else:
                empresa_transporte = empresa_vinculadora
                nit_empresa_vinculadora = request.form.get('nit_empresa_vinculadora', '').strip()
                if empresa_vinculadora:
                    cur.execute("SELECT nit FROM empresas_transporte_especial WHERE nombre=%s AND id_empresa=%s", (empresa_vinculadora, empresa_id))
                    t_data = cur.fetchone()
                    if t_data and t_data['nit']:
                        nit_empresa_vinculadora = t_data['nit']
            
            numero_tarjeta_operacion = request.form.get('numero_tarjeta_operacion', '').strip()
            modalidad_servicio = request.form.get('modalidad_servicio', '').strip()
            nivel_servicio = request.form.get('nivel_servicio', '').strip()
            radio_accion = request.form.get('radio_accion', '').strip()
            fecha_expedicion_tarjeta_operacion = request.form.get('fecha_expedicion_tarjeta_operacion') or None
            fecha_inicio_tarjeta_operacion = request.form.get('fecha_inicio_tarjeta_operacion') or None
            vencimiento_tarjeta_operacion = request.form.get('vencimiento_tarjeta_operacion') or None
            
            numero_poliza_rcc_rce = request.form.get('numero_poliza_rcc_rce', '').strip()
            aseguradora_rcc_rce = request.form.get('aseguradora_rcc_rce', '').strip()
            fecha_inicio_rcc_rce = request.form.get('fecha_inicio_rcc_rce') or None
            vencimiento_rcc_rce = request.form.get('vencimiento_rcc_rce') or None
            
            numero_poliza_soat = request.form.get('numero_poliza_soat', '').strip()
            aseguradora_soat = request.form.get('aseguradora_soat', '').strip()
            vencimiento_soat = request.form.get('vencimiento_soat') or None
            
            numero_certificado_rtm = request.form.get('numero_certificado_rtm', '').strip()
            vencimiento_rtm = request.form.get('vencimiento_rtm') or None

            r_prop = guardar_pdf_manual(request.files.get('file_pdf_propiedad'), 'prop')
            r_ope = guardar_pdf_manual(request.files.get('file_pdf_operacion'), 'ope')
            r_rcc = guardar_pdf_manual(request.files.get('file_pdf_poliza'), 'rcc')
            r_soat = guardar_pdf_manual(request.files.get('file_pdf_soat'), 'soat')
            r_rtm = guardar_pdf_manual(request.files.get('file_pdf_rtm'), 'rtm')
            
            if placa and clase:
                try:
                    if accion == 'crear':
                        cur.execute("""
                            INSERT INTO vehiculos_especial 
                            (id_empresa, placa, clase, carroceria, marca, linea, modelo, color, combustible, 
                             cilindraje, capacidad, potencia_hp, puertas, vin, chasis, motor, numero_serie,
                             servicio, organismo_transito, fecha_matricula, fecha_expedicion_licencia,
                             limitacion_propiedad, declaracion_importacion, fecha_importacion,
                             restriccion_movilidad, blindaje, tipo_vinculacion, numero_tarjeta_operacion,
                             modalidad_servicio, nivel_servicio, radio_accion, empresa_vinculadora,
                             nit_empresa_vinculadora, empresa_transporte, fecha_expedicion_tarjeta_operacion,
                             fecha_inicio_tarjeta_operacion, vencimiento_tarjeta_operacion,
                             numero_poliza_rcc_rce, aseguradora_rcc_rce, fecha_inicio_rcc_rce, vencimiento_rcc_rce,
                             numero_poliza_soat, aseguradora_soat, vencimiento_soat,
                             numero_certificado_rtm, vencimiento_rtm,
                             ruta_pdf_tarjeta_propiedad, ruta_pdf_tarjeta_operacion, ruta_pdf_rcc_rce, ruta_pdf_soat, ruta_pdf_tecnomecanica) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            empresa_id, placa, clase, carroceria, marca, linea, modelo, color, combustible,
                            cilindraje, capacidad, potencia_hp, puertas, vin, chasis, motor, numero_serie,
                            servicio, organismo_transito, fecha_matricula, fecha_expedicion_licencia,
                            limitacion_propiedad, declaracion_importacion, fecha_importacion,
                            restriccion_movilidad, blindaje, tipo_vinculacion, numero_tarjeta_operacion,
                            modalidad_servicio, nivel_servicio, radio_accion, empresa_vinculadora,
                            nit_empresa_vinculadora, empresa_transporte, fecha_expedicion_tarjeta_operacion,
                            fecha_inicio_tarjeta_operacion, vencimiento_tarjeta_operacion,
                            numero_poliza_rcc_rce, aseguradora_rcc_rce, fecha_inicio_rcc_rce, vencimiento_rcc_rce,
                            numero_poliza_soat, aseguradora_soat, vencimiento_soat,
                            numero_certificado_rtm, vencimiento_rtm,
                            r_prop or '', r_ope or '', r_rcc or '', r_soat or '', r_rtm or ''
                        ))
                        mysql.connection.commit()
                        flash(f"Vehículo especial {placa} registrado manualmente con éxito.", "success")
                        
                    elif accion == 'editar' and v_id:
                        cur.execute("""
                            UPDATE vehiculos_especial 
                            SET placa=%s, clase=%s, carroceria=%s, marca=%s, linea=%s, modelo=%s, color=%s, 
                                combustible=%s, cilindraje=%s, capacidad=%s, potencia_hp=%s, puertas=%s, 
                                vin=%s, chasis=%s, motor=%s, numero_serie=%s,
                                servicio=%s, organismo_transito=%s, fecha_matricula=%s, fecha_expedicion_licencia=%s,
                                limitacion_propiedad=%s, declaracion_importacion=%s, fecha_importacion=%s,
                                restriccion_movilidad=%s, blindaje=%s, tipo_vinculacion=%s, numero_tarjeta_operacion=%s,
                                modalidad_servicio=%s, nivel_servicio=%s, radio_accion=%s, empresa_vinculadora=%s,
                                nit_empresa_vinculadora=%s, empresa_transporte=%s, fecha_expedicion_tarjeta_operacion=%s,
                                fecha_inicio_tarjeta_operacion=%s, vencimiento_tarjeta_operacion=%s,
                                numero_poliza_rcc_rce=%s, aseguradora_rcc_rce=%s, fecha_inicio_rcc_rce=%s, vencimiento_rcc_rce=%s,
                                numero_poliza_soat=%s, aseguradora_soat=%s, vencimiento_soat=%s,
                                numero_certificado_rtm=%s, vencimiento_rtm=%s
                            WHERE id=%s AND id_empresa=%s
                        """, (
                            placa, clase, carroceria, marca, linea, modelo, color, combustible,
                            cilindraje, capacidad, potencia_hp, puertas, vin, chasis, motor, numero_serie,
                            servicio, organismo_transito, fecha_matricula, fecha_expedicion_licencia,
                            limitacion_propiedad, declaracion_importacion, fecha_importacion,
                            restriccion_movilidad, blindaje, tipo_vinculacion, numero_tarjeta_operacion,
                            modalidad_servicio, nivel_servicio, radio_accion, empresa_vinculadora,
                            nit_empresa_vinculadora, empresa_transporte, fecha_expedicion_tarjeta_operacion,
                            fecha_inicio_tarjeta_operacion, vencimiento_tarjeta_operacion,
                            numero_poliza_rcc_rce, aseguradora_rcc_rce, fecha_inicio_rcc_rce, vencimiento_rcc_rce,
                            numero_poliza_soat, aseguradora_soat, vencimiento_soat,
                            numero_certificado_rtm, vencimiento_rtm,
                            v_id, empresa_id
                        ))
                        if r_prop: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_tarjeta_propiedad=%s WHERE id=%s", (r_prop, v_id))
                        if r_ope: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_tarjeta_operacion=%s WHERE id=%s", (r_ope, v_id))
                        if r_rcc: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_rcc_rce=%s WHERE id=%s", (r_rcc, v_id))
                        if r_soat: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_soat=%s WHERE id=%s", (r_soat, v_id))
                        if r_rtm: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_tecnomecanica=%s WHERE id=%s", (r_rtm, v_id))
                        
                        mysql.connection.commit()
                        flash(f"Expediente del vehículo {placa} actualizado correctamente.", "success")
                        
                except Exception as e:
                    mysql.connection.rollback()
                    flash(f"Error en base de datos: {str(e)}", "danger")
            else:
                flash("Faltan datos obligatorios (Placa y Clase).", "warning")

        elif accion == 'eliminar':
            vehiculo_id = request.form.get('vehiculo_id')
            try:
                cur.execute("DELETE FROM vehiculos_especial WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                mysql.connection.commit()
                flash("Vehículo especial eliminado de la base de datos.", "success")
            except Exception as e:
                flash("Error al eliminar vehículo.", "danger")

        return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='vehiculos'))

    # ================= MODO LECTURA / VISTA SEGÚN MÓDULO =================
    
    if active_module == 'conductores':
        cur.execute("""
            SELECT c.*, u.telegram_id 
            FROM conductores_flotaespecial c
            LEFT JOIN usuarios u ON c.cedula COLLATE utf8mb4_unicode_ci = u.cedula COLLATE utf8mb4_unicode_ci AND c.id_empresa = u.empresa_id
            WHERE c.id_empresa = %s 
            ORDER BY c.nombre ASC
        """, (empresa_id,))
        conductores_db = cur.fetchall()
        cur.close()
        
        return render_template(
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='conductores', conductores=conductores_db
        )
        
    elif active_module == 'preoperacionales':
        fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
        fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
        placa_filtro = request.args.get('placa', 'todas')

        cur.execute("SELECT DISTINCT placa FROM vehiculos_especial WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
        vehiculos_historicos = cur.fetchall()

        query = """
            SELECT id_inspeccion, consecutivo_anual, fecha_inspeccion, hora_inspeccion, 
                   placa_vehiculo, nombre_conductor, vehiculo_aprobado 
            FROM inspeccion_preoperacional 
            WHERE id_empresa = %s AND fecha_inspeccion BETWEEN %s AND %s
        """
        params = [empresa_id, fecha_inicio, fecha_fin]
        
        if placa_filtro != 'todas':
            query += " AND placa_vehiculo = %s"
            params.append(placa_filtro)
            
        query += " ORDER BY fecha_inspeccion DESC, hora_inspeccion DESC"
        
        cur.execute(query, tuple(params))
        inspecciones = cur.fetchall()
        cur.close()

        return render_template(
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='preoperacionales', inspecciones=inspecciones,
            vehiculos_historicos=vehiculos_historicos,
            filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'placa': placa_filtro}
        )
        
    elif active_module == 'terceros':
        cur.execute("SELECT * FROM empresas_transporte_especial WHERE id_empresa = %s ORDER BY nombre ASC", (empresa_id,))
        terceros_db = cur.fetchall()
        cur.close()
        
        return render_template(
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='terceros', terceros=terceros_db
        )
        
    else:
        # VISTA POR DEFECTO: VEHÍCULOS + TABLERO KPIs
        cur.execute("SELECT * FROM vehiculos_especial WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
        vehiculos_db = cur.fetchall()
        
        hoy = datetime.now().date()
        exentos_rtm_count = 0
        
        for v in vehiculos_db:
            v['rtm_exento'] = False
            if v.get('fecha_matricula'):
                f_mat = v['fecha_matricula']
                if isinstance(f_mat, str):
                    try: f_mat = datetime.strptime(f_mat, '%Y-%m-%d').date()
                    except ValueError: continue
                try: f_limite = f_mat.replace(year=f_mat.year + 5)
                except ValueError: f_limite = f_mat.replace(year=f_mat.year + 5, day=28)
                
                if hoy <= f_limite:
                    v['rtm_exento'] = True
                    exentos_rtm_count += 1
        
        cur.execute("SELECT id, nombre, nit FROM empresas_transporte_especial WHERE id_empresa = %s ORDER BY nombre ASC", (empresa_id,))
        terceros_db = cur.fetchall()
        
        cur.execute("SELECT nombre, cedula, estatus FROM conductores_flotaespecial WHERE id_empresa = %s", (empresa_id,))
        conductores_db = cur.fetchall()
        
        conductores_activos = [c for c in conductores_db if c.get('estatus') in ['Logueado', 'Prelogueado']]
        
        kpis = {
            'total_flota': len(vehiculos_db),
            'exentos_rtm': exentos_rtm_count,
            'total_conductores': len(conductores_db),
            'total_activos': len(conductores_activos)
        }
        
        cur.close()

        return render_template(
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='vehiculos', vehiculos=vehiculos_db, terceros=terceros_db,
            kpis=kpis, conductores_activos=conductores_activos
        )

# =========================================================
# ENDPOINTS AJAX: VISORES INDIVIDUALES
# =========================================================
@bp_flotaespecial_vehiculos.route('/visor_individual_conductor', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def visor_conductor_individual():
    empresa_id = session.get('empresa_id')
    cedula_busqueda = request.args.get('cedula', '').strip()
    
    if not cedula_busqueda:
        return jsonify({'success': False, 'message': 'Cédula no proporcionada.'})

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT c.*, u.telegram_id 
        FROM conductores_flotaespecial c
        LEFT JOIN usuarios u ON c.cedula COLLATE utf8mb4_unicode_ci = u.cedula COLLATE utf8mb4_unicode_ci AND c.id_empresa = u.empresa_id
        WHERE c.cedula = %s AND c.id_empresa = %s LIMIT 1
    """, (cedula_busqueda, empresa_id))
    conductor = cur.fetchone()
    cur.close()

    if conductor:
        return jsonify({'success': True, 'conductor': conductor})
    else:
        return jsonify({'success': False, 'message': f'Conductor con cédula {cedula_busqueda} no encontrado.'})

@bp_flotaespecial_vehiculos.route('/visor_viaje_activo', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def visor_viaje_activo():
    empresa_id = session.get('empresa_id')
    conductor_nombre = request.args.get('conductor_nombre', '').strip()
    
    if not conductor_nombre:
        return jsonify({'success': False, 'message': 'Conductor no especificado.'})

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT id_viaje, nombre_usuario, telefono_usuario, direccion_origen, direccion_destino, hora_inicio, vehiculo_asignado
        FROM control_viajes_flota_especial
        WHERE id_empresa = %s AND conductor_asignado = %s AND estatus_servicio = 'EN EJECUCION'
        LIMIT 1
    """, (empresa_id, conductor_nombre))
    viaje = cur.fetchone()
    cur.close()

    if viaje:
        if 'hora_inicio' in viaje and isinstance(viaje['hora_inicio'], timedelta):
            viaje['hora_inicio'] = str(viaje['hora_inicio'])
        return jsonify({'success': True, 'viaje': viaje})
    else:
        return jsonify({'success': False, 'message': 'El conductor no tiene un viaje "EN EJECUCIÓN" en este momento.'})

@bp_flotaespecial_vehiculos.route('/visor_tiempos_conduccion', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def visor_tiempos_conduccion():
    empresa_id = session.get('empresa_id')
    placa = request.args.get('placa', '').strip()
    fecha_inicio = request.args.get('fecha_inicio', '').strip()
    fecha_fin = request.args.get('fecha_fin', '').strip()

    if not placa or not fecha_inicio or not fecha_fin:
        return jsonify({'success': False, 'message': 'Faltan parámetros de búsqueda (Placa, Fecha Inicio, Fecha Fin).'})

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT v.id, v.consecutivo_viaje, v.fecha_hora_inicio, v.fecha_hora_fin, 
                   v.hora_reinicio, v.tiempo_efectivo_minutos,
                   c.nombre_usuario, c.id_viaje
            FROM viajes_flotaespecial v
            LEFT JOIN control_viajes_flota_especial c ON v.id_traslado_eps COLLATE utf8mb4_unicode_ci = c.id_viaje AND v.id_empresa = c.id_empresa
            WHERE v.id_empresa = %s 
              AND v.placa_vehiculo = %s 
              AND DATE(v.fecha_hora_inicio) BETWEEN %s AND %s
              AND v.estado = 'Finalizado'
            ORDER BY v.fecha_hora_inicio DESC
        """, (empresa_id, placa, fecha_inicio, fecha_fin))
        viajes = cur.fetchall()
        
        for v in viajes:
            if v.get('fecha_hora_inicio'): v['fecha_hora_inicio'] = str(v['fecha_hora_inicio'])
            if v.get('fecha_hora_fin'): v['fecha_hora_fin'] = str(v['fecha_hora_fin'])
            if v.get('hora_reinicio'): v['hora_reinicio'] = str(v['hora_reinicio'])

        return jsonify({'success': True, 'viajes': viajes})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()

# =========================================================
# CRON: AUDITORÍA DE VENCIMIENTOS (VEHÍCULOS Y CONDUCTORES)
# =========================================================
@bp_flotaespecial_vehiculos.route('/cron/auditoria_vencimientos', methods=['GET'])
def cron_auditoria_vencimientos():
    token = request.args.get('token')
    if token != 'BQA_CRON_2026':
        return jsonify({"status": "error", "message": "No autorizado"}), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        hoy = datetime.now().date()
        limite_alerta = hoy + timedelta(days=30)
        
        cur.execute("SELECT id, nombre_comercial FROM empresas")
        empresas = cur.fetchall()

        alertas_generadas = 0

        for emp in empresas:
            empresa_id = emp['id']
            empresa_nombre = emp['nombre_comercial']
            alertas_controlador = []

            cur.execute("SELECT telegram_id FROM usuarios WHERE empresa_id = %s AND perfil = 'controlador_flotaespecial'", (empresa_id,))
            controladores = cur.fetchall()
            telegram_controlador = [c['telegram_id'] for c in controladores if c.get('telegram_id')]

            # 1. Auditar Vehículos
            cur.execute("""
                SELECT v.placa, v.fecha_matricula, v.vencimiento_soat, v.vencimiento_rcc_rce, v.vencimiento_rtm,
                       u.telegram_id AS telegram_operador, u.nombre AS nombre_operador
                FROM vehiculos_especial v
                LEFT JOIN usuarios u ON v.cedula_operador COLLATE utf8mb4_unicode_ci = u.cedula COLLATE utf8mb4_unicode_ci AND v.id_empresa = u.empresa_id
                WHERE v.id_empresa = %s
            """, (empresa_id,))
            vehiculos = cur.fetchall()

            for v in vehiculos:
                placa = v['placa']
                mensajes_vehiculo = []

                rtm_requerida = True
                if v.get('fecha_matricula'):
                    f_mat = v['fecha_matricula']
                    try: f_limite = f_mat.replace(year=f_mat.year + 5)
                    except ValueError: f_limite = f_mat.replace(year=f_mat.year + 5, day=28)
                    if hoy <= f_limite:
                        rtm_requerida = False

                documentos_veh = [
                    ('SOAT', v.get('vencimiento_soat'), True),
                    ('Póliza RCC', v.get('vencimiento_rcc_rce'), True),
                    ('Tecnomecánica (RTM)', v.get('vencimiento_rtm'), rtm_requerida)
                ]

                for doc_nombre, fecha_vence, requerido in documentos_veh:
                    if not requerido:
                        estado = "EXENTO (Menor 5 Años)"
                        cur.execute("INSERT INTO historial_verificaciones_flotaespecial (id_empresa, tipo_entidad, identificador, documento_verificado, estado_documento) VALUES (%s, %s, %s, %s, %s)", (empresa_id, 'VEHICULO', placa, doc_nombre, estado))
                        continue

                    if not fecha_vence:
                        estado = "FALTANTE"
                        mensajes_vehiculo.append(f"❌ {doc_nombre}: Faltante en sistema.")
                    elif fecha_vence <= hoy:
                        estado = "VENCIDO"
                        mensajes_vehiculo.append(f"🔴 {doc_nombre}: VENCIDO ({fecha_vence})")
                    elif fecha_vence <= limite_alerta:
                        estado = "PROXIMO_VENCER"
                        mensajes_vehiculo.append(f"🟡 {doc_nombre}: Vence el {fecha_vence}")
                    else:
                        estado = "VIGENTE"
                    
                    cur.execute("INSERT INTO historial_verificaciones_flotaespecial (id_empresa, tipo_entidad, identificador, documento_verificado, estado_documento) VALUES (%s, %s, %s, %s, %s)", (empresa_id, 'VEHICULO', placa, doc_nombre, estado))

                if mensajes_vehiculo:
                    msg_txt = "\n".join(mensajes_vehiculo)
                    alertas_controlador.append(f"🚐 *Vehículo {placa}:*\n{msg_txt}")
                    if v.get('telegram_operador'):
                        _enviar_mensajes_telegram_hilo([v['telegram_operador']], f"⚠️ *Alerta Documental - Vehículo {placa}*\nHola {v['nombre_operador']}, tienes documentos del vehículo próximos a vencer o vencidos:\n\n{msg_txt}")
                        alertas_generadas += 1

            # 2. Auditar Conductores
            cur.execute("""
                SELECT c.cedula, c.nombre, c.vencimiento_licencia_conduccion, c.vencimiento_seguridad_social, u.telegram_id
                FROM conductores_flotaespecial c
                LEFT JOIN usuarios u ON c.cedula COLLATE utf8mb4_unicode_ci = u.cedula COLLATE utf8mb4_unicode_ci AND c.id_empresa = u.empresa_id
                WHERE c.id_empresa = %s
            """, (empresa_id,))
            conductores = cur.fetchall()

            for c in conductores:
                cedula = c['cedula']
                mensajes_conductor = []

                documentos_cond = [
                    ('Licencia de Conducción', c.get('vencimiento_licencia_conduccion')),
                    ('Seguridad Social (Planilla)', c.get('vencimiento_seguridad_social'))
                ]

                for doc_nombre, fecha_vence in documentos_cond:
                    if not fecha_vence:
                        estado = "FALTANTE"
                        mensajes_conductor.append(f"❌ {doc_nombre}: Faltante en sistema.")
                    elif fecha_vence <= hoy:
                        estado = "VENCIDO"
                        mensajes_conductor.append(f"🔴 {doc_nombre}: VENCIDO ({fecha_vence})")
                    elif fecha_vence <= limite_alerta:
                        estado = "PROXIMO_VENCER"
                        mensajes_conductor.append(f"🟡 {doc_nombre}: Vence el {fecha_vence}")
                    else:
                        estado = "VIGENTE"
                    
                    cur.execute("INSERT INTO historial_verificaciones_flotaespecial (id_empresa, tipo_entidad, identificador, documento_verificado, estado_documento) VALUES (%s, %s, %s, %s, %s)", (empresa_id, 'CONDUCTOR', cedula, doc_nombre, estado))

                if mensajes_conductor:
                    msg_txt = "\n".join(mensajes_conductor)
                    alertas_controlador.append(f"👨‍✈️ *Conductor {c['nombre']}:*\n{msg_txt}")
                    if c.get('telegram_id'):
                        _enviar_mensajes_telegram_hilo([c.get('telegram_id')], f"⚠️ *Alerta Documental - Personal*\nHola {c['nombre']}, tus documentos están próximos a vencer o vencidos:\n\n{msg_txt}\n\nPor favor, actualiza tu expediente.")
                        alertas_generadas += 1

            if alertas_controlador and telegram_controlador:
                cuerpo_reporte = "\n\n".join(alertas_controlador)
                mensaje_gerencial = f"📊 *REPORTE MENSUAL DE VENCIMIENTOS*\n🏢 {empresa_nombre}\n\nLos siguientes recursos requieren atención inmediata:\n\n{cuerpo_reporte}"
                _enviar_mensajes_telegram_hilo(telegram_controlador, mensaje_gerencial)
                alertas_generadas += 1

        mysql.connection.commit()
        return jsonify({"status": "success", "mensajes_telegram_generados": alertas_generadas}), 200

    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# DESCARGA PDF PREOPERACIONAL
# =========================================================
@bp_flotaespecial_vehiculos.route('/preoperacionales/pdf/<consecutivo>', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def descargar_preoperacional_pdf(consecutivo):
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    nit_empresa = session.get('nit')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM inspeccion_preoperacional WHERE consecutivo_anual = %s AND id_empresa = %s LIMIT 1", (consecutivo, empresa_id))
    insp = cur.fetchone()
    cur.close()

    if not insp:
        flash("Error: Inspección no encontrada o no pertenece a tu empresa.", "danger")
        return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='preoperacionales'))

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=14, textColor=colors.HexColor('#015249'), alignment=1, spaceAfter=10)
    sub_title_style = ParagraphStyle('SubTitle', parent=styles['Heading2'], fontSize=11, textColor=colors.HexColor('#015249'), spaceAfter=5, spaceBefore=10)
    cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=8, leading=10)
    cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold')

    base_dir = os.path.abspath(os.path.dirname(__file__))
    static_dir = os.path.join(base_dir, '..', 'static')
    logo_cliente_path = os.path.join(static_dir, f'logo_{nit_empresa}.PNG')
    logo_app_path = os.path.join(static_dir, 'logo_energix360.png')
    
    img_cliente = RLImage(logo_cliente_path, width=1.5*inch, height=0.5*inch, kind='proportional') if os.path.exists(logo_cliente_path) else Paragraph(empresa_nombre, cell_bold)
    img_app = RLImage(logo_app_path, width=1.5*inch, height=0.5*inch, kind='proportional') if os.path.exists(logo_app_path) else Paragraph("BQA-ONE", cell_bold)
    
    t_logos = Table([[img_cliente, img_app]], colWidths=[270, 270])
    t_logos.setStyle(TableStyle([('ALIGN', (0,0), (0,0), 'LEFT'), ('ALIGN', (1,0), (1,0), 'RIGHT'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
    story.append(t_logos)
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>INSPECCIÓN PREOPERACIONAL DETALLADA - SEGURIDAD VIAL</b>", title_style))

    dictamen_texto = "APROBADO (OPERATIVO)" if insp['vehiculo_aprobado'] == 1 else "ALERTA (CRÍTICA)"
    color_dictamen = colors.HexColor('#d1fae5') if insp['vehiculo_aprobado'] == 1 else colors.HexColor('#fee2e2')

    meta_data = [
        [Paragraph("<b>Consecutivo:</b>", cell_style), Paragraph(consecutivo, cell_bold), Paragraph("<b>Fecha / Hora:</b>", cell_style), Paragraph(f"{insp['fecha_inspeccion']} {insp['hora_inspeccion']}", cell_style)],
        [Paragraph("<b>Placa Vehículo:</b>", cell_style), Paragraph(str(insp['placa_vehiculo']).upper(), cell_bold), Paragraph("<b>Conductor:</b>", cell_style), Paragraph(insp['nombre_conductor'], cell_style)],
        [Paragraph("<b>Kilometraje:</b>", cell_style), Paragraph(str(insp['kilometraje_inicial']), cell_style), Paragraph("<b>Ruta:</b>", cell_style), Paragraph(insp['ruta_destino'], cell_style)],
        [Paragraph("<b>DICTAMEN:</b>", cell_style), Paragraph(f"<b>{dictamen_texto}</b>", cell_bold), "", ""]
    ]
    t_meta = Table(meta_data, colWidths=[100, 170, 100, 170])
    t_meta.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f9fafb')), ('BACKGROUND', (1,3), (1,3), color_dictamen),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('TOPPADDING', (0,0), (-1,-1), 4), ('BOTTOMPADDING', (0,0), (-1,-1), 4), ('SPAN', (1,3), (3,3))
    ]))
    story.append(t_meta)

    def get_estado_html(valor, es_doc=False):
        if es_doc:
            return "<font color='#16a34a'><b>AL DÍA / PORTA</b></font>" if valor == 1 else "<font color='#dc2626'><b>FALTANTE / VENCIDO</b></font>"
        if valor == 1: return "<font color='#16a34a'>Operativo</font>"
        elif valor == 2: return "<font color='#d97706'><b>Ajuste</b></font>"
        elif valor == 3: return "<font color='#dc2626'><b>Crítico</b></font>"
        return "N/A"

    checklist_config = [
        ("DOCUMENTACIÓN LEGAL", True, [
            ('doc_cedula', 'Cédula de Ciudadanía'), ('doc_licencia_conduccion', 'Licencia de Conducción'), 
            ('doc_licencia_transito', 'Licencia de Tránsito (Propiedad)'), ('doc_soat_vigente', 'SOAT Vigente'),
            ('doc_tecnomecanica_vigente', 'Revisión Tecnomecánica'), ('doc_tarjeta_operacion', 'Tarjeta de Operación')
        ]),
        ("ESTADO MECÁNICO Y MOTOR", False, [
            ('mec_nivel_aceite_motor', 'Nivel Aceite Motor'), ('mec_liquido_frenos', 'Líquido de Frenos/Embrague'),
            ('mec_nivel_refrigerante', 'Nivel de Refrigerante'), ('mec_estado_correas', 'Estado de Correas'),
            ('mec_ausencia_fugas', 'Ausencia Fugas (Aceite/Agua/Aire)')
        ]),
        ("SISTEMA DE LUCES", False, [
            ('luc_altas_bajas', 'Luces Altas y Bajas'), ('luc_frenos_stop', 'Luces de Freno (Stop)'),
            ('luc_direccionales', 'Luces Direccionales'), ('luc_parqueo_estacionarias', 'Luces de Parqueo/Estacionarias'),
            ('luc_reversa_alarma', 'Luz y Alarma de Reversa'), ('luc_delimitadoras_cocuyos', 'Luces Delimitadoras (Cocuyos)')
        ]),
        ("SUSPENSIÓN Y REPUESTO", False, [
            ('lla_tuercas_pernos', 'Tuercas y Pernos Completos'), ('lla_repuesto_operativa', 'Llanta Repuesto Operativa'),
            ('lla_suspension_muelles', 'Suspensión y Muelles')
        ]),
        ("FRENOS Y MANDOS DE CABINA", False, [
            ('fre_pedal_firme', 'Firmeza Pedal de Freno'), ('fre_parqueo_mano', 'Freno de Parqueo/Mano'),
            ('fre_presion_aire_manometro', 'Manómetro Presión Aire'), ('fre_juego_direccion', 'Juego de Dirección'),
            ('fre_pito_corneta', 'Pito y Corneta'), ('fre_limpiaparabrisas_plumillas', 'Limpiaparabrisas y Plumillas')
        ]),
        ("CARROCERÍA Y ESTRUCTURA", False, [
            ('car_estado_estructura', 'Estado de Estructura General'), ('car_compuertas_carpas_amarres', 'Compuertas, Carpas y Amarres'),
            ('car_cinturones_seguridad', 'Cinturones de Seguridad'), ('car_espejos_retrovisores', 'Espejos Retrovisores'),
            ('car_vidrio_parabrisas', 'Vidrio Parabrisas')
        ]),
        ("EQUIPO DE PREVENCIÓN", False, [
            ('equ_extintor_10lbs', 'Extintor Cargado'), ('equ_tacos_bloqueo', 'Tacos de Bloqueo'),
            ('equ_senales_reflectivas', 'Señales Reflectivas'), ('equ_gato_hidraulico', 'Gato Hidráulico'),
            ('equ_cruceta_herramientas', 'Cruceta y Herramientas'), ('equ_botiquin_completo', 'Botiquín Completo')
        ])
    ]

    try: novedades_dict = json.loads(insp.get('detalles_novedades_json') or '{}')
    except: novedades_dict = {}

    story.append(Spacer(1, 10))

    for titulo, es_doc, campos in checklist_config:
        story.append(Paragraph(f"<b>{titulo}</b>", sub_title_style))
        tabla_datos = [["Ítem Inspeccionado", "Estado", "Observación / Novedad"]]
        for campo_db, label in campos:
            valor = insp.get(campo_db)
            estado_lbl = get_estado_html(valor, es_doc)
            obs = novedades_dict.get(campo_db, {}).get('detalle', 'Sin novedad') if valor in [2, 3] else ''
            tabla_datos.append([Paragraph(label, cell_style), Paragraph(estado_lbl, cell_style), Paragraph(obs, cell_style)])
        t_grupo = Table(tabla_datos, colWidths=[200, 80, 260])
        t_grupo.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')), ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOTTOMPADDING', (0,0), (-1,-1), 2), ('TOPPADDING', (0,0), (-1,-1), 2)
        ]))
        story.append(t_grupo)
        story.append(Spacer(1, 5))

    story.append(Paragraph("<b>ESQUEMA POSICIONAL DE LLANTAS</b>", sub_title_style))
    try: llantas_dict = json.loads(insp.get('estado_llantas_json') or '{}')
    except: llantas_dict = {}

    if llantas_dict:
        llantas_data = [["Posición de la Llanta", "Estado Labrado", "Novedad Reportada"]]
        for pos, l_data in llantas_dict.items():
            lab_txt = str(l_data.get('labrado')).upper()
            color_l = "#16a34a" if lab_txt == 'OPERATIVA' else ("#dc2626" if lab_txt == 'LISA' else "#d97706")
            llantas_data.append([
                Paragraph(l_data.get('nombre_legible', pos), cell_style),
                Paragraph(f"<font color='{color_l}'><b>{lab_txt}</b></font>", cell_style),
                Paragraph(l_data.get('novedad', ''), cell_style)
            ])
        t_llan = Table(llantas_data, colWidths=[200, 80, 260])
        t_llan.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')), ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOTTOMPADDING', (0,0), (-1,-1), 2)
        ]))
        story.append(t_llan)
    else:
        story.append(Paragraph("No se registró esquema posicional de llantas en esta inspección.", cell_style))

    def convertir_base64_rlimage(b64_string, w, h):
        try:
            if b64_string and ',' in b64_string:
                img_data = base64.b64decode(b64_string.split(',')[1])
                img_buffer = io.BytesIO(img_data)
                return RLImage(img_buffer, width=w, height=h, kind='proportional')
        except: pass
        return Paragraph("<i>No disponible</i>", cell_style)

    if insp.get('observaciones_hallazgos'):
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>OBSERVACIONES GENERALES DEL CONDUCTOR</b>", sub_title_style))
        story.append(Paragraph(f"<i>{insp['observaciones_hallazgos']}</i>", cell_style))

    story.append(Spacer(1, 15))
    story.append(Paragraph("<b>AUTENTICACIÓN Y FIRMA</b>", sub_title_style))
    declaracion_texto = "<b>Declaración de Veracidad y Cumplimiento Normativo:</b> Declaro bajo la gravedad de juramento que la información aquí registrada es veraz, exacta y ha sido recolectada mediante inspección física directa del vehículo. Este registro preoperacional da cumplimiento estricto al <b>Paso 16 de la Metodología del Plan Estratégico de Seguridad Vial (PESV)</b>."
    story.append(Paragraph(declaracion_texto, cell_style))
    story.append(Spacer(1, 10))
    
    img_firma = convertir_base64_rlimage(insp.get('firma_grafica_base64'), 2*inch, 1*inch)
    img_foto = convertir_base64_rlimage(insp.get('foto_conductor_base64'), 1.2*inch, 1.2*inch)

    firma_data = [[Paragraph("<b>Foto Auditoría:</b>", cell_style), Paragraph("<b>Firma Gráfica:</b>", cell_style)], [img_foto, img_firma]]
    t_firma = Table(firma_data, colWidths=[150, 200])
    t_firma.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.HexColor('#16a34a')), ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0fdf4')), ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
    story.append(t_firma)

    doc.build(story)
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name=f"Preoperacional_Especial_{consecutivo}.pdf", mimetype='application/pdf')

# =========================================================
# DESCARGA QR ESTRUCTURADO EN PDF (NUEVO REQUERIMIENTO)
# =========================================================
@bp_flotaespecial_vehiculos.route('/vehiculo/qr_pdf/<placa>', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def descargar_qr_vehiculo_pdf(placa):
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    nit_empresa = session.get('nit')

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=30, leftMargin=30, topMargin=50, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=24, textColor=colors.HexColor('#015249'), alignment=1, spaceAfter=20)
    placa_style = ParagraphStyle('Placa', parent=styles['Heading1'], fontSize=48, textColor=colors.HexColor('#111827'), alignment=1, spaceBefore=20, spaceAfter=10)
    subtitle_style = ParagraphStyle('SubTitle', parent=styles['Normal'], fontSize=14, textColor=colors.HexColor('#6b7280'), alignment=1, spaceAfter=30)
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'], fontSize=12, textColor=colors.HexColor('#374151'), alignment=1, spaceBefore=30)

    base_dir = os.path.abspath(os.path.dirname(__file__))
    static_dir = os.path.join(base_dir, '..', 'static')
    logo_cliente_path = os.path.join(static_dir, f'logo_{nit_empresa}.PNG')
    
    if os.path.exists(logo_cliente_path):
        img_cliente = RLImage(logo_cliente_path, width=2.5*inch, height=1*inch, kind='proportional')
        story.append(img_cliente)
        story.append(Spacer(1, 20))
    else:
        story.append(Paragraph(empresa_nombre, title_style))

    story.append(Paragraph("<b>TRANSPORTE ESPECIAL</b>", title_style))
    story.append(Paragraph("Escanear para Operar", subtitle_style))

    try:
        payload = json.dumps({"placa": placa, "nit": str(empresa_id)}) 
        qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data={urllib.parse.quote(payload)}&margin=0"
        resp = requests.get(qr_url, timeout=10)
        if resp.status_code == 200:
            img_buffer = io.BytesIO(resp.content)
            qr_img = RLImage(img_buffer, width=4*inch, height=4*inch)
            t_qr = Table([[qr_img]], colWidths=[500])
            t_qr.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
            story.append(t_qr)
        else:
            story.append(Paragraph("[Error generando QR]", subtitle_style))
    except Exception as e:
        story.append(Paragraph(f"[Error de conexión QR: {str(e)}]", subtitle_style))

    story.append(Paragraph(f"<b>{placa.upper()}</b>", placa_style))
    story.append(Paragraph(f"Propiedad de: {empresa_nombre}", footer_style))
    story.append(Paragraph("<b>BQA-ONE</b>", ParagraphStyle('BQA', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#015249'), alignment=1, spaceBefore=10)))

    doc.build(story)
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name=f"QR_Vehiculo_{placa}.pdf", mimetype='application/pdf')