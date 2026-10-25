# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: GESTION_FLOTA | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/B_bp_flotacarga_flota.py
import os
import time
import json
import io
import base64
import smtplib
import threading
import qrcode
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import make_msgid, formatdate
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, current_app, send_file, jsonify
from werkzeug.utils import secure_filename
from PIL import Image
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

# Librerías PDF (Reportes Platypus)
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

bp_flotacarga_flota = Blueprint('flotacarga_flota', __name__, url_prefix='/gestor_flota/flota_bp')

# ==============================================================================
# CONFIGURACIÓN GLOBAL DE EMAIL CORPORATIVO
# ==============================================================================
EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.hostinger.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "465"))
EMAIL_USER = os.environ.get("EMAIL_USER", "bqa-one@baquia-esm.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

# ==============================================================================
# RUTINA LOCAL DE AUDITORÍA (SANA ERROR 500 TRAS COMMIT)
# ==============================================================================
def registrar_operacion_bqa_local(cur, accion, datos_afectados):
    """
    Registra operaciones CRUD en la tabla global de auditoría exigida, 
    atrapando cualquier excepción para no romper la ejecución del backend.
    """
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS registro_operaciones_bqa_one (
                id INT AUTO_INCREMENT PRIMARY KEY,
                fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                empresa VARCHAR(150),
                empresa_id VARCHAR(50),
                backend_emisor VARCHAR(100),
                usuario VARCHAR(100),
                cedula VARCHAR(50),
                accion VARCHAR(100),
                datos JSON
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        emp = session.get('empresa', 'Sistema')
        emp_id = session.get('empresa_id', 'N/A')
        usr = session.get('nombre', 'N/A')
        ced = session.get('cedula', 'N/A')
        cur.execute("""
            INSERT INTO registro_operaciones_bqa_one 
            (empresa, empresa_id, backend_emisor, usuario, cedula, accion, datos) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (emp, emp_id, 'B_bp_flotacarga_flota.py', usr, ced, accion, json.dumps(datos_afectados)))
    except Exception as e:
        print(f"Error auditoría local BQA-ONE en flota: {e}")

# ==============================================================================
# RUTINAS AUXILIARES Y DE INTEGRIDAD DE DATOS PESV
# ==============================================================================
def _enviar_email_vencimientos_carga_hilo(destinatarios, empresa_nombre, alertas_vehiculos, alertas_conductores):
    def tarea():
        if not EMAIL_USER or not EMAIL_PASS or not destinatarios:
            return

        vehiculos_html = ""
        for a in alertas_vehiculos:
            color_estado = "#c62828" if a[3] in ["VENCIDO", "FALTANTE"] else "#d97706"
            bg_estado = "#ffebee" if a[3] in ["VENCIDO", "FALTANTE"] else "#fef3c7"
            vehiculos_html += f"""
                <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 10px; text-align: center;"><strong>{a[0]}</strong></td>
                    <td style="padding: 10px; text-align: center;">{a[1]}</td>
                    <td style="padding: 10px; text-align: center;">{a[2]}</td>
                    <td style="padding: 10px; text-align: center; background-color: {bg_estado}; color: {color_estado}; font-weight: bold;">{a[3]}</td>
                    <td style="padding: 10px; text-align: center;">{a[4]}</td>
                </tr>
            """

        conductores_html = ""
        for a in alertas_conductores:
            color_estado = "#c62828" if a[5] in ["VENCIDO", "FALTANTE"] else "#d97706"
            bg_estado = "#ffebee" if a[5] in ["VENCIDO", "FALTANTE"] else "#fef3c7"
            conductores_html += f"""
                <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 10px;"><strong>{a[0]}</strong><br><span style="font-size: 11px; color: #777;">ID: {a[1]} | Tel: {a[2]}</span></td>
                    <td style="padding: 10px; text-align: center;">{a[3]}</td>
                    <td style="padding: 10px; text-align: center;">{a[4]}</td>
                    <td style="padding: 10px; text-align: center; background-color: {bg_estado}; color: {color_estado}; font-weight: bold;">{a[5]}</td>
                    <td style="padding: 10px; text-align: center;">{a[6]}</td>
                </tr>
            """

        cuerpo = f"""
        <!DOCTYPE html>
        <html>
        <head>
        <style>
            body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
            .container {{ max-width: 700px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
            .header {{ background-color: #015249; color: #ffffff; padding: 20px; text-align: center; }}
            .header h2 {{ margin: 0; font-size: 22px; }}
            .content {{ padding: 25px; }}
            .alert-box {{ background-color: #ffebee; border-left: 4px solid #c62828; color: #c62828; padding: 15px; border-radius: 4px; font-size: 15px; margin-bottom: 20px; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 15px; margin-bottom: 25px; }}
            th {{ background-color: #f2f2f2; padding: 10px; text-align: center; color: #555; }}
            .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
        </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>⚠️ Auditoría de Vencimientos - Flota de Carga</h2>
                </div>
                <div class="content">
                    <div class="alert-box">
                        <strong>Atención:</strong> Se detectaron documentos vencidos o próximos a vencer (≤ 8 días) en la flota operativa de <strong>{empresa_nombre}</strong>.
                    </div>

                    {f'''<h3 style="color: #015249; border-bottom: 1px solid #ddd; padding-bottom: 5px;">🚗 Vehículos</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Placa</th>
                                <th>Documento</th>
                                <th>Fecha Venc.</th>
                                <th>Estado</th>
                                <th>Días Restantes</th>
                            </tr>
                        </thead>
                        <tbody>
                            {vehiculos_html}
                        </tbody>
                    </table>''' if alertas_vehiculos else ''}

                    {f'''<h3 style="color: #015249; border-bottom: 1px solid #ddd; padding-bottom: 5px;">👷 Conductores</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Conductor</th>
                                <th>Documento</th>
                                <th>Fecha Venc.</th>
                                <th>Estado</th>
                                <th>Días Restantes</th>
                            </tr>
                        </thead>
                        <tbody>
                            {conductores_html}
                        </tbody>
                    </table>''' if alertas_conductores else ''}

                    <p style="margin-top: 20px; font-size: 12px; color: #777;">
                        * Por favor, ingrese a la plataforma BQA-ONE para realizar la refrendación de estos documentos y evitar bloqueos en la operación.
                    </p>
                </div>
                <div class="footer">
                    <p>Reporte automático generado por <strong>BQA-ONE / Energix360</strong>.</p>
                </div>
            </div>
        </body>
        </html>
        """

        try:
            msg = MIMEMultipart()
            msg["Subject"] = f"🚨 Alerta Vencimientos PESV - Flota Carga {empresa_nombre}"
            msg["From"] = EMAIL_FROM
            msg["To"] = ", ".join(destinatarios)
            msg["Message-ID"] = make_msgid()
            msg["Date"] = formatdate(localtime=True)
            msg["Reply-To"] = EMAIL_FROM

            msg.attach(MIMEText(cuerpo, "html", "utf-8"))

            try:
                with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
                    server.login(EMAIL_USER, EMAIL_PASS)
                    server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
            except Exception:
                with smtplib.SMTP(EMAIL_HOST, 587) as server:
                    server.starttls()
                    server.login(EMAIL_USER, EMAIL_PASS)
                    server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
        except Exception as e:
            print(f"Error despachando correo de auditoría vencimientos: {e}")

    hilo = threading.Thread(target=tarea)
    hilo.daemon = True
    hilo.start()

def asegurar_tablas_carga():
    try:
        cur = mysql.connection.cursor()
        cur.execute("SHOW COLUMNS FROM vehiculos LIKE 'estado_soat'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE vehiculos ADD COLUMN estado_soat VARCHAR(50) DEFAULT 'Actualizado'")
            cur.execute("ALTER TABLE vehiculos ADD COLUMN estado_tecnomecanica VARCHAR(50) DEFAULT 'Actualizado'")
            cur.execute("ALTER TABLE vehiculos ADD COLUMN estado_poliza_rce VARCHAR(50) DEFAULT 'Actualizado'")
            
        cur.execute("SHOW COLUMNS FROM conductores_carga LIKE 'estado_licencia'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE conductores_carga ADD COLUMN estado_licencia VARCHAR(50) DEFAULT 'Actualizado'")
            cur.execute("ALTER TABLE conductores_carga ADD COLUMN estado_seguridad_social VARCHAR(50) DEFAULT 'Actualizado'")
            cur.execute("ALTER TABLE conductores_carga ADD COLUMN fecha_expedicion_licencia DATE DEFAULT NULL")
            cur.execute("ALTER TABLE conductores_carga ADD COLUMN categoria_licencia VARCHAR(50) DEFAULT 'Publico'")
        
        mysql.connection.commit()
        cur.close()
    except Exception as e:
        print(f"Aviso asegurando tablas de carga: {e}")

def calcular_vencimiento_ss(cedula, ultimo_pago_str):
    if not ultimo_pago_str or not cedula: return None
    try:
        ult_pago = datetime.strptime(str(ultimo_pago_str).strip(), '%Y-%m-%d').date()
        cedula_str = ''.join(filter(str.isdigit, str(cedula)))
        if not cedula_str: return None
        ult_digitos = int(cedula_str[-2:]) if len(cedula_str) >= 2 else int(cedula_str)
        
        dia_habil = 2
        if 8 <= ult_digitos <= 14: dia_habil = 3
        elif 15 <= ult_digitos <= 21: dia_habil = 4
        elif 22 <= ult_digitos <= 28: dia_habil = 5
        elif 29 <= ult_digitos <= 35: dia_habil = 6
        elif 36 <= ult_digitos <= 42: dia_habil = 7
        elif 43 <= ult_digitos <= 49: dia_habil = 8
        elif 50 <= ult_digitos <= 56: dia_habil = 9
        elif 57 <= ult_digitos <= 63: dia_habil = 10
        elif 64 <= ult_digitos <= 69: dia_habil = 11
        elif 70 <= ult_digitos <= 75: dia_habil = 12
        elif 76 <= ult_digitos <= 81: dia_habil = 13
        elif 82 <= ult_digitos <= 87: dia_habil = 14
        elif 88 <= ult_digitos <= 93: dia_habil = 15
        elif 94 <= ult_digitos <= 99: dia_habil = 16
        
        mes_siguiente = ult_pago.replace(day=1) + relativedelta(months=1)
        dias_contados = 0
        fecha_actual = mes_siguiente
        
        while True:
            if fecha_actual.weekday() < 5:
                dias_contados += 1
            if dias_contados == dia_habil:
                break
            fecha_actual += timedelta(days=1)
                
        return fecha_actual.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error calculando vencimiento SS: {e}")
        return None

def calcular_vencimiento_licencia(fecha_exp_str, fecha_nac_str, categoria):
    if not fecha_exp_str or not fecha_nac_str: return None
    try:
        f_exp = datetime.strptime(str(fecha_exp_str).strip(), '%Y-%m-%d').date()
        f_nac = datetime.strptime(str(fecha_nac_str).strip(), '%Y-%m-%d').date()
        edad_exp = relativedelta(f_exp, f_nac).years
        cat = str(categoria).strip().capitalize()
        
        if cat == 'Particular':
            if edad_exp < 60: f_teo = f_exp + relativedelta(years=10)
            elif 60 <= edad_exp <= 79: f_teo = f_exp + relativedelta(years=5)
            else: f_teo = f_exp + relativedelta(years=1)
        else:
            if edad_exp < 60: f_teo = f_exp + relativedelta(years=3)
            else: f_teo = f_exp + relativedelta(years=1)
            
        return f_teo.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error calculando vencimiento Licencia: {e}")
        return None

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def gestor_flota_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        # INYECCIÓN PERFIL CONTROLADOR_PESV
        perfiles_permitidos = [
            'gestor_flotacarga', 
            'controlador_transportecarga', 
            'controlador_flotacarga', 
            'controlador_logistica',
            'controlador_pesv', 
            'webmaster'
        ]
        
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Gestor/Controlador de Flota o PESV.', 'danger')
            return redirect(url_for('panel_principal'))
            
        if 'webmaster' not in tipo_empresa and perfil != 'controlador_pesv':
            autorizados = session.get('submodulos_activos', [])
            if not autorizados:
                autorizados = session.get('modulos_activos', [])
                
            if not any(m in autorizados for m in ['controlador_flota', 'flota', 'carga']):
                flash('Acceso denegado: Tu empresa no tiene activo este submódulo.', 'danger')
                return redirect(url_for('panel_principal'))
                
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# MOTOR DE CONVERSIÓN Y PROCESAMIENTO DE ARCHIVOS
# ==============================================================================
def procesar_archivo_pdf(archivo, identificador, tipo_doc, empresa_id):
    if not archivo or archivo.filename == '':
        return None
    
    filename = secure_filename(archivo.filename)
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    
    base_dir = os.path.join(current_app.root_path, 'static', 'uploads', 'flotacarga', 'documentos', str(empresa_id), str(identificador).upper())
    os.makedirs(base_dir, exist_ok=True)
    
    timestamp = int(time.time())
    
    if tipo_doc == 'FOTO_FACIAL':
        nuevo_nombre = f"{tipo_doc}_{identificador}_{timestamp}.{ext}"
        ruta_fisica = os.path.join(base_dir, nuevo_nombre)
        ruta_bd = f"uploads/flotacarga/documentos/{empresa_id}/{str(identificador).upper()}/{nuevo_nombre}"
        try:
            archivo.save(ruta_fisica)
            return ruta_bd
        except Exception as e:
            print(f"Error procesando foto facial: {e}")
            return None

    nuevo_nombre = f"{tipo_doc}_{identificador}_{timestamp}.pdf"
    ruta_fisica = os.path.join(base_dir, nuevo_nombre)
    ruta_bd = f"uploads/flotacarga/documentos/{empresa_id}/{str(identificador).upper()}/{nuevo_nombre}"
    
    try:
        if ext == 'pdf':
            archivo.save(ruta_fisica)
        elif ext in ['jpg', 'jpeg', 'png', 'webp']:
            image = Image.open(archivo)
            if image.mode != 'RGB':
                image = image.convert('RGB')
            image.save(ruta_fisica, 'PDF', resolution=100.0)
        else:
            return None
        return ruta_bd
    except Exception as e:
        print(f"Error procesando archivo a PDF: {e}")
        return None

# =========================================================
# RUTAS DEL SUBMÓDULO (VEHÍCULOS Y CONDUCTORES)
# =========================================================
@bp_flotacarga_flota.route('/')
@login_required_custom
@gestor_flota_required
def index():
    return redirect(url_for('flotacarga_flota.gestion_vehiculos'))

@bp_flotacarga_flota.route('/vehiculos', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_vehiculos():
    asegurar_tablas_carga()
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        placa = str(request.form.get('placa', '')).upper().strip()
        tipo = (request.form.get('tipo') or request.form.get('clasificacion_runt_vehiculo') or '').strip()
        caja_de_carga = request.form.get('caja_de_carga', '').strip()
        referencia = request.form.get('referencia', '').strip()
        peso_vacio = request.form.get('peso_vacio', 0)
        capacidad = request.form.get('capacidad', 0)
        propiedad = request.form.get('propiedad', 'Propio').strip()
        
        id_tarjeta_propiedad = request.form.get('id_tarjeta_propiedad', '').strip()
        id_soat = request.form.get('id_soat', '').strip()
        vencimiento_soat = request.form.get('vencimiento_soat', '').strip() or None
        id_tecnomecanica = request.form.get('id_tecnomecanica', '').strip()
        vencimiento_tecnomecanica = request.form.get('vencimiento_tecnomecanica', '').strip() or None
        id_poliza_rce = request.form.get('id_poliza_rce', '').strip()
        aseguradora_rce = request.form.get('aseguradora_rce', '').strip()
        vencimiento_poliza_rce = request.form.get('vencimiento_poliza_rce', '').strip() or None

        if accion == 'crear':
            if placa and caja_de_carga and tipo:
                ruta_tarjeta = procesar_archivo_pdf(request.files.get('archivo_tarjeta_propiedad'), placa, 'TARJETA_PROP', empresa_id)
                ruta_soat = procesar_archivo_pdf(request.files.get('archivo_soat'), placa, 'SOAT', empresa_id)
                ruta_tecno = procesar_archivo_pdf(request.files.get('archivo_tecnomecanica'), placa, 'TECNOMECANICA', empresa_id)
                ruta_rce = procesar_archivo_pdf(request.files.get('archivo_poliza_rce'), placa, 'POLIZA_RCE', empresa_id)

                cur = mysql.connection.cursor()
                try:
                    # BIFURCACIÓN DE TABLA SEGÚN TIPO DE EMPRESA
                    if tipo_empresa == 'transporte_especial':
                        cur.execute("""
                            INSERT INTO vehiculos_especial (
                                id_empresa, placa, clase, carroceria, linea, capacidad, tipo_vinculacion,
                                ruta_pdf_tarjeta_propiedad,
                                numero_poliza_soat, vencimiento_soat, ruta_pdf_soat,
                                numero_certificado_rtm, vencimiento_rtm, ruta_pdf_tecnomecanica,
                                numero_poliza_rcc_rce, aseguradora_rcc_rce, vencimiento_rcc_rce, ruta_pdf_rcc_rce,
                                estado_soat, estado_rtm, estado_rcc_rce
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Actualizado', 'Actualizado', 'Actualizado')
                        """, (
                            empresa_id, placa, tipo, caja_de_carga, referencia, capacidad, propiedad,
                            ruta_tarjeta,
                            id_soat, vencimiento_soat, ruta_soat,
                            id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecno,
                            id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_rce
                        ))
                    else:
                        cur.execute("""
                            INSERT INTO vehiculos (
                                empresa, id_empresa, placa, tipo, caja_de_carga, referencia, peso_vacio, `capacidad (kg)`, propiedad,
                                id_tarjeta_propiedad, ruta_tarjeta_propiedad,
                                id_soat, vencimiento_soat, ruta_soat,
                                id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecnomecanica,
                                id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_poliza_rce,
                                estado_soat, estado_tecnomecanica, estado_poliza_rce
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Actualizado', 'Actualizado', 'Actualizado')
                        """, (
                            empresa_nombre, empresa_id, placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, propiedad,
                            id_tarjeta_propiedad, ruta_tarjeta,
                            id_soat, vencimiento_soat, ruta_soat,
                            id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecno,
                            id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_rce
                        ))
                    
                    registrar_operacion_bqa_local(cur, 'CREAR_VEHICULO', {'placa': placa, 'tipo_empresa': tipo_empresa})
                    mysql.connection.commit()
                    flash(f"Vehículo con placa {placa} registrado correctamente.", "success")
                except Exception as e:
                    mysql.connection.rollback()
                    for ruta_relativa in filter(None, [ruta_tarjeta, ruta_soat, ruta_tecno, ruta_rce]):
                        ruta_fisica = os.path.join(current_app.static_folder, ruta_relativa)
                        if os.path.exists(ruta_fisica):
                            try: os.remove(ruta_fisica)
                            except: pass
                            
                    if hasattr(e, 'args') and e.args[0] == 1062:
                        flash("La placa del vehículo ya se encuentra registrada en el sistema.", "warning")
                    else:
                        print(f"Error Inserción DB Vehículos: {e}")
                        flash("Ocurrió un error al registrar el vehículo. Verifica los datos.", "danger")
                finally:
                    cur.close()

        elif accion == 'editar':
            vehiculo_id = request.form.get('vehiculo_id')
            if vehiculo_id and placa and caja_de_carga and tipo:
                cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
                try:
                    tabla_consulta = 'vehiculos_especial' if tipo_empresa == 'transporte_especial' else 'vehiculos'
                    cur.execute(f"SELECT * FROM {tabla_consulta} WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                    old_data = cur.fetchone()
                    
                    if old_data:
                        new_ruta_tarjeta = procesar_archivo_pdf(request.files.get('archivo_tarjeta_propiedad'), placa, 'TARJETA_PROP', empresa_id)
                        new_ruta_soat = procesar_archivo_pdf(request.files.get('archivo_soat'), placa, 'SOAT', empresa_id)
                        new_ruta_tecno = procesar_archivo_pdf(request.files.get('archivo_tecnomecanica'), placa, 'TECNOMECANICA', empresa_id)
                        new_ruta_rce = procesar_archivo_pdf(request.files.get('archivo_poliza_rce'), placa, 'POLIZA_RCE', empresa_id)
                        
                        if tipo_empresa == 'transporte_especial':
                            ruta_tarjeta = new_ruta_tarjeta or old_data.get('ruta_pdf_tarjeta_propiedad')
                            ruta_soat = new_ruta_soat or old_data.get('ruta_pdf_soat')
                            ruta_tecno = new_ruta_tecno or old_data.get('ruta_pdf_tecnomecanica')
                            ruta_rce = new_ruta_rce or old_data.get('ruta_pdf_rcc_rce')
                            
                            cur.execute("""
                                UPDATE vehiculos_especial 
                                SET placa = %s, clase = %s, carroceria = %s, linea = %s, capacidad = %s, tipo_vinculacion = %s,
                                    ruta_pdf_tarjeta_propiedad = %s,
                                    numero_poliza_soat = %s, vencimiento_soat = %s, ruta_pdf_soat = %s,
                                    numero_certificado_rtm = %s, vencimiento_rtm = %s, ruta_pdf_tecnomecanica = %s,
                                    numero_poliza_rcc_rce = %s, aseguradora_rcc_rce = %s, vencimiento_rcc_rce = %s, ruta_pdf_rcc_rce = %s,
                                    estado_soat = 'Actualizado', estado_rtm = 'Actualizado', estado_rcc_rce = 'Actualizado'
                                WHERE id = %s AND id_empresa = %s
                            """, (
                                placa, tipo, caja_de_carga, referencia, capacidad, propiedad,
                                ruta_tarjeta,
                                id_soat, vencimiento_soat, ruta_soat,
                                id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecno,
                                id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_rce,
                                vehiculo_id, empresa_id
                            ))
                        else:
                            ruta_tarjeta = new_ruta_tarjeta or old_data.get('ruta_tarjeta_propiedad')
                            ruta_soat = new_ruta_soat or old_data.get('ruta_soat')
                            ruta_tecno = new_ruta_tecno or old_data.get('ruta_tecnomecanica')
                            ruta_rce = new_ruta_rce or old_data.get('ruta_poliza_rce')
                            
                            cur.execute("""
                                UPDATE vehiculos 
                                SET placa = %s, tipo = %s, caja_de_carga = %s, referencia = %s, peso_vacio = %s, `capacidad (kg)` = %s, propiedad = %s,
                                    id_tarjeta_propiedad = %s, ruta_tarjeta_propiedad = %s,
                                    id_soat = %s, vencimiento_soat = %s, ruta_soat = %s,
                                    id_tecnomecanica = %s, vencimiento_tecnomecanica = %s, ruta_tecnomecanica = %s,
                                    id_poliza_rce = %s, aseguradora_rce = %s, vencimiento_poliza_rce = %s, ruta_poliza_rce = %s,
                                    estado_soat = 'Actualizado', estado_tecnomecanica = 'Actualizado', estado_poliza_rce = 'Actualizado'
                                WHERE id = %s AND id_empresa = %s
                            """, (
                                placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, propiedad,
                                id_tarjeta_propiedad, ruta_tarjeta,
                                id_soat, vencimiento_soat, ruta_soat,
                                id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecno,
                                id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_rce,
                                vehiculo_id, empresa_id
                            ))
                        registrar_operacion_bqa_local(cur, 'EDITAR_VEHICULO', {'placa': placa, 'id': vehiculo_id})
                        mysql.connection.commit()
                        flash(f"Vehículo {placa} actualizado correctamente.", "success")
                except Exception as e:
                    mysql.connection.rollback()
                    for ruta_relativa in filter(None, [new_ruta_tarjeta, new_ruta_soat, new_ruta_tecno, new_ruta_rce]):
                        ruta_fisica = os.path.join(current_app.static_folder, ruta_relativa)
                        if os.path.exists(ruta_fisica):
                            try: os.remove(ruta_fisica)
                            except: pass
                            
                    if hasattr(e, 'args') and e.args[0] == 1062:
                        flash("La placa del vehículo ya se encuentra registrada en el sistema.", "warning")
                    else:
                        flash("Ocurrió un error al procesar el formulario de edición.", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            vehiculo_id = request.form.get('vehiculo_id')
            cur = mysql.connection.cursor()
            try:
                tabla_borrado = 'vehiculos_especial' if tipo_empresa == 'transporte_especial' else 'vehiculos'
                cur.execute(f"DELETE FROM {tabla_borrado} WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                registrar_operacion_bqa_local(cur, 'ELIMINAR_VEHICULO', {'id': vehiculo_id, 'tabla': tabla_borrado})
                mysql.connection.commit()
                flash("Vehículo eliminado de la base de datos.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash("Ocurrió una interrupción al intentar eliminar el vehículo.", "danger")
            finally:
                cur.close()

        return redirect(url_for('flotacarga_flota.gestion_vehiculos'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    if tipo_empresa == 'transporte_especial':
        cur.execute("""
            SELECT id, placa, clase as tipo, carroceria as caja_de_carga, linea as referencia, 0 as peso_vacio, capacidad as `capacidad (kg)`, tipo_vinculacion as propiedad, 
                   NULL as id_tarjeta_propiedad, numero_poliza_soat as id_soat, vencimiento_soat, 
                   numero_certificado_rtm as id_tecnomecanica, vencimiento_rtm as vencimiento_tecnomecanica, 
                   numero_poliza_rcc_rce as id_poliza_rce, aseguradora_rcc_rce as aseguradora_rce, vencimiento_rcc_rce as vencimiento_poliza_rce 
            FROM vehiculos_especial WHERE id_empresa = %s ORDER BY id DESC
        """, (empresa_id,))
    else:
        cur.execute("SELECT * FROM vehiculos WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_flotacarga_flota.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='vehiculos', 
        vehiculos=vehiculos_db
    )

# ==============================================================================
# NUEVO: GENERACIÓN NATIVA DE QR EN FORMATO PDF DESDE EL SERVIDOR
# ==============================================================================
@bp_flotacarga_flota.route('/vehiculos/qr/<placa>', methods=['GET'])
@login_required_custom
@gestor_flota_required
def generar_qr_vehiculo(placa):
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    nit_empresa = session.get('nit')
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    if tipo_empresa == 'transporte_especial':
        cur.execute("SELECT clase as tipo FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s LIMIT 1", (placa, empresa_id))
    else:
        cur.execute("SELECT tipo FROM vehiculos WHERE placa = %s AND id_empresa = %s LIMIT 1", (placa, empresa_id))
    vehiculo = cur.fetchone()
    cur.close()

    if not vehiculo:
        flash("Vehículo no encontrado o no pertenece a la empresa.", "danger")
        return redirect(url_for('flotacarga_flota.gestion_vehiculos'))

    tipo = vehiculo.get('tipo', '')
    
    payload_dict = {
        "placa": placa,
        "nit": str(nit_empresa),
        "tipo": tipo
    }
    
    payload_str = json.dumps(payload_dict, ensure_ascii=False)
    
    qr = qrcode.QRCode(
        version=1, 
        error_correction=qrcode.constants.ERROR_CORRECT_M, 
        box_size=10, 
        border=1
    )
    qr.add_data(payload_str.encode('utf-8'))
    qr.make(fit=True)
    img_qr = qr.make_image(fill_color="#015249", back_color="white")
    
    qr_io = io.BytesIO()
    img_qr.save(qr_io, format='PNG')
    qr_io.seek(0)
    
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=(4*inch, 5*inch))
    
    base_dir = os.path.abspath(os.path.dirname(__file__))
    static_dir = os.path.join(base_dir, '..', 'static')
    logo_cliente_path = os.path.join(static_dir, f'logo_{nit_empresa}.PNG')
    logo_app_path = os.path.join(static_dir, 'logo_energix360.png')
    
    pdf.setFillColor(colors.white)
    pdf.rect(0, 0, 4*inch, 5*inch, fill=1)
    
    if os.path.exists(logo_cliente_path):
        pdf.drawImage(logo_cliente_path, 0.5*inch, 3.6*inch, width=3*inch, height=1.2*inch, preserveAspectRatio=True, anchor='c')
    else:
        pdf.setFillColor(colors.HexColor('#015249'))
        pdf.setFont("Helvetica-Bold", 16)
        pdf.drawCentredString(2*inch, 4.2*inch, empresa_nombre)

    pdf.setFillColor(colors.black)
    pdf.setFont("Helvetica-Bold", 26)
    pdf.drawCentredString(2*inch, 3.1*inch, placa.upper())
    
    pdf.setFont("Helvetica", 10)
    pdf.drawCentredString(2*inch, 2.8*inch, f"Propiedad de: {empresa_nombre}")
    
    pdf.drawImage(ImageReader(qr_io), 1*inch, 0.7*inch, width=2*inch, height=2*inch, preserveAspectRatio=True)
    
    if os.path.exists(logo_app_path):
        pdf.drawImage(logo_app_path, 1.5*inch, 0.2*inch, width=1*inch, height=0.4*inch, preserveAspectRatio=True, anchor='c')
    else:
        pdf.setFillColor(colors.HexColor('#015249'))
        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawCentredString(2*inch, 0.3*inch, "BQA ONE")
        
    pdf.showPage()
    pdf.save()
    buffer.seek(0)
    
    return send_file(
        buffer, 
        as_attachment=True, 
        download_name=f"QR_{placa}.pdf", 
        mimetype='application/pdf'
    )

@bp_flotacarga_flota.route('/operadores', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_operadores():
    asegurar_tablas_carga()
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
    tabla_cond = 'conductores_flotaespecial' if tipo_empresa == 'transporte_especial' else 'conductores_carga'

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'crear':
            tipo_conductor = request.form.get('tipo_conductor', 'Propio').strip()
            perfil = request.form.get('perfil', 'operador_flotacarga').strip()
            nombre = request.form.get('nombre', '').strip()
            cedula = request.form.get('cedula', '').strip()
            email = request.form.get('email', '').strip()
            password = request.form.get('password', '').strip()
            vehiculo_asignado = request.form.get('vehiculo_asignado', '').strip() if tipo_conductor == 'Tercero' else None
            
            fecha_nacimiento = request.form.get('fecha_nacimiento', '').strip() or None
            edad = request.form.get('edad', '').strip() or None
            eps = request.form.get('eps', '').strip()
            arl = request.form.get('arl', '').strip()
            fondo_pensiones = request.form.get('fondo_pensiones', '').strip()
            
            numero_licencia_conduccion = request.form.get('numero_licencia_conduccion', '').strip()
            fecha_expedicion_licencia = request.form.get('fecha_expedicion_licencia', '').strip() or None
            categoria_licencia = request.form.get('categoria_licencia', 'Publico').strip()
            ultimo_pago_seguridad_social = request.form.get('ultimo_pago_seguridad_social', '').strip() or None
            
            vencimiento_licencia_conduccion = calcular_vencimiento_licencia(fecha_expedicion_licencia, fecha_nacimiento, categoria_licencia)
            
            if tipo_conductor == 'Propio':
                ultimo_pago_seguridad_social = None
                vencimiento_seguridad_social = None
            else:
                vencimiento_seguridad_social = calcular_vencimiento_ss(cedula, ultimo_pago_seguridad_social)

            if not nombre or not cedula:
                flash("El nombre y la cédula son obligatorios.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_operadores'))

            if tipo_conductor == 'Propio' and not password:
                flash("El conductor propio requiere una contraseña de acceso.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_operadores'))

            ruta_licencia = procesar_archivo_pdf(request.files.get('archivo_licencia'), cedula, 'LICENCIA', empresa_id)
            ruta_foto = procesar_archivo_pdf(request.files.get('archivo_foto_facial'), cedula, 'FOTO_FACIAL', empresa_id)

            if tipo_conductor == 'Propio':
                ruta_seg_soc = None
                ruta_pago = None
            else:
                ruta_seg_soc = procesar_archivo_pdf(request.files.get('archivo_seguridad_social'), cedula, 'SEG_SOCIAL', empresa_id)
                ruta_pago = procesar_archivo_pdf(request.files.get('archivo_soporte_pago'), cedula, 'PAGO', empresa_id)

            cur = mysql.connection.cursor()
            try:
                cur.execute(f"SELECT id FROM {tabla_cond} WHERE cedula = %s AND id_empresa = %s", (cedula, empresa_id))
                if cur.fetchone():
                    flash(f"El conductor con cédula {cedula} ya existe.", "danger")
                else:
                    cur.execute(f"""
                        INSERT INTO {tabla_cond} (
                            id_empresa, tipo_conductor, nombre, cedula, fecha_nacimiento, edad,
                            password_visible, email, vehiculo_asignado,
                            eps, arl, fondo_pensiones, numero_licencia_conduccion, 
                            fecha_expedicion_licencia, categoria_licencia, vencimiento_licencia_conduccion,
                            ultimo_pago_seguridad_social, vencimiento_seguridad_social,
                            ruta_pdf_licencia, ruta_pdf_seguridad_social, ruta_pdf_soporte_pago, ruta_foto_facial,
                            estado_licencia, estado_seguridad_social
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'Actualizado', 'Actualizado')
                    """, (
                        empresa_id, tipo_conductor, nombre, cedula, fecha_nacimiento, edad,
                        password if tipo_conductor == 'Propio' else None, email, vehiculo_asignado,
                        eps, arl, fondo_pensiones, numero_licencia_conduccion, 
                        fecha_expedicion_licencia, categoria_licencia, vencimiento_licencia_conduccion,
                        ultimo_pago_seguridad_social, vencimiento_seguridad_social,
                        ruta_licencia, ruta_seg_soc, ruta_pago, ruta_foto
                    ))
                    
                    if tipo_conductor == 'Propio':
                        hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
                        cur.execute("""
                            INSERT INTO usuarios (nombre, cedula, password, perfil, empresa, empresa_id) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (nombre, cedula, hashed_pw, perfil, empresa_nombre, empresa_id))
                    
                    registrar_operacion_bqa_local(cur, 'CREAR_CONDUCTOR', {'cedula': cedula, 'tipo_empresa': tipo_empresa})
                    mysql.connection.commit()
                    flash(f"Personal registrado exitosamente.", "success")
            except Exception as e:
                mysql.connection.rollback()
                for ruta_relativa in filter(None, [ruta_licencia, ruta_seg_soc, ruta_pago, ruta_foto]):
                    ruta_fisica = os.path.join(current_app.static_folder, ruta_relativa)
                    if os.path.exists(ruta_fisica):
                        try: os.remove(ruta_fisica)
                        except: pass
                
                if hasattr(e, 'args') and e.args[0] == 1062:
                    flash("El número de identificación del conductor ya se encuentra registrado.", "warning")
                else:
                    flash("Ocurrió una interrupción temporal al procesar la solicitud.", "danger")
            finally:
                cur.close()

        elif accion == 'editar':
            operador_id = request.form.get('operador_id')
            tipo_conductor = request.form.get('tipo_conductor', 'Propio').strip()
            perfil = request.form.get('perfil', 'operador_flotacarga').strip()
            nombre = request.form.get('nombre', '').strip()
            cedula = request.form.get('cedula', '').strip()
            email = request.form.get('email', '').strip()
            password_nueva = request.form.get('password', '').strip()
            vehiculo_asignado = request.form.get('vehiculo_asignado', '').strip() if tipo_conductor == 'Tercero' else None
            
            fecha_nacimiento = request.form.get('fecha_nacimiento', '').strip() or None
            edad = request.form.get('edad', '').strip() or None
            eps = request.form.get('eps', '').strip()
            arl = request.form.get('arl', '').strip()
            fondo_pensiones = request.form.get('fondo_pensiones', '').strip()
            
            numero_licencia_conduccion = request.form.get('numero_licencia_conduccion', '').strip()
            fecha_expedicion_licencia = request.form.get('fecha_expedicion_licencia', '').strip() or None
            categoria_licencia = request.form.get('categoria_licencia', 'Publico').strip()
            ultimo_pago_seguridad_social = request.form.get('ultimo_pago_seguridad_social', '').strip() or None
            
            vencimiento_licencia_conduccion = calcular_vencimiento_licencia(fecha_expedicion_licencia, fecha_nacimiento, categoria_licencia)
            
            if tipo_conductor == 'Propio':
                ultimo_pago_seguridad_social = None
                vencimiento_seguridad_social = None
            else:
                vencimiento_seguridad_social = calcular_vencimiento_ss(cedula, ultimo_pago_seguridad_social)

            if operador_id and nombre and cedula:
                cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
                try:
                    cur.execute(f"SELECT * FROM {tabla_cond} WHERE id = %s AND id_empresa = %s", (operador_id, empresa_id))
                    old_data = cur.fetchone()

                    if old_data:
                        new_ruta_licencia = procesar_archivo_pdf(request.files.get('archivo_licencia'), cedula, 'LICENCIA', empresa_id)
                        new_ruta_foto = procesar_archivo_pdf(request.files.get('archivo_foto_facial'), cedula, 'FOTO_FACIAL', empresa_id)

                        ruta_licencia = new_ruta_licencia or old_data.get('ruta_pdf_licencia')
                        ruta_foto = new_ruta_foto or old_data.get('ruta_foto_facial')

                        if tipo_conductor == 'Propio':
                            new_ruta_seg_soc = None
                            new_ruta_pago = None
                            ruta_seg_soc = None
                            ruta_pago = None
                        else:
                            new_ruta_seg_soc = procesar_archivo_pdf(request.files.get('archivo_seguridad_social'), cedula, 'SEG_SOCIAL', empresa_id)
                            new_ruta_pago = procesar_archivo_pdf(request.files.get('archivo_soporte_pago'), cedula, 'PAGO', empresa_id)
                            ruta_seg_soc = new_ruta_seg_soc or old_data.get('ruta_pdf_seguridad_social')
                            ruta_pago = new_ruta_pago or old_data.get('ruta_pdf_soporte_pago')

                        pass_visible = password_nueva if password_nueva else old_data.get('password_visible')
                        if tipo_conductor == 'Tercero':
                            pass_visible = None

                        cur.execute(f"""
                            UPDATE {tabla_cond} SET
                                tipo_conductor = %s, nombre = %s, cedula = %s, fecha_nacimiento = %s, edad = %s,
                                password_visible = %s, email = %s, vehiculo_asignado = %s,
                                eps = %s, arl = %s, fondo_pensiones = %s, numero_licencia_conduccion = %s, 
                                fecha_expedicion_licencia = %s, categoria_licencia = %s, vencimiento_licencia_conduccion = %s,
                                ultimo_pago_seguridad_social = %s, vencimiento_seguridad_social = %s,
                                ruta_pdf_licencia = %s, ruta_pdf_seguridad_social = %s, ruta_pdf_soporte_pago = %s, ruta_foto_facial = %s,
                                estado_licencia = 'Actualizado', estado_seguridad_social = 'Actualizado'
                            WHERE id = %s AND id_empresa = %s
                        """, (
                            tipo_conductor, nombre, cedula, fecha_nacimiento, edad,
                            pass_visible, email, vehiculo_asignado,
                            eps, arl, fondo_pensiones, numero_licencia_conduccion, 
                            fecha_expedicion_licencia, categoria_licencia, vencimiento_licencia_conduccion,
                            ultimo_pago_seguridad_social, vencimiento_seguridad_social,
                            ruta_licencia, ruta_seg_soc, ruta_pago, ruta_foto,
                            operador_id, empresa_id
                        ))

                        cur.execute("SELECT id FROM usuarios WHERE cedula = %s AND empresa_id = %s", (old_data['cedula'], empresa_id))
                        usr = cur.fetchone()

                        if tipo_conductor == 'Propio':
                            if usr:
                                if password_nueva:
                                    hashed_pw = bcrypt.generate_password_hash(password_nueva).decode('utf-8')
                                    cur.execute("UPDATE usuarios SET nombre=%s, cedula=%s, perfil=%s, password=%s WHERE id=%s AND empresa_id=%s", (nombre, cedula, perfil, hashed_pw, usr['id'], empresa_id))
                                else:
                                    cur.execute("UPDATE usuarios SET nombre=%s, cedula=%s, perfil=%s WHERE id=%s AND empresa_id=%s", (nombre, cedula, perfil, usr['id'], empresa_id))
                            else:
                                if pass_visible:
                                    hashed_pw = bcrypt.generate_password_hash(pass_visible).decode('utf-8')
                                    cur.execute("""
                                        INSERT INTO usuarios (nombre, cedula, password, perfil, empresa, empresa_id) 
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                    """, (nombre, cedula, hashed_pw, perfil, empresa_nombre, empresa_id))
                        elif tipo_conductor == 'Tercero' and usr:
                            cur.execute("DELETE FROM usuarios WHERE id = %s AND empresa_id = %s", (usr['id'], empresa_id))

                        registrar_operacion_bqa_local(cur, 'EDITAR_OPERADOR', {'cedula': cedula, 'id': operador_id})
                        mysql.connection.commit()
                        flash("Registro de personal actualizado.", "success")
                except Exception as e:
                    mysql.connection.rollback()
                    for ruta_relativa in filter(None, [new_ruta_licencia, new_ruta_seg_soc, new_ruta_pago, new_ruta_foto]):
                        ruta_fisica = os.path.join(current_app.static_folder, ruta_relativa)
                        if os.path.exists(ruta_fisica):
                            try: os.remove(ruta_fisica)
                            except: pass
                            
                    if hasattr(e, 'args') and e.args[0] == 1062:
                        flash("El número de identificación del conductor ya se encuentra registrado.", "warning")
                    else:
                        flash("Ocurrió un error al procesar el formulario de edición.", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            operador_id = request.form.get('operador_id')
            cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
            try:
                cur.execute(f"SELECT cedula FROM {tabla_cond} WHERE id = %s AND id_empresa = %s", (operador_id, empresa_id))
                cd_data = cur.fetchone()
                if cd_data:
                    cur.execute("DELETE FROM usuarios WHERE cedula = %s AND empresa_id = %s", (cd_data['cedula'], empresa_id))
                    cur.execute(f"DELETE FROM {tabla_cond} WHERE id = %s AND id_empresa = %s", (operador_id, empresa_id))
                    registrar_operacion_bqa_local(cur, 'ELIMINAR_OPERADOR', {'id': operador_id, 'tabla': tabla_cond})
                    mysql.connection.commit()
                    flash("Registro desvinculado por completo.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash("Ocurrió una interrupción al intentar eliminar el conductor.", "danger")
            finally:
                cur.close()

        return redirect(url_for('flotacarga_flota.gestion_operadores'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    if tipo_empresa == 'transporte_especial':
        cur.execute("SELECT id, placa, tipo_vinculacion as propiedad FROM vehiculos_especial WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    else:
        cur.execute("SELECT id, placa, propiedad FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()

    cur.execute(f"""
        SELECT c.*, 
               IFNULL(u.perfil, 'operador_flotacarga') as perfil 
        FROM {tabla_cond} c 
        LEFT JOIN usuarios u 
          ON c.cedula COLLATE utf8mb4_unicode_ci = u.cedula COLLATE utf8mb4_unicode_ci 
         AND c.id_empresa = u.empresa_id 
        WHERE c.id_empresa = %s 
        ORDER BY c.nombre ASC
    """, (empresa_id,))
    operadores_db = cur.fetchall()
    
    cur.close()

    return render_template(
        'B_flotacarga_flota.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='operadores', 
        operadores=operadores_db,
        vehiculos=vehiculos_db
    )

@bp_flotacarga_flota.route('/preoperacionales', methods=['GET'])
@login_required_custom
@gestor_flota_required
def gestion_preoperacionales():
    empresa_id = session.get('empresa_id')
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    placa_filtro = request.args.get('placa', '')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("SELECT DISTINCT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_historicos = cur.fetchall()

    inspecciones = []
    if placa_filtro:
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
        'B_flotacarga_flota.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='preoperacionales',
        inspecciones=inspecciones,
        vehiculos_historicos=vehiculos_historicos,
        filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'placa': placa_filtro}
    )

@bp_flotacarga_flota.route('/documentos_vencidos', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_documentos_vencidos():
    empresa_id = session.get('empresa_id')
    hoy = datetime.now().date()
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        if accion == 'verificar_documento':
            entidad_tipo = request.form.get('entidad_tipo')
            entidad_id = request.form.get('entidad_id')
            clave = request.form.get('documento_clave')
            
            if entidad_tipo == 'vehiculo':
                col_estado = 'estado_soat' if clave == 'vencimiento_soat' else 'estado_tecnomecanica' if clave == 'vencimiento_tecnomecanica' else 'estado_poliza_rce'
                query = f"UPDATE vehiculos SET {col_estado} = 'Verificado' WHERE id = %s AND id_empresa = %s"
            else:
                col_estado = 'estado_licencia' if clave == 'licencia_conduccion' else 'estado_seguridad_social'
                query = f"UPDATE conductores_carga SET {col_estado} = 'Verificado' WHERE id = %s AND id_empresa = %s"
            
            cur = mysql.connection.cursor()
            try:
                cur.execute(query, (entidad_id, empresa_id))
                mysql.connection.commit()
                flash("Soporte documental validado exitosamente.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash("Ocurrió una interrupción temporal al validar el soporte. Por favor intenta nuevamente.", "danger")
            finally:
                cur.close()
            return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM vehiculos WHERE id_empresa = %s", (empresa_id,))
    vehiculos = cur.fetchall()
    
    cur.execute("SELECT * FROM conductores_carga WHERE id_empresa = %s", (empresa_id,))
    conductores = cur.fetchall()
    cur.close()
    
    novedades = []
    
    def parse_date(date_val):
        if not date_val: return None
        if isinstance(date_val, datetime): return date_val.date()
        if isinstance(date_val, str):
            try: return datetime.strptime(date_val, "%Y-%m-%d").date()
            except: return None
        return date_val
        
    for v in vehiculos:
        docs_v = [
            ("SOAT", "vencimiento_soat", parse_date(v.get('vencimiento_soat')), v.get('estado_soat'), v.get('ruta_soat')),
            ("Tecnomecánica", "vencimiento_tecnomecanica", parse_date(v.get('vencimiento_tecnomecanica')), v.get('estado_tecnomecanica'), v.get('ruta_tecnomecanica')),
            ("Póliza RCE", "vencimiento_poliza_rce", parse_date(v.get('vencimiento_poliza_rce')), v.get('estado_poliza_rce'), v.get('ruta_poliza_rce'))
        ]
        for nombre_doc, clave, fecha, estado_db, ruta in docs_v:
            if estado_db == 'Actualizado':
                novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Actualizado", "dias": "N/A", "ruta": ruta})
            elif not fecha:
                novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Faltante", "dias": "N/A", "ruta": ""})
            else:
                dias = (fecha - hoy).days
                if dias <= 0:
                    novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Vencido", "dias": abs(dias), "ruta": ""})
                elif dias <= 8:
                    novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Por Vencer", "dias": dias, "ruta": ""})
                    
    for c in conductores:
        docs_c = [
            ("Licencia de Conducción", "licencia_conduccion", parse_date(c.get('vencimiento_licencia_conduccion')), c.get('estado_licencia'), c.get('ruta_pdf_licencia'))
        ]
        
        if c.get('tipo_conductor') == 'Tercero':
            docs_c.append(("Seguridad Social (Planilla)", "seguridad_social", parse_date(c.get('vencimiento_seguridad_social')), c.get('estado_seguridad_social'), c.get('ruta_pdf_seguridad_social')))
            
        for nombre_doc, clave, fecha, estado_db, ruta in docs_c:
            if estado_db == 'Actualizado':
                novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": nombre_doc, "clave": clave, "estado": "Actualizado", "dias": "N/A", "ruta": ruta})
            elif not fecha:
                novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": nombre_doc, "clave": clave, "estado": "Faltante", "dias": "N/A", "ruta": ""})
            else:
                dias = (fecha - hoy).days
                if dias <= 0:
                    novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": nombre_doc, "clave": clave, "estado": "Vencido", "dias": abs(dias), "ruta": ""})
                elif dias <= 8:
                    novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": nombre_doc, "clave": clave, "estado": "Por Vencer", "dias": dias, "ruta": ""})
    
    return render_template(
        'B_flotacarga_flota.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='documentos_vencidos',
        novedades=novedades
    )

@bp_flotacarga_flota.route('/actualizar_documento_vencido', methods=['POST'])
@login_required_custom
@gestor_flota_required
def actualizar_documento_vencido():
    empresa_id = session.get('empresa_id')
    
    entidad_tipo = request.form.get('entidad_tipo')
    entidad_id = request.form.get('entidad_id')
    documento_clave = request.form.get('documento_clave')
    nueva_fecha = request.form.get('nueva_fecha')
    archivo = request.files.get('archivo_soporte')
    
    if not all([entidad_tipo, entidad_id, documento_clave, nueva_fecha, archivo]):
        flash("Todos los campos son obligatorios para actualizar el documento.", "danger")
        return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        if entidad_tipo == 'vehiculo':
            cur.execute("SELECT placa FROM vehiculos WHERE id = %s AND id_empresa = %s", (entidad_id, empresa_id))
            v = cur.fetchone()
            if not v:
                flash("Vehículo no encontrado.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            placa = v['placa']
            
            if documento_clave == 'vencimiento_soat':
                tipo_doc = 'SOAT'
                col_fecha = 'vencimiento_soat'
                col_ruta = 'ruta_soat'
                col_estado = 'estado_soat'
            elif documento_clave == 'vencimiento_tecnomecanica':
                tipo_doc = 'TECNOMECANICA'
                col_fecha = 'vencimiento_tecnomecanica'
                col_ruta = 'ruta_tecnomecanica'
                col_estado = 'estado_tecnomecanica'
            elif documento_clave == 'vencimiento_poliza_rce':
                tipo_doc = 'POLIZA_RCE'
                col_fecha = 'vencimiento_poliza_rce'
                col_ruta = 'ruta_poliza_rce'
                col_estado = 'estado_poliza_rce'
            else:
                flash("Tipo de documento de vehículo inválido.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            ruta_pdf = procesar_archivo_pdf(archivo, placa, tipo_doc, empresa_id)
            if not ruta_pdf:
                flash("Error al procesar el archivo adjunto.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            cur.execute(f"UPDATE vehiculos SET {col_fecha} = %s, {col_ruta} = %s, {col_estado} = 'Actualizado' WHERE id = %s AND id_empresa = %s", (nueva_fecha, ruta_pdf, entidad_id, empresa_id))
            mysql.connection.commit()
            flash(f"Documento {tipo_doc} actualizado correctamente para el vehículo {placa}.", "success")
            
        elif entidad_tipo == 'conductor':
            cur.execute("SELECT cedula, nombre, fecha_nacimiento, categoria_licencia FROM conductores_carga WHERE id = %s AND id_empresa = %s", (entidad_id, empresa_id))
            c = cur.fetchone()
            if not c:
                flash("Conductor no encontrado.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            cedula = c['cedula']
            nombre = c['nombre']
            
            if documento_clave == 'licencia_conduccion':
                tipo_doc = 'LICENCIA'
                col_ruta = 'ruta_pdf_licencia'
                col_estado = 'estado_licencia'
                real_vencimiento = calcular_vencimiento_licencia(nueva_fecha, c.get('fecha_nacimiento'), c.get('categoria_licencia', 'Publico'))
                ruta_pdf = procesar_archivo_pdf(archivo, cedula, tipo_doc, empresa_id)
                query = f"UPDATE conductores_carga SET fecha_expedicion_licencia = %s, vencimiento_licencia_conduccion = %s, {col_ruta} = %s, {col_estado} = 'Actualizado' WHERE id = %s"
                cur.execute(query, (nueva_fecha, real_vencimiento, ruta_pdf, entidad_id))
                
            elif documento_clave == 'seguridad_social':
                tipo_doc = 'SEG_SOCIAL'
                col_ruta = 'ruta_pdf_seguridad_social'
                col_estado = 'estado_seguridad_social'
                real_vencimiento = calcular_vencimiento_ss(cedula, nueva_fecha)
                ruta_pdf = procesar_archivo_pdf(archivo, cedula, tipo_doc, empresa_id)
                query = f"UPDATE conductores_carga SET ultimo_pago_seguridad_social = %s, vencimiento_seguridad_social = %s, {col_ruta} = %s, {col_estado} = 'Actualizado' WHERE id = %s"
                cur.execute(query, (nueva_fecha, real_vencimiento, ruta_pdf, entidad_id))
                
            else:
                flash("Tipo de documento de conductor inválido.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            if not ruta_pdf:
                flash("Error al procesar el archivo adjunto.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            mysql.connection.commit()
            flash(f"Documento {tipo_doc} actualizado correctamente para {nombre}.", "success")
            
    except Exception as e:
        mysql.connection.rollback()
        if 'ruta_pdf' in locals() and ruta_pdf:
            ruta_fisica = os.path.join(current_app.static_folder, ruta_pdf)
            if os.path.exists(ruta_fisica):
                try: os.remove(ruta_fisica)
                except: pass
                
        if hasattr(e, 'args') and e.args[0] == 1213:
            flash("Hay un alto volumen de registros en este instante. Por favor, espera 3 segundos y vuelve a intentarlo.", "warning")
        elif hasattr(e, 'args') and e.args[0] == 1062:
            flash("El registro ya se encuentra procesado en el sistema.", "warning")
        else:
            flash("Ocurrió una interrupción temporal al guardar el expediente. Por favor, verifica tu conexión y vuelve a intentarlo.", "danger")
    finally:
        cur.close()
        
    return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))

@bp_flotacarga_flota.route('/preoperacionales/pdf/<consecutivo>', methods=['GET'])
@login_required_custom
@gestor_flota_required
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
        return redirect(url_for('flotacarga_flota.gestion_preoperacionales'))

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
            ('doc_cedula', 'Cédula de Ciudadanía'),
            ('doc_licencia_conduccion', 'Licencia de Conducción'), 
            ('doc_licencia_transito', 'Licencia de Tránsito (Propiedad)'),
            ('doc_soat_vigente', 'SOAT Vigente'),
            ('doc_tecnomecanica_vigente', 'Revisión Tecnomecánica'), 
            ('doc_tarjeta_operacion', 'Tarjeta de Operación')
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

    try:
        novedades_dict = json.loads(insp.get('detalles_novedades_json') or '{}')
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
    try:
        llantas_dict = json.loads(insp.get('estado_llantas_json') or '{}')
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
        except Exception as e:
            pass
        return Paragraph("<i>No disponible</i>", cell_style)

    if insp.get('observaciones_hallazgos'):
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>OBSERVACIONES GENERALES DEL CONDUCTOR</b>", sub_title_style))
        story.append(Paragraph(f"<i>{insp['observaciones_hallazgos']}</i>", cell_style))

    story.append(Spacer(1, 15))
    story.append(Paragraph("<b>AUTENTICACIÓN Y FIRMA</b>", sub_title_style))
    
    declaracion_texto = "<b>Declaración de Veracidad y Cumplimiento Normativo:</b> Declaro bajo la gravedad de juramento que la información aquí registrada es veraz, exacta y ha sido recolectada mediante inspección física directa del vehículo. Este registro preoperacional da cumplimiento estricto al <b>Paso 16 de la Metodología del Plan Estratégico de Seguridad Vial (PESV)</b>, adoptada mediante la <b>Resolución 40595 de 2022 del Ministerio de Transporte de Colombia.</b>"
    story.append(Paragraph(declaracion_texto, cell_style))
    story.append(Spacer(1, 10))
    
    img_firma = convertir_base64_rlimage(insp.get('firma_grafica_base64'), 2*inch, 1*inch)
    img_foto = convertir_base64_rlimage(insp.get('foto_conductor_base64'), 1.2*inch, 1.2*inch)

    firma_data = [
        [Paragraph("<b>Foto Auditoría:</b>", cell_style), Paragraph("<b>Firma Gráfica:</b>", cell_style)], 
        [img_foto, img_firma]
    ]
    t_firma = Table(firma_data, colWidths=[150, 200])
    t_firma.setStyle(TableStyle([
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#16a34a')), 
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), 
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0fdf4')), 
        ('ALIGN', (0,0), (-1,-1), 'CENTER'), 
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE')
    ]))
    story.append(t_firma)

    doc.build(story)
    buffer.seek(0)
    
    return send_file(
        buffer, 
        as_attachment=True, 
        download_name=f"Preoperacional_Carga_{consecutivo}.pdf", 
        mimetype='application/pdf'
    )

# ==============================================================================
# NUEVO: AUDITORÍA DE VENCIMIENTOS AUTOMATIZADA (CRON)
# ==============================================================================
@bp_flotacarga_flota.route('/cron/auditoria_vencimientos', methods=['GET'])
def cron_auditoria_vencimientos():
    token = request.args.get('token')
    if token != 'BQA_CRON_2026':
        return jsonify({"status": "error", "message": "No autorizado"}), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT id, nombre_comercial, nit FROM empresas")
        empresas = cur.fetchall()

        alertas_generadas = 0
        hoy = datetime.now().date()

        for emp in empresas:
            empresa_id = emp['id']
            empresa_nombre = emp['nombre_comercial']
            
            alertas_vehiculos = []
            alertas_conductores = []

            cur.execute("SELECT placa, vencimiento_soat, vencimiento_tecnomecanica, vencimiento_poliza_rce, estado_soat, estado_tecnomecanica, estado_poliza_rce FROM vehiculos WHERE id_empresa = %s", (empresa_id,))
            vehiculos = cur.fetchall()
            for v in vehiculos:
                docs = [
                    ('SOAT', v.get('vencimiento_soat'), v.get('estado_soat')),
                    ('Tecnomecánica', v.get('vencimiento_tecnomecanica'), v.get('estado_tecnomecanica')),
                    ('Póliza RCE', v.get('vencimiento_poliza_rce'), v.get('estado_poliza_rce'))
                ]
                for doc_nombre, fecha_vence, estado in docs:
                    if not fecha_vence:
                        alertas_vehiculos.append((v['placa'], doc_nombre, "N/A", "FALTANTE", "N/A"))
                    else:
                        if isinstance(fecha_vence, str):
                            try: fecha_vence = datetime.strptime(fecha_vence, '%Y-%m-%d').date()
                            except: continue
                        if isinstance(fecha_vence, datetime):
                            fecha_vence = fecha_vence.date()
                        
                        dias = (fecha_vence - hoy).days
                        if dias <= 0:
                            alertas_vehiculos.append((v['placa'], doc_nombre, str(fecha_vence), "VENCIDO", abs(dias)))
                        elif dias <= 8:
                            alertas_vehiculos.append((v['placa'], doc_nombre, str(fecha_vence), "POR VENCER", dias))
            
            cur.execute("SELECT cedula, nombre, email as telefono, vencimiento_licencia_conduccion, vencimiento_seguridad_social, estado_licencia, estado_seguridad_social, tipo_conductor FROM conductores_carga WHERE id_empresa = %s", (empresa_id,))
            conductores = cur.fetchall()
            for c in conductores:
                tel = c.get('telefono') or 'N/A'
                docs = [
                    ('Licencia de Conducción', c.get('vencimiento_licencia_conduccion'), c.get('estado_licencia'))
                ]
                
                if c.get('tipo_conductor') == 'Tercero':
                    docs.append(('Seguridad Social', c.get('vencimiento_seguridad_social'), c.get('estado_seguridad_social')))
                
                for doc_nombre, fecha_vence, estado in docs:
                    if not fecha_vence:
                        alertas_conductores.append((c['nombre'], c['cedula'], tel, doc_nombre, "N/A", "FALTANTE", "N/A"))
                    else:
                        if isinstance(fecha_vence, str):
                            try: fecha_vence = datetime.strptime(fecha_vence, '%Y-%m-%d').date()
                            except: continue
                        if isinstance(fecha_vence, datetime):
                            fecha_vence = fecha_vence.date()
                        
                        dias = (fecha_vence - hoy).days
                        if dias <= 0:
                            alertas_conductores.append((c['nombre'], c['cedula'], tel, doc_nombre, str(fecha_vence), "VENCIDO", abs(dias)))
                        elif dias <= 8:
                            alertas_conductores.append((c['nombre'], c['cedula'], tel, doc_nombre, str(fecha_vence), "POR VENCER", dias))

            if alertas_vehiculos or alertas_conductores:
                cur.execute("""
                    SELECT email FROM usuarios 
                    WHERE empresa_id = %s 
                    AND perfil IN ('gestor_flotacarga', 'controlador_pesv') 
                    AND email IS NOT NULL 
                    AND email != ''
                """, (empresa_id,))
                destinatarios = [row['email'] for row in cur.fetchall()]
                
                if destinatarios:
                    _enviar_email_vencimientos_carga_hilo(destinatarios, empresa_nombre, alertas_vehiculos, alertas_conductores)
                
                alertas_generadas += 1

        mysql.connection.commit()
        return jsonify({"status": "success", "reportes_procesados": alertas_generadas}), 200

    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()