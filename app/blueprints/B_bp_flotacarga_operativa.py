# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: GESTION_OPERATIVA | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/B_bp_flotacarga_operativa.py
import os
import io
import json
import math
import qrcode
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, send_file
from app import mysql
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta

# Librerías PDF (QR)
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

halfLetter = (5.5 * inch, 8.5 * inch)

bp_flotacarga_operativa = Blueprint('flotacarga_operativa', __name__, url_prefix='/gestor_flota/operativa_bp')

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
            'webmaster'
        ]
        
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Gestor/Controlador de Flota.', 'danger')
            return redirect('/gestor_flota/dashboard')
            
        if 'webmaster' not in tipo_empresa:
            # Fallback correcto en Python para listas vacías
            autorizados = session.get('submodulos_activos', [])
            if not autorizados:
                autorizados = session.get('modulos_activos', [])
                
            if not any(m in autorizados for m in ['controlador_flota', 'flota', 'carga']):
                flash('Acceso denegado: Tu empresa no tiene activo este submódulo.', 'danger')
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
# RUTAS DEL SUBMÓDULO (OPERATIVA GENERAL: RUTAS, MAPA, QRS)
# =========================================================
@bp_flotacarga_operativa.route('/')
@login_required_custom
@gestor_flota_required
def index():
    return redirect(url_for('flotacarga_operativa.gestion_rutas'))

@bp_flotacarga_operativa.route('/rutas', methods=['GET', 'POST'])
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
                    flash(f"Error al actualizar la ruta.", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            ruta_id = request.form.get('ruta_id')
            cur = mysql.connection.cursor()
            try:
                cur.execute("DELETE FROM rutas WHERE id = %s AND id_empresa = %s", (ruta_id, empresa_id))
                mysql.connection.commit()
                flash("Ruta eliminada.", "success")
            except Exception as e:
                flash("Error al eliminar la ruta.", "danger")
            finally:
                cur.close()

        return redirect(url_for('flotacarga_operativa.gestion_rutas'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM rutas WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    rutas_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_flotacarga_operativa.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='rutas', 
        rutas=rutas_db,
        funcionalidades_operador=session.get('funcionalidades_operador', ['registro_paradas'])
    )

@bp_flotacarga_operativa.route('/mapa_rutas', methods=['GET'])
@login_required_custom
@gestor_flota_required
def mapa_rutas():
    empresa_id = session.get('empresa_id')
    fecha_filtro = request.args.get('fecha', datetime.now().strftime('%Y-%m-%d'))
    placa_filtro = request.args.get('placa', '').upper().strip()

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()

    puntos_ruta = []
    if placa_filtro:
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
        
        for row in cur.fetchall():
            if isinstance(row['fecha_hora'], datetime):
                row['fecha_hora'] = row['fecha_hora'].strftime('%Y-%m-%d %H:%M:%S')
            puntos_ruta.append(row)
            
    cur.close()

    return render_template(
        'B_flotacarga_operativa.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='mapa_rutas',
        vehiculos=vehiculos_db,
        puntos_ruta=puntos_ruta,
        filtros={'fecha': fecha_filtro, 'placa': placa_filtro}
    )

@bp_flotacarga_operativa.route('/qrs')
@login_required_custom
@gestor_flota_required
def generacion_qrs():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, placa, tipo, caja_de_carga, `capacidad (kg)`, referencia FROM vehiculos WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_flotacarga_operativa.html',
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
        qr_data = {"placa": placa, "nit": str(nit_empresa), "empresa": nombre_empresa}
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
            except Exception: pass
        
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
            except Exception: pass
                
        c.showPage()
        
    c.save()
    buffer.seek(0)
    return buffer

@bp_flotacarga_operativa.route('/qrs/imprimir/<int:vehiculo_id>')
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
        flash("Vehículo no encontrado.", "danger")
        return redirect(url_for('flotacarga_operativa.generacion_qrs'))

    pdf_buffer = _generar_pdf_qrs([vehiculo], empresa_id, empresa_nombre)
    return send_file(pdf_buffer, as_attachment=False, download_name=f"QR_{vehiculo['placa']}.pdf", mimetype='application/pdf')

@bp_flotacarga_operativa.route('/qrs/imprimir_todos')
@login_required_custom
@gestor_flota_required
def imprimir_todos_qrs():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s", (empresa_id,))
    vehiculos_list = cur.fetchall()
    cur.close()

    pdf_buffer = _generar_pdf_qrs(vehiculos_list, empresa_id, empresa_nombre)
    return send_file(pdf_buffer, as_attachment=False, download_name=f"Todos_QRs_{empresa_nombre}.pdf", mimetype='application/pdf')

@bp_flotacarga_operativa.route('/qrs/imprimir_terceros')
@login_required_custom
@gestor_flota_required
def imprimir_qr_terceros():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=halfLetter)
    width, height = halfLetter

    url_terceros = request.host_url.rstrip('/') + url_for('validacion_terceros_carga.ingreso_terceros', nit_empresa=empresa_id)
    
    qr = qrcode.QRCode(version=1, box_size=10, border=2)
    qr.add_data(url_terceros)
    qr.make(fit=True)
    
    img_qr = qr.make_image(fill_color="#0f172a", back_color="white") 
    qr_buffer = io.BytesIO()
    img_qr.save(qr_buffer, format="PNG")
    qr_buffer.seek(0)
    qr_image_reader = ImageReader(qr_buffer)
    
    c.setFillColorRGB(0.0039, 0.321, 0.286) 
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(width/2, height - 1.5*inch, "ACCESO VEHÍCULOS TERCEROS")
    
    c.setFillColorRGB(0.2, 0.2, 0.2)
    c.setFont("Helvetica", 11)
    c.drawCentredString(width/2, height - 1.8*inch, "Escanee para validar su placa, pagar derechos y registrar")
    c.drawCentredString(width/2, height - 2.0*inch, "el preoperacional obligatorio.")
    
    qr_size = 4 * inch
    c.drawImage(qr_image_reader, width/2 - qr_size/2, height/2 - qr_size/2 - 0.5*inch, width=qr_size, height=qr_size)
    
    c.showPage()
    c.save()
    buffer.seek(0)
    
    return send_file(buffer, as_attachment=False, download_name=f"QR_Porteria_Terceros_{empresa_nombre}.pdf", mimetype='application/pdf')