# app/blueprints/B_bp_controlador_flotacarga.py
import os
import io
import json
import qrcode
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, send_file
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta
import base64
from PIL import Image as PILImage

# Librerías PDF (QR)
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

# Librerías PDF (Reportes Platypus)
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors

halfLetter = (5.5 * inch, 8.5 * inch)

bp_gestorflota = Blueprint('gestorflota', __name__, url_prefix='/gestor_flota')

def gestor_flota_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['gestor_flotacarga', 'controlador_transportecarga', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Gestor/Controlador de Flota para ingresar a este módulo.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# RUTAS DEL PANEL ADMINISTRATIVO
# =========================================================

@bp_gestorflota.route('/dashboard')
@login_required_custom
@gestor_flota_required
def dashboard_gestor():
    return render_template(
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='dashboard' 
    )

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
            
            if placa and caja_de_carga and tipo:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        INSERT INTO vehiculos (empresa, id_empresa, placa, tipo, caja_de_carga, referencia, peso_vacio, `capacidad (kg)`) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (empresa_nombre, empresa_id, placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad))
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
            
            if vehiculo_id and placa and caja_de_carga and tipo:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        UPDATE vehiculos 
                        SET placa = %s, tipo = %s, caja_de_carga = %s, referencia = %s, peso_vacio = %s, `capacidad (kg)` = %s
                        WHERE id = %s AND id_empresa = %s
                    """, (placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, vehiculo_id, empresa_id))
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
            
            if perfil == 'operador_transportecarga':
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
                    if perfil == 'operador_transportecarga':
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
    cur.execute("""
        SELECT id, nombre, cedula, perfil 
        FROM usuarios 
        WHERE empresa_id = %s AND perfil IN ('operador_transportecarga', 'auxiliar_transportecarga') 
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

# =========================================================
# MÓDULO 5: AUDITORÍA DE PREOPERACIONALES
# =========================================================
@bp_gestorflota.route('/preoperacionales')
@login_required_custom
@gestor_flota_required
def historial_preoperacionales():
    empresa_id = session.get('empresa_id')
    
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    placa_filtro = request.args.get('placa', 'todas')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    # 1. Extraer placas para el selector de filtro
    cur.execute("SELECT DISTINCT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_historicos = cur.fetchall()

    # 2. Consultar Preoperacionales según filtro
    query = """
        SELECT id_inspeccion, consecutivo_anual, fecha_inspeccion, hora_inspeccion, 
               placa_vehiculo, nombre_conductor, vehiculo_aprobado 
        FROM inspeccion_preoperacional_carga 
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
        'B_modulo_controlador_flotacarga.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='preoperacionales',
        inspecciones=inspecciones,
        vehiculos_historicos=vehiculos_historicos,
        filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'placa': placa_filtro}
    )


@bp_gestorflota.route('/cron/mantenimiento_bd', methods=['GET'])
def cron_limpieza_datos():
    """
    CRON JOB: Ejecutar el día 1 de cada mes en la madrugada.
    Elimina los registros preoperacionales antiguos para no saturar el servidor.
    """
    # Candado de seguridad para evitar ejecuciones externas
    if request.args.get('token') != 'BQA_CRON_2026':
        return jsonify({"success": False, "message": "No autorizado"}), 403

    cur = mysql.connection.cursor()
    try:
        # Ejecuta el borrado masivo de registros con más de 1 año (12 meses)
        cur.execute("""
            DELETE FROM inspeccion_preoperacional_carga 
            WHERE fecha_inspeccion < DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
        """)
        
        filas_eliminadas = cur.rowcount
        mysql.connection.commit()
        
        return jsonify({
            "success": True, 
            "message": "Mantenimiento BD Flota completado exitosamente.",
            "registros_eliminados": filas_eliminadas
        }), 200

    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"success": False, "message": f"Error en mantenimiento: {str(e)}"}), 500
    finally:
        cur.close()

@bp_gestorflota.route('/preoperacionales/pdf/<consecutivo>', methods=['GET'])
@login_required_custom
@gestor_flota_required
def descargar_preoperacional_pdf(consecutivo):
    import base64
    
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    nit_empresa = session.get('nit')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM inspeccion_preoperacional_carga WHERE consecutivo_anual = %s AND id_empresa = %s LIMIT 1", (consecutivo, empresa_id))
    insp = cur.fetchone()
    cur.close()

    if not insp:
        flash("Error: Inspección no encontrada o no pertenece a tu empresa.", "danger")
        return redirect(url_for('gestorflota.historial_preoperacionales'))

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=14, textColor=colors.HexColor('#015249'), alignment=1, spaceAfter=10)
    sub_title_style = ParagraphStyle('SubTitle', parent=styles['Heading2'], fontSize=11, textColor=colors.HexColor('#015249'), spaceAfter=5, spaceBefore=10)
    cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=8, leading=10)
    cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold')

    # 1. Cabecera con Logos
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

    # 2. Metadatos
    dictamen_texto = "APROBADO (OPERATIVO)" if insp['vehiculo_aprobado'] == 1 else "RECHAZADO (ALERTA CRÍTICA)"
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

    # Helper para renderizar estados
    def get_estado_html(valor, es_doc=False):
        if es_doc:
            return "<font color='#16a34a'><b>AL DÍA</b></font>" if valor == 1 else "<font color='#dc2626'><b>FALTANTE/VENCIDO</b></font>"
        if valor == 1: return "<font color='#16a34a'>Operativo</font>"
        elif valor == 2: return "<font color='#d97706'><b>Ajuste</b></font>"
        elif valor == 3: return "<font color='#dc2626'><b>Crítico</b></font>"
        return "N/A"

    # 3. Mapeo del Checklist Completo
    checklist_config = [
        ("DOCUMENTACIÓN LEGAL", True, [
            ('doc_licencia_conduccion', 'Licencia de Conducción'), ('doc_soat_vigente', 'SOAT Vigente'),
            ('doc_tecnomecanica_vigente', 'Revisión Tecnomecánica'), ('doc_tarjeta_operacion', 'Tarjeta de Operación'),
            ('doc_manifiesto_carga', 'Manifiesto de Carga')
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

    # Renderizar cada bloque del checklist
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

    # 4. Esquema Individual de Llantas (JSON)
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

    # Helper para convertir Base64 a RLImage
    def convertir_base64_rlimage(b64_string, w, h):
        try:
            if b64_string and ',' in b64_string:
                img_data = base64.b64decode(b64_string.split(',')[1])
                img_buffer = io.BytesIO(img_data)
                return RLImage(img_buffer, width=w, height=h, kind='proportional')
        except Exception as e:
            pass
        return Paragraph("<i>No disponible</i>", cell_style)

    # 5. Observaciones Finales y Firma Biométrica
    if insp.get('observaciones_hallazgos'):
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>OBSERVACIONES GENERALES DEL CONDUCTOR</b>", sub_title_style))
        story.append(Paragraph(f"<i>{insp['observaciones_hallazgos']}</i>", cell_style))

    story.append(Spacer(1, 15))
    story.append(Paragraph("<b>AUTENTICACIÓN Y FIRMA</b>", sub_title_style))
    story.append(Paragraph(f"Yo, <b>{insp['nombre_conductor']}</b>, declaro que la información contenida en este documento es veraz.", cell_style))
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
        download_name=f"Preoperacional_{consecutivo}.pdf", 
        mimetype='application/pdf'
    )