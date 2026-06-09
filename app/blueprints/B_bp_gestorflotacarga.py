# app/blueprints/B_bp_gestorflotacarga.py
import os
import io
import json
import qrcode
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, send_file
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

# Librerías para generación de PDF
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader

# Definición manual del tamaño Media Carta (5.5 x 8.5 pulgadas)
halfLetter = (5.5 * inch, 8.5 * inch)

bp_gestorflota = Blueprint('gestorflota', __name__, url_prefix='/gestor_flota')

# --- DECORADOR DE PERFIL ESPECÍFICO ---
def gestor_flota_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['gestor_flotacarga', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Gestor de Flota para ingresar a este módulo.', 'danger')
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
        'B_dashboard_gestortc.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='dashboard' 
    )

# =========================================================
# MÓDULO 1: GESTIÓN DE VEHÍCULOS (CRUD)
# =========================================================

@bp_gestorflota.route('/vehiculos', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_vehiculos():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        # --- CREAR VEHÍCULO ---
        if accion == 'crear':
            placa = str(request.form.get('placa', '')).upper().strip()
            tipo = request.form.get('tipo', '').strip()
            referencia = request.form.get('referencia', '').strip()
            peso_vacio = request.form.get('peso_vacio', 0)
            capacidad = request.form.get('capacidad', 0)
            
            if placa and tipo:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        INSERT INTO vehiculos (empresa, id_empresa, placa, tipo, referencia, peso_vacio, `capacidad (kg)`) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (empresa_nombre, empresa_id, placa, tipo, referencia, peso_vacio, capacidad))
                    mysql.connection.commit()
                    flash(f"Vehículo con placa {placa} registrado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al registrar vehículo: {str(e)}", "danger")
                finally:
                    cur.close()

        # --- EDITAR VEHÍCULO ---
        elif accion == 'editar':
            vehiculo_id = request.form.get('vehiculo_id')
            placa = str(request.form.get('placa', '')).upper().strip()
            tipo = request.form.get('tipo', '').strip()
            referencia = request.form.get('referencia', '').strip()
            peso_vacio = request.form.get('peso_vacio', 0)
            capacidad = request.form.get('capacidad', 0)
            
            if vehiculo_id and placa and tipo:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("""
                        UPDATE vehiculos 
                        SET placa = %s, tipo = %s, referencia = %s, peso_vacio = %s, `capacidad (kg)` = %s
                        WHERE id = %s AND id_empresa = %s
                    """, (placa, tipo, referencia, peso_vacio, capacidad, vehiculo_id, empresa_id))
                    mysql.connection.commit()
                    flash(f"Vehículo {placa} actualizado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al actualizar vehículo: {str(e)}", "danger")
                finally:
                    cur.close()

        # --- ELIMINAR VEHÍCULO ---
        elif accion == 'eliminar':
            vehiculo_id = request.form.get('vehiculo_id')
            cur = mysql.connection.cursor()
            try:
                # Validamos doblemente con id_empresa por seguridad
                cur.execute("DELETE FROM vehiculos WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                mysql.connection.commit()
                flash("Vehículo eliminado de la base de datos.", "success")
            except Exception as e:
                flash("Error al eliminar vehículo.", "danger")
            finally:
                cur.close()

        return redirect(url_for('gestorflota.gestion_vehiculos'))

    # --- LECTURA (GET) PARA MOSTRAR LA TABLA ---
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM vehiculos WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_dashboard_gestortc.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='vehiculos', 
        vehiculos=vehiculos_db
    )

# =========================================================
# MÓDULO 2: GESTIÓN DE RUTAS (CRUD NUEVO)
# =========================================================

@bp_gestorflota.route('/rutas', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_rutas():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        # --- CREAR RUTA ---
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

        # --- EDITAR RUTA ---
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

        # --- ELIMINAR RUTA ---
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

    # --- LECTURA (GET) PARA MOSTRAR LA TABLA ---
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM rutas WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    rutas_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_dashboard_gestortc.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='rutas', 
        rutas=rutas_db
    )

# =========================================================
# MÓDULO 3: CÓDIGOS QR (VISTA Y GENERACIÓN PDF)
# =========================================================

@bp_gestorflota.route('/qrs')
@login_required_custom
@gestor_flota_required
def generacion_qrs():
    """Muestra la tabla de vehículos lista para generar los QR."""
    empresa_id = session.get('empresa_id')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, placa, tipo, `capacidad (kg)`, referencia FROM vehiculos WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_dashboard_gestortc.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='qrs', 
        vehiculos=vehiculos_db
    )

def _generar_pdf_qrs(vehiculos_list, nit_empresa, nombre_empresa):
    """Función interna para construir el PDF Media Carta con códigos QR."""
    buffer = io.BytesIO()
    
    c = canvas.Canvas(buffer, pagesize=halfLetter)
    width, height = halfLetter

    # Rutas físicas de los logos
    base_dir = os.path.abspath(os.path.dirname(__file__))
    static_dir = os.path.join(base_dir, '..', 'static')
    logo_cliente_path = os.path.join(static_dir, f'logo_{nit_empresa}.PNG')
    logo_app_path = os.path.join(static_dir, 'logo_energix360.png')

    for v in vehiculos_list:
        placa = str(v['placa']).strip().upper()
        
        # 1. Preparar Payload del QR compatible con el lector actual
        qr_data = {
            "placa": placa,
            "nit": str(nit_empresa),
            "empresa": nombre_empresa
        }
        qr_json = json.dumps(qr_data)
        
        # 2. Crear la imagen QR en memoria
        qr = qrcode.QRCode(version=1, box_size=10, border=2)
        qr.add_data(qr_json)
        qr.make(fit=True)
        img_qr = qr.make_image(fill_color="#015249", back_color="white") 
        
        qr_buffer = io.BytesIO()
        img_qr.save(qr_buffer, format="PNG")
        qr_buffer.seek(0)
        qr_image_reader = ImageReader(qr_buffer)
        
        # 3. Dibujar en el PDF
        # Logo Cliente (Arriba Centro)
        if os.path.exists(logo_cliente_path):
            try:
                c.drawImage(logo_cliente_path, width/2 - 1.5*inch, height - 1.5*inch, width=3*inch, height=1*inch, preserveAspectRatio=True, anchor='c')
            except Exception:
                pass
        
        # Textos (Placa y Nombre)
        c.setFillColorRGB(0.0039, 0.321, 0.286) # Verde BQA-ONE (#015249)
        c.setFont("Helvetica-Bold", 32)
        c.drawCentredString(width/2, height - 2.2*inch, placa)
        
        c.setFillColorRGB(0.2, 0.2, 0.2)
        c.setFont("Helvetica", 14)
        c.drawCentredString(width/2, height - 2.5*inch, f"Propiedad de: {nombre_empresa}")
        
        # Código QR (Centro)
        qr_size = 3.8 * inch
        c.drawImage(qr_image_reader, width/2 - qr_size/2, height/2 - qr_size/2 - 0.3*inch, width=qr_size, height=qr_size)
        
        # Logo App (Abajo Centro)
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
    """Genera el PDF Media Carta de un solo QR por ID."""
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
    """Genera un PDF multipágina con todos los QRs de la empresa."""
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

# =========================================================
# MÓDULO 4: GESTIÓN DE OPERADORES (CONDUCTOR / AUXILIAR)
# =========================================================

@bp_gestorflota.route('/operadores', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_operadores():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        # --- CREAR OPERADOR ---
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
                # El Auxiliar no tendrá acceso (contraseña aleatoria in-descifrable)
                hashed_pw = bcrypt.generate_password_hash(os.urandom(12).hex()).decode('utf-8')

            if nombre and cedula and perfil:
                cur = mysql.connection.cursor()
                try:
                    # Evitar duplicados por cédula
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

        # --- EDITAR OPERADOR ---
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

        # --- ELIMINAR OPERADOR ---
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

    # --- LECTURA (GET) PARA MOSTRAR LA TABLA ---
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
        'B_dashboard_gestortc.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='operadores', 
        operadores=operadores_db
    )

# =========================================================
# RUTAS PENDIENTES
# =========================================================

@bp_gestorflota.route('/preoperacionales')
@login_required_custom
@gestor_flota_required
def historial_preoperacionales():
    return "Módulo de Historial Preoperacionales en construcción..."