# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: GESTION_FLOTA | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/B_bp_flotacarga_flota.py
import os
import time
import json
import io
import base64
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, current_app, send_file
from werkzeug.utils import secure_filename
from PIL import Image
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

# Librerías PDF (Reportes Platypus)
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

bp_flotacarga_flota = Blueprint('flotacarga_flota', __name__, url_prefix='/gestor_flota/flota_bp')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
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
            autorizados = session.get('submodulos_activos', [])
            if not autorizados:
                autorizados = session.get('modulos_activos', [])
                
            if not any(m in autorizados for m in ['controlador_flota', 'flota', 'carga']):
                flash('Acceso denegado: Tu empresa no tiene activo este submódulo.', 'danger')
                return redirect('/gestor_flota/dashboard')
                
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# MOTOR DE CONVERSIÓN Y PROCESAMIENTO DE ARCHIVOS
# ==============================================================================
def procesar_archivo_pdf(archivo, identificador, tipo_doc, empresa_id):
    """
    Recibe un archivo (FileStorage de Flask).
    Si es FOTO_FACIAL, la guarda en su formato original de imagen para reconocimiento biométrico.
    Para los demás, si es PDF lo guarda directo; si es imagen, la convierte a PDF mediante Pillow.
    Retorna la ruta relativa para almacenar en BD.
    """
    if not archivo or archivo.filename == '':
        return None
    
    filename = secure_filename(archivo.filename)
    ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
    
    # Ruta física: app/static/uploads/flotacarga/documentos/<empresa_id>/<identificador>/
    base_dir = os.path.join(current_app.root_path, 'static', 'uploads', 'flotacarga', 'documentos', str(empresa_id), str(identificador).upper())
    os.makedirs(base_dir, exist_ok=True)
    
    timestamp = int(time.time())
    
    # Excepción para Foto Facial: mantener como imagen
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

    # Procesamiento estándar a PDF para documentos
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
            # Convertir y guardar en formato PDF
            image.save(ruta_fisica, 'PDF', resolution=100.0)
        else:
            return None # Extensión no soportada
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
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

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
                    cur.execute("""
                        INSERT INTO vehiculos (
                            empresa, id_empresa, placa, tipo, caja_de_carga, referencia, peso_vacio, `capacidad (kg)`, propiedad,
                            id_tarjeta_propiedad, ruta_tarjeta_propiedad,
                            id_soat, vencimiento_soat, ruta_soat,
                            id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecnomecanica,
                            id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_poliza_rce
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        empresa_nombre, empresa_id, placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, propiedad,
                        id_tarjeta_propiedad, ruta_tarjeta,
                        id_soat, vencimiento_soat, ruta_soat,
                        id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecno,
                        id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_rce
                    ))
                    mysql.connection.commit()
                    flash(f"Vehículo con placa {placa} registrado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al registrar vehículo: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'editar':
            vehiculo_id = request.form.get('vehiculo_id')
            
            if vehiculo_id and placa and caja_de_carga and tipo:
                cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
                try:
                    cur.execute("SELECT * FROM vehiculos WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                    old_data = cur.fetchone()
                    
                    if old_data:
                        ruta_tarjeta = procesar_archivo_pdf(request.files.get('archivo_tarjeta_propiedad'), placa, 'TARJETA_PROP', empresa_id) or old_data.get('ruta_tarjeta_propiedad')
                        ruta_soat = procesar_archivo_pdf(request.files.get('archivo_soat'), placa, 'SOAT', empresa_id) or old_data.get('ruta_soat')
                        ruta_tecno = procesar_archivo_pdf(request.files.get('archivo_tecnomecanica'), placa, 'TECNOMECANICA', empresa_id) or old_data.get('ruta_tecnomecanica')
                        ruta_rce = procesar_archivo_pdf(request.files.get('archivo_poliza_rce'), placa, 'POLIZA_RCE', empresa_id) or old_data.get('ruta_poliza_rce')
                        
                        cur = mysql.connection.cursor()
                        cur.execute("""
                            UPDATE vehiculos 
                            SET placa = %s, tipo = %s, caja_de_carga = %s, referencia = %s, peso_vacio = %s, `capacidad (kg)` = %s, propiedad = %s,
                                id_tarjeta_propiedad = %s, ruta_tarjeta_propiedad = %s,
                                id_soat = %s, vencimiento_soat = %s, ruta_soat = %s,
                                id_tecnomecanica = %s, vencimiento_tecnomecanica = %s, ruta_tecnomecanica = %s,
                                id_poliza_rce = %s, aseguradora_rce = %s, vencimiento_poliza_rce = %s, ruta_poliza_rce = %s
                            WHERE id = %s AND id_empresa = %s
                        """, (
                            placa, tipo, caja_de_carga, referencia, peso_vacio, capacidad, propiedad,
                            id_tarjeta_propiedad, ruta_tarjeta,
                            id_soat, vencimiento_soat, ruta_soat,
                            id_tecnomecanica, vencimiento_tecnomecanica, ruta_tecno,
                            id_poliza_rce, aseguradora_rce, vencimiento_poliza_rce, ruta_rce,
                            vehiculo_id, empresa_id
                        ))
                        mysql.connection.commit()
                        flash(f"Vehículo {placa} actualizado correctamente.", "success")
                except Exception as e:
                    flash(f"Error al actualizar vehículo: {str(e)}", "danger")
                finally:
                    cur.close()
            else:
                flash("No fue posible editar: verifique que la clasificación RUNT y la caja de carga estén seleccionadas.", "danger")

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

        return redirect(url_for('flotacarga_flota.gestion_vehiculos'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
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


@bp_flotacarga_flota.route('/operadores', methods=['GET', 'POST'])
@login_required_custom
@gestor_flota_required
def gestion_operadores():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

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
            vencimiento_licencia_conduccion = request.form.get('vencimiento_licencia_conduccion', '').strip() or None
            ultimo_pago_seguridad_social = request.form.get('ultimo_pago_seguridad_social', '').strip() or None
            vencimiento_seguridad_social = request.form.get('vencimiento_seguridad_social', '').strip() or None

            if not nombre or not cedula:
                flash("El nombre y la cédula son obligatorios.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_operadores'))

            if tipo_conductor == 'Propio' and not password:
                flash("El conductor propio requiere una contraseña de acceso.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_operadores'))

            # Procesamiento de documentos y biometría
            ruta_licencia = procesar_archivo_pdf(request.files.get('archivo_licencia'), cedula, 'LICENCIA', empresa_id)
            ruta_seg_soc = procesar_archivo_pdf(request.files.get('archivo_seguridad_social'), cedula, 'SEG_SOCIAL', empresa_id)
            ruta_pago = procesar_archivo_pdf(request.files.get('archivo_soporte_pago'), cedula, 'PAGO', empresa_id)
            ruta_foto = procesar_archivo_pdf(request.files.get('archivo_foto_facial'), cedula, 'FOTO_FACIAL', empresa_id)

            cur = mysql.connection.cursor()
            try:
                cur.execute("SELECT id FROM conductores_carga WHERE cedula = %s AND id_empresa = %s", (cedula, empresa_id))
                if cur.fetchone():
                    flash(f"El conductor con cédula {cedula} ya existe.", "danger")
                else:
                    # Inserción en Hoja de Vida Integral (conductores_carga)
                    cur.execute("""
                        INSERT INTO conductores_carga (
                            id_empresa, tipo_conductor, nombre, cedula, fecha_nacimiento, edad,
                            password_visible, email, vehiculo_asignado,
                            eps, arl, fondo_pensiones, numero_licencia_conduccion, vencimiento_licencia_conduccion,
                            ultimo_pago_seguridad_social, vencimiento_seguridad_social,
                            ruta_pdf_licencia, ruta_pdf_seguridad_social, ruta_pdf_soporte_pago, ruta_foto_facial
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        empresa_id, tipo_conductor, nombre, cedula, fecha_nacimiento, edad,
                        password if tipo_conductor == 'Propio' else None, email, vehiculo_asignado,
                        eps, arl, fondo_pensiones, numero_licencia_conduccion, vencimiento_licencia_conduccion,
                        ultimo_pago_seguridad_social, vencimiento_seguridad_social,
                        ruta_licencia, ruta_seg_soc, ruta_pago, ruta_foto
                    ))
                    
                    # Si es Propio, se le crea usuario maestro de App
                    if tipo_conductor == 'Propio':
                        hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
                        cur.execute("""
                            INSERT INTO usuarios (nombre, cedula, password, perfil, empresa, empresa_id) 
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (nombre, cedula, hashed_pw, perfil, empresa_nombre, empresa_id))
                    
                    mysql.connection.commit()
                    flash(f"Personal registrado exitosamente.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al registrar: {str(e)}", "danger")
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
            vencimiento_licencia_conduccion = request.form.get('vencimiento_licencia_conduccion', '').strip() or None
            ultimo_pago_seguridad_social = request.form.get('ultimo_pago_seguridad_social', '').strip() or None
            vencimiento_seguridad_social = request.form.get('vencimiento_seguridad_social', '').strip() or None

            if operador_id and nombre and cedula:
                cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
                try:
                    cur.execute("SELECT * FROM conductores_carga WHERE id = %s AND id_empresa = %s", (operador_id, empresa_id))
                    old_data = cur.fetchone()

                    if old_data:
                        ruta_licencia = procesar_archivo_pdf(request.files.get('archivo_licencia'), cedula, 'LICENCIA', empresa_id) or old_data.get('ruta_pdf_licencia')
                        ruta_seg_soc = procesar_archivo_pdf(request.files.get('archivo_seguridad_social'), cedula, 'SEG_SOCIAL', empresa_id) or old_data.get('ruta_pdf_seguridad_social')
                        ruta_pago = procesar_archivo_pdf(request.files.get('archivo_soporte_pago'), cedula, 'PAGO', empresa_id) or old_data.get('ruta_pdf_soporte_pago')
                        ruta_foto = procesar_archivo_pdf(request.files.get('archivo_foto_facial'), cedula, 'FOTO_FACIAL', empresa_id) or old_data.get('ruta_foto_facial')

                        # Si se proporciona contraseña nueva, actualizamos el visible
                        pass_visible = password_nueva if password_nueva else old_data.get('password_visible')
                        if tipo_conductor == 'Tercero':
                            pass_visible = None

                        # 1. Actualizar Hoja de Vida Integral
                        cur.execute("""
                            UPDATE conductores_carga SET
                                tipo_conductor = %s, nombre = %s, cedula = %s, fecha_nacimiento = %s, edad = %s,
                                password_visible = %s, email = %s, vehiculo_asignado = %s,
                                eps = %s, arl = %s, fondo_pensiones = %s, numero_licencia_conduccion = %s, vencimiento_licencia_conduccion = %s,
                                ultimo_pago_seguridad_social = %s, vencimiento_seguridad_social = %s,
                                ruta_pdf_licencia = %s, ruta_pdf_seguridad_social = %s, ruta_pdf_soporte_pago = %s, ruta_foto_facial = %s
                            WHERE id = %s AND id_empresa = %s
                        """, (
                            tipo_conductor, nombre, cedula, fecha_nacimiento, edad,
                            pass_visible, email, vehiculo_asignado,
                            eps, arl, fondo_pensiones, numero_licencia_conduccion, vencimiento_licencia_conduccion,
                            ultimo_pago_seguridad_social, vencimiento_seguridad_social,
                            ruta_licencia, ruta_seg_soc, ruta_pago, ruta_foto,
                            operador_id, empresa_id
                        ))

                        # 2. Sincronizar con Usuarios (Login Maestro)
                        cur.execute("SELECT id FROM usuarios WHERE cedula = %s AND empresa_id = %s", (old_data['cedula'], empresa_id))
                        usr = cur.fetchone()

                        if tipo_conductor == 'Propio':
                            if usr:
                                if password_nueva:
                                    hashed_pw = bcrypt.generate_password_hash(password_nueva).decode('utf-8')
                                    cur.execute("UPDATE usuarios SET nombre=%s, cedula=%s, perfil=%s, password=%s WHERE id=%s", (nombre, cedula, perfil, hashed_pw, usr['id']))
                                else:
                                    cur.execute("UPDATE usuarios SET nombre=%s, cedula=%s, perfil=%s WHERE id=%s", (nombre, cedula, perfil, usr['id']))
                            else:
                                if pass_visible:
                                    hashed_pw = bcrypt.generate_password_hash(pass_visible).decode('utf-8')
                                    cur.execute("""
                                        INSERT INTO usuarios (nombre, cedula, password, perfil, empresa, empresa_id) 
                                        VALUES (%s, %s, %s, %s, %s, %s)
                                    """, (nombre, cedula, hashed_pw, perfil, empresa_nombre, empresa_id))
                        elif tipo_conductor == 'Tercero' and usr:
                            # Terceros no tienen login
                            cur.execute("DELETE FROM usuarios WHERE id = %s", (usr['id'],))

                        mysql.connection.commit()
                        flash("Registro de personal actualizado.", "success")
                except Exception as e:
                    mysql.connection.rollback()
                    flash(f"Error al actualizar: {str(e)}", "danger")
                finally:
                    cur.close()

        elif accion == 'eliminar':
            operador_id = request.form.get('operador_id')
            cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
            try:
                cur.execute("SELECT cedula FROM conductores_carga WHERE id = %s AND id_empresa = %s", (operador_id, empresa_id))
                cd_data = cur.fetchone()
                if cd_data:
                    cur.execute("DELETE FROM usuarios WHERE cedula = %s AND empresa_id = %s", (cd_data['cedula'], empresa_id))
                    cur.execute("DELETE FROM conductores_carga WHERE id = %s AND id_empresa = %s", (operador_id, empresa_id))
                    mysql.connection.commit()
                    flash("Registro desvinculado por completo.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash("Error al eliminar.", "danger")
            finally:
                cur.close()

        return redirect(url_for('flotacarga_flota.gestion_operadores'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # Lista de Vehículos para Selector de Terceros
    cur.execute("SELECT id, placa, propiedad FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()

    cur.execute("""
        SELECT c.*, 
               IFNULL(u.perfil, 'operador_flotacarga') as perfil 
        FROM conductores_carga c 
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
    
    # Consultar Vehículos (Propios y Terceros) registrados para la empresa
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

@bp_flotacarga_flota.route('/documentos_vencidos', methods=['GET'])
@login_required_custom
@gestor_flota_required
def gestion_documentos_vencidos():
    empresa_id = session.get('empresa_id')
    hoy = datetime.now().date()
    
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
        
    # Motor simplificado local para vehículos
    for v in vehiculos:
        docs_v = [
            ("SOAT", "vencimiento_soat", parse_date(v.get('vencimiento_soat'))),
            ("Tecnomecánica", "vencimiento_tecnomecanica", parse_date(v.get('vencimiento_tecnomecanica'))),
            ("Póliza RCE", "vencimiento_poliza_rce", parse_date(v.get('vencimiento_poliza_rce')))
        ]
        for nombre_doc, clave, fecha in docs_v:
            if not fecha:
                novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Faltante", "dias": "N/A"})
            else:
                dias = (fecha - hoy).days
                if dias <= 0:
                    novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Vencido", "dias": abs(dias)})
                elif dias <= 8:
                    novedades.append({"entidad_tipo": "vehiculo", "entidad_id": v['id'], "identificador": v['placa'], "nombre": "Vehículo " + v['placa'], "documento": nombre_doc, "clave": clave, "estado": "Por Vencer", "dias": dias})
                    
    # Motor simplificado local para conductores
    for c in conductores:
        # Licencia
        f_exp = parse_date(c.get('fecha_expedicion_licencia'))
        f_nac = parse_date(c.get('fecha_nacimiento'))
        cat = str(c.get('categoria_licencia', '')).strip().capitalize()
        
        if f_exp and f_nac:
            edad_exp = relativedelta(f_exp, f_nac).years
            if cat == 'Particular':
                if edad_exp < 60: f_teo = f_exp + relativedelta(years=10)
                elif 60 <= edad_exp <= 79: f_teo = f_exp + relativedelta(years=5)
                else: f_teo = f_exp + relativedelta(years=1)
            elif cat in ['Publico', 'Público']:
                if edad_exp < 60: f_teo = f_exp + relativedelta(years=3)
                else: f_teo = f_exp + relativedelta(years=1)
            else:
                f_teo = f_exp + relativedelta(years=3)
                
            dias_lic = (f_teo - hoy).days
            if dias_lic <= 0:
                novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": "Licencia de Conducción", "clave": "licencia_conduccion", "estado": "Vencido", "dias": abs(dias_lic)})
            elif dias_lic <= 30:
                novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": "Licencia de Conducción", "clave": "licencia_conduccion", "estado": "Por Vencer", "dias": dias_lic})
        else:
            novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": "Licencia de Conducción", "clave": "licencia_conduccion", "estado": "Faltante", "dias": "N/A"})
            
        # Seguridad Social
        ult_pago = parse_date(c.get('ultimo_pago_seguridad_social'))
        if ult_pago:
            f_fin = ult_pago + timedelta(days=30)
            dias_ss = (f_fin - hoy).days
            if dias_ss <= 0:
                novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": "Seguridad Social (Planilla)", "clave": "seguridad_social", "estado": "Vencido", "dias": abs(dias_ss)})
            elif dias_ss <= 5:
                novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": "Seguridad Social (Planilla)", "clave": "seguridad_social", "estado": "Por Vencer", "dias": dias_ss})
        else:
            novedades.append({"entidad_tipo": "conductor", "entidad_id": c['id'], "identificador": c['cedula'], "nombre": c['nombre'], "documento": "Seguridad Social (Planilla)", "clave": "seguridad_social", "estado": "Faltante", "dias": "N/A"})
    
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
            elif documento_clave == 'vencimiento_tecnomecanica':
                tipo_doc = 'TECNOMECANICA'
                col_fecha = 'vencimiento_tecnomecanica'
                col_ruta = 'ruta_tecnomecanica'
            elif documento_clave == 'vencimiento_poliza_rce':
                tipo_doc = 'POLIZA_RCE'
                col_fecha = 'vencimiento_poliza_rce'
                col_ruta = 'ruta_poliza_rce'
            else:
                flash("Tipo de documento de vehículo inválido.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            ruta_pdf = procesar_archivo_pdf(archivo, placa, tipo_doc, empresa_id)
            if not ruta_pdf:
                flash("Error al procesar el archivo.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            cur.execute(f"UPDATE vehiculos SET {col_fecha} = %s, {col_ruta} = %s WHERE id = %s", (nueva_fecha, ruta_pdf, entidad_id))
            mysql.connection.commit()
            flash(f"Documento {tipo_doc} actualizado correctamente para el vehículo {placa}.", "success")
            
        elif entidad_tipo == 'conductor':
            cur.execute("SELECT cedula, nombre FROM conductores_carga WHERE id = %s AND id_empresa = %s", (entidad_id, empresa_id))
            c = cur.fetchone()
            if not c:
                flash("Conductor no encontrado.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            cedula = c['cedula']
            nombre = c['nombre']
            
            if documento_clave == 'licencia_conduccion':
                tipo_doc = 'LICENCIA'
                col_fecha = 'vencimiento_licencia_conduccion'
                col_ruta = 'ruta_pdf_licencia'
            elif documento_clave == 'seguridad_social':
                tipo_doc = 'SEG_SOCIAL'
                col_fecha = 'ultimo_pago_seguridad_social' 
                col_ruta = 'ruta_pdf_seguridad_social'
            else:
                flash("Tipo de documento de conductor inválido.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            ruta_pdf = procesar_archivo_pdf(archivo, cedula, tipo_doc, empresa_id)
            if not ruta_pdf:
                flash("Error al procesar el archivo.", "danger")
                return redirect(url_for('flotacarga_flota.gestion_documentos_vencidos'))
                
            cur.execute(f"UPDATE conductores_carga SET {col_fecha} = %s, {col_ruta} = %s WHERE id = %s", (nueva_fecha, ruta_pdf, entidad_id))
            mysql.connection.commit()
            flash(f"Documento {tipo_doc} actualizado correctamente para {nombre}.", "success")
            
    except Exception as e:
        mysql.connection.rollback()
        flash(f"Error al actualizar documento: {str(e)}", "danger")
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
    return send_file(buffer, as_attachment=True, download_name=f"Preoperacional_Carga_{consecutivo}.pdf", mimetype='application/pdf')