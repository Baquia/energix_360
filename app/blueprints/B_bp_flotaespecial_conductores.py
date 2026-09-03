# app/blueprints/B_bp_flotaespecial_conductores.py
import os
import uuid
from datetime import datetime, timedelta
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, current_app, jsonify
from werkzeug.utils import secure_filename
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

bp_flotaespecial_conductores = Blueprint('flotaespecial_conductores', __name__, url_prefix='/gestor_flotaespecial/conductores_bp')

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
# HELPER: CREACIÓN DE TABLAS (CONDUCTORES Y AUDITORÍA)
# =========================================================
def asegurar_tablas_conductores(cur):
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
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX(id_empresa),
            INDEX(cedula)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)
    
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
# HELPER: GUARDAR PDF MANUAL
# =========================================================
def guardar_pdf_manual(file_obj, prefix):
    if file_obj and file_obj.filename.endswith('.pdf'):
        filename = secure_filename(f"{prefix}_{uuid.uuid4().hex[:8]}.pdf")
        ruta_base = os.path.join(current_app.static_folder, 'uploads', 'flotaespecial', 'conductores')
        os.makedirs(ruta_base, exist_ok=True)
        file_obj.save(os.path.join(ruta_base, filename))
        return f"uploads/flotaespecial/conductores/{filename}"
    return None

# =========================================================
# RUTAS: CRUD Y GESTIÓN DE CONDUCTORES
# =========================================================
@bp_flotaespecial_conductores.route('/', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_conductores():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    asegurar_tablas_conductores(cur)
    mysql.connection.commit()

    if request.method == 'POST':
        accion = request.form.get('accion')

        if accion in ['crear', 'editar']:
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

            # Cálculo exacto de vencimiento de seguridad social (+30 días)
            vencimiento_ss = None
            if ultimo_pago_ss:
                try:
                    fecha_pago = datetime.strptime(ultimo_pago_ss, '%Y-%m-%d')
                    vencimiento_ss = (fecha_pago + timedelta(days=30)).strftime('%Y-%m-%d')
                except ValueError:
                    vencimiento_ss = None

            # Archivos PDF
            r_ced = guardar_pdf_manual(request.files.get('file_pdf_cedula'), 'ced')
            r_lic = guardar_pdf_manual(request.files.get('file_pdf_licencia'), 'lic')
            r_ss = guardar_pdf_manual(request.files.get('file_pdf_seguridad_social'), 'ss')

            if nombre and cedula:
                try:
                    if accion == 'crear':
                        # 1. Crear en tabla usuarios (Perfil: operador_flotaespecial)
                        cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
                        if cur.fetchone():
                            flash(f"La cédula {cedula} ya está registrada como usuario.", "danger")
                            return redirect(url_for('flotaespecial_conductores.gestion_conductores'))
                        
                        hashed_pw = bcrypt.generate_password_hash(cedula).decode('utf-8')
                        cur.execute("""
                            INSERT INTO usuarios (nombre, cedula, password, tipo_usuario, clase, perfil, empresa, empresa_id, telegram_id) 
                            VALUES (%s, %s, %s, 'cliente', 'op', 'operador_flotaespecial', %s, %s, %s)
                        """, (nombre, cedula, hashed_pw, empresa_nombre, empresa_id, telegram_id or None))
                        
                        # 2. Insertar en tabla conductores_flotaespecial
                        cur.execute("""
                            INSERT INTO conductores_flotaespecial 
                            (id_empresa, nombre, cedula, departamento_base, municipio_base, numero_licencia_conduccion, 
                             vencimiento_licencia_conduccion, eps, fondo_pensiones, arl, ultimo_pago_seguridad_social, 
                             vencimiento_seguridad_social, ruta_pdf_cedula, ruta_pdf_licencia, ruta_pdf_seguridad_social) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (empresa_id, nombre, cedula, departamento_base, municipio_base, numero_licencia,
                              vencimiento_licencia, eps, fondo_pensiones, arl, ultimo_pago_ss,
                              vencimiento_ss, r_ced or '', r_lic or '', r_ss or ''))
                        
                        mysql.connection.commit()
                        flash(f"Conductor {nombre} registrado exitosamente.", "success")
                        
                    elif accion == 'editar' and conductor_id:
                        # Actualizar conductor
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
                        
                        # Actualizar usuario relacionado
                        cur.execute("UPDATE usuarios SET nombre=%s, telegram_id=%s WHERE cedula=%s AND empresa_id=%s", (nombre, telegram_id or None, cedula, empresa_id))

                        mysql.connection.commit()
                        flash(f"Expediente del conductor {nombre} actualizado.", "success")

                except Exception as e:
                    mysql.connection.rollback()
                    flash(f"Error en base de datos: {str(e)}", "danger")
            else:
                flash("Faltan datos obligatorios (Nombre y Cédula).", "warning")

        elif accion == 'eliminar':
            conductor_id = request.form.get('conductor_id')
            cedula_eliminar = request.form.get('cedula')
            try:
                cur.execute("DELETE FROM conductores_flotaespecial WHERE id = %s AND id_empresa = %s", (conductor_id, empresa_id))
                cur.execute("DELETE FROM usuarios WHERE cedula = %s AND empresa_id = %s", (cedula_eliminar, empresa_id))
                mysql.connection.commit()
                flash("Conductor eliminado permanentemente de la flota y del sistema de usuarios.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash("Error al eliminar conductor.", "danger")

        return redirect(url_for('flotaespecial_conductores.gestion_conductores'))

    # Modo Lectura - Listado de conductores
    cur.execute("""
        SELECT c.*, u.telegram_id 
        FROM conductores_flotaespecial c
        LEFT JOIN usuarios u ON c.cedula = u.cedula AND c.id_empresa = u.empresa_id
        WHERE c.id_empresa = %s 
        ORDER BY c.nombre ASC
    """, (empresa_id,))
    conductores_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_modulo_flotaespecial_conductores.html',
        nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        conductores=conductores_db
    )

# =========================================================
# RUTAS: VISOR INDIVIDUAL DE CONDUCTOR (AJAX)
# =========================================================
@bp_flotaespecial_conductores.route('/visor_individual', methods=['GET'])
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
        LEFT JOIN usuarios u ON c.cedula = u.cedula AND c.id_empresa = u.empresa_id
        WHERE c.cedula = %s AND c.id_empresa = %s LIMIT 1
    """, (cedula_busqueda, empresa_id))
    conductor = cur.fetchone()
    cur.close()

    if conductor:
        return jsonify({'success': True, 'conductor': conductor})
    else:
        return jsonify({'success': False, 'message': f'Conductor con cédula {cedula_busqueda} no encontrado.'})