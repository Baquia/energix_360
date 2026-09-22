# app/blueprints/B_bp_controlador_flotaespecial.py
import os
import hashlib
import pytz
from datetime import datetime, timedelta
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify, current_app
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

bp_controlador_flotaespecial = Blueprint('controlador_flotaespecial', __name__, url_prefix='/gestor_flotaespecial')
BOGOTA_TZ = pytz.timezone('America/Bogota')

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
# 1. SUB-MENÚ INTERMEDIO DE SELECCIÓN
# =========================================================
@bp_controlador_flotaespecial.route('/dashboard')
@login_required_custom
@controlador_flotaespecial_required
def dashboard_controlador():
    return render_template(
        'B_modulo_controlador_flotaespecial.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        perfil=session.get('perfil')
    )

# =========================================================
# 2. DASHBOARD OPERATIVO DE CONTROL Y MONITOREO
# =========================================================
@bp_controlador_flotaespecial.route('/operativa')
@login_required_custom
@controlador_flotaespecial_required
def dashboard_operativo():
    empresa_id = session.get('empresa_id')
    empresa_nit = session.get('nit')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN oculto_kanban BOOLEAN DEFAULT FALSE")
    except: pass
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN vuelta_activada BOOLEAN DEFAULT FALSE")
    except: pass
    
    # KPIs generales activos o del día (Se elimina filtro de fechas y se hace 100% dinámico)
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

    # Consulta maestra de viajes operativos en vivo
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

    # Cargar flota, conductores y contratos vigentes
    cur.execute("SELECT placa, clase AS tipo FROM vehiculos_especial WHERE id_empresa = %s OR id_empresa = %s", (empresa_id, empresa_nit))
    vehiculos = cur.fetchall()

    cur.execute("SELECT id, nombre, cedula, telegram_id FROM usuarios WHERE (empresa_id = %s OR empresa_id = %s) AND perfil IN ('operador_flotaespecial', 'auxiliar_transporte_especial')", (empresa_id, empresa_nit))
    cond_usrs = list(cur.fetchall())

    cur.execute("SELECT id, nombre, cedula, vencimiento_licencia_conduccion FROM conductores_flotaespecial WHERE id_empresa = %s OR id_empresa = %s", (empresa_id, empresa_nit))
    cond_flota = list(cur.fetchall())

    cond_dict = {c['cedula']: c for c in cond_usrs}
    for c in cond_flota:
        if c['cedula'] not in cond_dict:
            cond_dict[c['cedula']] = c
    conductores = list(cond_dict.values())

    cur.execute("SELECT id, contratante_nombre, numero_contrato, categoria_contrato FROM contratos_transporte_especial WHERE (id_empresa = %s OR id_empresa = %s) AND estado = 'ACTIVO'", (empresa_id, empresa_nit))
    contratos = cur.fetchall()

    cur.close()

    now_col = datetime.now(BOGOTA_TZ).replace(tzinfo=None)
    for v in viajes:
        h_init = v.get('hora_inicio')
        f_serv = v.get('fecha_servicio')
        v['is_urgencia'] = False
        v['is_36h'] = False
        
        if h_init is not None and f_serv is not None:
            try:
                if isinstance(h_init, timedelta):
                    dt_val = datetime.combine(f_serv, datetime.min.time()) + h_init
                    h_str = (datetime.min + h_init).time().strftime('%H:%M:%S')
                else:
                    dt_val = datetime.combine(f_serv, h_init)
                    h_str = h_init.strftime('%H:%M:%S')
                    
                diff_hours = (dt_val - now_col).total_seconds() / 3600.0
                
                if diff_hours <= 36.0:
                    v['is_36h'] = True
                if diff_hours <= 12.0:
                    v['is_urgencia'] = True
                    
                v['hora_inicio_str'] = f"{f_serv} | {h_str}"
            except:
                v['hora_inicio_str'] = f"{f_serv} | {h_init}"
                v['is_36h'] = True
        else:
            v['hora_inicio_str'] = f"{f_serv} | Pendiente" if f_serv else "Pendiente"
            v['is_36h'] = True
            
        v['hora_inicio'] = v['hora_inicio_str']
        v['trayecto'] = (v.get('trayecto') or 'IDA').upper()
        v['estatus_servicio'] = (v.get('estatus_servicio') or '').upper()

    # CLASIFICACIÓN ESTRICTA DE COLUMNAS KANBAN (Con corrección de visibilidad para novedades)
    parte1_ida_programados = [v for v in viajes if v['trayecto'] == 'IDA' and ((v['estatus_servicio'] == 'PROGRAMADO' and v['is_36h']) or v['estatus_servicio'] == 'NOVEDAD_PRE_VIAJE')]
    parte2_col1 = [v for v in viajes if v['trayecto'] == 'IDA' and v['estatus_servicio'] in ('EN EJECUCION', 'NOVEDAD_RECORRIDO')]
    parte2_col2 = [v for v in viajes if v['trayecto'] == 'IDA' and v['estatus_servicio'] in ('TERMINADO-PDTE AUDITAR', 'AUDITADO')]
    parte2_col3 = [v for v in viajes if v['trayecto'] == 'VUELTA' and v.get('vuelta_activada') and v['estatus_servicio'] not in ('EN EJECUCION', 'NOVEDAD_RECORRIDO', 'TERMINADO-PDTE AUDITAR', 'AUDITADO')]
    parte2_col4 = [v for v in viajes if v['trayecto'] == 'VUELTA' and v['estatus_servicio'] in ('EN EJECUCION', 'NOVEDAD_RECORRIDO')]
    parte2_col5 = [v for v in viajes if v['trayecto'] == 'VUELTA' and v['estatus_servicio'] in ('TERMINADO-PDTE AUDITAR', 'AUDITADO')]

    return render_template(
        'B_dashboard_operativo_eps.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='dashboard',
        kpis=kpis,
        filtros={},
        parte1_ida_programados=parte1_ida_programados,
        parte2_col1=parte2_col1,
        parte2_col2=parte2_col2,
        parte2_col3=parte2_col3,
        parte2_col4=parte2_col4,
        parte2_col5=parte2_col5,
        vehiculos=vehiculos,
        conductores=conductores,
        contratos=contratos
    )

# =========================================================
# 2.1 MOTOR DE DATOS EN VIVO (SHORT-POLLING DEL TABLERO)
# =========================================================
@bp_controlador_flotaespecial.route('/api/operativa/vivo', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def api_operativa_vivo():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT c.id, c.id_viaje, c.numero_autorizacion, c.numero_prescripcion, c.nombre_usuario, c.id_usuario, 
                   c.direccion_origen, c.direccion_destino, c.departamento, c.municipio, c.departamento_destino, c.municipio_destino,
                   c.telefono_usuario, c.hora_inicio, c.fecha_servicio, c.trayecto, c.estatus_servicio, c.vehiculo_asignado, 
                   c.conductor_asignado, c.id_viaje_padre, c.estado_novedad, c.descripcion_novedad, c.tipo_documento, c.vuelta_activada,
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
            "novedades_activas": [],
            "parte1_ida_programados": [],
            "parte2_col1": [],
            "parte2_col2": [],
            "parte2_col3": [],
            "parte2_col4": [],
            "parte2_col5": []
        }

        now_col = datetime.now(BOGOTA_TZ).replace(tzinfo=None)

        for v in viajes:
            h_init = v.get('hora_inicio')
            f_serv = v.get('fecha_servicio')
            v['is_urgencia'] = False
            v['is_36h'] = False
            
            if h_init is not None and f_serv is not None:
                try:
                    if isinstance(h_init, timedelta):
                        dt_val = datetime.combine(f_serv, datetime.min.time()) + h_init
                        h_str = (datetime.min + h_init).time().strftime('%H:%M:%S')
                    else:
                        dt_val = datetime.combine(f_serv, h_init)
                        h_str = h_init.strftime('%H:%M:%S')
                        
                    diff_hours = (dt_val - now_col).total_seconds() / 3600.0
                    
                    if diff_hours <= 36.0:
                        v['is_36h'] = True
                    if diff_hours <= 12.0:
                        v['is_urgencia'] = True
                        
                    v['hora_inicio_str'] = f"{f_serv} | {h_str}"
                except:
                    v['hora_inicio_str'] = f"{f_serv} | {h_init}"
                    v['is_36h'] = True
            else:
                v['hora_inicio_str'] = f"{f_serv} | Pendiente" if f_serv else "Pendiente"
                v['is_36h'] = True
                
            v['hora_inicio'] = v['hora_inicio_str']
            trayecto = (v.get('trayecto') or 'IDA').upper()
            estatus = (v.get('estatus_servicio') or '').upper()
            
            v['trayecto'] = trayecto
            v['estatus_servicio'] = estatus

            if estatus in ['NOVEDAD_PRE_VIAJE', 'NOVEDAD_RECORRIDO']:
                datos["novedades_activas"].append(v)
            
            # CLASIFICACIÓN ESTRICTA DE COLUMNAS KANBAN (Con corrección de visibilidad para novedades)
            if trayecto == 'IDA':
                if (estatus == 'PROGRAMADO' and v['is_36h']) or estatus == 'NOVEDAD_PRE_VIAJE':
                    datos["parte1_ida_programados"].append(v)
                elif estatus in ['EN EJECUCION', 'NOVEDAD_RECORRIDO']:
                    datos["parte2_col1"].append(v)
                elif estatus in ['TERMINADO-PDTE AUDITAR', 'AUDITADO']:
                    datos["parte2_col2"].append(v)
                    
            elif trayecto == 'VUELTA':
                if v.get('vuelta_activada') and estatus not in ['EN EJECUCION', 'NOVEDAD_RECORRIDO', 'TERMINADO-PDTE AUDITAR', 'AUDITADO']:
                    datos["parte2_col3"].append(v)
                elif estatus in ['EN EJECUCION', 'NOVEDAD_RECORRIDO']:
                    datos["parte2_col4"].append(v)
                elif estatus in ['TERMINADO-PDTE AUDITAR', 'AUDITADO']:
                    datos["parte2_col5"].append(v)

        return jsonify({"status": "success", "data": datos}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# 2.2 OCULTAR VIAJES Y DESENCADENAR VUELTA
# =========================================================
@bp_controlador_flotaespecial.route('/api/operativa/ocultar_kanban', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
def api_ocultar_kanban():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    viaje_id = datos.get('viaje_id')

    if not viaje_id:
        return jsonify({"status": "error", "message": "ID de viaje no proporcionado"}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN oculto_kanban BOOLEAN DEFAULT FALSE")
        except: pass
        try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN vuelta_activada BOOLEAN DEFAULT FALSE")
        except: pass

        cur.execute("SELECT trayecto, id_viaje_padre FROM control_viajes_flota_especial WHERE id = %s AND id_empresa = %s", (viaje_id, empresa_id))
        viaje_actual = cur.fetchone()
        
        if viaje_actual:
            # 1. Ocultar el viaje en el que se hizo clic
            cur.execute("""
                UPDATE control_viajes_flota_especial 
                SET oculto_kanban = TRUE
                WHERE id = %s AND id_empresa = %s
            """, (viaje_id, empresa_id))
            
            # 2. Activar la vuelta si el que se ocultó fue IDA
            trayecto = (viaje_actual.get('trayecto') or 'IDA').upper()
            id_padre = viaje_actual.get('id_viaje_padre')
            viaje_vuelta = None
            
            if trayecto == 'IDA' and id_padre:
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET vuelta_activada = TRUE 
                    WHERE id_viaje_padre = %s AND trayecto = 'VUELTA' AND id_empresa = %s
                """, (id_padre, empresa_id))
                
                # Obtener datos de la vuelta para auto-lanzar los modales
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
# 2.3 RE-NOTIFICACIÓN MANUAL AL OPERADOR
# =========================================================
@bp_controlador_flotaespecial.route('/api/operativa/notificar', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
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
        
        from app.blueprints.B_bp_flotaespecial_eps import _enviar_documento_telegram_hilo, _enviar_mensajes_telegram_hilo
        
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
# 3. GESTIÓN DE OPERADORES
# =========================================================
@bp_controlador_flotaespecial.route('/operadores', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_operadores():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'crear':
            nombre = request.form.get('nombre', '').strip()
            cedula = request.form.get('cedula', '').strip()
            perfil = request.form.get('perfil', '').strip()
            
            if perfil == 'operador_flotaespecial':
                password = request.form.get('password', '').strip()
                if not password:
                    flash("El operador requiere una contraseña de acceso.", "danger")
                    return redirect(url_for('controlador_flotaespecial.gestion_operadores'))
                hashed_pw = bcrypt.generate_password_hash(password).decode('utf-8')
            else:
                hashed_pw = bcrypt.generate_password_hash(os.urandom(12).hex()).decode('utf-8')

            if nombre and cedula and perfil:
                cur = mysql.connection.cursor()
                try:
                    cur.execute("SELECT id FROM usuarios WHERE cedula = %s AND empresa_id = %s", (cedula, empresa_id))
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
                    if perfil == 'operador_flotaespecial':
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

        return redirect(url_for('controlador_flotaespecial.gestion_operadores'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT id, nombre, cedula, perfil 
        FROM usuarios 
        WHERE empresa_id = %s AND perfil IN ('operador_flotaespecial', 'auxiliar_transporte_especial')
        ORDER BY nombre ASC
    """, (empresa_id,))
    operadores_db = cur.fetchall()
    cur.close()

    return render_template(
        'B_dashboard_operativo_eps.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='operadores', 
        operadores=operadores_db
    )