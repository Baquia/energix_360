# app/blueprints/B_bp_controlador_flotaespecial.py
import os
import hashlib
from datetime import datetime, timedelta
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, jsonify
from app import mysql, bcrypt
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

bp_controlador_flotaespecial = Blueprint('controlador_flotaespecial', __name__, url_prefix='/gestor_flotaespecial')

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
# 1. SUB-MENÚ INTERMEDIO DE SELECCIÓN (EL ENRUTADOR CAE AQUÍ)
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
# 2. DASHBOARD DE KPIS OPERATIVOS (ESTÁTICO/HISTÓRICO)
# =========================================================
@bp_controlador_flotaespecial.route('/operativa')
@login_required_custom
@controlador_flotaespecial_required
def dashboard_operativo():
    empresa_id = session.get('empresa_id')
    
    hoy = datetime.now()
    inicio_mes = hoy.replace(day=1).strftime('%Y-%m-%d')
    fin_mes = hoy.strftime('%Y-%m-%d')
    
    fecha_inicio = request.args.get('fecha_inicio', inicio_mes)
    fecha_fin = request.args.get('fecha_fin', fin_mes)
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN estatus_servicio = 'CAPTURADO' THEN 1 ELSE 0 END) as capturados,
            SUM(CASE WHEN estatus_servicio = 'PROGRAMADO' THEN 1 ELSE 0 END) as programados,
            SUM(CASE WHEN estatus_servicio = 'VERIFICADO' THEN 1 ELSE 0 END) as verificados,
            SUM(CASE WHEN estatus_servicio = 'ASIGNADO' THEN 1 ELSE 0 END) as asignados,
            SUM(CASE WHEN estatus_servicio = 'EN EJECUCION' THEN 1 ELSE 0 END) as ejecucion,
            SUM(CASE WHEN estatus_servicio IN ('TERMINADO-PDTE AUDITAR', 'AUDITADO') THEN 1 ELSE 0 END) as ejecutados
        FROM control_viajes_flota_especial 
        WHERE id_empresa = %s AND (fecha_servicio BETWEEN %s AND %s OR fecha_servicio IS NULL)
    """, (empresa_id, fecha_inicio, fecha_fin))
    kpis = cur.fetchone()
    
    if not kpis or kpis['total'] is None:
        kpis = {'total': 0, 'capturados': 0, 'programados': 0, 'verificados': 0, 'asignados': 0, 'ejecucion': 0, 'ejecutados': 0}

    cur.execute("""
        SELECT c.id_viaje, c.fecha_servicio, c.hora_inicio, c.vehiculo_asignado, c.conductor_asignado, 
               c.nombre_usuario, c.telefono_usuario, c.direccion_origen, c.direccion_destino, c.estatus_servicio,
               c.numero_prescripcion, c.id_eps_cliente AS ips, c.trayecto,
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
        WHERE c.id_empresa = %s AND (c.fecha_servicio BETWEEN %s AND %s OR c.fecha_servicio IS NULL)
        ORDER BY c.fecha_servicio ASC, c.hora_inicio ASC
    """, (empresa_id, fecha_inicio, fecha_fin))
    viajes = cur.fetchall()
    cur.close()

    viajes_capturados = [v for v in viajes if v['estatus_servicio'] == 'CAPTURADO']
    viajes_programados = [v for v in viajes if v['estatus_servicio'] == 'PROGRAMADO']
    viajes_verificados = [v for v in viajes if v['estatus_servicio'] == 'VERIFICADO']
    viajes_asignados = [v for v in viajes if v['estatus_servicio'] == 'ASIGNADO']
    viajes_ejecucion = [v for v in viajes if v['estatus_servicio'] == 'EN EJECUCION']
    viajes_ejecutados = [v for v in viajes if v['estatus_servicio'] in ('TERMINADO-PDTE AUDITAR', 'AUDITADO')]

    return render_template(
        'B_dashboard_operativo_eps.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='dashboard',
        kpis=kpis,
        filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin},
        viajes_capturados=viajes_capturados,
        viajes_programados=viajes_programados,
        viajes_verificados=viajes_verificados,
        viajes_asignados=viajes_asignados,
        viajes_ejecucion=viajes_ejecucion,
        viajes_ejecutados=viajes_ejecutados
    )

# =========================================================
# 2.1 MOTOR DE DATOS EN VIVO (SHORT-POLLING KANBAN + LISTA)
# =========================================================
@bp_controlador_flotaespecial.route('/api/operativa/vivo', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def api_operativa_vivo():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        # Asegurar existencia de la columna de ocultamiento visual
        try:
            cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN oculto_kanban BOOLEAN DEFAULT FALSE")
        except:
            pass

        # REGLA ACTUALIZADA: Se excluyen los viajes que el controlador ha marcado como ocultos (oculto_kanban = TRUE)
        cur.execute("""
            SELECT id, id_viaje, numero_autorizacion, numero_prescripcion, nombre_usuario, id_usuario, 
                   direccion_origen, direccion_destino, telefono_usuario,
                   hora_inicio, fecha_servicio, trayecto, estatus_servicio, vehiculo_asignado, conductor_asignado, id_viaje_padre
            FROM control_viajes_flota_especial 
            WHERE id_empresa = %s 
              AND estatus_servicio IN ('PROGRAMADO', 'PDTE. ASIGNAR VUELTA', 'ASIGNADO', 'EN EJECUCION', 'TERMINADO-PDTE AUDITAR')
              AND (oculto_kanban = FALSE OR oculto_kanban IS NULL)
            ORDER BY fecha_servicio ASC, hora_inicio ASC
        """, (empresa_id,))
        viajes = cur.fetchall()

        datos = {
            "verificacion_previaje": [], # 0. Lista Superior Requerida
            "ida_en_progreso": [],       # 1. Columna Kanban
            "ida_terminadas": [],        # 2. Columna Kanban
            "vuelta_pendientes": [],     # 3. Columna Kanban
            "vuelta_en_progreso": [],    # 4. Columna Kanban
            "vuelta_terminadas": []      # 5. Columna Kanban
        }

        for v in viajes:
            h_init = v.get('hora_inicio')
            f_serv = v.get('fecha_servicio')
            if h_init is not None and f_serv is not None:
                v['hora_inicio'] = f"{f_serv} | {h_init}"
            else:
                v['hora_inicio'] = f"{f_serv} | Pendiente" if f_serv else "Pendiente"
                
            trayecto = (v.get('trayecto') or 'IDA').upper()
            estatus = (v.get('estatus_servicio') or '').upper()
            
            v['trayecto'] = trayecto
            v['estatus_servicio'] = estatus

            if trayecto == 'IDA':
                if estatus == 'PROGRAMADO':
                    datos["verificacion_previaje"].append(v)
                elif estatus in ['ASIGNADO', 'EN EJECUCION']:
                    datos["ida_en_progreso"].append(v)
                elif estatus == 'TERMINADO-PDTE AUDITAR':
                    datos["ida_terminadas"].append(v)
                    
            elif trayecto == 'VUELTA':
                if estatus == 'PDTE. ASIGNAR VUELTA':
                    datos["vuelta_pendientes"].append(v)
                elif estatus in ['ASIGNADO', 'EN EJECUCION']:
                    datos["vuelta_en_progreso"].append(v)
                elif estatus == 'TERMINADO-PDTE AUDITAR':
                    datos["vuelta_terminadas"].append(v)

        return jsonify({"status": "success", "data": datos}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# 2.2 MOTOR PARA OCULTAR TARJETAS TERMINADAS DEL KANBAN
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

    cur = mysql.connection.cursor()
    try:
        try:
            cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN oculto_kanban BOOLEAN DEFAULT FALSE")
        except:
            pass

        cur.execute("""
            UPDATE control_viajes_flota_especial 
            SET oculto_kanban = TRUE
            WHERE id = %s AND id_empresa = %s
        """, (viaje_id, empresa_id))
        
        mysql.connection.commit()
        return jsonify({"status": "success", "message": "Viaje retirado del tablero operativo."}), 200

    except Exception as e:
        mysql.connection.rollback()
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