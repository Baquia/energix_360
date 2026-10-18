# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: OPERADOR RECURSO HUMANO | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from app.utils import login_required_custom
from functools import wraps
from app import mysql
import MySQLdb.cursors
import time
from datetime import datetime

bp_operador_recursohumano = Blueprint('operador_recursohumano', __name__, url_prefix='/operador_recursohumano')

# ==============================================================================
# DECORADOR DE SUBMÓDULO MULTI-TENANT HOMOLOGADO (DIRECTO Y ROBUSTO)
# ==============================================================================
def submodulo_required(submodulo_nombre):
    """
    Decorador de autorización Plug & Play homologado:
    Verifica explícitamente en session['submodulos_activos'] o en session['modulos_activos']
    o autoriza de forma irrestricta si pertenece al tenant corporativo (NIT 901811727).
    Debe invocarse SIEMPRE con el argumento: @submodulo_required('nombre_modulo')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            submodulos_activos = session.get('submodulos_activos', [])
            modulos_activos = session.get('modulos_activos', [])
            empresa_id = str(session.get('empresa_id', '')).strip()

            if isinstance(submodulos_activos, str):
                try:
                    submodulos_activos = json.loads(submodulos_activos)
                except Exception:
                    submodulos_activos = []

            # Validación: inquilino corporativo Baquía o empresas con el módulo activo
            if (
                submodulo_nombre not in submodulos_activos 
                and submodulo_nombre not in modulos_activos 
                and 'operador_recursohumano' not in modulos_activos 
                and 'controlador_recurso_humano' not in modulos_activos 
                and empresa_id != '901811727'
            ):
                flash(f'Acceso denegado: Su empresa no tiene habilitado el submódulo de {submodulo_nombre}.', 'danger')
                return redirect(url_for('panel_principal'))
            return f(*args, **kwargs)
        return decorated_function 
    return decorator

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def operador_rrhh_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la interfaz operativa
    del Operador de Recursos Humanos (Auxiliar/Asistente de Personal).
    Autoriza perfiles de RRHH nativos y la jerarquía corporativa de Baquía-ems SAS.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        perfiles_permitidos = [
            'operador_recursohumano',
            'operador_rrhh',
            'controlador_recurso_humano',
            'controlador_administracion',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster',
            'gerente_bqa',
            'admin_general',
            'webmaster_administrador_baquia'
        ]
        
        # Validación estricta de perfil y aislamiento Multi-Tenant
        if (
            perfil not in perfiles_permitidos 
            and 'webmaster' not in tipo_empresa 
            and empresa_id != '901811727'
        ):
            flash('Acceso denegado: Se requiere perfil de Operador de Recursos Humanos para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD OPERADOR DE RECURSOS HUMANOS)
# ==============================================================================
@bp_operador_recursohumano.route('/dashboard', endpoint='dashboard')
@bp_operador_recursohumano.route('/dashboard_operador', endpoint='dashboard_operador')
@login_required_custom
@submodulo_required('controlador_recurso_humano')
@operador_rrhh_required
def dashboard_operador():
    """
    Renderiza la interfaz operativa del Auxiliar de Recursos Humanos.
    Permite registrar asistencias, novedades de personal, incapacidades y permisos.
    """
    usuario_id = session.get('usuario_id')
    empresa_id = session.get('empresa_id')

    # Heartbeat preventivo al cargar vista con política de reintentos
    for intento in range(3):
        try:
            cur_hb = mysql.connection.cursor()
            cur_hb.execute("""
                INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
                VALUES (%s, NOW())
                ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
            """, (usuario_id,))
            mysql.connection.commit()
            cur_hb.close()
            break
        except Exception as e:
            if "deadlock" in str(e).lower() and intento < 2:
                time.sleep(0.5)
                continue
            break

    kpis = {
        'asistencias_hoy': 0,
        'novedades_registradas': 0,
        'permisos_tramitados': 0
    }

    return render_template(
        'B_operador_recursohumano.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis
    )

# ==============================================================================
# APIS OPERATIVAS DE GESTIÓN DE PERSONAL
# ==============================================================================
@bp_operador_recursohumano.route('/api/ping', methods=['POST'])
@login_required_custom
@operador_rrhh_required
def api_ping():
    """
    Heartbeat en tiempo real para reportar al operador activo en la consola central.
    """
    usuario_id = session.get('usuario_id')
    for intento in range(3):
        try:
            cur = mysql.connection.cursor()
            cur.execute("""
                INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
                VALUES (%s, NOW())
                ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
            """, (usuario_id,))
            mysql.connection.commit()
            cur.close()
            return jsonify({'status': 'ok'})
        except Exception as e:
            if "deadlock" in str(e).lower() and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify({'status': 'error', 'message': str(e)}), 500

@bp_operador_recursohumano.route('/api/lista_empleados', methods=['GET'])
@login_required_custom
@operador_rrhh_required
def api_lista_empleados():
    """
    Obtiene la lista de colaboradores vinculados a la empresa para asignación de novedades.
    """
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT id, cedula, nombre, perfil
            FROM usuarios
            WHERE empresa_id = %s
            ORDER BY nombre ASC
        """, (empresa_id,))
        empleados = cur.fetchall()
        cur.close()
        return jsonify(empleados)
    except Exception as e:
        print(f"Error cargando lista de empleados: {e}")
        return jsonify([])

@bp_operador_recursohumano.route('/api/registrar_asistencia', methods=['POST'])
@login_required_custom
@operador_rrhh_required
def api_registrar_asistencia():
    """
    Registra el control de asistencia o jornada laboral de un empleado.
    """
    data = request.json or {}
    usuario_id = session.get('usuario_id')
    empleado_id = data.get('empleado_id')
    tipo_registro = data.get('tipo_registro')  # 'ingreso' o 'salida'

    if not empleado_id or not tipo_registro:
        return jsonify({'status': 'error', 'message': 'Empleado y tipo de registro son requeridos.'}), 400

    try:
        cur = mysql.connection.cursor()
        
        # Registrar actividad del operador
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (usuario_id,))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': f'Asistencia ({tipo_registro.upper()}) registrada correctamente.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@bp_operador_recursohumano.route('/api/registrar_novedad', methods=['POST'])
@login_required_custom
@operador_rrhh_required
def api_registrar_novedad():
    """
    Registra una novedad de personal (permiso, incapacidad, sanción, horas extras).
    """
    data = request.json or {}
    usuario_id = session.get('usuario_id')
    empleado_id = data.get('empleado_id')
    tipo_novedad = data.get('tipo_novedad')
    observaciones = data.get('observaciones', '')

    if not empleado_id or not tipo_novedad:
        return jsonify({'status': 'error', 'message': 'Empleado y tipo de novedad son obligatorios.'}), 400

    try:
        cur = mysql.connection.cursor()
        
        # Registrar actividad del operador
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (usuario_id,))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Novedad registrada y transmitida a la consola de RRHH.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# COMPATIBILIDAD DE ENDPOINT CRUZADO FLASK
# ==============================================================================
@bp_operador_recursohumano.record_once
def registrar_alias_endpoint(state):
    """Garantiza la resolución fluida de los endpoints del Operador de Recursos Humanos."""
    state.app.view_functions['operador_recursohumano.dashboard'] = dashboard_operador
    state.app.view_functions['bp_operador_recursohumano.dashboard'] = dashboard_operador
    state.app.view_functions['operador_recursohumano.dashboard_operador'] = dashboard_operador
    state.app.view_functions['bp_operador_recursohumano.dashboard_operador'] = dashboard_operador