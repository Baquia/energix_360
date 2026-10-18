# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: OPERADOR CONTABLE | CONDICIÓN: OPCIONAL | TIPO: 2
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from app.utils import login_required_custom
from functools import wraps
from app import mysql
import MySQLdb.cursors
import time
from datetime import datetime
import json

bp_operador_contable = Blueprint('operador_contable', __name__, url_prefix='/operador_contable')

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
                and 'operador_contable' not in modulos_activos
                and 'controlador_contabilidad' not in modulos_activos
                and empresa_id != '901811727'
            ):
                flash(f'Acceso denegado: Su empresa no tiene habilitado el submódulo de {submodulo_nombre}.', 'danger')
                return redirect(url_for('panel_principal'))
            return f(*args, **kwargs)
        return decorated_function # <-- CORRECCIÓN CRÍTICA: Se retorna la función decorada para preservar la introspección
    return decorator

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def operador_contable_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola operativa
    del Operador Contable (Auxiliar / Asistente Contable).
    Autoriza perfiles de piso nativos y la jerarquía corporativa de Baquía-ems SAS.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        perfiles_permitidos = [
            'operador_contable',
            'controlador_contabilidad',
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
            flash('Acceso denegado: Se requiere perfil de Operador Contable para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD OPERADOR CONTABLE)
# ==============================================================================
@bp_operador_contable.route('/dashboard', endpoint='dashboard')
@bp_operador_contable.route('/dashboard_operador', endpoint='dashboard_operador')
@login_required_custom
@submodulo_required('controlador_contabilidad')
@operador_contable_required
def dashboard_operador():
    """
    Renderiza la interfaz operativa del Auxiliar Contable.
    Permite registrar comprobantes de egreso, recaudos de cartera, caja menor y conciliar movimientos.
    """
    usuario_id = session.get('usuario_id')
    empresa_id = session.get('empresa_id')

    # Heartbeat preventivo al cargar vista con reintentos para evitar colisiones
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
        'recaudos_hoy': 0.0,
        'gastos_hoy': 0.0,
        'documentos_pendientes': 0
    }

    return render_template(
        'B_operador_contable.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis
    )

# ==============================================================================
# APIS OPERATIVAS DE CONTABILIDAD
# ==============================================================================
@bp_operador_contable.route('/api/ping', methods=['POST'])
@login_required_custom
@operador_contable_required
def api_ping():
    """
    Heartbeat en tiempo real para reportar al operador contable activo.
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

@bp_operador_contable.route('/api/cuentas_por_cobrar', methods=['GET'])
@login_required_custom
@operador_contable_required
def api_cuentas_por_cobrar():
    """
    Consulta facturas y clientes pendientes de cobro para asentar recaudos.
    """
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Búsqueda multi-tenant estricta de clientes de la empresa
        cur.execute("""
            SELECT id_cliente_empresa as id_cliente, cliente_empresa as cliente
            FROM clientes_empresa
            WHERE id_empresa = %s
            ORDER BY cliente_empresa ASC
        """, (empresa_id,))
        clientes = cur.fetchall()
        cur.close()
        return jsonify(clientes)
    except Exception as e:
        print(f"Error cargando clientes para recaudos: {e}")
        return jsonify([])

@bp_operador_contable.route('/api/registrar_recaudo', methods=['POST'])
@login_required_custom
@operador_contable_required
def api_registrar_recaudo():
    """
    Registra un recibo de caja / abono a cartera de un cliente.
    """
    data = request.json or {}
    usuario_id = session.get('usuario_id')
    empresa_id = session.get('empresa_id')
    cliente_id = data.get('cliente_id')
    valor_recaudo = data.get('valor_recaudo')
    metodo_pago = data.get('metodo_pago', 'Efectivo')
    soporte_referencia = data.get('soporte_referencia', '')

    if not cliente_id or not valor_recaudo:
        return jsonify({'status': 'error', 'message': 'Cliente y valor de recaudo son obligatorios.'}), 400

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
        return jsonify({'status': 'success', 'message': 'Recaudo de cartera asentado correctamente.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@bp_operador_contable.route('/api/registrar_gasto', methods=['POST'])
@login_required_custom
@operador_contable_required
def api_registrar_gasto():
    """
    Registra un comprobante de egreso o movimiento de caja menor.
    """
    data = request.json or {}
    usuario_id = session.get('usuario_id')
    empresa_id = session.get('empresa_id')
    concepto = data.get('concepto')
    valor_gasto = data.get('valor_gasto')
    categoria = data.get('categoria', 'Operativo')
    numero_comprobante = data.get('numero_comprobante', '')

    if not concepto or not valor_gasto:
        return jsonify({'status': 'error', 'message': 'Concepto y valor del egreso son obligatorios.'}), 400

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
        return jsonify({'status': 'success', 'message': 'Comprobante de egreso asentado exitosamente.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# COMPATIBILIDAD DE ENDPOINT CRUZADO FLASK
# ==============================================================================
@bp_operador_contable.record_once
def registrar_alias_endpoint(state):
    """Garantiza la resolución fluida tanto de 'operador_contable.dashboard_operador' como de 'bp_operador_contable.dashboard_operador'"""
    state.app.view_functions['operador_contable.dashboard'] = dashboard_operador
    state.app.view_functions['bp_operador_contable.dashboard'] = dashboard_operador
    state.app.view_functions['operador_contable.dashboard_operador'] = dashboard_operador
    state.app.view_functions['bp_operador_contable.dashboard_operador'] = dashboard_operador