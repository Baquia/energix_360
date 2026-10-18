# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR VENTAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from app.utils import login_required_custom
from functools import wraps
from app import mysql
import MySQLdb.cursors
import json

bp_controlador_ventas = Blueprint('controlador_ventas', __name__, url_prefix='/controlador_ventas')

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
                and 'controlador_ventas' not in modulos_activos 
                and 'ventas' not in modulos_activos 
                and empresa_id != '901811727'
            ):
                flash(f'Acceso denegado: Su empresa no tiene habilitado el submódulo de {submodulo_nombre}.', 'danger')
                return redirect(url_for('panel_principal'))
            return f(*args, **kwargs)
        return decorated_function # <-- CORRECCIÓN CRÍTICA: Retorna la función preservando la introspección
    return decorator

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def controlador_ventas_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Ventas.
    Autoriza perfiles operativos comerciales y la jerarquía corporativa de Baquía-ems SAS.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        perfiles_permitidos = [
            'controlador_ventas',
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
            flash('Acceso denegado: Se requiere perfil de Controlador de Ventas para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD CONTROLADOR COMERCIAL)
# ==============================================================================
@bp_controlador_ventas.route('/dashboard', endpoint='dashboard')
@login_required_custom
@submodulo_required('controlador_ventas')
@controlador_ventas_required
def dashboard():
    """
    Renderiza el tablero de control principal para la gestión de ventas.
    Aquí se consolidan KPIs comerciales, seguimiento de operadores en campo y autorización de pedidos.
    """
    empresa_id = session.get('empresa_id')
    
    # Estructura base de KPIs para el frontend
    kpis = {
        'pedidos_hoy': 0,
        'ventas_totales_hoy': 0.0,
        'vendedores_activos': 0,
        'pedidos_pendientes_autorizacion': 0
    }
    
    operadores_ventas = []
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # 1. Contar vendedores activos (conectados recientemente o registrados)
        cur.execute("""
            SELECT id, nombre, cedula 
            FROM usuarios 
            WHERE empresa_id = %s AND perfil = 'operador_ventas'
        """, (empresa_id,))
        operadores_db = cur.fetchall()
        kpis['vendedores_activos'] = len(operadores_db)
        
        for op in operadores_db:
            op_dict = dict(op)
            op_dict['en_linea'] = 0 
            op_dict['pedidos_capturados'] = 0
            op_dict['monto_capturado'] = 0.0
            operadores_ventas.append(op_dict)
            
        cur.close()
    except Exception as e:
        print(f"Error cargando dashboard de ventas: {e}")

    return render_template(
        'B_controlador_ventas.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis,
        operadores=operadores_ventas
    )

# ==============================================================================
# APIS DE MONITOREO Y GESTIÓN EN TIEMPO REAL
# ==============================================================================
@bp_controlador_ventas.route('/api/monitoreo_vendedores', endpoint='api_monitoreo_vendedores')
@login_required_custom
@controlador_ventas_required
def api_monitoreo_vendedores():
    """Endpoint para alimentar el mapa o la lista de vendedores en campo en tiempo real."""
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                u.id, 
                u.nombre, 
                IF(TIMESTAMPDIFF(SECOND, ma.ultima_actividad, NOW()) <= 60, 1, 0) as en_linea
            FROM usuarios u
            LEFT JOIN monitoreo_actividad ma ON u.id = ma.id_usuario
            WHERE u.empresa_id = %s AND u.perfil = 'operador_ventas'
            ORDER BY en_linea DESC, u.nombre ASC
        """, (empresa_id,))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        print(f"Error API monitoreo vendedores: {e}")
        return jsonify([])

# ==============================================================================
# COMPATIBILIDAD DE ENDPOINT CRUZADO FLASK
# ==============================================================================
@bp_controlador_ventas.record_once
def registrar_alias_endpoint(state):
    """Garantiza la resolución fluida de los endpoints del Controlador de Ventas."""
    state.app.view_functions['controlador_ventas.dashboard'] = dashboard
    state.app.view_functions['bp_controlador_ventas.dashboard'] = dashboard