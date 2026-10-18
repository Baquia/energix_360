# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR CONTABILIDAD | CONDICIÓN: OPCIONAL | TIPO: 2
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps
import json

bp_controlador_contabilidad = Blueprint('controlador_contabilidad', __name__, url_prefix='/controlador_contabilidad')

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

            # Validación: inquilino corporativo Baquía o empresas con el módulo/submódulo activo
            if (
                submodulo_nombre not in submodulos_activos
                and submodulo_nombre not in modulos_activos
                and 'controlador_contabilidad' not in modulos_activos
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
def controlador_contabilidad_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Contabilidad.
    Autoriza perfiles operativos nativos de clientes M3 y la jerarquía corporativa de Baquía-ems SAS.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        perfiles_permitidos = [
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
            flash('Acceso denegado: Se requiere perfil contable o administrativo para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD CONTABLE)
# ==============================================================================
@bp_controlador_contabilidad.route('/dashboard', endpoint='dashboard')
@login_required_custom
@submodulo_required('controlador_contabilidad')
@controlador_contabilidad_required
def dashboard():
    """
    Renderiza el tablero de control principal para la gestión contable.
    Aquí se consolida el control de ingresos, egresos, cuentas por cobrar y pagar.
    """
    empresa_id = session.get('empresa_id')
    
    # KPIs proyectados para el frontend
    kpis = {
        'ingresos_mes': 0.0,
        'egresos_mes': 0.0,
        'cuentas_cobrar': 0.0,
        'cuentas_pagar': 0.0
    }
    
    # Listas vacías simuladas para la vista
    alertas_contables = []
    
    return render_template(
        'B_controlador_contabilidad.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis,
        alertas=alertas_contables
    )

# ==============================================================================
# COMPATIBILIDAD DE ENDPOINT CRUZADO FLASK
# ==============================================================================
@bp_controlador_contabilidad.record_once
def registrar_alias_endpoint(state):
    """Garantiza la resolución fluida tanto de 'controlador_contabilidad.dashboard' como de 'bp_controlador_contabilidad.dashboard'"""
    state.app.view_functions['controlador_contabilidad.dashboard'] = dashboard
    state.app.view_functions['bp_controlador_contabilidad.dashboard'] = dashboard