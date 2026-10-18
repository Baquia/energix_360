# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR RECURSO HUMANO | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps
import json

bp_controlador_recurso_humano = Blueprint('controlador_recurso_humano', __name__, url_prefix='/controlador_recurso_humano')

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
                and 'controlador_recurso_humano' not in modulos_activos 
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
def controlador_rrhh_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Recursos Humanos.
    Autoriza perfiles operativos de clientes M3 y la jerarquía corporativa de Baquía-ems SAS.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        perfiles_permitidos = [
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
            flash('Acceso denegado: Se requiere perfil de Recursos Humanos para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD RRHH)
# ==============================================================================
@bp_controlador_recurso_humano.route('/dashboard', endpoint='dashboard')
@login_required_custom
@submodulo_required('controlador_recurso_humano')
@controlador_rrhh_required
def dashboard():
    """
    Renderiza el tablero de control principal para la gestión de Recursos Humanos.
    Aquí se consolida el control de asistencia, nómina y evaluación de personal.
    """
    empresa_id = session.get('empresa_id')
    
    # KPIs proyectados para el frontend
    kpis = {
        'empleados_activos': 0,
        'novedades_nomina': 0,
        'contratos_por_vencer': 0,
        'ausentismos_hoy': 0
    }
    
    # Listas vacías simuladas para la vista
    alertas_rrhh = []
    
    return render_template(
        'B_controlador_recurso_humano.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis,
        alertas=alertas_rrhh
    )

# ==============================================================================
# COMPATIBILIDAD DE ENDPOINT CRUZADO FLASK
# ==============================================================================
@bp_controlador_recurso_humano.record_once
def registrar_alias_endpoint(state):
    """Garantiza la resolución fluida de los endpoints del Controlador de Recursos Humanos."""
    state.app.view_functions['controlador_recurso_humano.dashboard'] = dashboard
    state.app.view_functions['bp_controlador_recurso_humano.dashboard'] = dashboard