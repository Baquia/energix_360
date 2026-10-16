# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR RECURSO HUMANO | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps

bp_controlador_recurso_humano = Blueprint('controlador_recurso_humano', __name__, url_prefix='/controlador_recurso_humano')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def controlador_rrhh_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Recursos Humanos.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_recurso_humano',
            'controlador_administracion',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Recursos Humanos para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD RRHH)
# ==============================================================================
@bp_controlador_recurso_humano.route('/dashboard')
@login_required_custom
@controlador_rrhh_required
def dashboard():
    """
    Renderiza el tablero de control principal para la gestión de Recursos Humanos.
    Aquí se consolida el control de asistencia, nómina y evaluación de personal.
    """
    empresa_id = session.get('empresa_id')
    
    # Kpis proyectados para el frontend
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