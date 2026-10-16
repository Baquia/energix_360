# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR CONTABILIDAD | CONDICIÓN: OPCIONAL | TIPO: 2
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom, submodulo_required
from functools import wraps

bp_controlador_contabilidad = Blueprint('controlador_contabilidad', __name__, url_prefix='/controlador_contabilidad')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def controlador_contabilidad_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Contabilidad.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_contabilidad',
            'controlador_administracion',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil contable o administrativo para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD CONTABLE)
# ==============================================================================
@bp_controlador_contabilidad.route('/dashboard')
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