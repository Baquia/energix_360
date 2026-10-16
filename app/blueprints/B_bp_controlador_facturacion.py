# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR FACTURACIÓN | CONDICIÓN: OPCIONAL | TIPO: 2
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom, submodulo_required
from functools import wraps

bp_controlador_facturacion = Blueprint('controlador_facturacion', __name__, url_prefix='/controlador_facturacion')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def controlador_facturacion_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Facturación.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_facturacion',
            'controlador_administracion',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de facturación o administrativo para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD FACTURACIÓN)
# ==============================================================================
@bp_controlador_facturacion.route('/dashboard')
@login_required_custom
@submodulo_required('controlador_facturacion')
@controlador_facturacion_required
def dashboard():
    """
    Renderiza el tablero de control principal para la gestión de facturación.
    Aquí se consolida la emisión de facturas, notas de crédito, y seguimiento de cobros.
    """
    empresa_id = session.get('empresa_id')
    
    # KPIs proyectados para el frontend
    kpis = {
        'facturas_emitidas_hoy': 0,
        'valor_facturado_hoy': 0.0,
        'notas_credito_mes': 0,
        'facturas_pendientes_pago': 0
    }
    
    # Listas vacías simuladas para la vista
    alertas_facturacion = []
    
    return render_template(
        'B_controlador_facturacion.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis,
        alertas=alertas_facturacion
    )