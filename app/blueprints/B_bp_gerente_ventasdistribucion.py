# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: GERENCIA VENTAS Y DISTRIBUCIÓN | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps

bp_gerente_ventasdistribucion = Blueprint('gerencia_ventas', __name__, url_prefix='/gerencia_ventas')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def gerente_ventas_required(f):
    """
    Middleware Plug & Play para proteger el acceso al Hub de Gerencia.
    Solo permite el ingreso a perfiles de alta dirección dentro del esquema de Ventas y Distribución.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Gerencia para ingresar a este Hub.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# RUTA NATIVA DEL HUB DE GERENCIA (M3)
# =========================================================
@bp_gerente_ventasdistribucion.route('/dashboard')
@login_required_custom
@gerente_ventas_required
def dashboard():
    """
    Renderiza el tablero principal (Hub) del Gerente de Ventas y Distribución.
    Aloja los accesos centralizados a reportes gerenciales, KPIs globales y configuración estratégica.
    """
    return render_template(
        'B_gerente_ventasdistribucion.html',
        nit=session.get('empresa_id'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        submodulos_activos=session.get('submodulos_activos', []),
        perfil=session.get('perfil')
    )