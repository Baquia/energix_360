# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: HUB VENTAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps

bp_hub_ventas = Blueprint('hub_ventas', __name__, url_prefix='/hub_ventas')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def hub_ventas_required(f):
    """
    Middleware Plug & Play para proteger el acceso al Hub de Ventas.
    Solo permite el ingreso a perfiles autorizados dentro del esquema de Ventas y Distribución.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_ventas',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Ventas para ingresar a este Hub.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# RUTA NATIVA DEL HUB DE VENTAS (M3)
# =========================================================
@bp_hub_ventas.route('/dashboard')
@login_required_custom
@hub_ventas_required
def dashboard():
    """
    Renderiza el tablero principal (Hub) del Controlador de Ventas.
    Aloja los accesos centralizados a Gestión Comercial y reportes de fuerza de ventas.
    """
    return render_template(
        'B_hub_ventas.html',
        nit=session.get('empresa_id'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        submodulos_activos=session.get('submodulos_activos', []),
        perfil=session.get('perfil')
    )