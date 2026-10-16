# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: HUB BODEGAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps

bp_hub_bodegas = Blueprint('hub_bodegas', __name__, url_prefix='/hub_bodegas')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def hub_bodegas_required(f):
    """
    Middleware Plug & Play para proteger el acceso al Hub de Bodegas.
    Solo permite el ingreso a perfiles autorizados dentro del esquema de Ventas y Distribución.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_bodegas',
            'controlador_logistica',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Bodegas para ingresar a este Hub.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# RUTA NATIVA DEL HUB DE BODEGAS (M3)
# =========================================================
@bp_hub_bodegas.route('/dashboard')
@login_required_custom
@hub_bodegas_required
def dashboard():
    """
    Renderiza el tablero principal (Hub) del Controlador de Bodegas.
    Aloja los accesos centralizados a Gestión de Bodegas y Transporte de Carga.
    """
    return render_template(
        'B_hub_bodegas.html',
        nit=session.get('empresa_id'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        submodulos_activos=session.get('submodulos_activos', []),
        perfil=session.get('perfil')
    )