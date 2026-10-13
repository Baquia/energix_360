# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: HUB NATIVO | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom
from functools import wraps

bp_hub_ventas_distribucion = Blueprint('hub_ventas_distribucion', __name__, url_prefix='/hub_ventas_distribucion')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def hub_ventas_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'controlador_bodegas',
            'gestor_flotacarga',
            'controlador_logistica',
            'administrador',
            'webmaster'
        ]
        
        # Validación estricta de perfil (Solo Controladores ingresan al Hub M3)
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador para ingresar al Hub de Ventas y Distribución.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# RUTA NATIVA DEL HUB M3
# =========================================================
@bp_hub_ventas_distribucion.route('/dashboard')
@login_required_custom
@hub_ventas_required
def dashboard():
    return render_template(
        'B_hub_ventas_distribucion.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        submodulos_activos=session.get('submodulos_activos', []),
        perfil=session.get('perfil')
    )