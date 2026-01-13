from flask import Blueprint, render_template, session, redirect, url_for, flash, abort
from app.utils import login_required_custom
from app import mysql 

bp_transporte_especial = Blueprint('bp_transporte_especial', __name__, url_prefix='/te')

EMPRESAS_TE_AUTORIZADAS = ['100000000', '901999999']

# HELPER: Configuración de Colores
def get_config_te(nit):
    configuracion_empresas = {
        '100000000': {
            'logo': 'logo_100000000.png', 
            'verde_principal': '#2e7d32', 
            'verde_oscuro': '#1b5e20', 
            'fondo_gradiente': '#e8f5e9'
        }
    }
    return configuracion_empresas.get(nit, {
        'verde_principal': '#015249', 
        'verde_oscuro': '#003b35', 
        'fondo_gradiente': '#d7f2ec',
        'logo': f'logo_{nit}.PNG'
    })

# ---------------------------------------------------------
# 1. DASHBOARD GENERAL
# ---------------------------------------------------------
@bp_transporte_especial.route('/<string:nit>.html')
@login_required_custom
def dashboard_dinamico(nit):
    if nit not in EMPRESAS_TE_AUTORIZADAS: abort(404)
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))

    return render_template(
        'dashboard_te_generico.html', 
        nit=nit, 
        nombre=session.get('nombre'), 
        empresa=session.get('empresa'),
        config=get_config_te(nit)
    )

# ---------------------------------------------------------
# 2. ROUTER INTELIGENTE (El Semáforo)
# ---------------------------------------------------------
@bp_transporte_especial.route('/app/control_flota/<string:nit>')
@login_required_custom
def router_control_flota(nit):
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (session.get('usuario_id'),))
    usuario = cur.fetchone()
    cur.close()

    if not usuario: return redirect(url_for('index'))
    
    perfil = (usuario['perfil'] or '').strip().lower()

    # DECISIÓN DE RUTA
    if perfil == 'controlador_transporteespecial':
        return redirect(url_for('bp_transporte_especial.vista_controlador', nit=nit))
    
    elif perfil == 'operador_transporteespecial':
        return redirect(url_for('bp_transporte_especial.vista_operador', nit=nit))
    
    else:
        flash("Acceso denegado: Perfil no autorizado para Flota.", "warning")
        return redirect(url_for('bp_transporte_especial.dashboard_dinamico', nit=nit))

# ---------------------------------------------------------
# 3. VISTAS FINALES
# ---------------------------------------------------------

# A) Vista para CONTROLADOR (control_vehiculos_tespecial.html)
@bp_transporte_especial.route('/control_flota/admin/<string:nit>')
@login_required_custom
def vista_controlador(nit):
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))
    
    return render_template(
        'control_vehiculos_tespecial.html', 
        nit=nit, nombre=session.get('nombre'), config=get_config_te(nit)
    )

# B) Vista para OPERADOR (vehiculos_tespecial.html)
@bp_transporte_especial.route('/control_flota/operador/<string:nit>')
@login_required_custom
def vista_operador(nit):
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))
    
    return render_template(
        'vehiculos_tespecial.html', 
        nit=nit, nombre=session.get('nombre'), config=get_config_te(nit)
    )

# ---------------------------------------------------------
# 4. OTROS MÓDULOS
# ---------------------------------------------------------
@bp_transporte_especial.route('/app/conductores/<string:nit>')
@login_required_custom
def ruta_conductores(nit):
    flash("Módulo Conductores en construcción", "info")
    return redirect(url_for('bp_transporte_especial.dashboard_dinamico', nit=nit))

# INYECCIÓN DE DEPENDENCIAS
try:
    from . import bp_documentos_vehiculos, bp_mantenimiento_vehiculos, bp_inspecciones_vehiculos
    bp_documentos_vehiculos.init_routes(bp_transporte_especial)
    bp_mantenimiento_vehiculos.init_routes(bp_transporte_especial)
    bp_inspecciones_vehiculos.init_routes(bp_transporte_especial)
except ImportError: pass
except AttributeError: pass