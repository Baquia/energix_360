from flask import Blueprint, render_template, session, redirect, url_for, flash, abort
from app.utils import login_required_custom
from app import mysql 

bp_transporte_carga = Blueprint('bp_transporte_carga', __name__, url_prefix='/tc')

EMPRESAS_TC_AUTORIZADAS = ['890707006', '900000000']

# HELPER: Configuración de Colores
def get_config_tc(nit):
    configuracion_empresas = {
        '890707006': {
            'logo': 'logo_890707006.PNG', 
            'verde_principal': '#eab308', 
            'verde_oscuro': '#ca8a04', 
            'fondo_gradiente': '#fef9c3'
        },
        '900000000': {
            'logo': 'logo_cliente_carga.PNG',
            'verde_principal': '#dc2626', 
            'verde_oscuro': '#991b1b', 
            'fondo_gradiente': '#fee2e2'
        }
    }
    return configuracion_empresas.get(nit, {
        'verde_principal': '#015249', 
        'verde_oscuro': '#003b35', 
        'fondo_gradiente': '#d7f2ec',
        'logo': f'logo_{nit}.PNG'
    })

# ---------------------------------------------------------
# 1. DASHBOARD GENERAL (Menú)
# ---------------------------------------------------------
@bp_transporte_carga.route('/<string:nit>.html')
@login_required_custom
def dashboard_dinamico(nit):
    if nit not in EMPRESAS_TC_AUTORIZADAS: abort(404)
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))

    return render_template(
        'dashboard_tc_generico.html', 
        nit=nit, 
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        config=get_config_tc(nit)
    )

# ---------------------------------------------------------
# 2. ROUTER INTELIGENTE (El Semáforo)
# ---------------------------------------------------------
@bp_transporte_carga.route('/app/control_flota/<string:nit>')
@login_required_custom
def router_control_flota(nit):
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))

    # Consultamos el perfil
    cur = mysql.connection.cursor()
    cur.execute("SELECT perfil FROM usuarios WHERE id = %s", (session.get('usuario_id'),))
    usuario = cur.fetchone()
    cur.close()

    if not usuario: return redirect(url_for('index'))
    
    perfil = (usuario['perfil'] or '').strip().lower()

    # DECISIÓN DE RUTA
    if perfil == 'controlador_transportecarga':
        return redirect(url_for('bp_transporte_carga.vista_controlador', nit=nit))
    
    elif perfil == 'operador_transportecarga':
        return redirect(url_for('bp_transporte_carga.vista_operador', nit=nit))
    
    else:
        flash("Acceso denegado: Perfil no autorizado para Flota.", "warning")
        return redirect(url_for('bp_transporte_carga.dashboard_dinamico', nit=nit))

# ---------------------------------------------------------
# 3. VISTAS FINALES
# ---------------------------------------------------------

# A) Vista para CONTROLADOR (control_vehiculos_tcarga.html)
@bp_transporte_carga.route('/control_flota/admin/<string:nit>')
@login_required_custom
def vista_controlador(nit):
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))
    
    return render_template(
        'control_vehiculos_tcarga.html', 
        nit=nit, nombre=session.get('nombre'), empresa=session.get('empresa'), config=get_config_tc(nit)
    )

# B) Vista para OPERADOR (vehiculos_tcarga.html)
@bp_transporte_carga.route('/control_flota/operador/<string:nit>')
@login_required_custom
def vista_operador(nit):
    if str(session.get('empresa_id')) != nit: return redirect(url_for('index'))
    
    return render_template(
        'vehiculos_tcarga.html', 
        nit=nit, nombre=session.get('nombre'), empresa=session.get('empresa'), config=get_config_tc(nit)
    )

# ---------------------------------------------------------
# 4. OTROS MÓDULOS
# ---------------------------------------------------------
@bp_transporte_carga.route('/app/conductores/<string:nit>')
@login_required_custom
def ruta_conductores(nit):
    flash("Módulo Conductores en construcción", "info")
    return redirect(url_for('bp_transporte_carga.dashboard_dinamico', nit=nit))

# INYECCIÓN DE DEPENDENCIAS
try:
    from . import bp_documentos_vehiculos, bp_mantenimiento_vehiculos, bp_inspecciones_vehiculos
    bp_documentos_vehiculos.init_routes(bp_transporte_carga)
    bp_mantenimiento_vehiculos.init_routes(bp_transporte_carga)
    bp_inspecciones_vehiculos.init_routes(bp_transporte_carga)
except ImportError: pass
except AttributeError: pass