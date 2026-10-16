# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: CONTROLADOR VENTAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from app.utils import login_required_custom
from functools import wraps
from app import mysql
import MySQLdb.cursors

bp_controlador_ventas = Blueprint('controlador_ventas', __name__, url_prefix='/controlador_ventas')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def controlador_ventas_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola del Controlador de Ventas.
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
            flash('Acceso denegado: Se requiere perfil de Controlador de Ventas para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD CONTROLADOR COMERCIAL)
# ==============================================================================
@bp_controlador_ventas.route('/dashboard')
@login_required_custom
@controlador_ventas_required
def dashboard():
    """
    Renderiza el tablero de control principal para la gestión de ventas.
    Aquí se consolidan KPIs comerciales, seguimiento de operadores en campo y autorización de pedidos.
    """
    empresa_id = session.get('empresa_id')
    
    # Estructura base de KPIs para el frontend
    kpis = {
        'pedidos_hoy': 0,
        'ventas_totales_hoy': 0.0,
        'vendedores_activos': 0,
        'pedidos_pendientes_autorizacion': 0
    }
    
    operadores_ventas = []
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # 1. Contar vendedores activos (conectados recientemente o registrados)
        cur.execute("""
            SELECT id, nombre, cedula 
            FROM usuarios 
            WHERE empresa_id = %s AND perfil = 'operador_ventas'
        """, (empresa_id,))
        operadores_db = cur.fetchall()
        kpis['vendedores_activos'] = len(operadores_db)
        
        # Simulamos la integración del monitoreo de actividad
        for op in operadores_db:
            op_dict = dict(op)
            # Por defecto asumimos inactivo hasta que se cruce con monitoreo_actividad (se hará por API real-time)
            op_dict['en_linea'] = 0 
            op_dict['pedidos_capturados'] = 0
            op_dict['monto_capturado'] = 0.0
            operadores_ventas.append(op_dict)
            
        cur.close()
    except Exception as e:
        print(f"Error cargando dashboard de ventas: {e}")

    return render_template(
        'B_controlador_ventas.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis,
        operadores=operadores_ventas
    )

# ==============================================================================
# APIS DE MONITOREO Y GESTIÓN EN TIEMPO REAL
# ==============================================================================
@bp_controlador_ventas.route('/api/monitoreo_vendedores')
@login_required_custom
@controlador_ventas_required
def api_monitoreo_vendedores():
    """
    Endpoint para alimentar el mapa o la lista de vendedores en campo en tiempo real.
    """
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                u.id, 
                u.nombre, 
                IF(TIMESTAMPDIFF(SECOND, ma.ultima_actividad, NOW()) <= 60, 1, 0) as en_linea
            FROM usuarios u
            LEFT JOIN monitoreo_actividad ma ON u.id = ma.id_usuario
            WHERE u.empresa_id = %s AND u.perfil = 'operador_ventas'
            ORDER BY en_linea DESC, u.nombre ASC
        """, (empresa_id,))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        print(f"Error API monitoreo vendedores: {e}")
        return jsonify([])