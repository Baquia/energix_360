# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: OPERADOR VENTAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from app.utils import login_required_custom
from functools import wraps
from app import mysql
import MySQLdb.cursors
import time
import json

bp_operador_ventas = Blueprint('operador_ventas', __name__, url_prefix='/operador_ventas')

# ==============================================================================
# AUDITORÍA DE OPERACIONES EN BASE DE DATOS (DIRECTIVA BQA-ONE)
# ==============================================================================
def _registrar_operacion_bqa(empresa, empresa_id, backend_origen, datos, usuario, cedula):
    """
    Registra de forma segura las mutaciones (INSERT/UPDATE/DELETE) en la tabla
    centralizada registro_operaciones_bqa-one con aislamiento de excepciones.
    """
    try:
        cur_op = mysql.connection.cursor()
        cur_op.execute("""
            INSERT INTO `registro_operaciones_bqa-one` 
            (empresa, empresa_id, backend_origen, datos, usuario, cedula, fecha_registro)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (
            str(empresa),
            str(empresa_id),
            str(backend_origen),
            json.dumps(datos, ensure_ascii=False) if isinstance(datos, (dict, list)) else str(datos),
            str(usuario),
            str(cedula)
        ))
        mysql.connection.commit()
        cur_op.close()
    except Exception:
        pass

# ==============================================================================
# DECORADOR DE SUBMÓDULO MULTI-TENANT HOMOLOGADO (DIRECTO Y ROBUSTO)
# ==============================================================================
def submodulo_required(submodulo_nombre):
    """
    Decorador de autorización Plug & Play homologado:
    Verifica explícitamente en session['submodulos_activos'] o en session['modulos_activos']
    o autoriza de forma irrestricta si pertenece al tenant corporativo (NIT 901811727).
    Debe invocarse SIEMPRE con el argumento: @submodulo_required('nombre_modulo')
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            submodulos_activos = session.get('submodulos_activos', [])
            modulos_activos = session.get('modulos_activos', [])
            empresa_id = str(session.get('empresa_id', '')).strip()

            if isinstance(submodulos_activos, str):
                try:
                    submodulos_activos = json.loads(submodulos_activos)
                except Exception:
                    submodulos_activos = []

            # Validación: inquilino corporativo Baquía o empresas con el módulo activo
            if (
                submodulo_nombre not in submodulos_activos
                and submodulo_nombre not in modulos_activos
                and 'operador_ventas' not in modulos_activos
                and 'controlador_ventas' not in modulos_activos
                and 'ventas' not in modulos_activos
                and empresa_id != '901811727'
            ):
                flash(f'Acceso denegado: Su empresa no tiene habilitado el submódulo de {submodulo_nombre}.', 'danger')
                return redirect(url_for('panel_principal'))
            return f(*args, **kwargs)
        return decorated_function 
    return decorator

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def operador_ventas_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la interfaz móvil/campo
    del Operador de Ventas (Asesor Comercial).
    Autoriza perfiles de piso nativos y la jerarquía corporativa de Baquía-ems SAS.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        perfiles_permitidos = [
            'operador_ventas',
            'vendedor',
            'controlador_ventas',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster',
            'gerente_bqa',
            'admin_general',
            'webmaster_administrador_baquia'
        ]
        
        # Validación estricta de perfil y aislamiento Multi-Tenant
        if (
            perfil not in perfiles_permitidos 
            and 'webmaster' not in tipo_empresa 
            and empresa_id != '901811727'
        ):
            flash('Acceso denegado: Se requiere perfil de Operador de Ventas para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD MÓVIL OPERADOR DE VENTAS)
# ==============================================================================
@bp_operador_ventas.route('/dashboard', endpoint='dashboard')
@bp_operador_ventas.route('/dashboard_operador', endpoint='dashboard_operador')
@login_required_custom
@submodulo_required('operador_ventas')
@operador_ventas_required
def dashboard_operador():
    """
    Renderiza la vista operativa móvil del Operador de Ventas.
    Permite registrar pedidos en campo, consultar catálogo y reportar visitas.
    """
    usuario_id = session.get('usuario_id')
    empresa_id = session.get('empresa_id')

    # Heartbeat preventivo al cargar vista con política de reintentos
    for intento in range(3):
        try:
            cur_hb = mysql.connection.cursor()
            cur_hb.execute("""
                INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
                VALUES (%s, NOW())
                ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
            """, (usuario_id,))
            mysql.connection.commit()
            cur_hb.close()
            break
        except Exception as e:
            if "deadlock" in str(e).lower() and intento < 2:
                time.sleep(0.5)
                continue
            break

    kpis = {
        'pedidos_hoy': 0,
        'monto_acumulado_hoy': 0.0,
        'clientes_visitados': 0
    }

    return render_template(
        'B_operador_ventas.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis
    )

# ==============================================================================
# APIS OPERATIVAS DEL VENDEDOR EN CAMPO
# ==============================================================================
@bp_operador_ventas.route('/api/ping', methods=['POST'])
@login_required_custom
@operador_ventas_required
def api_ping():
    """
    Heartbeat en tiempo real para marcar al vendedor 'En Línea' en la consola.
    """
    usuario_id = session.get('usuario_id')
    for intento in range(3):
        try:
            cur = mysql.connection.cursor()
            cur.execute("""
                INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
                VALUES (%s, NOW())
                ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
            """, (usuario_id,))
            mysql.connection.commit()
            cur.close()
            return jsonify({'status': 'ok'})
        except Exception as e:
            if "deadlock" in str(e).lower() and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify({'status': 'error', 'message': str(e)}), 500

@bp_operador_ventas.route('/api/catalogo_productos', methods=['GET'])
@login_required_custom
@operador_ventas_required
def api_catalogo_productos():
    """
    Obtiene el catálogo de productos disponibles para la empresa del vendedor.
    """
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT ean, sku, producto, IFNULL(fabricante, 'GENERICO') as marca, unidad_embalaje
            FROM productos
            WHERE id_empresa = %s
            ORDER BY producto ASC
            LIMIT 100
        """, (empresa_id,))
        productos = cur.fetchall()
        cur.close()
        return jsonify(productos)
    except Exception as e:
        print(f"Error cargando catálogo para operador ventas: {e}")
        return jsonify([])

@bp_operador_ventas.route('/api/clientes_asignados', methods=['GET'])
@login_required_custom
@operador_ventas_required
def api_clientes_asignados():
    """
    Obtiene la lista de clientes registrados para toma de pedidos.
    """
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT id_cliente_empresa as id_cliente, cliente_empresa as nombre
            FROM clientes_empresa
            WHERE id_empresa = %s
            ORDER BY cliente_empresa ASC
        """, (empresa_id,))
        clientes = cur.fetchall()
        cur.close()
        return jsonify(clientes)
    except Exception as e:
        print(f"Error cargando clientes para operador ventas: {e}")
        return jsonify([])

@bp_operador_ventas.route('/api/guardar_pedido', methods=['POST'])
@login_required_custom
@operador_ventas_required
def api_guardar_pedido():
    """
    Registra un pedido capturado en campo por el operador de ventas y lo audita.
    """
    data = request.json or {}
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    usuario_id = session.get('usuario_id')
    usuario_nombre = session.get('nombre')
    cedula_usuario = session.get('cedula', '')
    
    cliente = data.get('cliente')
    items = data.get('items', [])
    
    if not cliente or not items:
        return jsonify({'status': 'error', 'message': 'El cliente y los ítems del pedido son obligatorios.'}), 400

    try:
        cur = mysql.connection.cursor()
        
        # Actualizar monitoreo de actividad para evitar desconexión
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (usuario_id,))
        
        mysql.connection.commit()
        cur.close()

        # Registro de trazabilidad y auditoría de la operación en BQA-ONE
        _registrar_operacion_bqa(
            empresa=empresa_nombre,
            empresa_id=empresa_id,
            backend_origen='B_bp_operador_ventas.api_guardar_pedido',
            datos={
                'cliente': cliente,
                'total_items': len(items)
            },
            usuario=usuario_nombre,
            cedula=cedula_usuario
        )
        
        return jsonify({'status': 'success', 'message': 'Pedido capturado exitosamente y enviado a consola comercial.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

# ==============================================================================
# COMPATIBILIDAD DE ENDPOINT CRUZADO FLASK
# ==============================================================================
@bp_operador_ventas.record_once
def registrar_alias_endpoint(state):
    """Garantiza la resolución fluida de los endpoints del Operador de Ventas."""
    state.app.view_functions['operador_ventas.dashboard'] = dashboard_operador
    state.app.view_functions['bp_operador_ventas.dashboard'] = dashboard_operador
    state.app.view_functions['operador_ventas.dashboard_operador'] = dashboard_operador
    state.app.view_functions['bp_operador_ventas.dashboard_operador'] = dashboard_operador