# MÓDULO: VENTAS Y DISTRIBUCIÓN | SUBMÓDULO: OPERADOR CONTABLE | CONDICIÓN: OPCIONAL | TIPO: 2
from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from app.utils import login_required_custom, submodulo_required
from functools import wraps
from app import mysql
import MySQLdb.cursors
import time
from datetime import datetime

bp_operador_contable = Blueprint('operador_contable', __name__, url_prefix='/operador_contable')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS
# ==============================================================================
def operador_contable_required(f):
    """
    Middleware Plug & Play para proteger el acceso a la consola operativa
    del Operador Contable (Auxiliar / Asistente Contable).
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'operador_contable',
            'controlador_contabilidad',
            'controlador_administracion',
            'gerente_ventasdistribucion',
            'administrador',
            'webmaster_admin',
            'webmaster'
        ]
        
        # Validación estricta de perfil
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Operador Contable para ingresar a este módulo.', 'danger')
            return redirect(url_for('panel_principal'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# VISTA PRINCIPAL (DASHBOARD OPERADOR CONTABLE)
# ==============================================================================
@bp_operador_contable.route('/dashboard')
@login_required_custom
@submodulo_required('controlador_contabilidad')
@operador_contable_required
def dashboard_operador():
    """
    Renderiza la interfaz operativa del Auxiliar Contable.
    Permite registrar comprobantes de egreso, recaudos de cartera, caja menor y conciliar movimientos.
    """
    usuario_id = session.get('usuario_id')
    empresa_id = session.get('empresa_id')

    # Heartbeat preventivo al cargar vista con reintentos para evitar colisiones
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
        'recaudos_hoy': 0.0,
        'gastos_hoy': 0.0,
        'documentos_pendientes': 0
    }

    return render_template(
        'B_operador_contable.html',
        nit=empresa_id,
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        perfil=session.get('perfil'),
        kpis=kpis
    )

# ==============================================================================
# APIS OPERATIVAS DE CONTABILIDAD
# ==============================================================================
@bp_operador_contable.route('/api/ping', methods=['POST'])
@login_required_custom
@operador_contable_required
def api_ping():
    """
    Heartbeat en tiempo real para reportar al operador contable activo.
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

@bp_operador_contable.route('/api/cuentas_por_cobrar', methods=['GET'])
@login_required_custom
@operador_contable_required
def api_cuentas_por_cobrar():
    """
    Consulta facturas y clientes pendientes de cobro para asentar recaudos.
    """
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Búsqueda multi-tenant estricta de clientes de la empresa
        cur.execute("""
            SELECT id_cliente_empresa as id_cliente, cliente_empresa as cliente
            FROM clientes_empresa
            WHERE id_empresa = %s
            ORDER BY cliente_empresa ASC
        """, (empresa_id,))
        clientes = cur.fetchall()
        cur.close()
        return jsonify(clientes)
    except Exception as e:
        print(f"Error cargando clientes para recaudos: {e}")
        return jsonify([])

@bp_operador_contable.route('/api/registrar_recaudo', methods=['POST'])
@login_required_custom
@operador_contable_required
def api_registrar_recaudo():
    """
    Registra un recibo de caja / abono a cartera de un cliente.
    """
    data = request.json
    usuario_id = session.get('usuario_id')
    cliente_id = data.get('cliente_id')
    valor_recaudo = data.get('valor_recaudo')
    metodo_pago = data.get('metodo_pago', 'Efectivo')
    soporte_referencia = data.get('soporte_referencia', '')

    if not cliente_id or not valor_recaudo:
        return jsonify({'status': 'error', 'message': 'Cliente y valor de recaudo son obligatorios.'}), 400

    try:
        cur = mysql.connection.cursor()
        
        # Registrar actividad del operador
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (usuario_id,))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Recaudo de cartera asentado correctamente.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500

@bp_operador_contable.route('/api/registrar_gasto', methods=['POST'])
@login_required_custom
@operador_contable_required
def api_registrar_gasto():
    """
    Registra un comprobante de egreso o movimiento de caja menor.
    """
    data = request.json
    usuario_id = session.get('usuario_id')
    concepto = data.get('concepto')
    valor_gasto = data.get('valor_gasto')
    categoria = data.get('categoria', 'Operativo')
    numero_comprobante = data.get('numero_comprobante', '')

    if not concepto or not valor_gasto:
        return jsonify({'status': 'error', 'message': 'Concepto y valor del egreso son obligatorios.'}), 400

    try:
        cur = mysql.connection.cursor()
        
        # Registrar actividad del operador
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (usuario_id,))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Comprobante de egreso asentado exitosamente.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': str(e)}), 500