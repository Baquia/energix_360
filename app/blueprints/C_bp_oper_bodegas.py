from flask import Blueprint, render_template, session, redirect, request, jsonify
from app import mysql, csrf
import MySQLdb.cursors

# 1. Definimos el nuevo Blueprint: 'oper_bodegas'
bp_oper_bodegas = Blueprint('oper_bodegas', __name__)

# --- VISTA: PANTALLA DEL CELULAR ---
@bp_oper_bodegas.route('/C_bodegas.html')
def bodega_operativa():
    if 'usuario_id' not in session: return redirect('/')
    
    # LÓGICA IMPORTADA DE B_bp_bodegas.py
    # Usamos el empresa_id como NIT, convertido a string
    nit = str(session.get('empresa_id', ''))
    
    return render_template('C_bodegas.html', 
                           usuario=session.get('nombre'),
                           empresa=session.get('empresa'),
                           nit=nit) # Pasamos el NIT para el logo

# --- API 1: MIS ÓRDENES ---
@bp_oper_bodegas.route('/api/operario/mis_ordenes')
def operario_mis_ordenes():
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    empresa_id = session.get('empresa_id')

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Trae órdenes asignadas que NO estén totalmente finalizadas
        cur.execute("""
            SELECT 
                numero_orden_origen as orden, 
                MAX(zona) as zona, 
                COUNT(*) as total_items, 
                SUM(CASE WHEN estado_actividad='FINALIZADO' THEN 1 ELSE 0 END) as items_listos
            FROM picking_importacion_raw 
            WHERE id_empresa=%s 
              AND id_auxiliar_asignado=%s 
              AND (estado_actividad IS NULL OR estado_actividad != 'FINALIZADO_TOTAL')
            GROUP BY numero_orden_origen
            HAVING items_listos < total_items
            ORDER BY orden ASC
        """, (empresa_id, uid))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        print(f"Error mis ordenes: {e}")
        return jsonify([])

# --- API 2: ITEMS DE UNA ORDEN ---
@bp_oper_bodegas.route('/api/operario/items_orden/<orden>')
def operario_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    empresa_id = session.get('empresa_id')

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                id, 
                codigo_producto, 
                descripcion_producto, 
                unidades_calculadas as cantidad, 
                estado_actividad
            FROM picking_importacion_raw 
            WHERE id_empresa=%s 
              AND numero_orden_origen=%s 
              AND id_auxiliar_asignado=%s
            ORDER BY estado_actividad ASC, descripcion_producto ASC
        """, (empresa_id, orden, uid))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([])

# --- API 3: CONFIRMAR ITEM ---
# --- API 3: CONFIRMAR ITEM CON CANTIDAD REAL ---
@bp_oper_bodegas.route('/api/operario/confirmar_item', methods=['POST'])
@csrf.exempt
def operario_confirmar_item():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    
    d = request.json
    id_row = d.get('id_row')
    # Recibimos la cantidad real que contó el operario
    cantidad_real = d.get('cantidad_alistada', 0) 
    
    try:
        cur = mysql.connection.cursor()
        
        # Actualizamos estado, fecha fin Y la cantidad real
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET 
                estado_actividad='FINALIZADO', 
                fecha_fin_alistamiento=NOW(),
                cantidad_alistada=%s 
            WHERE id=%s AND id_empresa=%s
        """, (cantidad_real, id_row, session.get('empresa_id')))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'ok'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500