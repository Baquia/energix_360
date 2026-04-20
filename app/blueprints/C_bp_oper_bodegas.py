from flask import Blueprint, render_template, session, redirect, request, jsonify
from app import mysql, csrf
import MySQLdb.cursors

bp_oper_bodegas = Blueprint('oper_bodegas', __name__)

@bp_oper_bodegas.route('/C_bodegas.html')
def bodega_operativa():
    if 'usuario_id' not in session: return redirect('/')
    nit = str(session.get('empresa_id', ''))
    return render_template('C_bodegas.html', 
                           usuario=session.get('nombre'),
                           empresa=session.get('empresa'),
                           nit=nit)

@bp_oper_bodegas.route('/api/operario/mis_ordenes')
def operario_mis_ordenes():
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
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

@bp_oper_bodegas.route('/api/operario/items_orden/<orden>')
def operario_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, 
                p.cajas_calculadas as req_cajas, p.unidades_calculadas as req_unidades,
                p.cajas_alistadas as act_cajas, p.unidades_alistadas as act_unidades, 
                p.estado_actividad, p.puerta_asignada,
                IFNULL(prod.unidad_embalaje, 'UND') as unidad_embalaje
            FROM picking_importacion_raw p
            LEFT JOIN productos prod 
                ON (p.codigo_producto = prod.ean OR p.codigo_producto = prod.sku) 
                AND p.id_empresa = prod.id_empresa
            WHERE p.id_empresa=%s AND p.numero_orden_origen=%s AND p.id_auxiliar_asignado=%s
            ORDER BY p.estado_actividad ASC, p.descripcion_producto ASC
        """, (session.get('empresa_id'), orden, session.get('usuario_id')))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e: return jsonify([])
    
@bp_oper_bodegas.route('/api/operario/confirmar_item', methods=['POST'])
@csrf.exempt 
def operario_confirmar_item():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    
    d = request.json
    id_row = d.get('id_row')
    act_cajas = d.get('cajas_alistadas', 0)
    act_unidades = d.get('unidades_alistadas', 0)
    
    if not id_row: return jsonify({'error': 'Datos incompletos'}), 400

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET 
                estado_actividad='FINALIZADO', 
                fecha_fin_alistamiento=NOW(),
                cajas_alistadas=%s,
                unidades_alistadas=%s 
            WHERE id=%s AND id_empresa=%s
        """, (act_cajas, act_unidades, id_row, session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'ok', 'message': 'Item confirmado'})
    except Exception as e: return jsonify({'error': str(e)}), 500
        
@bp_oper_bodegas.route('/api/operario/mis_marcas')
def operario_mis_marcas():
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                marca, 
                COUNT(DISTINCT numero_orden_origen) as total_ordenes,
                COUNT(*) as total_items, 
                SUM(CASE WHEN estado_actividad='FINALIZADO' THEN 1 ELSE 0 END) as items_listos
            FROM picking_importacion_raw 
            WHERE id_empresa=%s AND id_auxiliar_asignado=%s AND (estado_actividad IS NULL OR estado_actividad != 'FINALIZADO_TOTAL')
            GROUP BY marca
            HAVING items_listos < total_items
            ORDER BY marca ASC
        """, (empresa_id, uid))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except: return jsonify([])

@bp_oper_bodegas.route('/api/operario/items_lote/<marca>')
def operario_items_lote(marca):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, p.marca, p.zona, p.numero_orden_origen,
                p.cajas_calculadas as req_cajas, p.unidades_calculadas as req_unidades,
                p.cajas_alistadas as act_cajas, p.unidades_alistadas as act_unidades, 
                p.estado_actividad, p.puerta_asignada,
                IFNULL(prod.unidad_embalaje, 'UND') as unidad_embalaje
            FROM picking_importacion_raw p
            LEFT JOIN productos prod 
                ON (p.codigo_producto = prod.ean OR p.codigo_producto = prod.sku) 
                AND p.id_empresa = prod.id_empresa
            WHERE p.id_empresa=%s AND p.marca=%s AND p.id_auxiliar_asignado=%s
            ORDER BY p.estado_actividad ASC, p.zona ASC, p.numero_orden_origen ASC
        """, (session.get('empresa_id'), marca, session.get('usuario_id')))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except: return jsonify([])