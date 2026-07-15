from flask import Blueprint, render_template, session, redirect, request, jsonify
from app import mysql, csrf
import MySQLdb.cursors

bp_verificador_bodegas = Blueprint('verificador_bodegas', __name__)

# ==============================================================================
# VISTA PRINCIPAL (VERIFICADOR EN CAMPO)
# ==============================================================================
@bp_verificador_bodegas.route('/B_verificador_bodegas.html')
def bodega_verificador():
    if 'usuario_id' not in session: return redirect('/')
    nit = str(session.get('empresa_id', ''))
    return render_template('B_verificador_bodegas.html', 
                           usuario=session.get('nombre'),
                           empresa=session.get('empresa'),
                           nit=nit)

# ==============================================================================
# APIs DE VERIFICACIÓN (ÓRDENES LISTAS PARA AUDITAR)
# ==============================================================================
@bp_verificador_bodegas.route('/api/verificador/ordenes')
def verificador_ordenes():
    if 'usuario_id' not in session: return jsonify([])
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Selecciona las órdenes que tienen ítems en estado ALISTADO
        cur.execute("""
            SELECT 
                numero_orden_origen as orden, 
                MAX(zona) as zona, 
                MAX(secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items, 
                CAST(SUM(CASE WHEN estado_actividad IN ('VERIFICADO', 'DESPACHADO') THEN 1 ELSE 0 END) AS SIGNED) as items_verificados,
                CAST(SUM(CASE WHEN estado_actividad='ALISTADO' THEN 1 ELSE 0 END) AS SIGNED) as items_por_verificar
            FROM picking_importacion_raw 
            WHERE id_empresa=%s 
              AND estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO')
            GROUP BY numero_orden_origen
            HAVING items_por_verificar > 0
            ORDER BY secuencia ASC, orden ASC
        """, (empresa_id,))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        print(f"Error verificador ordenes: {e}")
        return jsonify([])

@bp_verificador_bodegas.route('/api/verificador/items_orden/<orden>')
def verificador_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, p.marca, p.zona, p.numero_orden_origen,
                p.cajas_alistadas as req_cajas, p.unidades_alistadas as req_unidades,
                p.cajas_verificadas as act_cajas, p.unidades_verificadas as act_unidades, 
                p.estado_actividad, p.puerta_asignada, p.novedad_alistamiento,
                IFNULL(prod.unidad_embalaje, 'UND') as unidad_embalaje,
                CASE 
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 'Caso A'
                    WHEN p.codigo_producto = 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 'Caso B'
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca = 'NO EN BASE DE DATOS' THEN 'Caso C'
                    ELSE 'Caso D'
                END as tipo_caso
            FROM picking_importacion_raw p
            LEFT JOIN productos prod 
                ON (p.codigo_producto = prod.ean OR p.codigo_producto = prod.sku) 
                AND p.id_empresa = prod.id_empresa
            WHERE p.id_empresa=%s AND p.numero_orden_origen=%s AND p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO')
            ORDER BY 
                CASE 
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 1
                    WHEN p.codigo_producto = 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 2
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca = 'NO EN BASE DE DATOS' THEN 3
                    ELSE 4
                END ASC,
                p.estado_actividad ASC, p.descripcion_producto ASC
        """, (session.get('empresa_id'), orden))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e: 
        print(f"Error items verificador: {e}")
        return jsonify([])

@bp_verificador_bodegas.route('/api/verificador/confirmar_item', methods=['POST'])
@csrf.exempt 
def verificador_confirmar_item():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    
    d = request.json
    id_row = d.get('id_row')
    verif_cajas = d.get('cajas_verificadas', 0)
    verif_unidades = d.get('unidades_verificadas', 0)
    
    if not id_row: return jsonify({'error': 'Datos incompletos'}), 400

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET 
                estado_actividad='VERIFICADO', 
                cajas_verificadas=%s,
                unidades_verificadas=%s,
                id_verificador=%s,
                fecha_verificacion=NOW()
            WHERE id=%s AND id_empresa=%s
        """, (verif_cajas, verif_unidades, session.get('usuario_id'), id_row, session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'ok', 'message': 'Item verificado con éxito'})
    except Exception as e: 
        return jsonify({'error': str(e)}), 500