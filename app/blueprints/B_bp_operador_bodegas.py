from flask import Blueprint, render_template, session, redirect, request, jsonify
from app import mysql, csrf
import MySQLdb.cursors

bp_oper_bodegas = Blueprint('oper_bodegas', __name__)

# ==============================================================================
# VISTA PRINCIPAL (OPERADOR EN CAMPO)
# ==============================================================================
@bp_oper_bodegas.route('/B_modulo_operador_bodegas.html')
def bodega_operativa():
    if 'usuario_id' not in session: return redirect('/')
    nit = str(session.get('empresa_id', ''))
    return render_template('B_modulo_operador_bodegas.html', 
                           usuario=session.get('nombre'),
                           empresa=session.get('empresa'),
                           nit=nit)

# ==============================================================================
# APIs DE OPERACIÓN (MIS ÓRDENES Y LOTES)
# ==============================================================================
@bp_oper_bodegas.route('/api/operario/mis_ordenes')
def operario_mis_ordenes():
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Solo trae las órdenes que tienen puerta asignada Y que tienen items de las marcas de este operador
        cur.execute("""
            SELECT 
                p.numero_orden_origen as orden, 
                MAX(p.zona) as zona, 
                MAX(p.secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items, 
                CAST(SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO') THEN 1 ELSE 0 END) AS SIGNED) as items_listos
            FROM picking_importacion_raw p
            INNER JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa=%s 
              AND fp.operador_asignado=%s 
              AND p.puerta_asignada IS NOT NULL
              AND (p.estado_actividad IS NULL OR p.estado_actividad NOT IN ('VERIFICADO', 'DESPACHADO', 'FINALIZADO_TOTAL'))
            GROUP BY p.numero_orden_origen
            HAVING items_listos < total_items
            ORDER BY secuencia ASC, orden ASC
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
        # Filtra exclusivamente los items de la orden cuya marca esté asignada a este operador
        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, 
                p.cajas_calculadas as req_cajas, p.unidades_calculadas as req_unidades,
                p.cajas_alistadas as act_cajas, p.unidades_alistadas as act_unidades, 
                p.estado_actividad, p.puerta_asignada, p.marca, p.novedad_alistamiento,
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
            INNER JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa=%s AND p.numero_orden_origen=%s AND fp.operador_asignado=%s AND p.puerta_asignada IS NOT NULL
            ORDER BY 
                CASE 
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 1
                    WHEN p.codigo_producto = 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 2
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca = 'NO EN BASE DE DATOS' THEN 3
                    ELSE 4
                END ASC,
                p.secuencia_alistamiento ASC, p.estado_actividad ASC, p.descripcion_producto ASC
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
    novedad = d.get('novedad', None)
    id_supervisor = d.get('id_supervisor', None)
    
    if not id_row: return jsonify({'error': 'Datos incompletos'}), 400

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # Consultar las cantidades originales para validar si el operador terminó realmente
        cur.execute("SELECT cajas_calculadas, unidades_calculadas FROM picking_importacion_raw WHERE id=%s AND id_empresa=%s", (id_row, session.get('empresa_id')))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            return jsonify({'error': 'Item no encontrado'}), 404
            
        estado_final = 'ALISTADO'
        
        # Si no se reportó novedad y las cantidades son menores a lo solicitado, es un guardado parcial
        if not novedad and (act_cajas < row['cajas_calculadas'] or act_unidades < row['unidades_calculadas']):
            estado_final = 'EN_PROCESO'
            
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET 
                estado_actividad=%s, 
                fecha_fin_alistamiento=NOW(),
                cajas_alistadas=%s,
                unidades_alistadas=%s,
                novedad_alistamiento=%s,
                id_supervisor_novedad=%s,
                id_auxiliar_asignado=%s,
                nombre_auxiliar_asignado=%s
            WHERE id=%s AND id_empresa=%s
        """, (estado_final, act_cajas, act_unidades, novedad, id_supervisor, session.get('usuario_id'), session.get('nombre'), id_row, session.get('empresa_id')))
        
        mysql.connection.commit()
        cur.close()
        
        if estado_final == 'EN_PROCESO':
            return jsonify({'status': 'ok', 'message': 'Guardado parcial exitoso. El ítem seguirá pendiente en tu lista.'})
        else:
            return jsonify({'status': 'ok', 'message': 'Item alistado, pendiente de verificación'})
    except Exception as e: 
        return jsonify({'error': str(e)}), 500
           
@bp_oper_bodegas.route('/api/operario/mis_marcas')
def operario_mis_marcas():
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    empresa_id = session.get('empresa_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                p.marca, 
                MAX(p.secuencia_alistamiento) as secuencia,
                COUNT(DISTINCT p.numero_orden_origen) as total_ordenes,
                COUNT(*) as total_items, 
                CAST(SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO') THEN 1 ELSE 0 END) AS SIGNED) as items_listos
            FROM picking_importacion_raw p
            INNER JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa=%s 
              AND fp.operador_asignado=%s 
              AND p.puerta_asignada IS NOT NULL
              AND (p.estado_actividad IS NULL OR p.estado_actividad NOT IN ('VERIFICADO', 'DESPACHADO', 'FINALIZADO_TOTAL'))
            GROUP BY p.marca
            HAVING items_listos < total_items
            ORDER BY secuencia ASC, p.marca ASC
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
            INNER JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa=%s AND p.marca=%s AND fp.operador_asignado=%s AND p.puerta_asignada IS NOT NULL
            ORDER BY 
                CASE 
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 1
                    WHEN p.codigo_producto = 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 2
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca = 'NO EN BASE DE DATOS' THEN 3
                    ELSE 4
                END ASC,
                p.estado_actividad ASC, p.secuencia_alistamiento ASC, p.numero_orden_origen ASC
        """, (session.get('empresa_id'), marca, session.get('usuario_id')))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except: return jsonify([])
        
# ==============================================================================
# VALIDACIÓN DE SEGURIDAD (SUPERVISOR)
# ==============================================================================
@bp_oper_bodegas.route('/api/operario/validar_supervisor', methods=['POST'])
@csrf.exempt
def validar_supervisor():
    if 'usuario_id' not in session: 
        return jsonify({'error': 'Sesión expirada'}), 401
    
    d = request.json
    cedula = d.get('cedula')
    clave = d.get('clave')
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT id, clave, nombre, perfil 
            FROM usuarios 
            WHERE cedula = %s AND empresa_id = %s
        """, (cedula, session.get('empresa_id')))
        user = cur.fetchone()
        cur.close()

        if user:
            from app import bcrypt 
            if bcrypt.check_password_hash(user['clave'], clave):
                if user.get('perfil') in ['controlador_logistica', 'administrador', 'supervisor']:
                    return jsonify({'status': 'ok', 'id_supervisor': user['id'], 'nombre': user['nombre']})
                else:
                    return jsonify({'error': 'El usuario no tiene rol de supervisor'}), 403
            else:
                return jsonify({'error': 'Contraseña incorrecta'}), 401
        
        return jsonify({'error': 'Supervisor no encontrado o no pertenece a tu empresa'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500