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
    
    uid = session.get('usuario_id')
    empresa_id = str(session.get('empresa_id', ''))
    
    kpis = {
        'avance_porcentaje': 0,
        'items_completados': 0,
        'total_asignados': 0,
        'indice_novedades': 0.0,
        'velocidad_picking': 0.0
    }
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        cur.execute("""
            SELECT 
                COUNT(*) as total_items,
                SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO', 'TERMINADO') THEN 1 ELSE 0 END) as completados,
                SUM(CASE WHEN p.novedad_alistamiento IS NOT NULL THEN 1 ELSE 0 END) as novedades,
                SUM(CASE WHEN p.fecha_inicio_alistamiento IS NOT NULL AND p.fecha_fin_alistamiento IS NOT NULL 
                         THEN TIMESTAMPDIFF(MINUTE, p.fecha_inicio_alistamiento, p.fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM picking_importacion_raw p
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa = %s AND (fp.operador_asignado = %s OR p.id_auxiliar_asignado = %s) AND p.puerta_asignada IS NOT NULL
        """, (empresa_id, uid, uid))
        
        row = cur.fetchone()
        if row and row['total_items']:
            kpis['total_asignados'] = int(row['total_items'])
            kpis['items_completados'] = int(row['completados'] or 0)
            novedades = int(row['novedades'] or 0)
            mins = float(row['minutos_totales'] or 0)
            
            if kpis['total_asignados'] > 0:
                kpis['avance_porcentaje'] = round((kpis['items_completados'] / kpis['total_asignados']) * 100)
                
            if kpis['items_completados'] > 0:
                kpis['indice_novedades'] = round((novedades / kpis['items_completados']) * 100, 1)
                if mins > 0:
                    kpis['velocidad_picking'] = round(kpis['items_completados'] / mins, 2)
                    
        cur.close()
    except Exception as e:
        pass

    return render_template('B_modulo_operador_bodegas.html', 
                           usuario=session.get('nombre'),
                           empresa=session.get('empresa'),
                           nit=empresa_id,
                           kpis=kpis)

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
        
        # --- HEARTBEAT OPERARIO ---
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (uid,))
        mysql.connection.commit()
        # -----------------------------

        cur.execute("""
            SELECT 
                p.numero_orden_origen as orden, 
                MAX(p.zona) as zona, 
                MAX(p.secuencia_alistamiento) as secuencia,
                COUNT(*) as total_items, 
                CAST(SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO', 'TERMINADO') THEN 1 ELSE 0 END) AS SIGNED) as items_listos
            FROM picking_importacion_raw p
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa=%s 
              AND (fp.operador_asignado=%s OR p.id_auxiliar_asignado=%s)
              AND p.puerta_asignada IS NOT NULL
              AND (p.estado_actividad IS NULL OR p.estado_actividad NOT IN ('VERIFICADO', 'DESPACHADO', 'FINALIZADO_TOTAL', 'TERMINADO'))
            GROUP BY p.numero_orden_origen
            HAVING items_listos < total_items
            ORDER BY secuencia ASC, orden ASC
        """, (empresa_id, uid, uid))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e:
        return jsonify([])
    
@bp_oper_bodegas.route('/api/operario/items_orden/<orden>')
def operario_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # --- HEARTBEAT OPERARIO ---
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (uid,))
        mysql.connection.commit()
        # -----------------------------

        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, 
                p.cajas_calculadas as req_cajas, p.unidades_calculadas as req_unidades,
                p.cajas_alistadas as act_cajas, p.unidades_alistadas as act_unidades, 
                p.estado_actividad, p.puerta_asignada, p.marca, p.novedad_alistamiento,
                p.autorizacion_alistamiento,
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
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            LEFT JOIN configuracion_rutas_picking crp ON p.marca = crp.marca AND p.id_empresa = crp.id_empresa
            WHERE p.id_empresa=%s AND p.numero_orden_origen=%s AND (fp.operador_asignado=%s OR p.id_auxiliar_asignado=%s) AND p.puerta_asignada IS NOT NULL
            ORDER BY 
                IFNULL(crp.secuencia_picking, 9999) ASC,
                CASE 
                    WHEN p.marca = 'NO EN BASE DE DATOS' THEN 99
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 1
                    WHEN p.codigo_producto = 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 2
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca = 'NO EN BASE DE DATOS' THEN 3
                    ELSE 4
                END ASC,
                p.secuencia_alistamiento ASC, p.estado_actividad ASC, p.descripcion_producto ASC
        """, (session.get('empresa_id'), orden, uid, uid))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except Exception as e: return jsonify([])
    
@bp_oper_bodegas.route('/api/operario/confirmar_item', methods=['POST'])
@csrf.exempt 
def operario_confirmar_item():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    
    uid = session.get('usuario_id')
    d = request.json
    id_row = d.get('id_row')
    act_cajas = d.get('cajas_alistadas', 0)
    act_unidades = d.get('unidades_alistadas', 0)
    novedad = d.get('novedad', None)
    id_supervisor = d.get('id_supervisor', None)
    
    if not id_row: return jsonify({'error': 'Datos incompletos'}), 400

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # --- HEARTBEAT OPERARIO ---
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (uid,))
        # -----------------------------

        cur.execute("SELECT cajas_calculadas, unidades_calculadas FROM picking_importacion_raw WHERE id=%s AND id_empresa=%s", (id_row, session.get('empresa_id')))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            return jsonify({'error': 'Item no encontrado'}), 404
            
        estado_final = 'ALISTADO'
        
        if novedad in ['NO_EN_BD', 'SIN_EAN', 'FALTA_EXISTENCIAS']:
            estado_final = 'EN_PROCESO'
        elif act_cajas < row['cajas_calculadas'] or act_unidades < row['unidades_calculadas']:
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
        """, (estado_final, act_cajas, act_unidades, novedad, id_supervisor, uid, session.get('nombre'), id_row, session.get('empresa_id')))
        
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
        
        # --- HEARTBEAT OPERARIO ---
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (uid,))
        mysql.connection.commit()
        # -----------------------------

        cur.execute("""
            SELECT 
                p.marca, 
                MAX(p.secuencia_alistamiento) as secuencia,
                MAX(crp.secuencia_picking) as ruta_fisica,
                COUNT(DISTINCT p.numero_orden_origen) as total_ordenes,
                COUNT(*) as total_items, 
                CAST(SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO', 'TERMINADO') THEN 1 ELSE 0 END) AS SIGNED) as items_listos
            FROM picking_importacion_raw p
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            LEFT JOIN configuracion_rutas_picking crp ON p.marca = crp.marca AND p.id_empresa = crp.id_empresa
            WHERE p.id_empresa=%s 
              AND (fp.operador_asignado=%s OR p.id_auxiliar_asignado=%s)
              AND p.puerta_asignada IS NOT NULL
              AND (p.estado_actividad IS NULL OR p.estado_actividad NOT IN ('VERIFICADO', 'DESPACHADO', 'FINALIZADO_TOTAL', 'TERMINADO'))
            GROUP BY p.marca
            HAVING items_listos < total_items
            ORDER BY IFNULL(ruta_fisica, 9999) ASC, secuencia ASC, p.marca ASC
        """, (empresa_id, uid, uid))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except: return jsonify([])

@bp_oper_bodegas.route('/api/operario/items_lote/<marca>')
def operario_items_lote(marca):
    if 'usuario_id' not in session: return jsonify([])
    uid = session.get('usuario_id')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # --- HEARTBEAT OPERARIO ---
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (uid,))
        mysql.connection.commit()
        # -----------------------------

        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, p.marca, p.zona, p.numero_orden_origen,
                p.cajas_calculadas as req_cajas, p.unidades_calculadas as req_unidades,
                p.cajas_alistadas as act_cajas, p.unidades_alistadas as act_unidades, 
                p.estado_actividad, p.puerta_asignada, p.novedad_alistamiento,
                p.autorizacion_alistamiento,
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
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            LEFT JOIN configuracion_rutas_picking crp ON p.marca = crp.marca AND p.id_empresa = crp.id_empresa
            WHERE p.id_empresa=%s AND p.marca=%s AND (fp.operador_asignado=%s OR p.id_auxiliar_asignado=%s) AND p.puerta_asignada IS NOT NULL
            ORDER BY 
                IFNULL(crp.secuencia_picking, 9999) ASC,
                CASE 
                    WHEN p.marca = 'NO EN BASE DE DATOS' THEN 99
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 1
                    WHEN p.codigo_producto = 'SIN_CODIGO' AND p.marca != 'NO EN BASE DE DATOS' THEN 2
                    WHEN p.codigo_producto != 'SIN_CODIGO' AND p.marca = 'NO EN BASE DE DATOS' THEN 3
                    ELSE 4
                END ASC,
                p.estado_actividad ASC, p.secuencia_alistamiento ASC, p.numero_orden_origen ASC
        """, (session.get('empresa_id'), marca, uid, uid))
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

# ==============================================================================
# NUEVO ENDPOINT: KPIS OPERADOR EN TIEMPO REAL
# ==============================================================================
@bp_oper_bodegas.route('/api/operario/mis_kpis')
def operario_mis_kpis():
    if 'usuario_id' not in session: return jsonify({})
    
    uid = session.get('usuario_id')
    empresa_id = str(session.get('empresa_id', ''))
    
    kpis = {
        'avance_porcentaje': 0,
        'items_completados': 0,
        'total_asignados': 0,
        'indice_novedades': 0.0,
        'velocidad_picking': 0.0
    }
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # --- HEARTBEAT OPERARIO ---
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE ultima_actividad = NOW()
        """, (uid,))
        mysql.connection.commit()
        # -----------------------------

        cur.execute("""
            SELECT 
                COUNT(p.id) as total_items,
                SUM(CASE WHEN p.estado_actividad IN ('ALISTADO', 'VERIFICADO', 'DESPACHADO', 'FINALIZADO', 'TERMINADO') THEN 1 ELSE 0 END) as completados,
                SUM(CASE WHEN p.novedad_alistamiento IS NOT NULL THEN 1 ELSE 0 END) as novedades,
                SUM(CASE WHEN p.fecha_inicio_alistamiento IS NOT NULL AND p.fecha_fin_alistamiento IS NOT NULL 
                         THEN TIMESTAMPDIFF(MINUTE, p.fecha_inicio_alistamiento, p.fecha_fin_alistamiento) ELSE 0 END) as minutos_totales
            FROM picking_importacion_raw p
            LEFT JOIN fabricantes_proveedores fp ON p.marca = fp.marca AND p.id_empresa = fp.id_empresa
            WHERE p.id_empresa = %s 
              AND p.puerta_asignada IS NOT NULL
              AND (p.id_auxiliar_asignado = %s OR (p.id_auxiliar_asignado IS NULL AND fp.operador_asignado = %s))
        """, (empresa_id, uid, uid))
        
        row = cur.fetchone()
        if row and row['total_items']:
            kpis['total_asignados'] = int(row['total_items'])
            kpis['items_completados'] = int(row['completados'] or 0)
            novedades = int(row['novedades'] or 0)
            mins = float(row['minutos_totales'] or 0)
            
            if kpis['total_asignados'] > 0:
                kpis['avance_porcentaje'] = round((kpis['items_completados'] / kpis['total_asignados']) * 100)
                
            if kpis['items_completados'] > 0:
                kpis['indice_novedades'] = round((novedades / kpis['items_completados']) * 100, 1)
                if mins > 0:
                    kpis['velocidad_picking'] = round(kpis['items_completados'] / mins, 2)
                    
        cur.close()
    except Exception as e:
        pass

    return jsonify(kpis)