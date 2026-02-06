from flask import Blueprint, render_template, request, jsonify, session, redirect
from app import mysql, csrf
import pandas as pd
import re
from datetime import datetime, timedelta
import MySQLdb.cursors

bp_bodegas = Blueprint('bodegas', __name__)

# --- 1. VISTA PRINCIPAL (DASHBOARD JEFE) ---
@bp_bodegas.route('/control_logistica')
def control_logistica():
    if 'usuario_id' not in session: return redirect('/')
    
    empresa_id = session.get('empresa_id')
    
    # A. KPI: Pedidos Totales y Pendientes
    kpis = {'pedidos_totales': 0, 'items_pendientes': 0, 'items_finalizados': 0}
    
    # B. Listas para la Vista
    ordenes_sin_asignar = [] # Para el Modal de Arriba
    ordenes_procesadas = []  # Lista final ordenada y filtrada para la tabla

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # 1. Obtener KPIs
        cur.execute("""
            SELECT 
                COUNT(DISTINCT numero_orden_origen) as total,
                SUM(CASE WHEN estado_actividad != 'FINALIZADO' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN estado_actividad = 'FINALIZADO' THEN 1 ELSE 0 END) as listos
            FROM picking_importacion_raw 
            WHERE id_empresa = %s
        """, (empresa_id,))
        row = cur.fetchone()
        if row:
            kpis['pedidos_totales'] = row['total']
            kpis['items_pendientes'] = int(row['pendientes'] or 0)
            kpis['items_finalizados'] = int(row['listos'] or 0)

        # 2. Obtener Órdenes SIN ASIGNAR (Para el Modal de Alistamiento)
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(zona) as zona,
                COUNT(*) as items,
                MAX(fecha_carga) as fecha
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL)
            GROUP BY numero_orden_origen
            ORDER BY MAX(fecha_carga) DESC
        """, (empresa_id,))
        ordenes_sin_asignar = cur.fetchall()

        # 3. LOGICA MEJORADA: Obtener Órdenes ASIGNADAS con Filtro 24h y Ordenamiento
        # Traemos MIN(inicio) para el arranque y MAX(fin) para el cierre
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(nombre_auxiliar_asignado) as operario,
                MAX(zona) as zona,
                COUNT(*) as total_items,
                SUM(CASE WHEN estado_actividad='FINALIZADO' THEN 1 ELSE 0 END) as items_listos,
                MIN(fecha_inicio_alistamiento) as inicio,
                MAX(fecha_fin_alistamiento) as fin
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND estado_actividad IN ('ASIGNADO', 'EN_PROCESO', 'FINALIZADO')
            GROUP BY numero_orden_origen
        """, (empresa_id,))
        raw_ordenes = cur.fetchall()
        
        activos = []
        finalizados = []
        ahora = datetime.now()

        for o in raw_ordenes:
            # Determinar si la orden está completamente finalizada (100% items listos)
            es_finalizado = (o['items_listos'] == o['total_items']) and (o['total_items'] > 0)
            
            # Calcular duración total estática si está finalizado
            o['duracion_str'] = "--:--"
            if es_finalizado and o['inicio'] and o['fin']:
                diff = o['fin'] - o['inicio']
                total_seconds = int(diff.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                o['duracion_str'] = f"{hours:02}:{minutes:02}:{seconds:02}"

            if es_finalizado:
                o['estado_visual'] = 'FINALIZADO'
                # REGLA DE 24 HORAS:
                # Si la fecha de fin fue hace menos de 24 horas, se muestra. Si no, se descarta.
                fecha_cierre = o['fin'] or ahora
                if (ahora - fecha_cierre) < timedelta(hours=24):
                    finalizados.append(o)
            else:
                o['estado_visual'] = 'EN_PROCESO'
                activos.append(o)

        # ORDENAMIENTO:
        # 1. Activos: Los más recientes (por fecha inicio) arriba
        activos.sort(key=lambda x: x['inicio'] or datetime.min, reverse=True)
        # 2. Finalizados: Los que terminaron más recientemente arriba
        finalizados.sort(key=lambda x: x['fin'] or datetime.min, reverse=True)

        # Concatenar: Primero los ACTIVOS, luego los FINALIZADOS
        ordenes_procesadas = activos + finalizados
        
        cur.close()

    except Exception as e:
        print(f"Error cargando dashboard: {e}")

    # Pasamos las listas a la plantilla
    return render_template('B_control_logistica.html', 
                           kpis=kpis, 
                           ordenes_pendientes=ordenes_sin_asignar, # Para el modal
                           ordenes_asignadas=ordenes_procesadas)   # Para la tabla principal

# --- 2. API: DETALLE DE ITEMS DE UNA ORDEN ---
@bp_bodegas.route('/bodegas/api/items_orden/<orden>')
def get_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT 
                id, codigo_producto, descripcion_producto, marca, 
                unidades_calculadas as cantidad, cantidad_alistada, estado_actividad
            FROM picking_importacion_raw 
            WHERE numero_orden_origen = %s AND id_empresa = %s
        """, (orden, session.get('empresa_id')))
        items = cur.fetchall()
        cur.close()
        return jsonify(items)
    except Exception as e:
        return jsonify([])

# --- 3. API: OBTENER LISTA DE OPERARIOS ---
@bp_bodegas.route('/bodegas/api/get_empleados')
def get_empleados():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT id, nombre 
            FROM usuarios 
            WHERE empresa_id = %s AND perfil = 'operador_logistica'
        """, (session.get('empresa_id'),))
        return jsonify(cur.fetchall())
    except: return jsonify([])

# --- 4. API: ASIGNAR ORDEN ---
@bp_bodegas.route('/bodegas/asignar_orden', methods=['POST'])
@csrf.exempt
def asignar_orden():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET id_auxiliar_asignado=%s, nombre_auxiliar_asignado=%s, estado_actividad='ASIGNADO', fecha_inicio_alistamiento=NOW()
            WHERE numero_orden_origen=%s AND id_empresa=%s
        """, (d['id_operario'], d['nombre_operario'], d['numero_orden'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Orden asignada.'})
    except Exception as e: return jsonify({'error': str(e)}), 500

# --- 5. FUNCIONES AUXILIARES Y CARGA EXCEL ---
def normalizar_codigo(valor):
    if pd.isna(valor) or str(valor).strip() == '': return ''
    val_str = str(valor).strip()
    if 'E' in val_str.upper():
        try: return str(int(float(valor)))
        except: pass
    if val_str.endswith('.0'): return val_str[:-2]
    return val_str

@bp_bodegas.route('/bodegas/upload_excel', methods=['POST'])
@csrf.exempt 
def upload_excel():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    empresa_id = str(session.get('empresa_id'))
    file = request.files.get('file')
    if not file: return jsonify({'error': 'No se recibió archivo'}), 400

    try:
        # 1. MAESTRA
        cur = mysql.connection.cursor()
        cur.execute("SELECT sku, ean, producto, fabricante FROM productos WHERE id_empresa = %s", (empresa_id,))
        db_products = cur.fetchall()
        cur.close()
        maestra_productos = {}
        for row in db_products:
            if row[0]: maestra_productos[normalizar_codigo(row[0])] = {'desc': row[2], 'marca': row[3]}
            if row[1]: maestra_productos[normalizar_codigo(row[1])] = {'desc': row[2], 'marca': row[3]}

        # 2. EXCEL
        filename = file.filename
        df_raw = pd.read_excel(file, header=None)
        
        # FASE 1: METADATOS
        meta_planilla = filename.split('.')[0].replace('_', ' ').strip()
        found_orden = False
        keywords_orden = ['PLANILA', 'PLANILLA', 'REMISION', 'ENTREGA', 'PEDIDO', 'ORDEN', 'DOC']
        max_r = min(20, len(df_raw)); max_c = min(30, len(df_raw.columns))
        
        for r in range(max_r):
            for c in range(max_c):
                val_celda = str(df_raw.iloc[r, c]).upper().strip()
                if any(k in val_celda for k in keywords_orden):
                    candidatos = []
                    for offset in range(1, 8): 
                        if c + offset < len(df_raw.columns):
                            cand = str(df_raw.iloc[r, c + offset]).strip()
                            if cand and cand.upper() != 'NAN': candidatos.append(cand)
                    if r + 1 < len(df_raw):
                        for offset in range(0, 5): 
                            if c + offset < len(df_raw.columns):
                                cand = str(df_raw.iloc[r + 1, c + offset]).strip()
                                if cand and cand.upper() != 'NAN': candidatos.append(cand)
                    for cand in candidatos:
                        cand_clean = cand.replace(' ', '').upper()
                        if cand.upper() in ['NAT', 'NAN', 'NONE', 'NULL']: continue
                        if '-' in cand and any(x.isdigit() for x in cand): continue
                        if empresa_id in cand_clean: continue
                        if len(cand) > 20: continue 
                        if any(k in cand.upper() for k in keywords_orden): continue
                        meta_planilla = cand; found_orden = True; break
                if found_orden: break
            if found_orden: break

        # Zona y Fecha
        raw_head = [str(x).strip().upper() for x in df_raw.head(20).values.flatten() if pd.notna(x)]
        text_dump = " ".join(raw_head)
        meta_zona = 'GENERAL'
        match_zona = re.search(r'(ZONA|RUTA|UBICACION|DESTINO)\s*[:#]?\s*(\w+)', text_dump)
        if match_zona: meta_zona = match_zona.group(2)
        meta_fecha = datetime.now().strftime('%Y-%m-%d')
        match_fecha = re.search(r'(\d{2,4}[-/]\d{2}[-/]\d{2,4})', text_dump)
        if match_fecha: meta_fecha = match_fecha.group(1)

        # FASE 2: TABLA
        start_row = 0; header_map = {}; found_table = False
        keywords_cols = {
            'CODIGO': ['CODIGO', 'EAN', 'ITEM', 'SKU', 'REF', 'MATERIAL', 'ARTICULO'],
            'DESCRIPCION': ['DESCRIPCION', 'PRODUCTO', 'NOMBRE', 'DETALLE', 'TEXTO', 'MATERIAL'],
            'CANTIDAD': ['CANTIDAD', 'CANT', 'UND', 'UNIDADES', 'SEPARAR', 'QTY', 'PEDIDO', 'SOLICITADO']
        }
        for i, row in df_raw.iterrows():
            row_str = [str(val).upper() for val in row.values]
            matches = 0; temp_map = {}
            for col_idx, cell_val in enumerate(row_str):
                for key, words in keywords_cols.items():
                    if any(w in cell_val for w in words):
                        if key not in temp_map: temp_map[key] = col_idx; matches += 1
            if 'CANTIDAD' in temp_map and matches >= 2:
                start_row = i + 1; header_map = temp_map; found_table = True; break
        if not found_table: return jsonify({'error': 'No se detectaron columnas.'}), 400

        # FASE 3: PROCESAMIENTO
        data_to_insert = []
        marca_visual_actual = 'GENERICO'
        fecha_creacion = datetime.now()

        for i in range(start_row, len(df_raw)):
            row = df_raw.iloc[i]
            try:
                idx_cant = header_map.get('CANTIDAD')
                val_cant = row[idx_cant]
                cantidad = float(val_cant) if (pd.notna(val_cant) and str(val_cant).strip()!='') else 0
            except: cantidad = 0

            idx_desc = header_map.get('DESCRIPCION'); idx_code = header_map.get('CODIGO')
            raw_desc = str(row[idx_desc]).strip() if idx_desc is not None and pd.notna(row[idx_desc]) else ""
            raw_code = row[idx_code] if idx_code is not None else ""
            val_desc = raw_desc if raw_desc.upper() != 'NAN' else ""
            val_code = normalizar_codigo(raw_code)

            if cantidad <= 0 and len(val_desc) > 2:
                if "TOTAL" not in val_desc.upper() and "PÁGINA" not in val_desc.upper():
                    marca_visual_actual = val_desc.replace(':', '').strip()
                continue

            if cantidad > 0:
                final_code = val_code if val_code else 'SIN_CODIGO'
                final_desc = val_desc
                final_marca = marca_visual_actual
                if final_code in maestra_productos:
                    prod_db = maestra_productos[final_code]
                    final_desc = prod_db['desc']
                    if prod_db['marca']: final_marca = prod_db['marca']
                if not final_desc: final_desc = f"ITEM SIN NOMBRE ({final_code})"
                data_to_insert.append((empresa_id, meta_planilla, meta_zona, final_code, final_desc, final_marca, cantidad, 0, 'PENDIENTE', fecha_creacion, meta_fecha))

        if not data_to_insert: return jsonify({'error': 'Archivo sin items válidos.'}), 400

        # FASE 4: INSERTAR
        cur = mysql.connection.cursor()
        query = """INSERT INTO picking_importacion_raw (id_empresa, numero_orden_origen, zona, codigo_producto, descripcion_producto, marca, unidades_calculadas, cantidad_alistada, estado_actividad, fecha_creacion_orden, fecha_entrega_orden) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cur.executemany(query, data_to_insert)
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': f'✅ Carga Exitosa: {meta_planilla}', 'detalles': f'Items: {len(data_to_insert)}'})

    except Exception as e:
        print(f"Error Upload: {e}")
        return jsonify({'error': f'Error procesando: {str(e)}'}), 500

# API STATS
@bp_bodegas.route('/api/bodegas/stats')
def bodegas_stats():
    if 'usuario_id' not in session: return jsonify({})
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor()
    cur.execute("SELECT COUNT(DISTINCT numero_orden_origen), SUM(CASE WHEN estado_actividad='PENDIENTE' THEN 1 ELSE 0 END), SUM(CASE WHEN estado_actividad='FINALIZADO' THEN 1 ELSE 0 END) FROM picking_importacion_raw WHERE id_empresa = %s", (empresa_id,))
    row = cur.fetchone()
    cur.close()
    return jsonify({'ordenes_activas': row[0] or 0, 'items_pendientes': int(row[1] or 0), 'items_finalizados': int(row[2] or 0)})