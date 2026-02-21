from flask import Blueprint, render_template, request, jsonify, session, redirect
from app import mysql, csrf
import pandas as pd
import re
from datetime import datetime, timedelta
import MySQLdb.cursors

bp_bodegas = Blueprint('bodegas', __name__)

# --- 1. VISTA PRINCIPAL (DASHBOARD JEFE) ---

# --- 1. VISTA PRINCIPAL (DASHBOARD JEFE) ---
@bp_bodegas.route('/control_logistica')
def control_logistica():
    if 'usuario_id' not in session: return redirect('/')
    
    empresa_id = session.get('empresa_id')
    
    # A. KPI: Estructura inicial
    kpis = {
        'pedidos_totales': 0, 
        'items_pendientes': 0, 
        'items_finalizados': 0, 
        'pedidos_pendientes_reales': 0 # Nuevo campo calculado
    }
    
    # B. Listas para la Vista
    ordenes_sin_asignar = [] 
    ordenes_procesadas = []
    marcas_pendientes = [] # NUEVA LISTA PARA LOS LOTES POR MARCA

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # 1. Obtener KPIs Generales (Totales históricos)
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

        # 2. Obtener Órdenes SIN ASIGNAR (Para el Modal)
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

        # ==========================================================
        # 2.5 Obtener Lotes por MARCA SIN ASIGNAR (NUEVO)
        # ==========================================================
        cur.execute("""
            SELECT 
                marca,
                COUNT(DISTINCT numero_orden_origen) as ordenes,
                COUNT(*) as items,
                MAX(fecha_carga) as fecha
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL)
            GROUP BY marca
            ORDER BY MAX(fecha_carga) DESC
        """, (empresa_id,))
        marcas_pendientes = cur.fetchall()
        # ==========================================================

        # 3. Obtener Órdenes ASIGNADAS / EN PROCESO / FINALIZADAS
        # Traemos fechas de inicio y fin para cálculos de tiempo
        # SE MODIFICÓ EL GROUP BY Y SE QUITÓ EL MAX() DEL OPERARIO PARA SEPARARLOS
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                nombre_auxiliar_asignado as operario,
                MAX(zona) as zona,
                COUNT(*) as total_items,
                SUM(CASE WHEN estado_actividad='FINALIZADO' THEN 1 ELSE 0 END) as items_listos,
                MIN(fecha_inicio_alistamiento) as inicio,
                MAX(fecha_fin_alistamiento) as fin
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND estado_actividad IN ('ASIGNADO', 'EN_PROCESO', 'FINALIZADO')
            GROUP BY numero_orden_origen, nombre_auxiliar_asignado
        """, (empresa_id,))
        raw_ordenes = cur.fetchall()
        
        activos = []
        finalizados = []
        ahora = datetime.now()

        for o in raw_ordenes:
            # Determinar si la orden está 100% finalizada
            es_finalizado = (o['items_listos'] == o['total_items']) and (o['total_items'] > 0)
            
            # Calcular duración estática (solo para visualización de finalizados)
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
                # REGLA DE 24 HORAS: Solo mostrar si finalizó hace menos de 1 día
                fecha_cierre = o['fin'] or ahora
                if (ahora - fecha_cierre) < timedelta(hours=24):
                    finalizados.append(o)
            else:
                o['estado_visual'] = 'EN_PROCESO'
                activos.append(o)

        # --- CORRECCIÓN QUIRÚRGICA DEL KPI ---
        # La carga pendiente real es: (Lo que nadie ha tomado) + (Lo que se está haciendo)
        kpis['pedidos_pendientes_reales'] = len(ordenes_sin_asignar) + len(activos)

        # ORDENAMIENTO DE LA TABLA
        # Activos: Más recientes primero
        activos.sort(key=lambda x: x['inicio'] or datetime.min, reverse=True)
        # Finalizados: Recién terminados primero
        finalizados.sort(key=lambda x: x['fin'] or datetime.min, reverse=True)

        # Unimos las listas
        ordenes_procesadas = activos + finalizados
        
        cur.close()

    except Exception as e:
        print(f"Error cargando dashboard: {e}")

    # AÑADIMOS marcas_pendientes AL RENDER TEMPLATE
    return render_template('B_control_logistica.html', 
                           kpis=kpis, 
                           ordenes_pendientes=ordenes_sin_asignar, 
                           marcas_pendientes=marcas_pendientes,
                           ordenes_asignadas=ordenes_procesadas)

# --- 2. API: ITEMS DE ORDEN ---
@bp_bodegas.route('/bodegas/api/items_orden/<orden>')
def get_items_orden(orden):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Traemos también 'cantidad_alistada' para la lógica dinámica del frontend
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

# --- 3. API: EMPLEADOS ---
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

# --- 4. API: ASIGNAR ---
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

# --- 5. CARGA EXCEL ---
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
        # --- CARGA DE PRODUCTOS NORMALES ---
        cur = mysql.connection.cursor()
        cur.execute("SELECT sku, ean, producto, fabricante FROM productos WHERE id_empresa = %s", (empresa_id,))
        db_products = cur.fetchall()
        cur.close()
        maestra_productos = {}
        for row in db_products:
            if row[0]: maestra_productos[normalizar_codigo(row[0]).upper()] = {'desc': row[2], 'marca': row[3]}
            if row[1]: maestra_productos[normalizar_codigo(row[1]).upper()] = {'desc': row[2], 'marca': row[3]}

        # =====================================================================
        # 👇 FASE 2: RECETAS DE PROMOCIONES (CAJAS Y FRACCIONES INTERNAS) 👇
        # =====================================================================
        cur = mysql.connection.cursor() 
        # AQUI ESTA EL CAMBIO: Ahora leemos cajas_componente y fracciones_componente
        cur.execute("SELECT ean_promo, nombre_promo, ean_componente, cajas_componente, fracciones_componente FROM promociones_clientes WHERE id_empresa = %s", (empresa_id,))
        db_promos = cur.fetchall()
        cur.close()
        
        diccionario_promos = {}
        for row in db_promos:
            p_padre = normalizar_codigo(row[0]).upper() 
            p_nombre = str(row[1]).strip() if row[1] else "PROMO"
            p_hijo = normalizar_codigo(row[2]).upper()
            p_cajas = int(row[3]) if row[3] is not None else 0
            p_fracc = int(row[4]) if row[4] is not None else 0
            
            if p_padre not in diccionario_promos:
                diccionario_promos[p_padre] = {'nombre': p_nombre, 'componentes': []}
            
            diccionario_promos[p_padre]['componentes'].append({'ean': p_hijo, 'cajas': p_cajas, 'fracciones': p_fracc})
        # =====================================================================

        filename = file.filename
        df_raw = pd.read_excel(file, header=None)
        
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

        raw_head = [str(x).strip().upper() for x in df_raw.head(20).values.flatten() if pd.notna(x)]
        text_dump = " ".join(raw_head)
        meta_zona = 'GENERAL'
        match_zona = re.search(r'(ZONA|RUTA|UBICACION|DESTINO)\s*[:#]?\s*(\w+)', text_dump)
        if match_zona: meta_zona = match_zona.group(2)
        meta_fecha = datetime.now().strftime('%Y-%m-%d')
        match_fecha = re.search(r'(\d{2,4}[-/]\d{2}[-/]\d{2,4})', text_dump)
        if match_fecha: meta_fecha = match_fecha.group(1)

        start_row = 0; header_map = {}; found_table = False
        
        keywords_cols = {
            'CODIGO': ['CODIGO', 'EAN', 'ITEM', 'SKU', 'REF', 'MATERIAL', 'ARTICULO'],
            'DESCRIPCION': ['DESCRIPCION', 'PRODUCTO', 'NOMBRE', 'DETALLE', 'TEXTO', 'MATERIAL'],
            'CAJAS': ['CAJA', 'CJ', 'BULTOS', 'EMPAQUE'],
            'UNIDADES': ['UNIDADES', 'CANTIDAD', 'CANT', 'UND', 'FRACCIONES', 'FRACCION', 'SUELTAS']
        }
        
        for i, row in df_raw.iterrows():
            row_str = [str(val).upper() for val in row.values]
            matches = 0; temp_map = {}
            for col_idx, cell_val in enumerate(row_str):
                for key, words in keywords_cols.items():
                    if any(w in cell_val for w in words):
                        if key not in temp_map: temp_map[key] = col_idx; matches += 1
            
            if ('CAJAS' in temp_map or 'UNIDADES' in temp_map) and matches >= 2:
                start_row = i + 1; header_map = temp_map; found_table = True; break
                
        if not found_table: return jsonify({'error': 'No se detectaron columnas.'}), 400

        data_to_insert = []
        marca_visual_actual = 'GENERICO'
        fecha_creacion = datetime.now()

        for i in range(start_row, len(df_raw)):
            row = df_raw.iloc[i]
            
            try:
                idx_cajas = header_map.get('CAJAS')
                val_cajas = row[idx_cajas] if idx_cajas is not None else 0
                cajas = int(float(val_cajas)) if pd.notna(val_cajas) and str(val_cajas).strip()!='' else 0
            except: cajas = 0

            try:
                idx_unid = header_map.get('UNIDADES')
                val_unid = row[idx_unid] if idx_unid is not None else 0
                unidades = int(float(val_unid)) if pd.notna(val_unid) and str(val_unid).strip()!='' else 0
            except: unidades = 0

            idx_desc = header_map.get('DESCRIPCION'); idx_code = header_map.get('CODIGO')
            raw_desc = str(row[idx_desc]).strip() if idx_desc is not None and pd.notna(row[idx_desc]) else ""
            raw_code = row[idx_code] if idx_code is not None else ""
            val_desc = raw_desc if raw_desc.upper() != 'NAN' else ""
            val_code = normalizar_codigo(raw_code)

            if cajas <= 0 and unidades <= 0 and len(val_desc) > 2:
                if "TOTAL" not in val_desc.upper() and "PÁGINA" not in val_desc.upper():
                    marca_visual_actual = val_desc.replace(':', '').strip()
                continue

            # =====================================================================
            # 👇 FASE 3: LA MATEMÁTICA CORRECTA (MULTIPLICADOR ENTERO) 👇
            # =====================================================================
            if cajas > 0 or unidades > 0:
                final_code = val_code.upper() if val_code else 'SIN_CODIGO'
                
                # --- CAMINO A: ES UNA PROMOCIÓN ---
                if final_code in diccionario_promos:
                    promo_info = diccionario_promos[final_code]
                    nombre_promo = promo_info['nombre']
                    
                    # EL GRAN CAMBIO: Sumamos todo lo que el cliente pidió en el Excel (Ej: 3 Promociones)
                    total_promos_pedidas = cajas + unidades 
                    
                    for comp in promo_info['componentes']:
                        hijo_code = comp['ean']
                        
                        # MULTIPLICACIÓN EXACTA (Receta BD * Cantidad de Excel)
                        hijo_cajas_total = total_promos_pedidas * comp['cajas'] 
                        hijo_unid_total = total_promos_pedidas * comp['fracciones'] 
                        
                        hijo_desc = f"ITEM SIN NOMBRE ({hijo_code})"
                        hijo_marca = marca_visual_actual
                        
                        if hijo_code in maestra_productos:
                            hijo_desc = maestra_productos[hijo_code]['desc']
                            if maestra_productos[hijo_code]['marca']: 
                                hijo_marca = maestra_productos[hijo_code]['marca']
                        
                        hijo_desc_visual = f"{hijo_desc} (Kit: {nombre_promo})"
                        
                        # Si da más de 0, lo guardamos en la base de datos
                        if hijo_cajas_total > 0 or hijo_unid_total > 0:
                            data_to_insert.append((empresa_id, meta_planilla, meta_zona, hijo_code, hijo_desc_visual, hijo_marca, hijo_cajas_total, 0, hijo_unid_total, 0, 'PENDIENTE', fecha_creacion, meta_fecha))
                
                # --- CAMINO B: ES UN PRODUCTO NORMAL ---
                else:
                    final_desc = val_desc
                    final_marca = marca_visual_actual
                    if final_code in maestra_productos:
                        prod_db = maestra_productos[final_code]
                        final_desc = prod_db['desc']
                        if prod_db['marca']: final_marca = prod_db['marca']
                    if not final_desc: final_desc = f"ITEM SIN NOMBRE ({final_code})"
                    
                    # 👇 CORRECCIÓN EXPERTA PARA ASEGURAR CAJAS EN PRODUCTOS NORMALES 👇
                    cajas_finales = cajas
                    unidades_finales = unidades
                    
                    # Si el Excel trajo la cantidad bajo la columna "CANTIDAD" (unidades)
                    # y 0 en CAJAS, movemos ese valor a cajas_finales.
                    if cajas == 0 and unidades > 0:
                        cajas_finales = unidades
                        unidades_finales = 0

                    data_to_insert.append((empresa_id, meta_planilla, meta_zona, final_code, final_desc, final_marca, cajas_finales, 0, unidades_finales, 0, 'PENDIENTE', fecha_creacion, meta_fecha))
            # =====================================================================

        if not data_to_insert: return jsonify({'error': 'Archivo sin items válidos o con formatos de cantidad no reconocidos.'}), 400

        cur = mysql.connection.cursor()
        query = """INSERT INTO picking_importacion_raw (id_empresa, numero_orden_origen, zona, codigo_producto, descripcion_producto, marca, cajas_calculadas, cajas_alistadas, unidades_calculadas, unidades_alistadas, estado_actividad, fecha_creacion_orden, fecha_entrega_orden) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        cur.executemany(query, data_to_insert)
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': f'✅ Carga Exitosa: {meta_planilla}', 'detalles': f'Items generados: {len(data_to_insert)}'})

    except Exception as e:
        print(f"Error Upload: {e}")
        return jsonify({'error': f'Error procesando: {str(e)}'}), 500


@bp_bodegas.route('/api/bodegas/stats')
def bodegas_stats():
    if 'usuario_id' not in session: return jsonify({})
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor()
    cur.execute("SELECT COUNT(DISTINCT numero_orden_origen), SUM(CASE WHEN estado_actividad='PENDIENTE' THEN 1 ELSE 0 END), SUM(CASE WHEN estado_actividad='FINALIZADO' THEN 1 ELSE 0 END) FROM picking_importacion_raw WHERE id_empresa = %s", (empresa_id,))
    row = cur.fetchone()
    cur.close()
    return jsonify({'ordenes_activas': row[0] or 0, 'items_pendientes': int(row[1] or 0), 'items_finalizados': int(row[2] or 0)})

# --- API: ASIGNAR POR MARCA ---
@bp_bodegas.route('/bodegas/asignar_marca', methods=['POST'])
@csrf.exempt
def asignar_marca():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor()
        # Se agrega TRIM() para evitar fallos por espacios en blanco invisibles
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET id_auxiliar_asignado=%s, nombre_auxiliar_asignado=%s, estado_actividad='ASIGNADO', fecha_inicio_alistamiento=NOW()
            WHERE TRIM(marca)=TRIM(%s) AND id_empresa=%s AND (estado_actividad='PENDIENTE' OR estado_actividad IS NULL)
        """, (d['id_operario'], d['nombre_operario'], d['marca'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Lote de Marca asignado correctamente.'})
    except Exception as e: return jsonify({'error': str(e)}), 500