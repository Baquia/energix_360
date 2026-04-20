from flask import Blueprint, render_template, request, jsonify, session, redirect, send_file
import io
from app import mysql, csrf
import pandas as pd
import re
from datetime import datetime, timedelta
import MySQLdb.cursors
import unicodedata
import difflib

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
        'total_items_pendientes': 0, 
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
        # PASO 1: SOLO LAS QUE NO TIENEN PUERTA ASIGNADA
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                MAX(puerta_asignada) as puerta_asignada,
                MAX(zona) as zona,
                COUNT(*) as total_items,
                MAX(fecha_carga) as fecha
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND puerta_asignada IS NULL
            GROUP BY numero_orden_origen
            ORDER BY MAX(fecha_carga) DESC
        """, (empresa_id,))
        ordenes_sin_asignar = cur.fetchall()

        # ==========================================================
        # 2.5 Obtener Lotes por MARCA SIN ASIGNAR (NUEVO)
        # PASO 2: SOLO LAS QUE TIENEN PUERTA, PERO NO TIENEN OPERARIO
        # ==========================================================
        cur.execute("""
            SELECT 
                marca,
                COUNT(DISTINCT numero_orden_origen) as ordenes,
                COUNT(*) as total_items,
                MAX(fecha_carga) as fecha,
                MAX(puerta_asignada) as puerta
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND puerta_asignada IS NOT NULL AND id_auxiliar_asignado IS NULL
            GROUP BY marca
            ORDER BY MAX(fecha_carga) DESC
        """, (empresa_id,))
        marcas_pendientes = cur.fetchall()
        # ==========================================================

        # 3. Obtener Órdenes ASIGNADAS / EN PROCESO / FINALIZADAS / DESPACHADAS
        cur.execute("""
            SELECT 
                numero_orden_origen as orden,
                nombre_auxiliar_asignado as operario,
                MAX(puerta_asignada) as puerta_asignada,
                MAX(id_vehiculo) as id_vehiculo,
                MAX(zona) as zona,
                COUNT(*) as total_items,
                SUM(CASE WHEN estado_actividad IN ('FINALIZADO', 'DESPACHADO') THEN 1 ELSE 0 END) as items_listos,
                MIN(fecha_inicio_alistamiento) as inicio,
                MAX(fecha_fin_alistamiento) as fin
            FROM picking_importacion_raw
            WHERE id_empresa = %s AND estado_actividad IN ('ASIGNADO', 'EN_PROCESO', 'FINALIZADO', 'DESPACHADO')
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
                horas = int(diff.total_seconds()) // 3600
                minutos = (int(diff.total_seconds()) % 3600) // 60
                segundos = int(diff.total_seconds()) % 60
                o['duracion_str'] = f"{horas:02}:{minutos:02}:{segundos:02}"

            # NUEVO: Validar si ya está despachado
            if o.get('id_vehiculo'):
                o['estado_visual'] = 'DESPACHADO'
                finalizados.append(o)
            elif es_finalizado:
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
        # Hacemos JOIN con productos para traer la unidad de embalaje
        cur.execute("""
            SELECT 
                p.id, p.codigo_producto, p.descripcion_producto, p.marca, 
                p.cajas_calculadas, p.cajas_alistadas,
                p.unidades_calculadas, p.unidades_alistadas,
                p.estado_actividad,
                IFNULL(prod.unidad_embalaje, 'UND') as embalaje
            FROM picking_importacion_raw p
            LEFT JOIN productos prod 
                ON (p.codigo_producto = prod.ean OR p.codigo_producto = prod.sku) 
                AND p.id_empresa = prod.id_empresa
            WHERE p.numero_orden_origen = %s AND p.id_empresa = %s
        """, (orden, session.get('empresa_id')))
        items = cur.fetchall()
        cur.close()
        return jsonify(items)
    except Exception as e:
        print(f"Error api items_orden: {e}")
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

# --- 5. CARGA EXCEL (NUEVO MOTOR INTELIGENTE) ---

def normalizar_codigo(valor):
    if pd.isna(valor) or str(valor).strip() == '': return ''
    val_str = str(valor).strip()
    if 'E' in val_str.upper():
        try: return str(int(float(valor)))
        except: pass
    if val_str.endswith('.0'): return val_str[:-2]
    return val_str

def limpiar_texto(texto):
    """ TÚNEL DE LAVADO: Quita tildes, mayúsculas, y espacios dobles """
    if pd.isna(texto) or texto is None: return ""
    texto = str(texto).upper().strip()
    # Elimina tildes y caracteres diacríticos
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    # Comprime múltiples espacios en uno solo
    texto = re.sub(r'\s+', ' ', texto)
    return texto


@bp_bodegas.route('/bodegas/upload_excel', methods=['POST'])
@csrf.exempt 
def upload_excel():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    empresa_id = str(session.get('empresa_id')) # Candado Multi-Tenant
    
    archivos = request.files.getlist('file')
    if not archivos or all(f.filename == '' for f in archivos): 
        return jsonify({'error': 'No se recibieron archivos'}), 400

    try:
        # =====================================================================
        # 1. EL CEREBRO EN MEMORIA (CARGA DE MAESTRA Y DICCIONARIOS INTELIGENTES)
        # =====================================================================
        cur = mysql.connection.cursor()
        
        # A. Cargar Productos Regulares
        cur.execute("SELECT sku, ean, producto, fabricante FROM productos WHERE id_empresa = %s", (empresa_id,))
        db_products = cur.fetchall()
        
        maestra_productos_ean = {}
        maestra_productos_nombre = {}
        mapa_marcas_conocidas = {} 

        for row in db_products:
            sku_val = normalizar_codigo(row[0])
            ean_val = normalizar_codigo(row[1])
            desc_original = str(row[2]).strip() if row[2] else ""
            desc_limpia = limpiar_texto(desc_original)
            marca_original = str(row[3]).strip() if row[3] and str(row[3]).upper() != 'NAN' else "GENERICO"
            marca_limpia = limpiar_texto(marca_original)
            
            item_data = {'desc': desc_original, 'marca': marca_original, 'ean': ean_val or sku_val}

            if ean_val: maestra_productos_ean[ean_val] = item_data
            if sku_val: maestra_productos_ean[sku_val] = item_data
            if desc_limpia: maestra_productos_nombre[desc_limpia] = item_data
            if marca_limpia and marca_limpia != 'GENERICO': 
                mapa_marcas_conocidas[marca_limpia] = marca_original

        lista_marcas_limpias = list(mapa_marcas_conocidas.keys())

        # B. Cargar Promociones (Kits) y crear Memoria de Nombres
        cur.execute("SELECT ean_promo, nombre_promo, ean_componente, cajas_componente, fracciones_componente FROM promociones_clientes WHERE id_empresa = %s AND estado = 'ACTIVO'", (empresa_id,))
        db_promos = cur.fetchall()
        cur.close()
        
        diccionario_promos = {}
        maestra_promos_nombre = {} # NUEVO: Diccionario para adivinar promociones por nombre
        
        for row in db_promos:
            p_padre = normalizar_codigo(row[0]).upper() 
            p_nombre = str(row[1]).strip() if row[1] else "PROMO"
            p_hijo = normalizar_codigo(row[2]).upper()
            p_cajas = int(row[3]) if row[3] is not None else 0
            p_fracc = int(row[4]) if row[4] is not None else 0
            
            if p_padre not in diccionario_promos: 
                diccionario_promos[p_padre] = {'nombre': p_nombre, 'componentes': []}
                # Alimentar el cerebro de nombres de promociones
                nombre_promo_limpio = limpiar_texto(p_nombre)
                if nombre_promo_limpio:
                    maestra_promos_nombre[nombre_promo_limpio] = p_padre
                    
            diccionario_promos[p_padre]['componentes'].append({'ean': p_hijo, 'cajas': p_cajas, 'fracciones': p_fracc})
        # =====================================================================

        resultados_exito = []
        resultados_error = []
        total_items_insertados = 0

        for file in archivos:
            if file.filename == '': continue
            filename = file.filename
            
            try:
                df_raw = pd.read_excel(file, header=None)
                
                # --- METADATOS: Planilla y Zona ---
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

                # --- BUSCAR CABECERAS ---
                start_row = 0; header_map = {}; found_table = False
                keywords_cols = {
                    'CODIGO': ['CODIGO', 'EAN', 'ITEM', 'SKU', 'REF', 'MATERIAL', 'ARTICULO'],
                    'DESCRIPCION': ['DESCRIPCION', 'DESCRIPCIÓN', 'PRODUCTO', 'NOMBRE', 'DETALLE', 'TEXTO', 'MATERIAL', 'ARTICULO'],
                    'CAJAS': ['CAJA', 'CJ', 'BULTOS', 'EMPAQUE'],
                    'UNIDADES': ['UNIDADES', 'CANTIDAD', 'CANT', 'UND', 'FRACCIONES', 'FRACCION', 'SUELTAS']
                }
                
                for i, row in df_raw.iterrows():
                    row_str = [limpiar_texto(val) for val in row.values]
                    matches = 0; temp_map = {}
                    for col_idx, cell_val in enumerate(row_str):
                        for key, words in keywords_cols.items():
                            if any(limpiar_texto(w) in cell_val for w in words):
                                if key not in temp_map: temp_map[key] = col_idx; matches += 1
                    
                    if ('CAJAS' in temp_map or 'UNIDADES' in temp_map) and matches >= 2:
                        start_row = i + 1; header_map = temp_map; found_table = True; break
                        
                if not found_table: 
                    resultados_error.append(f"❌ {filename}: No se detectaron columnas de producto/cantidad.")
                    continue 

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

                    # =========================================================
                    # 💡 MOTOR INTELIGENTE: LECTURA DE MARCA DESALINEADA
                    # =========================================================
                    if cajas <= 0 and unidades <= 0:
                        textos_fila = [str(x) for x in row.values if pd.notna(x) and str(x).strip().upper() not in ['NAN', '']]
                        mejor_match_marca = None
                        texto_mas_largo = ""

                        for txt in textos_fila:
                            txt_limpio = limpiar_texto(txt)
                            if len(txt_limpio) < 3 or any(w in txt_limpio for w in ['TOTAL', 'PAGINA', 'DESCRIPCIO']): continue
                            if len(txt_limpio) > len(texto_mas_largo): texto_mas_largo = txt.replace(':', '').strip()
                            
                            matches = difflib.get_close_matches(txt_limpio, lista_marcas_limpias, n=1, cutoff=0.80)
                            if matches:
                                mejor_match_marca = mapa_marcas_conocidas[matches[0]]
                                break
                        
                        if mejor_match_marca: marca_visual_actual = mejor_match_marca
                        elif texto_mas_largo: marca_visual_actual = texto_mas_largo
                        continue
                    # =========================================================

                    # SI LLEGAMOS AQUÍ, ES UN PRODUCTO CON CANTIDADES
                    if cajas > 0 or unidades > 0:
                        val_desc = raw_desc if raw_desc.upper() != 'NAN' else ""
                        val_code = normalizar_codigo(raw_code)

                        final_code = val_code.upper() if val_code else ''
                        final_desc = val_desc
                        final_marca = marca_visual_actual

                        if not final_code:
                            for celda in row.values:
                                celda_str = normalizar_codigo(celda)
                                if re.match(r'^\d{10,14}$', celda_str):
                                    final_code = celda_str; break

                        es_promo = False

                        # =========================================================
                        # 🧠 EL EMBUDO INTELIGENTE DE RESCATE (NUEVA LÓGICA)
                        # =========================================================
                        desc_limpia = limpiar_texto(final_desc)

                        # INTENTO 1: Búsqueda exacta por Código
                        if final_code in diccionario_promos:
                            es_promo = True
                        elif final_code in maestra_productos_ean:
                            prod_db = maestra_productos_ean[final_code]
                            final_desc = prod_db['desc']
                            final_marca = prod_db['marca']
                        
                        # Si el código falló o vino vacío de Abaco, activamos la IA de rescate
                        elif desc_limpia:
                            # INTENTO 2: Rescate de Promociones (Fuzzy Match 80%)
                            matches_promo = difflib.get_close_matches(desc_limpia, maestra_promos_nombre.keys(), n=1, cutoff=0.80)
                            
                            if matches_promo:
                                # ¡Rescatado! Era una promoción mal codificada por Abaco
                                final_code = maestra_promos_nombre[matches_promo[0]]
                                es_promo = True
                            else:
                                # INTENTO 3: Rescate de Productos Regulares (Fuzzy Match 85%)
                                matches_prod = difflib.get_close_matches(desc_limpia, maestra_productos_nombre.keys(), n=1, cutoff=0.85)
                                
                                if matches_prod:
                                    # ¡Rescatado! Era un producto regular mal codificado
                                    prod_db = maestra_productos_nombre[matches_prod[0]]
                                    final_code = prod_db['ean'] 
                                    final_desc = prod_db['desc'] 
                                    final_marca = prod_db['marca']

                        # INTENTO 4: Fallback (Desastre total en el Excel)
                        if not final_code: final_code = 'SIN_CODIGO'
                        if not final_desc: final_desc = f"ITEM SIN NOMBRE ({final_code})"

                        # =========================================================

                        # --- INSERTAR KITS (PROMOCIONES) ---
                        if es_promo:
                            promo_info = diccionario_promos[final_code]
                            nombre_promo = promo_info['nombre']
                            total_promos_pedidas = cajas + unidades 
                            
                            for comp in promo_info['componentes']:
                                hijo_code = comp['ean']
                                hijo_cajas_total = total_promos_pedidas * comp['cajas'] 
                                hijo_unid_total = total_promos_pedidas * comp['fracciones'] 
                                hijo_desc = f"ITEM SIN NOMBRE ({hijo_code})"
                                hijo_marca = marca_visual_actual
                                
                                if hijo_code in maestra_productos_ean:
                                    hijo_desc = maestra_productos_ean[hijo_code]['desc']
                                    hijo_marca = maestra_productos_ean[hijo_code]['marca']
                                
                                hijo_desc_visual = f"{hijo_desc} (Kit: {nombre_promo})"
                                
                                if hijo_cajas_total > 0 or hijo_unid_total > 0:
                                    data_to_insert.append((empresa_id, meta_planilla, meta_zona, hijo_code, hijo_desc_visual, hijo_marca, hijo_cajas_total, 0, hijo_unid_total, 0, 'PENDIENTE', fecha_creacion, meta_fecha))
                        
                        # --- INSERTAR PRODUCTOS NORMALES ---
                        else:
                            cajas_finales = cajas
                            unidades_finales = unidades
                            if cajas == 0 and unidades > 0:
                                cajas_finales = unidades; unidades_finales = 0

                            data_to_insert.append((empresa_id, meta_planilla, meta_zona, final_code, final_desc, final_marca, cajas_finales, 0, unidades_finales, 0, 'PENDIENTE', fecha_creacion, meta_fecha))

                # INSERTAR EN BASE DE DATOS
                if data_to_insert:
                    cur = mysql.connection.cursor()
                    query = """INSERT INTO picking_importacion_raw (id_empresa, numero_orden_origen, zona, codigo_producto, descripcion_producto, marca, cajas_calculadas, cajas_alistadas, unidades_calculadas, unidades_alistadas, estado_actividad, fecha_creacion_orden, fecha_entrega_orden) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    cur.executemany(query, data_to_insert)
                    mysql.connection.commit()
                    cur.close()
                    
                    resultados_exito.append(f"✅ {meta_planilla} ({len(data_to_insert)} items)")
                    total_items_insertados += len(data_to_insert)
                else:
                    resultados_error.append(f"❌ {filename}: Sin items válidos para insertar.")

            except Exception as e:
                resultados_error.append(f"❌ {filename}: Error de lectura ({str(e)})")

        # REPORTE FINAL
        mensaje_alerta = f"📊 Reporte de Carga:\nArchivos exitosos: {len(resultados_exito)}\nErrores: {len(resultados_error)}\nTotal items generados: {total_items_insertados}\n\n"
        if resultados_exito: mensaje_alerta += "ÓRDENES SUBIDAS:\n" + "\n".join(resultados_exito) + "\n\n"
        if resultados_error: mensaje_alerta += "NO SE PUDIERON SUBIR:\n" + "\n".join(resultados_error)

        return jsonify({'message': mensaje_alerta, 'recargar': len(resultados_exito) > 0})

    except Exception as e:
        print(f"Error Upload General: {e}")
        return jsonify({'error': f'Error crítico procesando carga: {str(e)}'}), 500
    

    
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
    
# --- CREACIÓN MANUAL DE PRODUCTO ---
# --- CREACIÓN MANUAL ---
@bp_bodegas.route('/bodegas/crear_producto_manual', methods=['POST'])
@csrf.exempt
def crear_producto_manual():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    
    data = request.json
    nit_empresa = str(session.get('empresa_id', ''))
    nombre_empresa = str(session.get('nombre_empresa', ''))
    
    try:
        cur = mysql.connection.cursor()
        # Consultamos el tipo oficial para este nombre comercial
        cur.execute("SELECT tipo_empresa FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
        res = cur.fetchone()
        tipo_empresa = res[0] if res else 'general'

        cur.execute("""
            INSERT INTO productos (id_empresa, empresa, tipo_empresa, sku, ean, producto, fabricante, unidad_embalaje)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            producto = VALUES(producto), fabricante = VALUES(fabricante), unidad_embalaje = VALUES(unidad_embalaje)
        """, (nit_empresa, nombre_empresa, tipo_empresa, '', data.get('ean'), data.get('producto'), data.get('fabricante'), data.get('unidad_embalaje', 'UND')))
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': '✅ Producto guardado correctamente.'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@bp_bodegas.route('/bodegas/descargar_plantilla')
def descargar_plantilla_productos():
    if 'usuario_id' not in session: return redirect('/')
    
    # El id_sesion en tu sistema es el NIT (ej: 901240605)
    nit_sesion = str(session.get('empresa_id', ''))
    
    try:
        cur = mysql.connection.cursor()
        # BUSQUEDA POR NIT: Aquí estaba el error, buscábamos por 'id'
        cur.execute("SELECT nombre_comercial, tipo_empresa FROM empresas WHERE nit = %s", (nit_sesion,))
        emp_data = cur.fetchone()
        cur.close()
        
        if emp_data:
            nombre_real = emp_data[0]
            tipo_real = emp_data[1]
        else:
            # Si no lo encuentra por NIT, intentamos por ID por si acaso
            cur = mysql.connection.cursor()
            cur.execute("SELECT nombre_comercial, tipo_empresa FROM empresas WHERE id = %s", (nit_sesion,))
            emp_data = cur.fetchone()
            cur.close()
            nombre_real = emp_data[0] if emp_data else "Empresa no encontrada"
            tipo_real = emp_data[1] if emp_data else "general"
    except Exception as e:
        nombre_real = "Error de Conexión"
        tipo_real = "general"

    # Generamos el Excel con los datos reales encontrados
    df_plantilla = pd.DataFrame([{
        'ID_EMPRESA': nit_sesion,
        'EMPRESA': nombre_real,
        'TIPO_EMPRESA': tipo_real,
        'FABRICANTE': 'MARCA',
        'PRODUCTO': 'DESCRIPCIÓN',
        'EAN': '0000000000000',
        'UNIDAD_EMBALAJE': 'UND',
        'FACTOR_CONVERSION': 1
    }])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_plantilla.to_excel(writer, index=False, sheet_name='Plantilla')
    output.seek(0)

    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, download_name='Plantilla_Productos.xlsx')
    

@bp_bodegas.route('/bodegas/upload_productos_masivo', methods=['POST'])
@csrf.exempt
def upload_productos_masivo():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    file = request.files.get('file')
    if not file: return jsonify({'error': 'No hay archivo'}), 400

    try:
        df = pd.read_excel(file, dtype=str) if file.filename.endswith('.xlsx') else pd.read_csv(file, dtype=str)
        df.columns = df.columns.str.strip().str.upper()

        cur = mysql.connection.cursor()
        # Mapa de empresas: Buscamos tanto por ID como por NIT para no fallar
        cur.execute("SELECT id, nit, nombre_comercial, tipo_empresa FROM empresas")
        rows = cur.fetchall()
        # Creamos un mapa que entienda NIT e ID
        mapa = {}
        for r in rows:
            mapa[str(r[0])] = {'n': r[2], 't': r[3]} # Por ID
            mapa[str(r[1])] = {'n': r[2], 't': r[3]} # Por NIT

        data_to_upsert = []
        for _, row in df.iterrows():
            id_foco = str(row.get('ID_EMPRESA', '')).strip()
            if id_foco in mapa:
                nombre_ok = mapa[id_foco]['n']
                tipo_ok = mapa[id_foco]['t']
                data_to_upsert.append((id_foco, nombre_ok, tipo_ok, '', row.get('EAN'), row.get('PRODUCTO'), row.get('FABRICANTE'), row.get('UNIDAD_EMBALAJE', 'UND')))

        if data_to_upsert:
            cur.executemany("""
                INSERT INTO productos (id_empresa, empresa, tipo_empresa, sku, ean, producto, fabricante, unidad_embalaje)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE empresa=VALUES(empresa), tipo_empresa=VALUES(tipo_empresa), producto=VALUES(producto)
            """, data_to_upsert)
            mysql.connection.commit()
        
        cur.close()
        return jsonify({'message': f'✅ {len(data_to_upsert)} productos procesados.'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
# ==============================================================================
# NUEVAS APIS: DESPACHO, VEHÍCULOS, PUERTAS Y ACTAS DE ENTREGA
# ==============================================================================

@bp_bodegas.route('/bodegas/api/vehiculos')
def get_vehiculos():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Adaptado asumiendo que tu tabla se llama `vehiculos` y tiene campos placa/conductor
        cur.execute("""
            SELECT id, placa, conductor 
            FROM vehiculos 
            WHERE id_empresa = %s
        """, (session.get('empresa_id'),))
        data = cur.fetchall()
        cur.close()
        return jsonify(data)
    except: return jsonify([])

@bp_bodegas.route('/bodegas/asignar_puerta', methods=['POST'])
@csrf.exempt
def asignar_puerta():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor()
        
        # Si asigna por Orden completa
        if 'numero_orden' in d:
            cur.execute("""
                UPDATE picking_importacion_raw 
                SET puerta_asignada=%s
                WHERE numero_orden_origen=%s AND id_empresa=%s
            """, (d['puerta'], d['numero_orden'], session.get('empresa_id')))
        
        # Si asigna por Marca/Lote
        elif 'marca' in d:
            cur.execute("""
                UPDATE picking_importacion_raw 
                SET puerta_asignada=%s
                WHERE marca=%s AND id_empresa=%s AND (estado_actividad = 'PENDIENTE' OR estado_actividad IS NULL)
            """, (d['puerta'], d['marca'], session.get('empresa_id')))
            
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Puerta asignada correctamente.'})
    except Exception as e: return jsonify({'error': str(e)}), 500

@bp_bodegas.route('/bodegas/despachar_orden', methods=['POST'])
@csrf.exempt
def despachar_orden():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión'}), 401
    d = request.json
    try:
        cur = mysql.connection.cursor()
        # Marca toda la orden como despachada, le sella el vehículo, la fecha y quién autorizó
        cur.execute("""
            UPDATE picking_importacion_raw 
            SET estado_actividad='DESPACHADO', id_vehiculo=%s, fecha_despacho=NOW(), id_supervisor_despacho=%s
            WHERE numero_orden_origen=%s AND id_empresa=%s AND estado_actividad='FINALIZADO'
        """, (d['id_vehiculo'], session.get('usuario_id'), d['numero_orden'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'message': 'Orden despachada y acta generada.'})
    except Exception as e: return jsonify({'error': str(e)}), 500

@bp_bodegas.route('/bodegas/imprimir_acta/<orden>')
def imprimir_acta(orden):
    if 'usuario_id' not in session: return redirect('/')
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Cruzamos las tablas de usuarios y vehículos para generar un documento formal
        cur.execute("""
            SELECT 
                p.codigo_producto, p.descripcion_producto, p.cajas_alistadas, p.unidades_alistadas,
                p.nombre_auxiliar_asignado, p.puerta_asignada, p.fecha_despacho,
                v.placa, v.conductor,
                u.nombre as supervisor
            FROM picking_importacion_raw p
            LEFT JOIN vehiculos v ON p.id_vehiculo = v.id
            LEFT JOIN usuarios u ON p.id_supervisor_despacho = u.id
            WHERE p.numero_orden_origen = %s AND p.id_empresa = %s
        """, (orden, session.get('empresa_id')))
        items = cur.fetchall()
        cur.close()
        
        if not items:
            return "Orden no encontrada", 404
            
        head = items[0]
        
        # HTML Formateado para impresión (A4)
        html = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="UTF-8">
            <title>Acta de Entrega - {orden}</title>
            <style>
                body {{ font-family: Arial, sans-serif; padding: 40px; color: #333; }}
                h1, h2, h3 {{ color: #000; text-align: center; margin: 5px 0; }}
                .header-box {{ border: 2px solid #000; padding: 20px; border-radius: 10px; margin-bottom: 30px; }}
                .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-top: 15px; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ border: 1px solid #000; padding: 10px; text-align: left; font-size:14px; }}
                th {{ background: #f0f0f0; }}
                .firmas {{ display: grid; grid-template-columns: 1fr 1fr; gap: 40px; margin-top: 80px; }}
                .firma-box {{ border-top: 2px solid #000; text-align: center; padding-top: 10px; font-size:15px; }}
                .btn-print {{ display: block; margin: 0 auto 30px auto; padding: 15px 30px; background: #004e92; color: white; border: none; border-radius: 8px; font-size: 16px; font-weight: bold; cursor: pointer; }}
                @media print {{
                    .btn-print {{ display: none; }}
                    body {{ padding: 0; }}
                }}
            </style>
        </head>
        <body>
            <button class="btn-print" onclick="window.print()">🖨️ Imprimir Manifiesto</button>
            <div class="header-box">
                <h1>MANIFIESTO DE CARGA Y ENTREGA</h1>
                <h3>ORDEN DE PEDIDO: #{orden}</h3>
                <div class="info-grid">
                    <div><b>🏢 Puerta Asignada:</b> {head.get('puerta_asignada', 'SIN PUERTA')}</div>
                    <div><b>🚚 Placa Vehículo:</b> {head.get('placa', 'No Registrado')}</div>
                    <div><b>👨‍✈️ Conductor:</b> {head.get('conductor', 'No Registrado')}</div>
                    <div><b>📅 Fecha de Despacho:</b> {head.get('fecha_despacho', 'N/A')}</div>
                    <div><b>📋 Supervisado por:</b> {head.get('supervisor', 'N/A')}</div>
                </div>
            </div>
            
            <h3>Detalle de la Mercancía</h3>
            <table>
                <thead>
                    <tr>
                        <th>Código</th>
                        <th>Producto</th>
                        <th>Alistado por</th>
                        <th style="text-align:center;">Cajas</th>
                        <th style="text-align:center;">Unidades</th>
                    </tr>
                </thead>
                <tbody>
        """
        for item in items:
            html += f"""
                    <tr>
                        <td>{item['codigo_producto'] or 'S/C'}</td>
                        <td>{item['descripcion_producto']}</td>
                        <td>{item['nombre_auxiliar_asignado'] or 'N/A'}</td>
                        <td style="text-align:center; font-weight:bold;">{item['cajas_alistadas']}</td>
                        <td style="text-align:center; font-weight:bold;">{item['unidades_alistadas']}</td>
                    </tr>
            """
        html += f"""
                </tbody>
            </table>
            
            <div class="firmas">
                <div class="firma-box">
                    <b>Firma Supervisor</b><br>
                    {head.get('supervisor', 'Firma Responsable')}
                </div>
                <div class="firma-box">
                    <b>Recibí Conforme (Conductor)</b><br>
                    {head.get('conductor', 'Firma Conductor')}
                </div>
            </div>
        </body>
        </html>
        """
        return html
    except Exception as e:
        return str(e), 500
    
from flask import request, jsonify # Asegúrate de tener request importado al inicio de tu archivo si no lo tienes

# ==========================================
# API: EDICIÓN Y MANTENIMIENTO DE PRODUCTOS
# ==========================================

@bp_bodegas.route('/api/bodegas/marcas', methods=['GET'])
def get_marcas():
    """Devuelve la lista de fabricantes, agrupando los vacíos en 'SIN MARCA'"""
    if 'usuario_id' not in session: return jsonify([])
    try:
        empresa_id = session.get('empresa_id')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # El IF transforma los vacíos o NULL en "SIN MARCA"
        cur.execute("""
            SELECT DISTINCT IF(fabricante IS NULL OR fabricante = '', 'SIN MARCA', fabricante) as fabricante 
            FROM productos 
            WHERE id_empresa = %s
            ORDER BY fabricante ASC
        """, (empresa_id,))
        marcas = cur.fetchall()
        cur.close()
        return jsonify(marcas)
    except Exception as e: return jsonify([])
    
@bp_bodegas.route('/api/bodegas/productos_por_marca/<marca>', methods=['GET'])
def get_productos_por_marca(marca):
    if 'usuario_id' not in session: return jsonify([])
    try:
        empresa_id = session.get('empresa_id')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # Si el usuario eligió "SIN MARCA", buscamos los NULL o vacíos
        if marca == 'SIN MARCA':
            cur.execute("""
                SELECT ean, producto, fabricante, unidad_embalaje 
                FROM productos 
                WHERE id_empresa = %s AND (fabricante IS NULL OR fabricante = '')
                ORDER BY producto ASC
            """, (empresa_id,))
        else:
            cur.execute("""
                SELECT ean, producto, fabricante, unidad_embalaje 
                FROM productos 
                WHERE id_empresa = %s AND fabricante = %s 
                ORDER BY producto ASC
            """, (empresa_id, marca))
            
        productos = cur.fetchall()
        cur.close()
        return jsonify(productos)
    except Exception as e: return jsonify([])

@bp_bodegas.route('/api/bodegas/editar_producto', methods=['POST'])
def editar_producto():
    """Recibe los datos corregidos y actualiza la base de datos de forma segura"""
    if 'usuario_id' not in session:
        return jsonify({'status': 'error', 'message': 'Sesión expirada o no válida'})
        
    try:
        empresa_id = session.get('empresa_id')
        data = request.get_json()
        
        ean = data.get('ean')
        nuevo_nombre = data.get('producto')
        nuevo_embalaje = data.get('unidad_embalaje')
        
        if not ean or not nuevo_nombre:
            return jsonify({'status': 'error', 'message': 'El nombre y EAN son obligatorios'})

        cur = mysql.connection.cursor()
        # Candado de seguridad: id_empresa + ean
        cur.execute("""
            UPDATE productos 
            SET producto = %s, unidad_embalaje = %s 
            WHERE ean = %s AND id_empresa = %s
        """, (nuevo_nombre, nuevo_embalaje, ean, empresa_id))
        
        mysql.connection.commit()
        cur.close()
        
        return jsonify({'status': 'success', 'message': 'Producto actualizado correctamente'})
    except Exception as e:
        print(f"Error actualizando producto: {e}")
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error interno del servidor'})
    
# ==========================================
# API: ELIMINACIÓN DE PRODUCTOS Y MARCAS
# ==========================================

@bp_bodegas.route('/api/bodegas/eliminar_producto', methods=['POST'])
def eliminar_producto():
    """Elimina un producto específico por su EAN, validando la empresa"""
    if 'usuario_id' not in session:
        return jsonify({'status': 'error', 'message': 'Sesión expirada o no válida'})
        
    try:
        empresa_id = session.get('empresa_id')
        data = request.get_json()
        
        ean = data.get('ean')
        
        if not ean:
            return jsonify({'status': 'error', 'message': 'El EAN es obligatorio'})

        cur = mysql.connection.cursor()
        
        # CANDADO MULTI-EMPRESA: Solo borra si coincide el EAN y pertenece a su empresa
        cur.execute("""
            DELETE FROM productos 
            WHERE ean = %s AND id_empresa = %s
        """, (ean, empresa_id))
        
        filas_afectadas = cur.rowcount
        mysql.connection.commit()
        cur.close()
        
        if filas_afectadas > 0:
            return jsonify({'status': 'success', 'message': 'Producto eliminado del catálogo maestro'})
        else:
            return jsonify({'status': 'error', 'message': 'No se encontró el producto o no tienes permisos'})
            
    except Exception as e:
        print(f"Error eliminando producto: {e}")
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error interno del servidor'})


@bp_bodegas.route('/api/bodegas/eliminar_marca', methods=['POST'])
def eliminar_marca():
    """Elimina TODOS los productos de un fabricante, validando la empresa"""
    if 'usuario_id' not in session:
        return jsonify({'status': 'error', 'message': 'Sesión expirada o no válida'})
        
    try:
        empresa_id = session.get('empresa_id')
        data = request.get_json()
        
        fabricante = data.get('fabricante')
        
        if not fabricante:
            return jsonify({'status': 'error', 'message': 'La marca es obligatoria'})

        cur = mysql.connection.cursor()
        
        # LÓGICA INTELIGENTE: Evaluar si es una marca normal o los "SIN MARCA" (Vacíos/NULL)
        if fabricante == 'SIN MARCA':
            cur.execute("""
                DELETE FROM productos 
                WHERE (fabricante IS NULL OR fabricante = '') AND id_empresa = %s
            """, (empresa_id,))
        else:
            cur.execute("""
                DELETE FROM productos 
                WHERE fabricante = %s AND id_empresa = %s
            """, (fabricante, empresa_id))
        
        filas_afectadas = cur.rowcount
        mysql.connection.commit()
        cur.close()
        
        if filas_afectadas > 0:
            return jsonify({'status': 'success', 'message': f'Se eliminaron {filas_afectadas} productos de la marca {fabricante}'})
        else:
            return jsonify({'status': 'error', 'message': 'No se encontraron productos para esta marca'})
            
    except Exception as e:
        print(f"Error eliminando marca masivamente: {e}")
        mysql.connection.rollback()
        return jsonify({'status': 'error', 'message': 'Error interno del servidor'})
    
# ==========================================
# API: GESTIÓN DE PROMOCIONES (KITS)
# ==========================================

@bp_bodegas.route('/api/promociones/listar', methods=['GET'])
def listar_promociones():
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Agrupamos por promo para la vista principal de la tabla
        cur.execute("""
            SELECT ean_promo, MAX(nombre_promo) as nombre_promo, MAX(estado) as estado,
                   COUNT(ean_componente) as total_componentes
            FROM promociones_clientes
            WHERE id_empresa = %s
            GROUP BY ean_promo
            ORDER BY created_at DESC
        """, (session.get('empresa_id'),))
        promos = cur.fetchall()
        cur.close()
        return jsonify(promos)
    except Exception as e:
        print(f"Error listando promos: {e}")
        return jsonify([])

@bp_bodegas.route('/api/promociones/detalle/<ean_promo>', methods=['GET'])
def detalle_promocion(ean_promo):
    if 'usuario_id' not in session: return jsonify([])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # Traemos los hijos cruzando con la tabla productos para sacar el nombre
        cur.execute("""
            SELECT p.ean_componente, p.cajas_componente, p.fracciones_componente,
                   IFNULL(m.producto, 'Producto Desconocido') as descripcion_componente,
                   IFNULL(m.fabricante, 'N/A') as marca_componente
            FROM promociones_clientes p
            LEFT JOIN productos m ON (p.ean_componente = m.ean OR p.ean_componente = m.sku) AND p.id_empresa = m.id_empresa
            WHERE p.ean_promo = %s AND p.id_empresa = %s
        """, (ean_promo, session.get('empresa_id')))
        detalle = cur.fetchall()
        cur.close()
        return jsonify(detalle)
    except Exception as e:
        print(f"Error detalle promo: {e}")
        return jsonify([])

@bp_bodegas.route('/api/promociones/guardar', methods=['POST'])
def guardar_promocion():
    if 'usuario_id' not in session: return jsonify({'status': 'error', 'message': 'Sesión expirada'})
    try:
        data = request.json
        empresa_id = session.get('empresa_id')
        nombre_empresa = session.get('nombre_empresa', 'Empresa')
        ean_promo = data.get('ean_promo')
        nombre_promo = data.get('nombre_promo')
        componentes = data.get('componentes', [])

        if not ean_promo or not componentes:
            return jsonify({'status': 'error', 'message': 'Faltan datos requeridos'})

        cur = mysql.connection.cursor()
        
        # Para editar de forma segura, primero borramos la receta vieja y la re-insertamos limpia
        cur.execute("DELETE FROM promociones_clientes WHERE ean_promo = %s AND id_empresa = %s", (ean_promo, empresa_id))
        
        data_to_insert = []
        for comp in componentes:
            data_to_insert.append((
                nombre_empresa, empresa_id, ean_promo, nombre_promo,
                comp['ean'], comp['cajas'], comp['unidades'], 'ACTIVO'
            ))
        
        if data_to_insert:
            cur.executemany("""
                INSERT INTO promociones_clientes 
                (empresa, id_empresa, ean_promo, nombre_promo, ean_componente, cajas_componente, fracciones_componente, estado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, data_to_insert)
        
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success', 'message': 'Promoción guardada exitosamente'})
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error guardando promo: {e}")
        return jsonify({'status': 'error', 'message': 'Error al guardar la promoción'})

@bp_bodegas.route('/api/promociones/estado', methods=['POST'])
def cambiar_estado_promocion():
    if 'usuario_id' not in session: return jsonify({'status': 'error'})
    try:
        data = request.json
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE promociones_clientes SET estado = %s 
            WHERE ean_promo = %s AND id_empresa = %s
        """, (data['estado'], data['ean_promo'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success'})
    except:
        mysql.connection.rollback()
        return jsonify({'status': 'error'})

@bp_bodegas.route('/api/promociones/eliminar', methods=['POST'])
def eliminar_promocion():
    if 'usuario_id' not in session: return jsonify({'status': 'error'})
    try:
        data = request.json
        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM promociones_clientes WHERE ean_promo = %s AND id_empresa = %s", 
                    (data['ean_promo'], session.get('empresa_id')))
        mysql.connection.commit()
        cur.close()
        return jsonify({'status': 'success'})
    except:
        mysql.connection.rollback()
        return jsonify({'status': 'error'})