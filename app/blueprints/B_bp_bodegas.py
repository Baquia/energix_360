from flask import Blueprint, render_template, session, redirect, request, jsonify
from app import mysql, csrf
import pandas as pd
import MySQLdb.cursors
from werkzeug.utils import secure_filename

bp_bodegas = Blueprint('bodegas', __name__)

# --- RUTAS DE NAVEGACIÓN ---
@bp_bodegas.route('/bodegas') 
def home():
    nit = str(session.get('empresa_id', ''))
    nombre = session.get('nombre', '')
    empresa = session.get('empresa', '')
    return render_template('B_bodegas.html', nit=nit, nombre=nombre, empresa=empresa)

@bp_bodegas.route('/bodegas/control')
def dashboard_control():
    nit = str(session.get('empresa_id', ''))
    nombre = session.get('nombre', '')
    empresa = session.get('empresa', '')
    kpis = { "pedidos_totales": 0, "pedidos_pendientes": 0, "operarios_activos": 0, "eficiencia_global": "0%" }
    operarios = []
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        # 1. KPIs
        query_kpi = "SELECT COUNT(DISTINCT numero_orden_origen) as total_ordenes, COUNT(CASE WHEN estado_actividad = 'PENDIENTE' THEN 1 END) as items_pendientes, COUNT(DISTINCT id_auxiliar_asignado) as total_operarios FROM picking_importacion_raw WHERE id_empresa = %s"
        cur.execute(query_kpi, (nit,))
        data_kpi = cur.fetchone()
        if data_kpi:
            kpis["pedidos_totales"] = data_kpi['total_ordenes']
            kpis["pedidos_pendientes"] = data_kpi['items_pendientes'] 
            kpis["operarios_activos"] = data_kpi['total_operarios']
            total_items = cur.execute("SELECT COUNT(*) FROM picking_importacion_raw WHERE id_empresa = %s", (nit,))
            if total_items > 0:
                picked = total_items - kpis["pedidos_pendientes"]
                eficiencia = (picked / total_items) * 100
                kpis["eficiencia_global"] = f"{int(eficiencia)}%"
        # 2. TABLA
        query_ops = "SELECT nombre_auxiliar_asignado as nombre, MAX(estado_actividad) as estado, COUNT(DISTINCT numero_orden_origen) as ordenes_asignadas, SUM(unidades_calculadas) as total_unidades, MAX(numero_orden_origen) as orden_actual FROM picking_importacion_raw WHERE id_empresa = %s AND nombre_auxiliar_asignado IS NOT NULL GROUP BY nombre_auxiliar_asignado"
        cur.execute(query_ops, (nit,))
        raw_operarios = cur.fetchall()
        for op in raw_operarios:
            operarios.append({ "nombre": op['nombre'], "estado": op['estado'] if op['estado'] else "Asignado", "orden": op['orden_actual'], "items_hora": int(op['total_unidades'] / 8), "avance": 0 })
        cur.close()
    except Exception as e: print(f"Error Dashboard: {e}")
    return render_template('B_control_logistica.html', nit=nit, nombre=nombre, empresa=empresa, kpis=kpis, operarios=operarios)


# --- LÓGICA DE CARGA CALIBRADA ---
@bp_bodegas.route('/bodegas/upload_excel', methods=['POST'])
@csrf.exempt 
def upload_excel():
    if 'usuario_id' not in session: return jsonify({'error': 'Sesión expirada'}), 401
    file = request.files.get('file')
    if not file: return jsonify({'error': 'No se recibió archivo'}), 400
    filename = secure_filename(file.filename)
    
    try:
        # 1. LEER EXCEL
        df_raw = pd.read_excel(file, header=None)
        
        # Variables Metadata
        meta_orden = "SIN_ORDEN"
        meta_zona = None
        meta_creacion = None
        meta_entrega = None

        # --- MOTOR DE BÚSQUEDA ---
        for r_idx, row in df_raw.head(20).iterrows():
            for c_idx, cell_value in enumerate(row):
                txt = str(cell_value).upper().strip() 
                
                # CASO 1: PLANILLA (Abajo 1)
                if "PLANILA" in txt or "PLANILLA" in txt:
                    try:
                        val = str(df_raw.iloc[r_idx + 1, c_idx]).strip()
                        if val.lower() not in ['nan', 'nat', 'none', '']: meta_orden = val
                    except: pass
                
                # CASO 2: ZONA (Derecha +3) -> ¡AJUSTE CRÍTICO AQUÍ!
                if "OBSERVACI" in txt:
                    try:
                        # Cambiado de +2 a +3 porque hay columnas vacías intermedias
                        val = str(df_raw.iloc[r_idx, c_idx + 3]).strip()
                        if val.lower() not in ['nan', 'nat', 'none', '']: meta_zona = val
                    except: pass

                # CASO 3: CREACION (Derecha +4)
                if "CREACI" in txt or "GENERADO" in txt:
                    try:
                        val = str(df_raw.iloc[r_idx, c_idx + 4]).strip()
                        if val.lower() not in ['nan', 'nat', 'none', '']: meta_creacion = val
                    except: pass

                # CASO 4: ENTREGA (Derecha +4)
                if "ENTREGA" in txt or "DESPACHO" in txt:
                    try:
                        val = str(df_raw.iloc[r_idx, c_idx + 4]).strip()
                        if val.lower() not in ['nan', 'nat', 'none', '']: meta_entrega = val
                    except: pass

        # --- BÚSQUEDA DE ENCABEZADO ---
        header_row_index = -1
        for i, row in df_raw.head(25).iterrows():
            row_text = [str(v).lower() for v in row.values]
            has_prod = any('código' in x or 'codigo' in x for x in row_text)
            has_desc = any('descripción' in x or 'descripcion' in x for x in row_text)
            if has_prod and has_desc:
                header_row_index = i
                break
        
        if header_row_index == -1: return jsonify({'error': 'No encontré fila de títulos (Código/Descripción).'}), 400

        # --- PROCESAR TABLA ---
        df = df_raw.iloc[header_row_index + 1:].copy()
        df.columns = df_raw.iloc[header_row_index]
        df.columns = [str(c).strip().lower() for c in df.columns]

        col_map = {
            'codigo_producto': ['código', 'codigo', 'item'],
            'descripcion_producto': ['descripción', 'descripcion', 'nombre'],
            'unidades_calculadas': ['unids', 'unidades', 'cant']
        }

        df_final = pd.DataFrame()
        for campo_bd, posibles_nombres in col_map.items():
            found = False
            for nombre in posibles_nombres:
                matches = [c for c in df.columns if nombre in str(c)]
                if matches:
                    df_final[campo_bd] = df[matches[0]]
                    found = True; break
            if not found: df_final[campo_bd] = None

        empresa_id = session.get('empresa_id')
        cur = mysql.connection.cursor()
        filas_insertadas = 0
        
        for index, row in df_final.iterrows():
            if pd.isna(row.get('codigo_producto')): continue
            val_codigo = str(row['codigo_producto']).strip()
            if val_codigo.lower() in ['código', 'codigo', 'cambios', 'nan', 'nat']: continue
            try: val_unidades = int(row['unidades_calculadas'])
            except: val_unidades = 0
            if val_unidades == 0: continue 

            query = """
                INSERT INTO picking_importacion_raw 
                (id_empresa, nombre_archivo, numero_orden_origen, codigo_producto, 
                 descripcion_producto, unidades_calculadas, nombre_auxiliar_asignado, 
                 bodega_detectada, zona, fecha_creacion_orden, fecha_entrega_orden, 
                 placa_vehiculo, nombre_conductor, estado_actividad)
                VALUES (%s, %s, %s, %s, %s, %s, NULL, 'General', %s, %s, %s, NULL, NULL, 'PENDIENTE')
            """
            cur.execute(query, (
                empresa_id, filename, meta_orden, val_codigo,
                str(row['descripcion_producto']) if pd.notna(row['descripcion_producto']) else '',
                val_unidades, meta_zona, meta_creacion, meta_entrega
            ))
            filas_insertadas += 1

        mysql.connection.commit()
        cur.close()
        
        msg = f"Carga Exitosa. Orden: {meta_orden} | Zona: {meta_zona} | Creación: {meta_creacion} | Entrega: {meta_entrega}"
        return jsonify({'message': msg, 'rows': filas_insertadas})

    except Exception as e:
        print(f"Error Excel: {e}")
        return jsonify({'error': f'Error interno: {str(e)}'}), 500