# bp_901811727_energia_glp.py
from flask import Blueprint, render_template, session, request, jsonify, flash, redirect, url_for, current_app, send_file
from datetime import datetime
from app import mysql, csrf
from app.utils import login_required_custom
import traceback
import math
import io
import json
import qrcode
import MySQLdb
import textwrap
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.utils import ImageReader

bp_energia_glp = Blueprint('bp_energia_glp', __name__)

# ==============================================================================
# CONFIGURACIÓN SMTP Y ALERTAS DE SEGURIDAD
# ==============================================================================

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = 587
EMAIL_USER = os.environ.get("EMAIL_USER", "tu_email@ejemplo.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "tu_password")
EMAIL_FROM = EMAIL_USER

def _enviar_alerta_gerencia(empresa_id, empresa_nombre, datos, archivos):
    try:
        e_id = int(empresa_id) if empresa_id else 0
    except:
        e_id = 0
        
    e_nombre = str(empresa_nombre).strip() if empresa_nombre else ""

    cur = mysql.connection.cursor()
    
    query = """
        SELECT email 
        FROM contactos 
        WHERE (id_empresa = %s OR TRIM(empresa) = %s) 
          AND area_contacto = 'gerenciagranjas' 
        LIMIT 1
    """
    cur.execute(query, (e_id, e_nombre))
    row = cur.fetchone()
    cur.close()

    destinatario = None
    if row:
        if isinstance(row, dict):
            destinatario = row.get('email')
        else:
            destinatario = row[0]

    if not destinatario:
        print(f"❌ ERROR: No se encontró email 'gerenciagranjas'.")
        return False

    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = destinatario
    msg['Subject'] = f"🚨 ALERTA: Irregularidad en Tanqueo - {datos['ubicacion']}"

    cuerpo = f"""
    <h3>Reporte de Irregularidad en Tanqueo</h3>
    <p>Se ha detectado una inconsistencia en la información validada por el tanqueo.</p>
    <ul>
        <li><strong>Fecha:</strong> {datos['fecha']}</li>
        <li><strong>Ubicación:</strong> {datos['ubicacion']}</li>
        <li><strong>Validado por:</strong> {session.get('nombre', 'Usuario Sistema')}</li>
        <li><strong>Factura/Lote Ref:</strong> {datos['lote']}</li>
    </ul>
    <p style="color:red; font-weight:bold;">
        Por favor contacte al responsable inmediato para que rinda explicaciones sobre las diferencias evidenciadas en las fotografías adjuntas.
    </p>
    <p style="font-size:0.8rem; color:#666;">Sistema BQA ONE - Gas Avícola</p>
    """
    msg.attach(MIMEText(cuerpo, 'html'))

    base_dir = current_app.static_folder
    if archivos:
        for ruta in archivos:
            if ruta:
                clean_path = ruta.replace('/static/', '').replace('\\', '/')
                full_path = os.path.join(base_dir, clean_path)
                
                if os.path.exists(full_path):
                    try:
                        with open(full_path, "rb") as f:
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(full_path)}")
                        msg.attach(part)
                    except Exception as e:
                        print(f"⚠️ Error adjuntando archivo {full_path}: {e}")

    try:
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_FROM, destinatario, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"⛔ Error crítico enviando email SMTP: {e}")
        return False        

def _borrar_evidencias_tanqueo(rutas):
    base_dir = current_app.static_folder 
    for rel_path in rutas:
        if rel_path:
            clean_name = rel_path.lstrip('/')
            if clean_name.startswith('static/'):
                clean_name = clean_name.replace('static/', '', 1)
            full_path = os.path.join(base_dir, clean_name)
            if os.path.exists(full_path):
                try:
                    os.remove(full_path)
                except Exception as e:
                    print(f"⚠️ Error borrando {full_path}: {e}")

# ==============================================================================
# VALIDACIÓN Y AUDITORÍA DE TANQUEOS
# ==============================================================================

@csrf.exempt
@bp_energia_glp.route('/obtener_tanqueos_validacion', methods=['POST'])
@login_required_custom
def obtener_tanqueos_validacion():
    def formatear_ruta(ruta):
        if not ruta: return None
        ruta = ruta.strip().replace('\\', '/')
        if ruta.startswith('/'):
            ruta = ruta[1:]
        if ruta.startswith('static/'):
            ruta = ruta.replace('static/', '', 1)
        return f"/static/{ruta}"

    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id: 
        return jsonify(success=False, message="ID Empresa requerido")

    try:
        cur = mysql.connection.cursor()
        sql = """
            SELECT * FROM cardex_glp 
            WHERE id_empresa = %s 
              AND clase = 'ingreso' 
              AND (estatus_validacion IS NULL OR estatus_validacion = 'pendiente')
            ORDER BY fecha DESC
        """
        cur.execute(sql, (empresa_id,))
        rows = cur.fetchall()
        
        column_names = [d[0] for d in cur.description] if cur.description else []
        items = []
        
        for row in rows:
            if isinstance(row, dict):
                r_dict = row
            else:
                r_dict = dict(zip(column_names, row))
            
            tanques_activos = []
            
            for i in range(1, 12):
                k_nivel_antes = f'nivel tk-{i}'            
                k_nivel_desp  = f'nivelfinal tk-{i}'       
                k_foto_antes  = f'testigo nivel tk-{i}'    
                k_foto_desp   = f'testigo nivelfinal tk-{i}'
                k_foto_voucher = f'testigo_baucher_tk_{i}'

                val_antes = r_dict.get(k_nivel_antes)
                val_desp = r_dict.get(k_nivel_desp)
                path_antes = r_dict.get(k_foto_antes)
                path_desp = r_dict.get(k_foto_desp)
                path_voucher = r_dict.get(k_foto_voucher)

                tiene_valor = (val_antes is not None) or (val_desp is not None)
                tiene_foto = bool(path_antes or path_desp or path_voucher)
                
                if tiene_valor or tiene_foto:
                    tanques_activos.append({
                        'numero': i,
                        'pct_antes': val_antes if val_antes is not None else '-',
                        'pct_despues': val_desp if val_desp is not None else '-',
                        'foto_antes': formatear_ruta(path_antes), 
                        'foto_despues': formatear_ruta(path_desp),
                        'foto_voucher': formatear_ruta(path_voucher)
                    })

            if tanques_activos:
                items.append({
                    'id': r_dict.get('id'),
                    'fecha': str(r_dict.get('fecha')),
                    'ubicacion': r_dict.get('ubicacion') or 'Sin Ubicación',
                    'lote': r_dict.get('lote') or 'Sin Lote',
                    'empresa': r_dict.get('empresa'),
                    'usuario': r_dict.get('registro') or 'Sistema',
                    'subfilas': tanques_activos
                })

        cur.close()
        return jsonify(success=True, items=items)

    except Exception as e:
        print("Error validacion:", traceback.format_exc())
        return jsonify(success=False, message=str(e))

@csrf.exempt
@bp_energia_glp.route('/procesar_validacion_tanqueo', methods=['POST'])
@login_required_custom
def procesar_validacion_tanqueo():
    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'Datos no recibidos'}), 400

    id_registro = data.get('id')
    decision = data.get('decision') 

    if not id_registro:
         return jsonify({'status': 'error', 'message': 'ID no proporcionado'})

    cur = mysql.connection.cursor()
    cur.execute("SELECT * FROM cardex_glp WHERE id = %s", (id_registro,))
    row = cur.fetchone()
    
    if not row:
        cur.close()
        return jsonify({'status': 'error', 'message': 'Registro no encontrado'})

    if isinstance(row, dict):
        row_dict = row
    else:
        columns = [desc[0] for desc in cur.description]
        row_dict = dict(zip(columns, row))

    empresa_id = row_dict.get('id_empresa')
    empresa_nombre = row_dict.get('empresa')
    ubicacion = row_dict.get('ubicacion')

    archivos_adjuntos = []
    
    for key, value in row_dict.items():
        if value and isinstance(value, str) and 'testigo' in key.lower():
            archivos_adjuntos.append(value.strip())

    msg = ""

    if decision == 'NO': 
        datos_alerta = {
            'ubicacion': ubicacion,
            'fecha': str(row_dict.get('fecha')),
            'lote': str(row_dict.get('lote'))
        }
        
        enviado = _enviar_alerta_gerencia(empresa_id, empresa_nombre, datos_alerta, archivos_adjuntos)
        
        if enviado:
            _borrar_evidencias_tanqueo(archivos_adjuntos)
            
            cur.execute("DELETE FROM cardex_glp WHERE id = %s", (id_registro,))
            mysql.connection.commit()
            msg = "Registro rechazado. Se envió alerta a Gerencia y se eliminaron las evidencias."
        else:
            cur.close()
            return jsonify({'success': False, 'message': 'Fallo el envío de correo. No se eliminó el registro por seguridad.'})
            
    elif decision == 'SI':
        cur.execute("""
            UPDATE cardex_glp 
            SET estatus_validacion = 'validado', 
                fecha_validacion = NOW(), 
                validador_id = %s 
            WHERE id = %s
        """, (session.get('nombre'), id_registro))
        mysql.connection.commit()
        msg = "Registro validado correctamente."

    cur.close()
    return jsonify({'success': True, 'message': msg})

# ==============================================================================
# LÓGICA DE INFORMES Y ESTADÍSTICAS GLP (KPIs y POBLACIÓN)
# ==============================================================================

def _procesar_resultados_glp(resultados, tipo_informe, periodo, mapa_poblacion_rescatada):
    if not resultados:
        return None

    def safe_float(val):
        if val is None: return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    math_saldo_inicial_kg_global = 0.0
    math_ingresos_kg = 0.0
    math_consumo_real_acumulado = 0.0
    math_dinero_total = 0.0 

    series = { 
        'fechas': [], 'kg_pollito': [], 'velocidad_consumo': [],  
        'saldo_inicial': [], 'saldo_final': [], 'ingresos': [] 
    }
    
    granjas_data = {}
    lotes_info = {} 

    resultados.sort(key=lambda x: str(x.get('fecha')))

    for row in resultados:
        lote_id = row.get('lote')
        pollitos_reales = mapa_poblacion_rescatada.get(lote_id, 0) if lote_id else 0

        if pollitos_reales <= 0:
            continue

        fecha_str = str(row.get('fecha'))
        clase = str(row.get('clase') or '').lower().strip()
        ubicacion = row.get('ubicacion') or 'Desconocida'

        val_kg_saldo = safe_float(row.get('saldo_estimado_kg'))
        masa_fact = safe_float(row.get('masa_kg_facturada'))
        neto_gast = safe_float(row.get('neto_gastado'))
        
        if clase in ['egreso', 'consumo'] and neto_gast <= 0:
            continue

        if lote_id not in lotes_info:
            lotes_info[lote_id] = {
                'estatus': str(row.get('estatus_lote', '')).upper(),
                'consumo': 0.0,
                'pollitos': pollitos_reales,
                'fecha_fin': fecha_str[:10],
                'ultimo_saldo': 0.0
            }
            
        if str(row.get('estatus_lote', '')).upper() == 'INACTIVO':
            lotes_info[lote_id]['estatus'] = 'INACTIVO'
            
        if clase in ['egreso', 'consumo'] and neto_gast > 0:
            lotes_info[lote_id]['consumo'] += neto_gast
            
        if fecha_str[:10] >= lotes_info[lote_id]['fecha_fin']:
            lotes_info[lote_id]['fecha_fin'] = fecha_str[:10]
            if val_kg_saldo > 0 or clase == 'saldo final':
                lotes_info[lote_id]['ultimo_saldo'] = val_kg_saldo

        val_precio = safe_float(row.get('precio_total')) 
        kg_pollo  = safe_float(row.get('kg_pollito'))
        vel_consumo = safe_float(row.get('velocidad_consumo'))
        
        math_consumo_real_acumulado += neto_gast
        math_dinero_total += val_precio

        if clase == 'saldo inicial': math_saldo_inicial_kg_global += val_kg_saldo
        if clase == 'ingreso': math_ingresos_kg += masa_fact

        series['fechas'].append(fecha_str)
        series['kg_pollito'].append(kg_pollo)
        series['velocidad_consumo'].append(vel_consumo) 
        series['saldo_inicial'].append(val_kg_saldo if clase == 'saldo inicial' else None)
        series['saldo_final'].append(val_kg_saldo if clase == 'saldo final' else None)
        series['ingresos'].append(masa_fact if clase == 'ingreso' else None)

        if ubicacion not in granjas_data:
            granjas_data[ubicacion] = {
                'inicial': 0.0, 'ingresos': 0.0, 'consumo_real': 0.0,
                'pollitos': 0, 'lotes': set()
            }
        
        d = granjas_data[ubicacion]
        d['consumo_real'] += neto_gast
        if clase == 'saldo inicial': d['inicial'] += val_kg_saldo
        elif clase == 'ingreso': d['ingresos'] += masa_fact
        
        if lote_id and lote_id not in d['lotes']:
            if pollitos_reales > 0: d['pollitos'] += pollitos_reales
            d['lotes'].add(lote_id)

    grupos = {}
    from datetime import datetime as dt
    meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    
    for l_id, data in lotes_info.items():
        es_activo = (data['estatus'] == 'ACTIVO')
        if tipo_informe in ['general', 'zona']:
            if es_activo:
                key = "ACTIVOS"
                label = "Activos (Mes Actual)"
                orden = "9999-99" 
            else:
                try:
                    d_fin = dt.strptime(data['fecha_fin'], '%Y-%m-%d')
                    key = d_fin.strftime('%Y-%m')
                    label = f"{meses[d_fin.month - 1]} {d_fin.year}"
                    orden = key
                except:
                    key = "Desconocido"
                    label = "Desconocido"
                    orden = "0000-00"
        else: 
            key = l_id
            label = f"{l_id} (Act)" if es_activo else l_id
            orden = data['fecha_fin']
            
        if key not in grupos:
            grupos[key] = {'orden': orden, 'label': label, 'consumo': 0.0, 'pollitos': 0, 'saldo': 0.0}
            
        grupos[key]['consumo'] += data['consumo']
        grupos[key]['pollitos'] += data['pollitos']
        grupos[key]['saldo'] += data['ultimo_saldo']

    grupos_ordenados = sorted(grupos.values(), key=lambda x: x['orden'])
    
    grafico_ciclos = {
        'labels': [g['label'] for g in grupos_ordenados],
        'consumos': [g['consumo'] for g in grupos_ordenados],
        'eficiencias': [(g['consumo'] / g['pollitos']) if g['pollitos'] > 0 else 0.0 for g in grupos_ordenados],
        'saldos': [g['saldo'] for g in grupos_ordenados]
    }

    tabla_resumen = []
    lista_rendimientos = []
    total_pollitos_global = 0
    total_consumo_final = 0.0

    for nombre_granja, datos in granjas_data.items():
        consumo_granja = datos['consumo_real']
        if consumo_granja <= 0.0001:
            continue

        rend_granja = 0.0
        if datos['pollitos'] > 0:
            rend_granja = consumo_granja / datos['pollitos']
            if rend_granja > 0.000001:
                lista_rendimientos.append(rend_granja)

        total_pollitos_global += datos['pollitos']
        total_consumo_final += consumo_granja

        tabla_resumen.append({
            'granja': nombre_granja,
            'total_kg': consumo_granja,
            'pollitos': datos['pollitos'],
            'kg_pollito': rend_granja
        })

    rendimiento = 0.0
    if total_pollitos_global > 0:
        rendimiento = total_consumo_final / total_pollitos_global
    
    kpis = {
        "card1_label": "Saldo Inicial (kg)", "card1_value": math_saldo_inicial_kg_global,
        "card2_label": "Pedidos Gas (kg)", "card2_value": math_ingresos_kg,
        "card3_label": "Consumo Real (kg)", "card3_value": total_consumo_final,
        "card4_label": "Eficiencia (kg/ave)", "card4_value": rendimiento,
        "card5_label": "Pollitos", "card5_value": total_pollitos_global,
        "card6_label": "Inversión Total ($)", "card6_value": math_dinero_total
    }

    media = 0.0
    desviacion = 0.0
    n = len(lista_rendimientos) 
    if n > 0:
        media = sum(lista_rendimientos) / n
        if n > 1:
            import math
            varianza = sum((x - media) ** 2 for x in lista_rendimientos) / (n - 1)
            desviacion = math.sqrt(varianza)

    nota_informativa = "Nota: Estadísticas ajustadas. Se omiten granjas sin consumo en el periodo."

    return {
        "kpis": kpis,
        "series": series,
        "tabla_resumen": tabla_resumen,
        "grafico_ciclos": grafico_ciclos,
        "estadisticas": { "media": media, "desviacion": desviacion, "n_muestras": n },
        "periodo_tipo": periodo,
        "nota_metodologica": nota_informativa
    }

@csrf.exempt
@bp_energia_glp.route('/generar_informe', methods=['POST'])
@login_required_custom
def generar_informe():
    cursor = None
    try:
        data = request.get_json() or {}
        tipo_informe = data.get('tipo_informe')
        periodo = data.get('periodo')
        fecha_ini = data.get('fecha_inicio')
        fecha_fin = data.get('fecha_fin')
        ubicacion = data.get('ubicacion') 
        empresa_id = data.get('empresa_id')

        if not empresa_id or not tipo_informe:
            return jsonify({"success": False, "message": "Faltan datos obligatorios."}), 400

        cursor = mysql.connection.cursor()
        
        wheres = ["WHERE c.id_empresa = %s AND (c.pollitos > 0 OR c.neto_gastado > 0 OR c.clase = 'ingreso' OR c.operacion = 'inicio_calefaccion')"]
        params = [empresa_id]

        if tipo_informe == 'zona' and ubicacion:
            cursor.execute("SELECT DISTINCT ubicacion FROM tanques_sedes WHERE zona = %s AND empresa_id = %s", (ubicacion, empresa_id))
            granjas = [r['ubicacion'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
            if not granjas:
                return jsonify({"success": False, "message": f"Zona '{ubicacion}' sin granjas."})
            placeholders = ', '.join(['%s'] * len(granjas))
            wheres.append(f"AND c.ubicacion IN ({placeholders})")
            params.extend(granjas)
        elif tipo_informe == 'granja' and ubicacion:
            wheres.append("AND c.ubicacion = %s")
            params.append(ubicacion)

        if periodo == 'Personalizado' and fecha_ini and fecha_fin:
            wheres.append("AND c.fecha BETWEEN %s AND %s")
            params.append(fecha_ini)
            params.append(fecha_fin)
            
        elif periodo == 'Actual':
            wheres.append("AND c.estatus_lote = 'ACTIVO'")

        sql = """
            SELECT c.fecha, c.ubicacion, c.lote, c.estatus_lote, c.operacion, c.clase, 
                   c.saldo_estimado_kg, c.saldo_estimado_galones,
                   c.pollitos, c.kg_pollito,c.velocidad_consumo, c.masa_kg_facturada, c.neto_gastado,
                   c.precio_total 
            FROM cardex_glp c
        """
        final_sql = f"{sql} {' '.join(wheres)} ORDER BY c.fecha ASC"
        
        cursor.execute(final_sql, tuple(params))
        rows = cursor.fetchall()

        raw_results = []
        if rows:
            columns = [col[0] for col in cursor.description]
            for row in rows:
                if isinstance(row, dict):
                    raw_results.append(row)
                else:
                    raw_results.append(dict(zip(columns, row)))

        lotes_activos = list(set([r['lote'] for r in raw_results if r.get('lote')]))
        
        mapa_pob_rescatada = {}
        if lotes_activos:
            placeholders_lotes = ', '.join(['%s'] * len(lotes_activos))
            sql_rescate = f"""
                SELECT lote, pollitos 
                FROM cardex_glp 
                WHERE operacion = 'inicio_calefaccion' 
                  AND lote IN ({placeholders_lotes})
            """
            cursor.execute(sql_rescate, tuple(lotes_activos))
            for p_row in cursor.fetchall():
                l_id = p_row['lote'] if isinstance(p_row, dict) else p_row[0]
                p_qty = int(p_row['pollitos'] if isinstance(p_row, dict) else p_row[1])
                mapa_pob_rescatada[l_id] = p_qty

        cursor.close()

        datos = _procesar_resultados_glp(raw_results, tipo_informe, periodo, mapa_pob_rescatada)
        if not datos or not datos.get('tabla_resumen'):
            return jsonify({"success": False, "message": "No hay datos operativos válidos para mostrar."})

        return jsonify({"success": True, "data": datos})

    except Exception as e:
        if cursor: cursor.close()
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_energia_glp.route('/obtener_ubicaciones', methods=['POST'])
@login_required_custom
def obtener_ubicaciones():
    cursor = None
    try:
        data = request.get_json()
        tipo = data.get('tipo')
        empresa_id = data.get('empresa_id')

        if not empresa_id or not tipo:
            return jsonify({"success": False})

        cursor = mysql.connection.cursor()
        query = ""
        col = ""
        if tipo == "granja":
            query = "SELECT DISTINCT ubicacion FROM cardex_glp WHERE id_empresa = %s ORDER BY ubicacion ASC"
            col = "ubicacion"
        elif tipo == "zona":
            query = "SELECT DISTINCT zona FROM tanques_sedes WHERE empresa_id = %s ORDER BY zona ASC"
            col = "zona"
        else:
            return jsonify({"success": True, "ubicaciones": []})

        cursor.execute(query, (empresa_id,))
        rows = cursor.fetchall()
        
        results = []
        for r in rows:
            val = r.get(col) if isinstance(r, dict) else r[0]
            if val: results.append(val)

        cursor.close()
        return jsonify({"success": True, "ubicaciones": sorted(list(set(results)))})
    except Exception as e:
        if cursor: cursor.close()
        print("Error ubicaciones:", e)
        return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_energia_glp.route('/generar_informe_saldos', methods=['POST'])
@login_required_custom
def generar_informe_saldos():
    data = request.get_json() or {}
    empresa_id = data.get('empresa_id') or session.get('empresa_id')
    
    if not empresa_id:
        return jsonify({'error': 'ID Empresa no identificado'}), 400

    cur = mysql.connection.cursor()
    
    try:
        sql_lotes = """
            SELECT ubicacion, MAX(lote) as ultimo_lote_inactivo
            FROM cardex_glp
            WHERE id_empresa = %s AND estatus_lote = 'INACTIVO'
            GROUP BY ubicacion
        """
        cur.execute(sql_lotes, (empresa_id,))
        lotes_inactivos = cur.fetchall()
        
        col_names = [d[0] for d in cur.description]
        lista_lotes = []
        for row in lotes_inactivos:
            if isinstance(row, dict): lista_lotes.append(row)
            else: lista_lotes.append(dict(zip(col_names, row)))

        reporte_data = []

        for item in lista_lotes:
            ubicacion = item['ubicacion']
            lote = item['ultimo_lote_inactivo']
            
            tanques_estado = {} 

            sql_detalle = """
                SELECT fecha, 
                       `nivel tk-1`, `capacidad tk-1`,
                       `nivel tk-2`, `capacidad tk-2`,
                       `nivel tk-3`, `capacidad tk-3`,
                       `nivel tk-4`, `capacidad tk-4`,
                       `nivel tk-5`, `capacidad tk-5`,
                       `nivel tk-6`, `capacidad tk-6`
                FROM cardex_glp
                WHERE id_empresa = %s AND ubicacion = %s AND lote = %s
                ORDER BY fecha DESC, id DESC
            """
            cur.execute(sql_detalle, (empresa_id, ubicacion, lote))
            filas_lote = cur.fetchall()

            fecha_cierre = None
            
            filas_dict = []
            if filas_lote:
                cols_det = [d[0] for d in cur.description]
                for f in filas_lote:
                    filas_dict.append(f if isinstance(f, dict) else dict(zip(cols_det, f)))
                
                fecha_cierre = filas_dict[0].get('fecha')

            for row in filas_dict:
                for i in range(1, 7):
                    tk_key = f'tk-{i}'
                    nivel_col = f'nivel tk-{i}'
                    cap_col = f'capacidad tk-{i}'
                    
                    if tk_key in tanques_estado: continue 
                        
                    nivel_val = row.get(nivel_col)
                    cap_val = row.get(cap_col)

                    if nivel_val is not None:
                        try:
                            nivel_pct = float(nivel_val)
                            capacidad = float(cap_val or 250)
                            saldo_kg = (nivel_pct / 100.0) * capacidad * 2.0
                            
                            tanques_estado[tk_key] = {
                                'nivel_pct': nivel_pct,
                                'capacidad': capacidad,
                                'saldo_kg': saldo_kg
                            }
                        except: pass

            if tanques_estado:
                lista_tanques_final = []
                total_kg_granja = 0
                
                for i in range(1, 7):
                    tk_key = f'tk-{i}'
                    if tk_key in tanques_estado:
                        d = tanques_estado[tk_key]
                        lista_tanques_final.append({
                            'tanque': f'Tanque {i}',
                            'nivel': d['nivel_pct'],
                            'capacidad_gl': d['capacidad'],
                            'saldo_kg': d['saldo_kg']
                        })
                        total_kg_granja += d['saldo_kg']
                
                if lista_tanques_final:
                    reporte_data.append({
                        'ubicacion': ubicacion,
                        'lote_cerrado': lote,
                        'fecha_cierre': str(fecha_cierre),
                        'tanques': lista_tanques_final,
                        'total_kg_granja': total_kg_granja
                    })

        return jsonify({
            'success': True,
            'data': reporte_data,
            'fecha_generacion': str(datetime.now().strftime("%Y-%m-%d %H:%M"))
        })

    except Exception as e:
        print(f"Error informe saldos: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        cur.close()

# ==============================================================================
# AUDITORÍA AVANZADA: LOTES ACTIVOS Y RUPTURAS
# ==============================================================================

@csrf.exempt
@bp_energia_glp.route('/obtener_auditoria_activos', methods=['POST'])
@login_required_custom
def obtener_auditoria_activos():
    data = request.get_json()
    empresa_id = data.get('empresa_id')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        query = """
            SELECT id, fecha, ubicacion, lote, dias_operacion, operacion, 
                   neto_gastado, porcentaje_diferencia, registro,
                   `testigo nivel tk-1`, `testigo nivel tk-2`, `testigo nivel tk-3`, `testigo nivel tk-4`, `testigo nivel tk-5`, `testigo nivel tk-6`, `testigo nivel tk-7`, `testigo nivel tk-8`, `testigo nivel tk-9`, `testigo nivel tk-10`, `testigo nivel tk-11`,
                   `testigo nivelfinal tk-1`, `testigo nivelfinal tk-2`, `testigo nivelfinal tk-3`, `testigo nivelfinal tk-4`, `testigo nivelfinal tk-5`, `testigo nivelfinal tk-6`, `testigo nivelfinal tk-7`, `testigo nivelfinal tk-8`, `testigo nivelfinal tk-9`, `testigo nivelfinal tk-10`, `testigo nivelfinal tk-11`,
                   testigo_baucher_tk_1, testigo_baucher_tk_2, testigo_baucher_tk_3, testigo_baucher_tk_4, testigo_baucher_tk_5, testigo_baucher_tk_6, testigo_baucher_tk_7, testigo_baucher_tk_8, testigo_baucher_tk_9, testigo_baucher_tk_10, testigo_baucher_tk_11
            FROM cardex_glp 
            WHERE id_empresa = %s AND estatus_lote = 'ACTIVO'
            ORDER BY fecha DESC, id DESC
        """
        cur.execute(query, (empresa_id,))
        filas = cur.fetchall()

        if not filas:
            return jsonify({'success': False, 'message': 'No hay lotes activos en este momento.'}), 404

        resumen_granjas = {}
        historial = []

        for f in filas:
            ubi = f['ubicacion']
            op = f['operacion']

            if ubi not in resumen_granjas:
                resumen_granjas[ubi] = {
                    'dias_maximos': 0,
                    'cant_consumos': 0,
                    'cant_tanqueos': 0,
                    'total_gastado': 0.0
                }

            if f['dias_operacion'] and f['dias_operacion'] > resumen_granjas[ubi]['dias_maximos']:
                resumen_granjas[ubi]['dias_maximos'] = f['dias_operacion']

            if op == 'consumo':
                resumen_granjas[ubi]['cant_consumos'] += 1
                if f['neto_gastado']:
                    resumen_granjas[ubi]['total_gastado'] += float(f['neto_gastado'])
            elif op == 'tanqueo':
                resumen_granjas[ubi]['cant_tanqueos'] += 1

            fotos = []
            for i in range(1, 12):
                col_ini = f'testigo nivel tk-{i}'
                col_fin = f'testigo nivelfinal tk-{i}'
                col_bau = f'testigo_baucher_tk_{i}'
                
                if f.get(col_ini) and str(f[col_ini]).strip(): fotos.append(f[col_ini])
                if f.get(col_fin) and str(f[col_fin]).strip(): fotos.append(f[col_fin])
                if f.get(col_bau) and str(f[col_bau]).strip(): fotos.append(f[col_bau])

            historial.append({
                'id': f['id'],
                'fecha': f['fecha'].strftime('%Y-%m-%d') if f['fecha'] else 'Sin Fecha',
                'ubicacion': ubi,
                'operacion': op,
                'neto_gastado': float(f['neto_gastado']) if f['neto_gastado'] else 0.0,
                'diferencia': float(f['porcentaje_diferencia']) if f['porcentaje_diferencia'] else 0.0,
                'registro': f['registro'] or 'Desconocido',
                'fotos': fotos,
                'dia': f['dias_operacion'] or 0
            })

        return jsonify({'success': True, 'resumen': resumen_granjas, 'historial': historial})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        cur.close()

@csrf.exempt
@bp_energia_glp.route('/obtener_lotes_vencidos', methods=['POST'])
@login_required_custom
def obtener_lotes_vencidos():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "ID Empresa requerido"})

    try:
        cur = mysql.connection.cursor()
        sql = """
            SELECT 
                ubicacion, 
                lote, 
                MIN(fecha) as fecha_inicio,
                DATEDIFF(NOW(), MIN(fecha)) as dias_abierto,
                MAX(fecha) as ultima_actividad
            FROM cardex_glp
            WHERE id_empresa = %s AND estatus_lote = 'ACTIVO'
            GROUP BY ubicacion, lote
            HAVING DATEDIFF(NOW(), MIN(fecha)) > 15
            ORDER BY dias_abierto DESC
        """
        cur.execute(sql, (empresa_id,))
        rows = cur.fetchall()
        
        lotes_vencidos = []
        col_names = [d[0] for d in cur.description] if cur.description else []
        
        for r in rows:
            rd = dict(zip(col_names, r)) if not isinstance(r, dict) else r
            lotes_vencidos.append({
                "ubicacion": rd.get('ubicacion'),
                "lote": rd.get('lote'),
                "fecha_inicio": str(rd.get('fecha_inicio')),
                "ultima_actividad": str(rd.get('ultima_actividad')),
                "dias": int(rd.get('dias_abierto') or 0)
            })

        cur.close()
        return jsonify({"success": True, "items": lotes_vencidos})

    except Exception as e:
        print("Error lotes vencidos:", str(e))
        return jsonify({"success": False, "message": str(e)})

@csrf.exempt
@bp_energia_glp.route('/obtener_alertas_ruptura_validacion', methods=['POST'])
@login_required_custom
def obtener_alertas_ruptura_validacion():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "ID Empresa requerido"})

    try:
        cur = mysql.connection.cursor()
        
        sql = """
            SELECT 
                p.id,
                p.codigo_pedido,
                p.fecha_validacion,
                p.ubicacion,
                p.numero_factura,
                DATEDIFF(NOW(), p.fecha_validacion) as dias_alerta
            FROM pedidos_gas_glp p
            WHERE p.estatus = 'validado'
              AND p.fecha_validacion >= '2026-04-30'
              AND p.cliente = (SELECT nombre_comercial FROM empresas WHERE nit = %s LIMIT 1)
              AND p.codigo_pedido COLLATE utf8mb4_general_ci NOT IN (
                  SELECT codigo_pedido COLLATE utf8mb4_general_ci 
                  FROM cardex_glp 
                  WHERE codigo_pedido IS NOT NULL AND id_empresa = %s
              )
            ORDER BY dias_alerta DESC
        """
        cur.execute(sql, (empresa_id, empresa_id))
        rows = cur.fetchall()
        
        alertas = []
        col_names = [d[0] for d in cur.description] if cur.description else []
        
        for r in rows:
            rd = dict(zip(col_names, r)) if not isinstance(r, dict) else r
            alertas.append({
                "id": rd.get('id'),
                "codigo": rd.get('codigo_pedido'),
                "fecha_v": str(rd.get('fecha_validacion')),
                "ubicacion": rd.get('ubicacion'),
                "factura": rd.get('numero_factura'),
                "dias": int(rd.get('dias_alerta') or 0)
            })

        cur.close()
        return jsonify({"success": True, "items": alertas})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

# ==============================================================================
# EDICIÓN DE TANQUES Y GENERACIÓN DE QR
# ==============================================================================

@csrf.exempt
@bp_energia_glp.route('/obtener_tanques_granja', methods=['POST'])
@login_required_custom
def obtener_tanques_granja():
    data = request.get_json()
    empresa_id = data.get('empresa_id')
    ubicacion = data.get('ubicacion')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT * FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s ORDER BY id ASC", (empresa_id, ubicacion))
        filas = cur.fetchall()
        
        if not filas:
            return jsonify({'success': False, 'message': 'No se encontró configuración para esta granja.'}), 404
        
        general = {
            'proveedor': filas[0]['proveedor'],
            'email': filas[0]['email'],
            'zona': filas[0]['zona'],
            'propietario': filas[0]['propietario'],
            'empresa': filas[0]['empresa']
        }
        
        tanques = []
        for f in filas:
            cap = int(f['capacidad_gls']) if f['capacidad_gls'] else 0
            if cap > 0:
                tanques.append({
                    'nombre_tanque': f['nombre_tanque'],
                    'capacidad': cap
                })
            
        return jsonify({'success': True, 'general': general, 'tanques': tanques})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        cur.close()

@csrf.exempt
@bp_energia_glp.route('/guardar_tanques_granja', methods=['POST'])
@login_required_custom
def guardar_tanques_granja():
    data = request.get_json()
    empresa_id = data.get('empresa_id')
    ubicacion = data.get('ubicacion')
    proveedor = data.get('proveedor')
    email = data.get('email')
    tanques = data.get('tanques')
    
    if not tanques or len(tanques) == 0:
        return jsonify({'success': False, 'message': 'Debe haber al menos un tanque registrado.'}), 400
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT empresa, zona, propietario FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s LIMIT 1", (empresa_id, ubicacion))
        row = cur.fetchone()
        if not row:
            return jsonify({'success': False, 'message': 'Granja original no encontrada en la base de datos.'}), 404
            
        empresa_nombre = row['empresa']
        zona = row['zona']
        propietario = row['propietario']
        
        cur.execute("DELETE FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s", (empresa_id, ubicacion))
        
        for tk in tanques:
            cur.execute("""
                INSERT INTO tanques_sedes (empresa, empresa_id, nombre_tanque, propietario, capacidad_gls, ubicacion, zona, proveedor, email)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (empresa_nombre, empresa_id, tk['nombre'], propietario, tk['capacidad'], ubicacion, zona, proveedor, email))
            
        mysql.connection.commit()
        return jsonify({'success': True, 'message': f'Configuración de tanques para {ubicacion} actualizada correctamente.'})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        cur.close()

@csrf.exempt
@bp_energia_glp.route('/generar_qrs_pdf', methods=['POST'])
@login_required_custom
def generar_qrs_pdf():
    try:
        data = request.get_json()
        empresa_id = data.get('empresa_id')
        ubicacion_filtro = data.get('ubicacion') 

        if not empresa_id:
            return jsonify({'success': False, 'message': 'Falta el ID de la empresa'}), 400

        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        if ubicacion_filtro:
            query = "SELECT * FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s"
            cur.execute(query, (empresa_id, ubicacion_filtro))
        else:
            query = "SELECT * FROM tanques_sedes WHERE empresa_id = %s"
            cur.execute(query, (empresa_id,))
            
        tanques_data = cur.fetchall()
        cur.close()

        if not tanques_data:
            nombre_lugar = ubicacion_filtro if ubicacion_filtro else "esta empresa"
            return jsonify({
                'success': False, 
                'message': f'No hay tanques registrados para {nombre_lugar}. Ve a "Editar Tanques" primero.'
            }), 404

        granjas_agrupadas = {}
        for row in tanques_data:
            ubi = row['ubicacion']
            if ubi not in granjas_agrupadas:
                granjas_agrupadas[ubi] = {
                    "sede": ubi,
                    "empresa": row['empresa'],
                    "tanques": [],
                    "proveedor": row['proveedor'],
                    "email_proveedor": row['email']
                }
            granjas_agrupadas[ubi]["tanques"].append({
                "numero": row['nombre_tanque'],
                "capacidad": row['capacidad_gls']
            })

        buffer = io.BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        
        x_pos = 50 
        y_pos = height - 50
        
        for ubi, datos_granja in granjas_agrupadas.items():
            qr_content = json.dumps(datos_granja, ensure_ascii=False)
            
            qr = qrcode.QRCode(version=1, error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
            qr.add_data(qr_content)
            qr.make(fit=True)
            img_qr = qr.make_image(fill_color="black", back_color="white")
            
            qr_buffer = io.BytesIO()
            img_qr.save(qr_buffer, format="PNG")
            qr_buffer.seek(0)
            
            if y_pos < 300:
                pdf.showPage()
                y_pos = height - 50
            
            qr_size = 180
            img_reader = ImageReader(qr_buffer)
            pdf.drawImage(img_reader, x_pos, y_pos - qr_size, width=qr_size, height=qr_size)
            
            text_y = y_pos - qr_size - 20
            pdf.setFont("Helvetica-Bold", 12)
            pdf.drawString(x_pos, text_y, f"QR - {datos_granja['sede']}")
            
            text_y -= 15
            pdf.setFont("Helvetica", 10)
            
            wrapped_text = textwrap.wrap(qr_content, width=90) 
            
            for line in wrapped_text:
                pdf.drawString(x_pos, text_y, line)
                text_y -= 14 
            
            y_pos = text_y - 40

        pdf.save()
        buffer.seek(0)

        nombre_archivo = f"QRs_{ubicacion_filtro.replace(' ', '_') if ubicacion_filtro else 'General'}.pdf"
        return send_file(buffer, as_attachment=True, download_name=nombre_archivo, mimetype='application/pdf')

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': f'Error interno: {str(e)}'}), 500

# ==============================================================================
# UTILIDADES Y OPERACIONES
# ==============================================================================

@csrf.exempt
@bp_energia_glp.route('/glp/admin/rechazar_solicitud', methods=['POST'])
@login_required_custom
def rechazar_solicitud():
    try:
        data = request.get_json()
        id_solicitud = data.get('id')
        
        if not id_solicitud:
            return jsonify({"success": False, "message": "ID de solicitud requerido."}), 400

        cur = mysql.connection.cursor()
        
        query = """
            UPDATE pedidos_gas_glp 
            SET estatus_flujo = 'rechazado' 
            WHERE id = %s
        """
        cur.execute(query, (id_solicitud,))
        mysql.connection.commit()
        cur.close()

        return jsonify({"success": True, "message": "Solicitud anulada correctamente."})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_energia_glp.route('/ejecutar_limpieza_automatica')
@login_required_custom
def ejecutar_limpieza_automatica():
    dias_limite = 60
    total_borrados = 0
    base_dir = current_app.static_folder
    
    cur = mysql.connection.cursor()
    try:
        cur.execute(f"""
            SELECT * FROM cardex_glp 
            WHERE fecha < DATE_SUB(NOW(), INTERVAL {dias_limite} DAY)
        """)
        filas_glp = cur.fetchall()
        cols_glp = [desc[0] for desc in cur.description] if filas_glp else []

        for fila in filas_glp:
            row = dict(zip(cols_glp, fila)) if not isinstance(fila, dict) else fila
            rutas_glp = []
            cols_update_glp = []

            for col, val in row.items():
                if val and isinstance(val, str) and 'testigo' in col.lower():
                    rutas_glp.append(val.strip())
                    cols_update_glp.append(col)
            
            if rutas_glp:
                count = _borrar_evidencias_tanqueo(rutas_glp) 
                total_borrados += (count or 0) # En esta migración, _borrar_evidencias no retorna count, así que lo ignoramos
            
            if cols_update_glp:
                set_clause = ", ".join([f"`{c}` = NULL" for c in cols_update_glp])
                cur.execute(f"UPDATE cardex_glp SET {set_clause} WHERE id = %s", (row['id'],))

        mysql.connection.commit()
        mensaje = f"Mantenimiento de GLP completado. Se limpiaron evidencias antiguas."
        return jsonify({'success': True, 'message': mensaje})

    except Exception as e:
        print(f"⚠️ Error en limpieza automática: {e}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()

@bp_energia_glp.route('/util/recalcular_historico')
@login_required_custom
def recalcular_historico_glp():
    try:
        cur = mysql.connection.cursor()
        
        cur.execute("SELECT DISTINCT lote FROM cardex_glp WHERE lote IS NOT NULL")
        lotes_raw = cur.fetchall()
        lotes = [row['lote'] if isinstance(row, dict) else row[0] for row in lotes_raw]
        
        registros_actualizados = 0
        
        for lote in lotes:
            cur.execute("""
                SELECT pollitos FROM cardex_glp 
                WHERE lote=%s AND operacion='inicio_calefaccion' LIMIT 1
            """, (lote,))
            row_p = cur.fetchone()
            
            pollitos = 0
            if row_p:
                pollitos = row_p.get('pollitos') if isinstance(row_p, dict) else row_p[0]
            
            if not pollitos or float(pollitos) <= 0:
                continue
                
            cur.execute("""
                SELECT id, COALESCE(neto_gastado, 0) as neto 
                FROM cardex_glp 
                WHERE lote=%s 
                ORDER BY id ASC
            """, (lote,))
            registros = cur.fetchall()
            
            consumo_acumulado = 0.0
            
            for reg in registros:
                r_id = reg['id'] if isinstance(reg, dict) else reg[0]
                r_neto = float(reg['neto'] if isinstance(reg, dict) else reg[1])
                
                consumo_acumulado += r_neto
                nuevo_kg_pollito = consumo_acumulado / float(pollitos)
                
                cur.execute("UPDATE cardex_glp SET kg_pollito=%s WHERE id=%s", (nuevo_kg_pollito, r_id))
                registros_actualizados += 1
                
        mysql.connection.commit()
        cur.close()
        
        return jsonify({
            "success": True, 
            "message": f"¡Éxito! Se han recalculado y corregido {registros_actualizados} registros históricos."
        })

    except Exception as e:
        mysql.connection.rollback()
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)})

@csrf.exempt
@bp_energia_glp.route('/obtener_pendientes_tanqueo_reporte', methods=['POST'])
@login_required_custom
def obtener_pendientes_tanqueo_reporte():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "ID Empresa requerido"})

    try:
        cur = mysql.connection.cursor()
        
        cur.execute("SELECT nombre_comercial FROM empresas WHERE nit = %s", (empresa_id,))
        row_emp = cur.fetchone()
        if not row_emp:
            return jsonify({"success": False, "message": "Empresa no encontrada"})
            
        nombre_empresa = row_emp['nombre_comercial'] if isinstance(row_emp, dict) else row_emp[0]

        sql = """
            SELECT 
                p.id,
                p.fecha_registro,
                p.ubicacion,
                p.proveedor,
                p.nivel_solicitado,
                p.codigo_pedido,
                DATEDIFF(NOW(), p.fecha_registro) as dias_retraso
            FROM pedidos_gas_glp p
            WHERE TRIM(UPPER(p.cliente)) = TRIM(UPPER(%s))
              AND p.estatus_flujo IN ('aprobado_webmaster', 'enviado_auto')
              AND p.fecha_registro >= '2026-04-30'
            ORDER BY dias_retraso DESC
        """
        cur.execute(sql, (nombre_empresa,))
        rows = cur.fetchall()
        
        pendientes = []
        col_names = [d[0] for d in cur.description] if cur.description else []
        
        for r in rows:
            rd = dict(zip(col_names, r)) if not isinstance(r, dict) else r
            pendientes.append({
                "id": rd.get('id'),
                "fecha": str(rd.get('fecha_registro')),
                "ubicacion": rd.get('ubicacion'),
                "proveedor": rd.get('proveedor'),
                "solicitado": float(rd.get('nivel_solicitado') or 0),
                "codigo": rd.get('codigo_pedido'),
                "dias": int(rd.get('dias_retraso') or 0)
            })

        cur.close()
        return jsonify({"success": True, "items": pendientes})

    except Exception as e:
        print("Error reporte pendientes:", str(e))
        return jsonify({"success": False, "message": str(e)})