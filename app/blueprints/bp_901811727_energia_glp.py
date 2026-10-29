# MÓDULO: GAS | SUBMÓDULO: bp_901811727_energia_glp | CONDICIÓN: OBLIGATORIO | TIPO: 1
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
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.utils import ImageReader
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.lib.units import inch

bp_energia_glp = Blueprint('bp_energia_glp', __name__)

# ==============================================================================
# CONFIGURACIÓN SMTP Y ALERTAS DE SEGURIDAD
# ==============================================================================

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = 587
EMAIL_USER = os.environ.get("EMAIL_USER", "tu_email@ejemplo.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "tu_password")
EMAIL_FROM = EMAIL_USER

# ==============================================================================
# HELPERS GLOBALES (AUDITORÍA BD)
# ==============================================================================

def _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, datos_modificados):
    """Registra cualquier mutación en la tabla de auditoría global obligatoria."""
    try:
        cur.execute("SELECT nombre_comercial FROM empresas WHERE nit = %s LIMIT 1", (empresa_id,))
        row = cur.fetchone()
        if isinstance(row, dict):
            empresa_nombre = row.get('nombre_comercial', 'N/D')
        else:
            empresa_nombre = row[0] if row else 'N/D'

        query = """
            INSERT INTO `registro_operaciones_bqa-one` 
            (empresa, empresa_id, backend_origen, datos_modificados, usuario_nombre, usuario_cedula, fecha_operacion) 
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """
        cur.execute(query, (empresa_nombre, empresa_id, "bp_901811727_energia_glp", json.dumps(datos_modificados), usuario_nombre, usuario_cedula))
    except Exception as e:
        print(f"Error registrando en registro_operaciones_bqa-one: {e}")

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
    usuario_nombre = session.get('nombre', 'Sistema')
    usuario_cedula = session.get('cedula', session.get('user_id', '0'))

    if not id_registro:
         return jsonify({'status': 'error', 'message': 'ID no proporcionado'})

    for intento in range(3):
        try:
            cur = mysql.connection.cursor()
            cur.execute("SELECT * FROM cardex_glp WHERE id = %s FOR UPDATE", (id_registro,))
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
                    _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "rechazo_eliminacion_tanqueo", "id_registro": id_registro})
                    
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
                
                _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "validacion_tanqueo", "id_registro": id_registro})
                
                mysql.connection.commit()
                msg = "Registro validado correctamente."

            cur.close()
            return jsonify({'success': True, 'message': msg})

        except MySQLdb.OperationalError as e:
            if e.args[0] == 1213:
                mysql.connection.rollback()
                time.sleep(0.5)
                if intento == 2: return jsonify({'success': False, 'message': 'Conflicto de red.'}), 500
            else:
                mysql.connection.rollback()
                return jsonify({'success': False, 'message': str(e)}), 500
        except Exception as e:
            mysql.connection.rollback()
            return jsonify({'success': False, 'message': str(e)}), 500

# ==============================================================================
# LÓGICA DE INFORMES Y ESTADÍSTICAS GLP (CON NUEVAS DIMENSIONES E IQR)
# ==============================================================================

def _calcular_estadistica_desviacion(desviaciones):
    if not desviaciones: return 0.0, 0.0, 0
    desviaciones.sort()
    n = len(desviaciones)
    if n < 4:
        media = sum(desviaciones) / n
        varianza = sum((x - media)**2 for x in desviaciones) / (n - 1) if n > 1 else 0
        return media, math.sqrt(varianza), n
        
    q1 = desviaciones[int(n * 0.25)]
    q3 = desviaciones[int(n * 0.75)]
    iqr = q3 - q1
    lim_inf = q1 - 1.5 * iqr
    lim_sup = q3 + 1.5 * iqr
    
    filtradas = [x for x in desviaciones if lim_inf <= x <= lim_sup]
    if not filtradas: return 0.0, 0.0, 0
    
    n_filt = len(filtradas)
    media = sum(filtradas) / n_filt
    varianza = sum((x - media)**2 for x in filtradas) / (n_filt - 1) if n_filt > 1 else 0
    return media, math.sqrt(varianza), n_filt

def _procesar_resultados_glp(resultados, tipo_informe, periodo, mapa_poblacion_rescatada, kpi_tecnicos, kpi_por_granja):
    if not resultados:
        return None

    def safe_float(val):
        if val is None: return 0.0
        try: return float(val)
        except (ValueError, TypeError): return 0.0

    math_saldo_inicial_kg_global = 0.0
    math_ingresos_kg = 0.0
    math_consumo_real_acumulado = 0.0
    math_dinero_total = 0.0 
    math_ahorro_total = 0.0
    math_sobrecosto_total = 0.0

    series = { 'fechas': [], 'kg_pollito': [], 'velocidad_consumo': [], 'saldo_inicial': [], 'saldo_final': [], 'ingresos': [] }
    granjas_data = {}
    lotes_info = {} 
    desviaciones_tanqueo = []
    comparativa_proveedores = {}

    resultados.sort(key=lambda x: str(x.get('fecha')))

    for row in resultados:
        lote_id = row.get('lote')
        pollitos_reales = mapa_poblacion_rescatada.get(lote_id, 0) if lote_id else 0
        if pollitos_reales <= 0: continue

        fecha_str = str(row.get('fecha'))
        clase = str(row.get('clase') or '').lower().strip()
        ubicacion = row.get('ubicacion') or 'Desconocida'
        clima = row.get('clima') or 'Sin Definir'
        tecnologia = row.get('tecnologia_produccion') or row.get('tecnologia') or 'Sin Definir'
        proveedor = row.get('proveedor') or 'Sin Asignar'

        val_kg_saldo = safe_float(row.get('saldo_estimado_kg'))
        masa_fact = safe_float(row.get('masa_kg_facturada'))
        neto_gast = safe_float(row.get('neto_gastado'))

        if lote_id not in lotes_info:
            lotes_info[lote_id] = {
                'estatus': str(row.get('estatus_lote', '')).upper(),
                'consumo': 0.0, 'pollitos': pollitos_reales, 'fecha_fin': fecha_str[:10], 'ultimo_saldo': 0.0,
                'clima': clima, 'tecnologia': tecnologia
            }
            
        if str(row.get('estatus_lote', '')).upper() == 'INACTIVO': lotes_info[lote_id]['estatus'] = 'INACTIVO'
        
        if neto_gast > 0: lotes_info[lote_id]['consumo'] += neto_gast
            
        if fecha_str[:10] >= lotes_info[lote_id]['fecha_fin']:
            lotes_info[lote_id]['fecha_fin'] = fecha_str[:10]
            if val_kg_saldo > 0 or clase == 'saldo final': lotes_info[lote_id]['ultimo_saldo'] = val_kg_saldo

        val_precio = safe_float(row.get('precio_total')) 
        precio_hist = safe_float(row.get('precio_unitario'))
        if precio_hist <= 0 and masa_fact > 0: 
            precio_hist = val_precio / masa_fact

        if ubicacion not in granjas_data:
            granjas_data[ubicacion] = {
                'inicial': 0.0, 'ingresos': 0.0, 'consumo_real': 0.0,
                'pollitos': 0, 'lotes': set(),
                'limite_inferior': safe_float(row.get('limite_inferior')),
                'limite_superior': safe_float(row.get('limite_superior')),
                'precio_promedio': precio_hist,
                'clima': clima, 'tecnologia': tecnologia,
                'desviaciones': [],
                'proveedor': proveedor
            }
            
        if precio_hist > 0:
            granjas_data[ubicacion]['precio_promedio'] = precio_hist

        if row.get('limite_inferior') is not None: granjas_data[ubicacion]['limite_inferior'] = safe_float(row.get('limite_inferior'))
        if row.get('limite_superior') is not None: granjas_data[ubicacion]['limite_superior'] = safe_float(row.get('limite_superior'))
        if row.get('clima') and str(row.get('clima')).strip() not in ['', 'Sin Definir', 'None']: granjas_data[ubicacion]['clima'] = row.get('clima')
        if row.get('tecnologia_produccion') and str(row.get('tecnologia_produccion')).strip() not in ['', 'Sin Definir', 'None']: granjas_data[ubicacion]['tecnologia'] = row.get('tecnologia_produccion')

        if proveedor and proveedor != 'Sin Asignar':
            granjas_data[ubicacion]['proveedor'] = proveedor

        if clase == 'ingreso' and masa_fact > 0:
            pct_dif = safe_float(row.get('porcentaje_diferencia'))
            desviaciones_tanqueo.append(pct_dif)
            granjas_data[ubicacion]['desviaciones'].append(pct_dif)
            
            if proveedor not in comparativa_proveedores:
                comparativa_proveedores[proveedor] = {'volumen': 0.0, 'costo': 0.0, 'desviaciones': []}
            comparativa_proveedores[proveedor]['volumen'] += masa_fact
            comparativa_proveedores[proveedor]['costo'] += val_precio
            comparativa_proveedores[proveedor]['desviaciones'].append(pct_dif)

        math_consumo_real_acumulado += neto_gast
        math_dinero_total += val_precio

        if clase == 'saldo inicial': math_saldo_inicial_kg_global += val_kg_saldo
        if clase == 'ingreso': math_ingresos_kg += masa_fact

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
                    key = "Desconocido"; label = "Desconocido"; orden = "0000-00"
        else: 
            key = l_id; label = f"{l_id} (Act)" if es_activo else l_id; orden = data['fecha_fin']
            
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
        if consumo_granja <= 0.0001: continue

        rend_granja = 0.0
        ahorro = 0.0
        sobrecosto = 0.0
        if datos['pollitos'] > 0:
            rend_granja = consumo_granja / datos['pollitos']
            if rend_granja > 0.000001: lista_rendimientos.append(rend_granja)
            
            if datos['limite_inferior'] > 0 and rend_granja < datos['limite_inferior']:
                ahorro = (datos['limite_inferior'] - rend_granja) * datos['pollitos'] * datos['precio_promedio']
            elif datos['limite_superior'] > 0 and rend_granja > datos['limite_superior']:
                sobrecosto = (rend_granja - datos['limite_superior']) * datos['pollitos'] * datos['precio_promedio']

        math_ahorro_total += ahorro
        math_sobrecosto_total += sobrecosto
        total_pollitos_global += datos['pollitos']
        total_consumo_final += consumo_granja
        
        media_desv_granja, _, _ = _calcular_estadistica_desviacion(datos['desviaciones'])
        
        kpi_granja = kpi_por_granja.get(nombre_granja, kpi_tecnicos)
        kpi_val = kpi_granja.get('cumplimiento', 0.0)

        tabla_resumen.append({
            'granja': nombre_granja,
            'clima': datos['clima'],
            'tecnologia': datos['tecnologia'],
            'limite_inferior': datos['limite_inferior'],
            'limite_superior': datos['limite_superior'],
            'rango_esperado': f"{datos['limite_inferior']} - {datos['limite_superior']}",
            'total_kg': consumo_granja,
            'pollitos': datos['pollitos'],
            'kg_pollito': rend_granja,
            'ahorro': ahorro,
            'sobrecosto': sobrecosto,
            'proveedor': datos['proveedor'],
            'media_desviacion': media_desv_granja,
            'kpi_operador': kpi_val
        })

    tabla_resumen.sort(key=lambda x: x['kg_pollito'], reverse=True)

    rendimiento = total_consumo_final / total_pollitos_global if total_pollitos_global > 0 else 0.0
    
    media_desv, std_desv, n_desv = _calcular_estadistica_desviacion(desviaciones_tanqueo)

    kpis = {
        "card1_label": "Saldo Inicial (kg)", "card1_value": math_saldo_inicial_kg_global,
        "card2_label": "Pedidos Gas (kg)", "card2_value": math_ingresos_kg,
        "card3_label": "Consumo Real (kg)", "card3_value": total_consumo_final,
        "card4_label": "Eficiencia (kg/ave)", "card4_value": rendimiento,
        "card5_label": "Ahorro Financiero ($)", "card5_value": math_ahorro_total,
        "card6_label": "Sobrecosto Ineficiencia ($)", "card6_value": math_sobrecosto_total
    }

    kpi_tecnico = {
        "tiempo_promedio_cierre": kpi_tecnicos.get('promedio', 0),
        "cumplimiento_sla_pct": kpi_tecnicos.get('cumplimiento', 0),
        "calificacion": "Óptimo" if kpi_tecnicos.get('cumplimiento', 0) > 85 else ("Aceptable" if kpi_tecnicos.get('cumplimiento', 0) > 60 else "Crítico")
    }

    tabla_proveedores = []
    for prov, dprov in comparativa_proveedores.items():
        pm, ps, pn = _calcular_estadistica_desviacion(dprov['desviaciones'])
        tabla_proveedores.append({
            'proveedor': prov, 'volumen_kg': dprov['volumen'], 'costo': dprov['costo'],
            'desv_media': pm, 'desv_std': ps
        })

    media = 0.0
    desviacion = 0.0
    n = len(lista_rendimientos) 
    if n > 0:
        media = sum(lista_rendimientos) / n
        if n > 1:
            varianza = sum((x - media) ** 2 for x in lista_rendimientos) / (n - 1)
            desviacion = math.sqrt(varianza)

    nota_informativa = "Nota: Estadísticas ajustadas. Se omiten granjas sin consumo en el periodo."

    return {
        "kpis": kpis,
        "series": series,
        "tabla_resumen": tabla_resumen,
        "grafico_ciclos": grafico_ciclos,
        "estadisticas": { "media": media, "desviacion": desviacion, "n_muestras": n },
        "tabla_proveedores": tabla_proveedores,
        "estadisticas_tanqueos": { "media_desv": media_desv, "std_desv": std_desv, "muestras": n_desv },
        "kpi_operativo": kpi_tecnico,
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
        clima_filtro = data.get('clima')
        tecnologia_filtro = data.get('tecnologia')

        if not empresa_id or not tipo_informe:
            return jsonify({"success": False, "message": "Faltan datos obligatorios."}), 400

        cursor = mysql.connection.cursor()
        
        # =========================================================================
        # REFACTORIZACIÓN KPI OP: DISCIPLINA OPERATIVA DE CAMPO
        # (50% Puntualidad Reporte Tanqueo base 26h + 50% Franja Diurna Lun-Sáb 06:00-18:00)
        # =========================================================================

        # 1. Telemetría de Auditoría: Horario de Operaciones en Sitio (Lun-Sáb, 06:00 a 18:00)
        cursor.execute("""
            SELECT fecha, accion, detalle
            FROM audit_log
            WHERE empresa_id = %s
              AND modulo = 'GLP'
              AND accion IN ('Consumo Registrado', 'Tanqueo Registrado', 'Inicio Calefacción', 'Fin Calefacción')
        """, (empresa_id,))
        audit_rows = cursor.fetchall()

        col_audit = [d[0] for d in cursor.description] if cursor.description else []
        ops_por_granja = {}
        total_ops_empresa = 0
        diurnas_ops_empresa = 0

        import re as _re_kpi
        for a_row in audit_rows:
            a = dict(zip(col_audit, a_row)) if not isinstance(a_row, dict) else a_row
            f_op = a.get('fecha')
            det = a.get('detalle') or ''
            
            match_sede = _re_kpi.search(r'Sede:\s*([^.]+)', det)
            sede_audit = match_sede.group(1).strip() if match_sede else 'General'

            if sede_audit not in ops_por_granja:
                ops_por_granja[sede_audit] = {'total': 0, 'diurnas': 0}

            ops_por_granja[sede_audit]['total'] += 1
            total_ops_empresa += 1

            if isinstance(f_op, datetime):
                if 6 <= f_op.hour <= 18 and f_op.weekday() != 6:
                    ops_por_granja[sede_audit]['diurnas'] += 1
                    diurnas_ops_empresa += 1

        # 2. Puntualidad en Reporte de Tanqueos (Base física 26h desde solicitud/aprobación)
        cursor.execute("""
            SELECT ubicacion, codigo_pedido, fecha_registro, fecha_solicitud, fecha_validacion
            FROM pedidos_gas_glp
            WHERE TRIM(UPPER(cliente)) COLLATE utf8mb4_general_ci = (
                SELECT TRIM(UPPER(nombre_comercial)) COLLATE utf8mb4_general_ci FROM empresas WHERE nit = %s LIMIT 1
            )
            AND (estatus = 'validado' OR estatus_flujo = 'tanqueo_registrado')
        """, (empresa_id,))
        ped_rows = cursor.fetchall()
        col_ped = [d[0] for d in cursor.description] if cursor.description else []

        tanqueos_por_granja = {}
        todos_pts_tanqueo = []

        for p_row in ped_rows:
            p = dict(zip(col_ped, p_row)) if not isinstance(p_row, dict) else p_row
            ubi = p.get('ubicacion') or 'General'
            f_sol = p.get('fecha_solicitud') or p.get('fecha_registro')
            f_val = p.get('fecha_validacion')

            if not f_sol or not f_val:
                continue

            if isinstance(f_sol, datetime):
                dt_ini = f_sol
            elif hasattr(f_sol, 'year'):
                dt_ini = datetime(f_sol.year, f_sol.month, f_sol.day, 8, 0, 0)
            else:
                continue

            if isinstance(f_val, datetime):
                dt_fin = f_val
            elif hasattr(f_val, 'year'):
                dt_fin = datetime(f_val.year, f_val.month, f_val.day, 12, 0, 0)
            else:
                continue

            delta_horas = (dt_fin - dt_ini).total_seconds() / 3600.0
            if delta_horas < 0:
                delta_horas = 0.0

            if delta_horas <= 28.0:
                pts = 100.0
            elif delta_horas <= 48.0:
                pts = 70.0
            elif delta_horas <= 72.0:
                pts = 40.0
            else:
                pts = 0.0

            if ubi not in tanqueos_por_granja:
                tanqueos_por_granja[ubi] = []
            tanqueos_por_granja[ubi].append(pts)
            todos_pts_tanqueo.append(pts)

        # 3. Consolidación ponderada de KPI Op (50% Tanqueo + 50% Horario de Campo)
        todas_sedes_kpi = set(list(ops_por_granja.keys()) + list(tanqueos_por_granja.keys()))

        kpi_por_granja = {}
        for s in todas_sedes_kpi:
            op_data = ops_por_granja.get(s, {'total': 0, 'diurnas': 0})
            p_horario = (op_data['diurnas'] / op_data['total'] * 100.0) if op_data['total'] > 0 else 100.0

            t_pts = tanqueos_por_granja.get(s, [])
            if t_pts:
                p_tanqueo = sum(t_pts) / len(t_pts)
                kpi_final = (p_tanqueo * 0.50) + (p_horario * 0.50)
            else:
                p_tanqueo = None
                kpi_final = p_horario

            kpi_por_granja[s] = {
                'promedio': round(p_tanqueo if p_tanqueo is not None else 0.0, 1),
                'cumplimiento': round(kpi_final, 1)
            }

        p_horario_global = (diurnas_ops_empresa / total_ops_empresa * 100.0) if total_ops_empresa > 0 else 100.0
        if todos_pts_tanqueo:
            p_tanqueo_global = sum(todos_pts_tanqueo) / len(todos_pts_tanqueo)
            kpi_global = (p_tanqueo_global * 0.50) + (p_horario_global * 0.50)
        else:
            p_tanqueo_global = 0.0
            kpi_global = p_horario_global

        kpi_tecnicos = {
            'promedio': round(p_tanqueo_global, 1),
            'cumplimiento': round(kpi_global, 1)
        }
        # =========================================================================

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
            
        if clima_filtro:
            wheres.append("AND s.clima = %s")
            params.append(clima_filtro)
        if tecnologia_filtro:
            wheres.append("AND s.tecnologia_produccion = %s")
            params.append(tecnologia_filtro)

        sql = """
            SELECT c.fecha, c.ubicacion, c.lote, c.estatus_lote, c.operacion, c.clase, 
                   c.saldo_estimado_kg, c.saldo_estimado_galones,
                   c.pollitos, c.kg_pollito, c.velocidad_consumo, c.masa_kg_facturada, c.neto_gastado,
                   c.precio_total, c.precio_unitario, c.proveedor, c.porcentaje_diferencia,
                   s.clima, s.tecnologia_produccion as tecnologia, s.kpi_eficiencia
            FROM cardex_glp c
            LEFT JOIN sedes_empresas s ON c.id_empresa = s.empresa_id AND TRIM(UPPER(c.ubicacion)) = TRIM(UPPER(s.nombre_sede))
        """
        final_sql = f"{sql} {' '.join(wheres)} ORDER BY c.fecha ASC"
        
        cursor.execute(final_sql, tuple(params))
        rows = cursor.fetchall()

        raw_results = []
        if rows:
            columns = [col[0] for col in cursor.description]
            for row in rows:
                if isinstance(row, dict):
                    r_dict = dict(row)
                else:
                    r_dict = dict(zip(columns, row))
                
                kpi_json = r_dict.get('kpi_eficiencia')
                r_dict['limite_inferior'] = 0.0
                r_dict['limite_superior'] = 0.0
                try:
                    if kpi_json:
                        kpis = json.loads(kpi_json) if isinstance(kpi_json, str) else kpi_json
                        if isinstance(kpis, dict) and 'GLP' in kpis:
                            r_dict['limite_inferior'] = float(kpis['GLP'].get('min', 0.0))
                            r_dict['limite_superior'] = float(kpis['GLP'].get('max', 0.0))
                except:
                    pass
                raw_results.append(r_dict)

        lotes_activos = list(set([r['lote'] for r in raw_results if r.get('lote')]))
        
        mapa_pob_rescatada = {}
        if lotes_activos:
            placeholders_lotes = ', '.join(['%s'] * len(lotes_activos))
            sql_rescate = f"""
                SELECT lote, pollitos 
                FROM cardex_glp 
                WHERE operacion = 'inicio_calefaccion' 
                  AND id_empresa = %s
                  AND lote IN ({placeholders_lotes})
            """
            cursor.execute(sql_rescate, tuple([empresa_id] + lotes_activos))
            for p_row in cursor.fetchall():
                l_id = p_row['lote'] if isinstance(p_row, dict) else p_row[0]
                raw_qty = p_row['pollitos'] if isinstance(p_row, dict) else p_row[1]
                p_qty = int(raw_qty) if raw_qty is not None else 0
                mapa_pob_rescatada[l_id] = p_qty

        cursor.close()

        datos = _procesar_resultados_glp(raw_results, tipo_informe, periodo, mapa_pob_rescatada, kpi_tecnicos, kpi_por_granja)
        if not datos or not datos.get('tabla_resumen'):
            return jsonify({"success": False, "message": "No hay datos operativos válidos para mostrar."})

        return jsonify({"success": True, "data": datos})

    except Exception as e:
        if cursor: cursor.close()
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500
    
@bp_energia_glp.route('/glp/admin/descargar_informe_pdf', methods=['POST'])
@login_required_custom
def descargar_informe_pdf():
    data = request.get_json() or {}
    empresa = session.get('empresa', 'N/D')
    nit = session.get('empresa_id', '')
    
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    
    styles = getSampleStyleSheet()
    title_style = styles['Heading1']
    title_style.alignment = 1

    base_dir = current_app.static_folder
    logo_bqa = os.path.join(base_dir, 'logo_energix360.png')
    logo_cliente = os.path.join(base_dir, f'logo_{nit}.png')
    
    img_bqa = RLImage(logo_bqa, width=1.5*inch, height=0.5*inch) if os.path.exists(logo_bqa) else ""
    img_cliente = RLImage(logo_cliente, width=1.5*inch, height=0.5*inch) if os.path.exists(logo_cliente) else ""
    
    header_table = Table([[img_cliente, Paragraph(f"Informe Analítico y Financiero GLP - {empresa}", title_style), img_bqa]], colWidths=[2*inch, 5*inch, 2*inch])
    header_table.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    
    elements.append(header_table)
    elements.append(Spacer(1, 20))
    
    table_data = [['Granja', 'Clima', 'Tecnología', 'Rango Esp.', 'Consumo (kg)', 'Eficiencia', 'Ahorro / Sobrecosto ($)', 'Proveedor', 'Desv. Media', 'KPI Operador']]
    for row in data.get('tabla_resumen', []):
        ahorro = row.get('ahorro', 0)
        sobrecosto = row.get('sobrecosto', 0)
        ahorro_val = f"+${ahorro:,.0f}" if ahorro > 0 else (f"-${sobrecosto:,.0f}" if sobrecosto > 0 else "$0")
        
        table_data.append([
            row.get('granja'), row.get('clima'), row.get('tecnologia'), row.get('rango_esperado'),
            f"{row.get('total_kg', 0):.2f}", f"{row.get('kg_pollito', 0):.4f}", 
            ahorro_val, row.get('proveedor'), f"{row.get('media_desviacion', 0):.2f}%", f"{row.get('kpi_operador', 0):.1f}%"
        ])
    
    t = Table(table_data, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#015249")),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 12),
        ('BACKGROUND', (0,1), (-1,-1), colors.beige),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ]))
    
    elements.append(t)
    doc.build(elements)
    buffer.seek(0)
    
    return send_file(buffer, as_attachment=True, download_name=f"Informe_GLP_{empresa}.pdf", mimetype='application/pdf')

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
        
        col_names = [d[0] for d in cur.description] if cur.description else []
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
                SELECT fecha, operacion,
                       `nivel tk-1`, `nivelfinal tk-1`, `capacidad tk-1`,
                       `nivel tk-2`, `nivelfinal tk-2`, `capacidad tk-2`,
                       `nivel tk-3`, `nivelfinal tk-3`, `capacidad tk-3`,
                       `nivel tk-4`, `nivelfinal tk-4`, `capacidad tk-4`,
                       `nivel tk-5`, `nivelfinal tk-5`, `capacidad tk-5`,
                       `nivel tk-6`, `nivelfinal tk-6`, `capacidad tk-6`,
                       `nivel tk-7`, `nivelfinal tk-7`, `capacidad tk-7`,
                       `nivel tk-8`, `nivelfinal tk-8`, `capacidad tk-8`,
                       `nivel tk-9`, `nivelfinal tk-9`, `capacidad tk-9`,
                       `nivel tk-10`, `nivelfinal tk-10`, `capacidad tk-10`,
                       `nivel tk-11`, `nivelfinal tk-11`, `capacidad tk-11`
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
                for i in range(1, 12):
                    tk_key = f'tk-{i}'
                    nivel_col_ini = f'nivel tk-{i}'
                    nivel_col_fin = f'nivelfinal tk-{i}'
                    cap_col = f'capacidad tk-{i}'
                    
                    if tk_key in tanques_estado: continue 
                        
                    nivel_fin_val = row.get(nivel_col_fin)
                    nivel_ini_val = row.get(nivel_col_ini)
                    nivel_val = nivel_fin_val if nivel_fin_val is not None else nivel_ini_val

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
                
                for i in range(1, 12):
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
                MIN(COALESCE(fecha_llegada_pollitos, fecha)) as fecha_inicio,
                MAX(dias_operacion) as dias_abierto,
                MAX(fecha) as ultima_actividad
            FROM cardex_glp
            WHERE id_empresa = %s AND estatus_lote = 'ACTIVO'
            GROUP BY ubicacion, lote
            HAVING MAX(dias_operacion) > 15
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
              AND TRIM(UPPER(p.cliente)) COLLATE utf8mb4_general_ci = (
                  SELECT TRIM(UPPER(nombre_comercial)) COLLATE utf8mb4_general_ci FROM empresas WHERE nit = %s LIMIT 1
              )
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
# EDICIÓN DE TANQUE Y GENERACIÓN DE QR
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
        cur.execute("""
            SELECT ts.*, s.clima, s.tecnologia_produccion as tecnologia, s.kpi_eficiencia 
            FROM tanques_sedes ts
            LEFT JOIN sedes_empresas s ON ts.empresa_id = s.empresa_id AND TRIM(UPPER(ts.ubicacion)) = TRIM(UPPER(s.nombre_sede))
            WHERE ts.empresa_id = %s AND ts.ubicacion = %s ORDER BY ts.id ASC
        """, (empresa_id, ubicacion))
        filas = cur.fetchall()
        
        if not filas:
            return jsonify({'success': False, 'message': 'No se encontró configuración para esta granja.'}), 404
            
        kpi_json = filas[0].get('kpi_eficiencia')
        lim_inf = 0.0
        lim_sup = 0.0
        try:
            if kpi_json:
                kpis = json.loads(kpi_json) if isinstance(kpi_json, str) else kpi_json
                if isinstance(kpis, dict) and 'GLP' in kpis:
                    lim_inf = float(kpis['GLP'].get('min', 0.0))
                    lim_sup = float(kpis['GLP'].get('max', 0.0))
        except:
            pass
        
        general = {
            'proveedor': filas[0]['proveedor'],
            'email': filas[0]['email'],
            'zona': filas[0]['zona'],
            'propietario': filas[0]['propietario'],
            'empresa': filas[0]['empresa'],
            'clima': filas[0].get('clima', 'Cálido'),
            'tecnologia': filas[0].get('tecnologia', 'Convencional'),
            'limite_inferior': lim_inf,
            'limite_superior': lim_sup
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
    clima = data.get('clima', 'Cálido')
    tecnologia = data.get('tecnologia', 'Convencional')
    limite_inferior = float(data.get('limite_inferior', 0))
    limite_superior = float(data.get('limite_superior', 0))
    zona = data.get('zona', 'N/D')
    propietario = data.get('propietario', 'Propio')
    
    usuario_cedula = session.get('cedula', session.get('user_id', '0'))
    usuario_nombre = session.get('nombre', 'Sistema')
    
    if not tanques or len(tanques) == 0:
        return jsonify({'success': False, 'message': 'Debe haber al menos un tanque registrado.'}), 400
        
    for intento in range(3):
        try:
            cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
            
            cur.execute("SELECT nombre_comercial FROM empresas WHERE nit = %s LIMIT 1", (empresa_id,))
            emp_row = cur.fetchone()
            empresa_nombre = emp_row['nombre_comercial'] if emp_row else 'Empresa Desconocida'

            cur.execute("SELECT zona, propietario FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s LIMIT 1 FOR UPDATE", (empresa_id, ubicacion))
            row = cur.fetchone()
            
            if row:
                zona_bd = row['zona']
                propietario_bd = row['propietario']
            else:
                zona_bd = zona
                propietario_bd = propietario
                
            cur.execute("DELETE FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s", (empresa_id, ubicacion))
            
            for tk in tanques:
                cur.execute("""
                    INSERT INTO tanques_sedes (empresa, empresa_id, nombre_tanque, propietario, capacidad_gls, ubicacion, zona, proveedor, email)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (empresa_nombre, empresa_id, tk['nombre'], propietario_bd, tk['capacidad'], ubicacion, zona_bd, proveedor, email))
                
            cur.execute("SELECT kpi_eficiencia FROM sedes_empresas WHERE empresa_id = %s AND TRIM(UPPER(nombre_sede)) = TRIM(UPPER(%s)) FOR UPDATE", (empresa_id, ubicacion))
            row_sede = cur.fetchone()
            if row_sede:
                kpi_json = row_sede['kpi_eficiencia'] if isinstance(row_sede, dict) else row_sede[0]
                try: 
                    kpis = json.loads(kpi_json) if isinstance(kpi_json, str) else kpi_json
                except: 
                    kpis = {}
                if not isinstance(kpis, dict): kpis = {}
                if 'GLP' not in kpis: kpis['GLP'] = {}
                kpis['GLP']['min'] = limite_inferior
                kpis['GLP']['max'] = limite_superior
                
                cur.execute("UPDATE sedes_empresas SET clima=%s, tecnologia_produccion=%s, kpi_eficiencia=%s WHERE empresa_id=%s AND TRIM(UPPER(nombre_sede))=TRIM(UPPER(%s))",
                            (clima, tecnologia, json.dumps(kpis), empresa_id, ubicacion))
            else:
                kpis = {"GLP": {"min": limite_inferior, "max": limite_superior}}
                cur.execute("INSERT INTO sedes_empresas (empresa_id, empresa_nombre, nombre_sede, tipo_sede, clima, tecnologia_produccion, kpi_eficiencia) VALUES (%s, %s, %s, 'granja', %s, %s, %s)",
                            (empresa_id, empresa_nombre, ubicacion, clima, tecnologia, json.dumps(kpis)))
                
            _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "guardar_tanques_granja", "ubicacion": ubicacion, "clima": clima, "tecnologia": tecnologia})
                
            mysql.connection.commit()
            cur.close()
            return jsonify({'success': True, 'message': f'Configuración de granja {ubicacion} guardada correctamente.'})
            
        except MySQLdb.OperationalError as e:
            if e.args[0] == 1213:
                mysql.connection.rollback()
                time.sleep(0.5)
                if intento == 2: return jsonify({'success': False, 'message': 'Conflicto de red.'}), 500
            else:
                mysql.connection.rollback()
                return jsonify({'success': False, 'message': str(e)}), 500
        except Exception as e:
            mysql.connection.rollback()
            return jsonify({'success': False, 'message': str(e)}), 500

@csrf.exempt
@bp_energia_glp.route('/glp/admin/eliminar_granja', methods=['POST'])
@login_required_custom
def eliminar_granja():
    data = request.get_json()
    empresa_id = data.get('empresa_id')
    ubicacion = data.get('ubicacion')
    usuario_cedula = session.get('cedula', session.get('user_id', '0'))
    usuario_nombre = session.get('nombre', 'Sistema')
    
    try:
        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM tanques_sedes WHERE empresa_id = %s AND ubicacion = %s", (empresa_id, ubicacion))
        cur.execute("DELETE FROM sedes_empresas WHERE empresa_id = %s AND nombre_sede = %s", (empresa_id, ubicacion))
        
        _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "eliminar_granja", "ubicacion": ubicacion})
        mysql.connection.commit()
        cur.close()
        return jsonify({'success': True, 'message': f'Granja {ubicacion} eliminada correctamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ==============================================================================
# CRUD PROVEEDORES
# ==============================================================================

@csrf.exempt
@bp_energia_glp.route('/glp/admin/proveedores/listar', methods=['POST'])
@login_required_custom
def listar_proveedores():
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM proveedores ORDER BY proveedor ASC")
        rows = cur.fetchall()
        cur.close()
        return jsonify({'success': True, 'proveedores': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@csrf.exempt
@bp_energia_glp.route('/glp/admin/proveedores/guardar', methods=['POST'])
@login_required_custom
def guardar_proveedor():
    data = request.get_json()
    proveedor = data.get('proveedor')
    precio = data.get('precio', 0)
    email1 = data.get('email1', '')
    email2 = data.get('email2', '')
    empresa_id = session.get('empresa_id', 0)
    usuario_cedula = session.get('cedula', session.get('user_id', '0'))
    usuario_nombre = session.get('nombre', 'Sistema')
    
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            INSERT INTO proveedores (proveedor, precio, email1, email2)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE precio=VALUES(precio), email1=VALUES(email1), email2=VALUES(email2)
        """, (proveedor, precio, email1, email2))
        
        _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "guardar_proveedor", "proveedor": proveedor, "precio": precio})
        mysql.connection.commit()
        cur.close()
        return jsonify({'success': True, 'message': f'Proveedor {proveedor} guardado correctamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

@csrf.exempt
@bp_energia_glp.route('/glp/admin/proveedores/eliminar', methods=['POST'])
@login_required_custom
def eliminar_proveedor():
    data = request.get_json()
    proveedor = data.get('proveedor')
    empresa_id = session.get('empresa_id', 0)
    usuario_cedula = session.get('cedula', session.get('user_id', '0'))
    usuario_nombre = session.get('nombre', 'Sistema')
    
    try:
        cur = mysql.connection.cursor()
        cur.execute("DELETE FROM proveedores WHERE proveedor = %s", (proveedor,))
        _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "eliminar_proveedor", "proveedor": proveedor})
        mysql.connection.commit()
        cur.close()
        return jsonify({'success': True, 'message': f'Proveedor {proveedor} eliminado correctamente.'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ==============================================================================
# EMPRESAS URE
# ==============================================================================

@csrf.exempt
@bp_energia_glp.route('/glp/admin/obtener_empresas_ure', methods=['POST'])
@login_required_custom
def obtener_empresas_ure():
    data = request.get_json()
    tipo_ure = data.get('tipo_ure', 'URE_GLP')
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        valid_columns = ['URE_GLP', 'URE_COMBUSTIBLE', 'URE_ELECTRICIDAD', 'URE_CARBON', 'URE_SOLAR']
        if tipo_ure not in valid_columns:
            tipo_ure = 'URE_GLP'
            
        query = f"SELECT nit, nombre_comercial FROM empresas WHERE {tipo_ure} = 1 ORDER BY nombre_comercial ASC"
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        return jsonify({'success': True, 'empresas': rows})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# ==============================================================================
# GENERACIÓN DE CÓDIGOS QR EN FORMATO PDF (INDIVIDUAL Y MASIVO)
# ==============================================================================

@bp_energia_glp.route('/generar_qrs_pdf', methods=['POST'])
@login_required_custom
def generar_qrs_pdf():
    try:
        data = request.get_json(silent=True) or {}
        empresa_id = data.get('empresa_id') or session.get('empresa_id')
        ubicacion_filtro = data.get('ubicacion')

        if not empresa_id:
            return jsonify({'success': False, 'message': 'Identificador de empresa no detectado o sesión inválida.'}), 400

        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        if ubicacion_filtro:
            query = """
                SELECT * FROM tanques_sedes 
                WHERE empresa_id = %s AND ubicacion = %s 
                ORDER BY id ASC
            """
            cur.execute(query, (empresa_id, ubicacion_filtro))
        else:
            query = """
                SELECT * FROM tanques_sedes 
                WHERE empresa_id = %s 
                ORDER BY ubicacion ASC, id ASC
            """
            cur.execute(query, (empresa_id,))
            
        tanques_data = cur.fetchall()
        cur.close()

        if not tanques_data:
            nombre_lugar = f"la granja '{ubicacion_filtro}'" if ubicacion_filtro else "esta empresa"
            return jsonify({
                'success': False, 
                'message': f'No hay tanques registrados para {nombre_lugar}. Configure los tanques primero.'
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
        return jsonify({'success': False, 'message': f'Error interno generando el PDF: {str(e)}'}), 500
    
# ==============================================================================
# RUTAS DE ADMINISTRACIÓN: OBTENER, ANÁLISIS Y APROBACIÓN
# ==============================================================================
@csrf.exempt
@bp_energia_glp.route('/glp/admin/obtener_solicitudes_pendientes', methods=['POST'])
@login_required_custom
def admin_obtener_solicitudes():
    try:
        data = request.get_json(silent=True) or {}
        empresa_id_req = data.get('empresa_id')
        session_id = str(session.get('empresa_id', '')).strip()
        empresa_id = empresa_id_req if empresa_id_req else session_id

        if not empresa_id:
            return jsonify({"success": False, "message": "ID Empresa no identificado"})

        cur = mysql.connection.cursor()
        
        es_webmaster = (session_id == '901811727')
        filtrar_empresa = bool(empresa_id_req and str(empresa_id_req).strip() != '901811727')
        
        nombres_clean = set()
        
        if not es_webmaster or filtrar_empresa:
            try:
                cur.execute("SELECT nombre_comercial FROM empresas WHERE nit = %s", (empresa_id,))
                for row in cur.fetchall():
                    val = row['nombre_comercial'] if isinstance(row, dict) else row[0]
                    if val: nombres_clean.add(str(val).upper().replace('.', '').replace(' ', ''))
            except Exception:
                pass
                
            try:
                cur.execute("SELECT DISTINCT empresa FROM cardex_glp WHERE id_empresa = %s", (empresa_id,))
                for row in cur.fetchall():
                    val = row['empresa'] if isinstance(row, dict) else row[0]
                    if val: nombres_clean.add(str(val).upper().replace('.', '').replace(' ', ''))
            except Exception:
                pass
                
            if session.get('empresa'):
                nombres_clean.add(str(session.get('empresa')).upper().replace('.', '').replace(' ', ''))
                
            nombres_clean.add(str(empresa_id).upper().replace('.', '').replace(' ', ''))

        cur.execute("""
            SELECT id, fecha_registro, cliente, ubicacion, lote, nivel_solicitado, dias_extra 
            FROM pedidos_gas_glp 
            WHERE estatus_flujo = 'pendiente_aprobacion' 
              AND estatus = 'generado'
            ORDER BY fecha_registro DESC
        """)
        pedidos_pendientes = cur.fetchall()
        
        items = []
        col_pedidos = [d[0] for d in cur.description] if cur.description else []
        
        for p_row in pedidos_pendientes:
            p = dict(zip(col_pedidos, p_row)) if not isinstance(p_row, dict) else p_row
            
            if not es_webmaster or filtrar_empresa:
                cliente_str = str(p['cliente'] or '').upper().replace('.', '').replace(' ', '')
                if cliente_str not in nombres_clean:
                    continue 

            dias_op = 0
            nivel_tk1 = None
            foto_tk1 = None
            
            try:
                if p.get('lote'):
                    cur.execute("""
                        SELECT dias_operacion, `nivel tk-1`, `testigo nivel tk-1` 
                        FROM cardex_glp 
                        WHERE lote = %s AND operacion IN ('consumo','inicio_calefaccion') 
                        ORDER BY id DESC LIMIT 1
                    """, (p['lote'],))
                elif p.get('ubicacion'):
                    cur.execute("""
                        SELECT dias_operacion, `nivel tk-1`, `testigo nivel tk-1` 
                        FROM cardex_glp 
                        WHERE TRIM(ubicacion) = TRIM(%s) AND operacion IN ('consumo','inicio_calefaccion') 
                        ORDER BY id DESC LIMIT 1
                    """, (p['ubicacion'],))
                else:
                    cur.execute("SELECT 0, NULL, NULL")

                c_row = cur.fetchone()
                if c_row:
                    c = dict(zip([d[0] for d in cur.description], c_row)) if not isinstance(c_row, dict) else c_row
                    dias_op = c.get('dias_operacion') or 0
                    nivel_tk1 = c.get('nivel tk-1')
                    foto_tk1 = c.get('testigo nivel tk-1')
            except Exception:
                pass

            tk_info = []
            if nivel_tk1 is not None:
                tk_info.append({"numero": "Ref", "nivel": nivel_tk1, "foto": foto_tk1})

            items.append({
                "id": p.get('id'), 
                "fecha": str(p.get('fecha_registro')), 
                "cliente": p.get('cliente'), 
                "ubicacion": p.get('ubicacion'), 
                "lote": p.get('lote') or 'Sin Lote', 
                "dias_operacion": dias_op, 
                "nivel_solicitado": float(p.get('nivel_solicitado') or 0), 
                "solicitado": float(p.get('nivel_solicitado') or 0),
                "dias_extra": p.get('dias_extra') or 0, 
                "tanques": tk_info
            })
            
        cur.close()
        return jsonify({"success": True, "items": items})
        
    except Exception as e:
        import traceback
        print("❌ Error de lectura solicitudes pendientes:\n", traceback.format_exc())
        return jsonify({"success": False, "message": str(e)})

@csrf.exempt
@bp_energia_glp.route('/glp/admin/analizar_proyeccion', methods=['POST'])
@login_required_custom
def admin_analizar_proyeccion():
    data = request.get_json(force=True) or {}
    ped_id = data.get('id')
    empresa_id_req = data.get('empresa_id')
    
    empresa_id = empresa_id_req if empresa_id_req else session.get('empresa_id')
    session_id = str(session.get('empresa_id', '')).strip()

    try:
        cur = mysql.connection.cursor()
        
        if session_id == '901811727':
            cur.execute("""
                SELECT lote, cliente, ubicacion, nivel_solicitado, dias_extra 
                FROM pedidos_gas_glp 
                WHERE id=%s
            """, (ped_id,))
        else:
            cur.execute("""
                SELECT lote, cliente, ubicacion, nivel_solicitado, dias_extra 
                FROM pedidos_gas_glp 
                WHERE id=%s AND TRIM(UPPER(cliente)) COLLATE utf8mb4_general_ci = (
                    SELECT TRIM(UPPER(nombre_comercial)) COLLATE utf8mb4_general_ci 
                    FROM empresas 
                    WHERE nit = %s LIMIT 1
                )
            """, (ped_id, empresa_id))
            
        head = cur.fetchone()
        
        if not head: 
            cur.close()
            return jsonify({"success": False, "message": "Pedido no encontrado o sin permisos de acceso."})
            
        if isinstance(head, dict):
            lote = head['lote']
            solicitado = float(head.get('nivel_solicitado') or 0)
            dias_extra = int(head.get('dias_extra') or 0)
        else:
            lote = head[0]
            solicitado = float(head[3] or 0)
            dias_extra = int(head[4] or 0)
        
        cur.execute("""
            SELECT fecha, `nivel tk-1` FROM cardex_glp 
            WHERE lote=%s ORDER BY fecha DESC LIMIT 5
        """, (lote,))
        rows_hist = cur.fetchall()
        tasa = 8.0
        if rows_hist and len(rows_hist) > 1:
            deltas = []
            for i in range(len(rows_hist)-1):
                f1 = rows_hist[i]['fecha'] if isinstance(rows_hist[i], dict) else rows_hist[i][0]
                n1 = float((rows_hist[i]['nivel tk-1'] if isinstance(rows_hist[i], dict) else rows_hist[i][1]) or 0)
                
                f2 = rows_hist[i+1]['fecha'] if isinstance(rows_hist[i+1], dict) else rows_hist[i+1][0]
                n2 = float((rows_hist[i+1]['nivel tk-1'] if isinstance(rows_hist[i+1], dict) else rows_hist[i+1][1]) or 0)
                
                diff_days = (f1 - f2).days
                if diff_days > 0:
                    diff_niv = n2 - n1
                    if diff_niv > 0: deltas.append(diff_niv / diff_days)
            if deltas: tasa = sum(deltas)/len(deltas)

        cur.execute("SELECT `nivel tk-1`, dias_operacion, fecha FROM cardex_glp WHERE lote=%s ORDER BY id DESC LIMIT 1", (lote,))
        curr = cur.fetchone()
        
        niv_act = float(curr[0] or 0) if curr else 0
        dia_act = int(curr[1] or 0) if curr else 0
        fecha_ultima = curr[2] if curr else datetime.now().date()
        if isinstance(fecha_ultima, datetime): fecha_ultima = fecha_ultima.date()
        
        from datetime import timedelta
        import holidays
        co_holidays = holidays.CO() 
        
        puntos_grafica = []
        fecha_sim = fecha_ultima
        
        # PARADIGMA OBJETIVO: Arranque de simulación directo en la meta
        nivel_sim = solicitado
        if nivel_sim > 100: nivel_sim = 100
        
        ciclo_total = 15 + dias_extra
        dias_a_proyectar = (ciclo_total - dia_act) + 4 

        for d in range(1, dias_a_proyectar + 1):
            fecha_sim += timedelta(days=1)
            
            es_festivo = (fecha_sim in co_holidays) or (fecha_sim.weekday() >= 5) 
            factor_consumo = 0.2 if es_festivo else 1.0 
            consumo_dia = tasa * factor_consumo
            
            nivel_sim -= consumo_dia
            if nivel_sim < 0: nivel_sim = 0
            
            puntos_grafica.append({
                "dia_ciclo": dia_act + d,
                "fecha_str": fecha_sim.strftime("%d/%m"),
                "nivel": round(nivel_sim, 1),
                "es_festivo": es_festivo
            })

        return jsonify({
            "success": True, 
            "nivel_actual": niv_act, 
            "tasa_descenso_diaria": round(tasa, 2), 
            "dia_actual": dia_act, 
            "dias_extra_aprobados": dias_extra,
            "solicitado_original": solicitado,
            "proyeccion_inteligente": puntos_grafica, 
            "ciclo_meta": ciclo_total
        })
        
    except Exception as e: 
        return jsonify({"success": False, "message": str(e)})

def _enviar_correo_aprobado_proveedor(pedido_id, nivel_aprobado, empresa_id):
    if not EMAIL_USER or not EMAIL_PASS:
        return False

    try:
        cur = mysql.connection.cursor()
        
        session_id = str(session.get('empresa_id', '')).strip()
        
        if session_id == '901811727':
            cur.execute("""
                SELECT cliente, ubicacion, lote, codigo_pedido, proveedor 
                FROM pedidos_gas_glp 
                WHERE id=%s
            """, (pedido_id,))
        else:
            cur.execute("""
                SELECT cliente, ubicacion, lote, codigo_pedido, proveedor 
                FROM pedidos_gas_glp 
                WHERE id=%s AND TRIM(UPPER(cliente)) COLLATE utf8mb4_general_ci = (
                    SELECT TRIM(UPPER(nombre_comercial)) COLLATE utf8mb4_general_ci FROM empresas WHERE nit = %s LIMIT 1
                )
            """, (pedido_id, empresa_id))
            
        res = cur.fetchone()
        
        if not res:
            cur.close()
            return False
            
        if isinstance(res, dict):
            emp, ubi, lot, cod, prov = res['cliente'], res['ubicacion'], res['lote'], res['codigo_pedido'], res['proveedor']
        else:
            emp, ubi, lot, cod, prov = res[0], res[1], res[2], res[3], res[4]

        cur.execute("SELECT `nivel tk-1`, `nivel tk-2`, `nivel tk-3`, `nivel tk-4`, `nivel tk-5`, `nivel tk-6` FROM cardex_glp WHERE lote = %s ORDER BY id DESC LIMIT 1", (lot,))
        row_niveles = cur.fetchone()
        
        nivel_actual_promedio = 0.0
        if row_niveles:
            valores_validos = []
            if isinstance(row_niveles, dict):
                for i in range(1, 7):
                    val = row_niveles.get(f'nivel tk-{i}')
                    if val is not None: valores_validos.append(float(val))
            else:
                for val in row_niveles:
                    if val is not None: valores_validos.append(float(val))
            
            if valores_validos:
                nivel_actual_promedio = sum(valores_validos) / len(valores_validos)
                
        # PARADIGMA OBJETIVO: Nivel final directo
        try: 
            nivel_objetivo = float(nivel_aprobado)
        except: 
            nivel_objetivo = 80.0
            
        if nivel_objetivo > 80.0: 
            nivel_objetivo = 80.0
        
        cur.execute("SELECT email1, email2 FROM proveedores WHERE proveedor=%s", (prov,))
        pdat = cur.fetchone()
        cur.close()
        
        emails = []
        if pdat:
            if isinstance(pdat, dict):
                if pdat.get('email1'): emails.append(pdat.get('email1'))
                if pdat.get('email2'): emails.append(pdat.get('email2'))
            else:
                if pdat[0]: emails.append(pdat[0])
                if pdat[1]: emails.append(pdat[1])
        
        if not emails:
            emails = [EMAIL_USER]

        cuerpo = f"""
        <!DOCTYPE html>
        <html lang="es">
        <body style="background-color: #f4f4f4; padding: 20px; font-family: Arial, sans-serif;">
            <div style="background-color: #ffffff; max-width: 500px; margin: 0 auto; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); overflow: hidden;">
                <div style="background-color: #015249; color: white; padding: 20px; text-align: center;">
                    <h2 style="margin: 0; font-size: 22px;">Orden de Suministro GLP</h2>
                    <p style="margin: 5px 0 0 0; opacity: 0.9; font-size: 14px;">Solicitud Aprobada por Webmaster</p>
                </div>
                <div style="padding: 25px;">
                    <div style="background-color: #e8f5e9; border: 2px dashed #015249; padding: 15px; text-align: center; margin-bottom: 25px; border-radius: 6px;">
                        <div style="font-size: 11px; text-transform: uppercase; color: #555; letter-spacing: 1px; margin-bottom: 5px;">Código de Pedido</div>
                        <div style="font-size: 26px; font-weight: 800; color: #015249;">{cod}</div>
                    </div>
                    <p style="color: #333; margin-bottom: 15px;">Se autoriza el despacho de gas para:</p>
                    <table style="width: 100%; border-collapse: collapse; font-size: 15px;">
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 10px 0; color: #666;">🏢 Cliente:</td>
                            <td style="padding: 10px 0; font-weight: bold; text-align: right; color: #333;">{emp}</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #eee;">
                            <td style="padding: 10px 0; color: #666;">📍 Sede:</td>
                            <td style="padding: 10px 0; font-weight: bold; text-align: right; color: #333;">{ubi}</td>
                        </tr>
                        <tr>
                            <td style="padding: 10px 0; color: #666;">📊 Nivel Objetivo en Manómetro:</td>
                            <td style="padding: 10px 0; font-weight: bold; text-align: right; color: #015249; font-size: 18px;">{round(nivel_objetivo, 2)}%</td>
                        </tr>
                        <tr>
                            <td colspan="2" style="padding-bottom: 10px; font-size: 12px; color: #888; text-align: right;">
                                <em>(Nivel actual estimado de la granja: {round(nivel_actual_promedio, 1)}%)</em>
                            </td>
                        </tr>
                    </table>
                    <div style="margin-top: 25px; padding: 10px; background-color: #fff8e1; border-left: 4px solid #ffc107; font-size: 13px; color: #795548;">
                        <strong>Instrucción:</strong> Favor suministrar gas hasta alcanzar el Nivel Objetivo detallado ({round(nivel_objetivo, 2)}%). Incluir el código de pedido en la factura. No sobrepasar la marca de seguridad (80%).
                    </div>
                </div>
            </div>
        </body>
        </html>
        """
        
        msg = MIMEMultipart()
        msg["Subject"] = f"✅ Orden de Suministro Aprobada: {cod} - {ubi}"
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(emails)
        
        from email.utils import make_msgid, formatdate
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = EMAIL_FROM

        msg.attach(MIMEText(cuerpo, "html", "utf-8"))

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as s:
            s.starttls()
            s.login(EMAIL_USER, EMAIL_PASS)
            s.sendmail(EMAIL_USER, emails, msg.as_string())
        
        return True
    except Exception as e:
        print(f"⛔ Error enviando correo GLP: {e}")
        return False

@csrf.exempt
@bp_energia_glp.route('/glp/admin/aprobar_solicitud', methods=['POST'])
@bp_energia_glp.route('/admin/aprobar_solicitud', methods=['POST'])
@login_required_custom
def admin_aprobar_solicitud():
    data = request.get_json(force=True) or {}
    ped_id = data.get('id')
    nivel = data.get('nivel_aprobado')
    empresa_id_req = data.get('empresa_id')
    
    empresa_id = empresa_id_req if empresa_id_req else session.get('empresa_id')
    session_id = str(session.get('empresa_id', '')).strip()
    usuario_cedula = session.get('cedula', session.get('user_id', '0'))
    usuario_nombre = session.get('nombre', 'Sistema')
    
    for intento in range(3):
        try:
            cur = mysql.connection.cursor()
            
            if session_id == '901811727':
                 cur.execute("SELECT id, nivel_solicitado FROM pedidos_gas_glp WHERE id=%s FOR UPDATE", (ped_id,))
            else:
                cur.execute("""
                    SELECT id, nivel_solicitado FROM pedidos_gas_glp 
                    WHERE id=%s AND TRIM(UPPER(cliente)) COLLATE utf8mb4_general_ci = (
                        SELECT TRIM(UPPER(nombre_comercial)) COLLATE utf8mb4_general_ci FROM empresas WHERE nit = %s LIMIT 1
                    ) FOR UPDATE
                """, (ped_id, empresa_id))
                
            row = cur.fetchone()
            if not row:
                cur.close()
                return jsonify({"success": False, "message": "Acceso denegado a este registro."})
            
            if not nivel: 
                nivel = row[1] if not isinstance(row, dict) else row.get('nivel_solicitado')
                
            cur.execute("UPDATE pedidos_gas_glp SET nivel_solicitado=%s, estatus_flujo='aprobado_webmaster' WHERE id=%s", (nivel, ped_id))
            
            _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "aprobar_solicitud_gas", "pedido_id": ped_id, "nivel_aprobado": nivel})
            
            mysql.connection.commit()
            cur.close()
            
            env = _enviar_correo_aprobado_proveedor(ped_id, nivel, empresa_id)
            
            return jsonify({
                "success": True, 
                "message": "Solicitud aprobada correctamente." + (" Proveedor notificado." if env else " Error enviando correo al proveedor.")
            })
            
        except MySQLdb.OperationalError as e:
            if e.args[0] == 1213:
                mysql.connection.rollback()
                time.sleep(0.5)
                if intento == 2: return jsonify({'success': False, 'message': 'Conflicto de red.'}), 500
            else:
                mysql.connection.rollback()
                return jsonify({'success': False, 'message': str(e)}), 500
        except Exception as e: 
            mysql.connection.rollback()
            return jsonify({"success": False, "message": str(e)})

@csrf.exempt
@bp_energia_glp.route('/glp/admin/rechazar_solicitud', methods=['POST'])
@login_required_custom
def rechazar_solicitud():
    try:
        data = request.get_json()
        id_solicitud = data.get('id')
        empresa_id_req = data.get('empresa_id')
        
        empresa_id = empresa_id_req if empresa_id_req else session.get('empresa_id')
        session_id = str(session.get('empresa_id', '')).strip()
        usuario_cedula = session.get('cedula', session.get('user_id', '0'))
        usuario_nombre = session.get('nombre', 'Sistema')

        if not id_solicitud:
            return jsonify({"success": False, "message": "ID de solicitud requerido."}), 400

        for intento in range(3):
            try:
                cur = mysql.connection.cursor()
                
                if session_id == '901811727':
                    cur.execute("SELECT id FROM pedidos_gas_glp WHERE id=%s FOR UPDATE", (id_solicitud,))
                else:
                    cur.execute("""
                        SELECT id FROM pedidos_gas_glp 
                        WHERE id=%s AND TRIM(UPPER(cliente)) COLLATE utf8mb4_general_ci = (
                            SELECT TRIM(UPPER(nombre_comercial)) COLLATE utf8mb4_general_ci FROM empresas WHERE nit = %s LIMIT 1
                        ) FOR UPDATE
                    """, (id_solicitud, empresa_id))
                    
                if not cur.fetchone():
                    cur.close()
                    return jsonify({"success": False, "message": "Acceso denegado a este registro."})
                
                query = "UPDATE pedidos_gas_glp SET estatus_flujo = 'rechazado' WHERE id = %s"
                cur.execute(query, (id_solicitud,))
                
                _registrar_operacion_global(cur, empresa_id, usuario_cedula, usuario_nombre, {"accion": "rechazar_solicitud", "pedido_id": id_solicitud})
                
                mysql.connection.commit()
                cur.close()

                return jsonify({"success": True, "message": "Solicitud anulada correctamente."})

            except MySQLdb.OperationalError as e:
                if e.args[0] == 1213:
                    mysql.connection.rollback()
                    time.sleep(0.5)
                    if intento == 2: return jsonify({'success': False, 'message': 'Conflicto de red.'}), 500
                else:
                    mysql.connection.rollback()
                    return jsonify({'success': False, 'message': str(e)}), 500
            except Exception as e:
                mysql.connection.rollback()
                import traceback
                traceback.print_exc()
                return jsonify({"success": False, "message": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500