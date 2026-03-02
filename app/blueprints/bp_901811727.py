from flask import Blueprint, render_template, session, request, jsonify, flash, redirect, url_for, current_app
from datetime import datetime
from app import mysql, csrf
from app.forms import RegistroUsuarioForm
from app.utils import login_required_custom
from flask_bcrypt import Bcrypt
import traceback
import math

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import holidays 

bcrypt = Bcrypt()

bp_901811727 = Blueprint('bp_901811727', __name__)
co_holidays = holidays.CO() 

# ==============================================================================
# RUTAS DE GESTIÓN (ADMINISTRACIÓN CRUD COMPLETA)
# ==============================================================================

@bp_901811727.route('/901811727.html')
@login_required_custom
def panel_webmaster():
    form = RegistroUsuarioForm()
    try:
        cur = mysql.connection.cursor()
        
        # 1. Obtener empresas incluyendo tipo_empresa
        cur.execute("SELECT nit, nombre_comercial, tipo_empresa FROM empresas")
        data_empresas = cur.fetchall()
        empresas = []
        if data_empresas:
            if isinstance(data_empresas[0], dict):
                empresas = data_empresas
            else:
                empresas = [{'nit': row[0], 'nombre_comercial': row[1], 'tipo_empresa': row[2] if len(row)>2 else 'general'} for row in data_empresas]

        # 2. Obtener la nueva lista de tipos de empresa
        cur.execute("SELECT tipo FROM tipos_empresa")
        data_tipos = cur.fetchall()
        tipos_empresa = [r['tipo'] if isinstance(r, dict) else r[0] for r in data_tipos]
        
        cur.execute("SELECT nombre_comercial FROM empresas")
        clientes = cur.fetchall()
        
        cur.close()
        
        # Pasamos la variable tipos_empresa al render_template
        return render_template('901811727.html', 
                               nombre=session.get('nombre'), 
                               empresa=session.get('empresa'), 
                               form=form, 
                               empresas=empresas, 
                               tipos_empresa=tipos_empresa,
                               clientes=clientes)
    except Exception as e:
        print("Error panel:", e)
        return "Error cargando panel", 500

@csrf.exempt
@bp_901811727.route('/registrar_empresa', methods=['POST'])
@login_required_custom
def registrar_empresa():
    nombre_comercial = request.form.get('nombre_comercial', '').strip()
    nit = request.form.get('nit', '').strip()
    tipo_empresa = request.form.get('tipo_empresa', 'general').strip()
    accion = request.form.get('accion', 'crear').strip()
    
    if not nombre_comercial or not nit:
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("SELECT * FROM empresas WHERE nit = %s", (nit,))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'La empresa ya existe.'})
            cur.execute("INSERT INTO empresas (nit, nombre_comercial, tipo_empresa) VALUES (%s, %s, %s)", (nit, nombre_comercial, tipo_empresa))
            msg = 'Empresa creada correctamente.'
        else:
            cur.execute("UPDATE empresas SET nombre_comercial=%s, tipo_empresa=%s WHERE nit=%s", (nombre_comercial, tipo_empresa, nit))
            msg = 'Empresa actualizada correctamente.'
            
        mysql.connection.commit()
        return jsonify({'success': True, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/registrar_perfil', methods=['POST'])
@login_required_custom
def registrar_perfil():
    empresa_nombre = request.form.get('empresa_select', '').strip()
    nit = request.form.get('nit', '').strip()
    operacion = request.form.get('operacion', '').strip()
    perfil = request.form.get('perfil', '').strip()
    accion = request.form.get('accion', 'crear').strip()
    pid = request.form.get('id', '')

    if not all([empresa_nombre, nit, operacion, perfil]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("SELECT nit FROM empresas WHERE nombre_comercial = %s", (empresa_nombre,))
            empresa = cur.fetchone()
            nit_db = empresa['nit'] if isinstance(empresa, dict) else (empresa[0] if empresa else None)
            if not empresa or str(nit_db) != nit:
                return jsonify({'success': False, 'message': 'Empresa no válida o NIT incorrecto.'})
                
            cur.execute("SELECT * FROM perfiles WHERE nit = %s AND operacion = %s AND perfil = %s", (nit, operacion, perfil))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'El perfil ya existe.'})
                
            cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil) VALUES (%s, %s, %s, %s)", (empresa_nombre, nit, operacion, perfil))
            msg = 'Perfil creado correctamente.'
        else:
            cur.execute("UPDATE perfiles SET operacion=%s, perfil=%s WHERE id=%s", (operacion, perfil, pid))
            msg = 'Perfil actualizado correctamente.'

        mysql.connection.commit()
        return jsonify({'success': True, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/obtener_perfiles')
@login_required_custom
def obtener_perfiles():
    empresa_id = request.args.get('empresa_id', '').strip()
    operacion = request.args.get('operacion', '').strip()
    if not empresa_id or not operacion:
        return jsonify({'perfiles': []})
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT DISTINCT perfil FROM perfiles WHERE nit = %s AND operacion = %s", (empresa_id, operacion))
        perfiles = [row['perfil'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
        cur.close()
        return jsonify({'perfiles': perfiles})
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'perfiles': []})

@csrf.exempt
@bp_901811727.route('/registrar_usuario', methods=['POST'])
@login_required_custom
def registrar_usuario():
    data = request.form
    cedula = data.get('cedula', '').strip()
    nombre = data.get('nombre', '').strip()
    password = data.get('password', '').strip()
    accion = data.get('accion', 'crear').strip()
    
    if accion == 'crear' and not all([cedula, nombre, password]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
        
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'El usuario ya existe.'})
            password_hashed = bcrypt.generate_password_hash(password).decode('utf-8')
            cur.execute("""
                INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa, telegram_id, telefono)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (cedula, nombre, password_hashed, data.get('tipo_usuario'), data.get('clase'), 
                  data.get('perfil'), data.get('empresa_id'), data.get('empresa_select'), data.get('telegram_id'), data.get('telefono')))
            msg = 'Usuario creado.'
        else:
            if password:
                password_hashed = bcrypt.generate_password_hash(password).decode('utf-8')
                cur.execute("""
                    UPDATE usuarios SET nombre=%s, password=%s, perfil=%s, empresa_id=%s, empresa=%s, telegram_id=%s, telefono=%s
                    WHERE cedula=%s
                """, (nombre, password_hashed, data.get('perfil'), data.get('empresa_id'), data.get('empresa_select'), data.get('telegram_id'), data.get('telefono'), cedula))
            else:
                cur.execute("""
                    UPDATE usuarios SET nombre=%s, perfil=%s, empresa_id=%s, empresa=%s, telegram_id=%s, telefono=%s
                    WHERE cedula=%s
                """, (nombre, data.get('perfil'), data.get('empresa_id'), data.get('empresa_select'), data.get('telegram_id'), data.get('telefono'), cedula))
            msg = 'Usuario actualizado.'

        mysql.connection.commit()
        return jsonify({'success': True, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/registrar_proveedor', methods=['POST'])
@login_required_custom
def registrar_proveedor():
    d = request.form
    accion = d.get('accion', 'crear')
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("""
                INSERT INTO proveedores (proveedor, id_proveedor, email1, email2, producto_servicio, precio)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), 'GLP', d.get('precio')))
            msg = "Proveedor creado."
        else:
            cur.execute("""
                UPDATE proveedores 
                SET proveedor=%s, email1=%s, email2=%s, precio=%s 
                WHERE id_proveedor=%s
            """, (d.get('proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor')))
            msg = "Proveedor actualizado."
        mysql.connection.commit()
        return jsonify(success=True, message=msg)
    except Exception as e: return jsonify(success=False, message=str(e))
    finally: cur.close()

@csrf.exempt
@bp_901811727.route('/registrar_contacto', methods=['POST'])
@login_required_custom
def registrar_contacto():
    d = request.form
    accion = d.get('accion', 'crear')
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("""
                INSERT INTO contactos (empresa, id_empresa, area_contacto, email)
                VALUES (%s, %s, %s, %s)
            """, (d.get('empresa_nombre'), d.get('id_empresa'), d.get('area_contacto'), d.get('email')))
            msg = "Contacto creado."
        else:
            cur.execute("""
                UPDATE contactos SET area_contacto=%s, email=%s 
                WHERE id=%s
            """, (d.get('area_contacto'), d.get('email'), d.get('id')))
            msg = "Contacto actualizado."
        mysql.connection.commit()
        return jsonify(success=True, message=msg)
    except Exception as e: return jsonify(success=False, message=str(e))
    finally: cur.close()

# --- RUTAS DE CONSULTA PARA LAS TABLAS FRONTEND ---


@csrf.exempt
@bp_901811727.route('/consultar_proveedores', methods=['POST'])
@login_required_custom
def consultar_proveedores():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "Empresa no especificada"})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT proveedor, id_proveedor, email1, email2 FROM proveedores WHERE id_empresa = %s", (empresa_id,))
        data = [{"proveedor": r[0], "id_proveedor": r[1], "email1": r[2], "email2": r[3]} for r in cur.fetchall()]
        return jsonify({"success": True, "proveedores": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})
    finally:
        cur.close()

@csrf.exempt
@bp_901811727.route('/obtener_periodo', methods=['POST'])
@login_required_custom
def obtener_periodo():
    p = request.form.get('periodo')
    return jsonify({'success': True, 'periodo': p, 'fecha_inicio': request.form.get('fecha_inicio'), 'fecha_fin': request.form.get('fecha_fin')})


# ==============================================================================
# LÓGICA DE INFORMES Y ESTADÍSTICAS (KPIs ACTUALIZADOS Y RESCATE POBLACIÓN)
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
        'fechas': [], 
        'kg_pollito': [], 
        'velocidad_consumo': [],  
        'saldo_inicial': [], 
        'saldo_final': [], 
        'ingresos': [] 
    }
    
    granjas_data = {}

    resultados.sort(key=lambda x: str(x.get('fecha')))

    for row in resultados:
        lote_id = row.get('lote')
        
        # 1. RESCATE DE POBLACIÓN (El puente del Lote)
        pollitos_reales = mapa_poblacion_rescatada.get(lote_id, 0)

        # CRITERIO DE EXCLUSIÓN 1: Si el lote no tiene población registrada en la historia, se ignora
        if pollitos_reales <= 0:
            continue

        fecha_str = str(row.get('fecha'))
        clase = str(row.get('clase') or '').lower().strip()
        ubicacion = row.get('ubicacion') or 'Desconocida'

        val_kg_saldo = safe_float(row.get('saldo_estimado_kg'))
        masa_fact = safe_float(row.get('masa_kg_facturada'))
        neto_gast = safe_float(row.get('neto_gastado'))
        
        # CRITERIO DE EXCLUSIÓN 2: Si el registro es un egreso/consumo pero el valor es 0, es ruido
        if clase in ['egreso', 'consumo'] and neto_gast <= 0:
            continue

        val_precio = safe_float(row.get('precio_total')) 
        kg_pollo  = safe_float(row.get('kg_pollito'))
        vel_consumo = safe_float(row.get('velocidad_consumo'))
        
        math_consumo_real_acumulado += neto_gast
        math_dinero_total += val_precio

        if clase == 'saldo inicial':
            math_saldo_inicial_kg_global += val_kg_saldo

        if clase == 'ingreso':
            math_ingresos_kg += masa_fact

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
            if pollitos_reales > 0:
                d['pollitos'] += pollitos_reales
            d['lotes'].add(lote_id)

    # 2. FILTRADO FINAL Y CÁLCULO DE EFICIENCIA
    tabla_resumen = []
    lista_rendimientos = []
    total_pollitos_global = 0
    total_consumo_final = 0.0

    for nombre_granja, datos in granjas_data.items():
        consumo_granja = datos['consumo_real']
        
        # REGLA MAESTRA: Eliminar granjas con consumo cero del reporte
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
        "card1_label": "Saldo Inicial (kg)",
        "card1_value": math_saldo_inicial_kg_global,
        "card2_label": "Pedidos Gas (kg)",
        "card2_value": math_ingresos_kg,
        "card3_label": "Consumo Real (kg)",
        "card3_value": total_consumo_final,
        "card4_label": "Eficiencia (kg/ave)", 
        "card4_value": rendimiento,
        "card5_label": "Pollitos",
        "card5_value": total_pollitos_global,
        "card6_label": "Inversión Total ($)",
        "card6_value": math_dinero_total
    }

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
        "estadisticas": { 
            "media": media, 
            "desviacion": desviacion,
            "n_muestras": n
        },
        "periodo_tipo": periodo,
        "nota_metodologica": nota_informativa
    }
    
@csrf.exempt
@bp_901811727.route('/generar_informe', methods=['POST'])
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
        
        # Filtro Base (Fechas y Empresa)
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

        # 3. LÓGICA DE RESCATE DE POBLACIÓN (MÁS ALLÁ DEL MURO DE FECHAS)
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
@bp_901811727.route('/obtener_ubicaciones', methods=['POST'])
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
    

# ==============================================================================
# VALIDACIÓN DE TANQUEOS
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

@csrf.exempt
@bp_901811727.route('/obtener_tanqueos_validacion', methods=['POST'])
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
@bp_901811727.route('/procesar_validacion_tanqueo', methods=['POST'])
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
                
@csrf.exempt
@bp_901811727.route('/obtener_audit_log', methods=['POST'])
@login_required_custom
def obtener_audit_log():
    empresa_id = request.form.get('empresa_id') 
    if not empresa_id:
        return jsonify({'success': False, 'logs': []})

    try:
        cur = mysql.connection.cursor()
        
        # 1. Buscamos el nombre de la empresa para rescatar registros huérfanos (con ID 0)
        cur.execute("SELECT nombre_comercial FROM empresas WHERE nit = %s", (empresa_id,))
        row_emp = cur.fetchone()
        emp_nombre = ""
        if row_emp:
            emp_nombre = row_emp['nombre_comercial'] if isinstance(row_emp, dict) else row_emp[0]

        # 2. Búsqueda blindada: Busca por ID o por Nombre (para atrapar los que tienen ID 0)
        # Se elimina la restricción de 10 días y se limita a los últimos 100 eventos
        cur.execute("""
            SELECT fecha, modulo, usuario, accion, detalle, nivel 
            FROM audit_log 
            WHERE empresa_id = %s OR empresa_nombre = %s
            ORDER BY fecha DESC 
            LIMIT 100
        """, (empresa_id, emp_nombre))
        
        logs = cur.fetchall()
        cur.close()
        
        data = []
        nombres_cols = ['fecha', 'modulo', 'usuario', 'accion', 'detalle', 'nivel']

        for row in logs:
            if isinstance(row, tuple):
                r = dict(zip(nombres_cols, row))
            else:
                r = row 
            
            fecha_val = r.get('fecha')
            if fecha_val:
                fecha_str = fecha_val.strftime('%Y-%m-%d %H:%M:%S')
            else:
                fecha_str = 'N/A'
            
            data.append({
                'fecha': fecha_str,
                'modulo': r.get('modulo'),
                'usuario': r.get('usuario'),
                'accion': r.get('accion'),
                'detalle': r.get('detalle'),
                'nivel': r.get('nivel')
            }) 
            
        return jsonify({'success': True, 'logs': data})

    except Exception as e:
        print(f"Error Audit Log: {e}")
        return jsonify({'success': False, 'message': str(e)})


@csrf.exempt
@bp_901811727.route('/ejecutar_limpieza_automatica')
@login_required_custom
def ejecutar_limpieza_automatica():
    dias_limite = 60
    total_borrados = 0
    base_dir = current_app.static_folder
    
    cur = mysql.connection.cursor()
    try:
        # GLP
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
                count = _borrar_lista_archivos(base_dir, rutas_glp) 
                total_borrados += count
            
            if cols_update_glp:
                set_clause = ", ".join([f"`{c}` = NULL" for c in cols_update_glp])
                cur.execute(f"UPDATE cardex_glp SET {set_clause} WHERE id = %s", (row['id'],))

        # MERMAS
        cur.execute(f"""
            SELECT id, evidencia_url, evidencia_url1, evidencia_url2 
            FROM mermas_pollosgar 
            WHERE fecha < DATE_SUB(NOW(), INTERVAL {dias_limite} DAY)
        """)
        filas_mermas = cur.fetchall()
        
        for fila in filas_mermas:
            if isinstance(fila, dict):
                r_m = fila
            else:
                r_m = {'id': fila[0], 'evidencia_url': fila[1], 'evidencia_url1': fila[2], 'evidencia_url2': fila[3]}
            
            rutas_mermas = []
            cols_update_mermas = []
            
            for col_name in ['evidencia_url', 'evidencia_url1', 'evidencia_url2']:
                val = r_m.get(col_name)
                if val and isinstance(val, str) and len(val) > 5:
                    rutas_mermas.append(val.strip())
                    cols_update_mermas.append(col_name)
            
            if rutas_mermas:
                count = _borrar_lista_archivos(base_dir, rutas_mermas)
                total_borrados += count
                
            if cols_update_mermas:
                set_clause = ", ".join([f"`{c}` = NULL" for c in cols_update_mermas])
                cur.execute(f"UPDATE mermas_pollosgar SET {set_clause} WHERE id = %s", (r_m['id'],))

        mysql.connection.commit()
        mensaje = f"Mantenimiento completado: Se eliminaron {total_borrados} archivos antiguos."
        return jsonify({'success': True, 'message': mensaje})

    except Exception as e:
        print(f"⚠️ Error en limpieza automática: {e}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()

def _borrar_lista_archivos(base_dir, lista_rutas):
    borrados = 0
    for ruta in lista_rutas:
        try:
            clean = ruta.strip().replace('\\', '/')
            if clean.startswith('/static/') or clean.startswith('static/'):
                clean = clean.lstrip('/').replace('static/', '', 1)
            full_path = os.path.join(base_dir, clean)
            
            if os.path.exists(full_path):
                os.remove(full_path)
                borrados += 1
        except Exception: pass
    return borrados


# ==============================================================================
# REPORTE DE PENDIENTES DE TANQUEO
# ==============================================================================
@csrf.exempt
@bp_901811727.route('/obtener_pendientes_tanqueo_reporte', methods=['POST'])
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

        # LOGICA CORREGIDA: Busca pedidos aprobados donde NO haya un 'tanqueo' registrado posteriormente en ese lote.
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
            WHERE p.cliente = %s
              AND p.estatus_flujo = 'aprobado_webmaster'
              AND NOT EXISTS (
                  SELECT 1 FROM cardex_glp c 
                  WHERE c.lote = p.lote 
                    AND c.operacion = 'tanqueo' 
                    AND c.fecha >= DATE(p.fecha_registro)
              )
            ORDER BY dias_retraso DESC
        """
        cur.execute(sql, (nombre_empresa,))
        rows = cur.fetchall()
        
        pendientes = []
        col_names = [d[0] for d in cur.description] if cur.description else []
        
        for r in rows:
            rd = dict(zip(col_names, r)) if not isinstance(r, dict) else r
            pendientes.append({
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
        
# ==============================================================================
# INFORME DE SALDOS AL CIERRE
# ==============================================================================
@csrf.exempt
@bp_901811727.route('/generar_informe_saldos', methods=['POST'])
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
# RUTA TEMPORAL: RECALCULAR HISTÓRICO DE EFICIENCIA (KG_POLLITO)
# ==============================================================================
@bp_901811727.route('/util/recalcular_historico')
@login_required_custom
def recalcular_historico_glp():
    try:
        cur = mysql.connection.cursor()
        
        # 1. Obtener todos los lotes únicos
        cur.execute("SELECT DISTINCT lote FROM cardex_glp WHERE lote IS NOT NULL")
        lotes_raw = cur.fetchall()
        lotes = [row['lote'] if isinstance(row, dict) else row[0] for row in lotes_raw]
        
        registros_actualizados = 0
        
        for lote in lotes:
            # 2. Buscar pollitos iniciales
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
                
            # 3. Traer registros cronológicos
            cur.execute("""
                SELECT id, COALESCE(neto_gastado, 0) as neto 
                FROM cardex_glp 
                WHERE lote=%s 
                ORDER BY id ASC
            """, (lote,))
            registros = cur.fetchall()
            
            consumo_acumulado = 0.0
            
            # 4. Recalcular acumulado
            for reg in registros:
                r_id = reg['id'] if isinstance(reg, dict) else reg[0]
                r_neto = float(reg['neto'] if isinstance(reg, dict) else reg[1])
                
                consumo_acumulado += r_neto
                nuevo_kg_pollito = consumo_acumulado / float(pollitos)
                
                # 5. Actualizar BD
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
    
# ==============================================================================
# NUEVAS RUTAS DE ADMINISTRACIÓN (CRUD INTEGRAL)
# ==============================================================================

@csrf.exempt
@bp_901811727.route('/gestionar_tipo_empresa', methods=['POST'])
@login_required_custom
def gestionar_tipo_empresa():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO tipos_empresa (tipo) VALUES (%s)", (d.get('tipo'),))
        else:
            cur.execute("UPDATE tipos_empresa SET tipo=%s WHERE id=%s", (d.get('tipo'), d.get('id')))
        mysql.connection.commit()
        return jsonify(success=True, message="Tipo de empresa gestionado exitosamente.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_901811727.route('/gestionar_empresa', methods=['POST'])
@login_required_custom
def gestionar_empresa():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO empresas (nit, nombre_comercial, tipo_empresa) VALUES (%s, %s, %s)", 
                       (d.get('nit'), d.get('nombre_comercial'), d.get('tipo_empresa')))
        else:
            cur.execute("UPDATE empresas SET nombre_comercial=%s, tipo_empresa=%s WHERE nit=%s", 
                       (d.get('nombre_comercial'), d.get('tipo_empresa'), d.get('nit')))
        mysql.connection.commit()
        return jsonify(success=True, message="Empresa procesada correctamente.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_901811727.route('/gestionar_usuario', methods=['POST'])
@login_required_custom
def gestionar_usuario():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            pw = bcrypt.generate_password_hash(d.get('password')).decode('utf-8')
            cur.execute("""INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa, telegram_id, telefono) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (d.get('cedula'), d.get('nombre'), pw, d.get('tipo_usuario'), d.get('clase'), d.get('perfil'), d.get('empresa_id'), d.get('empresa_select'), None, d.get('telefono')))
        else:
            cedula = d.get('cedula')
            nuevo_telefono = d.get('telefono')
            telegram_id_enviado = d.get('telegram_id')

            # --- LÓGICA DE SEGURIDAD: VERIFICAR CAMBIO DE TELÉFONO ---
            cur.execute("SELECT telefono FROM usuarios WHERE cedula=%s", (cedula,))
            row = cur.fetchone()
            telefono_actual = row['telefono'] if isinstance(row, dict) else row[0]

            # Si el teléfono cambió, forzamos a borrar el ID de Telegram
            if str(telefono_actual) != str(nuevo_telefono):
                telegram_id_enviado = None
            elif not telegram_id_enviado or telegram_id_enviado.strip() == "":
                telegram_id_enviado = None
            # ---------------------------------------------------------

            query = "UPDATE usuarios SET nombre=%s, perfil=%s, telegram_id=%s, telefono=%s, empresa_id=%s, empresa=%s"
            params = [d.get('nombre'), d.get('perfil'), telegram_id_enviado, nuevo_telefono, d.get('empresa_id'), d.get('empresa_select')]
            
            if d.get('password'):
                query += ", password=%s"
                params.append(bcrypt.generate_password_hash(d.get('password')).decode('utf-8'))
                
            query += " WHERE cedula=%s"
            params.append(cedula)
            
            cur.execute(query, tuple(params))
            
        mysql.connection.commit()
        return jsonify(success=True, message="Usuario gestionado exitosamente.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_901811727.route('/gestionar_proveedor', methods=['POST'])
@login_required_custom
def gestionar_proveedor():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("""INSERT INTO proveedores (proveedor, id_proveedor, email1, email2, producto_servicio, precio) 
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                       (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), 'GLP', d.get('precio')))
        else:
            cur.execute("""UPDATE proveedores SET proveedor=%s, email1=%s, email2=%s, precio=%s WHERE id_proveedor=%s""",
                       (d.get('proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor')))
        mysql.connection.commit()
        return jsonify(success=True, message="Proveedor actualizado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_901811727.route('/gestionar_perfil', methods=['POST'])
@login_required_custom
def gestionar_perfil():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil) VALUES (%s, %s, %s, %s)",
                       (d.get('empresa_select'), d.get('nit'), d.get('operacion'), d.get('perfil')))
        else:
            cur.execute("UPDATE perfiles SET operacion=%s, perfil=%s WHERE id=%s", (d.get('operacion'), d.get('perfil'), d.get('id')))
        mysql.connection.commit()
        return jsonify(success=True, message="Perfil procesado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_901811727.route('/gestionar_contacto', methods=['POST'])
@login_required_custom
def gestionar_contacto():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO contactos (empresa, id_empresa, area_contacto, email) VALUES (%s, %s, %s, %s)",
                       (d.get('empresa_nombre'), d.get('id_empresa'), d.get('area_contacto'), d.get('email')))
        else:
            cur.execute("UPDATE contactos SET area_contacto=%s, email=%s WHERE id=%s", (d.get('area_contacto'), d.get('email'), d.get('id')))
        mysql.connection.commit()
        return jsonify(success=True, message="Contacto gestionado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

# --- RUTAS DE LECTURA PARA LLENAR TABLAS ---
@bp_901811727.route('/obtener_todos_tipos_empresa')
@login_required_custom
def obtener_todos_tipos_empresa():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, tipo FROM tipos_empresa")
    rows = cur.fetchall()
    res = [dict(zip(['id','tipo'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, tipos=res)

@bp_901811727.route('/obtener_todos_usuarios')
@login_required_custom
def obtener_todos_usuarios():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, cedula, nombre, perfil, empresa, empresa_id, telegram_id, telefono FROM usuarios")
    rows = cur.fetchall()
    cols = ['id','cedula','nombre','perfil','empresa','empresa_id','telegram_id','telefono']
    res = [dict(zip(cols, r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, users=res)

@bp_901811727.route('/obtener_todos_proveedores')
@login_required_custom
def obtener_todos_proveedores():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id_proveedor, proveedor, email1, email2, precio FROM proveedores")
    rows = cur.fetchall()
    res = [dict(zip(['id_proveedor','proveedor','email1','email2','precio'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, providers=res)

@bp_901811727.route('/obtener_todos_contactos')
@login_required_custom
def obtener_todos_contactos():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, empresa, id_empresa, area_contacto, email FROM contactos")
    rows = cur.fetchall()
    res = [dict(zip(['id','empresa','id_empresa','area_contacto','email'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, contacts=res)

@bp_901811727.route('/obtener_todos_perfiles')
@login_required_custom
def obtener_todos_perfiles():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, empresa, nit, operacion, perfil FROM perfiles")
    rows = cur.fetchall()
    res = [dict(zip(['id','empresa','nit','operacion','perfil'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, profiles=res)
    
# ==============================================================================
# REPORTE DE LOTES ACTIVOS SIN FINALIZAR (>15 DÍAS)
# ==============================================================================
@csrf.exempt
@bp_901811727.route('/obtener_lotes_vencidos', methods=['POST'])
@login_required_custom
def obtener_lotes_vencidos():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "ID Empresa requerido"})

    try:
        cur = mysql.connection.cursor()
        
        # Calcula los días reales desde la primera fecha del lote hasta HOY
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