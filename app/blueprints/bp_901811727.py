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

bcrypt = Bcrypt()

bp_901811727 = Blueprint('bp_901811727', __name__)

# ==============================================================================
# RUTAS DE GESTIÓN
# ==============================================================================

@bp_901811727.route('/901811727.html')
@login_required_custom
def panel_webmaster():
    form = RegistroUsuarioForm()
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT nit, nombre_comercial FROM empresas")
        empresas = cur.fetchall()
        cur.execute("SELECT nombre_comercial FROM empresas")
        clientes = cur.fetchall()
        cur.close()
        return render_template('901811727.html', nombre=session.get('nombre'), empresa=session.get('empresa'), form=form, empresas=empresas, clientes=clientes)
    except Exception as e:
        print("Error panel:", e)
        return "Error cargando panel", 500

@csrf.exempt
@bp_901811727.route('/registrar_empresa', methods=['POST'])
@login_required_custom
def registrar_empresa():
    nombre_comercial = request.form.get('nombre_comercial', '').strip()
    nit = request.form.get('nit', '').strip()
    if not nombre_comercial or not nit:
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT * FROM empresas WHERE nit = %s", (nit,))
        if cur.fetchone():
            return jsonify({'success': False, 'message': 'La empresa ya existe.'})
        cur.execute("INSERT INTO empresas (nit, nombre_comercial) VALUES (%s, %s)", (nit, nombre_comercial))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Empresa creada correctamente.'})
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
    if not all([empresa_nombre, nit, operacion, perfil]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT nit FROM empresas WHERE nombre_comercial = %s", (empresa_nombre,))
        empresa = cur.fetchone()
        if not empresa or str(empresa['nit']) != nit:
            return jsonify({'success': False, 'message': 'Empresa no válida o NIT incorrecto.'})
        cur.execute("SELECT * FROM perfiles WHERE nit = %s AND operacion = %s AND perfil = %s", (nit, operacion, perfil))
        if cur.fetchone():
            return jsonify({'success': False, 'message': 'El perfil ya existe.'})
        cur.execute("INSERT INTO perfiles (nit, operacion, perfil) VALUES (%s, %s, %s)", (nit, operacion, perfil))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Perfil creado correctamente.'})
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
        perfiles = [row['perfil'] for row in cur.fetchall()]
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
    if not all([cedula, nombre, password]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
        if cur.fetchone():
            return jsonify({'success': False, 'message': 'El usuario ya existe.'})
        password_hashed = bcrypt.generate_password_hash(password).decode('utf-8')
        cur.execute("""
            INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (cedula, nombre, password_hashed, data.get('tipo_usuario'), data.get('clase'), 
              data.get('perfil'), data.get('empresa_id'), data.get('empresa_select')))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Usuario creado.'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

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
# LÓGICA DE INFORMES Y ESTADÍSTICAS (KPIs ACTUALIZADOS)
# ==============================================================================

def _procesar_resultados_glp(resultados, tipo_informe, periodo):
    if not resultados:
        return None

    def safe_float(val):
        if val is None: return 0.0
        try:
            return float(val)
        except (ValueError, TypeError):
            return 0.0

    # Inicializar contadores globales
    math_saldo_inicial_kg_global = 0.0
    math_ingresos_kg = 0.0
    math_consumo_real_acumulado = 0.0 # <--- AQUÍ ACUMULAREMOS EL NETO_GASTADO TOTAL
    math_dinero_total = 0.0 
    
    total_pollitos_global = 0
    lotes_procesados_global = set()

    # Inicializar estructuras de datos para gráficos y tablas
    series = { 'fechas': [], 'kg_pollito': [], 'saldo_inicial': [], 'saldo_final': [], 'ingresos': [] }
    granjas_data = {}

    # Ordenar resultados por fecha
    resultados.sort(key=lambda x: str(x.get('fecha')))

    for row in resultados:
        fecha_str = str(row.get('fecha'))
        clase = str(row.get('clase') or '').lower().strip()
        lote_id = row.get('lote')
        ubicacion = row.get('ubicacion') or 'Desconocida'

        # Extracción segura de valores
        val_kg_saldo = safe_float(row.get('saldo_estimado_kg'))
        masa_fact = safe_float(row.get('masa_kg_facturada'))
        neto_gast = safe_float(row.get('neto_gastado')) # <--- COLUMNA CRÍTICA
        val_precio = safe_float(row.get('precio_total')) 
        kg_pollo  = safe_float(row.get('kg_pollito'))
        pollitos  = int(row.get('pollitos') or 0)

        # -----------------------------------------------------------
        # 1. CÁLCULO DIRECTO (SUMAS)
        # -----------------------------------------------------------
        
        # A. Consumo Real: Suma directa de la columna neto_gastado (Sin importar la clase)
        math_consumo_real_acumulado += neto_gast

        # B. Inversión: Suma directa de precio_total
        math_dinero_total += val_precio

        # C. Saldo Inicial: Solo sumamos si la clase lo indica explícitamente
        if clase == 'saldo inicial':
            math_saldo_inicial_kg_global += val_kg_saldo

        # D. Pedidos / Ingresos: Suma de lo facturado cuando es ingreso
        if clase == 'ingreso':
            math_ingresos_kg += masa_fact

        # -----------------------------------------------------------
        # 2. SERIES PARA GRÁFICOS
        # -----------------------------------------------------------
        series['fechas'].append(fecha_str)
        series['kg_pollito'].append(kg_pollo)
        series['saldo_inicial'].append(val_kg_saldo if clase == 'saldo inicial' else None)
        series['saldo_final'].append(val_kg_saldo if clase == 'saldo final' else None)
        series['ingresos'].append(masa_fact if clase == 'ingreso' else None)

        # 3. CONTEO DE POLLITOS (Evitar duplicados por lote)
        if lote_id and lote_id not in lotes_procesados_global:
            total_pollitos_global += pollitos
            lotes_procesados_global.add(lote_id)

        # 4. DATOS AGRUPADOS POR GRANJA (Para la Tabla Resumen)
        if ubicacion not in granjas_data:
            granjas_data[ubicacion] = {
                'inicial': 0.0, 'ingresos': 0.0, 'consumo_real': 0.0,
                'pollitos': 0, 'lotes': set()
            }
        
        d = granjas_data[ubicacion]
        
        # Acumular consumo real por granja (Suma directa de neto_gastado)
        d['consumo_real'] += neto_gast
        
        if clase == 'saldo inicial': d['inicial'] += val_kg_saldo
        elif clase == 'ingreso': d['ingresos'] += masa_fact
        
        if lote_id and lote_id not in d['lotes']:
            d['pollitos'] += pollitos
            d['lotes'].add(lote_id)

    # --- CÁLCULOS KPI FINALES ---
    
    # Eficiencia = Consumo Real (Suma neto_gastado) / Pollitos
    rendimiento = 0.0
    if total_pollitos_global > 0:
        rendimiento = math_consumo_real_acumulado / total_pollitos_global
    
    kpis = {
        "card1_label": "Saldo Inicial (kg)",
        "card1_value": math_saldo_inicial_kg_global,
        
        "card2_label": "Pedidos Gas (kg)",
        "card2_value": math_ingresos_kg,
        
        "card3_label": "Consumo Real (kg)", # <--- AHORA ES LA SUMA DIRECTA DE NETO_GASTADO
        "card3_value": math_consumo_real_acumulado,
        
        "card4_label": "Eficiencia (kg/ave)", 
        "card4_value": rendimiento,
        
        "card5_label": "Pollitos",
        "card5_value": total_pollitos_global,

        "card6_label": "Inversión Total ($)",
        "card6_value": math_dinero_total
    }

    # --- GENERACIÓN DE TABLA RESUMEN ---
    tabla_resumen = []
    lista_rendimientos = []

    for nombre_granja, datos in granjas_data.items():
        consumo_granja = datos['consumo_real'] # Usamos el acumulado directo
        
        rend_granja = 0.0
        if datos['pollitos'] > 0:
            rend_granja = consumo_granja / datos['pollitos']
            lista_rendimientos.append(rend_granja)

        tabla_resumen.append({
            'granja': nombre_granja,
            'total_kg': consumo_granja,
            'pollitos': datos['pollitos'],
            'kg_pollito': rend_granja
        })

    # --- ESTADÍSTICAS ---
    media = 0.0
    desviacion = 0.0
    n = len(lista_rendimientos)
    
    if n > 0:
        media = sum(lista_rendimientos) / n
        if n > 1:
            varianza = sum((x - media) ** 2 for x in lista_rendimientos) / (n - 1)
            desviacion = math.sqrt(varianza)

    return {
        "kpis": kpis,
        "series": series,
        "tabla_resumen": tabla_resumen,
        "estadisticas": { "media": media, "desviacion": desviacion },
        "periodo_tipo": periodo
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
        wheres = ["WHERE c.id_empresa = %s"]
        params = [empresa_id]

        # Filtros por Ubicación (Zona o Granja)
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

        # --- LÓGICA DE PERIODOS AJUSTADA ---
        if periodo == 'Personalizado' and fecha_ini and fecha_fin:
            # HISTÓRICO: Filtramos solo por fechas.
            # Trae tanto lotes ACTIVOS como INACTIVOS que tuvieron movimiento en ese rango.
            wheres.append("AND c.fecha BETWEEN %s AND %s")
            params.append(fecha_ini)
            params.append(fecha_fin)
            
        elif periodo == 'Actual':
            # ACTUAL: Solo lo que está vivo hoy (ACTIVO)
            wheres.append("AND c.estatus_lote = 'ACTIVO'")

        # SELECT
        sql = """
            SELECT c.fecha, c.ubicacion, c.lote, c.estatus_lote, c.operacion, c.clase, 
                   c.saldo_estimado_kg, c.saldo_estimado_galones,
                   c.pollitos, c.kg_pollito, c.masa_kg_facturada, c.neto_gastado,
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

        cursor.close()

        datos = _procesar_resultados_glp(raw_results, tipo_informe, periodo)
        if not datos:
            return jsonify({"success": False, "message": "No hay datos para mostrar."})

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
# NUEVO MÓDULO: VALIDACIÓN DE TANQUEOS (Similar a Investigación NC)
# ==============================================================================


# Configuración Email (Asegúrate de que estas variables de entorno existan o configúralas aquí)
EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = 587
EMAIL_USER = os.environ.get("EMAIL_USER", "tu_email@ejemplo.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "tu_password")
EMAIL_FROM = EMAIL_USER

def _enviar_alerta_gerencia(empresa_id, empresa_nombre, datos, archivos):
    """
    Busca contacto 'gerenciagranjas' de forma robusta (TRIM, Types) y envía correo.
    """
    # 1. Limpieza de datos de entrada para evitar errores de tipo
    try:
        e_id = int(empresa_id) if empresa_id else 0
    except:
        e_id = 0
        
    e_nombre = str(empresa_nombre).strip() if empresa_nombre else ""

    print(f"🔍 DEBUG ALERTAS: Buscando 'gerenciagranjas' para ID=[{e_id}] o Nombre=[{e_nombre}]")

    cur = mysql.connection.cursor()
    
    # 2. Consulta Blindada: Usa TRIM para ignorar espacios y busca por ambos campos.
    # Intenta encontrar el contacto si coincide el ID numérico O si coincide el Nombre exacto.
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

    # 3. Verificación del resultado (Soporta si el cursor devuelve Diccionario o Tupla)
    destinatario = None
    if row:
        if isinstance(row, dict):
            destinatario = row.get('email')
        else:
            destinatario = row[0]

    if not destinatario:
        print(f"❌ ERROR: No se encontró email 'gerenciagranjas' en tabla contactos.")
        # AQUÍ ESTABA EL ERROR ANTERIOR, YA CORREGIDO (Paréntesis cerrado correctamente):
        print(f"   --> Verifique que exista un registro con id_empresa={e_id} OR empresa='{e_nombre}' en la tabla contactos.")
        return False

    print(f"✅ Contacto encontrado: {destinatario}. Preparando envío...")

    # 4. Construir el correo
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

    # 5. Adjuntar Fotos
    base_dir = current_app.static_folder
    if archivos:
        for ruta in archivos:
            if ruta:
                # Limpieza de ruta para encontrar el archivo físico en el sistema
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
                else:
                     print(f"⚠️ Archivo no encontrado para adjuntar: {full_path}")

    # 6. Enviar vía SMTP
    try:
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_FROM, destinatario, msg.as_string())
        server.quit()
        print("📨 Correo enviado con éxito a Gerencia.")
        return True
    except Exception as e:
        print(f"⛔ Error crítico enviando email SMTP: {e}")
        return False        
# ==============================================================================
# VALIDACIÓN DE TANQUEOS (CORREGIDO)
# ==============================================================================

@csrf.exempt
@bp_901811727.route('/obtener_tanqueos_validacion', methods=['POST'])
@login_required_custom
def obtener_tanqueos_validacion():
    # --- FUNCIÓN AUXILIAR PARA CORREGIR RUTAS ---
    def formatear_ruta(ruta):
        if not ruta: return None
        
        # 1. Normalizar slashes (Windows usa \, web usa /)
        ruta = ruta.strip().replace('\\', '/')
        
        # 2. Limpieza: Quitamos barra inicial si existe
        if ruta.startswith('/'):
            ruta = ruta[1:]
            
        # 3. Limpieza: Quitamos el prefijo 'static/' si ya viene en la BD
        # Esto evita errores tipo: /static/static/testigos...
        if ruta.startswith('static/'):
            ruta = ruta.replace('static/', '', 1)

        # 4. Construcción Final: 
        # Agregamos /static/ al inicio. Como limpiamos la ruta antes,
        # el resultado será limpio, ej: "/static/testigos/Empresa/foto.jpg"
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
                    # --- APLICAMOS FORMATEO AQUÍ ---
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
    # 1. Obtener datos JSON
    data = request.get_json()
    if not data:
        return jsonify({'status': 'error', 'message': 'Datos no recibidos'}), 400

    id_registro = data.get('id')
    decision = data.get('decision') 

    if not id_registro:
         return jsonify({'status': 'error', 'message': 'ID no proporcionado'})

    cur = mysql.connection.cursor()
    
    # Traemos TODO el registro
    cur.execute("SELECT * FROM cardex_glp WHERE id = %s", (id_registro,))
    row = cur.fetchone()
    
    if not row:
        cur.close()
        return jsonify({'status': 'error', 'message': 'Registro no encontrado'})

    # --- CORRECCIÓN CRÍTICA AQUÍ ---
    # Detectamos si 'row' ya es un diccionario o es una tupla
    if isinstance(row, dict):
        row_dict = row
    else:
        # Si es tupla, hacemos la conversión manual
        columns = [desc[0] for desc in cur.description]
        row_dict = dict(zip(columns, row))
    # -------------------------------

    # Extraemos datos informativos con seguridad
    empresa_id = row_dict.get('id_empresa')
    empresa_nombre = row_dict.get('empresa')
    ubicacion = row_dict.get('ubicacion')

    # Debug para confirmar en consola que ahora sí toma los datos reales
    print(f"DEBUG PROCESAMIENTO: ID_Empresa={empresa_id}, Nombre={empresa_nombre}")

    # --- RECOLECCIÓN DE RUTAS DE FOTOS ---
    archivos_adjuntos = []
    
    for key, value in row_dict.items():
        # Buscamos columnas de tipo texto que contengan 'testigo' y tengan datos
        if value and isinstance(value, str) and 'testigo' in key.lower():
            archivos_adjuntos.append(value.strip())

    msg = ""

    # --- CASO RECHAZO (NO) ---
    if decision == 'NO': 
        datos_alerta = {
            'ubicacion': ubicacion,
            'fecha': str(row_dict.get('fecha')),
            'lote': str(row_dict.get('lote'))
        }
        
        # Enviamos alerta (Ahora empresa_id llevará el número correcto, ej: 890707006)
        enviado = _enviar_alerta_gerencia(empresa_id, empresa_nombre, datos_alerta, archivos_adjuntos)
        
        if enviado:
            # Borramos evidencias físicas
            _borrar_evidencias_tanqueo(archivos_adjuntos)
            
            # Borramos registro SQL
            cur.execute("DELETE FROM cardex_glp WHERE id = %s", (id_registro,))
            mysql.connection.commit()
            msg = "Registro rechazado. Se envió alerta a Gerencia y se eliminaron las evidencias."
        else:
            cur.close()
            # Si falla el envío de correo, avisamos al frontend
            return jsonify({'success': False, 'message': 'Fallo el envío de correo. No se eliminó el registro por seguridad.'})
            
    # --- CASO VALIDACIÓN (SI) ---
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
    """Elimina archivos físicos del servidor"""
    base_dir = current_app.static_folder 
    
    for rel_path in rutas:
        if rel_path:
            # 1. Quitamos la barra inicial si existe
            clean_name = rel_path.lstrip('/')
            
            # 2. Si empieza con 'static/', lo quitamos SOLO UNA VEZ para no romper nombres de archivos
            if clean_name.startswith('static/'):
                clean_name = clean_name.replace('static/', '', 1)
            
            # 3. Construimos la ruta absoluta
            full_path = os.path.join(base_dir, clean_name)
            
            # 4. Verificamos y borramos
            if os.path.exists(full_path):
                try:
                    os.remove(full_path)
                    print(f"🗑️ Eliminado: {full_path}")
                except Exception as e:
                    print(f"⚠️ Error borrando {full_path}: {e}")
            else:
                print(f"⚠️ Archivo no encontrado para borrar: {full_path}")
@csrf.exempt
@bp_901811727.route('/obtener_audit_log', methods=['POST'])
@login_required_custom
def obtener_audit_log():
    # Recibe el ID de la empresa seleccionada en el select
    empresa_id = request.form.get('empresa_id') 
    
    cur = mysql.connection.cursor()
    
    # --- CAMBIO APLICADO ---
    # En lugar de LIMIT 100, usamos DATE_SUB para restar 10 días a la fecha actual (NOW)
    cur.execute("""
        SELECT fecha, modulo, usuario, accion, detalle, nivel 
        FROM audit_log 
        WHERE empresa_id = %s 
          AND fecha >= DATE_SUB(NOW(), INTERVAL 10 DAY)
        ORDER BY fecha DESC 
    """, (empresa_id,))
    
    logs = cur.fetchall()
    cur.close()
    
    # Formateamos para JSON
    data = []
    for row in logs:
        # Aseguramos que la fecha sea un objeto datetime antes de formatear
        fecha_str = row['fecha'].strftime('%Y-%m-%d %H:%M:%S') if row['fecha'] else 'N/A'
        
        data.append({
            'fecha': fecha_str,
            'modulo': row['modulo'],
            'usuario': row['usuario'],
            'accion': row['accion'],
            'detalle': row['detalle'],
            'nivel': row['nivel']
        }) 
    return jsonify({'success': True, 'logs': data})

@csrf.exempt
@bp_901811727.route('/ejecutar_limpieza_automatica')
@login_required_custom
def ejecutar_limpieza_automatica():
    """
    Limpieza Profunda (60 días):
    1. Borra testigos de gas (cardex_glp).
    2. Borra evidencias de mermas (mermas_pollosgar).
    3. Soporta fotos y videos.
    """
    dias_limite = 60
    total_borrados = 0
    base_dir = current_app.static_folder
    
    cur = mysql.connection.cursor()
    try:
        # ==============================================================================
        # 1. LIMPIEZA DE TESTIGOS GLP (Tabla cardex_glp)
        # ==============================================================================
        cur.execute(f"""
            SELECT * FROM cardex_glp 
            WHERE fecha < DATE_SUB(NOW(), INTERVAL {dias_limite} DAY)
        """)
        filas_glp = cur.fetchall()
        
        # Obtenemos nombres de columnas para iterar
        cols_glp = [desc[0] for desc in cur.description] if filas_glp else []

        for fila in filas_glp:
            row = dict(zip(cols_glp, fila)) if not isinstance(fila, dict) else fila
            rutas_glp = []
            cols_update_glp = []

            for col, val in row.items():
                # En cardex_glp las columnas clave contienen la palabra 'testigo'
                if val and isinstance(val, str) and 'testigo' in col.lower():
                    rutas_glp.append(val.strip())
                    cols_update_glp.append(col)
            
            # Borrar archivos y limpiar BD
            if rutas_glp:
                count = _borrar_lista_archivos(base_dir, rutas_glp) # Función auxiliar abajo
                total_borrados += count
            
            if cols_update_glp:
                set_clause = ", ".join([f"`{c}` = NULL" for c in cols_update_glp])
                cur.execute(f"UPDATE cardex_glp SET {set_clause} WHERE id = %s", (row['id'],))

        # ==============================================================================
        # 2. LIMPIEZA DE MERMAS (Tabla mermas_pollosgar)
        # ==============================================================================
        # En mermas, las evidencias están en columnas fijas: evidencia_url, url1, url2
        cur.execute(f"""
            SELECT id, evidencia_url, evidencia_url1, evidencia_url2 
            FROM mermas_pollosgar 
            WHERE fecha < DATE_SUB(NOW(), INTERVAL {dias_limite} DAY)
        """)
        filas_mermas = cur.fetchall()
        
        for fila in filas_mermas:
            # Normalizar a diccionario si viene como tupla
            if isinstance(fila, dict):
                r_m = fila
            else:
                r_m = {'id': fila[0], 'evidencia_url': fila[1], 'evidencia_url1': fila[2], 'evidencia_url2': fila[3]}
            
            rutas_mermas = []
            cols_update_mermas = []
            
            # Revisamos las 3 columnas de evidencia posibles
            for col_name in ['evidencia_url', 'evidencia_url1', 'evidencia_url2']:
                val = r_m.get(col_name)
                if val and isinstance(val, str) and len(val) > 5: # Filtro básico
                    rutas_mermas.append(val.strip())
                    cols_update_mermas.append(col_name)
            
            # Borrar archivos y limpiar BD
            if rutas_mermas:
                count = _borrar_lista_archivos(base_dir, rutas_mermas)
                total_borrados += count
                
            if cols_update_mermas:
                set_clause = ", ".join([f"`{c}` = NULL" for c in cols_update_mermas])
                cur.execute(f"UPDATE mermas_pollosgar SET {set_clause} WHERE id = %s", (r_m['id'],))

        mysql.connection.commit()
        mensaje = f"Mantenimiento completado: Se eliminaron {total_borrados} archivos (Fotos/Videos) antiguos de GLP y Mermas."
        print(f"🧹 {mensaje}")
        return jsonify({'success': True, 'message': mensaje})

    except Exception as e:
        print(f"⚠️ Error en limpieza automática: {e}")
        return jsonify({'success': False, 'message': str(e)})
    finally:
        cur.close()

def _borrar_lista_archivos(base_dir, lista_rutas):
    """Auxiliar para borrar archivos manejando rutas con/sin /static/"""
    borrados = 0
    for ruta in lista_rutas:
        try:
            # Limpieza inteligente de la ruta
            clean = ruta.strip().replace('\\', '/')
            
            # Caso 1: Ruta viene como '/static/mermas/...' (Típico de cardex_glp)
            if clean.startswith('/static/') or clean.startswith('static/'):
                clean = clean.lstrip('/').replace('static/', '', 1)
            
            # Caso 2: Ruta viene como 'mermas/pollos_gar_sas/...' (Típico de mermas_pollosgar)
            # No hacemos nada extra, ya que join(static_folder, 'mermas/...') funciona bien.
            
            full_path = os.path.join(base_dir, clean)
            
            if os.path.exists(full_path):
                os.remove(full_path)
                print(f"🗑️ Eliminado (+60d): {clean}")
                borrados += 1
        except Exception as ex:
            print(f"Error borrando archivo {ruta}: {ex}")
    return borrados
