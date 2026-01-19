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
            wheres.append("AND c.estatus_lote = 'INACTIVO'")
        elif periodo == 'Actual':
            wheres.append("AND c.estatus_lote = 'ACTIVO'")

        # SELECT ACTUALIZADO: INCLUYE precio_total
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

def _enviar_alerta_gerencia(empresa_id, datos, archivos):
    """Busca contacto 'gerenciagranjas' y envía el correo"""
    cur = mysql.connection.cursor()
    # 1. Buscar el correo del área gerenciagranjas
    cur.execute("SELECT email FROM contactos WHERE id_empresa = %s AND area_contacto = 'gerenciagranjas' LIMIT 1", (empresa_id,))
    row = cur.fetchone()
    cur.close()

    destinatario = row['email'] if row and isinstance(row, dict) else (row[0] if row else None)

    if not destinatario:
        print("❌ No se encontró contacto 'gerenciagranjas' para enviar alerta.")
        return False

    # 2. Construir el correo
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

    # 3. Adjuntar Fotos
    base_dir = current_app.static_folder
    for ruta in archivos:
        if ruta:
            clean_path = ruta.replace('/static/', '')
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
                    print(f"Error adjuntando {full_path}: {e}")

    # 4. Enviar
    try:
        server = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_FROM, destinatario, msg.as_string())
        server.quit()
        return True
    except Exception as e:
        print(f"Error enviando email: {e}")
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
    data = request.get_json()
    reg_id = data.get('id')
    decision = data.get('decision') # 'SI' o 'NO'
    validador = session.get('nombre') or 'Admin'
    
    if not reg_id or decision not in ['SI', 'NO']:
        return jsonify(success=False, message="Datos incompletos")

    cur = mysql.connection.cursor()
    try:
        # 1. Recuperar datos para procesar fotos y emails
        cur.execute("SELECT * FROM cardex_glp WHERE id = %s", (reg_id,))
        row = cur.fetchone()
        if not row: return jsonify(success=False, message="Registro no encontrado")
        
        col_names = [d[0] for d in cur.description]
        r_dict = dict(zip(col_names, row))

        # 2. Recolectar rutas de fotos para eliminar (físicamente)
        fotos_a_borrar = []
        cols_sql_to_null = [] # Para limpiar la BD

        for i in range(1, 12):
            keys = [
                f'testigo nivel tk-{i}', 
                f'testigo nivelfinal tk-{i}', 
                f'testigo_baucher_tk_{i}' # Recordar guion bajo para voucher
            ]
            for k in keys:
                if r_dict.get(k):
                    fotos_a_borrar.append(r_dict[k])
                    cols_sql_to_null.append(f"`{k}` = NULL") # Backticks por si acaso

        # 3. Lógica según decisión
        if decision == 'SI':
            # --- FLUJO APROBADO ---
            # 1. Borrar fotos físicas
            _borrar_evidencias_tanqueo(fotos_a_borrar)
            
            # 2. Actualizar BD (Validado + Poner rutas en NULL)
            sql_set = ", ".join(cols_sql_to_null)
            if sql_set: sql_set = ", " + sql_set # Añadir coma inicial si hay columnas

            sql_update = f"""
                UPDATE cardex_glp 
                SET estatus_validacion = 'validado', 
                    fecha_validacion = NOW(), 
                    validador_id = %s
                    {sql_set}
                WHERE id = %s
            """
            cur.execute(sql_update, (validador, reg_id))

        else:
            # --- FLUJO RECHAZADO ---
            # 1. Enviar Email a Gerencia
            datos_email = {
                'fecha': str(r_dict['fecha']),
                'ubicacion': r_dict.get('ubicacion'),
                'lote': r_dict.get('lote')
            }
            # Enviamos email (adjuntando fotos ANTES de borrarlas)
            email_enviado = _enviar_alerta_gerencia(r_dict['id_empresa'], datos_email, fotos_a_borrar)
            
            # 2. Borrar fotos físicas (Se borran igual por política de limpieza)
            _borrar_evidencias_tanqueo(fotos_a_borrar)

            # 3. Actualizar BD (Rechazado + Poner rutas en NULL)
            sql_set = ", ".join(cols_sql_to_null)
            if sql_set: sql_set = ", " + sql_set

            sql_update = f"""
                UPDATE cardex_glp 
                SET estatus_validacion = 'rechazado', 
                    fecha_validacion = NOW(), 
                    validador_id = %s
                    {sql_set}
                WHERE id = %s
            """
            cur.execute(sql_update, (validador, reg_id))

        mysql.connection.commit()
        return jsonify(success=True)

    except Exception as e:
        mysql.connection.rollback()
        print(f"Error procesando validación: {e}")
        return jsonify(success=False, message=str(e))
    finally:
        cur.close()

def _borrar_evidencias_tanqueo(rutas):
    """Elimina archivos físicos del servidor"""
    base_dir = current_app.static_folder # Asegura obtener la ruta absoluta a /static
    for rel_path in rutas:
        if rel_path:
            # Limpiar ruta relativa (ej: /static/img.jpg -> img.jpg)
            clean_name = rel_path.lstrip('/').replace('static/', '')
            full_path = os.path.join(base_dir, clean_name)
            
            if os.path.exists(full_path):
                try:
                    os.remove(full_path)
                    print(f"🗑️ Eliminado: {full_path}")
                except Exception as e:
                    print(f"Error borrando {full_path}: {e}")


