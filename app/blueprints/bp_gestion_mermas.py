from flask import Blueprint, request, jsonify, current_app, session, send_file, url_for
from datetime import datetime, timedelta, time
import os
import base64
import uuid
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO
from twilio.rest import Client

# --- LIBRERÍAS DE REPORTE Y PDF (ReportLab) ---
from reportlab.lib.pagesizes import LETTER, landscape, A4
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import cm

# --- IMPORTANTE: CURSORES PARA DICCIONARIOS ---
import MySQLdb.cursors 

from app import mysql, csrf

bp_gestion_mermas = Blueprint('bp_gestion_mermas', __name__)

# ==============================================================================
# CONFIGURACIÓN DE HORARIOS (NUEVO)
# ==============================================================================
# Define el horario en el que HAY controladores disponibles (Ej: 8am a 6pm).
HORA_INICIO_CONTROL = 8
HORA_FIN_CONTROL = 18

# ==============================================================================
# 1. CONFIGURACIÓN DE CORREO
# ==============================================================================

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
try:
    EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))
except:
    EMAIL_PORT = 587
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

# =========================
# CONFIGURACIÓN TWILIO (WHATSAPP)
# =========================
TWILIO_SID = "AC10eb9cb9b1a1bddb8505857e7d6a5bd5" 
TWILIO_TOKEN = "7183bc6d507e1de54da5dead100871ab"
TWILIO_FROM = "+12067523027"

# ==============================================================================
# 2. UTILIDADES INTERNAS (HELPERS)
# ==============================================================================

def _es_horario_control():
    """
    Retorna True si la hora actual está DENTRO del horario de oficina:
    - Inicio: 7:45 AM
    - Fin: 5:30 PM (17:30)
    Fuera de este rango es Horario Nocturno/Extendido.
    """
    ahora = datetime.now().time()
    
    # Definimos los límites exactos
    inicio_jornada = time(7, 45)  # 7:45:00
    fin_jornada = time(17, 30)    # 17:30:00 (5:30 PM)
    
    # Comparamos si 'ahora' está entre el inicio y el fin
    return inicio_jornada <= ahora <= fin_jornada

def _get_umbral_pct(empresa_id=None):
    return 2.0

def _save_base64_image(data_url, empresa_slug='generico'):
    if not data_url or not isinstance(data_url, str) or not data_url.startswith('data:'):
        return None
    try:
        header, b64data = data_url.split(',', 1)
        if 'video' in header: ext = 'webm' 
        elif 'png' in header: ext = 'png'
        else: ext = 'jpg'
        
        yyyymm = datetime.now().strftime('%Y%m')
        folder = os.path.join(current_app.static_folder, 'mermas', empresa_slug, yyyymm)
        os.makedirs(folder, exist_ok=True)
        
        filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.{ext}"
        path = os.path.join(folder, filename)
        with open(path, 'wb') as f:
            f.write(base64.b64decode(b64data))
        return f"mermas/{empresa_slug}/{yyyymm}/{filename}"
    except Exception as e:
        current_app.logger.error(f"Error guardando archivo multimedia: {e}")
        return None

def _delete_evidence_files(file_paths):
    if not file_paths: return
    base_dir = current_app.static_folder
    for rel_path in file_paths:
        if rel_path:
            full_path = os.path.join(base_dir, rel_path)
            if os.path.exists(full_path):
                try: os.remove(full_path)
                except Exception as e: print(f"Error eliminando archivo: {e}")

def _limpiar_mermas_antiguas():
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        fecha_limite = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        sql = """
            SELECT id, evidencia_url, evidencia_url1, evidencia_url2 
            FROM mermas_pollosgar 
            WHERE estatus = 'aprobada' 
              AND fecha <= %s 
              AND (evidencia_url IS NOT NULL OR evidencia_url1 IS NOT NULL)
        """
        cur.execute(sql, (fecha_limite,))
        rows = cur.fetchall()
        if rows:
            ids_to_clean = []
            files_to_delete = []
            for r in rows:
                ids_to_clean.append(r['id'])
                if r.get('evidencia_url'): files_to_delete.append(r['evidencia_url'])
                if r.get('evidencia_url1'): files_to_delete.append(r['evidencia_url1'])
                if r.get('evidencia_url2'): files_to_delete.append(r['evidencia_url2'])
            
            _delete_evidence_files(files_to_delete)
            format_strings = ','.join(['%s'] * len(ids_to_clean))
            update_sql = f"UPDATE mermas_pollosgar SET evidencia_url = NULL, evidencia_url1 = NULL, evidencia_url2 = NULL WHERE id IN ({format_strings})"
            cur.execute(update_sql, tuple(ids_to_clean))
            mysql.connection.commit()
        cur.close()
    except Exception as e:
        print(f"Error en limpieza automática: {e}")

def _get_email_talento_humano(empresa_id):
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        sql = "SELECT email FROM contactos WHERE id_empresa = %s AND area_contacto = 'talentohumano' LIMIT 1"
        cur.execute(sql, (empresa_id,))
        row = cur.fetchone()
        cur.close()
        return row['email'] if row else None
    except:
        return None

def _enviar_email_no_conforme(destinatario, data_merma, argumentos, archivos, testimonio_op=None):
    """
    Envía email a RRHH. Modificado para incluir el testimonio opcionalmente.
    """
    if not destinatario or not EMAIL_USER or not EMAIL_PASS: return False

    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = destinatario
    msg['Subject'] = f"🚨 REPORTE NO CONFORME - {data_merma.get('operador_nombre')} - Fact: {data_merma.get('factura')}"

    # Si hay testimonio del operador (Etapa 4), lo incluimos
    bloque_testimonio = ""
    if testimonio_op:
        bloque_testimonio = f"""
        <div style="background: #e0f2fe; border-left: 4px solid #0284c7; padding: 15px; margin: 20px 0;">
            <h3 style="color: #0284c7; margin-top: 0;">Testimonio del Operador</h3>
            <p>"{testimonio_op}"</p>
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
        <div style="max-width: 600px; margin: 0 auto; background: #fff; border-radius: 8px; overflow: hidden;">
            <div style="background-color: #b91c1c; color: #fff; padding: 20px; text-align: center;">
                <h2 style="margin:0;">Notificación de Merma No Conforme</h2>
            </div>
            <div style="padding: 20px; color: #333;">
                <p>Se ha generado una validación <strong>NO CONFORME</strong> tras la auditoría.</p>
                
                {bloque_testimonio}

                <div style="background: #fef2f2; border-left: 4px solid #b91c1c; padding: 15px; margin: 20px 0;">
                    <h3 style="color: #b91c1c; margin-top: 0;">Dictamen del Auditor</h3>
                    <p>"{argumentos}"</p>
                </div>

                <ul>
                    <li><strong>Operador:</strong> {data_merma.get('operador_nombre')}</li>
                    <li><strong>Factura:</strong> {data_merma.get('factura')}</li>
                    <li><strong>Ítem:</strong> {data_merma.get('item')}</li>
                    <li><strong>Merma:</strong> {data_merma.get('merma_kg')} kg ({data_merma.get('merma_pct')}%)</li>
                </ul>
                <p style="font-size: 12px; color: #666; margin-top: 20px;">* Las evidencias se encuentran adjuntas.</p>
            </div>
        </div>
    </body>
    </html>
    """
    msg.attach(MIMEText(html, 'html'))

    base_dir = current_app.static_folder
    for rel_path in archivos:
        if rel_path:
            full_path = os.path.join(base_dir, rel_path)
            if os.path.exists(full_path):
                try:
                    with open(full_path, "rb") as f:
                        part = MIMEBase("application", "octet-stream")
                        part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f"attachment; filename={os.path.basename(full_path)}")
                    msg.attach(part)
                except Exception as ex: print(f"Error adjuntando archivo: {ex}")

    try:
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_FROM, destinatario, msg.as_string())
        return True
    except Exception as e:
        print(f"Error SMTP: {e}")
        return False

def _obtener_siguiente_consecutivo(empresa_id, empresa_nombre):
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT id, numero_consecutivo FROM consecutivo_documentos WHERE id_empresa=%s AND tipo_documento='nota_credito' FOR UPDATE", (empresa_id,))
        row = cur.fetchone()
        if row:
            nuevo_numero = int(row['numero_consecutivo']) + 1
            cur.execute("UPDATE consecutivo_documentos SET numero_consecutivo=%s WHERE id=%s", (nuevo_numero, row['id']))
        else:
            nuevo_numero = 1
            cur.execute("INSERT INTO consecutivo_documentos (empresa, id_empresa, tipo_documento, numero_consecutivo) VALUES (%s, %s, 'nota_credito', 1)", (empresa_nombre, empresa_id))
        mysql.connection.commit()
        cur.close()
        return nuevo_numero
    except Exception as e:
        if cur: cur.close()
        raise e

# ==============================================================================
# 3. RUTAS OPERATIVAS (REGISTRO Y GESTIÓN)
# ==============================================================================

@bp_gestion_mermas.route('/mermas/umbral', methods=['GET'])
def mermas_umbral():
    return jsonify(success=True, umbral_pct=_get_umbral_pct())

@bp_gestion_mermas.route('/mermas/clientes', methods=['GET'])
def mermas_clientes():
    empresa_id = session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    sql = "SELECT DISTINCT cliente_empresa FROM clientes_empresa WHERE cliente_empresa IS NOT NULL AND cliente_empresa <> ''"
    params = []
    if empresa_id:
        sql += " AND id_empresa = %s"
        params.append(empresa_id)
    sql += " ORDER BY cliente_empresa ASC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=[r['cliente_empresa'] for r in rows])

@bp_gestion_mermas.route('/mermas/vehiculos', methods=['GET'])
def mermas_vehiculos():
    empresa_id = session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    sql = "SELECT DISTINCT placa FROM vehiculos"
    params = []
    if empresa_id:
        sql += " WHERE id_empresa = %s"
        params.append(empresa_id)
    sql += " ORDER BY placa ASC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=[r['placa'] for r in rows])

@csrf.exempt
@bp_gestion_mermas.route('/mermas/iniciar_sesion', methods=['POST'])
def mermas_iniciar_sesion():
    """
    CANDADO ESTRICTO: Verifica si la factura ya existe.
    Si existe -> Bloquea (409 Conflict).
    Si no existe -> Genera Session ID único.
    """
    try: j = request.get_json(force=True)
    except: return jsonify(success=False), 400
    
    factura = str(j.get('factura', '')).strip()
    if not factura: return jsonify(success=False, message="Falta factura"), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # 1. VERIFICACIÓN ESTRICTA
    # Buscamos si existe CUALQUIER registro con esa factura, sin importar el estado
    cur.execute("SELECT id FROM mermas_pollosgar WHERE factura=%s LIMIT 1", (factura,))
    existe = cur.fetchone()
    cur.close()
    
    if existe:
        return jsonify({
            'success': False, 
            'code': 'DUPLICATE',
            'message': f"⛔ ERROR CRÍTICO:\nLa factura {factura} YA EXISTE en el sistema.\n\nNo se permite duplicar facturas."
        }), 409
        
    # 2. GENERAR HUELLA DIGITAL (SESSION ID)
    # Usamos UUID para garantizar que sea única mundialmente
    new_session_id = str(uuid.uuid4())
    
    return jsonify({'success': True, 'session_id': new_session_id})


@csrf.exempt
@bp_gestion_mermas.route('/mermas/registrar', methods=['POST'])
def mermas_registrar():
    """
    Ruta PRINCIPAL de registro.
    - Maneja lógica Día/Noche.
    - Guarda el SESSION_ID para aislamiento de viajes.
    - Maneja concurrencia con bloqueos.
    """
    try:
        j = request.get_json(force=True, silent=True) or {}
    except:
        return jsonify(success=False, message="JSON mal formado"), 400

    # 1. Extracción de Datos
    empresa = (session.get('empresa') or j.get('empresa') or '').strip()
    empresa_id = str(session.get('empresa_id') or session.get('nit') or j.get('empresa_id') or '').strip()
    operador_id = str(session.get('cedula') or session.get('usuario_id') or j.get('operador_id') or '').strip()
    operador_nombre = (session.get('usuario_nombre') or session.get('nombre') or j.get('operador_nombre') or '').strip()
    
    cliente = (j.get('cliente') or '').strip()
    vehiculo = (j.get('vehiculo') or '').strip()
    factura = (j.get('factura') or '').strip()
    
    # NUEVO: Capturar el ID de Sesión generado en el inicio
    session_id = (j.get('session_id') or '').strip()
    
    try: total_kg_factura = float(j.get('total_kg', 0))
    except: total_kg_factura = 0.0
        
    items = j.get('items')
    is_retry = j.get('is_retry', False)
    retry_id = j.get('retry_id')

    # Validaciones básicas
    if not (cliente and vehiculo and factura and items):
        return jsonify(success=False, message="Faltan datos obligatorios"), 400

    # 2. Gestión de Concurrencia (Lock)
    lock_name = f"mermas_op_{operador_id}"
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    try:
        cur.execute("SELECT GET_LOCK(%s, 10) AS candado", (lock_name,))
        res_lock = cur.fetchone()
        if (res_lock.get('candado') if res_lock else 0) != 1:
            cur.close()
            return jsonify(success=False, message="Servidor ocupado, intente de nuevo"), 409

        # Validación de duplicados en proceso (solo si no es reintento)
        if not is_retry:
            cur.execute("SELECT id FROM mermas_pollosgar WHERE factura=%s AND estatus='pendiente' LIMIT 1", (factura,))
            if cur.fetchone():
                 cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                 return jsonify(success=False, code='PENDING_EXISTS', message="Ya hay una solicitud pendiente para esta factura."), 409

        empresa_slug = (empresa or 'generico').lower().replace(' ', '_')
        umbral = _get_umbral_pct(empresa_id)
        
        # 3. Procesamiento de Ítems
        hay_controlador = _es_horario_control()
        global_status = 'aprobada' 
        last_inserted_id = None
        
        for it in items:
            kg_item_val = float(it.get('kg_facturados', 0)) 
            kge = float(it.get('kg_entregados', 0))
            nombre_item = (it.get('item') or 'General').strip()
            merma_kg = kg_item_val - kge 
            merma_pct = (merma_kg / kg_item_val * 100.0) if kg_item_val > 0 else 0.0

            # Guardar evidencias
            vid_path = _save_base64_image(it.get('evidencia_url'), empresa_slug)
            foto1_path = _save_base64_image(it.get('evidencia_url2'), empresa_slug)
            foto2_path = _save_base64_image(it.get('evidencia_url3'), empresa_slug)

            # LÓGICA DE ESTADOS (Día vs Noche)
            item_status = 'aprobada'
            if merma_pct > umbral:
                if hay_controlador:
                    # DÍA: Flujo síncrono (espera aprobación)
                    item_status = 'segunda_revision' if is_retry else 'pendiente'
                else:
                    # NOCHE: Flujo asíncrono (aprobación provisional)
                    item_status = 'aprobada_pendiente_revision'
                
                global_status = item_status

            # 4. Inserción o Actualización en BD
            if is_retry and retry_id:
                # En reintento actualizamos el registro existente
                cur.execute("""
                    UPDATE mermas_pollosgar
                    SET kg_item=%s, kg_entregados=%s, merma_kg=%s, merma_pct=%s,
                        evidencia_url=%s, evidencia_url1=%s, evidencia_url2=%s,
                        estatus=%s, decision='rectificada'
                    WHERE id=%s
                """, (kg_item_val, kge, merma_kg, merma_pct, foto1_path, vid_path, foto2_path, item_status, retry_id))
                last_inserted_id = retry_id
            else:
                # NUEVO REGISTRO: Incluye session_id
                cur.execute("""
                    INSERT INTO mermas_pollosgar
                        (fecha, empresa, empresa_id, operador_id, operador_nombre,
                         cliente, vehiculo, factura, item,
                         kg_factura, kg_item, kg_entregados, merma_kg, merma_pct, 
                         evidencia_url, evidencia_url1, evidencia_url2,
                         estatus, decision, fecha_decision, nota_descuento, session_id)
                    VALUES
                        (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), '', %s)
                """, (
                    empresa, empresa_id, operador_id, operador_nombre,
                    cliente, vehiculo, factura, nombre_item,
                    total_kg_factura, kg_item_val, kge, merma_kg, merma_pct,
                    foto1_path, vid_path, foto2_path,
                    item_status, 'revision' if merma_pct > umbral else 'aprobada',
                    session_id 
                ))
                last_inserted_id = cur.lastrowid

        mysql.connection.commit()
        cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        cur.close()

        return jsonify(success=True, status=global_status, id=last_inserted_id, message="Registrado correctamente")

    except Exception as e:
        try: cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        except: pass
        if cur: cur.close()
        return jsonify(success=False, message=f"Error interno: {str(e)}"), 500
    
@csrf.exempt
@bp_gestion_mermas.route('/mermas/accion', methods=['POST'])
def mermas_accion():
    """
    Ruta para el Controlador: MODIFICADA para manejar Flujo a Investigación.
    """
    j = request.get_json(force=True, silent=True) or {}
    reg_id = j.get('id')
    accion = (j.get('accion') or '').lower()
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT estatus FROM mermas_pollosgar WHERE id=%s", (reg_id,))
    row = cur.fetchone()
    
    if not row:
        cur.close()
        return jsonify(success=False, message="Registro no encontrado"), 404

    current_status = str(row['estatus']).strip().lower()
    estatus_final = None 
    decision = 'pendiente'
    
    if accion == 'aprobar':
        estatus_final = 'validada'
        decision = 'validada_manual'

    elif accion == 'objetar':
        # SI ES ETAPA 1 -> Devuelve al conductor
        if current_status == 'pendiente':
            estatus_final = 'en_revision'
            decision = 'solicitud_rectificacion'
        else:
            # SI ES ETAPA 2 (Rectificación o Nocturna) -> Pasa a Investigación (Etapa 3)
            estatus_final = 'en_investigacion'
            decision = 'en_investigacion'
            
    elif accion == 'a_investigacion':
        estatus_final = 'en_investigacion'
        decision = 'en_investigacion'
            
    if estatus_final:
        cur.execute("UPDATE mermas_pollosgar SET estatus=%s, decision=%s, fecha_decision=NOW() WHERE id=%s", (estatus_final, decision, reg_id))
        mysql.connection.commit()
    
    cur.close()
    return jsonify(success=True, id=reg_id, estado=estatus_final)

@bp_gestion_mermas.route('/mermas/check_status_live', methods=['GET'])
def check_status_live():
    factura = request.args.get('factura')
    if not factura: return jsonify(completed=False)
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    # Estados bloqueantes: 'pendiente' y 'segunda_revision'. 
    # 'aprobada_pendiente_revision' NO bloquea.
    cur.execute("SELECT count(*) as pendientes FROM mermas_pollosgar WHERE factura=%s AND estatus IN ('pendiente', 'segunda_revision')", (factura,))
    res = cur.fetchone()
    
    if res['pendientes'] > 0:
        cur.close()
        return jsonify(completed=False)
    
    cur.execute("SELECT id, estatus FROM mermas_pollosgar WHERE factura=%s ORDER BY id DESC LIMIT 1", (factura,))
    row = cur.fetchone()
    cur.close()
    
    final_status = 'aprobada'
    row_id = None
    if row:
        final_status = row['estatus']
        row_id = row['id']
        
    return jsonify(completed=True, status=final_status, id=row_id)

@bp_gestion_mermas.route('/mermas/pending', methods=['GET'])
def mermas_pending():
    """Ruta Legacy (Se mantiene por compatibilidad)"""
    empresa_id = request.args.get('empresa_id') or session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    sql = """
        SELECT id, fecha, cliente, vehiculo, factura, item, operador_nombre,
               kg_factura AS total_factura_kg, kg_item, kg_entregados, merma_kg, merma_pct,
               evidencia_url, evidencia_url1, evidencia_url2, estatus, nota_descuento
        FROM mermas_pollosgar 
        WHERE estatus IN ('pendiente', 'segunda_revision')
    """
    params = []
    if empresa_id:
        sql += " AND empresa_id=%s"
        params.append(empresa_id)
    sql += " ORDER BY fecha ASC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=rows)

@bp_gestion_mermas.route('/mermas/review_list', methods=['GET'])
def mermas_review_list():
    """Ruta Legacy (Se mantiene por compatibilidad)"""
    empresa_id = session.get('empresa_id') or session.get('nit')
    _limpiar_mermas_antiguas()
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    sql = """
        SELECT id, fecha, cliente, vehiculo, factura, item, operador_nombre,
               kg_factura AS total_factura_kg, kg_item, kg_entregados, merma_kg, merma_pct,
               evidencia_url, evidencia_url1, evidencia_url2, estatus, nota_descuento
        FROM mermas_pollosgar 
        WHERE estatus IN ('objetada', 'aprobada_no_conforme') AND evidencia_url IS NOT NULL 
    """
    params = []
    if empresa_id:
        sql += " AND empresa_id=%s"
        params.append(empresa_id)
    sql += " ORDER BY fecha DESC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=rows)

# ==============================================================================
# NUEVAS RUTAS PARA EL DASHBOARD MEJORADO (Listas, Testimonio, Decision)
# ==============================================================================

@bp_gestion_mermas.route('/mermas/list_dashboard', methods=['GET'])
def mermas_list_dashboard():
    """
    Ruta unificada para las 3 pestañas del Dashboard.
    Param 'view': 'envivo', 'nocturna', 'investigacion'
    """
    view = request.args.get('view', 'envivo')
    empresa_id = session.get('empresa_id') or session.get('nit')
    
    status_filter = ""
    if view == 'envivo':
        # Etapa 1 y 2 (Diurnas Activas)
        status_filter = "('pendiente', 'segunda_revision')"
    elif view == 'nocturna':
        # Etapa 2 (Diferidas Nocturnas)
        status_filter = "('aprobada_pendiente_revision')"
    elif view == 'investigacion':
        # Etapa 3
        status_filter = "('en_investigacion')"
    else:
        return jsonify(items=[])

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # CONSULTA ACTUALIZADA: Se agrega 'evidencia_nota' al final
    sql = f"""
        SELECT id, fecha, cliente, vehiculo, factura, item, operador_nombre,
               kg_factura AS total_factura_kg, kg_item, kg_entregados, merma_kg, merma_pct,
               evidencia_url, evidencia_url1, evidencia_url2, estatus, 
               testimonio_operador, nota_descuento, evidencia_nota
        FROM mermas_pollosgar 
        WHERE estatus IN {status_filter}
    """
    params = []
    if empresa_id:
        sql += " AND empresa_id=%s"
        params.append(empresa_id)
    
    sql += " ORDER BY fecha ASC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=rows)

@csrf.exempt
@bp_gestion_mermas.route('/mermas/guardar_testimonio', methods=['POST'])
def mermas_guardar_testimonio():
    """ETAPA 3: Guarda el relato del operador sin cambiar el estado."""
    j = request.get_json(force=True, silent=True)
    rid = j.get('id')
    txt = j.get('testimonio')
    cur = mysql.connection.cursor()
    cur.execute("UPDATE mermas_pollosgar SET testimonio_operador=%s WHERE id=%s", (txt, rid))
    mysql.connection.commit()
    cur.close()
    return jsonify(success=True)

@csrf.exempt
@bp_gestion_mermas.route('/mermas/decision_final', methods=['POST'])
def mermas_decision_final():
    """ETAPA 3 -> 4 o FIN: Decisión final tras investigación."""
    j = request.get_json(force=True, silent=True)
    rid = j.get('id')
    decision = j.get('decision') # 'conforme' o 'no_conforme'
    comentario = j.get('comentario', '')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM mermas_pollosgar WHERE id=%s", (rid,))
    row = cur.fetchone()

    files = [row.get('evidencia_url'), row.get('evidencia_url1'), row.get('evidencia_url2')]

    if decision == 'conforme':
        _delete_evidence_files(files)
        cur.execute("UPDATE mermas_pollosgar SET estatus='validada', comentario_control=%s WHERE id=%s", (comentario, rid))
    else:
        # ETAPA 4: A RRHH
        email_rrhh = _get_email_talento_humano(row['empresa_id']) or "rrhh@pollosgar.com"
        # Usamos la funcion existente pasandole el testimonio tambien
        _enviar_email_no_conforme(email_rrhh, row, comentario, files, testimonio_op=row.get('testimonio_operador'))
        _delete_evidence_files(files) 
        cur.execute("UPDATE mermas_pollosgar SET estatus='no_conforme_rrhh', comentario_control=%s WHERE id=%s", (comentario, rid))

    mysql.connection.commit()
    cur.close()
    return jsonify(success=True)

# ==============================================================================
# 4. RUTAS DE BÚSQUEDA (AUTOCOMPLETE)
# ==============================================================================

@bp_gestion_mermas.route('/mermas/buscar_productos', methods=['GET'])
def mermas_buscar_productos():
    q = request.args.get('q', '').strip()
    empresa_id = session.get('empresa_id') or session.get('nit')
    if not empresa_id or len(q) < 2: return jsonify(success=True, items=[])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        term = f"%{q}%"
        cur.execute("SELECT producto, sku FROM productos WHERE id_empresa=%s AND (producto LIKE %s OR sku LIKE %s) ORDER BY producto ASC LIMIT 15", (empresa_id, term, term))
        rows = cur.fetchall()
        cur.close()
        return jsonify(success=True, items=[{'label': f"{r['producto']} ({r['sku']})", 'value': r['producto']} for r in rows])
    except:
        return jsonify(success=False, items=[])

@bp_gestion_mermas.route('/mermas/buscar_vehiculos', methods=['GET'])
def mermas_buscar_vehiculos():
    q = request.args.get('q', '').strip()
    empresa_id = session.get('empresa_id') or session.get('nit')
    if not empresa_id: return jsonify(success=True, items=[])
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        term = f"%{q}%"
        cur.execute("SELECT DISTINCT placa FROM vehiculos WHERE id_empresa=%s AND placa LIKE %s ORDER BY placa ASC LIMIT 10", (empresa_id, term))
        rows = cur.fetchall()
        cur.close()
        return jsonify(success=True, items=[{'label': r['placa'], 'value': r['placa']} for r in rows])
    except:
        return jsonify(success=False, items=[])

@bp_gestion_mermas.route('/mermas/opciones', methods=['GET'])
def mermas_opciones():
    tipo = (request.args.get('tipo') or '').lower()
    empresa_id = session.get('empresa_id') or session.get('nit')
    col = {'zona': 'zona', 'vendedor': 'operador_nombre', 'cliente': 'cliente', 'vehiculo': 'vehiculo'}.get(tipo)
    if not col: return jsonify(success=False), 400
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    sql = f"SELECT DISTINCT {col} AS val FROM mermas_pollosgar WHERE {col} IS NOT NULL AND {col} <> ''"
    params = []
    if empresa_id: 
        sql += " AND empresa_id=%s"
        params.append(empresa_id)
    sql += f" ORDER BY {col} ASC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=[r['val'] for r in rows if r['val']])

# ==============================================================================
# 5. RUTAS DE EXTRAS Y CIERRE (DESCUENTOS, DEVOLUCIONES, PDF)
# ==============================================================================

@csrf.exempt
@bp_gestion_mermas.route('/mermas/actualizar_extras', methods=['POST'])
def mermas_actualizar_extras():
    j = request.get_json(force=True, silent=True) or {}
    row_id, tipo = j.get('id'), j.get('tipo')
    if not row_id or not tipo: return jsonify(success=False), 400
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT kg_item, kg_entregados, vunit_f FROM mermas_pollosgar WHERE id=%s", (row_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return jsonify(success=False), 404
    
    kg_entregados = float(row.get('kg_entregados') or 0)
    vunit_old = float(row.get('vunit_f') or 0)

    try:
        if tipo == 'descuento':
            d_unidad = int(j.get('d_unidad') or 0)
            vunit_in = float(j.get('vunit_f') or 0) 
            d_vunit = float(j.get('d_vunit') or 0) 
            diff_precio = vunit_in - d_vunit
            d_vtotal = kg_entregados * diff_precio
            cur.execute("UPDATE mermas_pollosgar SET d_unidad=%s, vunit_f=%s, d_vunit=%s, d_vtotal=%s WHERE id=%s", (d_unidad, vunit_in, d_vunit, d_vtotal, row_id))
            
        elif tipo == 'devolucion':
            dv_unidad = int(j.get('dv_unidad') or 0)
            dv_kg = float(j.get('dv_kg') or 0) 
            vunit_in = j.get('vunit_f')
            final_vunit = float(vunit_in) if (vunit_in and float(vunit_in) > 0) else vunit_old
            dv_vtotal = dv_kg * final_vunit
            cur.execute("UPDATE mermas_pollosgar SET dv_unidad=%s, dv_kg=%s, dv_vtotal=%s, vunit_f=%s WHERE id=%s", (dv_unidad, dv_kg, dv_vtotal, final_vunit, row_id))
        
        mysql.connection.commit()
        cur.close()
        return jsonify(success=True)
    except Exception as e:
        if cur: cur.close()
        return jsonify(success=False, message=str(e)), 500

@csrf.exempt
@bp_gestion_mermas.route('/mermas/finalizar_con_nota', methods=['POST'])
def mermas_finalizar_con_nota():
    """
    Finaliza el proceso:
    1. Recibe la firma digital del cliente.
    2. Genera el consecutivo de Nota Crédito.
    3. Guarda la firma en carpeta específica.
    4. Genera el PDF (Media Carta) con la firma incrustada.
    5. Envía el PDF por WhatsApp.
    """
    try:
        j = request.get_json(force=True, silent=True) or {}
    except:
        return jsonify(success=False, message="JSON inválido"), 400
    
    fac = j.get('factura')
    whatsapp_destino = j.get('whatsapp') 
    firma_b64 = j.get('firma_base64') # Recibimos la imagen en Base64

    if not fac:
        return jsonify(success=False, message="Falta número de factura"), 400
    
    empresa_id = session.get('empresa_id') or session.get('nit')
    empresa_nombre = session.get('empresa') or 'EMPRESA GENÉRICA'

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    try:
        # 1. Obtener el siguiente número de Nota Crédito
        nota_num = _obtener_siguiente_consecutivo(empresa_id, empresa_nombre)
        
        # 2. PROCESAMIENTO DE LA FIRMA (Carpeta Específica)
        firma_db_url = None
        
        if firma_b64:
            try:
                # Definir carpeta específica para firmas de notas crédito
                folder_name = "firmas_notasdev_pollosgar"
                folder_path = os.path.join(current_app.static_folder, folder_name)
                
                # Crear la carpeta si no existe
                os.makedirs(folder_path, exist_ok=True)
                
                # Crear nombre único para la firma: firma_NOTA_UUID.png
                # Usamos uuid para evitar colisiones de nombres
                filename = f"firma_{nota_num}_{uuid.uuid4().hex[:8]}.png"
                file_path = os.path.join(folder_path, filename)
                
                # Decodificar Base64 y guardar archivo
                # El string suele venir como "data:image/png;base64,iVBORw0KGgo..."
                if ',' in firma_b64:
                    header, b64data = firma_b64.split(',', 1)
                else:
                    b64data = firma_b64
                    
                with open(file_path, 'wb') as f:
                    f.write(base64.b64decode(b64data))
                
                # Guardar la ruta relativa para la BD (ej: firmas_notasdev_pollosgar/archivo.png)
                firma_db_url = f"{folder_name}/{filename}"
                
            except Exception as e:
                print(f"Error guardando firma: {str(e)}")
                # No detenemos el proceso, pero la firma quedará nula

        # 3. ACTUALIZAR BASE DE DATOS
        # Asignamos el número de nota y la ruta de la firma a los registros de esta factura
        sql_update = "UPDATE mermas_pollosgar SET nota_descuento=%s"
        params_update = [nota_num]
        
        if firma_db_url:
            sql_update += ", firma_receptor_url=%s"
            params_update.append(firma_db_url)
            
        sql_update += " WHERE factura=%s"
        params_update.append(fac)
        
        cur.execute(sql_update, tuple(params_update))
        
        # 4. GENERAR EL PDF
        # Recuperamos los datos actualizados para armar el documento
        cur.execute("SELECT * FROM mermas_pollosgar WHERE factura=%s", (fac,))
        rows = cur.fetchall()
        
        if rows:
            meta = rows[0]
            
            # Configuración de rutas para el PDF
            pdf_name = f"{nota_num}.pdf"
            folder_pdf_rel = "notasdev_pollosgar" # Carpeta de PDFs finales
            folder_pdf_abs = os.path.join(current_app.static_folder, folder_pdf_rel)
            
            # Crear carpeta de PDFs si no existe
            os.makedirs(folder_pdf_abs, exist_ok=True)
            
            pdf_full_path = os.path.join(folder_pdf_abs, pdf_name)
            
            # LLAMADA A LA FUNCIÓN GENERADORA (MEDIA CARTA)
            # Le pasamos 'firma_db_url' que acabamos de guardar
            _generar_pdf_nota_credito_media_carta(
                filepath=pdf_full_path,
                nota=nota_num,
                factura=fac,
                cliente=meta['cliente'],
                fecha=meta['fecha'],
                items=rows,
                empresa_nombre=empresa_nombre,
                empresa_id=empresa_id,
                operador=meta['operador_nombre'],
                whatsapp=whatsapp_destino,
                firma_path=firma_db_url
            )
            
            # 5. GUARDAR LA RUTA DEL PDF EN LA BD
            db_pdf_val = f"{folder_pdf_rel}/{pdf_name}"
            cur.execute("UPDATE mermas_pollosgar SET evidencia_nota=%s WHERE factura=%s", (db_pdf_val, fac))
            mysql.connection.commit()
            
            # 6. ENVIAR POR WHATSAPP (Twilio)
            if whatsapp_destino:
                # Generamos la URL pública (http://dominio.com/static/...)
                url_publica_pdf = url_for('static', filename=db_pdf_val, _external=True)
                _enviar_whatsapp_pdf(whatsapp_destino, url_publica_pdf, nota_num, empresa_nombre)
        
        cur.close()
        
        # Respuesta exitosa al Frontend
        return jsonify(
            success=True, 
            nota=nota_num, 
            message="Proceso finalizado correctamente. Nota generada y enviada."
        )
        
    except Exception as e:
        if cur: cur.close()
        current_app.logger.error(f"Error en mermas_finalizar_con_nota: {str(e)}")
        return jsonify(success=False, message=f"Error interno: {str(e)}"), 500    
    
def _generar_pdf_nota_credito_media_carta(filepath, nota, factura, cliente, fecha, items, empresa_nombre, empresa_id, operador, whatsapp, firma_path):
    """
    Genera PDF en MEDIA CARTA (Horizontal: 8.5 x 5.5 pulgadas)
    Dimensiones aprox: 612 x 396 puntos.
    """
    # Configurar tamaño MEDIA CARTA (Landscape de medio folio)
    # Ancho = 612 (Letter width), Alto = 396 (Half Letter height)
    PAGE_W, PAGE_H = 612, 396
    c = canvas.Canvas(filepath, pagesize=(PAGE_W, PAGE_H))
    
    color_corp = colors.HexColor("#015249")
    
    # 1. LOGO Y ENCABEZADO COMPACTO
    logo_file = os.path.join(current_app.static_folder, f"logo_{empresa_id}.png")
    if not os.path.exists(logo_file): logo_file = os.path.join(current_app.static_folder, "logo_energix360.png")
    
    if os.path.exists(logo_file):
        try: c.drawImage(ImageReader(logo_file), 20, PAGE_H-50, width=80, height=40, preserveAspectRatio=True, mask='auto')
        except: pass

    # Título y Datos Empresa
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(PAGE_W-20, PAGE_H-25, str(empresa_nombre).upper())
    c.setFont("Helvetica", 8)
    c.drawRightString(PAGE_W-20, PAGE_H-35, f"NIT: {empresa_id} | Fecha: {str(fecha)[:10]}")
    
    # Caja Nota Crédito
    c.setStrokeColor(color_corp); c.setLineWidth(1)
    c.roundRect(PAGE_W-160, PAGE_H-75, 140, 30, 4, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 9); c.setFillColor(color_corp)
    c.drawCentredString(PAGE_W-90, PAGE_H-58, "NOTA CRÉDITO")
    c.setFont("Helvetica-Bold", 14); c.setFillColor(colors.HexColor("#b91c1c"))
    c.drawCentredString(PAGE_W-90, PAGE_H-70, f"No. {nota}")
    c.setFillColor(colors.black)
    
    # Datos Cliente
    y = PAGE_H - 70
    c.setFont("Helvetica-Bold", 9); c.drawString(20, y, "CLIENTE:"); 
    c.setFont("Helvetica", 9);      c.drawString(70, y, str(cliente)[:45])
    c.setFont("Helvetica-Bold", 9); c.drawString(20, y-12, "REF:"); 
    c.setFont("Helvetica", 9);      c.drawString(70, y-12, f"Factura {factura}")

    # 2. TABLA DE ÍTEMS
    y_hdr = y - 35
    c.setFillColor(color_corp)
    c.rect(20, y_hdr-4, PAGE_W-40, 15, fill=1, stroke=0)
    c.setFillColor(colors.white); c.setFont("Helvetica-Bold", 7)
    
    cols = [25, 60, 260, 300, 360, 430, 500]
    c.drawString(cols[0], y_hdr, "TIPO")
    c.drawString(cols[1], y_hdr, "DESCRIPCIÓN")
    c.drawCentredString(cols[2]+10, y_hdr, "CANT")
    c.drawRightString(cols[4]-5, y_hdr, "V.UNIT")
    c.drawRightString(cols[5]-5, y_hdr, "TOTAL")
    c.drawString(cols[5], y_hdr, " DETALLE")
    
    c.setFillColor(colors.black); c.setFont("Helvetica", 7)
    y = y_hdr - 12
    total_nota = 0
    
    for it in items:
        # Lógica de cálculo (igual que antes)
        merma_kg = float(it.get('merma_kg') or 0)
        vunit = float(it.get('vunit_f') or 0)
        d_vunit = float(it.get('d_vunit') or 0)
        dv_kg = float(it.get('dv_kg') or 0)
        prod = str(it.get('item'))[:40]
        
        # Render Merma
        if merma_kg > 0:
            p_app = d_vunit if d_vunit > 0 else vunit
            tot = merma_kg * p_app
            c.drawString(cols[0], y, "MRM"); c.drawString(cols[1], y, prod)
            c.drawCentredString(cols[2]+10, y, f"{merma_kg:.2f}")
            c.drawRightString(cols[4]-5, y, f"{p_app:,.0f}"); c.drawRightString(cols[5]-5, y, f"{tot:,.0f}")
            c.drawString(cols[5], y, " MERMA")
            total_nota += tot; y -= 10
            
        # Render Devolución
        if dv_kg > 0:
            tot = dv_kg * vunit
            c.drawString(cols[0], y, "DEV"); c.drawString(cols[1], y, prod)
            c.drawCentredString(cols[2]+10, y, f"{dv_kg:.2f}")
            c.drawRightString(cols[4]-5, y, f"{vunit:,.0f}"); c.drawRightString(cols[5]-5, y, f"{tot:,.0f}")
            c.drawString(cols[5], y, " DEVOLUCIÓN")
            total_nota += tot; y -= 10
            
        # Render Descuento
        if d_vunit > 0 and vunit > d_vunit:
            diff = vunit - d_vunit
            kg_base = float(it.get('kg_entregados') or 0)
            tot = kg_base * diff
            c.drawString(cols[0], y, "DCTO"); c.drawString(cols[1], y, prod)
            c.drawCentredString(cols[2]+10, y, f"{kg_base:.2f}")
            c.drawRightString(cols[4]-5, y, f"{diff:,.0f}"); c.drawRightString(cols[5]-5, y, f"{tot:,.0f}")
            c.drawString(cols[5], y, " DESCUENTO")
            total_nota += tot; y -= 10
            
        if y < 60: c.showPage(); y = PAGE_H - 50

    # 3. TOTALES Y FIRMAS (PIE DE PÁGINA)
    # Línea de Total
    y -= 5
    c.setStrokeColor(color_corp); c.line(20, y, PAGE_W-20, y); y -= 15
    c.setFont("Helvetica-Bold", 10)
    c.drawRightString(PAGE_W-130, y, "TOTAL A FAVOR:")
    c.setFont("Helvetica-Bold", 12); c.setFillColor(colors.HexColor("#15803d"))
    c.drawRightString(PAGE_W-20, y, f"$ {total_nota:,.0f}")
    c.setFillColor(colors.black)
    
    # Bloque de Firmas (Ajustado al pie)
    y_firmas = 40
    
    # Firma Cliente (Imagen capturada)
    if firma_path:
        full_firma_path = os.path.join(current_app.static_folder, firma_path)
        if os.path.exists(full_firma_path):
            try:
                # Dibujar imagen firma
                c.drawImage(ImageReader(full_firma_path), 40, y_firmas, width=100, height=50, preserveAspectRatio=True, mask='auto')
            except: pass
    
    c.setStrokeColor(colors.black); c.setLineWidth(1)
    c.line(30, y_firmas, 180, y_firmas) # Línea sobre la que va la firma (o debajo)
    c.setFont("Helvetica", 6)
    c.drawCentredString(105, y_firmas-8, "FIRMA RECIBIDO / CLIENTE")
    
    # Info Operador
    c.setFont("Helvetica-Oblique", 6); c.setFillColor(colors.grey)
    c.drawRightString(PAGE_W-20, 15, f"Diligenciado digitalmente por: {str(operador).upper()}")
    c.drawRightString(PAGE_W-20, 8, f"Plataforma BQA-ONE | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    c.save()
    
def _enviar_whatsapp_pdf(numero_destino, pdf_url_publica, nota_num, empresa_nombre):
    if not numero_destino or not TWILIO_SID or not TWILIO_TOKEN: return False
    try:
        from twilio.rest import Client
        client = Client(TWILIO_SID, TWILIO_TOKEN)
        num_str = str(numero_destino).strip().replace('whatsapp:', '')
        if not num_str.startswith('+'): num_str = f"+57{num_str}"
        whatsapp_to = f"whatsapp:{num_str}"
        whatsapp_from = "whatsapp:+14155238886"
        es_local = '127.0.0.1' in pdf_url_publica or 'localhost' in pdf_url_publica
        cuerpo = f"✅ *PROCESO FINALIZADO*\n\nSu Nota Crédito *No. {nota_num}* ha sido generada exitosamente.\n\n_Documento generado por BQA-ONE para {empresa_nombre}_"
        send_params = { "body": cuerpo, "from_": whatsapp_from, "to": whatsapp_to }
        if es_local: send_params["body"] += "\n\n⚠️ *Nota:* El PDF no se adjunta en pruebas locales."
        else: send_params["media_url"] = [pdf_url_publica]
        client.messages.create(**send_params)
        return True
    except Exception as e:
        print(f"❌ Error Twilio: {e}")
        return False

@csrf.exempt
@bp_gestion_mermas.route('/mermas/consulta', methods=['POST'])
def mermas_consulta():
    j = request.get_json(force=True, silent=True) or {}
    t, d, h = (j.get('tipo') or '').lower(), j.get('desde'), j.get('hasta')
    eid = session.get('empresa_id') or session.get('nit')
    w, p = [], []
    if eid: w.append("empresa_id=%s"); p.append(eid)
    
    if t=='zona': w.append("zona=%s"); p.append(j.get('zona'))
    elif t=='vendedor': w.append("operador_nombre=%s"); p.append(j.get('vendedor'))
    elif t=='cliente': w.append("cliente=%s"); p.append(j.get('cliente'))
    elif t=='vehiculo': 
        # Usamos UPPER para ignorar si escribieron en minúscula o mayúscula
        w.append("UPPER(vehiculo) = %s")
        p.append(str(j.get('vehiculo')).upper().strip())
    
    if d: w.append("DATE(fecha)>=%s"); p.append(d)
    if h: w.append("DATE(fecha)<=%s"); p.append(h)
    
    ws = ("WHERE "+" AND ".join(w)) if w else ""
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute(f"SELECT COALESCE(SUM(kg_item),0)k, COALESCE(SUM(merma_kg),0)m, COUNT(*)c FROM mermas_pollosgar {ws}", tuple(p))
    res = cur.fetchone()
    cur.execute(f"SELECT cliente, COALESCE(SUM(kg_item),0)k, COALESCE(SUM(merma_kg),0)m FROM mermas_pollosgar {ws} GROUP BY cliente ORDER BY k DESC", tuple(p))
    det = cur.fetchall()
    cur.execute(f"SELECT * FROM mermas_pollosgar {ws} ORDER BY fecha ASC", tuple(p))
    ops = cur.fetchall()
    cur.close()
    
    kgt, mt = float(res['k']), float(res['m'])
    dcli = [{'cliente':r['cliente'],'kg_cliente':float(r['k']),'merma_cliente':float(r['m']),'pct_cliente':(float(r['m'])/float(r['k'])*100) if float(r['k'])>0 else 0} for r in det]
    
    oper = []
    for o in ops:
        oper.append({'fecha':str(o['fecha']), 'cliente':o['cliente'], 'factura':o['factura'], 'kg_factura':float(o['kg_item']), 'kg_entregados':float(o['kg_entregados']), 'merma_kg':float(o['merma_kg']), 'merma_pct':float(o['merma_pct'])})
    
    cht = {'type':'bar','labels':[],'values':[]}
    if t in ('zona','vendedor'): 
        cht['labels']=[x['cliente'] for x in dcli]
        cht['values']=[x['pct_cliente'] for x in dcli]
    elif t=='cliente':
        fd={} 
        for o in oper: 
            f=o['factura']
            fd[f]=fd.get(f,{'k':0,'m':0})
            fd[f]['k']+=o['kg_factura']
            fd[f]['m']+=o['merma_kg']
        cht['labels']=list(fd.keys())
        cht['values']=[(v['m']/v['k']*100 if v['k']>0 else 0) for v in fd.values()]
        
    return jsonify({'success':True, 'resumen':{'kg_totales':kgt,'merma_total':mt,'merma_pct_total':(mt/kgt*100) if kgt>0 else 0,'registros':res['c']}, 'detalle_clientes':dcli, 'operaciones':oper, 'chart':cht})

@csrf.exempt
@bp_gestion_mermas.route('/mermas/consulta/pdf', methods=['POST'])
def mermas_consulta_pdf():
    j = request.get_json(force=True, silent=True) or {}
    res = j.get('resumen') or {}
    cpng = j.get('chart_png')
    
    # 1. CAPTURAR NOMBRE DINÁMICO
    filename = j.get('filename') or "Reporte_Mermas.pdf"
    titulo = j.get('titulo') or "Informe de Mermas"
    
    b = BytesIO()
    # Usamos A4 Horizontal (Landscape) para mejor gráfica
    p = canvas.Canvas(b, pagesize=landscape(A4))
    W, H = landscape(A4)
    
    # Encabezado
    p.setFont("Helvetica-Bold", 16)
    p.drawString(2*cm, H-2*cm, titulo)
    p.setFont("Helvetica", 10)
    p.drawString(2*cm, H-2.7*cm, f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    p.setFont("Helvetica-Bold", 12)
    p.drawString(2*cm, H-4*cm, f"Total KG: {res.get('kg_totales')} | Merma: {res.get('merma_total')} kg | %: {float(res.get('merma_pct_total',0)):.2f}%")
    
    y = H-6*cm
    
    # Imagen del Gráfico
    if cpng and cpng.startswith('data:'):
        try: 
            p.drawImage(ImageReader(BytesIO(base64.b64decode(cpng.split(',')[1]))), 2*cm, y-9*cm, width=W-4*cm, height=9*cm, preserveAspectRatio=True)
            y -= 10*cm
        except: pass
        
    # Tabla de datos
    p.setFont("Helvetica-Bold",10)
    # Ajustamos columnas para que se vean bien
    headers = ["Hora", "Fecha", "Cliente", "Factura", "Kg Ent", "Merma", "%"]
    x_pos = [2*cm, 4*cm, 7*cm, 14*cm, 18*cm, 21*cm, 24*cm]
    
    for i, h_text in enumerate(headers):
        p.drawString(x_pos[i], y, h_text)
        
    y -= 0.8*cm
    p.setStrokeColor(colors.lightgrey)
    p.line(2*cm, y+0.5*cm, W-2*cm, y+0.5*cm)
    
    p.setFont("Helvetica",9)
    for o in (j.get('operaciones')or[])[:50]:
        if y < 2*cm: p.showPage(); y = H-2*cm
        
        # Parseo seguro de fecha
        f_str = str(o.get('fecha'))
        hora = f_str[11:16] if len(f_str)>16 else ""
        fecha = f_str[:10]
        
        p.drawString(x_pos[0], y, hora)
        p.drawString(x_pos[1], y, fecha)
        p.drawString(x_pos[2], y, str(o.get('cliente'))[:25])
        p.drawString(x_pos[3], y, str(o.get('factura')))
        p.drawString(x_pos[4], y, str(o.get('kg_entregados')))
        p.setFillColor(colors.HexColor("#b91c1c"))
        p.drawString(x_pos[5], y, str(o.get('merma_kg')))
        p.setFillColor(colors.black)
        p.drawString(x_pos[6], y, f"{o.get('merma_pct')}%")
        
        y -= 0.6*cm
        
    p.showPage(); p.save(); b.seek(0)
    
    # 2. ENVIAR CON NOMBRE DINÁMICO
    return send_file(b, mimetype='application/pdf', as_attachment=True, download_name=filename)
# ==============================================================================
# 6. RUTAS DE FLUJO DE TRABAJO DEL CONTROLADOR (NUEVAS)
# ==============================================================================

@csrf.exempt
@bp_gestion_mermas.route('/mermas/reporte_controlador_diario', methods=['GET'])
def mermas_reporte_controlador_diario():
    """
    ACCIÓN 1: Reporte detallado del día anterior (Merma <= 2%).
    Incluye totales de Merma, Devoluciones y Descuentos.
    """
    # Determinar fecha ayer
    ayer = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    # Opcional: permitir pasar fecha por param
    fecha_reporte = request.args.get('fecha', ayer)
    
    empresa_id = session.get('empresa_id') or session.get('nit')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    # Filtro: Fecha específica Y Merma <= 2%
    sql = """
        SELECT fecha, factura, nota_descuento, operador_nombre, cliente, vehiculo,
               kg_item as kg_factura, merma_kg, merma_pct,
               d_vtotal, dv_vtotal
        FROM mermas_pollosgar 
        WHERE DATE(fecha) = %s AND merma_pct <= 2.0
    """
    params = [fecha_reporte]
    if empresa_id:
        sql += " AND empresa_id = %s"
        params.append(empresa_id)
        
    sql += " ORDER BY factura ASC"
    
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()

    # Calcular Totales
    total_merma_kg = sum([float(r['merma_kg'] or 0) for r in rows])
    total_desc_money = sum([float(r['d_vtotal'] or 0) for r in rows])
    total_dev_money = sum([float(r['dv_vtotal'] or 0) for r in rows])

    # Generar PDF
    b = BytesIO()
    p = canvas.Canvas(b, pagesize=landscape(A4))
    W, H = landscape(A4)
    
    # Encabezado
    p.setFont("Helvetica-Bold", 14)
    p.drawString(2*cm, H-2*cm, f"Reporte Diario de Control (Mermas ≤ 2%)")
    p.setFont("Helvetica", 10)
    p.drawString(2*cm, H-2.6*cm, f"Fecha Reportada: {fecha_reporte} | Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    
    # Resumen Totales
    y = H-4*cm
    p.setFillColor(colors.HexColor("#f0fdf4"))
    p.rect(2*cm, y-15, W-4*cm, 30, fill=1, stroke=0)
    p.setFillColor(colors.black)
    p.setFont("Helvetica-Bold", 10)
    p.drawString(3*cm, y-5, f"TOTAL MERMA: {total_merma_kg:,.2f} Kg")
    p.drawString(10*cm, y-5, f"TOTAL DESCUENTOS: ${total_desc_money:,.0f}")
    p.drawString(18*cm, y-5, f"TOTAL DEVOLUCIONES: ${total_dev_money:,.0f}")
    
    # Tabla Detalle
    y -= 2*cm
    headers = ["Factura", "NC", "Operador", "Cliente", "Vehículo", "Merma Kg", "Desc ($)", "Dev ($)"]
    x_pos = [2*cm, 4*cm, 6*cm, 12*cm, 18*cm, 21*cm, 24*cm, 27*cm]
    
    p.setFont("Helvetica-Bold", 9)
    for i, h_text in enumerate(headers):
        p.drawString(x_pos[i], y, h_text)
    
    y -= 0.8*cm
    p.setFont("Helvetica", 8)
    
    for r in rows:
        if y < 2*cm: p.showPage(); y = H-2*cm
        
        p.drawString(x_pos[0], y, str(r['factura']))
        p.drawString(x_pos[1], y, str(r['nota_descuento'] or '-'))
        p.drawString(x_pos[2], y, str(r['operador_nombre'])[:20])
        p.drawString(x_pos[3], y, str(r['cliente'])[:25])
        p.drawString(x_pos[4], y, str(r['vehiculo']))
        p.drawString(x_pos[5], y, f"{float(r['merma_kg']):.2f}")
        p.drawString(x_pos[6], y, f"{float(r['d_vtotal']):,.0f}")
        p.drawString(x_pos[7], y, f"{float(r['dv_vtotal']):,.0f}")
        
        y -= 0.6*cm
        p.setStrokeColor(colors.lightgrey)
        p.line(2*cm, y+0.4*cm, W-2*cm, y+0.4*cm)

    p.save()
    b.seek(0)
    return send_file(b, mimetype='application/pdf', as_attachment=True, download_name=f"Control_Diario_{fecha_reporte}.pdf")

@csrf.exempt
@bp_gestion_mermas.route('/mermas/notas_masivas_diario', methods=['GET'])
def mermas_notas_masivas_diario():
    """
    ACCIÓN 2: Generar un solo PDF con todas las Notas Crédito del día anterior (<= 2%).
    """
    ayer = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    fecha_reporte = request.args.get('fecha', ayer)
    empresa_id = session.get('empresa_id') or session.get('nit')
    empresa_nombre = session.get('empresa') or 'EMPRESA'

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    # Obtenemos las facturas únicas que cumplen la condición
    sql = """
        SELECT DISTINCT factura, cliente, fecha, operador_nombre, nota_descuento 
        FROM mermas_pollosgar 
        WHERE DATE(fecha) = %s AND merma_pct <= 2.0 AND nota_descuento != ''
    """
    params = [fecha_reporte]
    if empresa_id:
        sql += " AND empresa_id = %s"
        params.append(empresa_id)
        
    cur.execute(sql, tuple(params))
    facturas_cabecera = cur.fetchall()
    
    if not facturas_cabecera:
        return jsonify(success=False, message="No hay notas crédito generadas para esa fecha/criterio.")

    # Generamos un PDF multipágina
    b = BytesIO()
    c = canvas.Canvas(b, pagesize=LETTER)
    
    for fac_row in facturas_cabecera:
        # Obtener items de esa factura
        cur.execute("SELECT * FROM mermas_pollosgar WHERE factura=%s", (fac_row['factura'],))
        items = cur.fetchall()
        
        # Usamos la lógica de dibujo existente pero sobre el canvas 'c' compartido
        # NOTA: _generar_pdf_nota_credito guarda y cierra el canvas. 
        # Aquí debemos replicar el dibujo sin cerrar el canvas para concatenar páginas.
        # Por simplicidad, llamaremos a una función interna de dibujo que no haga .save()
        
        _dibujar_pagina_nota(c, fac_row['nota_descuento'], fac_row['factura'], fac_row['cliente'], 
                             fac_row['fecha'], items, empresa_nombre, empresa_id, fac_row['operador_nombre'])
        
        c.showPage() # Nueva página por cada factura

    c.save()
    cur.close()
    b.seek(0)
    return send_file(b, mimetype='application/pdf', as_attachment=True, download_name=f"Notas_Masivas_{fecha_reporte}.pdf")

def _dibujar_pagina_nota(c, nota, factura, cliente, fecha, items, empresa_nombre, empresa_id, operador):
    """Auxiliar para dibujar una nota en un canvas abierto (Reutilizando lógica)"""
    w, h = LETTER
    # (Copiar aquí la lógica de dibujo de _generar_pdf_nota_credito pero SIN c.save())
    # ... Lógica resumida de dibujo ...
    logo = os.path.join(current_app.static_folder, f"logo_{empresa_id}.png")
    if os.path.exists(logo): 
        try: c.drawImage(logo, 30, h-80, width=90, preserveAspectRatio=True, mask='auto')
        except: pass
        
    c.setFont("Helvetica-Bold", 11); c.drawCentredString(w/2, h-40, str(empresa_nombre).upper())
    c.setFont("Helvetica", 9); c.drawCentredString(w/2, h-55, f"NIT: {empresa_id}")
    c.setStrokeColor(colors.black); c.roundRect(w-170, h-85, 140, 50, 4, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 12); c.drawString(w-155, h-55, "NOTA CRÉDITO")
    c.setFont("Helvetica-Bold", 18); c.setFillColor(colors.red); c.drawCentredString(w-100, h-78, str(nota)); c.setFillColor(colors.black)
    
    y = h-125
    c.setFont("Helvetica-Bold",10); c.drawString(35,y,f"Cliente: {cliente}"); c.drawString(350,y,f"Factura: {factura}")
    y-=18; c.setFont("Helvetica-Bold",10); c.drawString(35,y,f"Fecha: {str(fecha)[:10]}")
    
    y_hdr = y-30
    c.setFillColor(colors.lightgrey); c.rect(30, y_hdr-5, w-60, 20, fill=1, stroke=1); c.setFillColor(colors.black)
    cols = [35, 80, 260, 310, 370, 440, 500]
    hdrs = ["COD", "DESCRIPCIÓN", "UND", "KILOS", "V. UNIT", "TOTAL", "CONCEPTO"]
    c.setFont("Helvetica-Bold",8); [c.drawString(cols[i], y_hdr, t) for i,t in enumerate(hdrs)]
    
    y = y_hdr-20; c.setFont("Helvetica",8)
    total_nota = 0
    
    for it in items:
        prod = str(it.get('item'))[:35]
        merma_kg = float(it.get('merma_kg') or 0)
        vunit = float(it.get('vunit_f') or 0)
        dv_kg = float(it.get('dv_kg') or 0)
        
        if merma_kg > 0:
            tot = merma_kg * vunit
            c.drawString(cols[0], y, "MRM"); c.drawString(cols[1], y, prod)
            c.drawCentredString(cols[2]+10, y, "1"); c.drawCentredString(cols[3]+10, y, f"{merma_kg:.2f}")
            c.drawRightString(cols[5]-5, y, f"{vunit:,.0f}"); c.drawRightString(cols[6]-10, y, f"{tot:,.0f}")
            c.drawString(cols[6], y, "MERMA")
            total_nota += tot; y -= 12
        if dv_kg > 0:
            tot = dv_kg * vunit
            c.drawString(cols[0], y, "DEV"); c.drawString(cols[1], y, prod)
            c.drawCentredString(cols[2]+10, y, str(it.get('dv_unidad')or 0))
            c.drawCentredString(cols[3]+10, y, f"{dv_kg:.2f}")
            c.drawRightString(cols[5]-5, y, f"{vunit:,.0f}"); c.drawRightString(cols[6]-10, y, f"{tot:,.0f}")
            c.drawString(cols[6], y, "DEVOLUCION")
            total_nota += tot; y -= 12
            
    y -= 30
    c.setFont("Helvetica-Bold", 11); c.drawString(w-250, y, "Total Nota Crédito:")
    c.setFont("Helvetica-Bold", 14); c.drawRightString(w-40, y, f"$ {total_nota:,.0f}")
    
    
# ==============================================================================
# REPORTE GERENCIAL DE VENTAS (INFO 1, 2 y 3)
# ==============================================================================

@csrf.exempt
@bp_gestion_mermas.route('/mermas/reporte_ventas_avanzado', methods=['GET'])
def mermas_reporte_ventas_avanzado():
    # 1. OBTENER FILTROS
    inicio = request.args.get('inicio')
    fin = request.args.get('fin')
    empresa_id = session.get('empresa_id') or session.get('nit')
    empresa_nombre = session.get('empresa', 'EMPRESA CLIENTE').upper()
    
    if not inicio or not fin:
        return "Fechas requeridas", 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # 2. CONSULTA MAESTRA
    sql = """
        SELECT fecha, factura, operador_nombre, cliente, vehiculo, zona,
               kg_item as kg_fact, kg_entregados as kg_entr, merma_kg,
               COALESCE(vunit_f, 0) as precio,
               d_vtotal, dv_vtotal
        FROM mermas_pollosgar 
        WHERE DATE(fecha) BETWEEN %s AND %s
    """
    params = [inicio, fin]
    if empresa_id:
        sql += " AND empresa_id = %s"
        params.append(empresa_id)
        
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()

    # 3. PROCESAMIENTO
    rank_ventas = {'vendedor': {}, 'vehiculo': {}, 'zona': {}}
    rank_mermas = {'vendedor': {}, 'vehiculo': {}}
    rank_devol = {'vendedor': {}, 'vehiculo': {}}
    rank_desc = {'vendedor': {}, 'vehiculo': {}}
    
    T = {
        'kg_fact': 0, 'kg_entr': 0, 'merma_kg': 0,
        'money_fact': 0, 'money_venta': 0, 
        'money_desc': 0, 'money_dev': 0, 'money_merma': 0, 'money_neto': 0
    }
    
    detalle_facturas = [] 

    for r in rows:
        precio = float(r['precio'])
        kg_f = float(r['kg_fact'] or 0)
        kg_e = float(r['kg_entr'] or 0)
        m_kg = float(r['merma_kg'] or 0)
        d_val = float(r['d_vtotal'] or 0)
        dv_val = float(r['dv_vtotal'] or 0)
        
        m_val = m_kg * precio
        f_val = kg_f * precio
        v_val = kg_e * precio
        neto_val = f_val - dv_val - d_val - m_val

        # Acumular
        T['kg_fact'] += kg_f; T['kg_entr'] += kg_e; T['merma_kg'] += m_kg
        T['money_fact'] += f_val; T['money_venta'] += v_val
        T['money_desc'] += d_val; T['money_dev'] += dv_val; T['money_merma'] += m_val; T['money_neto'] += neto_val

        def add_rank(dic, key, val):
            col_bd = 'operador_nombre' if key == 'vendedor' else key
            dato = r.get(col_bd)
            k = (str(dato) if dato else 'N/A').strip().upper()
            dic[key][k] = dic[key].get(k, 0) + val

        add_rank(rank_ventas, 'vendedor', v_val); add_rank(rank_ventas, 'vehiculo', v_val); add_rank(rank_ventas, 'zona', v_val)
        add_rank(rank_mermas, 'vendedor', m_val); add_rank(rank_mermas, 'vehiculo', m_val)
        add_rank(rank_devol, 'vendedor', dv_val); add_rank(rank_devol, 'vehiculo', dv_val)
        add_rank(rank_desc, 'vendedor', d_val); add_rank(rank_desc, 'vehiculo', d_val)

        detalle_facturas.append({
            'cliente': r['cliente'], 'vehiculo': r['vehiculo'], 'zona': r['zona'],
            'facturado': f_val, 'dev': dv_val, 'desc': d_val, 'merma': m_val, 'neto': neto_val, 'factura': r['factura']
        })

    T['pct_merma'] = (T['merma_kg'] / T['kg_fact'] * 100) if T['kg_fact'] > 0 else 0
    T['pct_desc'] = (T['money_desc'] / T['money_fact'] * 100) if T['money_fact'] > 0 else 0

    # 4. GENERACIÓN PDF
    b = BytesIO()
    c = canvas.Canvas(b, pagesize=LETTER)
    w, h = LETTER
    
    def draw_layout_base():
        # 1. LOGO IZQUIERDA
        logo_path = os.path.join(current_app.static_folder, f"logo_{empresa_id}.png")
        if not os.path.exists(logo_path):
            logo_path = os.path.join(current_app.static_folder, "logo_energix360.png")
        if os.path.exists(logo_path):
            try: c.drawImage(ImageReader(logo_path), 30, h-70, width=100, height=50, preserveAspectRatio=True, mask='auto')
            except: pass

        # 2. TÍTULO CENTRADO (NUEVO)
        c.setFont("Helvetica-Bold", 18)
        c.drawCentredString(w/2, h-50, "INFORME DE VENTAS") # <--- CENTRADO AQUÍ
        
        # Nombre empresa y fechas derecha
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(w-30, h-40, empresa_nombre)
        c.setFont("Helvetica", 9)
        c.drawRightString(w-30, h-55, f"Del: {inicio} Al: {fin}")
        
        # Línea separadora
        c.setStrokeColor(colors.HexColor("#015249")); c.setLineWidth(2)
        c.line(30, h-75, w-30, h-75)
        
        # Footer
        c.setFont("Helvetica-Oblique", 8); c.setFillColor(colors.grey)
        c.drawCentredString(w/2, 20, f"Generado por BQA-ONE | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        c.setFillColor(colors.black)

    # --- PÁGINA 1 ---
    draw_layout_base()
    
    # --- CAJA DE TOTALES (AMPLIADA) ---
    y_start = h - 100
    box_height = 110
    c.setStrokeColor(colors.grey); c.setLineWidth(1)
    c.setFillColor(colors.HexColor("#f8fafc"))
    c.roundRect(30, y_start - box_height, w-60, box_height, 8, fill=1, stroke=1)
    c.setFillColor(colors.black)
    
    # Coordenadas Totales (Más separadas)
    c1_lbl = 45;  c1_val = 280
    c2_lbl = 310; c2_val = 560
    curr_y = y_start - 25
    gap = 22

    # Fila 1
    c.setFont("Helvetica-Bold", 10); c.drawString(c1_lbl, curr_y, "KG FACTURADOS:")
    c.setFont("Helvetica", 10);      c.drawRightString(c1_val, curr_y, f"{T['kg_fact']:,.2f}")
    c.setFont("Helvetica-Bold", 10); c.drawString(c2_lbl, curr_y, "KG ENTREGADOS:")
    c.setFont("Helvetica", 10);      c.drawRightString(c2_val, curr_y, f"{T['kg_entr']:,.2f}")
    curr_y -= gap

    # Fila 2
    c.setFont("Helvetica-Bold", 10); c.drawString(c1_lbl, curr_y, "$ VENTA BRUTA:")
    c.setFont("Helvetica", 10);      c.drawRightString(c1_val, curr_y, f"${T['money_venta']:,.0f}")
    c.setFont("Helvetica-Bold", 10); c.drawString(c2_lbl, curr_y, "$ VENTA NETA:")
    c.setFont("Helvetica-Bold", 12); c.setFillColor(colors.HexColor("#15803d"))
    c.drawRightString(c2_val, curr_y, f"${T['money_neto']:,.0f}")
    c.setFillColor(colors.black)
    curr_y -= gap

    # Fila 3
    c.setFont("Helvetica-Bold", 10); c.drawString(c1_lbl, curr_y, "$ DESCUENTOS:")
    c.setFont("Helvetica", 10);      c.drawRightString(c1_val, curr_y, f"${T['money_desc']:,.0f} ({T['pct_desc']:.1f}%)")
    c.setFont("Helvetica-Bold", 10); c.drawString(c2_lbl, curr_y, "$ DEVOLUCIONES:")
    c.setFont("Helvetica", 10);      c.drawRightString(c2_val, curr_y, f"${T['money_dev']:,.0f}")
    curr_y -= gap

    # Fila 4
    c.setFont("Helvetica-Bold", 10); c.drawString(c1_lbl, curr_y, "$ MERMA TOTAL:")
    c.setFont("Helvetica-Bold", 10); c.setFillColor(colors.HexColor("#b91c1c"))
    c.drawRightString(c1_val, curr_y, f"${T['money_merma']:,.0f}")
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 10); c.drawString(c2_lbl, curr_y, "% MERMA GLOBAL:")
    c.setFont("Helvetica", 10);      c.drawRightString(c2_val, curr_y, f"{T['pct_merma']:.2f}%")

    # --- RANKINGS (ESPACIADO CORREGIDO) ---
    y = y_start - box_height - 30
    c.setFont("Helvetica-Bold", 12); c.setFillColor(colors.HexColor("#015249"))
    c.drawString(30, y, "RANKINGS OPERATIVOS"); c.setFillColor(colors.black)
    y -= 25
    
    # Coordenadas Columnas Ranking
    rc1 = 30; rc2 = 230; rc3 = 430
    
    def draw_ranking_table(title, data_dict, x, start_y, is_bad=False):
        sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)[:5]
        cur_y = start_y
        c.setFillColor(colors.HexColor("#b91c1c") if is_bad else colors.HexColor("#015249"))
        c.setFont("Helvetica-Bold", 8) # Letra más pequeña título
        c.drawString(x, cur_y, title)
        c.setFillColor(colors.black)
        cur_y -= 12
        c.setFont("Helvetica", 8)
        
        for k, val in sorted_items:
            # --- CORRECCIÓN CLAVE: TRUNCAR NOMBRE A 15 CARACTERES ---
            nombre_corto = (k[:15] + '..') if len(k) > 15 else k
            
            c.drawString(x, cur_y, nombre_corto)
            # Valor alineado a la derecha, con espacio suficiente
            c.drawRightString(x + 140, cur_y, f"${val:,.0f}")
            cur_y -= 10
        return cur_y

    draw_ranking_table("TOP VENTAS - VENDEDOR", rank_ventas['vendedor'], rc1, y)
    draw_ranking_table("TOP VENTAS - VEHÍCULO", rank_ventas['vehiculo'], rc2, y)
    draw_ranking_table("TOP VENTAS - ZONA", rank_ventas['zona'], rc3, y)
    
    y -= 85
    draw_ranking_table("MAYOR MERMA ($) - VENDEDOR", rank_mermas['vendedor'], rc1, y, True)
    draw_ranking_table("MAYOR MERMA ($) - VEHÍCULO", rank_mermas['vehiculo'], rc2, y, True)
    
    y -= 85
    draw_ranking_table("MAYOR DEVOLUCIÓN ($)", rank_devol['vendedor'], rc1, y, True)
    draw_ranking_table("MAYOR DESCUENTO ($)", rank_desc['vendedor'], rc2, y, True)

    c.showPage()

    # --- PÁGINA 2: DETALLE ---
    draw_layout_base()
    y = h - 100
    c.setFont("Helvetica-Bold", 12); c.setFillColor(colors.HexColor("#015249"))
    c.drawString(30, y, "DETALLE DE FACTURAS Y NETOS"); c.setFillColor(colors.black)
    y -= 20
    
    headers = ["Fact", "Cliente", "Zona", "$ Fact", "$ Dev", "$ Desc", "$ Merma", "$ NETO"]
    pos_x = [30, 70, 200, 260, 320, 370, 430, 500] # Ajustado
    
    c.setFont("Helvetica-Bold", 7)
    c.setFillColor(colors.lightgrey); c.rect(25, y-5, w-50, 15, fill=1); c.setFillColor(colors.black)
    for i, head in enumerate(headers):
        c.drawString(pos_x[i], y, head)
    y -= 15
    
    c.setFont("Helvetica", 7)
    for row in detalle_facturas:
        if y < 50:
            c.showPage(); draw_layout_base(); y = h - 100
            
        c.drawString(pos_x[0], y, str(row['factura']))
        c.drawString(pos_x[1], y, str(row['cliente'])[:22]) # Truncar cliente
        c.drawString(pos_x[2], y, str(row['zona'])[:8])
        
        c.drawRightString(pos_x[4]-10, y, f"{row['facturado']:,.0f}")
        c.drawRightString(pos_x[5]-10, y, f"{row['dev']:,.0f}")
        c.drawRightString(pos_x[6]-10, y, f"{row['desc']:,.0f}")
        c.drawRightString(pos_x[7]-10, y, f"{row['merma']:,.0f}")
        
        c.setFont("Helvetica-Bold", 7)
        c.drawRightString(pos_x[7]+50, y, f"{row['neto']:,.0f}")
        c.setFont("Helvetica", 7)
        
        c.setStrokeColor(colors.lightgrey); c.line(30, y-2, w-30, y-2)
        y -= 12

    c.save()
    b.seek(0)
    return send_file(b, mimetype='application/pdf', as_attachment=True, download_name=f"Reporte_Ventas_{inicio}.pdf")

@csrf.exempt
@bp_gestion_mermas.route('/mermas/buscar_notas_historial', methods=['POST'])
def mermas_buscar_notas_historial():
    """
    Busca Notas Crédito generadas (agrupadas por número de nota).
    Retorna fecha, factura, cliente, total $ y link al PDF.
    """
    try:
        j = request.get_json(force=True, silent=True) or {}
        f_ini = j.get('desde')
        f_fin = j.get('hasta')
        operador = j.get('operador') # Nombre del operador o vacío para todos
        empresa_id = session.get('empresa_id') or session.get('nit')

        if not f_ini or not f_fin:
            return jsonify(success=False, message="Fechas requeridas"), 400

        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # Consulta: Agrupamos por número de nota para no repetir filas por cada pollo
        # Sumamos Descuentos + Devoluciones + (Merma * Precio) para tener un aproximado del total
        sql = """
            SELECT 
                nota_descuento, 
                factura, 
                MIN(fecha) as fecha, 
                MAX(cliente) as cliente, 
                MAX(operador_nombre) as operador,
                MAX(evidencia_nota) as pdf_url,
                COUNT(*) as items_count
            FROM mermas_pollosgar 
            WHERE empresa_id = %s 
              AND DATE(fecha) BETWEEN %s AND %s
              AND nota_descuento != '' 
              AND nota_descuento IS NOT NULL
        """
        params = [empresa_id, f_ini, f_fin]

        if operador:
            sql += " AND operador_nombre = %s"
            params.append(operador)

        sql += " GROUP BY nota_descuento ORDER BY fecha DESC, nota_descuento DESC"

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        cur.close()

        # Formatear resultados para el frontend
        resultados = []
        for r in rows:
            # Validar si el PDF existe
            pdf_link = None
            if r['pdf_url']:
                pdf_link = url_for('static', filename=r['pdf_url'])
            
            resultados.append({
                'nota': r['nota_descuento'],
                'factura': r['factura'],
                'fecha': str(r['fecha']),
                'cliente': r['cliente'],
                'operador': r['operador'],
                'items': r['items_count'],
                'url': pdf_link
            })

        return jsonify(success=True, notas=resultados)

    except Exception as e:
        return jsonify(success=False, message=str(e)), 500