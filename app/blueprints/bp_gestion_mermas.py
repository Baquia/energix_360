from flask import Blueprint, request, jsonify, current_app, session, send_file
from datetime import datetime, timedelta
import os, base64, uuid, smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from io import BytesIO

# --- Librerías para PDF y Reportes ---
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader

from app import mysql, csrf

bp_gestion_mermas = Blueprint('bp_gestion_mermas', __name__)

# =========================
# CONFIGURACIÓN EMAIL
# =========================
EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
try:
    EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))
except:
    EMAIL_PORT = 587
EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

# =========================
# UTILIDADES INTERNAS
# =========================

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
        current_app.logger.error(f"Error guardando archivo: {e}")
        return None

def _delete_evidence_files(file_paths):
    """Borra físicamente los archivos del servidor."""
    if not file_paths: return
    base_dir = current_app.static_folder
    for rel_path in file_paths:
        if rel_path:
            full_path = os.path.join(base_dir, rel_path)
            if os.path.exists(full_path):
                try:
                    os.remove(full_path)
                    print(f"🗑️ Archivo eliminado: {full_path}")
                except Exception as e:
                    print(f"Error eliminando archivo {full_path}: {e}")

def _limpiar_mermas_antiguas():
    """Mantenimiento: Borra multimedia de aprobadas > 30 días."""
    try:
        cur = mysql.connection.cursor()
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
                rid = r['id'] if isinstance(r, dict) else r[0]
                e0 = r['evidencia_url'] if isinstance(r, dict) else r[1]
                e1 = r['evidencia_url1'] if isinstance(r, dict) else r[2]
                e2 = r['evidencia_url2'] if isinstance(r, dict) else r[3]
                
                ids_to_clean.append(rid)
                if e0: files_to_delete.append(e0)
                if e1: files_to_delete.append(e1)
                if e2: files_to_delete.append(e2)
            
            _delete_evidence_files(files_to_delete)
            
            format_strings = ','.join(['%s'] * len(ids_to_clean))
            update_sql = f"""
                UPDATE mermas_pollosgar 
                SET evidencia_url = NULL, evidencia_url1 = NULL, evidencia_url2 = NULL 
                WHERE id IN ({format_strings})
            """
            cur.execute(update_sql, tuple(ids_to_clean))
            mysql.connection.commit()
        cur.close()
    except Exception as e:
        print(f"Error en mantenimiento automático: {e}")

def _get_email_talento_humano(empresa_id):
    try:
        cur = mysql.connection.cursor()
        sql = "SELECT email FROM contactos WHERE id_empresa = %s AND area_contacto = 'talentohumano' LIMIT 1"
        cur.execute(sql, (empresa_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            return row['email'] if isinstance(row, dict) else row[0]
        return None
    except:
        return None

def _enviar_email_no_conforme(destinatario, data_merma, argumentos, archivos):
    """Envía email con ADJUNTOS y estilo corporativo."""
    if not destinatario or not EMAIL_USER or not EMAIL_PASS:
        return False

    msg = MIMEMultipart()
    msg['From'] = EMAIL_FROM
    msg['To'] = destinatario
    msg['Subject'] = f"🚨 REPORTE NO CONFORME - {data_merma.get('operador_nombre')} - Fact: {data_merma.get('factura')}"

    html = f"""
    <!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
        <div style="max-width: 600px; margin: 0 auto; background: #fff; border-radius: 8px; overflow: hidden;">
            <div style="background-color: #015249; color: #fff; padding: 20px; text-align: center;">
                <h2 style="margin:0;">Notificación de Merma No Conforme</h2>
                <p style="font-size: 12px; margin-top: 5px;">Sistema de Gestión BQA ONE</p>
            </div>
            <div style="padding: 20px; color: #333;">
                <p><strong>Departamento de Talento Humano,</strong></p>
                <p>Se ha generado una validación <strong>NO CONFORME</strong> tras la auditoría de mermas.</p>
                
                <div style="background: #fef2f2; border-left: 4px solid #b91c1c; padding: 15px; margin: 20px 0;">
                    <h3 style="color: #b91c1c; margin-top: 0;">Argumentos del Controlador</h3>
                    <p style="font-style: italic;">"{argumentos}"</p>
                </div>

                <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                    <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Operador:</td><td style="padding: 8px;">{data_merma.get('operador_nombre')}</td></tr>
                    <tr><td style="padding: 8px; font-weight: bold;">Factura:</td><td style="padding: 8px;">{data_merma.get('factura')}</td></tr>
                    <tr style="background: #f9fafb;"><td style="padding: 8px; font-weight: bold;">Ítem:</td><td style="padding: 8px;">{data_merma.get('item')}</td></tr>
                    <tr><td style="padding: 8px; font-weight: bold;">Merma:</td><td style="padding: 8px; color: #b91c1c; font-weight: bold;">{data_merma.get('merma_kg')} kg ({data_merma.get('merma_pct')}%)</td></tr>
                </table>
                
                <p style="font-size: 12px; color: #666; margin-top: 20px;">
                    * Las evidencias (fotos y video) se encuentran adjuntas a este correo.
                    <br>* Los archivos han sido eliminados del servidor por seguridad tras el envío.
                </p>
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
                except Exception as e:
                    print(f"Error adjuntando {full_path}: {e}")

    try:
        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_FROM, destinatario, msg.as_string())
        return True
    except Exception as e:
        print(f"Error SMTP: {e}")
        return False

# =========================
# RUTAS OPERATIVAS
# =========================

@bp_gestion_mermas.route('/mermas/umbral', methods=['GET'])
def mermas_umbral(): return jsonify(success=True, umbral_pct=_get_umbral_pct())

@bp_gestion_mermas.route('/mermas/clientes', methods=['GET'])
def mermas_clientes():
    empresa_id = session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor()
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
    cur = mysql.connection.cursor()
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
@bp_gestion_mermas.route('/mermas/registrar', methods=['POST'])
def mermas_registrar():
    try: j = request.get_json(force=True, silent=True) or {}
    except: return jsonify(success=False, message="JSON mal formado"), 400

    empresa = (session.get('empresa') or j.get('empresa') or '').strip()
    empresa_id = str(session.get('empresa_id') or session.get('nit') or j.get('empresa_id') or '').strip()
    operador_id = str(session.get('cedula') or session.get('usuario_id') or j.get('operador_id') or '').strip()
    operador_nombre = (session.get('usuario_nombre') or session.get('nombre') or j.get('operador_nombre') or '').strip()
    cliente = (j.get('cliente') or '').strip()
    vehiculo = (j.get('vehiculo') or '').strip()
    factura = (j.get('factura') or '').strip()
    
    try: total_kg_factura = float(j.get('total_kg', 0))
    except: total_kg_factura = 0.0
        
    items = j.get('items')
    is_retry = j.get('is_retry', False)
    retry_id = j.get('retry_id')

    if not (cliente and vehiculo and factura and items):
        return jsonify(success=False, message="Faltan datos"), 400

    lock_name = f"mermas_op_{operador_id}"
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT GET_LOCK(%s, 10) AS candado", (lock_name,))
        res_lock = cur.fetchone()
        if not res_lock:
            cur.close()
            return jsonify(success=False, message="Servidor ocupado"), 409
        
        # Validar lock
        locked = res_lock.get('candado') if isinstance(res_lock, dict) else res_lock[0]
        if locked != 1:
            cur.close()
            return jsonify(success=False, message="Servidor ocupado"), 409

        if not is_retry:
            cur.execute("SELECT id FROM mermas_pollosgar WHERE factura=%s AND estatus='pendiente' LIMIT 1", (factura,))
            if cur.fetchone():
                 cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                 return jsonify(success=False, code='PENDING_EXISTS', message="Factura ya en revisión."), 409

        empresa_slug = (empresa or 'generico').lower().replace(' ', '_')
        umbral = _get_umbral_pct(empresa_id)
        
        global_status = 'aprobada' 
        
        for it in items:
            try:
                kg_item_val = float(it.get('kg_facturados', 0)) 
                kge = float(it.get('kg_entregados', 0))
                nombre_item = (it.get('item') or 'General').strip()
            except:
                cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
                return jsonify(success=False, message="Error numérico"), 400

            merma_kg = kg_item_val - kge 
            merma_pct = (merma_kg / kg_item_val * 100.0) if kg_item_val > 0 else 0.0

            vid_path = _save_base64_image(it.get('evidencia_url'), empresa_slug)
            foto1_path = _save_base64_image(it.get('evidencia_url2'), empresa_slug)
            foto2_path = _save_base64_image(it.get('evidencia_url3'), empresa_slug)

            item_status = 'aprobada'
            if merma_pct > umbral:
                item_status = 'segunda_revision' if is_retry else 'pendiente'
                global_status = item_status

            if is_retry and retry_id:
                cur.execute("""
                    UPDATE mermas_pollosgar
                    SET kg_item=%s, kg_entregados=%s, merma_kg=%s, merma_pct=%s,
                        evidencia_url=%s, evidencia_url1=%s, evidencia_url2=%s,
                        estatus=%s, decision='rectificada'
                    WHERE id=%s
                """, (kg_item_val, kge, merma_kg, merma_pct, foto1_path, vid_path, foto2_path, item_status, retry_id))
            else:
                cur.execute("""
                    INSERT INTO mermas_pollosgar
                        (fecha, empresa, empresa_id, operador_id, operador_nombre,
                         cliente, vehiculo, factura, item,
                         kg_factura, kg_item, kg_entregados, merma_kg, merma_pct, 
                         evidencia_url, evidencia_url1, evidencia_url2,
                         estatus, decision, fecha_decision, nota_descuento)
                    VALUES
                        (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '')
                """, (
                    empresa, empresa_id, operador_id, operador_nombre,
                    cliente, vehiculo, factura, nombre_item,
                    total_kg_factura, kg_item_val, kge, merma_kg, merma_pct,
                    foto1_path, vid_path, foto2_path,
                    item_status, 
                    'por_aprobar' if 'pendiente' in item_status else 'aprobada',
                    None if 'pendiente' in item_status else datetime.now()
                ))

        mysql.connection.commit()
        cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        cur.close()

        return jsonify(success=True, status=global_status, factura=factura, message="Registrado correctamente")

    except Exception as e:
        try: cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        except: pass
        cur.close()
        return jsonify(success=False, message=f"Error interno: {str(e)}"), 500

@csrf.exempt
@bp_gestion_mermas.route('/mermas/accion', methods=['POST'])
def mermas_accion():
    j = request.get_json(force=True, silent=True) or {}
    reg_id = j.get('id')
    accion = (j.get('accion') or '').lower()

    if not reg_id: return jsonify(success=False), 400
    
    cur = mysql.connection.cursor()
    cur.execute("SELECT estatus FROM mermas_pollosgar WHERE id=%s", (reg_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return jsonify(success=False, message="Registro no encontrado"), 404

    # Limpiar y normalizar estatus
    raw_status = row['estatus'] if isinstance(row, dict) else row[0]
    current_status = str(raw_status).strip().lower()
    
    estatus_final = None 
    decision = 'pendiente'
    
    if accion == 'aprobar':
        estatus_final = 'aprobada'
        decision = 'aprobada'
        
    elif accion == 'objetar':
        if current_status == 'pendiente':
            estatus_final = 'en_revision'
            decision = 'solicitud_rectificacion'
        elif current_status == 'segunda_revision':
            estatus_final = 'objetada'
            decision = 'objetada_auditoria'
        else:
            estatus_final = 'objetada'
            decision = 'objetada_fallback'
            
    if estatus_final is None:
        cur.close()
        return jsonify(success=False, message="Acción no permitida"), 400

    cur.execute("UPDATE mermas_pollosgar SET estatus=%s, decision=%s, fecha_decision=NOW() WHERE id=%s", (estatus_final, decision, reg_id))
    mysql.connection.commit()
    cur.close()

    return jsonify(success=True, id=reg_id, estado=estatus_final)

@bp_gestion_mermas.route('/mermas/validar_investigacion', methods=['POST'])
@csrf.exempt
def mermas_validar_investigacion():
    j = request.get_json(force=True, silent=True) or {}
    reg_id = j.get('id')
    decision = j.get('decision')
    argumentos = j.get('argumentos', '')

    if not reg_id or not decision: return jsonify(success=False, message="Datos incompletos"), 400

    cur = mysql.connection.cursor()
    cur.execute("SELECT * FROM mermas_pollosgar WHERE id=%s", (reg_id,))
    row = cur.fetchone()
    if not row:
        cur.close()
        return jsonify(success=False, message="Registro no encontrado"), 404

    data_merma = row if isinstance(row, dict) else {
        'id': row[0], 'empresa_id': row[3], 'operador_nombre': row[5], 'operador_id': row[4],
        'cliente': row[6], 'factura': row[9], 'item': row[10], 
        'merma_kg': row[14], 'merma_pct': row[15],
        'evidencia_url': row[8], 'evidencia_url1': row[16], 'evidencia_url2': row[17]
    }
    
    files = [data_merma['evidencia_url'], data_merma['evidencia_url1'], data_merma['evidencia_url2']]

    if decision == 'conforme':
        _delete_evidence_files(files)
        cur.execute("""
            UPDATE mermas_pollosgar 
            SET estatus='validada', decision='validada_conforme', fecha_decision=NOW(), 
                evidencia_url=NULL, evidencia_url1=NULL, evidencia_url2=NULL, comentario_control=%s
            WHERE id=%s
        """, (argumentos, reg_id))

    elif decision == 'no_conforme':
        email_to = _get_email_talento_humano(data_merma.get('empresa_id'))
        if email_to:
            _enviar_email_no_conforme(email_to, data_merma, argumentos, files)
        
        _delete_evidence_files(files)
        
        # Mantiene 'objetada' pero borra URLs, así que el filtro 'IS NOT NULL' la saca de la lista
        cur.execute("""
            UPDATE mermas_pollosgar 
            SET estatus='objetada', decision='no_conforme_rrhh', fecha_decision=NOW(),
                evidencia_url=NULL, evidencia_url1=NULL, evidencia_url2=NULL, comentario_control=%s
            WHERE id=%s
        """, (argumentos, reg_id))

    mysql.connection.commit()
    cur.close()
    return jsonify(success=True)

@bp_gestion_mermas.route('/mermas/check_status_live', methods=['GET'])
def check_status_live():
    factura = request.args.get('factura')
    if not factura: return jsonify(completed=False)
    cur = mysql.connection.cursor()
    cur.execute("SELECT count(*) as pendientes FROM mermas_pollosgar WHERE factura=%s AND estatus IN ('pendiente', 'segunda_revision')", (factura,))
    res = cur.fetchone()
    pendientes = res['pendientes'] if isinstance(res, dict) else res[0]
    if pendientes > 0:
        cur.close()
        return jsonify(completed=False)
    
    cur.execute("SELECT id, estatus FROM mermas_pollosgar WHERE factura=%s ORDER BY id DESC LIMIT 1", (factura,))
    row = cur.fetchone()
    cur.close()
    
    final_status = 'aprobada'
    row_id = None
    if row:
        if isinstance(row, dict):
             final_status = row['estatus']
             row_id = row['id']
        else:
             row_id = row[0]
             final_status = row[1]
    return jsonify(completed=True, status=final_status, id=row_id)

@bp_gestion_mermas.route('/mermas/pending', methods=['GET'])
def mermas_pending():
    empresa_id = request.args.get('empresa_id') or session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor()
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
    empresa_id = session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor()
    _limpiar_mermas_antiguas()
    
    sql = """
        SELECT id, fecha, cliente, vehiculo, factura, item, operador_nombre,
               kg_factura AS total_factura_kg, kg_item, kg_entregados, merma_kg, merma_pct,
               evidencia_url, evidencia_url1, evidencia_url2, estatus, nota_descuento
        FROM mermas_pollosgar 
        WHERE estatus IN ('objetada', 'aprobada_no_conforme') 
          AND evidencia_url IS NOT NULL 
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

@csrf.exempt
@bp_gestion_mermas.route('/mermas/consulta', methods=['POST'])
def mermas_consulta():
    j = request.get_json(force=True, silent=True) or {}
    tipo = (j.get('tipo') or '').lower()
    desde, hasta = _parse_date_range(j.get('desde'), j.get('hasta'))
    empresa_id = session.get('empresa_id') or session.get('nit')
    where, params = [], []
    if empresa_id: where.append("empresa_id=%s"); params.append(empresa_id)
    if tipo=='zona': where.append("zona=%s"); params.append(j.get('zona'))
    elif tipo=='vendedor': where.append("operador_nombre=%s"); params.append(j.get('vendedor'))
    elif tipo=='cliente': where.append("cliente=%s"); params.append(j.get('cliente'))
    elif tipo=='vehiculo': where.append("vehiculo=%s"); params.append(j.get('vehiculo'))
    if desde: where.append("DATE(fecha) >= %s"); params.append(desde)
    if hasta: where.append("DATE(fecha) <= %s"); params.append(hasta)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    cur = mysql.connection.cursor()
    try:
        cur.execute(f"SELECT COALESCE(SUM(kg_item),0) as kg, COALESCE(SUM(merma_kg),0) as merma, COUNT(*) as regs FROM mermas_pollosgar {where_sql}", tuple(params))
        res = cur.fetchone()
        cur.execute(f"SELECT cliente, COALESCE(SUM(kg_item),0) as kg, COALESCE(SUM(merma_kg),0) as merma FROM mermas_pollosgar {where_sql} GROUP BY cliente ORDER BY kg DESC", tuple(params))
        det = cur.fetchall()
        cur.execute(f"SELECT fecha, cliente, vehiculo, factura, kg_item, kg_entregados, merma_kg, merma_pct, operador_nombre FROM mermas_pollosgar {where_sql} ORDER BY fecha ASC", tuple(params))
        ops = cur.fetchall()
    finally: cur.close()
    kg_tot = float(res['kg']) if isinstance(res, dict) else float(res[0])
    merma_tot = float(res['merma']) if isinstance(res, dict) else float(res[1])
    regs = res['regs'] if isinstance(res, dict) else res[2]
    detalle = []
    for r in det:
        c_cli = r['cliente'] if isinstance(r, dict) else r[0]
        c_kg = float(r['kg']) if isinstance(r, dict) else float(r[1])
        c_merma = float(r['merma']) if isinstance(r, dict) else float(r[2])
        detalle.append({'cliente': c_cli, 'kg_cliente': c_kg, 'merma_cliente': c_merma, 'pct_cliente': (c_merma/c_kg*100) if c_kg>0 else 0})
    operaciones = []
    for r in ops:
        if isinstance(r, dict): op = r
        else: op = {'fecha': r[0], 'cliente': r[1], 'factura': r[3], 'kg_item': r[4], 'kg_entregados': r[5], 'merma_kg': r[6], 'merma_pct': r[7]}
        operaciones.append({'fecha': str(op['fecha']), 'cliente': op['cliente'], 'factura': op['factura'], 'kg_factura': float(op['kg_item']), 'kg_entregados': float(op['kg_entregados']), 'merma_kg': float(op['merma_kg']), 'merma_pct': float(op['merma_pct'])})
    chart = {'type': 'bar', 'labels': [], 'values': [], 'series_label': 'Merma (%)'}
    if tipo in ('zona', 'vendedor'):
        chart['labels'] = [d['cliente'] for d in detalle]
        chart['values'] = [d['pct_cliente'] for d in detalle]
    elif tipo == 'cliente':
        fdata = {}
        for o in operaciones:
            f = o['factura']
            if f not in fdata: fdata[f] = {'k':0, 'm':0}
            fdata[f]['k'] += float(o['kg_factura'])
            fdata[f]['m'] += float(o['merma_kg'])
        chart['labels'] = list(fdata.keys())
        chart['values'] = [(v['m']/v['k']*100 if v['k']>0 else 0) for v in fdata.values()]
    elif tipo == 'vehiculo':
        chart['type'] = 'points'
        chart['labels'] = [o['fecha'] for o in operaciones]
        chart['values'] = [o['merma_pct'] for o in operaciones]
    return jsonify({'success': True, 'resumen': {'kg_totales': kg_tot, 'merma_total': merma_tot, 'merma_pct_total': (merma_tot/kg_tot*100) if kg_tot>0 else 0, 'registros': regs}, 'detalle_clientes': detalle, 'operaciones': operaciones, 'chart': chart, 'filtros': {'tipo': tipo, 'desde': desde, 'hasta': hasta}})

@csrf.exempt
@bp_gestion_mermas.route('/mermas/consulta/pdf', methods=['POST'])
def mermas_consulta_pdf():
    j = request.get_json(force=True, silent=True) or {}
    res = j.get('resumen') or {}
    chart = j.get('chart_png')
    buf = BytesIO()
    p = canvas.Canvas(buf, pagesize=landscape(A4))
    W, H = landscape(A4)
    p.setFont("Helvetica-Bold", 16)
    p.drawString(2*cm, H-2*cm, "Informe de Mermas - Energix360")
    p.setFont("Helvetica", 10)
    p.drawString(2*cm, H-2.7*cm, f"Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    p.setFont("Helvetica-Bold", 12)
    p.drawString(2*cm, H-4*cm, f"Total KG: {res.get('kg_totales')}  |  Merma: {res.get('merma_total')} kg  |  %: {float(res.get('merma_pct_total',0)):.2f}%")
    y = H-6*cm
    if chart and chart.startswith('data:image'):
        try:
            img = ImageReader(BytesIO(base64.b64decode(chart.split(',')[1])))
            p.drawImage(img, 2*cm, y-8*cm, width=W-4*cm, height=8*cm, preserveAspectRatio=True)
            y -= 9*cm
        except: pass
    p.setFont("Helvetica-Bold", 10)
    p.drawString(2*cm, y, "Fecha")
    p.drawString(5*cm, y, "Cliente")
    p.drawString(12*cm, y, "Factura")
    p.drawString(16*cm, y, "Merma (kg)")
    p.drawString(20*cm, y, "%")
    y -= 0.5*cm
    p.setFont("Helvetica", 10)
    for op in (j.get('operaciones') or [])[:40]: 
        if y < 2*cm: p.showPage(); y = H-2*cm
        p.drawString(2*cm, y, str(op.get('fecha')))
        p.drawString(5*cm, y, str(op.get('cliente'))[:35])
        p.drawString(12*cm, y, str(op.get('factura')))
        p.drawString(16*cm, y, str(op.get('merma_kg')))
        p.drawString(20*cm, y, str(op.get('merma_pct'))+"%")
        y -= 0.5*cm
    p.showPage(); p.save(); buf.seek(0)
    return send_file(buf, mimetype='application/pdf', as_attachment=True, download_name="Informe_Energix.pdf")

@bp_gestion_mermas.route('/mermas/opciones', methods=['GET'])
def mermas_opciones():
    tipo = (request.args.get('tipo') or '').lower()
    empresa_id = session.get('empresa_id') or session.get('nit')
    col = {'zona': 'zona', 'vendedor': 'operador_nombre', 'cliente': 'cliente', 'vehiculo': 'vehiculo'}.get(tipo)
    if not col: return jsonify(success=False), 400
    cur = mysql.connection.cursor()
    sql = f"SELECT DISTINCT {col} AS val FROM mermas_pollosgar WHERE {col} IS NOT NULL AND {col} <> ''"
    params = []
    if empresa_id: sql += " AND empresa_id=%s"; params.append(empresa_id)
    sql += f" ORDER BY {col} ASC"
    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close()
    items = []
    for r in rows:
        val = r['val'] if isinstance(r, dict) else r[0]
        if val: items.append(val)
    return jsonify(success=True, items=items)