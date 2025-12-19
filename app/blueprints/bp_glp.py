# -*- coding: utf-8 -*-
import os, base64, smtplib, traceback, random, string, uuid, sys
from datetime import datetime, date
from flask import Blueprint, jsonify, request, session, current_app as app
from email.mime.text import MIMEText
from app import mysql, csrf
from app.utils import login_required_custom
import re as _re

bp_glp = Blueprint('bp_glp', __name__, url_prefix='/glp')

# ==============================================================================
# 1. UTILIDADES Y CONSTANTES
# ==============================================================================

DENSIDAD_ESTIMADA = 2.0
EMAIL_CONTROL = "bqa-one@baquia-esm.com" 

def _get_connection():
    return mysql.connection

def _normalize_sede(s):
    if not s: return ""
    s = s.strip()
    if "|" in s: s = s.split("|", 1)[0]
    return _re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()

def _extraer_numero(valor):
    if isinstance(valor, int): return valor
    s = str(valor)
    digits = "".join(filter(str.isdigit, s))
    return int(digits) if digits else 0

def _guardar_testigo(base64_data, carpeta, nombre_archivo):
    if not base64_data or len(base64_data) < 100: return ""
    try:
        if "," in base64_data: base64_data = base64_data.split(",", 1)[1]
        static_dir = os.path.join(app.root_path, "static", "testigos", carpeta)
        if not os.path.exists(static_dir): os.makedirs(static_dir)
        filename = f"{nombre_archivo}.jpg"
        file_path = os.path.join(static_dir, filename)
        with open(file_path, 'wb') as f: f.write(base64.b64decode(base64_data))
        return filename 
    except Exception as e:
        print(f"Error guardando imagen: {e}", file=sys.stderr)
        return ""

def _generar_op_id():
    return str(uuid.uuid4())

def _generar_codigo_pedido(cliente):
    prefix = "".join([c for c in cliente if c.isalnum()]).upper()[:3]
    fecha = datetime.now().strftime("%y%m%d")
    rand = ''.join(random.choices(string.digits, k=4))
    return f"{prefix}{fecha}{rand}"

def _obtener_emails_proveedor_db(empresa, ubicacion):
    """Retorna lista de emails. Usa TRIM para evitar errores de espacios."""
    emails = []
    try:
        cur = _get_connection().cursor()
        query = """
            SELECT p.email1, p.email2 
            FROM tanques_sedes ts
            JOIN proveedores p ON TRIM(ts.proveedor) = TRIM(p.proveedor)
            WHERE ts.empresa = %s AND TRIM(ts.ubicacion) = TRIM(%s)
            LIMIT 1
        """
        cur.execute(query, (empresa, ubicacion))
        row = cur.fetchone()
        cur.close()
        if row:
            if row.get('email1') and '@' in row['email1']: emails.append(row['email1'])
            if row.get('email2') and '@' in row['email2']: emails.append(row['email2'])
    except Exception as e:
        print(f"Error buscando emails proveedor: {e}", file=sys.stderr)
    return emails

def _enviar_email_profesional(destinatarios, asunto, html_body):
    """Envía email y retorna estado para debug."""
    try:
        HOST, PORT = "smtp.gmail.com", 587
        USER = "bqa-one@baquia-esm.com" 
        PASS = "gskwbaergmvulvui" 
        
        lista_limpia = []
        if destinatarios:
            for d in destinatarios:
                if d and "@" in d:
                    for s in d.replace(";", ",").split(","):
                        if s.strip(): lista_limpia.append(s.strip())
        
        send_to_list = list(set(lista_limpia + [EMAIL_CONTROL]))
        
        msg = MIMEText(html_body, "html", "utf-8")
        msg["Subject"] = asunto
        msg["From"] = USER
        msg["To"] = ", ".join(lista_limpia) if lista_limpia else EMAIL_CONTROL
        
        server = smtplib.SMTP(HOST, PORT)
        server.starttls()
        server.login(USER, PASS)
        server.sendmail(USER, send_to_list, msg.as_string())
        server.quit()
        print(f"EMAIL ENVIADO EXITOSAMENTE: {asunto} a {send_to_list}", file=sys.stderr)
        return True, "Enviado"
    except Exception as e:
        err = str(e)
        print(f"CRITICAL EMAIL ERROR: {err}", file=sys.stderr)
        return False, f"Error: {err}"

# --- FORMATO SOLICITUD (CONSUMO / INICIO) ---
def _generar_html_solicitud(empresa, ubicacion, lote, filas_tabla, codigo):
    return f"""
    <div style="font-family:'Segoe UI',sans-serif;color:#333;max-width:600px;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden;">
        <div style="background-color:#015249;color:white;padding:20px;text-align:center;"><h2 style="margin:0;">SOLICITUD DE TANQUEO URGENTE</h2></div>
        <div style="padding:25px;">
            <p>Se requiere suministro de GLP para:</p>
            <ul style="list-style:none;padding:0;">
                <li>🏢 <strong>Cliente:</strong> {empresa}</li>
                <li>📍 <strong>Ubicación:</strong> {ubicacion}</li>
                <li>📦 <strong>Lote:</strong> {lote}</li>
                <li>📅 <strong>Fecha:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M')}</li>
            </ul>
            <table style="width:100%;border-collapse:collapse;margin:20px 0;font-size:14px;">
                <thead><tr style="background:#f4f4f4;text-align:left;"><th style="padding:10px;">Tanque</th><th style="padding:10px;">Nivel Actual</th><th style="padding:10px;color:#015249;">Meta Llenado</th></tr></thead>
                <tbody>{filas_tabla}</tbody>
            </table>
            <div style="background:#fff8e1;color:#856404;padding:15px;border-radius:6px;text-align:center;border:1px solid #ffeeba;margin:20px 0;">
                <p style="margin:0;font-size:11px;font-weight:bold;text-transform:uppercase;">Código de Autorización</p>
                <h1 style="margin:5px 0;color:#333;">{codigo}</h1>
                <p style="margin:0;font-size:12px;">⚠️ <strong>Obligatorio en factura para pago.</strong></p>
            </div>
            <p style="color:#d32f2f;font-weight:bold;text-align:center;">⏰ SUMINISTRAR EN LAS PRÓXIMAS 24 HORAS.</p>
            <hr style="border:0;border-top:1px solid #eee;margin:30px 0;">
            <p style="font-size:11px;color:#999;text-align:center;">Email generado automáticamente por BAQ-ONE</p>
        </div>
    </div>
    """

# --- FORMATO ALERTA (TANQUEO) ---
def _generar_html_alerta_desviacion(empresa, ubicacion, facturado, recibido, diferencia):
    color = "#d32f2f"
    return f"""
    <div style="font-family:'Segoe UI',sans-serif;color:#333;max-width:600px;border:2px solid {color};border-radius:8px;overflow:hidden;">
        <div style="background-color:{color};color:white;padding:20px;text-align:center;"><h2 style="margin:0;">⚠️ ALERTA DE SEGURIDAD: DESVIACIÓN GLP</h2></div>
        <div style="padding:25px;">
            <p>Se ha detectado una inconsistencia crítica en la carga:</p>
            <div style="background:#f9f9f9;padding:15px;border-radius:5px;margin:20px 0;">
                <p style="margin:5px 0;">🏢 <strong>Cliente:</strong> {empresa}</p>
                <p style="margin:5px 0;">📍 <strong>Ubicación:</strong> {ubicacion}</p>
                <p style="margin:5px 0;">📅 <strong>Fecha:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
            </div>
            <table style="width:100%;border-collapse:collapse;">
                <tr><td style="padding:8px;border-bottom:1px solid #eee;">⚖️ Facturado:</td><td style="padding:8px;border-bottom:1px solid #eee;font-weight:bold;">{facturado} Kg</td></tr>
                <tr><td style="padding:8px;border-bottom:1px solid #eee;">📉 Recibido (Calc):</td><td style="padding:8px;border-bottom:1px solid #eee;font-weight:bold;">{recibido:.2f} Kg</td></tr>
                <tr style="background:#ffebee;"><td style="padding:8px;color:{color};font-weight:bold;">❌ Diferencia:</td><td style="padding:8px;color:{color};font-weight:bold;font-size:18px;">{diferencia:.2f}%</td></tr>
            </table>
            <p style="font-size:13px;color:#666;margin-top:20px;">Verificar calibración o retención en cisterna.</p>
            <hr style="border:0;border-top:1px solid #eee;margin:30px 0;">
            <p style="font-size:11px;color:#999;text-align:center;">Email generado automáticamente por BAQ-ONE</p>
        </div>
    </div>
    """

# ==============================================================================
# ENDPOINTS
# ==============================================================================

@csrf.exempt
@bp_glp.route('/obtener_tanques', methods=['POST'])
@login_required_custom
def obtener_tanques():
    try:
        data = request.get_json(force=True)
        empresa = session.get('empresa'); ubicacion = _normalize_sede(data.get('sede'))
        cur = _get_connection().cursor()
        cur.execute("SELECT nombre_tanque, capacidad_gls FROM tanques_sedes WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s)", (empresa, ubicacion))
        rows = cur.fetchall(); cur.close()
        if not rows: return jsonify({"success": False, "message": "No se encontraron tanques."})
        tanques = [{"numero": r['nombre_tanque'], "etiqueta": r['nombre_tanque'], "capacidad": float(r['capacidad_gls'])} for r in rows]
        return jsonify({"success": True, "tanques": tanques})
    except Exception as e: return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_glp.route('/contexto_operacion', methods=['POST'])
@login_required_custom
def contexto_operacion():
    try:
        data = request.get_json(force=True)
        empresa = session.get('empresa'); ubicacion = _normalize_sede(data.get('sede')); op = data.get('operacion')
        cur = _get_connection().cursor()
        cur.execute("SELECT nombre_tanque, capacidad_gls, proveedor, zona, email FROM tanques_sedes WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s)", (empresa, ubicacion))
        tanques_db = cur.fetchall()
        if not tanques_db: return jsonify({"ok": False, "error": "Sede no configurada"}), 400
        lista = [{"tk": _extraer_numero(r['nombre_tanque']), "nombre_tanque": r['nombre_tanque'], "capacidad_gls": float(r['capacidad_gls']), "nivel_actual": 0} for r in tanques_db]
        
        cur.execute("SELECT lote, fecha, pollitos, tipo_granja, dias_operacion FROM cardex_glp WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO' ORDER BY fecha DESC, id DESC LIMIT 1", (empresa, ubicacion))
        act = cur.fetchone()
        resp = {"ok": True, "empresa_id": session.get('empresa_id'), "tanques": lista, "lote_activo": None, "pollitos": 0, "dias_operacion": 0, "tipo_granja": None, "error": None}
        if op == 'inicio_calefaccion':
            if act: return jsonify({"ok": False, "error": "no puede realizar la operaciòn, hay un lote activo en esa ubicación."})
        else:
            if not act: return jsonify({"ok": False, "error": "No hay lote ACTIVO."})
            resp['lote_activo'] = act['lote']; resp['pollitos'] = act['pollitos']; resp['tipo_granja'] = act.get('tipo_granja')
            f_ini = act['fecha']
            if isinstance(f_ini, str): f_ini = datetime.strptime(f_ini, '%Y-%m-%d').date()
            resp['dias_operacion'] = max(1, (datetime.now().date() - f_ini).days + 1)
        cur.close()
        return jsonify(resp)
    except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500

@csrf.exempt
@bp_glp.route('/registrar_inicio', methods=['POST'])
@login_required_custom
def registrar_inicio():
    try: 
        data = request.get_json(force=True)
        empresa = session.get('empresa'); ubicacion = _normalize_sede(data.get('sede')); usuario = session.get('nombre'); op_id = _generar_op_id()
        if not all(k in data for k in ['pollitos', 'tanques', 'tipo_granja']): return jsonify({"success": False, "message": "Datos incompletos"}), 400
        cur = _get_connection().cursor()
        cur.execute("SELECT id FROM cardex_glp WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO' LIMIT 1", (empresa, ubicacion))
        if cur.fetchone(): cur.close(); return jsonify({"success": False, "message": "no puede realizar la operaciòn, hay un lote activo en esa ubicación."}), 400

        lote = f"{datetime.now().strftime('%Y%m%d')}_{ubicacion.replace(' ', '')}"
        tanques_data = data['tanques']
        saldo_gal = 0; saldo_kg = 0; tanques_criticos = []; sql_cols = {}
        for i in range(1, 12):
            tk_key = f"tk-{i}"
            sql_cols[f"`nivel {tk_key}`"]=0; sql_cols[f"`capacidad {tk_key}`"]=0; sql_cols[f"`testigo nivel {tk_key}`"]=""; sql_cols[f"`testigo_baucher_{tk_key.replace('-','_')}`"]=""
            tk_input = next((t for t in tanques_data if _extraer_numero(t.get('numero', 0)) == i), None)
            if tk_input:
                nivel = float(tk_input.get('nivel', 0)); cap = float(tk_input.get('capacidad', 0))
                foto = _guardar_testigo(tk_input.get('testigo'), lote, f"RF-IC-TK{i}-{lote}-{op_id[:8]}")
                sg = cap * (nivel / 100.0); sk = sg * DENSIDAD_ESTIMADA
                saldo_gal += sg; saldo_kg += sk
                sql_cols[f"`nivel {tk_key}`"]=nivel; sql_cols[f"`capacidad {tk_key}`"]=cap; sql_cols[f"`testigo nivel {tk_key}`"]=foto
                if nivel <= 30: tanques_criticos.append({"tk": f"TK-{i}", "nivel": nivel, "meta": 80})

        cols = ["fecha", "empresa", "id_empresa", "ubicacion", "lote", "estatus_lote", "operacion", "clase", "tipo", "registro", "op_id", "dias_operacion", "pollitos", "tipo_granja", "densidad_estimada", "saldo_estimado_galones", "saldo_estimado_kg", "densidad_suministrada", "kg_pollito", "pesos_pollito", "masa_kg_facturada", "neto_gastado"]
        vals = [datetime.now(), empresa, session.get('empresa_id'), ubicacion, lote, 'ACTIVO', 'inicio_calefaccion', 'saldo inicial', 'manual', usuario, op_id, 1, data['pollitos'], data['tipo_granja'], DENSIDAD_ESTIMADA, saldo_gal, saldo_kg, 0, 0, 0, 0, 0]
        
        for c, v in sql_cols.items(): cols.append(c); vals.append(v)
        cur.execute(f"INSERT INTO cardex_glp ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(vals))})", tuple(vals))
        
        msg_pedido = ""
        if tanques_criticos:
            cod = _generar_codigo_pedido(empresa)
            cur.execute("INSERT INTO pedidos_gas_glp (cliente, codigo_pedido, estatus, fecha_registro, lote, ubicacion, proveedor, numero_factura, validador) VALUES (%s, %s, 'GENERADO', NOW(), %s, %s, %s, 'PENDIENTE', 'SISTEMA')", (empresa, cod, lote, ubicacion, data.get('proveedor', 'Desconocido')))
            emails = _obtener_emails_proveedor_db(empresa, ubicacion)
            filas = "".join([f"<tr><td style='padding:10px;border-bottom:1px solid #eee;'>{t['tk']}</td><td style='padding:10px;border-bottom:1px solid #eee;'>{t['nivel']}%</td><td style='padding:10px;border-bottom:1px solid #eee;font-weight:bold;color:#015249;'>{t['meta']}%</td></tr>" for t in tanques_criticos])
            # PASAMOS EMPRESA AL HTML
            html = _generar_html_solicitud(empresa, ubicacion, lote, filas, cod)
            _enviar_email_profesional(emails, f"SOLICITUD TANQUEO GRANJA --{ubicacion}--", html)
            msg_pedido = f"Pedido generado {cod}"

        _get_connection().commit(); cur.close()
        return jsonify({"success": True, "message": f"Inicio registrado. {msg_pedido}", "lote": lote})
    except Exception as e: traceback.print_exc(); return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_glp.route('/registrar_tanqueo', methods=['POST'])
@login_required_custom
def registrar_tanqueo():
    try:
        data = request.get_json(force=True)
        empresa = session.get('empresa'); ubicacion = _normalize_sede(data.get('sede')); usuario = session.get('nombre'); op_id = _generar_op_id()
        cur = _get_connection().cursor()
        
        cur.execute("SELECT * FROM cardex_glp WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO' ORDER BY fecha DESC, id DESC LIMIT 1", (empresa, ubicacion))
        prev = cur.fetchone()
        if not prev: return jsonify({"success": False, "message": "No hay lote activo"}), 400

        lote = prev['lote']; pollitos = prev.get('pollitos', 0) or 1
        f_ini_obj = prev['fecha'] 
        if isinstance(f_ini_obj, str): f_ini_obj = datetime.strptime(f_ini_obj, '%Y-%m-%d').date()
        dias_op = max(1, (datetime.now().date() - f_ini_obj).days + 1)

        tanques_input = data.get('tanques', [])
        masa_facturada_total = 0.0
        densidad_usada = 0.0
        
        for t in tanques_input:
            masa_facturada_total += float(t.get('kg_suministrados', 0))
            if densidad_usada == 0:
                densidad_usada = float(t.get('densidad_suministrada', 0))

        kg_gast = 0; masa_esp_total = 0; sql_cols = {}
        for i in range(1, 12):
            tk = f"tk-{i}"; tk_u = f"tk_{i}"
            sql_cols[f"`nivel {tk}`"]=0; sql_cols[f"`nivelfinal {tk}`"]=0; sql_cols[f"`capacidad {tk}`"]=0; sql_cols[f"`testigo nivel {tk}`"]=""; sql_cols[f"`testigo nivelfinal {tk}`"]=""; sql_cols[f"`testigo_baucher_{tk_u}`"]=""

        for t in tanques_input:
            num = _extraer_numero(t.get('numero')); tk_key = f"tk-{num}"; tk_u = f"tk_{num}"
            ni = float(t.get('nivel_inicial', 0)); nf = float(t.get('nivel_final', 0)); cap = float(t.get('capacidad', 0))
            d_tk = float(t.get('densidad_suministrada', 0))
            if d_tk == 0: d_tk = densidad_usada
            
            col_p = f"nivelfinal {tk_key}" if prev.get('operacion') == 'tanqueo' else f"nivel {tk_key}"
            nant = prev.get(col_p.replace('`','')) or prev.get(f"nivel {tk_key}", 0)
            kg_gast += max(0, (float(nant) - ni)/100.0 * cap * 2.0)
            masa_esp_total += (max(0, nf - ni)/100.0 * cap * d_tk)
            
            f1 = _guardar_testigo(t.get('foto_nivel_inicial'), lote, f"RF-NI-TK{num}-{op_id[:8]}")
            f2 = _guardar_testigo(t.get('foto_nivel_final'), lote, f"RF-NF-TK{num}-{op_id[:8]}")
            f3 = _guardar_testigo(t.get('foto_baucher'), lote, f"RF-BCH-TK{num}-{op_id[:8]}")
            sql_cols[f"`nivel {tk_key}`"]=ni; sql_cols[f"`nivelfinal {tk_key}`"]=nf; sql_cols[f"`capacidad {tk_key}`"]=cap
            sql_cols[f"`testigo nivel {tk_key}`"]=f1; sql_cols[f"`testigo nivelfinal {tk_key}`"]=f2; sql_cols[f"`testigo_baucher_{tk_u}`"]=f3

        diff = ((masa_facturada_total - masa_esp_total)/masa_facturada_total)*100 if masa_facturada_total > 0 else 0
        
        cols = ["fecha", "empresa", "id_empresa", "ubicacion", "lote", "estatus_lote", "operacion", "clase", "tipo", "registro", "op_id", "dias_operacion", "pollitos", "densidad_suministrada", "masa_kg_facturada", "masa_esperada_kg", "porcentaje_diferencia", "neto_gastado", "kg_pollito", "pesos_pollito"]
        vals = [datetime.now(), empresa, session.get('empresa_id'), ubicacion, lote, 'ACTIVO', 'tanqueo', 'ingreso', 'manual', usuario, op_id, dias_op, pollitos, densidad_usada, masa_facturada_total, masa_esp_total, diff, kg_gast, kg_gast/pollitos if pollitos else 0, 0]
        
        for c, v in sql_cols.items(): cols.append(c); vals.append(v)
        cur.execute(f"INSERT INTO cardex_glp ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(vals))})", tuple(vals))
        _get_connection().commit(); cur.close()

        status_email = "No enviado (Diff < 7%)"
        if abs(diff) >= 7.0:
            print(">>> DETECTADA DESVIACION MAYOR A 7%. INTENTANDO ENVIAR EMAIL...", file=sys.stderr)
            emails = _obtener_emails_proveedor_db(empresa, ubicacion)
            # PASAMOS EMPRESA AL HTML
            html = _generar_html_alerta_desviacion(empresa, ubicacion, masa_facturada_total, masa_esp_total, diff)
            ok, msg_mail = _enviar_email_profesional(emails, f"ALERTA DESVIACION {ubicacion}", html)
            status_email = msg_mail

        return jsonify({
            "success": True, 
            "message": f"Tanqueo OK. Facturado: {masa_facturada_total} | Esperado: {masa_esp_total:.1f} | Diff: {diff:.1f}% | Email: {status_email}"
        })
    except Exception as e: traceback.print_exc(); return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_glp.route('/registrar_consumo', methods=['POST'])
@login_required_custom
def registrar_consumo():
    try:
        data = request.get_json(force=True)
        empresa = session.get('empresa'); ubicacion = _normalize_sede(data.get('sede')); usuario = session.get('nombre'); op_id = _generar_op_id()
        cur = _get_connection().cursor()
        
        cur.execute("SELECT * FROM cardex_glp WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO' ORDER BY fecha DESC, id DESC LIMIT 1", (empresa, ubicacion))
        prev = cur.fetchone()
        if not prev: return jsonify({"success": False, "message": "No hay lote activo"}), 400
        lote = prev['lote']; pollitos = prev.get('pollitos', 0) or 1
        
        f_ini_obj = prev['fecha']
        if isinstance(f_ini_obj, str): f_ini_obj = datetime.strptime(f_ini_obj, '%Y-%m-%d').date()
        dias_op = max(1, (datetime.now().date() - f_ini_obj).days + 1)

        neto = 0; alertas = []; sql_cols = {}
        for i in range(1, 12): sql_cols[f"`nivel tk-{i}`"]=0; sql_cols[f"`capacidad tk-{i}`"]=0; sql_cols[f"`testigo nivel tk-{i}`"]=""

        for t in data.get('tanques', []):
            num = _extraer_numero(t.get('numero')); tk_key = f"tk-{num}"
            act = float(t.get('nivel', 0)); cap = float(t.get('capacidad', 0))
            col_p = f"nivelfinal {tk_key}" if prev.get('operacion') == 'tanqueo' else f"nivel {tk_key}"
            ant = prev.get(col_p.replace('`','')) or prev.get(f"nivel {tk_key}", 0)
            neto += max(0, (float(ant) - act)/100.0 * cap * 2.0)
            foto = _guardar_testigo(t.get('testigo'), lote, f"RF-NC-TK{num}-{op_id[:8]}")
            sql_cols[f"`nivel {tk_key}`"]=act; sql_cols[f"`capacidad {tk_key}`"]=cap; sql_cols[f"`testigo nivel {tk_key}`"]=foto
            if act <= 25:
                DRC = max(0, 17 - dias_op); TR = 8 * DRC; TS = min(80, max(TR, 25))
                alertas.append({"tk": f"TK-{num}", "nivel": act, "meta": TS})

        cols = ["fecha", "empresa", "id_empresa", "ubicacion", "lote", "estatus_lote", "operacion", "clase", "tipo", "registro", "op_id", "dias_operacion", "pollitos", "neto_gastado", "kg_pollito", "densidad_suministrada", "masa_kg_facturada", "pesos_pollito"]
        vals = [datetime.now(), empresa, session.get('empresa_id'), ubicacion, lote, 'ACTIVO', 'consumo', 'egreso', 'manual', usuario, op_id, dias_op, pollitos, neto, neto/pollitos if pollitos else 0, 0, 0, 0]
        
        for c, v in sql_cols.items(): cols.append(c); vals.append(v)
        cur.execute(f"INSERT INTO cardex_glp ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(vals))})", tuple(vals))
        
        msg_extra = "Email: N/A"
        if alertas:
            cod = _generar_codigo_pedido(empresa)
            cur.execute("INSERT INTO pedidos_gas_glp (cliente, codigo_pedido, estatus, fecha_registro, lote, ubicacion, proveedor, numero_factura, validador) VALUES (%s, %s, 'GENERADO', NOW(), %s, %s, 'PENDIENTE', 'PENDIENTE', 'SISTEMA')", (empresa, cod, lote, ubicacion))
            emails = _obtener_emails_proveedor_db(empresa, ubicacion)
            filas = "".join([f"<tr><td style='padding:10px;border-bottom:1px solid #eee;'>{t['tk']}</td><td style='padding:10px;border-bottom:1px solid #eee;'>{t['nivel']}%</td><td style='padding:10px;border-bottom:1px solid #eee;font-weight:bold;color:#015249;'>{t['meta']:.1f}%</td></tr>" for t in alertas])
            # PASAMOS EMPRESA AL HTML
            html = _generar_html_solicitud(empresa, ubicacion, lote, filas, cod)
            ok, msg_mail = _enviar_email_profesional(emails, f"SOLICITUD TANQUEO GRANJA --{ubicacion}--", html)
            msg_extra = msg_mail

        _get_connection().commit(); cur.close()
        return jsonify({"success": True, "message": f"Consumo registrado. {msg_extra}"})
    except Exception as e: traceback.print_exc(); return jsonify({"success": False, "message": str(e)}), 500

@csrf.exempt
@bp_glp.route('/finalizar_calefaccion', methods=['POST'])
@login_required_custom
def finalizar_calefaccion():
    try:
        data = request.get_json(force=True)
        empresa = session.get('empresa'); ubicacion = _normalize_sede(data.get('sede')); usuario = session.get('nombre'); op_id = _generar_op_id()
        cur = _get_connection().cursor()
        cur.execute("SELECT * FROM cardex_glp WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO' ORDER BY fecha DESC, id DESC LIMIT 1", (empresa, ubicacion))
        prev = cur.fetchone()
        if not prev: return jsonify({"success": False, "message": "No hay lote activo"}), 400
        lote = prev['lote']; pollitos = prev.get('pollitos', 0) or 1
        f_ini_obj = prev['fecha'] 
        if isinstance(f_ini_obj, str): f_ini_obj = datetime.strptime(f_ini_obj, '%Y-%m-%d').date()
        dias_op = (datetime.now().date() - f_ini_obj).days + 1
        
        sql_cols = {}
        for i in range(1, 12): sql_cols[f"`nivel tk-{i}`"]=0; sql_cols[f"`testigo nivel tk-{i}`"]=""
        for t in data.get('tanques', []):
            num = _extraer_numero(t.get('numero')); tk_key = f"tk-{num}"
            nf = float(t.get('nivel', 0))
            foto = _guardar_testigo(t.get('testigo'), lote, f"RF-FC-TK{num}-{op_id[:8]}")
            sql_cols[f"`nivel {tk_key}`"]=nf; sql_cols[f"`testigo nivel {tk_key}`"]=foto

        cols = ["fecha", "empresa", "id_empresa", "ubicacion", "lote", "estatus_lote", "operacion", "clase", "tipo", "registro", "op_id", "dias_operacion", "pollitos", "kg_pollito", "pesos_pollito"]
        vals = [datetime.now(), empresa, session.get('empresa_id'), ubicacion, lote, 'INACTIVO', 'finalizar_calefaccion', 'saldo final', 'manual', usuario, op_id, dias_op, pollitos, prev.get('kg_pollito', 0), 0]
        
        for c, v in sql_cols.items(): cols.append(c); vals.append(v)
        cur.execute(f"INSERT INTO cardex_glp ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(vals))})", tuple(vals))
        cur.execute("UPDATE cardex_glp SET estatus_lote='INACTIVO' WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND lote=%s", (empresa, ubicacion, lote))
        _get_connection().commit(); cur.close()
        return jsonify({"success": True, "message": "Lote cerrado exitosamente"})
    except Exception as e: traceback.print_exc(); return jsonify({"success": False, "message": str(e)}), 500