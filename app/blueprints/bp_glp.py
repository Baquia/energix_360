# -*- coding: utf-8 -*-
from flask import render_template  
from flask import Blueprint, jsonify, request, session
from flask import current_app as app
from datetime import datetime, timedelta
from app.utils import login_required_custom
import os, base64, smtplib, traceback, random, string
from email.mime.text import MIMEText
from app import mysql, csrf
import re as _re
import holidays # <--- NUEVO: Librería de Festivos
from app.utils import registrar_auditoria
from MySQLdb import IntegrityError
import math
import time
from MySQLdb import OperationalError

import requests

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
# --------------------------------

bp_glp = Blueprint('bp_glp', __name__, url_prefix='/glp')

# ==========================================
# CONFIGURACIÓN GLOBAL & HOLIDAYS
# ==========================================
co_holidays = holidays.CO()  # Inicializamos Festivos Colombia

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))

EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")

EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

def _enviar_alerta_telegram_oficial(ubicacion, usuario, nivel, codigo):
    """
    Envía alerta usando la API NATIVA de Telegram.
    100% Confiable para Producción.
    """
    # ================= TUS DATOS =================
    TOKEN = "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"  
    CHAT_ID = "5368207368"
    # =============================================

    mensaje = (
        f"🚨 *SOLICITUD DE GAS*\n\n"
        f"📍 Sede: {ubicacion}\n"
        f"👤 User: {usuario}\n"
        f"📉 Nivel: {nivel}%\n"
        f"🆔 Cod: {codigo}\n\n"
        f"⚠️ *Requiere Aprobación Inmediata*"
    )

    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        data = {
            "chat_id": CHAT_ID,
            "text": mensaje,
            "parse_mode": "Markdown"
        }
        # Enviamos la petición directa a Telegram
        resp = requests.post(url, data=data, timeout=5)
        
        if resp.status_code == 200:
            print("✅ Telegram enviado OK")
            return True
        else:
            print(f"❌ Error Telegram: {resp.text}")
            return False
    except Exception as e:
        print(f"❌ Error conexión Telegram: {e}")
        return False

# ==============
# Utilidades base
# ==============

# ==========================================
# UTILIDADES DE BLINDAJE (IDEMPOTENCIA)
# ==========================================
def _verificar_idempotencia(op_id, nombre_operacion, sede):
    """Barrera inicial: Revisa si la operación ya entró al servidor."""
    if not op_id: return None
    try:
        cur_check = mysql.connection.cursor()
        cur_check.execute("SELECT 1 FROM cardex_glp WHERE op_id=%s LIMIT 1", (op_id,))
        if cur_check.fetchone():
            cur_check.close()
            return jsonify({
                "success": True, 
                "message": "Operación ya registrada previamente.",
                "resumen": {"operacion": nombre_operacion, "sede": sede}
            }), 200
        cur_check.close()
    except Exception:
        pass
    return None

def _manejar_error_idempotencia(error, nombre_operacion, sede):
    """Barrera final: Captura el error 1062 de MySQL si ocurre una condición de carrera."""
    mysql.connection.rollback()
    if hasattr(error, 'args') and len(error.args) > 0 and error.args[0] == 1062:
        return jsonify({
            "success": True, 
            "message": "Operación recuperada (Ya estaba registrada).",
            "resumen": {"operacion": nombre_operacion, "sede": sede}
        })
    # Si es otro tipo de error de base de datos, lo mostramos
    return jsonify({"success": False, "message": f"Error BD: {str(error)}"})


def _normalize_sede(s):
    """Normaliza 'sede' aceptando formatos 'SEDE | EMPRESA' o espacios raros."""
    s = (s or "").strip()
    if "|" in s:
        s = s.split("|", 1)[0]
    s = _re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()
    return s


def _guardar_testigo(base64_data, carpeta, nombre_archivo):
    """Guarda un archivo de testigo (imagen) en la carpeta de estáticos CORRECTA (app/static)."""
    if not base64_data:
        return None

    try:
        data_match = _re.match(r'data:image/(?P<ext>png|jpeg);base64,(?P<data>.+)', base64_data)
        if not data_match:
            if "," in base64_data:
                base64_data = base64_data.split(",", 1)[1]
            ext = 'jpg'
            binary_data = base64.b64decode(base64_data)
        else:
            ext = data_match.group('ext')
            binary_data = base64.b64decode(data_match.group('data'))

        # --- Usar app.static_folder ---
        static_dir = os.path.join(app.static_folder, carpeta) 
        
        if not os.path.exists(static_dir):
            os.makedirs(static_dir)

        filename = f"{nombre_archivo}.{ext}"
        file_path = os.path.join(static_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(binary_data)

        # Calculamos la ruta relativa para la Base de Datos
        ruta_relativa = os.path.relpath(file_path, app.static_folder).replace(os.path.sep, "/")
        
        # Flask siempre sirve los estáticos bajo el prefijo /static/
        return f"/static/{ruta_relativa}"
        
    except Exception as e:
        app.logger.error(f"Error al guardar testigo: {e}")
        return None

def _resumen_tanques(tanques):
    salida = []
    for tk in tanques or []:
        num = tk.get("numero")
        try:
            niv = float(tk.get("nivel", 0))
        except Exception:
            niv = tk.get("nivel")
        try:
            cap = float(tk.get("capacidad", 0))
        except Exception:
            cap = tk.get("capacidad")
        salida.append({"numero": num, "nivel": niv, "capacidad": cap})
    return salida


def _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha_actual=None):
    """
    Calcula y actualiza los días de operación de un lote en cardex_glp.
    """
    if fecha_actual is None:
        fecha_actual = datetime.now().date()

    cur.execute("""
        SELECT MIN(fecha) AS fecha_ini
        FROM cardex_glp
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
          AND lote = %s
    """, (empresa, ubicacion, lote_id))
    
    # --- BLINDAJE ---
    row = cur.fetchone()
    fecha_ini = None
    if row:
        if isinstance(row, dict):
            fecha_ini = row.get("fecha_ini")
        else:
            fecha_ini = row[0]
    # ----------------

    dias = 1
    if fecha_ini:
        try:
            # Aseguramos que fecha_ini sea date si es necesario
            if isinstance(fecha_ini, datetime):
                fecha_ini = fecha_ini.date()
            dias = (fecha_actual - fecha_ini).days + 1
        except Exception:
            dias = 1

    cur.execute("""
        UPDATE cardex_glp
           SET dias_operacion = %s
         WHERE empresa = %s
           AND TRIM(ubicacion) = TRIM(%s)
           AND lote = %s
    """, (dias, empresa, ubicacion, lote_id))

    return dias


def _calcular_consumo_lote(cur, empresa, ubicacion, lote, id_operacion_actual, tanques):
    """
    Soporta N tanques variables.
    Acumula errores de todos los tanques y aborta al final si existe alguno.
    INCLUYE: Cálculo de velocidad de consumo diario por intervalo.
    CORRECCIÓN: kg_pollito ahora es ACUMULADO (Suma histórica de neto_gastado / pollitos).
    """
    if not tanques:
        return 0.0, 0.0, 0

    consumo_total_kg = 0.0
    errores_detectados = []
    
    for t in tanques:
        num_str = str(t.get('numero', ''))
        match = _re.search(r'\d+', num_str)
        num_tanque = match.group() if match else num_str 
        
        if not num_tanque: continue

        valor_actual_cierre = t.get('nivel')
        if valor_actual_cierre is None:
            valor_actual_cierre = t.get('nivel_inicial')
            
        try: valor_actual_cierre = float(valor_actual_cierre)
        except: valor_actual_cierre = 0.0

        try: capacidad_galones = float(t.get('capacidad') or 250)
        except: capacidad_galones = 250.0

        query = f"""
            SELECT operacion, 
                   `nivel tk-{num_tanque}`, 
                   `nivelfinal tk-{num_tanque}`, 
                   densidad_suministrada
            FROM cardex_glp 
            WHERE empresa = %s 
              AND TRIM(ubicacion) = TRIM(%s) 
              AND lote = %s
              AND id < %s
              AND (
                   `nivel tk-{num_tanque}` IS NOT NULL 
                   OR `nivelfinal tk-{num_tanque}` IS NOT NULL
              )
            ORDER BY fecha DESC, id DESC 
            LIMIT 1
        """
        
        cur.execute(query, (empresa, ubicacion, lote, id_operacion_actual))
        prev_raw = cur.fetchone()
        
        if not prev_raw: continue
        
        prev = {}
        if isinstance(prev_raw, dict):
             prev = prev_raw
        elif isinstance(prev_raw, tuple):
             prev = {
                 'operacion': prev_raw[0],
                 f'nivel tk-{num_tanque}': prev_raw[1],
                 f'nivelfinal tk-{num_tanque}': prev_raw[2],
                 'densidad_suministrada': prev_raw[3]
             }

        op_anterior = prev.get('operacion')
        nivel_anterior = 0.0
        densidad_calculo = 2.0

        if op_anterior == 'tanqueo':
            nivel_anterior = float(prev.get(f'nivelfinal tk-{num_tanque}') or 0)
            d_real = float(prev.get('densidad_suministrada') or 0)
            if d_real > 0:
                densidad_calculo = d_real
        else:
            nivel_anterior = float(prev.get(f'nivel tk-{num_tanque}') or 0)
        
        delta_pct = nivel_anterior - valor_actual_cierre

        if delta_pct < -0.1:
            errores_detectados.append(
                f"• TK-{num_tanque}: Ingresó {valor_actual_cierre}%, anterior {nivel_anterior}% (SUBIÓ)."
            )
        
        if delta_pct < 0: delta_pct = 0

        kg_tanque = (delta_pct / 100.0) * capacidad_galones * densidad_calculo
        consumo_total_kg += kg_tanque

    if errores_detectados:
        mensaje_final = "⛔ ERRORES DETECTADOS:\n\n" + "\n".join(errores_detectados) + "\n\nEl nivel no puede subir sin un tanqueo."
        raise ValueError(mensaje_final)

    # 1. Obtener la población inicial (pollitos)
    cur.execute("""
        SELECT pollitos
        FROM cardex_glp
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
          AND lote = %s
          AND operacion = 'inicio_calefaccion'
        ORDER BY fecha ASC, id ASC
        LIMIT 1
    """, (empresa, ubicacion, lote))
    
    row_ini = cur.fetchone()
    pollitos = 0
    if row_ini:
        if isinstance(row_ini, dict):
            pollitos = int(row_ini.get("pollitos") or 0)
        else:
            pollitos = int(row_ini[0] or 0)

    # ========================================================
    # NUEVA LÓGICA: CÁLCULO ACUMULADO DE KG_POLLITO
    # ========================================================
    
    # 2. Sumar todo el consumo (neto_gastado) ANTERIOR de este mismo lote
    cur.execute("""
        SELECT SUM(neto_gastado) 
        FROM cardex_glp 
        WHERE empresa = %s AND lote = %s AND id < %s
    """, (empresa, lote, id_operacion_actual))
    row_sum = cur.fetchone()
    
    consumo_previo = 0.0
    if row_sum:
        val = row_sum.get('SUM(neto_gastado)') if isinstance(row_sum, dict) else row_sum[0]
        consumo_previo = float(val or 0.0)

    # 3. Consumo Total Acumulado (Lo de antes + lo de hoy)
    consumo_acumulado_total = consumo_previo + consumo_total_kg

    # 4. Cálculo final de Eficiencia Acumulada
    kg_pollito_acumulado = 0.0
    if pollitos > 0:
        kg_pollito_acumulado = consumo_acumulado_total / float(pollitos)

    # 5. Actualizar la base de datos (Notar que neto_gastado sigue guardando solo el consumo de hoy, 
    # pero kg_pollito guarda el acumulado histórico)
    cur.execute("""
        UPDATE cardex_glp
           SET kg_pollito = %s,
               neto_gastado = %s
         WHERE id = %s
    """, (kg_pollito_acumulado, consumo_total_kg, id_operacion_actual))
    # ========================================================

    if pollitos > 0 and consumo_total_kg > 0:
        try:
            cur.execute("""
                SELECT fecha 
                FROM cardex_glp 
                WHERE empresa = %s AND TRIM(ubicacion) = TRIM(%s) AND lote = %s
                  AND id <= %s 
                ORDER BY fecha DESC, id DESC
                LIMIT 2
            """, (empresa, ubicacion, lote, id_operacion_actual))
            
            fechas_rows = cur.fetchall()
            dias_intervalo = 1.0

            if len(fechas_rows) >= 2:
                r_curr = fechas_rows[0]
                r_prev = fechas_rows[1]
                
                f_curr = r_curr.get('fecha') if isinstance(r_curr, dict) else r_curr[0]
                f_prev = r_prev.get('fecha') if isinstance(r_prev, dict) else r_prev[0]
                
                if isinstance(f_curr, datetime): f_curr = f_curr.date()
                if isinstance(f_prev, datetime): f_prev = f_prev.date()
                
                if f_curr and f_prev:
                    delta = (f_curr - f_prev).days
                    dias_intervalo = float(delta) if delta > 0 else 1.0
            
            # OJO: La velocidad_consumo sí debe seguir midiendo la agresividad del consumo 
            # de ESTE intervalo (para proyecciones), por eso no usa el acumulado.
            velocidad = (consumo_total_kg / float(pollitos)) / dias_intervalo
            
            cur.execute("""
                UPDATE cardex_glp 
                SET velocidad_consumo = %s 
                WHERE id = %s
            """, (velocidad, id_operacion_actual))
            
        except Exception as e:
            print(f"⚠️ Error calculando velocidad_consumo: {e}")
            pass

    return consumo_total_kg, kg_pollito_acumulado, pollitos


def _buscar_proveedor_principal(cur, empresa, ubicacion, tanques):
    if not tanques:
        return None
    
    primer_num = str(tanques[0].get("numero") or "").upper().strip()
    if not primer_num:
        return None

    cur.execute("""
        SELECT proveedor
        FROM tanques_sedes
        WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND UPPER(nombre_tanque)=UPPER(%s)
        LIMIT 1
    """, (empresa, ubicacion, primer_num))
    
    p = cur.fetchone()
    
    if p:
        if isinstance(p, dict):
            return p.get("proveedor")
        else:
            return p[0]
    return None


def _generar_codigo_pedido(cliente_nombre, lote_id, ubicacion, proveedor, cur):
    mes = datetime.now().strftime("%m")
    
    partes = _re.sub(r'[^a-zA-Z\s]', '', cliente_nombre).upper().split()
    iniciales = "".join(p[0] for p in partes if len(p) > 2)[:3].ljust(3, 'X')
    
    codigo_pedido = None
    max_intentos = 10 

    for _ in range(max_intentos):
        sufijo = ''.join(random.choices(string.digits, k=4))
        candidato = f"{mes}-{iniciales}-{sufijo}"
        
        cur.execute("SELECT codigo_pedido FROM pedidos_gas_glp WHERE codigo_pedido = %s", (candidato,))
        if cur.fetchone() is None:
            codigo_pedido = candidato
            break
            
    if codigo_pedido is None:
        raise Exception("No se pudo generar un código de pedido único.")

    query = """
        INSERT INTO pedidos_gas_glp 
            (cliente, codigo_pedido, estatus, fecha_registro, lote, ubicacion, proveedor) 
        VALUES 
            (%s, %s, 'generado', NOW(), %s, %s, %s)
    """
    cur.execute(query, (cliente_nombre, codigo_pedido, lote_id, ubicacion, proveedor))
    return codigo_pedido

def _enviar_alerta_webmaster_nueva_solicitud(empresa, ubicacion, usuario, nivel_actual, codigo_pedido):
    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    try:
        email_port = int(os.environ.get("EMAIL_PORT", "587"))
    except:
        email_port = 587
    email_webmaster = os.environ.get("EMAIL_ADMIN", "tu_email_webmaster@empresa.com") 

    if not email_user or not email_pass:
        return False

    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #fce4ec; padding: 20px;">
        <div style="background-color: #ffffff; max-width: 500px; margin: 0 auto; border: 1px solid #d81b60; border-radius: 8px; padding: 20px;">
            <h2 style="color: #d81b60; text-align: center;">🔔 Acción Requerida: Webmaster</h2>
            <p>Nueva <strong>Solicitud Manual de Gas</strong> pendiente de aprobación.</p>
            <ul>
                <li><strong>Sede:</strong> {ubicacion}</li>
                <li><strong>Usuario:</strong> {usuario}</li>
                <li><strong>Nivel Reportado:</strong> {nivel_actual}%</li>
                <li><strong>Código:</strong> {codigo_pedido}</li>
            </ul>
            <div style="text-align: center; margin-top: 20px;">
                <p style="font-size: 12px; color: #888;">Ingresa a la plataforma para aprobar o rechazar.</p>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"🔔 APROBAR SOLICITUD: {ubicacion} ({codigo_pedido})"
        msg["From"] = email_user
        msg["To"] = email_webmaster

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, [email_webmaster], msg.as_string())
        print(f"✅ Alerta enviada al Webmaster: {email_webmaster}")
        return True
    except Exception as e:
        print(f"❌ Error alerta webmaster: {e}")
        return False

def _enviar_alerta_pedido_tanqueo(empresa, ubicacion, lote_id, proveedor_principal, tanques_bajos, codigo_pedido):
    if not tanques_bajos:
        return False

    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    try:
        email_port = int(os.environ.get("EMAIL_PORT", "587"))
    except:
        email_port = 587
    email_from = os.environ.get("EMAIL_FROM", email_user)

    if not email_user or not email_pass:
        app.logger.error("⛔ Error: Credenciales de correo no configuradas.")
        return False

    cur = mysql.connection.cursor()
    cur.execute("SELECT email1, email2 FROM proveedores WHERE proveedor = %s", (proveedor_principal,))
    c_raw = cur.fetchone()
    c = {}
    if c_raw:
        if isinstance(c_raw, dict):
            c = c_raw
        else:
            c = {'email1': c_raw[0], 'email2': c_raw[1]}
    
    destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
    cur.close()

    if not destinatarios and email_user:
        destinatarios = [email_user]

    if not destinatarios:
        return False

    items_html = ""
    for t in (tanques_bajos or []):
        numero = t.get("numero") if isinstance(t, dict) else None
        nivel = t.get("nivel") if isinstance(t, dict) else t.get("nivel_inicial")
        try:
            nivel_float = float(nivel if nivel is not None else 0)
        except:
            nivel_float = 0.0

        items_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 10px; text-align: center;"><strong>{numero or "-"}</strong></td>
                <td style="padding: 10px; text-align: center; color: #d9534f;"><strong>{round(nivel_float, 2)}%</strong></td>
                <td style="padding: 10px; text-align: center;">80%</td>
            </tr>
        """

    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
        .container {{ max-width: 600px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
        .header {{ background-color: #015249; color: #ffffff; padding: 20px; text-align: center; }}
        .header h2 {{ margin: 0; font-size: 24px; }}
        .content {{ padding: 25px; }}
        .info-box {{ background-color: #f9fbfb; border-left: 4px solid #015249; padding: 15px; margin-bottom: 20px; }}
        .info-box p {{ margin: 5px 0; }}
        .codigo-box {{ background-color: #e8f5e9; border: 2px dashed #015249; padding: 15px; text-align: center; margin: 20px 0; border-radius: 6px; }}
        .codigo-title {{ font-size: 14px; color: #555; margin-bottom: 5px; }}
        .codigo-valor {{ font-size: 28px; font-weight: bold; color: #015249; letter-spacing: 1px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th {{ background-color: #f2f2f2; padding: 10px; text-align: center; font-size: 14px; }}
        .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
        .footer p {{ margin: 5px 0; }}
    </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>Solicitud de Tanqueo GLP</h2>
            </div>
            <div class="content">
                <p>Estimado proveedor <strong>{proveedor_principal or 'N/D'}</strong>,</p>
                <p>Se requiere el suministro de GLP para la siguiente sede operativa:</p>
                
                <div class="info-box">
                    <p><strong>Empresa:</strong> {empresa}</p>
                    <p><strong>Sede:</strong> {ubicacion}</p>
                    <p><strong>Fecha de Solicitud:</strong> {datetime.now().strftime('%Y-%m-%d')}</p>
                </div>

                <div class="codigo-box">
                    <div class="codigo-title">CÓDIGO DE PEDIDO (OBLIGATORIO EN FACTURA)</div>
                    <div class="codigo-valor">{codigo_pedido}</div>
                </div>

                <p><strong>Tanques que requieren llenado (Nivel ≤ 30%):</strong></p>
                <table>
                    <thead>
                        <tr>
                            <th>Tanque</th>
                            <th>Nivel Actual</th>
                            <th>Llenar hasta</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items_html}
                    </tbody>
                </table>

                <p style="margin-top: 20px; font-size: 14px; color: #555;">
                    <em>Por favor, incluya el código de pedido anterior en la factura o remisión para que el suministro sea procesado correctamente en el sistema BQA-ONE.</em>
                </p>
            </div>
            <div class="footer">
                <p>Este es un mensaje automático generado por el sistema <strong>BQA-ONE / Energix360</strong>.</p>
                <p>No responda a este correo.</p>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"🆕 Solicitud de Tanqueo GLP - {ubicacion} - Cod: {codigo_pedido}"
        msg["From"] = email_from
        msg["To"] = ", ".join(destinatarios)

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())
        app.logger.info(f"✅ Correo GLP enviado a: {destinatarios}. Código: {codigo_pedido}")
        return True

    except Exception as e:
        app.logger.error(f"⛔ Error al enviar correo GLP: {e}")
        return False


def _enviar_alerta_desviacion_tanqueo(
    empresa, ubicacion, lote_id, proveedor_principal, op_id,
    masa_esperada_total, masa_facturada_total, desvio_total_pct, dens_prom, tanques
):
    """Envía correo de alerta de desviación con diseño corporativo."""

    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    try:
        email_port = int(os.environ.get("EMAIL_PORT", "587"))
    except:
        email_port = 587
    email_from = os.environ.get("EMAIL_FROM", email_user)
    
    if not email_user or not email_pass:
        return False

    email_baqone = os.environ.get("EMAIL_ALERT_BAQONE", email_user)

    cur = mysql.connection.cursor()
    cur.execute("SELECT email1, email2 FROM proveedores WHERE proveedor = %s", (proveedor_principal,))
    c_raw = cur.fetchone()
    c = {}
    if c_raw:
        if isinstance(c_raw, dict):
            c = c_raw
        else:
            c = {'email1': c_raw[0], 'email2': c_raw[1]}
    
    destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
    cur.close()

    if email_baqone and email_baqone not in destinatarios:
        destinatarios.append(email_baqone)

    if not destinatarios:
        return False

    items_html = ""
    for t in (tanques or []):
        num = t.get("numero") or "-"
        ni = float(t.get("nivel_inicial") or 0)
        nf = float(t.get("nivel_final") or 0)
        delta = nf - ni
        cap = float(t.get("capacidad") or 0)
        dens = float(t.get("densidad_suministrada") or 0)
        kg_fact = float(t.get("kg_suministrados") or 0)
        kg_esp = dens * cap * (delta / 100.0) if delta > 0 else 0.0

        items_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 8px;"><strong>{num}</strong></td>
                <td style="padding: 8px;">{round(ni,1)}% → {round(nf,1)}%</td>
                <td style="padding: 8px;">{round(delta,1)}%</td>
                <td style="padding: 8px;">{round(kg_esp,1)} kg</td>
                <td style="padding: 8px; background-color: #ffebee;"><strong>{round(kg_fact,1)} kg</strong></td>
            </tr>
        """

    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; }}
        .container {{ max-width: 650px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
        .header {{ background-color: #d9534f; color: #ffffff; padding: 20px; text-align: center; }} 
        .header h2 {{ margin: 0; }}
        .content {{ padding: 25px; }}
        .alert-box {{ background-color: #ffebee; color: #c62828; padding: 15px; border-radius: 6px; text-align: center; font-size: 18px; font-weight: bold; margin-bottom: 20px; }}
        .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px; }}
        .info-item {{ background: #f9fbfb; padding: 12px; border-radius: 6px; border-left: 3px solid #d9534f; }}
        .info-label {{ font-size: 12px; color: #777; display: block; margin-bottom: 4px; }}
        .info-value {{ font-size: 16px; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        th {{ background-color: #f2f2f2; padding: 8px; text-align: left; }}
        .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
    </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>⚠️ Alerta de Desviación en Tanqueo GLP</h2>
            </div>
            <div class="content">
                <div class="alert-box">
                    Desviación detectada: {round(desvio_total_pct, 2)}%
                </div>
                <p>Se ha registrado un tanqueo con una diferencia significativa entre la masa esperada y la facturada.</p>

                <div class="info-grid">
                    <div class="info-item"><span class="info-label">Sede</span><span class="info-value">{ubicacion}</span></div>
                    <div class="info-item"><span class="info-label">Proveedor</span><span class="info-value">{proveedor_principal}</span></div>
                    <div class="info-item"><span class="info-label">Masa Esperada (Total)</span><span class="info-value">{round(masa_esperada_total, 2)} kg</span></div>
                    <div class="info-item"><span class="info-label" style="color: #c62828;">Masa Facturada (Total)</span><span class="info-value" style="color: #c62828;">{round(masa_facturada_total, 2)} kg</span></div>
                    <div class="info-item"><span class="info-label">Densidad Promedio</span><span class="info-value">{round(dens_prom, 3)} kg/gal</span></div>
                    <div class="info-item"><span class="info-label">ID Operación</span><span class="info-value">{op_id}</span></div>
                </div>

                <h3>Detalle por Tanque:</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Tanque</th>
                            <th>Nivel (Ini → Fin)</th>
                            <th>Δ Nivel</th>
                            <th>Esperado</th>
                            <th>Facturado</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items_html}
                    </tbody>
                </table>
                <p style="margin-top: 20px; font-size: 12px; color: #777;">
                    * Esta alerta se genera automáticamente cuando la masa facturada supera a la esperada en más del 8%.
                </p>
            </div>
            <div class="footer">
                <p>Reporte generado por <strong>BQA-ONE / Energix360</strong>.</p>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"🚨 Alerta: Desviación Tanqueo {round(desvio_total_pct,1)}% - {ubicacion}"
        msg["From"] = email_from
        msg["To"] = ", ".join(destinatarios)

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())
        app.logger.info(f"✅ Correo desviación enviado. op_id: {op_id}")
        return True

    except Exception as e:
        app.logger.error(f"⛔ Error al enviar correo desviación: {e}")
        return False

def _calcular_ts_consumo(dias_operacion: int):
    """
    Algoritmo (foto) para CONSUMO:
      dr = 15 - dias_operacion
      tr = 8 * dr
      si tr > 80  -> ts = 80
      si tr <= 80 -> ts = tr
    """
    try:
        d = int(dias_operacion or 0)
    except Exception:
        d = 0

    dr = 15 - d
    if dr < 0:
        dr = 0

    tr = 8.0 * float(dr)

    ts = 80.0 if tr > 80.0 else float(tr)
    if ts < 0.0:
        ts = 0.0
    if ts > 80.0:
        ts = 80.0

    return dr, tr, ts


def _enviar_alerta_pedido_tanqueo_consumo(
    empresa,
    ubicacion,
    lote_id,
    proveedor_principal,
    tanques_bajos,
    codigo_pedido,
    ts_solicitado
):
    """
    Envía correo de alerta de pedido (CONSUMO) con diseño corporativo.
    El nivel de llenado es variable (ts_solicitado).
    """

    # Validación mínima
    if not tanques_bajos:
        return False

    try:
        ts_val = float(ts_solicitado or 0)
    except Exception:
        ts_val = 0.0

    # Evitar pedidos absurdos
    if ts_val <= 0.0:
        return False

    # Usar variables locales
    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    try:
        email_port = int(os.environ.get("EMAIL_PORT", "587"))
    except:
        email_port = 587
    email_from = os.environ.get("EMAIL_FROM", email_user)

    if not email_user or not email_pass:
        app.logger.error("⛔ Error: Credenciales de correo no configuradas.")
        return False

    cur = mysql.connection.cursor()
    cur.execute("SELECT email1, email2 FROM proveedores WHERE proveedor = %s", (proveedor_principal,))
    
    # --- BLINDAJE ---
    c_raw = cur.fetchone()
    c = {}
    if c_raw:
        if isinstance(c_raw, dict):
            c = c_raw
        else:
            c = {'email1': c_raw[0], 'email2': c_raw[1]}
    # ----------------

    destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
    cur.close()

    if not destinatarios and email_user:
        destinatarios = [email_user]

    if not destinatarios:
        return False

    # --- Construcción de Filas de Tanques (HTML) ---
    items_html = ""
    for t in (tanques_bajos or []):
        numero = t.get("numero") if isinstance(t, dict) else None
        
        nivel = None
        if isinstance(t, dict):
            nivel = t.get("nivel")
            if nivel is None:
                nivel = t.get("nivel_inicial")

        try:
            nivel_float = float(nivel if nivel is not None else 0)
        except:
            nivel_float = 0.0

        items_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 10px; text-align: center;"><strong>{numero or "-"}</strong></td>
                <td style="padding: 10px; text-align: center; color: #d9534f;"><strong>{round(nivel_float, 2)}%</strong></td>
                <td style="padding: 10px; text-align: center;"><strong>{round(ts_val, 2)}%</strong></td>
            </tr>
        """

    # --- CUERPO DEL CORREO (Diseño Corporativo) ---
    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
        .container {{ max-width: 600px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
        .header {{ background-color: #015249; color: #ffffff; padding: 20px; text-align: center; }}
        .header h2 {{ margin: 0; font-size: 24px; }}
        .content {{ padding: 25px; }}
        .info-box {{ background-color: #f9fbfb; border-left: 4px solid #015249; padding: 15px; margin-bottom: 20px; }}
        .info-box p {{ margin: 5px 0; }}
        .codigo-box {{ background-color: #e8f5e9; border: 2px dashed #015249; padding: 15px; text-align: center; margin: 20px 0; border-radius: 6px; }}
        .codigo-title {{ font-size: 14px; color: #555; margin-bottom: 5px; }}
        .codigo-valor {{ font-size: 28px; font-weight: bold; color: #015249; letter-spacing: 1px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
        th {{ background-color: #f2f2f2; padding: 10px; text-align: center; font-size: 14px; }}
        .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
        .note {{ font-size: 13px; color: #666; font-style: italic; margin-top: 10px; }}
    </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>Solicitud de Tanqueo GLP (Consumo)</h2>
            </div>
            <div class="content">
                <p>Estimado proveedor <strong>{proveedor_principal or 'N/D'}</strong>,</p>
                <p>Se requiere el suministro de GLP para la siguiente sede operativa:</p>
                
                <div class="info-box">
                    <p><strong>Empresa:</strong> {empresa}</p>
                    <p><strong>Sede:</strong> {ubicacion}</p>
                    <p><strong>Fecha:</strong> {datetime.now().strftime('%Y-%m-%d')}</p>
                </div>

                <div class="codigo-box">
                    <div class="codigo-title">CÓDIGO DE PEDIDO (OBLIGATORIO EN FACTURA)</div>
                    <div class="codigo-valor">{codigo_pedido}</div>
                </div>

                <p><strong>Tanques con nivel crítico (≤ 25%):</strong></p>
                <table>
                    <thead>
                        <tr>
                            <th>Tanque</th>
                            <th>Nivel Actual</th>
                            <th>Llenar hasta</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items_html}
                    </tbody>
                </table>

                <p class="note">
                    * El nivel de llenado solicitado ha sido calculado automáticamente por el sistema según los días restantes de operación.
                </p>

                <p style="margin-top: 20px; font-size: 14px; color: #555;">
                    <em>Por favor, incluya el código de pedido anterior en la factura o remisión para que el suministro sea considerado <b>válido</b> en el sistema BQA-ONE.</em>
                </p>
            </div>
            <div class="footer">
                <p>Este es un mensaje automático generado por el sistema <strong>BQA-ONE / Energix360</strong>.</p>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"Solicitud de Tanqueo GLP (CONSUMO): {ubicacion} - Cod: {codigo_pedido}"
        msg["From"] = email_from
        msg["To"] = ", ".join(destinatarios)

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())

        app.logger.info(f"✅ Correo GLP (CONSUMO) enviado a: {destinatarios}. Código: {codigo_pedido}")
        return True

    except Exception:
        app.logger.error("⛔ Error al enviar correo GLP (CONSUMO):")
        traceback.print_exc()
        return False
    
def _enviar_alerta_pedido_inicio(empresa, ubicacion, lote_id, proveedor_principal, tanques, codigo_pedido):
    """
    Envía solicitud AUTOMÁTICA de tanqueo al iniciar calefacción.
    Diseño: Corporativo (Igual a Orden de Suministro).
    """
    email_user = os.environ.get("EMAIL_USER")
    email_pass = os.environ.get("EMAIL_PASS")
    email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    try:
        email_port = int(os.environ.get("EMAIL_PORT", "587"))
    except:
        email_port = 587
    email_from = os.environ.get("EMAIL_FROM", email_user)

    if not email_user or not email_pass:
        return False

    # 1. Obtener destinatarios
    cur = mysql.connection.cursor()
    cur.execute("SELECT email1, email2 FROM proveedores WHERE proveedor = %s", (proveedor_principal,))
    c_raw = cur.fetchone()
    destinatarios = []
    if c_raw:
        if isinstance(c_raw, dict):
            if c_raw.get('email1'): destinatarios.append(c_raw['email1'])
            if c_raw.get('email2'): destinatarios.append(c_raw['email2'])
        else:
            if c_raw[0]: destinatarios.append(c_raw[0])
            if c_raw[1]: destinatarios.append(c_raw[1])
    cur.close()

    if not destinatarios and email_user:
        destinatarios = [email_user] 

    # 2. Construir filas de la tabla
    items_html = ""
    for t in tanques:
        try:
            num = t.get("numero") or "N/A"
            nivel = float(t.get("nivel") or 0)
        except:
            num = "Ref"
            nivel = 0.0
            
        items_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 10px; text-align: center;"><strong>{num}</strong></td>
                <td style="padding: 10px; text-align: center; color: #d9534f;">{nivel}%</td>
                <td style="padding: 10px; text-align: center; font-weight:bold; color: #015249;">80%</td>
            </tr>
        """

    # 3. HTML Profesional (Identidad Corporativa #015249)
    cuerpo = f"""
    <!DOCTYPE html>
    <html lang="es">
    <body style="background-color: #f4f4f4; padding: 20px; font-family: Arial, sans-serif;">

        <div style="background-color: #ffffff; max-width: 500px; margin: 0 auto; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); overflow: hidden;">
            
            <div style="background-color: #015249; color: white; padding: 20px; text-align: center;">
                <h2 style="margin: 0; font-size: 22px;">Orden de Suministro GLP</h2>
                <p style="margin: 5px 0 0 0; opacity: 0.9; font-size: 14px;">Protocolo de Inicio de Calefacción</p>
            </div>

            <div style="padding: 25px;">
                
                <p style="color: #333; margin-bottom: 15px;">Estimado proveedor <strong>{proveedor_principal}</strong>,</p>
                <p style="color: #555; line-height: 1.5;">
                    Se ha iniciado un nuevo ciclo productivo en la sede <strong>{ubicacion}</strong>. 
                    Se solicita programar el suministro para llevar los tanques al <strong>80%</strong>.
                </p>

                <div style="background-color: #e8f5e9; border: 2px dashed #015249; padding: 15px; text-align: center; margin: 25px 0; border-radius: 6px;">
                    <div style="font-size: 11px; text-transform: uppercase; color: #555; letter-spacing: 1px; margin-bottom: 5px;">CÓDIGO DE PEDIDO</div>
                    <div style="font-size: 26px; font-weight: 800; color: #015249;">{codigo_pedido}</div>
                </div>

                <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                    <thead style="background-color: #f8f9fa;">
                        <tr>
                            <th style="padding: 10px; text-align: center; color: #666;">Tanque</th>
                            <th style="padding: 10px; text-align: center; color: #666;">Nivel Actual</th>
                            <th style="padding: 10px; text-align: center; color: #666;">Objetivo</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items_html}
                    </tbody>
                </table>

                <div style="margin-top: 25px; padding: 10px; background-color: #fff8e1; border-left: 4px solid #ffc107; font-size: 12px; color: #795548;">
                    <strong>Nota:</strong> Este es un pedido automático generado por el sistema de gestión BQA-ONE.
                </div>
            </div>

            <div style="background-color: #f8f9fa; padding: 15px; text-align: center; border-top: 1px solid #eee;">
                <p style="margin: 0; color: #999; font-size: 11px;">
                    Energix360 System
                </p>
            </div>

        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"✅ Pedido Inicial: {ubicacion} - Cod: {codigo_pedido}"
        msg["From"] = email_from
        msg["To"] = ", ".join(destinatarios)

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())
        return True
    except Exception as e:
        print(f"Error email inicio: {e}")
        return False

# ==========================================
# CEREBRO MATEMÁTICO & COPILOTO (NUEVO)
# ==========================================

def _calcular_velocidad_consumo(cur, empresa, ubicacion, lote):
    """Calcula la DERIVADA del consumo (Tasa de descenso % por día)."""
    cur.execute("""
        SELECT fecha, `nivel tk-1`, `nivel tk-2`
        FROM cardex_glp
        WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND lote=%s
          AND operacion IN ('inicio_calefaccion', 'consumo')
        ORDER BY fecha DESC, id DESC
        LIMIT 5
    """, (empresa, ubicacion, lote))
    
    rows = cur.fetchall()
    if not rows or len(rows) < 2: return 8.0 

    deltas = []
    data_points = []
    for r in rows:
        if isinstance(r, dict):
            v1 = float(r.get('nivel tk-1') or 0)
            v2 = float(r.get('nivel tk-2') or 0)
            fecha = r.get('fecha')
        else:
            v1 = float(r[1] or 0)
            v2 = float(r[2] or 0)
            fecha = r[0]
        
        avg_nivel = v1 if v2 == 0 else (v1 + v2) / 2
        data_points.append({'fecha': fecha, 'nivel': avg_nivel})

    for i in range(len(data_points) - 1):
        actual = data_points[i]
        anterior = data_points[i+1]
        diff_dias = (actual['fecha'] - anterior['fecha']).days
        if diff_dias == 0: diff_dias = 1
        
        diff_nivel = anterior['nivel'] - actual['nivel']
        if diff_nivel > 0:
            deltas.append(diff_nivel / diff_dias)

    if not deltas: return 8.0
    velocidad_promedio = sum(deltas) / len(deltas)
    return max(velocidad_promedio, 1.0)

def _calcular_eficiencia_acumulada(cur, lote):
    """Calcula la INTEGRAL del consumo vs pollitos."""
    cur.execute("SELECT pollitos FROM cardex_glp WHERE lote=%s AND operacion='inicio_calefaccion' LIMIT 1", (lote,))
    p_row = cur.fetchone()
    pollitos = 0
    if p_row:
        pollitos = p_row.get('pollitos') if isinstance(p_row, dict) else p_row[0]
    
    if not pollitos or pollitos == 0: return 0.0

    cur.execute("SELECT SUM(neto_gastado) FROM cardex_glp WHERE lote=%s", (lote,))
    s_row = cur.fetchone()
    total_kg = 0.0
    if s_row:
        total_kg = s_row.get('SUM(neto_gastado)') if isinstance(s_row, dict) else s_row[0]
    
    return float(total_kg or 0.0) / float(pollitos)

def _analizar_riesgo_glp(cur, empresa, ubicacion, lote, nivel_actual_ponderado, fecha_actual):
    """
    Algoritmo 'Inteligente y Blindado':
    Unificado al 30% como umbral de seguridad base.
    """
    requiere_gas = False
    razon = ""
    sugerencia_carga = 0
    dias_extra = 0

    try:
        # 1. Obtener Días Extra Aprobados
        cur.execute("SELECT dias_extra FROM pedidos_gas_glp WHERE lote=%s AND estatus_flujo IN ('aprobado_webmaster', 'enviado_auto') ORDER BY id DESC LIMIT 1", (lote,))
        row_extra = cur.fetchone()
        if row_extra:
            dias_extra = int(row_extra['dias_extra'] if isinstance(row_extra, dict) else row_extra[0])

        # 2. Calcular Ciclo Total y Dias Transcurridos
        ciclo_base = 15
        ciclo_total = ciclo_base + dias_extra
        
        cur.execute("SELECT MIN(fecha) FROM cardex_glp WHERE lote=%s", (lote,))
        row_ini = cur.fetchone()
        fecha_ini = row_ini[0] if row_ini else fecha_actual
        if isinstance(fecha_ini, datetime): fecha_ini = fecha_ini.date()
        if isinstance(fecha_actual, datetime): fecha_actual = fecha_actual.date()
        
        dias_operacion = (fecha_actual - fecha_ini).days + 1
        dias_restantes = ciclo_total - dias_operacion

        # 3. Velocidad de consumo real
        velocidad = _calcular_velocidad_consumo(cur, empresa, ubicacion, lote)

        # 4. Detección de Puentes (Lookahead Dinámico)
        dias_cobertura_necesaria = 1
        check_date = fecha_actual + timedelta(days=1)
        es_puente = False
        
        while check_date in co_holidays or check_date.weekday() >= 5: 
            dias_cobertura_necesaria += 1
            check_date += timedelta(days=1)
            es_puente = True

        # 5. Cálculo de Proyección
        nivel_proyectado_fin_cobertura = nivel_actual_ponderado - (velocidad * dias_cobertura_necesaria)
        
        # PISO DE SEGURIDAD UNIFICADO
        piso_seguridad = 30.0

        # Lógica de Decisión
        if nivel_actual_ponderado <= 30.0:
            requiere_gas = True
            razon = f"Nivel Crítico ({round(nivel_actual_ponderado,1)}%). Riesgo inminente."
            sugerencia_carga = 85 - nivel_actual_ponderado

        elif es_puente and nivel_proyectado_fin_cobertura < piso_seguridad:
            requiere_gas = True
            razon = f"Riesgo por Puente/Festivo. Proy. al retorno: {round(nivel_proyectado_fin_cobertura,1)}%."
            necesario = (velocidad * dias_cobertura_necesaria) + 20 
            sugerencia_carga = necesario if (nivel_actual_ponderado + necesario) <= 85 else (85 - nivel_actual_ponderado)

        elif dias_restantes > 0:
            nivel_fin_ciclo = nivel_actual_ponderado - (velocidad * dias_restantes)
            if nivel_fin_ciclo < piso_seguridad:
                requiere_gas = True
                razon = f"No alcanza para terminar ciclo ({ciclo_total} días). Faltan {dias_restantes} días."
                sugerencia_carga = ((velocidad * dias_restantes) + 20) - nivel_actual_ponderado

        if sugerencia_carga < 0: sugerencia_carga = 0
        if sugerencia_carga > 85: sugerencia_carga = 85

    except Exception as e:
        if nivel_actual_ponderado < 30: 
            return {"requiere_gas": True, "razon": "Fallback seguridad", "sugerencia_carga": 50, "dias_actuales": 0, "dias_extra_active": 0}

    return {
        "requiere_gas": requiere_gas, 
        "razon": razon, 
        "sugerencia_carga": int(sugerencia_carga),
        "dias_actuales": dias_operacion,
        "dias_extra_active": dias_extra
    }

@csrf.exempt
@bp_glp.route('/context', methods=['GET'])
# @login_required_custom
def glp_context():
    return jsonify({"success": True, "message": "Servidor disponible"})

# ======================
# Obtener tanques por sede
# ======================
@csrf.exempt
@bp_glp.route('/obtener_tanques', methods=['POST'])
@login_required_custom
def obtener_tanques():
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "tanques": [], "message": "JSON inválido"}), 400

    empresa = session.get('empresa') or ''
    sede = _normalize_sede(data.get('sede') or '')

    try:
        with mysql.connection.cursor() as cur:
            # 1. Buscar Tanques
            cur.execute("""
                SELECT nombre_tanque AS numero,
                       capacidad_gls  AS capacidad
                FROM tanques_sedes
                WHERE empresa = %s
                  AND TRIM(ubicacion) = TRIM(%s)
                ORDER BY nombre_tanque
            """, (empresa, sede))
            rows = cur.fetchall() or []
            
            tanques = []
            for r in rows:
                # Lectura Híbrida (Dict o Tupla) para tanques
                num = r.get("numero") if isinstance(r, dict) else r[0]
                cap = r.get("capacidad") if isinstance(r, dict) else r[1]
                tanques.append({
                    "numero": num or "",
                    "capacidad": float(cap or 0),
                    "etiqueta": num or ""
                })

            # 2. NUEVO: Verificar si hay lote ACTIVO en esta sede
            cur.execute("""
                SELECT lote FROM cardex_glp 
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO' 
                LIMIT 1
            """, (empresa, sede))
            row_lote = cur.fetchone()
            
            lote_activo = False
            info_lote = ""
            
            if row_lote:
                lote_activo = True
                # Lectura Híbrida para el lote
                if isinstance(row_lote, dict):
                    info_lote = row_lote.get('lote', '')
                else:
                    info_lote = row_lote[0]

        return jsonify({
            "success": True, 
            "tanques": tanques, 
            "lote_activo": lote_activo,
            "info_lote": info_lote
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "tanques": [], "message": f"Error: {e}"}), 500

# ======================
# Iniciar calefacción (CORREGIDA Y BLINDADA)
# ======================
@csrf.exempt
@bp_glp.route('/registrar_inicio', methods=['POST'])
@login_required_custom
def registrar_inicio_calefaccion():
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"success": False, "message": "Error leyendo JSON"}), 400

    if not data:
        return jsonify({"success": False, "message": "No se recibió JSON válido"}), 400

    usuario    = session.get('nombre') or ''
    empresa    = session.get('empresa') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))
    op_id      = data.get('op_id')

    # 1. BLINDAJE INICIAL CENTRALIZADO
    check = _verificar_idempotencia(op_id, "inicio_calefaccion", ubicacion)
    if check: return check

    pollitos = data.get('pollitos')
    tanques  = data.get('tanques', []) or []

    try:
        with mysql.connection.cursor() as cur:
            # 2. Validar si ya hay lote activo
            cur.execute("SELECT COUNT(*) FROM cardex_glp WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'", (empresa, ubicacion))
            if cur.fetchone()[0] > 0:
                return jsonify({"success": False, "message": "⛔ YA EXISTE UN LOTE ACTIVO."})

            fecha = datetime.now().date()
            lote_id = f"{fecha.strftime('%Y%m%d')}_{ubicacion.replace(' ', '')}"

            # 3. Insertar Registro
            cur.execute("""
                INSERT INTO cardex_glp (fecha, empresa, id_empresa, ubicacion, lote, estatus_lote, operacion, tipo, clase, pollitos, registro, dias_operacion, op_id)
                VALUES (%s, %s, %s, %s, %s, 'ACTIVO', 'inicio_calefaccion', 'manual', 'saldo inicial', %s, %s, 1, %s)
            """, (fecha, empresa, id_empresa, ubicacion, lote_id, pollitos, usuario, op_id))
            
            id_operacion = cur.lastrowid
            carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)

            saldo_kg = 0.0
            densidad = 2.0
            tanques_para_pedido = [] 

            # 4. Procesar Tanques (Con corrección tk-tk)
            for tk in tanques:
                num_raw = str(tk.get("numero", ""))
                match = _re.search(r'\d+', num_raw)
                num = match.group() if match else num_raw.upper().replace("TK-", "").replace("TK", "").strip()

                if not num: continue

                try: nivel = float(tk.get("nivel", 0))
                except: nivel = 0.0
                try: cap = float(tk.get("capacidad", 250))
                except: cap = 250.0
                
                col_n = f"nivel tk-{num}"
                col_c = f"capacidad tk-{num}"
                col_t = f"testigo nivel tk-{num}"
                
                ruta_foto = None
                if tk.get("testigo"):
                    ruta_foto = _guardar_testigo(tk.get("testigo"), carpeta, f"tk{num}_{id_operacion}")

                query_tk = f"UPDATE cardex_glp SET `{col_n}`=%s, `{col_c}`=%s"
                params_tk = [nivel, cap]
                
                if ruta_foto:
                    query_tk += f", `{col_t}`=%s"
                    params_tk.append(ruta_foto)
                
                query_tk += " WHERE id=%s"
                params_tk.append(id_operacion)
                cur.execute(query_tk, params_tk)
                
                saldo_kg += (nivel/100.0) * cap * densidad

                if nivel < 78.0:
                    tanques_para_pedido.append({"numero": num, "nivel": nivel})

            proveedor = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
            cur.execute("UPDATE cardex_glp SET saldo_estimado_kg=%s, proveedor=%s WHERE id=%s", (saldo_kg, proveedor, id_operacion))

            # 5. Generación de Pedido (Dentro de try/except para no romper flujo si falla el mail)
            pedido_info = None
            if tanques_para_pedido and proveedor:
                try:
                    codigo = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor, cur)
                    
                    cur.execute("""
                        UPDATE pedidos_gas_glp 
                        SET nivel_solicitado=80, dias_extra=0, estatus_flujo='pendiente_envio_auto'
                        WHERE codigo_pedido=%s
                    """, (codigo,))
                    
                    cur.execute("UPDATE cardex_glp SET codigo_pedido=%s WHERE id=%s", (codigo, id_operacion))
                    
                    # Intentar enviar correo (si falla, solo loguea error)
                    try:
                        enviado = _enviar_alerta_pedido_inicio(empresa, ubicacion, lote_id, proveedor, tanques_para_pedido, codigo)
                    except Exception as e_mail:
                        print(f"⚠️ Error envío correo inicio: {e_mail}")
                        enviado = False
                    
                    pedido_info = {"generado": True, "codigo": codigo, "proveedor": proveedor, "enviado": enviado}
                    
                    if enviado:
                        cur.execute("UPDATE pedidos_gas_glp SET estatus_flujo='enviado_auto' WHERE codigo_pedido=%s", (codigo,))
                        
                except Exception as ep:
                    print(f"⚠️ Error generando pedido automático: {ep}")
                    pedido_info = {"generado": False, "error": str(ep)}

            # =======================================================
            # 🔊 AVISO A LA BITÁCORA
            # =======================================================
            try:
                registrar_auditoria(id_empresa, empresa, "GLP", usuario, "Inicio Calefacción", f"Sede: {ubicacion}. Lote: {lote_id}. Población: {pollitos}.", "INFO")
            except Exception as e:
                print("Error audit:", e)
            # =======================================================

            mysql.connection.commit()

        # 6. Preparar Respuesta Completa (Esto arregla el UI Roto)
        mensaje = f"Inicio registrado correctamente."
        if pedido_info and pedido_info.get("generado"):
            mensaje += f" ✅ Se generó pedido automático ({pedido_info['codigo']})."

        # IMPORTANTE: Aquí devolvemos TODA la data que el frontend espera para pintar el recibo
        resumen = {
            "operacion": "inicio_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "pollitos": pollitos,
            "saldo_estimado_kg": round(saldo_kg, 2),
            "tanques": _resumen_tanques([
                {"numero": t.get("numero"), "nivel": t.get("nivel"), "capacidad": t.get("capacidad")}
                for t in tanques
            ]),
            "pedido_automatico": pedido_info,
            "requiere_gas": False,
            "dias_operacion": 1,
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except IntegrityError as e:
        # 2. BLINDAJE FINAL (SI PASA LA CONDICIÓN DE CARRERA)
        return _manejar_error_idempotencia(e, "inicio_calefaccion", ubicacion)

    except Exception as e:
        mysql.connection.rollback()
        print("Error registro inicio:", e)
        return jsonify({"success": False, "message": str(e)})
    
# ======================
# Registrar tanqueo (VERSION FINAL: PROD + BLINDAJE + PRECIOS CORREGIDOS)
# ======================
@csrf.exempt
@bp_glp.route('/registrar_tanqueo', methods=['POST'])
@login_required_custom
def registrar_tanqueo():
    # --- BLINDAJE 1: Captura de errores globales ---
    try:
        try:
            data = request.get_json(force=True) or {}
        except Exception:
            return jsonify({"success": False, "message": "JSON inválido recibido."}), 400

        op_id = (data or {}).get("op_id")
        ubicacion  = _normalize_sede(data.get('sede'))
        
        if not op_id:
            return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

        # 1. BLINDAJE INICIAL CENTRALIZADO
        check = _verificar_idempotencia(op_id, "tanqueo", ubicacion)
        if check: return check

        empresa    = session.get('empresa') or ''
        usuario    = session.get('nombre') or ''
        id_empresa = session.get('empresa_id') or 0
        tanques    = data.get('tanques', []) or []

        if not empresa or not ubicacion:
            return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400
        if not tanques:
            return jsonify({"success": False, "message": "No se recibieron tanques para el tanqueo."}), 400

        # --- INICIO DE TRANSACCIÓN ---
        with mysql.connection.cursor() as cur:
            # 1. Buscar Lote Activo
            cur.execute("""
                SELECT lote
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC, id DESC LIMIT 1
            """, (empresa, ubicacion))

            row_raw = cur.fetchone()
            lote_id = None
            if row_raw:
                lote_id = row_raw.get("lote") if isinstance(row_raw, dict) else row_raw[0]

            if not lote_id:
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

            fecha = datetime.now().date()
            
            # 2. Insertar Registro Base
            columnas = [
                "fecha","empresa","id_empresa","ubicacion",
                "lote","estatus_lote","operacion","tipo","clase",
                "registro","op_id"
            ]
            valores  = [
                fecha, empresa, id_empresa, ubicacion,
                lote_id, 'ACTIVO', "tanqueo", "manual", "ingreso",
                usuario, op_id
            ]
            
            placeholders = ', '.join(['%s'] * len(columnas))
            col_names = ', '.join(columnas)
            
            cur.execute(f"INSERT INTO cardex_glp ({col_names}) VALUES ({placeholders})", valores)
            id_operacion = cur.lastrowid

            carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)

            set_cols = [] # <--- CORRECCION LISTAS VACIAS
            set_vals = [] # <--- CORRECCION LISTAS VACIAS
            
            densidad_estimada = 2.0
            saldo_estimado_kg = 0.0

            densidades_registradas = []
            masas_esperadas = []
            masas_facturadas = []
            errores_tanqueo = []

            # 3. Procesar Tanques
            for tk in tanques:
                num_str = str(tk.get("numero", ""))
                match = _re.search(r'\d+', num_str)
                num = match.group() if match else ""
                if not num: continue

                cap = float(tk.get("capacidad", 0) or 0.0)
                nivel_ini = float(tk.get("nivel_inicial", 0) or 0.0)
                nivel_fin = float(tk.get("nivel_final", 0) or 0.0)

                # Validación Lógica de Negocio: EL NIVEL DEBE SUBIR
                if (nivel_fin - nivel_ini) < -0.1:
                     errores_tanqueo.append(f"• TK-{num}: Inicio {nivel_ini}% -> Fin {nivel_fin}% (BAJÓ).")

                foto_ini = tk.get("foto_nivel_inicial")
                foto_fin = tk.get("foto_nivel_final")
                foto_bau = tk.get("foto_baucher")

                set_cols.append(f"`nivel tk-{num}`=%s");     set_vals.append(nivel_ini)
                if foto_ini:
                    try:
                        ruta_ini = _guardar_testigo(foto_ini, carpeta, f"tk{num}_ini_{id_operacion}")
                        set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta_ini)
                    except: pass

                set_cols.append(f"`nivelfinal tk-{num}`=%s");     set_vals.append(nivel_fin)
                if foto_fin:
                    try:
                        ruta_fin = _guardar_testigo(foto_fin, carpeta, f"tk{num}_fin_{id_operacion}")
                        set_cols.append(f"`testigo nivelfinal tk-{num}`=%s"); set_vals.append(ruta_fin)
                    except: pass

                set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(cap)

                if foto_bau:
                    try:
                        ruta_baucher = _guardar_testigo(foto_bau, carpeta, f"tk{num}_baucher_{id_operacion}")
                        col_baucher = f"testigo_baucher_tk_{num}"
                        set_cols.append(f"`{col_baucher}`=%s"); set_vals.append(ruta_baucher)
                    except: pass

                saldo_estimado_kg += densidad_estimada * cap * (nivel_fin/100.0)

                densidad_sum = float(tk.get("densidad_suministrada") or 0.0)
                if densidad_sum > 0: densidades_registradas.append(densidad_sum)

                kg_sumin = float(tk.get("kg_suministrados") or 0.0)
                masas_facturadas.append(kg_sumin) # Siempre sumamos lo facturado

                delta_nivel = nivel_fin - nivel_ini
                masa_esp_tk = 0.0
                if densidad_sum > 0 and cap > 0 and delta_nivel > 0:
                    masa_esp_tk = densidad_sum * cap * (delta_nivel/100.0)
                
                if masa_esp_tk > 0: masas_esperadas.append(masa_esp_tk)

            # Abortar transacción si hay errores lógicos (Nivel bajó)
            if errores_tanqueo:
                mensaje_final = "⛔ ERRORES LÓGICOS:\n" + "\n".join(errores_tanqueo) + "\n\nEn un tanqueo el nivel debe SUBIR."
                raise ValueError(mensaje_final)

            if set_cols:
                cur.execute(f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s", set_vals + [id_operacion])

            saldo_estimado_gal = saldo_estimado_kg/densidad_estimada if densidad_estimada else 0.0
            cur.execute("UPDATE cardex_glp SET saldo_estimado_kg=%s, saldo_estimado_galones=%s WHERE id=%s", (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            # 4. Cálculos de Consumo (Protegidos)
            consumo_kg, kg_pollito, pollitos = 0.0, 0.0, 0
            try:
                tks_consumo = [{"numero": t.get("numero"), "capacidad": t.get("capacidad"), "nivel": t.get("nivel_inicial")} for t in tanques]
                consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(cur, empresa, ubicacion, lote_id, id_operacion, tks_consumo)
            except Exception as e_calc:
                print(f"⚠️ Error cálculo consumo (no crítico): {e_calc}")

            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            dens_prom = sum(densidades_registradas)/len(densidades_registradas) if densidades_registradas else 0.0
            masa_esperada_total = sum(masas_esperadas) if masas_esperadas else 0.0
            masa_facturada_total = sum(masas_facturadas) if masas_facturadas else 0.0

            desvio_total = 0.0
            if masa_esperada_total > 0:
                desvio_total = ((masa_facturada_total - masa_esperada_total) / masa_esperada_total) * 100.0

            # 5. BUSCAR PROVEEDOR Y PRECIOS (MEJORADO CON TRIM)
            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
            print(f"🔍 Proveedor encontrado: '{proveedor_principal}'") # Debug en consola

            precio_unitario = 0.0
            precio_total = 0.0

            if proveedor_principal:
                # Usamos TRIM para evitar fallos por espacios
                cur.execute("SELECT precio FROM proveedores WHERE TRIM(UPPER(proveedor))=TRIM(UPPER(%s)) LIMIT 1", (proveedor_principal,))
                prow_raw = cur.fetchone()
                if prow_raw:
                    if isinstance(prow_raw, dict): precio_unitario = float(prow_raw.get("precio") or 0.0)
                    else: precio_unitario = float(prow_raw[0] or 0.0)
                else:
                    print(f"⚠️ No se encontró precio para el proveedor: {proveedor_principal}")

            precio_total = precio_unitario * float(masa_facturada_total or 0.0)

            cur.execute("""
                UPDATE cardex_glp
                   SET densidad_suministrada=%s, masa_esperada_kg=%s, masa_kg_facturada=%s, porcentaje_diferencia=%s, 
                       proveedor=%s, precio_unitario=%s, precio_total=%s
                 WHERE id=%s
            """, (
                round(dens_prom,3) if dens_prom else 0.0,
                round(masa_esperada_total,2),
                round(masa_facturada_total,2),
                round(desvio_total,2),
                proveedor_principal,
                round(precio_unitario, 6),
                round(precio_total, 2),
                id_operacion
            ))

            # 6. Auditoría y Alertas
            # Alerta Desviación > 8%
            if masa_esperada_total > 0 and desvio_total > 8.0 and proveedor_principal:
                try:
                    alerta_enviada = _enviar_alerta_desviacion_tanqueo(
                        empresa=empresa, ubicacion=ubicacion, lote_id=lote_id,
                        proveedor_principal=proveedor_principal, op_id=op_id,
                        masa_esperada_total=masa_esperada_total, masa_facturada_total=masa_facturada_total,
                        desvio_total_pct=desvio_total, dens_prom=dens_prom, tanques=tanques
                    )
                    if alerta_enviada:
                        registrar_auditoria(id_empresa, empresa, "GLP", "Sistema", "⚠️ Alerta Desviación", f"Desviación {round(desvio_total, 2)}% en {ubicacion}.", "ALERTA")
                except Exception as e_mail:
                    print(f"⛔ Error envío alerta desviación: {e_mail}")

            # Auditoría General del Tanqueo (Siempre se registra)
            registrar_auditoria(
                empresa_id=id_empresa,
                empresa_nombre=empresa,
                modulo="GLP",
                usuario=usuario,
                accion="Tanqueo Registrado",
                detalle=f"Sede: {ubicacion}. Carga: {round(masa_facturada_total, 2)} kg. Valor: ${precio_total:,.0f}",
                nivel="INFO"
            )

            mysql.connection.commit()

        # Respuesta Exitosa
        mensaje = "Tanqueo registrado correctamente."
        if masa_esperada_total > 0:
            mensaje += f" Facturado: {round(masa_facturada_total,2)} kg."
            if desvio_total > 8.0: mensaje += " ⚠️ Desviación detectada."
        
        valor_formato = f"{precio_total:,.0f}".replace(",", ".")
        mensaje += f" Valor: ${valor_formato}."

        resumen = {
            "operacion": "tanqueo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques([{"numero": t.get("numero"), "nivel": t.get("nivel_final"), "capacidad": t.get("capacidad")} for t in tanques]),
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg,2),
            "kg_pollito": round(kg_pollito,6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "masa_esperada_kg": round(masa_esperada_total,2),
            "masa_kg_facturada": round(masa_facturada_total,2),
            "porcentaje_diferencia": round(desvio_total,2),
            "proveedor": proveedor_principal,
            "precio_unitario": round(float(precio_unitario or 0.0), 6),
            "precio_total": round(float(precio_total or 0.0), 2),
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except IntegrityError as e:
        # 2. BLINDAJE FINAL (SI PASA LA CONDICIÓN DE CARRERA)
        return _manejar_error_idempotencia(e, "tanqueo", ubicacion)

    except ValueError as e:
        mysql.connection.rollback()
        # Registramos el intento fallido en auditoría también
        try: registrar_auditoria(id_empresa, empresa, "GLP", usuario, "⛔ Error Lógico Tanqueo", str(e), "WARNING")
        except: pass
        return jsonify({"success": False, "message": str(e)})

    except Exception as e:
        # --- BLINDAJE FINAL: REPORTE DE ERROR REAL ---
        mysql.connection.rollback()
        print("❌ ERROR CRÍTICO EN REGISTRAR TANQUEO:")
        traceback.print_exc()
        try: registrar_auditoria(id_empresa, empresa, "GLP", usuario, "💀 Error Sistema Tanqueo", str(e), "CRITICAL")
        except: pass
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})


# ======================
# Registrar consumo (VERSIÓN ATÓMICA Y BLINDADA)
# ======================

@csrf.exempt
@bp_glp.route('/registrar_consumo', methods=['POST'])
@login_required_custom
def registrar_consumo():
    try:
        try:
            data = request.get_json(force=True) or {}
        except Exception:
            return jsonify({"success": False, "message": "JSON inválido recibido."}), 400

        op_id = (data or {}).get("op_id")
        if not op_id:
            return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

        # BLINDAJE INICIAL
        ubicacion = _normalize_sede(data.get('sede'))
        check = _verificar_idempotencia(op_id, "consumo", ubicacion)
        if check: return check

        empresa    = session.get('empresa') or ''
        usuario    = session.get('nombre') or ''
        id_empresa = session.get('empresa_id') or 0
        tanques    = data.get('tanques', []) or []

        if not empresa or not ubicacion:
            return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400
        if not tanques:
            return jsonify({"success": False, "message": "No se recibieron tanques para el registro."}), 400

        lote_id = None
        fecha = datetime.now().date()
        saldo_estimado_kg = 0.0
        saldo_estimado_gal = 0.0
        consumo_kg = 0.0
        kg_pollito = 0.0
        pollitos = 0
        dias_operacion = 0
        proveedor_principal = None

        # --- NUEVO: SISTEMA DE REINTENTOS PARA DEADLOCKS (Igual que en cierre de lote) ---
        max_intentos = 3
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    # 1. Buscar Lote Activo
                    cur.execute("""
                        SELECT lote FROM cardex_glp
                        WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                        ORDER BY fecha DESC, id DESC LIMIT 1
                    """, (empresa, ubicacion))

                    row_raw = cur.fetchone()
                    if row_raw:
                        lote_id = row_raw.get("lote") if isinstance(row_raw, dict) else row_raw[0]

                    if not lote_id:
                        return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

                    # 2. Insertar Registro Base
                    columnas = ["fecha","empresa","id_empresa","ubicacion","lote","estatus_lote","operacion","tipo","clase","registro","op_id"]
                    valores  = [fecha, empresa, id_empresa, ubicacion, lote_id, 'ACTIVO', "consumo", "manual", "egreso", usuario, op_id]
                    
                    cur.execute(f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s']*len(columnas))})", valores)
                    id_operacion = cur.lastrowid

                    carpeta = os.path.join("testigos", empresa.replace(" ","_"), lote_id)
                    set_cols, set_vals = [], []
                    densidad = 2.0
                    saldo_estimado_kg = 0.0
                    sum_nivel_cap = 0.0
                    sum_cap = 0.0

                    # 3. Procesar Tanques
                    for tk in tanques:
                        num_str = str(tk.get("numero", ""))
                        match = _re.search(r'\d+', num_str)
                        num = match.group() if match else ""
                        if not num: continue

                        try: val = float(tk.get("nivel", 0) or 0)
                        except: val = 0.0
                        try: cap = float(tk.get("capacidad", 0) or 0)
                        except: cap = 0.0
                        tst = tk.get("testigo")

                        set_cols.append(f"`nivel tk-{num}`=%s"); set_vals.append(val)
                        set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(cap)

                        if tst:
                            ruta = _guardar_testigo(tst, carpeta, f"tk{num}_consumo_{id_operacion}")
                            if ruta: set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta)

                        saldo_estimado_kg += densidad * cap * (val/100.0)
                        sum_nivel_cap += (val * cap)
                        sum_cap += cap

                    if set_cols:
                        cur.execute(f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s", set_vals + [id_operacion])

                    saldo_estimado_gal = saldo_estimado_kg/densidad if densidad else 0.0
                    cur.execute("UPDATE cardex_glp SET saldo_estimado_kg=%s, saldo_estimado_galones=%s WHERE id=%s", (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

                    # 4. Cálculos
                    consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(cur, empresa, ubicacion, lote_id, id_operacion, tanques)
                    dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)
                    proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
                    cur.execute("UPDATE cardex_glp SET proveedor=%s WHERE id=%s", (proveedor_principal, id_operacion))

                    # 5. SOLICITUD DE GAS
                    solicitud_gas = data.get("solicitud_gas")
                    analisis_riesgo = {"requiere_gas": False, "razon": "", "codigo": None}

                    if solicitud_gas:
                        nivel_solicitado = float(solicitud_gas.get("nivel", 60))
                        dias_extra = int(solicitud_gas.get("dias_extra", 0))
                        
                        codigo = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor_principal, cur)
                        
                        try:
                            cur.execute("""
                                UPDATE pedidos_gas_glp 
                                SET nivel_solicitado=%s, dias_extra=%s, estatus_flujo='pendiente_aprobacion', comentarios='Solicitud generada automáticamente'
                                WHERE codigo_pedido=%s
                            """, (nivel_solicitado, dias_extra, codigo))
                            
                            if cur.rowcount == 0:
                                 cur.execute("""
                                    INSERT INTO pedidos_gas_glp (codigo_pedido, fecha_solicitud, cliente, ubicacion, proveedor, nivel_solicitado, estatus_flujo, comentarios, lote, dias_extra) 
                                    VALUES (%s, NOW(), %s, %s, %s, %s, 'pendiente_aprobacion', 'Solicitud automática', %s, %s)
                                """, (codigo, empresa, ubicacion, proveedor_principal, nivel_solicitado, lote_id, dias_extra))
                        except IntegrityError:
                            pass
                        
                        cur.execute("UPDATE cardex_glp SET codigo_pedido=%s WHERE id=%s", (codigo, id_operacion))
                        analisis_riesgo.update({"requiere_gas": True, "razon": f"Solicitud {codigo} enviada.", "codigo": codigo})

                        try:
                            nivel_notif = round(sum_nivel_cap/sum_cap if sum_cap > 0 else 0, 1)
                            _enviar_alerta_telegram_oficial(ubicacion, usuario, nivel_notif, codigo)
                            _enviar_alerta_webmaster_nueva_solicitud(empresa, ubicacion, usuario, nivel_notif, codigo)
                        except Exception: pass

                    # Auditoría
                    registrar_auditoria(id_empresa, empresa, "GLP", usuario, "Consumo Registrado", f"Sede: {ubicacion}. Consumo: {round(consumo_kg, 2)} kg.", "INFO")

                    mysql.connection.commit()
                    break # ÉXITO, SALIMOS DEL BUCLE

            except OperationalError as e:
                if e.args[0] == 1213: # DEADLOCK DETECTADO
                    print(f"⚠️ Deadlock en Consumo (Intento {intento+1})... reintentando.")
                    mysql.connection.rollback()
                    time.sleep(0.2)
                    if intento == max_intentos - 1: raise e
                else: raise e

        # 6. Preparar Respuesta
        resumen = {
            "operacion": "consumo", "sede": ubicacion, "lote": lote_id, "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques(tanques), "saldo_estimado_kg": round(saldo_estimado_kg,2), "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg,2), "kg_pollito": round(kg_pollito,6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos, "proveedor": proveedor_principal, "op_id": op_id,
            "requiere_gas": analisis_riesgo["requiere_gas"], "razon_alerta": analisis_riesgo["razon"], "codigo_pedido": analisis_riesgo["codigo"]
        }
        return jsonify({"success": True, "message": "Operación guardada exitosamente.", "resumen": resumen})

    except IntegrityError as e: return _manejar_error_idempotencia(e, "consumo", ubicacion)
    except ValueError as e: return jsonify({"success": False, "message": str(e)})
    except Exception as e: return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})

# ======================
# Finalizar calefacción (VERSION PRODUCCIÓN BLINDADA)
# ======================
@csrf.exempt
@bp_glp.route('/finalizar_calefaccion_batch', methods=['POST'])
@login_required_custom
def finalizar_calefaccion_batch():
    try:
        try:
            data = request.get_json(force=True) or {}
        except Exception:
            return jsonify({"success": False, "message": "JSON inválido recibido."}), 400

        op_id = (data or {}).get("op_id")
        ubicacion = _normalize_sede(data.get('sede'))
        
        if not op_id:
            return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

        check = _verificar_idempotencia(op_id, "finalizar_calefaccion", ubicacion)
        if check: return check

        empresa    = session.get('empresa') or ''
        usuario    = session.get('nombre') or ''
        id_empresa = session.get('empresa_id') or 0
        tanques    = data.get('tanques', []) or []

        # --- SISTEMA DE REINTENTOS PARA DEADLOCKS ---
        max_intentos = 3
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT lote FROM cardex_glp
                        WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                        ORDER BY fecha DESC, id DESC LIMIT 1
                    """, (empresa, ubicacion))

                    row_raw = cur.fetchone()
                    lote_id = row_raw.get("lote") if row_raw and isinstance(row_raw, dict) else (row_raw[0] if row_raw else None)

                    if not lote_id:
                        return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

                    fecha = datetime.now().date()
                    columnas = ["fecha","empresa","id_empresa","ubicacion","lote","estatus_lote","operacion","tipo","clase","registro","op_id"]
                    valores = [fecha, empresa, id_empresa, ubicacion, lote_id, 'ACTIVO', "finalizar_calefaccion", "manual", "saldo final", usuario, op_id]
                    
                    # INSERT BASE (Donde ocurría el deadlock)
                    cur.execute(f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s']*len(columnas))})", valores)
                    id_operacion = cur.lastrowid

                    carpeta = os.path.join("testigos", empresa.replace(" ","_"), lote_id)
                    set_cols, set_vals = [], []
                    densidad, saldo_estimado_kg = 2.0, 0.0

                    for tk in tanques:
                        num_match = _re.search(r'\d+', str(tk.get("numero", "")))
                        num = num_match.group() if num_match else ""
                        if not num: continue

                        try: niv, cap = float(tk.get("nivel", 0)), float(tk.get("capacidad", 0))
                        except: niv, cap = 0.0, 0.0

                        set_cols.append(f"`nivel tk-{num}`=%s"); set_vals.append(niv)
                        set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(cap)

                        if tk.get("testigo"):
                            ruta = _guardar_testigo(tk.get("testigo"), carpeta, f"tk{num}_final_{id_operacion}")
                            if ruta: set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta)

                        saldo_estimado_kg += densidad * cap * (niv/100.0)

                    if set_cols:
                        cur.execute(f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s", set_vals + [id_operacion])

                    cur.execute("UPDATE cardex_glp SET saldo_estimado_kg=%s, saldo_estimado_galones=%s WHERE id=%s", (round(saldo_estimado_kg,2), round(saldo_estimado_kg/densidad,2), id_operacion))

                    try: 
                        consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(cur, empresa, ubicacion, lote_id, id_operacion, tanques)
                    except: 
                        consumo_kg, kg_pollito, pollitos = 0.0, 0.0, 0

                    dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)
                    cur.execute("UPDATE cardex_glp SET proveedor=%s WHERE id=%s", (_buscar_proveedor_principal(cur, empresa, ubicacion, tanques), id_operacion))

                    # CIERRE DE LOTE
                    cur.execute("UPDATE cardex_glp SET estatus_lote='INACTIVO' WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND lote=%s", (empresa, ubicacion, lote_id))

                    # =======================================================
                    # 🔊 AVISO A LA BITÁCORA
                    # =======================================================
                    try:
                        registrar_auditoria(id_empresa, empresa, "GLP", usuario, "Fin Calefacción", f"Sede: {ubicacion}. Lote cerrado: {lote_id}.", "INFO")
                    except Exception as e:
                        print("Error audit:", e)
                    # =======================================================

                    mysql.connection.commit()
                    break # Éxito: Salir del bucle

            except OperationalError as e:
                if e.args[0] == 1213: # Si es Deadlock, reintentamos
                    print(f"⚠️ Deadlock en Cierre (Intento {intento+1})...")
                    mysql.connection.rollback()
                    time.sleep(0.2)
                    if intento == max_intentos - 1: raise e
                else: raise e

        return jsonify({"success": True, "message": f"Calefacción cerrada correctamente.", "resumen": {"operacion": "finalizar_calefaccion", "sede": ubicacion, "lote": lote_id, "dias": dias_operacion, "kg": round(consumo_kg,2)}})

    except IntegrityError as e:
        return _manejar_error_idempotencia(e, "finalizar_calefaccion", ubicacion)
    except Exception as e:
        mysql.connection.rollback()
        print("❌ ERROR CRÍTICO FINAL:", e)
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})
    

               
# --- FUNCIÓN DE CONSULTA DE PEDIDOS ---
@bp_glp.route('/consultar_pedidos_pendientes', methods=['POST'])
@login_required_custom
def consultar_pedidos_pendientes():
    empresa_nombre = session.get('empresa')
    if not empresa_nombre:
        return jsonify({"success": False, "message": "Falta el nombre de la empresa en la sesión."}), 400

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            SELECT 
                p.id, 
                p.codigo_pedido, 
                p.fecha_registro,  
                p.proveedor,       
                p.ubicacion,       
                p.lote 
            FROM pedidos_gas_glp p
            WHERE p.cliente = %s AND p.estatus = 'generado'
            ORDER BY p.fecha_registro DESC
        """, (empresa_nombre,))
        
        pedidos = cur.fetchall()
        cur.close()
        
        pedidos_listos = []
        for pedido in pedidos:
            # --- BLINDAJE ---
            if isinstance(pedido, dict):
                p_id = pedido['id']
                p_cod = pedido['codigo_pedido']
                p_fec = pedido['fecha_registro']
                p_prov = pedido.get('proveedor')
                p_ubi = pedido['ubicacion']
                p_lot = pedido['lote']
            else:
                p_id = pedido[0]
                p_cod = pedido[1]
                p_fec = pedido[2]
                p_prov = pedido[3]
                p_ubi = pedido[4]
                p_lot = pedido[5]
            # ----------------

            pedidos_listos.append({
                "id": p_id,
                "codigo": p_cod,
                "fecha": p_fec.strftime('%Y-%m-%d %H:%M') if p_fec else 'N/A',
                "proveedor": p_prov if p_prov else 'N/A',
                "ubicacion": p_ubi,
                "lote": p_lot
            })

        return jsonify({"success": True, "pedidos": pedidos_listos})

    except Exception:
        app.logger.error(f"Error al consultar pedidos pendientes GLP: {traceback.format_exc()}")
        return jsonify({"success": False, "message": "Error interno al consultar la base de datos."}), 500


# --- FUNCIÓN DE VALIDACIÓN ---
@bp_glp.route('/validar_pedido', methods=['POST'])
@login_required_custom
@csrf.exempt 
def validar_pedido():
    data = request.json
    pedido_id = data.get('pedido_id')
    numero_factura = data.get('numero_factura')
    validador_cedula = session.get('cedula') 
    
    if not all([pedido_id, numero_factura, validador_cedula]):
        return jsonify({"success": False, "message": "Faltan datos de Pedido, Número de Factura o Cédula del Validador."}), 400
        
    try:
        cur = mysql.connection.cursor()
        update_query = """
            UPDATE pedidos_gas_glp
            SET estatus = 'validado', 
                fecha_validacion = NOW(), 
                validador = %s,             
                numero_factura = %s         
            WHERE id = %s AND estatus = 'generado'
        """
        cur.execute(update_query, (validador_cedula, numero_factura, pedido_id))
        
        if cur.rowcount == 0:
            mysql.connection.rollback()
            cur.close()
            return jsonify({"success": False, "message": "El pedido no existe, ya fue validado, o no es un pedido 'generado'."}), 404
        
        mysql.connection.commit()
        cur.close()
        return jsonify({"success": True, "message": f"Pedido {pedido_id} validado correctamente. Número de factura: {numero_factura}."})

    except Exception:
        mysql.connection.rollback()
        app.logger.error(f"Error al validar pedido GLP: {traceback.format_exc()}")
        return jsonify({"success": False, "message": "Error interno al actualizar el pedido."}), 500

# ==========================================
# RUTA PARA VER LA PÁGINA DE FACTURAS (HTML)
# ==========================================
@bp_glp.route('/facturas', methods=['GET'])
@login_required_custom
def ver_facturas_glp():
    empresa = session.get('empresa')
    nombre = session.get('nombre')
    cedula = session.get('cedula') or session.get('user_id') or 'N/D'

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            SELECT 
                p.id, 
                p.codigo_pedido, 
                p.fecha_registro,  
                p.proveedor,       
                p.ubicacion 
            FROM pedidos_gas_glp p
            WHERE p.cliente = %s AND p.estatus = 'generado'
            ORDER BY p.fecha_registro DESC
        """, (empresa,))
        rows = cur.fetchall()
        cur.close()

        pedidos_listos = []
        for row in rows:
            # --- BLINDAJE ---
            if isinstance(row, dict):
                p_id = row['id']
                p_cod = row['codigo_pedido']
                p_fec = row['fecha_registro']
                p_prov = row.get('proveedor')
                p_ubi = row['ubicacion']
            else:
                p_id = row[0]
                p_cod = row[1]
                p_fec = row[2]
                p_prov = row[3]
                p_ubi = row[4]
            # ----------------

            pedidos_listos.append({
                "id": p_id,
                "codigo_pedido": p_cod,
                "fecha_generacion": p_fec.strftime('%Y-%m-%d %H:%M') if p_fec else 'N/A',
                "proveedor": p_prov,
                "ubicacion": p_ubi
            })

        return render_template('facturas_glp.html', 
                               pedidos=pedidos_listos, 
                               empresa=empresa, 
                               nombre=nombre, 
                               cedula=cedula)

    except Exception:
        app.logger.error(f"Error cargando vista facturas: {traceback.format_exc()}")
        return "Error cargando la página de facturas.", 500
    
    
# ==========================================
# NUEVOS ENDPOINTS: NEGOCIACIÓN Y ADMIN
# ==========================================

@csrf.exempt
@bp_glp.route('/solicitar_pedido_manual', methods=['POST'])
@login_required_custom
def solicitar_pedido_manual():
    # -------------------------------------------------------------------------
    # 1. VALIDACIÓN Y OBTENCIÓN DE DATOS
    # -------------------------------------------------------------------------
    data = request.get_json(force=True, silent=True) or {}
    
    op_id = data.get('op_id')
    nivel_solicitado = data.get('nivel_solicitado')
    dias_extra = data.get('dias_extra', 0)
    notas = data.get('comentarios', '')

    if not op_id:
        return jsonify({"success": False, "message": "Falta el ID de operación (op_id)."}), 400

    try:
        cur = mysql.connection.cursor()
        
        # ---------------------------------------------------------------------
        # 2. OBTENER CONTEXTO (Empresa, Ubicación, etc.)
        # ---------------------------------------------------------------------
        cur.execute("SELECT empresa, ubicacion, lote, proveedor FROM cardex_glp WHERE op_id=%s", (op_id,))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            return jsonify({"success": False, "message": "Operación no encontrada en Cardex."}), 404
            
        # Manejo seguro de tuplas o diccionarios (según tu configuración de MySQL)
        empresa = row['empresa'] if isinstance(row, dict) else row[0]
        ubicacion = row['ubicacion'] if isinstance(row, dict) else row[1]
        lote = row['lote'] if isinstance(row, dict) else row[2]
        proveedor = row['proveedor'] if isinstance(row, dict) else row[3]

        # ---------------------------------------------------------------------
        # 3. GENERAR CÓDIGO Y GUARDAR EN BASE DE DATOS
        # ---------------------------------------------------------------------
        codigo = _generar_codigo_pedido(empresa, lote, ubicacion, proveedor, cur)

        # A. Actualizar o Insertar en la tabla de pedidos
        cur.execute("""
            UPDATE pedidos_gas_glp 
            SET nivel_solicitado=%s, 
                dias_extra=%s, 
                estatus_flujo='pendiente_aprobacion', 
                comentarios=%s, 
                fecha_solicitud=NOW()
            WHERE codigo_pedido=%s
        """, (nivel_solicitado, dias_extra, notas, codigo))
        
        # Si no se actualizó nada (es un pedido nuevo), hacemos INSERT
        if cur.rowcount == 0:
             cur.execute("""
                INSERT INTO pedidos_gas_glp (
                    codigo_pedido, fecha_solicitud, cliente, ubicacion, 
                    proveedor, nivel_solicitado, estatus_flujo, comentarios, dias_extra
                ) VALUES (%s, NOW(), %s, %s, %s, %s, 'pendiente_aprobacion', %s, %s)
            """, (codigo, empresa, ubicacion, proveedor, nivel_solicitado, notas, dias_extra))

        # B. Vincular el pedido al registro actual del Cardex
        cur.execute("UPDATE cardex_glp SET codigo_pedido=%s WHERE op_id=%s", (codigo, op_id))
        
        mysql.connection.commit() # <--- ¡DATOS GUARDADOS CORRECTAMENTE!

        # ---------------------------------------------------------------------
        # 4. ZONA DE NOTIFICACIONES (EMAIL + TELEGRAM)
        # ---------------------------------------------------------------------
        try:
            # Obtener datos frescos para las alertas
            usuario_actual = session.get('nombre', 'Usuario App')
            
            # Consultamos el nivel real que tiene el tanque ahora mismo
            cur.execute("SELECT `nivel tk-1` FROM cardex_glp WHERE op_id=%s", (op_id,))
            res_nivel = cur.fetchone()
            nivel_reportado = res_nivel['nivel tk-1'] if res_nivel and isinstance(res_nivel, dict) else (res_nivel[0] if res_nivel else 0)

            # --- ALERTA 1: TELEGRAM (Inmediata) ---
            _enviar_alerta_telegram_oficial(
                ubicacion=ubicacion, 
                usuario=usuario_actual, 
                nivel=nivel_reportado, 
                codigo=codigo
            )
            print("Notificación Telegram enviada.")

            # --- ALERTA 2: EMAIL (Formal) ---
            # Nota: Llamamos a la función con los parámetros exactos que definiste
            _enviar_alerta_webmaster_nueva_solicitud(
                empresa=empresa, 
                ubicacion=ubicacion, 
                usuario=usuario_actual, 
                nivel_actual=nivel_reportado, 
                codigo_pedido=codigo
            )
            print("Notificación Email enviada.")

        except Exception as e_notify:
            # Si falla una notificación, NO cancelamos el pedido. Solo registramos el error.
            print(f"Advertencia: Alguna notificación falló: {e_notify}")
        
        # ---------------------------------------------------------------------
        
        cur.close()
        return jsonify({"success": True, "message": "Solicitud enviada a aprobación correctamente."})

    except Exception as e:
        mysql.connection.rollback()
        print(f"Error Crítico en solicitar_pedido_manual: {str(e)}")
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"}), 500
      
@csrf.exempt
@bp_glp.route('/admin/obtener_solicitudes_pendientes', methods=['GET'])
@login_required_custom
def admin_obtener_solicitudes():
    try:
        cur = mysql.connection.cursor()
        # SQL blindado contra errores de Collation y Modo Estricto
        sql = """
            SELECT 
                p.id, p.fecha_registro, p.cliente, p.ubicacion, p.lote, p.nivel_solicitado, p.dias_extra,
                (SELECT dias_operacion FROM cardex_glp WHERE lote = p.lote COLLATE utf8mb4_general_ci AND operacion IN ('consumo','inicio_calefaccion') ORDER BY id DESC LIMIT 1) as dias_operacion,
                (SELECT `nivel tk-1` FROM cardex_glp WHERE lote = p.lote COLLATE utf8mb4_general_ci AND operacion IN ('consumo','inicio_calefaccion') ORDER BY id DESC LIMIT 1) as `nivel tk-1`,
                (SELECT `testigo nivel tk-1` FROM cardex_glp WHERE lote = p.lote COLLATE utf8mb4_general_ci AND operacion IN ('consumo','inicio_calefaccion') ORDER BY id DESC LIMIT 1) as `testigo nivel tk-1`
            FROM pedidos_gas_glp p 
            WHERE p.estatus_flujo = 'pendiente_aprobacion' 
            ORDER BY p.fecha_registro DESC
        """
        cur.execute(sql)
        rows = cur.fetchall()
        items = []
        col_names = [d[0] for d in cur.description]
        
        for r in rows:
            rd = dict(zip(col_names, r)) if not isinstance(r, dict) else r
            tk_info = []
            if rd.get('nivel tk-1') is not None: 
                tk_info.append({"numero": "Ref", "nivel": rd.get('nivel tk-1'), "foto": rd.get('testigo nivel tk-1')})
            
            items.append({
                "id": rd.get('id'), 
                "fecha": str(rd.get('fecha_registro')), 
                "cliente": rd.get('cliente'), 
                "ubicacion": rd.get('ubicacion'), 
                "lote": rd.get('lote'), 
                "dias_operacion": rd.get('dias_operacion'), 
                "nivel_solicitado": float(rd.get('nivel_solicitado') or 0), 
                "dias_extra": rd.get('dias_extra'), 
                "tanques": tk_info
            })
        cur.close()
        return jsonify({"success": True, "items": items})
    except Exception as e: 
        print("❌ Error de lectura solicitudes:", e)
        return jsonify({"success": False, "message": str(e)})
# ==========================================
# RUTAS DE ADMIN Y APROBACIÓN (CORREGIDAS)
# ==========================================

@csrf.exempt
@bp_glp.route('/admin/analizar_proyeccion', methods=['POST'])
@login_required_custom
def admin_analizar_proyeccion():
    # Uso de force=True para evitar error 415
    data = request.get_json(force=True) or {}
    ped_id = data.get('id')
    
    try:
        cur = mysql.connection.cursor()
        # --- CAMBIO CRÍTICO: SE LEEN LOS DÍAS EXTRA ---
        cur.execute("SELECT lote, cliente, ubicacion, nivel_solicitado, dias_extra FROM pedidos_gas_glp WHERE id=%s", (ped_id,))
        head = cur.fetchone()
        
        if not head: 
            return jsonify({"success": False, "message": "Pedido no encontrado"})
            
        # Manejo híbrido tupla/dict
        if isinstance(head, dict):
            lote = head['lote']
            cliente = head['cliente']
            ubicacion = head['ubicacion']
            solicitado = float(head.get('nivel_solicitado') or 0)
            dias_extra = int(head.get('dias_extra') or 0)
        else:
            lote = head[0]
            cliente = head[1]
            ubicacion = head[2]
            solicitado = float(head[3] or 0)
            dias_extra = int(head[4] or 0)
        
        # Replicamos lógica de velocidad de consumo
        cur.execute("""
            SELECT fecha, `nivel tk-1` FROM cardex_glp 
            WHERE lote=%s ORDER BY fecha DESC LIMIT 5
        """, (lote,))
        rows_hist = cur.fetchall()
        tasa = 8.0 # Default
        if rows_hist and len(rows_hist) > 1:
            deltas = []
            for i in range(len(rows_hist)-1):
                # Extracción segura
                f1 = rows_hist[i]['fecha'] if isinstance(rows_hist[i], dict) else rows_hist[i][0]
                n1 = float((rows_hist[i]['nivel tk-1'] if isinstance(rows_hist[i], dict) else rows_hist[i][1]) or 0)
                
                f2 = rows_hist[i+1]['fecha'] if isinstance(rows_hist[i+1], dict) else rows_hist[i+1][0]
                n2 = float((rows_hist[i+1]['nivel tk-1'] if isinstance(rows_hist[i+1], dict) else rows_hist[i+1][1]) or 0)
                
                diff_days = (f1 - f2).days
                if diff_days > 0:
                    diff_niv = n2 - n1
                    if diff_niv > 0: deltas.append(diff_niv / diff_days)
            if deltas: tasa = sum(deltas)/len(deltas)

        # Datos Actuales
        cur.execute("SELECT `nivel tk-1`, dias_operacion, fecha FROM cardex_glp WHERE lote=%s ORDER BY id DESC LIMIT 1", (lote,))
        curr = cur.fetchone()
        
        niv_act = float(curr[0] or 0) if curr else 0
        dia_act = int(curr[1] or 0) if curr else 0
        fecha_ultima = curr[2] if curr else datetime.now().date()
        if isinstance(fecha_ultima, datetime): fecha_ultima = fecha_ultima.date()
        
        # PROYECCIÓN ESPEJO (ADMIN VE LO MISMO QUE OPERARIO)
        puntos_grafica = []
        fecha_sim = fecha_ultima
        nivel_sim = niv_act + solicitado
        if nivel_sim > 100: nivel_sim = 100
        
        # CICLO DINÁMICO
        ciclo_total = 15 + dias_extra
        dias_a_proyectar = (ciclo_total - dia_act) + 4 

        for d in range(1, dias_a_proyectar + 1):
            fecha_sim += timedelta(days=1)
            
            # Detectar si es día de bajo consumo (Festivo/Domingo) para aplanar la curva visualmente
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


@csrf.exempt
@bp_glp.route('/admin/aprobar_solicitud', methods=['POST'])
@login_required_custom
def admin_aprobar_solicitud():
    # Uso de force=True para evitar error 415
    data = request.get_json(force=True) or {}
    ped_id = data.get('id')
    nivel = data.get('nivel_aprobado')
    
    try:
        cur = mysql.connection.cursor()
        
        if nivel: 
            cur.execute("UPDATE pedidos_gas_glp SET nivel_solicitado=%s WHERE id=%s", (nivel, ped_id))
        else:
            cur.execute("SELECT nivel_solicitado FROM pedidos_gas_glp WHERE id=%s", (ped_id,))
            res = cur.fetchone()
            if res:
                nivel = res[0] if not isinstance(res, dict) else res.get('nivel_solicitado')
            
        cur.execute("UPDATE pedidos_gas_glp SET estatus_flujo='aprobado_webmaster' WHERE id=%s", (ped_id,))
        mysql.connection.commit()
        
        # Llamada a la función de correo
        env = _enviar_correo_aprobado_proveedor(ped_id, nivel)
        
        return jsonify({
            "success": True, 
            "message": "Aprobado." + (" Correo enviado." if env else " Error correo (revisar logs).")
        })
        
    except Exception as e: 
        return jsonify({"success": False, "message": str(e)})


def _enviar_correo_aprobado_proveedor(pedido_id, nivel_aprobado):
    """
    Envía correo de aprobación con DISEÑO PRO (Orden de Compra).
    """
    if not EMAIL_USER or not EMAIL_PASS:
        app.logger.warning(f"⚠️ GLP: No se envió correo para pedido {pedido_id}. Faltan credenciales.")
        return False

    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT cliente, ubicacion, lote, codigo_pedido, proveedor FROM pedidos_gas_glp WHERE id=%s", (pedido_id,))
        res = cur.fetchone()
        
        if not res:
            app.logger.error(f"⛔ GLP: Pedido {pedido_id} no encontrado para enviar correo.")
            return False
            
        if isinstance(res, dict):
            emp, ubi, lot, cod, prov = res['cliente'], res['ubicacion'], res['lote'], res['codigo_pedido'], res['proveedor']
        else:
            emp, ubi, lot, cod, prov = res[0], res[1], res[2], res[3], res[4]
        
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
            app.logger.info(f"ℹ️ GLP: Proveedor {prov} sin correos. Enviando copia a administración.")
            emails = [EMAIL_USER]

        # --- AQUÍ ESTÁ EL CAMBIO: DISEÑO PRO ---
        cuerpo = f"""
        <!DOCTYPE html>
        <html lang="es">
        <head>
        <meta charset="UTF-8">
        </head>
        <body style="background-color: #f4f4f4; padding: 20px; font-family: Arial, sans-serif;">

            <div style="background-color: #ffffff; max-width: 500px; margin: 0 auto; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); overflow: hidden;">
                
                <div style="background-color: #015249; color: white; padding: 20px; text-align: center;">
                    <h2 style="margin: 0; font-size: 22px;">Orden de Suministro GLP</h2>
                    <p style="margin: 5px 0 0 0; opacity: 0.9; font-size: 14px;">Solicitud Aprobada</p>
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
                            <td style="padding: 10px 0; color: #666;">📊 Nivel Aprobado:</td>
                            <td style="padding: 10px 0; font-weight: bold; text-align: right; color: #015249; font-size: 18px;">{nivel_aprobado}%</td>
                        </tr>
                    </table>

                    <div style="margin-top: 25px; padding: 10px; background-color: #fff8e1; border-left: 4px solid #ffc107; font-size: 13px; color: #795548;">
                        <strong>Instrucción:</strong> Favor suministrar gas hasta alcanzar el nivel aprobado. Incluir el código de pedido en la factura.
                    </div>
                </div>

                <div style="background-color: #f8f9fa; padding: 15px; text-align: center; border-top: 1px solid #eee;">
                    <p style="margin: 0; color: #999; font-size: 11px;">
                        Enviado automáticamente por <strong>BQA-ONE Automation</strong><br>
                        Energix360 System
                    </p>
                </div>

            </div>
        </body>
        </html>
        """
        
        msg = MIMEMultipart()
        msg["Subject"] = f"✅ Orden de Suministro: {cod} - {ubi}"
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(emails)
        msg.attach(MIMEText(cuerpo, "html", "utf-8"))

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as s:
            s.starttls()
            s.login(EMAIL_USER, EMAIL_PASS)
            s.sendmail(EMAIL_USER, emails, msg.as_string())
        
        app.logger.info(f"✅ GLP: Correo enviado a {emails} (Pedido {cod})")
        return True

    except Exception as e:
        app.logger.error(f"⛔ GLP Email: Falló el envío. Error: {str(e)}")
