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

bp_glp = Blueprint('bp_glp', __name__, url_prefix='/glp')


EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))

EMAIL_USER = os.environ.get("EMAIL_USER")
EMAIL_PASS = os.environ.get("EMAIL_PASS")

EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)


# ==============
# Utilidades base
# ==============
def _normalize_sede(s):
    """Normaliza 'sede' aceptando formatos 'SEDE | EMPRESA' o espacios raros."""
    s = (s or "").strip()
    if "|" in s:
        s = s.split("|", 1)[0]
    s = _re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()
    return s


def _guardar_testigo(base64_data, carpeta, nombre_archivo):
    """Guarda un archivo de testigo (imagen) en la carpeta de estáticos."""
    if not base64_data:
        return None

    try:
        data_match = _re.match(r'data:image/(?P<ext>png|jpeg);base64,(?P<data>.+)', base64_data)
        if not data_match:
            # Manejo de datos base64 que no tienen prefijo de tipo MIME
            if "," in base64_data:
                base64_data = base64_data.split(",", 1)[1]

            # Intenta determinar extensión si el archivo es solo el base64 raw (asumimos jpg)
            ext = 'jpg'
            binary_data = base64.b64decode(base64_data)
        else:
            ext = data_match.group('ext')
            binary_data = base64.b64decode(data_match.group('data'))

        # Asegurar la ruta estática
        static_dir = os.path.join("static", carpeta) # No usar app.root_path en un entorno virtual
        if not os.path.exists(static_dir):
            os.makedirs(static_dir)

        filename = f"{nombre_archivo}.{ext}"
        file_path = os.path.join(static_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(binary_data)

        # Retornar ruta relativa a static
        ruta_web = os.path.relpath(file_path, "static").replace(os.path.sep, "/")
        return f"/static/{ruta_web}"
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
    row = cur.fetchone() or {}
    fecha_ini = row.get("fecha_ini")

    dias = 1
    if fecha_ini:
        try:
            # Aseguramos que fecha_ini sea date si es necesario
            if isinstance(fecha_ini, datetime):
                fecha_ini = fecha_ini.date()
            dias = (fecha_actual - fecha_ini).days + 1
        except Exception:
            dias = 1

    # Nota: Aquí se debería buscar la ID de la operación actual para actualizar *esa* fila,
    # pero tu código original actualiza *todas* las filas con ese lote. Mantengo la lógica original.
    cur.execute("""
        UPDATE cardex_glp
           SET dias_operacion = %s
         WHERE empresa = %s
           AND TRIM(ubicacion) = TRIM(%s)
           AND lote = %s
    """, (dias, empresa, ubicacion, lote_id))

    return dias


def _calcular_consumo_lote(cur, empresa, ubicacion, lote_id, id_operacion_actual, tanques):
    """
    Consumo entre operación previa y actual (kg), para todos los tanques.
    Guarda kg_pollito y neto_gastado en fila actual.
    """
    if not tanques:
        return 0.0, 0.0, None

    cur.execute("""
        SELECT *
        FROM cardex_glp
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
          AND lote = %s
          AND id < %s
        ORDER BY fecha DESC, id DESC
        LIMIT 1
    """, (empresa, ubicacion, lote_id, id_operacion_actual))
    prev = cur.fetchone()

    if not prev:
        return 0.0, 0.0, None

    densidad = prev.get("densidad_suministrada") or prev.get("densidad_estimada") or 2.0
    try:
        densidad = float(densidad)
        if densidad <= 0:
            densidad = 2.0
    except Exception:
        densidad = 2.0

    consumo_total_kg = 0.0

    # Lógica de cálculo de consumo por diferencia de nivel entre tanques.
    # (Tu lógica original es compleja y depende de muchas columnas dinámicas). 
    # Usaremos una aproximación simplificada para demostrar la integración.
    
    # Asumo que en la tabla cardex_glp tienes columnas como `nivel 1`, `capacidad 1`, etc.
    for tk in tanques:
        num = tk.get("numero")
        if not num:
            continue
        try:
            cap = float(tk.get("capacidad", 0) or 0)
        except Exception:
            cap = 0.0
        try:
            nivel_act = float(tk.get("nivel", 0) or 0)
        except Exception:
            nivel_act = 0.0

        col_nivel_prev = f"nivel {num}"
        nivel_prev = prev.get(col_nivel_prev)
        
        if nivel_prev is None:
            # Intenta obtener el nivel de la capacidad/nivel de las columnas genéricas
            nivel_prev = float(prev.get(f"nivel_{num}", 0) or 0) 
            cap = float(prev.get(f"capacidad_{num}", 0) or 0)

        if nivel_prev is None:
            continue
        try:
            nivel_prev = float(nivel_prev)
        except Exception:
            nivel_prev = 0.0

        delta = nivel_prev - nivel_act
        if delta <= 0:
            continue

        # Convertir delta % a KG: Densidad * Capacidad * (Delta / 100)
        consumo_total_kg += densidad * cap * (delta / 100.0)


    # Obtener el total de pollitos del inicio de calefacción
    cur.execute("""
        SELECT pollitos
        FROM cardex_glp
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
          AND lote = %s
          AND operacion = 'inicio_calefaccion'
        ORDER BY fecha ASC, id ASC
        LIMIT 1
    """, (empresa, ubicacion, lote_id))
    row_ini = cur.fetchone() or {}
    pollitos = row_ini.get("pollitos") or 0
    try:
        pollitos = int(pollitos)
    except Exception:
        pollitos = 0

    kg_pollito = 0.0
    if pollitos and consumo_total_kg > 0:
        kg_pollito = consumo_total_kg / float(pollitos)

    # Actualizar la fila de la operación actual con el consumo
    cur.execute("""
        UPDATE cardex_glp
           SET kg_pollito = %s,
               neto_gastado = COALESCE(neto_gastado, 0) + %s
         WHERE id = %s
    """, (kg_pollito, consumo_total_kg, id_operacion_actual))

    return consumo_total_kg, kg_pollito, pollitos


def _buscar_proveedor_principal(cur, empresa, ubicacion, tanques):
    """
    Devuelve proveedor principal tomando el primer tanque reportado.
    """
    if not tanques:
        return None
    # Asumo que la columna 'nombre_tanque' en tanques_sedes coincide con el campo 'numero' en 'tanques'
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
    return p.get("proveedor") if p else None


# --- FUNCIÓN DE GENERACIÓN DE CÓDIGO ACTUALIZADA ---
# Agregamos el parámetro 'proveedor'
def _generar_codigo_pedido(cliente_nombre, lote_id, ubicacion, proveedor, cur):
    """
    Genera código y guarda el pedido INCLUYENDO EL PROVEEDOR.
    """
    mes = datetime.now().strftime("%m")
    
    # 1. Generar iniciales del cliente
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

    # 2. Registrar el pedido CON PROVEEDOR
    query = """
        INSERT INTO pedidos_gas_glp 
            (cliente, codigo_pedido, estatus, fecha_registro, lote, ubicacion, proveedor) 
        VALUES 
            (%s, %s, 'generado', NOW(), %s, %s, %s)
    """
    # Pasamos 'proveedor' al query
    cur.execute(query, (cliente_nombre, codigo_pedido, lote_id, ubicacion, proveedor))
    
    return codigo_pedido

def _enviar_alerta_pedido_tanqueo(empresa, ubicacion, lote_id, proveedor_principal, tanques_bajos, codigo_pedido):
    """Envía un correo de alerta de solicitud de pedido al proveedor con diseño corporativo."""

    if not tanques_bajos:
        return False

    # Usar variables locales leídas de os.environ (¡IMPORTANTE!)
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
    c = cur.fetchone() or {}
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

    # Usar variables locales (IMPORTANTE)
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
    c = cur.fetchone() or {}
    destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
    cur.close()

    if email_baqone and email_baqone not in destinatarios:
        destinatarios.append(email_baqone)

    if not destinatarios:
        return False

    # --- Filas de Detalle por Tanque ---
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

    # --- CUERPO DEL CORREO (Diseño Corporativo) ---
    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; }}
        .container {{ max-width: 650px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
        .header {{ background-color: #d9534f; color: #ffffff; padding: 20px; text-align: center; }} /* Rojo para alerta */
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

    Retorna: (dr, tr, ts)

    Salvaguardas:
      - dr >= 0
      - ts en [0, 80]
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

    # Usar variables locales (Igual que en las funciones anteriores)
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
    c = cur.fetchone() or {}
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

@csrf.exempt  # Excluye la verificación CSRF para esta ruta
@bp_glp.route('/context', methods=['GET'])
@login_required_custom
def glp_context():
    # Si no necesitas autenticación, omite @login_required_custom
    # Si la autenticación es necesaria, usa el decorador adecuado.
    # @login_required_custom   # Descomenta solo si necesitas login

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
            cur.execute("""
                SELECT nombre_tanque AS numero,
                       capacidad_gls  AS capacidad
                FROM tanques_sedes
                WHERE empresa = %s
                  AND TRIM(ubicacion) = TRIM(%s)
                ORDER BY nombre_tanque
            """, (empresa, sede))
            rows = cur.fetchall() or []
        tanques = [
            {"numero": r.get("numero") or "",
             "capacidad": float(r.get("capacidad") or 0),
             "etiqueta": r.get("numero") or ""}
            for r in rows
        ]
        return jsonify({"success": True, "tanques": tanques})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"success": False, "tanques": [], "message": f"Error: {e}"}), 500


# ======================
# Iniciar calefacción
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

    op_id = (data or {}).get("op_id")
    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

    # Idempotencia por op_id (tolerancia a reintentos offline/online)
    cur_check = mysql.connection.cursor()
    cur_check.execute("SELECT 1 FROM cardex_glp WHERE op_id=%s LIMIT 1", (op_id,))
    if cur_check.fetchone():
        cur_check.close()
        return jsonify({
            "success": True,
            "message": "Operación ya recibida (idempotente).",
            "resumen": {"operacion": "inicio_calefaccion", "sede": ubicacion, "op_id": op_id}
        }), 200
    cur_check.close()

    pollitos = data.get('pollitos')
    tanques  = data.get('tanques', []) or []

    # Regla real: NO promedio. Pedido único si existe al menos un tanque con nivel <= 30.
    tanques_bajos = []
    for tk in (tanques or []):
        try:
            niv = float(tk.get("nivel", 0) or 0)
        except Exception:
            niv = 0.0

        if niv <= 30.0:
            tanques_bajos.append({
                "numero": tk.get("numero"),
                "nivel": round(niv, 2)
            })

    try:
        with mysql.connection.cursor() as cur:
            # Verificar lote activo
            cur.execute("""
                SELECT COUNT(*) AS activo
                FROM cardex_glp
                WHERE empresa = %s
                  AND TRIM(ubicacion) = TRIM(%s)
                  AND estatus_lote = 'ACTIVO'
            """, (empresa, ubicacion))
            row = cur.fetchone() or {}
            if (row.get("activo", 0) or 0) > 0:
                cur.execute("""
                    SELECT lote, fecha
                    FROM cardex_glp
                    WHERE empresa = %s
                      AND TRIM(ubicacion) = TRIM(%s)
                      AND estatus_lote = 'ACTIVO'
                    ORDER BY fecha DESC, id DESC LIMIT 1
                """, (empresa, ubicacion))
                conf = cur.fetchone() or {}
                msg = f"Ya existe un lote activo (lote {conf.get('lote')} de el: {conf.get('fecha')}). Dale RELOAD"
                return jsonify({"success": False, "message": msg})

            dias_operacion = 1
            fecha = datetime.now().date()
            lote_id = f"{fecha.strftime('%Y%m%d')}_{ubicacion.replace(' ', '')}"

            columnas_insert = [
                "fecha","empresa","id_empresa","ubicacion","lote","estatus_lote",
                "operacion","tipo","clase","pollitos",
                "registro","dias_operacion","op_id"
            ]
            valores_insert = [
                fecha, empresa, id_empresa, ubicacion, lote_id, 'ACTIVO',
                'inicio_calefaccion', 'manual', 'saldo inicial',
                pollitos if empresa == "Pollos GAR SAS" else None,
                usuario, dias_operacion, op_id
            ]

            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas_insert)}) "
                f"VALUES ({', '.join(['%s']*len(columnas_insert))})",
                valores_insert
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)

            set_cols, set_vals = [], []
            densidad = 2.0
            saldo_estimado_kg = 0.0

            for tk in tanques:
                numero = tk.get("numero")

                try:
                    nivel = float(tk.get("nivel", 0) or 0)
                except Exception:
                    nivel = 0.0

                try:
                    capacidad = float(tk.get("capacidad", 0) or 0)
                except Exception:
                    capacidad = 0.0

                testigo = tk.get("testigo")

                if numero:
                    set_cols.append(f"`nivel {numero}`=%s"); set_vals.append(nivel)
                    set_cols.append(f"`capacidad {numero}`=%s"); set_vals.append(capacidad)

                if numero and testigo:
                    ruta_web = _guardar_testigo(testigo, carpeta, f"{numero}_{id_operacion}.jpg")
                    set_cols.append(f"`testigo nivel {numero}`=%s"); set_vals.append(ruta_web)

                saldo_estimado_kg += densidad * capacidad * (nivel / 100.0)

            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)

            if set_cols:
                cur.execute(
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s",
                    set_vals + [id_operacion]
                )

            saldo_estimado_gal = (saldo_estimado_kg / densidad) if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s,
                       proveedor=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg, 2), round(saldo_estimado_gal, 2), proveedor_principal, id_operacion))

            # Generación de pedido único si hay tanques bajos (<= 30%)
            codigo_pedido = None
            if tanques_bajos:
                # Le pasamos 'proveedor_principal' que ya calculaste unas líneas antes
                codigo_pedido = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor_principal, cur)
                # Enviar correo al proveedor listando SOLO los tanques bajos y adjuntando el código
                # Requiere que _enviar_alerta_pedido_tanqueo reciba tanques_bajos (ver nota abajo).
                _enviar_alerta_pedido_tanqueo(
                    empresa,
                    ubicacion,
                    lote_id,
                    proveedor_principal,
                    tanques_bajos,
                    codigo_pedido
                )

                # Guardar el código en el registro del inicio
                cur.execute("""
                    UPDATE cardex_glp
                       SET codigo_pedido = %s
                     WHERE id = %s
                """, (codigo_pedido, id_operacion))

            mysql.connection.commit()

        mensaje = f"Lote {lote_id} registrado correctamente."
        if tanques_bajos:
            mensaje += f" ⚠️ Hay {len(tanques_bajos)} tanque(s) con nivel ≤ 30%. Se solicitó tanqueo con código {codigo_pedido}."

        resumen = {
            "operacion": "inicio_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "pollitos": pollitos if empresa == "Pollos GAR SAS" else None,
            "tanques": _resumen_tanques(tanques),
            "dias_operacion": dias_operacion,
            "proveedor": proveedor_principal,
            "op_id": op_id,
            "codigo_pedido": codigo_pedido,
            "tanques_bajos": tanques_bajos
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except Exception:
        print("⛔ Error en registrar_inicio_calefaccion:")
        traceback.print_exc()
        mysql.connection.rollback()
        return jsonify({"success": False, "message": "Error al registrar los datos."})


# ======================
# Registrar tanqueo
# ======================
@csrf.exempt
@bp_glp.route('/registrar_tanqueo', methods=['POST'])
@login_required_custom
def registrar_tanqueo():
    # Código original restaurado + mejoras de unicidad
    # CAMBIO: calcular y guardar precio_unitario / precio_total (proveedor->precio)
    # CAMBIO: mensaje final incluye "Valor estimado del tanqueo: $XXXX"
    # CAMBIO: alerta por desviación se evalúa con DESVÍO TOTAL, solo positivo > 8%
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

    op_id = (data or {}).get("op_id")
    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

    cur_check = mysql.connection.cursor()
    cur_check.execute("SELECT 1 FROM cardex_glp WHERE op_id=%s LIMIT 1", (op_id,))
    if cur_check.fetchone():
        cur_check.close()
        return jsonify({
            "success": True,
            "message": "Operación ya recibida (idempotente).",
            "resumen": {"operacion": "tanqueo", "sede": data.get("sede", "")}
        }), 200
    cur_check.close()

    empresa    = session.get('empresa') or ''
    usuario    = session.get('nombre') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))
    tanques    = data.get('tanques', []) or []

    if not empresa or not ubicacion:
        return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400
    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para el tanqueo."}), 400

    try:
        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT lote
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC, id DESC LIMIT 1
            """, (empresa, ubicacion))
            row = cur.fetchone() or {}
            if not row.get("lote"):
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})
            lote_id = row["lote"]

            fecha = datetime.now().date()
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
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)

            set_cols, set_vals = [], []
            densidad_estimada = 2.0
            saldo_estimado_kg = 0.0

            densidades_registradas = []
            masas_esperadas = []
            masas_facturadas = []
            desviaciones_porcentuales = []

            for tk in tanques:
                num = (tk.get("numero") or "").lower().strip()

                cap = float(tk.get("capacidad", 0) or 0.0)
                nivel_ini = float(tk.get("nivel_inicial", 0) or 0.0)
                nivel_fin = float(tk.get("nivel_final", 0) or 0.0)

                foto_ini = tk.get("foto_nivel_inicial")
                foto_fin = tk.get("foto_nivel_final")
                foto_bau = tk.get("foto_baucher")

                if num:
                    # Guardamos en la fila de tanqueo los NIVELES FINALES (post tanqueo)
                    set_cols.append(f"`nivel {num}`=%s");     set_vals.append(nivel_fin)
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)

                    if foto_ini:
                        ruta_ini = _guardar_testigo(foto_ini, carpeta, f"{num}_nivel_inicial_tanqueo_{id_operacion}.jpg")
                        set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta_ini)
                    if foto_fin:
                        ruta_fin = _guardar_testigo(foto_fin, carpeta, f"{num}_nivel_final_tanqueo_{id_operacion}.jpg")
                        set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta_fin)

                    if foto_bau:
                        ruta_baucher = _guardar_testigo(foto_bau, carpeta, f"{num}_baucher_tanqueo_{id_operacion}.jpg")
                        col_baucher = f"testigo_baucher_{num.replace('-', '_')}"
                        set_cols.append(f"`{col_baucher}`=%s"); set_vals.append(ruta_baucher)

                saldo_estimado_kg += densidad_estimada * cap * (nivel_fin/100.0)

                densidad_sum = float(tk.get("densidad_suministrada") or 0.0)
                if densidad_sum > 0:
                    densidades_registradas.append(densidad_sum)

                kg_sumin = float(tk.get("kg_suministrados") or 0.0)

                delta_nivel = nivel_fin - nivel_ini
                masa_esp_tk = 0.0
                if densidad_sum > 0 and cap > 0 and delta_nivel > 0:
                    masa_esp_tk = densidad_sum * cap * (delta_nivel/100.0)

                if masa_esp_tk > 0:
                    masas_esperadas.append(masa_esp_tk)
                    masas_facturadas.append(kg_sumin)
                    if kg_sumin > 0:
                        desvio = ((kg_sumin - masa_esp_tk)/masa_esp_tk)*100.0
                        desviaciones_porcentuales.append(desvio)

            if set_cols:
                cur.execute(
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s",
                    set_vals + [id_operacion]
                )

            saldo_estimado_gal = saldo_estimado_kg/densidad_estimada if densidad_estimada else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            # ⚠️ Cálculo de consumo del tramo ANTERIOR al tanqueo
            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion,
                [{"numero": t.get("numero"),
                  "capacidad": t.get("capacidad"),
                  "nivel": t.get("nivel_inicial")} for t in tanques]
            )

            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            dens_prom = sum(densidades_registradas)/len(densidades_registradas) if densidades_registradas else 0.0
            masa_esperada_total = sum(masas_esperadas) if masas_esperadas else 0.0
            masa_facturada_total = sum(masas_facturadas) if masas_facturadas else 0.0
            porcentaje_dif_prom = sum(desviaciones_porcentuales)/len(desviaciones_porcentuales) if desviaciones_porcentuales else 0.0

            # Desvío TOTAL (regla de alerta y control corporativo)
            desvio_total = 0.0
            if masa_esperada_total > 0:
                desvio_total = ((masa_facturada_total - masa_esperada_total) / masa_esperada_total) * 100.0

            # Proveedor de la granja (según tu lógica existente)
            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)

            # Guardar control tanqueo
            cur.execute("""
                UPDATE cardex_glp
                   SET densidad_suministrada=%s,
                       masa_esperada_kg=%s,
                       masa_kg_facturada=%s,
                       porcentaje_diferencia=%s,
                       proveedor=%s
                 WHERE id=%s
            """, (
                round(dens_prom,3) if dens_prom else 0.0,
                round(masa_esperada_total,2),
                round(masa_facturada_total,2),
                round(desvio_total,2),
                proveedor_principal,
                id_operacion
            ))

            # ================================
            # CAMBIO: Precio del tanqueo (precio_unitario / precio_total)
            # 1) Consultar precio del proveedor (proveedores.precio)
            # 2) precio_total = precio_unitario * masa_kg_facturada (TOTAL)
            # 3) Guardar en cardex_glp.precio_unitario y cardex_glp.precio_total
            # ================================
            precio_unitario = 0.0
            precio_total = 0.0

            if proveedor_principal:
                cur.execute("""
                    SELECT precio
                    FROM proveedores
                    WHERE proveedor=%s
                    LIMIT 1
                """, (proveedor_principal,))
                prow = cur.fetchone() or {}
                try:
                    precio_unitario = float(prow.get("precio") or 0.0)
                except Exception:
                    precio_unitario = 0.0

            # Total estimado según kg facturado
            precio_total = precio_unitario * float(masa_facturada_total or 0.0)

            # Guardar en cardex_glp
            cur.execute("""
                UPDATE cardex_glp
                   SET precio_unitario=%s,
                       precio_total=%s
                 WHERE id=%s
            """, (
                round(precio_unitario, 6),
                round(precio_total, 2),
                id_operacion
            ))

            # ================================
            # Alerta por desviación >8% SOLO POSITIVA (facturado > esperado)
            # Usa tu función existente: _enviar_alerta_desviacion_tanqueo(...)
            # ================================
            alerta_enviada = False
            if masa_esperada_total > 0 and desvio_total > 8.0 and proveedor_principal:
                try:
                    alerta_enviada = _enviar_alerta_desviacion_tanqueo(
                        empresa=empresa,
                        ubicacion=ubicacion,
                        lote_id=lote_id,
                        proveedor_principal=proveedor_principal,
                        op_id=op_id,
                        masa_esperada_total=masa_esperada_total,
                        masa_facturada_total=masa_facturada_total,
                        desvio_total_pct=desvio_total,
                        dens_prom=dens_prom,
                        tanques=tanques
                    )
                except Exception:
                    app.logger.error("⛔ Error al ejecutar _enviar_alerta_desviacion_tanqueo:")
                    traceback.print_exc()
                    alerta_enviada = False

            mysql.connection.commit()

        # ================================
        # Mensaje final (ordenado, una sola vez)
        # ================================
        mensaje = "Tanqueo registrado correctamente."

        if consumo_kg > 0:
            mensaje += f" Consumo desde la última operación: {round(consumo_kg,2)} kg"
            if kg_pollito > 0:
                mensaje += f" ({round(kg_pollito,6)} kg_pollito)."

        if masa_esperada_total > 0:
            mensaje += (
                f" Control de tanqueo: esperado {round(masa_esperada_total,2)} kg, "
                f"facturado {round(masa_facturada_total,2)} kg "
                f"(desviación total {round(desvio_total,2)}%)."
            )
            if desvio_total > 8.0:
                mensaje += " ⚠️ Desviación positiva superior al 8%."
                if alerta_enviada:
                    mensaje += " Se envió alerta por email."

        # Valor estimado del tanqueo: $XXXX (formato pesos CO con separador de miles)
        # Ejemplo: 1234567.89 -> $1.234.568
        valor_pesos = round(float(precio_total or 0.0), 0)
        valor_formato = f"{valor_pesos:,.0f}".replace(",", ".")
        mensaje += f" Valor estimado del tanqueo: ${valor_formato}."

        resumen = {
            "operacion": "tanqueo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques([
                {"numero": t.get("numero"), "nivel": t.get("nivel_final"), "capacidad": t.get("capacidad")}
                for t in tanques
            ]),
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "saldo_estimado_galones": round(saldo_estimado_gal,2),
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

    except Exception:
        print("⛔ Error en registrar_tanqueo:")
        traceback.print_exc()
        mysql.connection.rollback()
        return jsonify({"success": False, "message": "Error al registrar tanqueo."})

# ======================
# Registrar consumo
# ======================
@csrf.exempt
@bp_glp.route('/registrar_consumo', methods=['POST'])
@login_required_custom
def registrar_consumo():
    # Registrar consumo:
    # - Define tanques_bajos (umbral CONSUMO <= 25%)
    # - Si hay tanques_bajos:
    #     - calcular ts_solicitado con algoritmo foto (depende de dias_operacion)
    #     - generar codigo_pedido
    #     - enviar correo (misma estructura que pedido estándar, pero llenar al ts variable)
    # - Enviar al proveedor
    # - Reforzar: factura válida solo si adjunta código de pedido
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

    op_id = (data or {}).get("op_id")
    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

    cur_check = mysql.connection.cursor()
    cur_check.execute("SELECT 1 FROM cardex_glp WHERE op_id=%s LIMIT 1", (op_id,))
    if cur_check.fetchone():
        cur_check.close()
        return jsonify({
            "success": True,
            "message": "Operación ya recibida (idempotente).",
            "resumen": {"operacion": "consumo", "sede": data.get("sede",""), "op_id": op_id}
        }), 200
    cur_check.close()

    empresa    = session.get('empresa') or ''
    usuario    = session.get('nombre') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))
    tanques    = data.get('tanques', []) or []

    if not empresa or not ubicacion:
        return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400
    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para registrar consumo."}), 400

    lote_id = None
    fecha = datetime.now().date()
    saldo_estimado_kg = 0.0
    saldo_estimado_gal = 0.0
    consumo_kg = 0.0
    kg_pollito = 0.0
    pollitos = 0
    dias_operacion = 0
    proveedor_principal = None

    codigo_pedido = None
    tanques_bajos = []
    dr = 0
    tr = 0.0
    ts_solicitado = 0.0

    try:
        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT lote
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC, id DESC LIMIT 1
            """, (empresa, ubicacion))
            row = cur.fetchone() or {}
            if not row.get("lote"):
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})
            lote_id = row["lote"]

            columnas = [
                "fecha","empresa","id_empresa","ubicacion",
                "lote","estatus_lote","operacion","tipo","clase",
                "registro","op_id"
            ]
            valores  = [
                fecha, empresa, id_empresa, ubicacion,
                lote_id, 'ACTIVO', "consumo", "manual", "egreso",
                usuario, op_id
            ]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("testigos",empresa.replace(" ","_"),lote_id)

            set_cols, set_vals = [], []
            densidad = 2.0
            saldo_estimado_kg = 0.0
            tanques_bajos = []

            for tk in tanques:
                num = tk.get("numero")
                try:
                    niv = float(tk.get("nivel", 0) or 0)
                except Exception:
                    niv = 0.0
                try:
                    cap = float(tk.get("capacidad", 0) or 0)
                except Exception:
                    cap = 0.0
                tst = tk.get("testigo")

                # UMBRAL CONSUMO: tanques bajos si nivel <= 25%
                if num and niv <= 25.0:
                    tanques_bajos.append({"numero": num, "nivel": round(niv, 2)})

                if num:
                    set_cols.append(f"`nivel {num}`=%s"); set_vals.append(niv)
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)

                if num and tst:
                    ruta = _guardar_testigo(tst, carpeta, f"{num}_consumo_{id_operacion}.jpg")
                    set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta)

                saldo_estimado_kg += densidad * cap * (niv/100.0)

            if set_cols:
                cur.execute(
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s",
                    set_vals + [id_operacion]
                )

            saldo_estimado_gal = saldo_estimado_kg/densidad if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion, tanques
            )

            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
            cur.execute("UPDATE cardex_glp SET proveedor=%s WHERE id=%s", (proveedor_principal, id_operacion))

            # Algoritmo (foto) -> ts variable
            dr, tr, ts_solicitado = _calcular_ts_consumo(dias_operacion)

            # Análogo a iniciar_calefaccion: si hay tanques_bajos -> generar pedido
            # (y luego enviar correo usando función específica de consumo)
            if tanques_bajos and ts_solicitado > 0:
                try:
                    # Le pasamos 'proveedor_principal'
                    codigo_pedido = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor_principal, cur)
                    cur.execute("""
                        UPDATE cardex_glp
                           SET codigo_pedido = %s
                         WHERE id = %s
                    """, (codigo_pedido, id_operacion))
                except Exception as e:
                    app.logger.error(f"Error al generar/guardar código de pedido en consumo: {e}")
                    codigo_pedido = None

            mysql.connection.commit()

        # Enviar correo al proveedor (no bloqueante)
        if codigo_pedido and tanques_bajos and ts_solicitado > 0:
            try:
                _enviar_alerta_pedido_tanqueo_consumo(
                    empresa=empresa,
                    ubicacion=ubicacion,
                    lote_id=lote_id,
                    proveedor_principal=proveedor_principal,
                    tanques_bajos=tanques_bajos,
                    codigo_pedido=codigo_pedido,
                    ts_solicitado=ts_solicitado
                )
            except Exception as e:
                app.logger.error(f"Error enviando correo pedido CONSUMO (no bloqueante): {e}")

        mensaje = "Consumo registrado correctamente."
        if consumo_kg > 0:
            mensaje += f" Consumo desde la última operación: {round(consumo_kg,2)} kg"
            if kg_pollito > 0:
                mensaje += f" ({round(kg_pollito,6)} kg/pollito)."

        if codigo_pedido and tanques_bajos and ts_solicitado > 0:
            mensaje += (
                f" ⚠️ Se solicitó tanqueo (CONSUMO) por tanques ≤ 25% "
                f"con código {codigo_pedido}. Nivel solicitado: {round(ts_solicitado,2)}%."
            )
        elif tanques_bajos and ts_solicitado <= 0:
            mensaje += " ⚠️ Hay tanques ≤ 25%, pero el nivel solicitado calculado es 0% (no se generó pedido)."
        elif tanques_bajos and not codigo_pedido:
            mensaje += " ⚠️ Hay tanques ≤ 25%, pero no se pudo generar código de pedido (ver logs)."

        resumen = {
            "operacion": "consumo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques(tanques),
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "saldo_estimado_galones": round(saldo_estimado_gal,2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg,2),
            "kg_pollito": round(kg_pollito,6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "proveedor": proveedor_principal,
            "op_id": op_id,
            "codigo_pedido": codigo_pedido,
            "tanques_bajos": tanques_bajos,
            "dr": dr,
            "tr": round(tr, 2),
            "ts_solicitado": round(ts_solicitado, 2),
            "umbral_tanques_bajos_pct": 25.0
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except Exception:
        print("⛔ Error en registrar_consumo:")
        traceback.print_exc()
        mysql.connection.rollback()
        return jsonify({"success": False, "message": "Error al registrar consumo."})
# ======================
# Finalizar calefacción (batch)
# ======================
@csrf.exempt
@bp_glp.route('/finalizar_calefaccion_batch', methods=['POST'])
@login_required_custom
def finalizar_calefaccion_batch():
    # Código original restaurado (simplificado)
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

    op_id = (data or {}).get("op_id")
    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400

    cur_check = mysql.connection.cursor()
    cur_check.execute("SELECT 1 FROM cardex_glp WHERE op_id=%s LIMIT 1", (op_id,))
    if cur_check.fetchone():
        cur_check.close()
        return jsonify({
            "success": True,
            "message": "Operación ya recibida (idempotente).",
            "resumen": {"operacion": "finalizar_calefaccion", "sede": data.get("sede","")}
        }), 200
    cur_check.close()

    empresa    = session.get('empresa') or ''
    usuario    = session.get('nombre') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))
    tanques    = data.get('tanques', []) or []

    try:
        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT lote, MIN(fecha) AS fecha_ini
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC, id DESC LIMIT 1
            """, (empresa, ubicacion))
            row = cur.fetchone() or {}
            if not row.get("lote"):
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

            lote_id = row["lote"]

            fecha = datetime.now().date()
            columnas = [
                "fecha","empresa","id_empresa","ubicacion",
                "lote","estatus_lote","operacion","tipo","clase",
                "registro","op_id"
            ]
            valores  = [
                fecha, empresa, id_empresa, ubicacion,
                lote_id, 'ACTIVO', "finalizar_calefaccion",
                "manual", "saldo final", usuario, op_id
            ]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("testigos", empresa.replace(" ","_"), lote_id)

            set_cols, set_vals = [], []
            densidad = 2.0
            saldo_estimado_kg = 0.0

            for tk in tanques:
                num = tk.get("numero")
                try:
                    niv = float(tk.get("nivel", 0))
                except Exception:
                    niv = 0.0
                try:
                    cap = float(tk.get("capacidad", 0))
                except Exception:
                    cap = 0.0
                tst = tk.get("testigo")

                if num and niv is not None:
                    set_cols.append(f"`nivel {num}`=%s"); set_vals.append(niv)
                if num and cap is not None:
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)
                if num and tst:
                    ruta = _guardar_testigo(tst, carpeta, f"{num}_final_{id_operacion}.jpg")
                    set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta)

                saldo_estimado_kg += densidad * cap * (niv/100.0)

            if set_cols:
                cur.execute(
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s",
                    set_vals + [id_operacion]
                )

            saldo_estimado_gal = saldo_estimado_kg/densidad if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion, tanques
            )

            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
            cur.execute("UPDATE cardex_glp SET proveedor=%s WHERE id=%s", (proveedor_principal, id_operacion))

            # cerrar lote
            cur.execute("""
                UPDATE cardex_glp
                   SET estatus_lote='INACTIVO'
                 WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND lote=%s
            """, (empresa, ubicacion, lote_id))

            mysql.connection.commit()

        mensaje = f"Calefacción cerrada en lote {lote_id}."
        if consumo_kg > 0:
            mensaje += f" Consumo desde la última operación: {round(consumo_kg,2)} kg"
            if kg_pollito > 0:
                mensaje += f" ({round(kg_pollito,6)} kg_pollito)."

        resumen = {
            "operacion": "finalizar_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques(tanques),
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "saldo_estimado_galones": round(saldo_estimado_gal,2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg,2),
            "kg_pollito": round(kg_pollito,6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "proveedor": proveedor_principal,
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except Exception:
        print("⛔ Error en finalizar_calefaccion_batch:")
        traceback.print_exc()
        mysql.connection.rollback()
        return jsonify({"success": False, "message": "Error al finalizar calefacción."})

# ==============
# NUEVOS: MÓDULO DE VALIDACIÓN DE FACTURAS
# ==============



# =======================================================
# AQUÍ SIGUE LO QUE YA TIENES (MÓDULO DE VALIDACIÓN...)
# =======================================================


# --- FUNCIÓN DE CONSULTA DE PEDIDOS (CORREGIDA) ---
@bp_glp.route('/consultar_pedidos_pendientes', methods=['POST'])
@login_required_custom
def consultar_pedidos_pendientes():
    """
    Consulta pedidos GLP con estatus 'generado' para la empresa del usuario.
    Incluye proveedor, ubicación, y lote.
    """
    
    empresa_nombre = session.get('empresa')
    
    if not empresa_nombre:
        return jsonify({"success": False, "message": "Falta el nombre de la empresa en la sesión."}), 400

    try:
        cur = mysql.connection.cursor()
        
        # === CORRECCIÓN AQUÍ ===
        # Cambiamos fecha_generacion por fecha_registro
        query = """
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
        """
        cur.execute(query, (empresa_nombre,))
        
        # Recuperar nombres de columnas si usas tuplas, o confiar en el orden
        pedidos = cur.fetchall()
        cur.close()
        
        # Si 'pedidos' son tuplas, el orden es: 
        # 0:id, 1:codigo, 2:fecha, 3:proveedor, 4:ubicacion, 5:lote
        
        pedidos_listos = []
        for pedido in pedidos:
            # Detectamos si es diccionario (DictCursor) o Tupla
            if isinstance(pedido, dict):
                p_id = pedido['id']
                p_cod = pedido['codigo_pedido']
                p_fec = pedido['fecha_registro']
                p_prov = pedido.get('proveedor')
                p_ubi = pedido['ubicacion']
                p_lot = pedido['lote']
            else:
                # Es tupla
                p_id = pedido[0]
                p_cod = pedido[1]
                p_fec = pedido[2]
                p_prov = pedido[3]
                p_ubi = pedido[4]
                p_lot = pedido[5]

            pedidos_listos.append({
                "id": p_id,
                "codigo": p_cod,
                # Formateamos la fecha para el Frontend
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
    """
    Actualiza el estatus de un pedido a 'validado',
    guardando el número de factura y el ID del validador (cédula).
    """
    
    data = request.json
    pedido_id = data.get('pedido_id')
    numero_factura = data.get('numero_factura')
    
    # Asumo que la cédula del validador está en la sesión
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