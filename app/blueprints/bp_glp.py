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
import holidays
from app.utils import registrar_auditoria
from MySQLdb import IntegrityError

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

        # --- CAMBIO CRÍTICO: Usar app.static_folder ---
        # Esto asegura que vaya a C:\Users\casti\energix_360\app\static
        # en lugar de crear una carpeta nueva fuera de la app.
        static_dir = os.path.join(app.static_folder, carpeta) 
        
        if not os.path.exists(static_dir):
            os.makedirs(static_dir)

        filename = f"{nombre_archivo}.{ext}"
        file_path = os.path.join(static_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(binary_data)

        # Calculamos la ruta relativa para la Base de Datos
        # Esto genera strings como: testigos/Empresa/foto.jpg
        ruta_relativa = os.path.relpath(file_path, app.static_folder).replace(os.path.sep, "/")
        
        # Flask siempre sirve los estáticos bajo el prefijo /static/
        # Retornamos: /static/testigos/Empresa/foto.jpg
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
    AJUSTE 3: Soporta N tanques variables.
    Acumula errores de todos los tanques y aborta al final si existe alguno.
    """
    if not tanques:
        return 0.0, 0.0, 0

    consumo_total_kg = 0.0
    errores_detectados = []  # <--- Lista para acumular errores de 1 o varios tanques
    
    # Este bucle se adapta a la cantidad de tanques que lleguen (1, 2, 5, etc.)
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

        # Consulta dinámica: busca la columna específica 'tk-X' según el tanque actual
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
        
        # --- BLINDAJE ---
        prev = {}
        if isinstance(prev_raw, dict):
             prev = prev_raw
        elif isinstance(prev_raw, tuple):
             # orden: operacion, nivel, nivelfinal, densidad
             prev = {
                 'operacion': prev_raw[0],
                 f'nivel tk-{num_tanque}': prev_raw[1],
                 f'nivelfinal tk-{num_tanque}': prev_raw[2],
                 'densidad_suministrada': prev_raw[3]
             }
        # ----------------

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

        # === VALIDACIÓN (Consumo) ===
        # Si el nivel sube (delta negativo), es un error.
        if delta_pct < -0.1:
            errores_detectados.append(
                f"• TK-{num_tanque}: Ingresó {valor_actual_cierre}%, anterior {nivel_anterior}% (SUBIÓ)."
            )
        
        if delta_pct < 0: delta_pct = 0

        kg_tanque = (delta_pct / 100.0) * capacidad_galones * densidad_calculo
        consumo_total_kg += kg_tanque

    # === AL FINAL DEL BUCLE: SI HUBO AL MENOS UN ERROR, ABORTAR ===
    if errores_detectados:
        mensaje_final = "⛔ ERRORES DETECTADOS:\n\n" + "\n".join(errores_detectados) + "\n\nEl nivel no puede subir sin un tanqueo."
        raise ValueError(mensaje_final)
    # ==============================================================

    # Cálculo final de pollitos
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
    
    # --- BLINDAJE ---
    row_ini = cur.fetchone()
    pollitos = 0
    if row_ini:
        if isinstance(row_ini, dict):
            pollitos = int(row_ini.get("pollitos") or 0)
        else:
            pollitos = int(row_ini[0] or 0)
    # ----------------

    kg_pollito = 0.0
    if pollitos > 0 and consumo_total_kg > 0:
        kg_pollito = consumo_total_kg / float(pollitos)

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
    BLINDADA CONTRA TUPLAS.
    """
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
    
    # --- BLINDAJE: Tupla o Dict ---
    if p:
        if isinstance(p, dict):
            return p.get("proveedor")
        else:
            return p[0] # Tupla: columna 0 es proveedor
    # ------------------------------
    return None


def _generar_codigo_pedido(cliente_nombre, lote_id, ubicacion, proveedor, cur):
    """
    Genera código y guarda el pedido INCLUYENDO EL PROVEEDOR.
    """
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

def _enviar_alerta_pedido_tanqueo(empresa, ubicacion, lote_id, proveedor_principal, tanques_bajos, codigo_pedido):
    """Envía un correo de alerta de solicitud de pedido al proveedor con diseño corporativo."""

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

    # Idempotencia por op_id
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
            # === 1. VALIDACIÓN BLINDADA DE LOTE ACTIVO ===
            cur.execute("""
                SELECT COUNT(*) AS activo 
                FROM cardex_glp 
                WHERE empresa = %s 
                  AND TRIM(ubicacion) = TRIM(%s) 
                  AND estatus_lote = 'ACTIVO'
            """, (empresa, ubicacion))
            
            row = cur.fetchone()
            
            # Lógica Híbrida: Detecta si la BD devolvió Diccionario o Tupla
            activo = 0
            if row:
                if isinstance(row, dict):
                    activo = row.get('activo', 0)
                else:
                    activo = row[0]

            if activo > 0:
                cur.execute("""
                    SELECT lote 
                    FROM cardex_glp 
                    WHERE empresa=%s 
                      AND TRIM(ubicacion)=TRIM(%s) 
                      AND estatus_lote='ACTIVO' 
                    LIMIT 1
                """, (empresa, ubicacion))
                
                info = cur.fetchone()
                nombre_lote = "Desconocido"
                
                # Blindaje Segunda Consulta
                if info:
                    if isinstance(info, dict):
                        nombre_lote = info.get('lote', 'Desconocido')
                    else:
                        nombre_lote = info[0]

                # Auditoría bloqueo
                registrar_auditoria(
                    empresa_id=id_empresa,
                    empresa_nombre=empresa,
                    modulo="GLP",
                    usuario=usuario,
                    accion="⛔ Inicio Bloqueado",
                    detalle=f"Intento fallido en {ubicacion}. Ya existe lote activo: {nombre_lote}",
                    nivel="WARNING"
                )
                mysql.connection.commit()

                return jsonify({
                    "success": False, 
                    "message": f"⛔ YA EXISTE UN LOTE ACTIVO ({nombre_lote}).\n\nNo puedes iniciar calefacción otra vez en esta sede sin finalizar el anterior."
                })

            # === 2. INSERCIÓN DE DATOS ===
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
                num_str = str(tk.get("numero", ""))
                match = _re.search(r'\d+', num_str)
                num = match.group() if match else ""
                
                if not num: continue

                try: nivel = float(tk.get("nivel", 0) or 0)
                except Exception: nivel = 0.0
                try: capacidad = float(tk.get("capacidad", 0) or 0)
                except Exception: capacidad = 0.0

                testigo = tk.get("testigo")

                set_cols.append(f"`nivel tk-{num}`=%s"); set_vals.append(nivel)
                set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(capacidad)

                if testigo:
                    ruta_web = _guardar_testigo(testigo, carpeta, f"tk{num}_{id_operacion}")
                    set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta_web)

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

            codigo_pedido = None
            if tanques_bajos:
                codigo_pedido = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor_principal, cur)
                _enviar_alerta_pedido_tanqueo(
                    empresa, ubicacion, lote_id, proveedor_principal, tanques_bajos, codigo_pedido
                )
                cur.execute("UPDATE cardex_glp SET codigo_pedido = %s WHERE id = %s", (codigo_pedido, id_operacion))
                
                registrar_auditoria(
                    empresa_id=id_empresa,
                    empresa_nombre=empresa,
                    modulo="GLP",
                    usuario=usuario,
                    accion="📧 Pedido Enviado (Inicio)",
                    detalle=f"Tanques bajos al iniciar. Código: {codigo_pedido}.",
                    nivel="INFO"
                )

            registrar_auditoria(
                empresa_id=id_empresa,
                empresa_nombre=empresa,
                modulo="GLP",
                usuario=usuario,
                accion="🔥 Inicio Calefacción OK",
                detalle=f"Sede: {ubicacion}. Lote: {lote_id}. Pollitos: {pollitos}",
                nivel="INFO"
            )

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

    except IntegrityError as e:
        mysql.connection.rollback()
        if e.args[0] == 1062 and 'op_id' in str(e.args[1]):
             return jsonify({"success": True, "message": "Operación ya recibida."}), 200
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "❌ Error Integridad (Inicio)", str(e), "ERROR")
            mysql.connection.commit()
        except: pass
        return jsonify({"success": False, "message": f"Error integridad: {e}"})

    except Exception as e:
        print("⛔ Error en registrar_inicio_calefaccion:")
        traceback.print_exc()
        mysql.connection.rollback()
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "💀 Error Crítico (Inicio)", str(e), "CRITICAL")
            mysql.connection.commit()
        except: pass
        return jsonify({"success": False, "message": "Error al registrar los datos."})
    
# ======================
# Registrar tanqueo
# ======================
@csrf.exempt
@bp_glp.route('/registrar_tanqueo', methods=['POST'])
@login_required_custom
def registrar_tanqueo():
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
            
            # --- BLINDAJE ---
            row_raw = cur.fetchone()
            lote_id = None
            if row_raw:
                if isinstance(row_raw, dict):
                    lote_id = row_raw.get("lote")
                else:
                    lote_id = row_raw[0]
            # ----------------

            if not lote_id:
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

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
            
            errores_tanqueo = [] 

            for tk in tanques:
                num_str = str(tk.get("numero", ""))
                match = _re.search(r'\d+', num_str)
                num = match.group() if match else ""
                if not num: continue

                cap = float(tk.get("capacidad", 0) or 0.0)
                nivel_ini = float(tk.get("nivel_inicial", 0) or 0.0)
                nivel_fin = float(tk.get("nivel_final", 0) or 0.0)

                # Validación de lógica de tanqueo: DEBE SUBIR
                if (nivel_fin - nivel_ini) < -0.1:
                     errores_tanqueo.append(
                        f"• TK-{num}: Inicio {nivel_ini}% -> Fin {nivel_fin}% (BAJÓ)."
                     )

                foto_ini = tk.get("foto_nivel_inicial")
                foto_fin = tk.get("foto_nivel_final")
                foto_bau = tk.get("foto_baucher")

                set_cols.append(f"`nivel tk-{num}`=%s");     set_vals.append(nivel_ini)
                if foto_ini:
                    ruta_ini = _guardar_testigo(foto_ini, carpeta, f"tk{num}_ini_{id_operacion}")
                    set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta_ini)

                set_cols.append(f"`nivelfinal tk-{num}`=%s");     set_vals.append(nivel_fin)
                if foto_fin:
                    ruta_fin = _guardar_testigo(foto_fin, carpeta, f"tk{num}_fin_{id_operacion}")
                    set_cols.append(f"`testigo nivelfinal tk-{num}`=%s"); set_vals.append(ruta_fin)

                set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(cap)

                if foto_bau:
                    ruta_baucher = _guardar_testigo(foto_bau, carpeta, f"tk{num}_baucher_{id_operacion}")
                    col_baucher = f"testigo_baucher_tk_{num}"
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

            # Abortar si hubo errores
            if errores_tanqueo:
                mensaje_final = "⛔ ERRORES EN DATOS DE TANQUEO:\n\n" + "\n".join(errores_tanqueo) + "\n\nEn un tanqueo el nivel debe SUBIR."
                raise ValueError(mensaje_final)

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
            
            desvio_total = 0.0
            if masa_esperada_total > 0:
                desvio_total = ((masa_facturada_total - masa_esperada_total) / masa_esperada_total) * 100.0

            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)

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

            precio_unitario = 0.0
            precio_total = 0.0

            if proveedor_principal:
                cur.execute("SELECT precio FROM proveedores WHERE proveedor=%s LIMIT 1", (proveedor_principal,))
                
                # --- BLINDAJE ---
                prow_raw = cur.fetchone()
                if prow_raw:
                    if isinstance(prow_raw, dict): precio_unitario = float(prow_raw.get("precio") or 0.0)
                    else: precio_unitario = float(prow_raw[0] or 0.0)
                # ----------------

            precio_total = precio_unitario * float(masa_facturada_total or 0.0)

            cur.execute("""
                UPDATE cardex_glp
                   SET precio_unitario=%s,
                       precio_total=%s
                 WHERE id=%s
            """, (round(precio_unitario, 6), round(precio_total, 2), id_operacion))

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
                    if alerta_enviada:
                        registrar_auditoria(
                            empresa_id=id_empresa,
                            empresa_nombre=empresa,
                            modulo="GLP",
                            usuario="Sistema",
                            accion="⚠️ Alerta Desviación",
                            detalle=f"Desviación {round(desvio_total, 2)}% en {ubicacion}.",
                            nivel="ALERTA"
                        )
                except Exception:
                    app.logger.error("⛔ Error al enviar alerta desviación.")

            registrar_auditoria(
                empresa_id=id_empresa,
                empresa_nombre=empresa,
                modulo="GLP",
                usuario=usuario,
                accion="Tanqueo Registrado",
                detalle=f"Sede: {ubicacion}. Facturado: {round(masa_facturada_total, 2)} kg.",
                nivel="INFO"
            )

            mysql.connection.commit()

        mensaje = "Tanqueo registrado correctamente."
        if consumo_kg > 0:
            mensaje += f" Consumo previo: {round(consumo_kg,2)} kg."

        if masa_esperada_total > 0:
            mensaje += f" Control: esperado {round(masa_esperada_total,2)} kg, facturado {round(masa_facturada_total,2)} kg."
            if desvio_total > 8.0:
                mensaje += " ⚠️ Desviación > 8%. Alerta enviada."

        valor_pesos = round(float(precio_total or 0.0), 0)
        valor_formato = f"{valor_pesos:,.0f}".replace(",", ".")
        mensaje += f" Valor estimado: ${valor_formato}."

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

    except ValueError as e:
        mysql.connection.rollback()
        try:
             registrar_auditoria(id_empresa, empresa, "GLP", usuario, "⛔ Error Datos Tanqueo", str(e), "WARNING")
             mysql.connection.commit()
        except: pass
        return jsonify({"success": False, "message": str(e)})

    except Exception as e:
        print("⛔ Error en registrar_tanqueo:")
        traceback.print_exc()
        mysql.connection.rollback()
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "💀 Error Crítico (Tanqueo)", str(e), "CRITICAL")
            mysql.connection.commit()
        except: pass
        return jsonify({"success": False, "message": "Error al registrar tanqueo."})
    
# ======================
# Registrar consumo
# ======================
@csrf.exempt
@bp_glp.route('/registrar_consumo', methods=['POST'])
@login_required_custom
def registrar_consumo():
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
        return jsonify({"success": False, "message": "No se recibieron tanques."}), 400

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
    ts_solicitado = 0.0

    try:
        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT lote
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC, id DESC LIMIT 1
            """, (empresa, ubicacion))
            
            # --- BLINDAJE ---
            row_raw = cur.fetchone()
            if row_raw:
                if isinstance(row_raw, dict):
                    lote_id = row_raw.get("lote")
                else:
                    lote_id = row_raw[0]
            # ----------------

            if not lote_id:
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

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

            co_holidays = holidays.CO()

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

                es_bajo = False
                if val <= 25.0:
                    es_bajo = True
                elif fecha.weekday() == 4: 
                    lunes_siguiente = fecha + timedelta(days=3)
                    es_puente = lunes_siguiente in co_holidays
                    dias_proyeccion = 3 if es_puente else 2
                    tasa_diaria = 8.0 
                    nivel_futuro_estimado = val - (dias_proyeccion * tasa_diaria)
                    if nivel_futuro_estimado < 15.0:
                        es_bajo = True
                
                if num and es_bajo:
                    tanques_bajos.append({"numero": num, "nivel": round(val, 2)})

                set_cols.append(f"`nivel tk-{num}`=%s"); set_vals.append(val)
                set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(cap)

                if tst:
                    ruta = _guardar_testigo(tst, carpeta, f"tk{num}_consumo_{id_operacion}")
                    set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta)

                saldo_estimado_kg += densidad * cap * (val/100.0)

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

            dr, tr, ts_solicitado = _calcular_ts_consumo(dias_operacion)

            if tanques_bajos and ts_solicitado > 0:
                try:
                    codigo_pedido = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor_principal, cur)
                    cur.execute("UPDATE cardex_glp SET codigo_pedido = %s WHERE id = %s", (codigo_pedido, id_operacion))
                except Exception as e:
                    app.logger.error(f"Error al generar código pedido consumo: {e}")
                    codigo_pedido = None

            mysql.connection.commit()

        if codigo_pedido and tanques_bajos and ts_solicitado > 0:
            try:
                enviado = _enviar_alerta_pedido_tanqueo_consumo(
                    empresa=empresa,
                    ubicacion=ubicacion,
                    lote_id=lote_id,
                    proveedor_principal=proveedor_principal,
                    tanques_bajos=tanques_bajos,
                    codigo_pedido=codigo_pedido,
                    ts_solicitado=ts_solicitado
                )
                if enviado:
                    registrar_auditoria(
                        empresa_id=id_empresa,
                        empresa_nombre=empresa,
                        modulo="GLP",
                        usuario="Sistema",
                        accion="📧 Pedido Gas (Consumo)",
                        detalle=f"Sede: {ubicacion}. Código: {codigo_pedido}.",
                        nivel="INFO"
                    )
            except Exception as e:
                app.logger.error(f"Error enviando correo pedido CONSUMO: {e}")

        registrar_auditoria(
            empresa_id=id_empresa,
            empresa_nombre=empresa,
            modulo="GLP",
            usuario=usuario,
            accion="Consumo Registrado",
            detalle=f"Sede: {ubicacion}. Consumo: {round(consumo_kg, 2)} kg.",
            nivel="INFO"
        )

        mensaje = "Consumo registrado correctamente."
        if consumo_kg > 0:
            mensaje += f" Consumo: {round(consumo_kg,2)} kg."

        if codigo_pedido and tanques_bajos and ts_solicitado > 0:
            mensaje += f" ⚠️ Pedido solicitado: {codigo_pedido}. Llenar a {round(ts_solicitado,2)}%."

        resumen = {
            "operacion": "consumo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques(tanques),
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg,2),
            "kg_pollito": round(kg_pollito,6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "proveedor": proveedor_principal,
            "op_id": op_id,
            "codigo_pedido": codigo_pedido,
            "tanques_bajos": tanques_bajos,
            "ts_solicitado": round(ts_solicitado, 2)
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})
    
    except ValueError as e:
        mysql.connection.rollback()
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "⛔ Error Lógico Consumo", str(e), "WARNING")
            mysql.connection.commit()
        except: pass
        return jsonify({"success": False, "message": str(e)})
    
    except Exception as e:
        print("⛔ Error en registrar_consumo:")
        traceback.print_exc()
        mysql.connection.rollback()
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "💀 Error Crítico (Consumo)", str(e), "CRITICAL")
            mysql.connection.commit()
        except: pass
        return jsonify({"success": False, "message": "Error al registrar consumo."})
    
# ======================
# Finalizar calefacción (batch)
# ======================
@csrf.exempt
@bp_glp.route('/finalizar_calefaccion_batch', methods=['POST'])
@login_required_custom
def finalizar_calefaccion_batch():
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
            
            # --- BLINDAJE ---
            row_raw = cur.fetchone()
            lote_id = None
            if row_raw:
                if isinstance(row_raw, dict):
                    lote_id = row_raw.get("lote")
                else:
                    lote_id = row_raw[0]
            # ----------------

            if not lote_id:
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

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
                num_str = str(tk.get("numero", ""))
                match = _re.search(r'\d+', num_str)
                num = match.group() if match else ""  
                if not num: continue

                try: niv = float(tk.get("nivel", 0))
                except: niv = 0.0
                try: cap = float(tk.get("capacidad", 0))
                except: cap = 0.0
                tst = tk.get("testigo")

                set_cols.append(f"`nivel tk-{num}`=%s"); set_vals.append(niv)
                set_cols.append(f"`capacidad tk-{num}`=%s"); set_vals.append(cap)

                if tst:
                    ruta = _guardar_testigo(tst, carpeta, f"tk{num}_final_{id_operacion}")
                    set_cols.append(f"`testigo nivel tk-{num}`=%s"); set_vals.append(ruta)

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

            cur.execute("""
                UPDATE cardex_glp
                   SET estatus_lote='INACTIVO'
                 WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND lote=%s
            """, (empresa, ubicacion, lote_id))

            registrar_auditoria(
                empresa_id=id_empresa,
                empresa_nombre=empresa,
                modulo="GLP",
                usuario=usuario,
                accion="Fin Calefacción",
                detalle=f"Sede: {ubicacion}. Lote cerrado: {lote_id}.",
                nivel="INFO"
            )

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

    except ValueError as e:
        mysql.connection.rollback()
        return jsonify({"success": False, "message": str(e)})
    
    except Exception:
        print("⛔ Error en finalizar_calefaccion_batch:")
        traceback.print_exc()
        mysql.connection.rollback()
        return jsonify({"success": False, "message": "Error al finalizar calefacción."})

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