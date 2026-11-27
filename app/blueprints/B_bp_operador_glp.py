# MÓDULO: GAS | SUBMÓDULO: B_bp_operador_glp | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/B_bp_operador_glp.py
from flask import render_template
from flask import Blueprint, jsonify, request, session
from flask import current_app as app
from datetime import datetime, timedelta
from app.utils import login_required_custom, registrar_auditoria
import os
import base64
import smtplib
import traceback
import random
import string
import json
import math
import time
import threading
import re as _re
import requests
import holidays
from MySQLdb import IntegrityError, OperationalError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import make_msgid, formatdate
from app import mysql

bp_glp = Blueprint('bp_glp', __name__, url_prefix='/glp')
bp_operador_glp = bp_glp


# ==========================================
# GESTIÓN DE CONFIGURACIÓN Y ESTADO EN APP.CONFIG (LEY 2)
# ==========================================
def _get_glp_settings():
    """Obtiene configuración y caché de festivos exclusivamente desde current_app.config."""
    if 'GLP_CO_HOLIDAYS' not in app.config:
        app.config['GLP_CO_HOLIDAYS'] = holidays.CO()

    email_user = app.config.get("EMAIL_USER") or os.environ.get("EMAIL_USER")
    email_pass = app.config.get("EMAIL_PASS") or os.environ.get("EMAIL_PASS")
    email_host = app.config.get("EMAIL_HOST") or os.environ.get("EMAIL_HOST", "smtp.gmail.com")
    try:
        email_port = int(app.config.get("EMAIL_PORT") or os.environ.get("EMAIL_PORT", "587"))
    except Exception:
        email_port = 587
    email_from = app.config.get("EMAIL_FROM") or os.environ.get("EMAIL_FROM") or email_user
    email_admin = app.config.get("EMAIL_ADMIN") or os.environ.get("EMAIL_ADMIN", "bqa-one@baquia-esm.com")
    email_alert_baqone = app.config.get("EMAIL_ALERT_BAQONE") or os.environ.get("EMAIL_ALERT_BAQONE") or email_user
    telegram_token = (
        app.config.get("TELEGRAM_TOKEN_GLP")
        or os.environ.get("TELEGRAM_TOKEN_GLP")
        or "8526515342:AAFDZuD3Qu-3Sc5VRfN9Wf_NoGh44YE25oE"
    )

    return {
        "co_holidays": app.config['GLP_CO_HOLIDAYS'],
        "email_user": email_user,
        "email_pass": email_pass,
        "email_host": email_host,
        "email_port": email_port,
        "email_from": email_from,
        "email_admin": email_admin,
        "email_alert_baqone": email_alert_baqone,
        "telegram_token": telegram_token
    }


def _obtener_contexto_sesion(data=None):
    """
    Normaliza y blinda los datos de sesión multi-tenant y evita cedula=None
    en registro_operaciones_bqa_one (Leyes 1 y 8).
    """
    payload = data if isinstance(data, dict) else {}
    raw_id_emp = session.get('empresa_id') or session.get('id_empresa') or 0
    try:
        id_empresa = int(raw_id_emp)
    except Exception:
        id_empresa = 0

    empresa_raw = session.get('empresa') or payload.get('empresa') or 'N/D'
    empresa = _re.sub(r"\s+", " ", str(empresa_raw).replace("\u00A0", " ")).strip() or 'N/D'

    usuario_raw = session.get('nombre') or session.get('usuario') or payload.get('usuario') or 'Operador GLP'
    usuario = str(usuario_raw).strip() or 'Operador GLP'

    cedula_raw = (
        session.get('cedula')
        or session.get('documento')
        or session.get('user_id')
        or payload.get('cedula')
        or 'N/D'
    )
    cedula = str(cedula_raw).strip() if cedula_raw is not None else 'N/D'
    if not cedula or cedula.lower() == 'none':
        cedula = 'N/D'

    perfil = str(session.get('perfil') or 'Operador').strip()

    return {
        "id_empresa": id_empresa,
        "empresa": empresa,
        "usuario": usuario,
        "cedula": cedula,
        "perfil": perfil
    }


def _registrar_operacion_bqa_one(cur, ctx, detalle_datos, backend_emisor="PWA_GLP"):
    """
    Inserta en registro_operaciones_bqa_one respetando estrictamente el esquema de la Ley 8.
    """
    datos_str = json.dumps(detalle_datos, ensure_ascii=False) if isinstance(detalle_datos, (dict, list)) else str(detalle_datos)
    cur.execute("""
        INSERT INTO `registro_operaciones_bqa_one`
            (empresa, empresa_id, backend_emisor, backend, datos, usuario, cedula)
        VALUES
            (%s, %s, %s, 'B_bp_operador_glp', %s, %s, %s)
    """, (
        ctx["empresa"],
        ctx["id_empresa"],
        backend_emisor,
        datos_str,
        ctx["usuario"],
        ctx["cedula"]
    ))


# ==========================================
# NOTIFICACIONES TELEGRAM (ESTRICTO POST-COMMIT)
# ==========================================
def _enviar_mensajes_telegram_hilo(chat_ids, mensaje, token_bot):
    """Envía mensajes a Telegram en segundo plano con reintentos y timeout extendido."""
    if not chat_ids or not token_bot:
        return

    def tarea_envio():
        url = f"https://api.telegram.org/bot{token_bot}/sendMessage"
        for chat_id in chat_ids:
            data = {
                "chat_id": chat_id,
                "text": mensaje,
                "parse_mode": "Markdown"
            }
            for intento in range(3):
                try:
                    resp = requests.post(url, data=data, timeout=10)
                    if resp.status_code == 200:
                        break
                    else:
                        print(f"⚠️ Telegram Error API a {chat_id} (Intento {intento+1}): {resp.text}")
                except Exception as e:
                    print(f"⚠️ Telegram Fallo Red a {chat_id} (Intento {intento+1}): {e}")
                time.sleep(2)

    hilo = threading.Thread(target=tarea_envio, daemon=True)
    hilo.start()


def _obtener_chat_ids_empresa(id_empresa):
    """Consulta aislada por empresa_id de los destinatarios Telegram vinculados (Ley 1)."""
    if not id_empresa:
        return []
    cur = mysql.connection.cursor()
    try:
        cur.execute("""
            SELECT telegram_id
            FROM usuarios
            WHERE empresa_id = %s
              AND telegram_id IS NOT NULL
              AND TRIM(telegram_id) != ''
        """, (id_empresa,))
        rows = cur.fetchall()
        return [str(r['telegram_id'] if isinstance(r, dict) else r[0]).strip() for r in rows if (r['telegram_id'] if isinstance(r, dict) else r[0])]
    finally:
        cur.close()


def _enviar_alerta_telegram_oficial(id_empresa, ubicacion, usuario, nivel, codigo):
    """Envía alerta de solicitud de gas a los usuarios vinculados de la empresa."""
    cfg = _get_glp_settings()
    mensaje = (
        f"🚨 *SOLICITUD DE GAS*\n\n"
        f"📍 Sede: {ubicacion}\n"
        f"👤 User: {usuario}\n"
        f"📉 Nivel: {nivel}%\n"
        f"🆔 Cod: {codigo}\n\n"
        f"⚠️ *Requiere Aprobación*"
    )
    try:
        chat_ids = _obtener_chat_ids_empresa(id_empresa)
        if not chat_ids:
            print(f"⚠️ Telegram: No hay usuarios vinculados para la empresa ID {id_empresa}")
            return False
        _enviar_mensajes_telegram_hilo(chat_ids, mensaje, cfg["telegram_token"])
        return True
    except Exception as e:
        print(f"❌ Error general en multi-envío Telegram: {e}")
        return False


def notificar_opglp_telegram(id_empresa, usuario, operacion, sede, fecha, estado="exito"):
    """Envía la Notificación Maestra de Operaciones GLP solo tras confirmar el resultado real."""
    cfg = _get_glp_settings()
    emoji_estado = "ÉXITO ✅"
    if estado == "fallo":
        emoji_estado = "FALLO ❌"
    elif estado == "sync":
        emoji_estado = "SYNC 🔄"
    elif estado == "pendientes":
        emoji_estado = "PENDIENTES ⚠️"

    mensaje = (
        f"{emoji_estado}\n\n"
        f"🏢 *Empresa ID:* {id_empresa}\n"
        f"📍 *Sede:* {sede}\n"
        f"👤 *Usuario:* {usuario}\n"
        f"⚙️ *Operación:* {operacion}\n"
        f"📅 *Fecha:* {fecha}"
    )

    try:
        chat_ids = _obtener_chat_ids_empresa(id_empresa)
        if not chat_ids:
            return False
        _enviar_mensajes_telegram_hilo(chat_ids, mensaje, cfg["telegram_token"])
        return True
    except Exception as e:
        print(f"❌ Error crítico ejecutando notificar_opglp_telegram: {e}")
        return False


# ==========================================
# UTILIDADES DE BLINDAJE (IDEMPOTENCIA, NORMALIZACIÓN Y AUTO-SANACIÓN)
# ==========================================
def _verificar_idempotencia(id_empresa, op_id, nombre_operacion, sede):
    """Barrera inicial multi-tenant: Revisa si op_id ya fue procesado en cardex_glp."""
    if not op_id or not id_empresa:
        return None
    cur_check = None
    try:
        cur_check = mysql.connection.cursor()
        cur_check.execute("""
            SELECT lote, saldo_estimado_kg, neto_gastado, dias_operacion, codigo_pedido
            FROM cardex_glp
            WHERE id_empresa = %s AND op_id = %s
            LIMIT 1
        """, (id_empresa, op_id))
        row = cur_check.fetchone()
        if row:
            if isinstance(row, dict):
                lote_r = row.get("lote")
                saldo_r = float(row.get("saldo_estimado_kg") or 0.0)
                gasto_r = float(row.get("neto_gastado") or 0.0)
                dias_r = int(row.get("dias_operacion") or 1)
                cod_r = row.get("codigo_pedido")
            else:
                lote_r = row[0]
                saldo_r = float(row[1] or 0.0)
                gasto_r = float(row[2] or 0.0)
                dias_r = int(row[3] or 1)
                cod_r = row[4]

            return jsonify({
                "success": True,
                "idempotent": True,
                "message": "Operación recuperada (Ya estaba registrada previamente).",
                "resumen": {
                    "operacion": nombre_operacion,
                    "sede": sede,
                    "lote": lote_r,
                    "saldo_estimado_kg": round(saldo_r, 2),
                    "kg_consumidos": round(gasto_r, 2),
                    "dias_operacion": dias_r,
                    "codigo_pedido": cod_r,
                    "op_id": op_id
                }
            }), 200
    except Exception:
        pass
    finally:
        if cur_check:
            try:
                cur_check.close()
            except Exception:
                pass
    return None


def _manejar_error_idempotencia(error, id_empresa, op_id, nombre_operacion, sede):
    """Barrera final: Captura el error 1062 de MySQL ante condición de carrera en op_id."""
    mysql.connection.rollback()
    if hasattr(error, 'args') and len(error.args) > 0 and error.args[0] == 1062:
        verif = _verificar_idempotencia(id_empresa, op_id, nombre_operacion, sede)
        if verif:
            return verif
        return jsonify({
            "success": True,
            "idempotent": True,
            "message": "Operación recuperada (Ya estaba registrada).",
            "resumen": {"operacion": nombre_operacion, "sede": sede, "op_id": op_id}
        }), 200
    return jsonify({"success": False, "message": f"Error BD: {str(error)}"}), 400


def _normalize_sede(s):
    """Normaliza 'sede' aceptando formatos 'SEDE | EMPRESA' o espacios no estándar."""
    s = (s or "").strip()
    if "|" in s:
        s = s.split("|", 1)[0]
    s = _re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()
    return s


def _extraer_fecha_iso(valor_fecha):
    """Extrae de forma segura un objeto date o string YYYY-MM-DD desde inputs date o datetime-local."""
    if not valor_fecha:
        return None
    texto = str(valor_fecha).strip()
    if "T" in texto:
        texto = texto.split("T", 1)[0]
    elif " " in texto:
        texto = texto.split(" ", 1)[0]
    try:
        return datetime.strptime(texto[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _guardar_testigo(base64_data, carpeta, nombre_archivo):
    """Guarda un archivo de testigo (imagen) en app/static y devuelve la ruta relativa /static/..."""
    if not base64_data:
        return None

    try:
        data_match = _re.match(r'data:image/(?P<ext>png|jpeg|jpg|webp);base64,(?P<data>.+)', base64_data)
        if not data_match:
            if "," in base64_data:
                base64_data = base64_data.split(",", 1)[1]
            ext = 'jpg'
            binary_data = base64.b64decode(base64_data)
        else:
            ext = data_match.group('ext')
            if ext == 'jpeg':
                ext = 'jpeg'
            binary_data = base64.b64decode(data_match.group('data'))

        static_dir = os.path.join(app.static_folder, carpeta)
        if not os.path.exists(static_dir):
            os.makedirs(static_dir, exist_ok=True)

        filename = f"{nombre_archivo}.{ext}"
        file_path = os.path.join(static_dir, filename)

        with open(file_path, 'wb') as f:
            f.write(binary_data)

        ruta_relativa = os.path.relpath(file_path, app.static_folder).replace(os.path.sep, "/")
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


def _autosanar_estado_lote(cur, ctx, sede, nom_lote, id_inicio, registro_inicio, fecha_lote_dt):
    """
    Auto-sanación transaccional de inconsistencias heredadas:
    1) Si un lote activo sin '| Conf. por ' ya tiene operaciones posteriores (tanqueo o consumo)
       en cardex_glp (ej. Granja Chucuní / Granja Cubana), convalida automáticamente la marca de arribo.
    2) Si existen pedidos abiertos en pedidos_gas_glp para este lote (incluyendo 'pendiente_envio_auto'
       o variantes de mayúsculas/espacios en cliente como Granja San Miguel), pero en cardex_glp ya se
       registró un tanqueo en fecha igual o posterior a la generación del pedido, los cierra como
       'tanqueo_registrado' y registra auditoría en registro_operaciones_bqa_one.
    Retorna (arribo_confirmado: bool, pedido_pendiente_codigo: str|None)
    """
    id_empresa = ctx["id_empresa"]
    empresa_nombre = ctx["empresa"]
    limite_fecha = datetime(2026, 6, 15).date()
    hubo_mutacion = False

    arribo_confirmado = True
    if fecha_lote_dt >= limite_fecha and "| Conf. por " not in (registro_inicio or ""):
        cur.execute("""
            SELECT COUNT(*) AS total_ops
            FROM cardex_glp
            WHERE id_empresa = %s
              AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
              AND lote = %s
              AND operacion IN ('tanqueo', 'consumo')
        """, (id_empresa, sede, nom_lote))
        row_ops = cur.fetchone()
        total_ops = int((row_ops.get("total_ops") if isinstance(row_ops, dict) else row_ops[0]) or 0) if row_ops else 0

        if total_ops > 0 and id_inicio:
            sello_auto = " | Conf. por Sistema (Convalidación Operativa)"
            cur.execute("""
                UPDATE cardex_glp
                SET registro = CONCAT(COALESCE(registro, ''), %s)
                WHERE id = %s AND id_empresa = %s
            """, (sello_auto, id_inicio, id_empresa))
            _registrar_operacion_bqa_one(
                cur,
                ctx,
                f"Auto-convalidación arribo por operaciones previas - Lote: {nom_lote} - Sede: {sede}",
                backend_emisor="AUTO_HEAL_GLP"
            )
            hubo_mutacion = True
            arribo_confirmado = True
        else:
            arribo_confirmado = False

    # Verificar el último tanqueo real registrado en cardex_glp para este lote
    cur.execute("""
        SELECT MAX(fecha) AS ultima_fecha_tanqueo, COUNT(*) AS cant_tanqueos
        FROM cardex_glp
        WHERE id_empresa = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
          AND operacion = 'tanqueo'
    """, (id_empresa, sede, nom_lote))
    row_tq = cur.fetchone()
    ultima_fecha_tq = None
    cant_tanqueos = 0
    if row_tq:
        ultima_fecha_tq = row_tq.get("ultima_fecha_tanqueo") if isinstance(row_tq, dict) else row_tq[0]
        cant_tanqueos = int((row_tq.get("cant_tanqueos") if isinstance(row_tq, dict) else row_tq[1]) or 0)
        if isinstance(ultima_fecha_tq, str):
            ultima_fecha_tq = _extraer_fecha_iso(ultima_fecha_tq)
        elif isinstance(ultima_fecha_tq, datetime):
            ultima_fecha_tq = ultima_fecha_tq.date()

    # Consultar pedidos abiertos del lote ignorando diferencias de mayúsculas/espacios en cliente
    cur.execute("""
        SELECT id, codigo_pedido, COALESCE(DATE(fecha_solicitud), DATE(fecha_registro)) AS fecha_ped
        FROM pedidos_gas_glp
        WHERE TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
          AND estatus_flujo IN ('enviado_auto', 'aprobado_webmaster', 'pendiente_aprobacion', 'pendiente_envio_auto')
          AND COALESCE(estatus, '') NOT IN ('cancelado', 'anulado')
        ORDER BY id DESC
    """, (empresa_nombre, sede, nom_lote))
    pedidos_abiertos = cur.fetchall()

    pedido_vigente_codigo = None
    ids_para_sanear = []
    codigos_saneados = []

    for ped in pedidos_abiertos:
        if isinstance(ped, dict):
            pid = ped.get("id")
            pcod = ped.get("codigo_pedido")
            pfec = ped.get("fecha_ped")
        else:
            pid = ped[0]
            pcod = ped[1]
            pfec = ped[2]

        if isinstance(pfec, str):
            pfec = _extraer_fecha_iso(pfec)
        elif isinstance(pfec, datetime):
            pfec = pfec.date()

        # También revisar si ese codigo_pedido ya aparece cumplido en una fila de tanqueo
        cur.execute("""
            SELECT 1
            FROM cardex_glp
            WHERE id_empresa = %s
              AND lote = %s
              AND operacion = 'tanqueo'
              AND codigo_pedido = %s
            LIMIT 1
        """, (id_empresa, nom_lote, pcod))
        ya_en_cardex = cur.fetchone() is not None

        cubierto_por_tanqueo_posterior = (
            cant_tanqueos > 0
            and ultima_fecha_tq is not None
            and (pfec is None or ultima_fecha_tq >= pfec)
        )

        if ya_en_cardex or cubierto_por_tanqueo_posterior:
            ids_para_sanear.append(pid)
            codigos_saneados.append(pcod)
        elif pedido_vigente_codigo is None:
            pedido_vigente_codigo = pcod

    if ids_para_sanear:
        placeholders = ", ".join(["%s"] * len(ids_para_sanear))
        cur.execute(f"""
            UPDATE pedidos_gas_glp
            SET estatus_flujo = 'tanqueo_registrado',
                comentarios = CONCAT(COALESCE(comentarios, ''), ' | Saneado auto por tanqueo registrado en Cardex')
            WHERE id IN ({placeholders})
              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
        """, tuple(ids_para_sanear) + (empresa_nombre,))
        _registrar_operacion_bqa_one(
            cur,
            ctx,
            f"Auto-cierre de pedidos ya tanqueados ({', '.join(codigos_saneados)}) - Lote: {nom_lote} - Sede: {sede}",
            backend_emisor="AUTO_HEAL_GLP"
        )
        hubo_mutacion = True

    if hubo_mutacion:
        mysql.connection.commit()

    return arribo_confirmado, pedido_vigente_codigo


def _calcular_actualizar_dias_operacion(cur, id_empresa, ubicacion, lote_id, fecha_actual=None):
    """
    Calcula y actualiza los días de operación de un lote en cardex_glp filtrando por id_empresa (Ley 1).
    """
    if fecha_actual is None:
        fecha_actual = datetime.now().date()
    elif isinstance(fecha_actual, datetime):
        fecha_actual = fecha_actual.date()

    cur.execute("""
        SELECT COALESCE(fecha_llegada_pollitos, fecha) AS fecha_ini
        FROM cardex_glp
        WHERE id_empresa = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
          AND operacion = 'inicio_calefaccion'
        LIMIT 1
    """, (id_empresa, ubicacion, lote_id))

    row = cur.fetchone()
    fecha_ini = None
    if row:
        fecha_ini = row.get("fecha_ini") if isinstance(row, dict) else row[0]

    dias = 1
    if fecha_ini:
        try:
            if isinstance(fecha_ini, str):
                fecha_ini = _extraer_fecha_iso(fecha_ini)
            elif isinstance(fecha_ini, datetime):
                fecha_ini = fecha_ini.date()

            if fecha_ini:
                dias = (fecha_actual - fecha_ini).days + 1
                if dias <= 0:
                    dias = 0
        except Exception as e:
            print(f"Error calculando días: {e}")
            dias = 1

    cur.execute("""
        UPDATE cardex_glp
        SET dias_operacion = %s
        WHERE id_empresa = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
    """, (dias, id_empresa, ubicacion, lote_id))

    return dias


def _calcular_consumo_lote(cur, id_empresa, ubicacion, lote, id_operacion_actual, tanques):
    """
    Calcula el consumo en kg del intervalo, la eficiencia acumulada (kg_pollito) y la velocidad diaria,
    aislando estrictamente por id_empresa (insensible a variantes de nombre de empresa en sesión).
    Lanza ValueError si algún tanque subió de nivel sin tanqueo previo.
    """
    if not tanques:
        return 0.0, 0.0, 0

    consumo_total_kg = 0.0
    errores_detectados = []

    for t in tanques:
        num_str = str(t.get('numero', ''))
        match = _re.search(r'\d+', num_str)
        num_tanque = match.group() if match else ""
        if not num_tanque:
            continue

        valor_actual_cierre = t.get('nivel')
        if valor_actual_cierre is None:
            valor_actual_cierre = t.get('nivel_inicial')

        try:
            valor_actual_cierre = float(valor_actual_cierre)
        except Exception:
            valor_actual_cierre = 0.0

        try:
            capacidad_galones = float(t.get('capacidad') or 250)
        except Exception:
            capacidad_galones = 250.0

        query = f"""
            SELECT operacion,
                   `nivel tk-{num_tanque}`,
                   `nivelfinal tk-{num_tanque}`,
                   densidad_suministrada
            FROM cardex_glp
            WHERE id_empresa = %s
              AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
              AND lote = %s
              AND id < %s
              AND (
                   `nivel tk-{num_tanque}` IS NOT NULL
                   OR `nivelfinal tk-{num_tanque}` IS NOT NULL
              )
            ORDER BY fecha DESC, id DESC
            LIMIT 1
        """
        cur.execute(query, (id_empresa, ubicacion, lote, id_operacion_actual))
        prev_raw = cur.fetchone()
        if not prev_raw:
            continue

        if isinstance(prev_raw, dict):
            prev = prev_raw
        else:
            prev = {
                'operacion': prev_raw[0],
                f'nivel tk-{num_tanque}': prev_raw[1],
                f'nivelfinal tk-{num_tanque}': prev_raw[2],
                'densidad_suministrada': prev_raw[3]
            }

        op_anterior = prev.get('operacion')
        densidad_calculo = 2.0

        if op_anterior == 'tanqueo' and prev.get(f'nivelfinal tk-{num_tanque}') is not None:
            nivel_anterior = float(prev.get(f'nivelfinal tk-{num_tanque}') or 0.0)
            d_real = float(prev.get('densidad_suministrada') or 0.0)
            if d_real > 0:
                densidad_calculo = d_real
        else:
            nivel_anterior = float(prev.get(f'nivel tk-{num_tanque}') or 0.0)

        delta_pct = nivel_anterior - valor_actual_cierre

        if delta_pct < -0.1:
            errores_detectados.append(
                f"• TK-{num_tanque}: Ingresó {valor_actual_cierre}%, anterior {nivel_anterior}% (SUBIÓ)."
            )

        if delta_pct < 0:
            delta_pct = 0.0

        kg_tanque = (delta_pct / 100.0) * capacidad_galones * densidad_calculo
        consumo_total_kg += kg_tanque

    if errores_detectados:
        mensaje_final = (
            "⛔ ERRORES DETECTADOS:\n\n"
            + "\n".join(errores_detectados)
            + "\n\nEl nivel no puede subir sin un tanqueo."
        )
        raise ValueError(mensaje_final)

    cur.execute("""
        SELECT pollitos
        FROM cardex_glp
        WHERE id_empresa = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
          AND operacion = 'inicio_calefaccion'
        ORDER BY fecha ASC, id ASC
        LIMIT 1
    """, (id_empresa, ubicacion, lote))
    row_ini = cur.fetchone()
    pollitos = 0
    if row_ini:
        val_p = row_ini.get("pollitos") if isinstance(row_ini, dict) else row_ini[0]
        pollitos = int(val_p or 0)

    cur.execute("""
        SELECT SUM(neto_gastado) AS total_previo
        FROM cardex_glp
        WHERE id_empresa = %s
          AND lote = %s
          AND id < %s
    """, (id_empresa, lote, id_operacion_actual))
    row_sum = cur.fetchone()
    consumo_previo = 0.0
    if row_sum:
        val_s = row_sum.get('total_previo') if isinstance(row_sum, dict) else row_sum[0]
        consumo_previo = float(val_s or 0.0)

    consumo_acumulado_total = consumo_previo + consumo_total_kg
    kg_pollito_acumulado = (consumo_acumulado_total / float(pollitos)) if pollitos > 0 else 0.0

    cur.execute("""
        UPDATE cardex_glp
        SET kg_pollito = %s,
            neto_gastado = %s
        WHERE id = %s AND id_empresa = %s
    """, (kg_pollito_acumulado, consumo_total_kg, id_operacion_actual, id_empresa))

    if pollitos > 0 and consumo_total_kg > 0:
        try:
            cur.execute("""
                SELECT fecha
                FROM cardex_glp
                WHERE id_empresa = %s
                  AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                  AND lote = %s
                  AND id <= %s
                ORDER BY fecha DESC, id DESC
                LIMIT 2
            """, (id_empresa, ubicacion, lote, id_operacion_actual))
            fechas_rows = cur.fetchall()
            dias_intervalo = 1.0

            if len(fechas_rows) >= 2:
                f_curr = fechas_rows[0].get('fecha') if isinstance(fechas_rows[0], dict) else fechas_rows[0][0]
                f_prev = fechas_rows[1].get('fecha') if isinstance(fechas_rows[1], dict) else fechas_rows[1][0]
                if isinstance(f_curr, str):
                    f_curr = _extraer_fecha_iso(f_curr)
                elif isinstance(f_curr, datetime):
                    f_curr = f_curr.date()
                if isinstance(f_prev, str):
                    f_prev = _extraer_fecha_iso(f_prev)
                elif isinstance(f_prev, datetime):
                    f_prev = f_prev.date()

                if f_curr and f_prev:
                    delta = (f_curr - f_prev).days
                    dias_intervalo = float(delta) if delta > 0 else 1.0

            velocidad = (consumo_total_kg / float(pollitos)) / dias_intervalo
            cur.execute("""
                UPDATE cardex_glp
                SET velocidad_consumo = %s
                WHERE id = %s AND id_empresa = %s
            """, (velocidad, id_operacion_actual, id_empresa))
        except Exception as e:
            print(f"⚠️ Error calculando velocidad_consumo: {e}")

    return consumo_total_kg, kg_pollito_acumulado, pollitos


def _buscar_proveedor_principal(cur, id_empresa, ubicacion, tanques):
    if tanques:
        primer_num = str(tanques[0].get("numero") or "").upper().strip()
        if primer_num:
            cur.execute("""
                SELECT proveedor
                FROM tanques_sedes
                WHERE empresa_id = %s
                  AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                  AND UPPER(nombre_tanque) = UPPER(%s)
                LIMIT 1
            """, (id_empresa, ubicacion, primer_num))
            p = cur.fetchone()
            if p:
                return p.get("proveedor") if isinstance(p, dict) else p[0]

    cur.execute("""
        SELECT proveedor
        FROM tanques_sedes
        WHERE empresa_id = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND proveedor IS NOT NULL
        LIMIT 1
    """, (id_empresa, ubicacion))
    p_any = cur.fetchone()
    if p_any:
        return p_any.get("proveedor") if isinstance(p_any, dict) else p_any[0]
    return None


def _generar_codigo_pedido(cliente_nombre, lote_id, ubicacion, proveedor, cur):
    mes = datetime.now().strftime("%m")
    partes = _re.sub(r'[^a-zA-Z\s]', '', str(cliente_nombre or '')).upper().split()
    iniciales = "".join(p[0] for p in partes if len(p) > 2)[:3].ljust(3, 'X')

    codigo_pedido = None
    for _ in range(15):
        sufijo = ''.join(random.choices(string.digits, k=4))
        candidato = f"{mes}-{iniciales}-{sufijo}"
        cur.execute(
            "SELECT codigo_pedido FROM pedidos_gas_glp WHERE codigo_pedido = %s AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))",
            (candidato, cliente_nombre)
        )
        if cur.fetchone() is None:
            cur.execute("SELECT codigo_pedido FROM pedidos_gas_glp WHERE codigo_pedido = %s", (candidato,))
            if cur.fetchone() is None:
                codigo_pedido = candidato
                break

    if codigo_pedido is None:
        raise Exception("No se pudo generar un código de pedido único.")

    cur.execute("""
        INSERT INTO pedidos_gas_glp
            (cliente, codigo_pedido, estatus, fecha_registro, lote, ubicacion, proveedor)
        VALUES
            (%s, %s, 'generado', NOW(), %s, %s, %s)
    """, (cliente_nombre, codigo_pedido, lote_id, ubicacion, proveedor))
    return codigo_pedido


# ==========================================
# NOTIFICACIONES SMTP CORPORATIVAS (POST-COMMIT)
# ==========================================
def _enviar_alerta_webmaster_nueva_solicitud(empresa, ubicacion, usuario, nivel_actual, codigo_pedido):
    cfg = _get_glp_settings()
    email_user = cfg["email_user"]
    email_pass = cfg["email_pass"]
    email_webmaster = cfg["email_admin"]

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
                <li><strong>Empresa:</strong> {empresa}</li>
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
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_user

        with smtplib.SMTP(cfg["email_host"], cfg["email_port"]) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, [email_webmaster], msg.as_string())
        return True
    except Exception as e:
        print(f"❌ Error alerta webmaster: {e}")
        return False


def _obtener_emails_proveedor(proveedor_principal):
    if not proveedor_principal:
        return []
    cur = mysql.connection.cursor()
    try:
        cur.execute(
            "SELECT email1, email2 FROM proveedores WHERE TRIM(UPPER(proveedor)) = TRIM(UPPER(%s)) LIMIT 1",
            (proveedor_principal,)
        )
        c_raw = cur.fetchone()
        if not c_raw:
            return []
        if isinstance(c_raw, dict):
            return [e for e in [c_raw.get("email1"), c_raw.get("email2")] if e]
        return [e for e in [c_raw[0], c_raw[1]] if e]
    finally:
        cur.close()


def _enviar_alerta_pedido_tanqueo(empresa, ubicacion, lote_id, proveedor_principal, tanques_bajos, codigo_pedido):
    if not tanques_bajos:
        return False

    cfg = _get_glp_settings()
    email_user = cfg["email_user"]
    email_pass = cfg["email_pass"]
    email_from = cfg["email_from"]

    if not email_user or not email_pass:
        app.logger.error("⛔ Error: Credenciales de correo no configuradas.")
        return False

    destinatarios = _obtener_emails_proveedor(proveedor_principal)
    if not destinatarios and email_user:
        destinatarios = [email_user]
    if not destinatarios:
        return False

    items_html = ""
    for t in (tanques_bajos or []):
        numero = t.get("numero") if isinstance(t, dict) else None
        nivel = t.get("nivel") if isinstance(t, dict) else None
        if nivel is None and isinstance(t, dict):
            nivel = t.get("nivel_inicial")
        try:
            nivel_float = float(nivel if nivel is not None else 0)
        except Exception:
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
                    <p><strong>Lote:</strong> {lote_id}</p>
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
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_from

        with smtplib.SMTP(cfg["email_host"], cfg["email_port"]) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())
        return True
    except Exception as e:
        app.logger.error(f"⛔ Error al enviar correo GLP: {e}")
        return False


def _enviar_alerta_desviacion_tanqueo(
    empresa, ubicacion, lote_id, proveedor_principal, op_id,
    masa_esperada_total, masa_facturada_total, desvio_total_pct, dens_prom, tanques
):
    """Envía correo de alerta de desviación (>8%) con diseño corporativo."""
    cfg = _get_glp_settings()
    email_user = cfg["email_user"]
    email_pass = cfg["email_pass"]
    email_from = cfg["email_from"]
    email_baqone = cfg["email_alert_baqone"]

    if not email_user or not email_pass:
        return False

    destinatarios = _obtener_emails_proveedor(proveedor_principal)
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
                <p>Se ha registrado un tanqueo con una diferencia significativa entre la masa esperada y la facturada ({empresa} - Lote {lote_id}).</p>
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
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_from

        with smtplib.SMTP(cfg["email_host"], cfg["email_port"]) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())
        return True
    except Exception as e:
        app.logger.error(f"⛔ Error al enviar correo desviación: {e}")
        return False


def _enviar_alerta_diferencia_saldos(empresa, ubicacion, usuario, lote_id, tanques_alerta):
    """Envía correo corporativo si hay caída > 10% en inicio de calefacción (Flameo)."""
    cfg = _get_glp_settings()
    email_user = cfg["email_user"]
    email_pass = cfg["email_pass"]
    email_from = cfg["email_from"]
    email_admin = cfg["email_admin"]

    if not email_user or not email_pass or not tanques_alerta:
        return False

    items_html = ""
    for t in tanques_alerta:
        items_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 8px; text-align: center;"><strong>{t['numero']}</strong></td>
                <td style="padding: 8px; text-align: center;">{round(t['nivel_anterior'], 1)}%</td>
                <td style="padding: 8px; text-align: center;">{round(t['nivel_nuevo'], 1)}%</td>
                <td style="padding: 8px; background-color: #ffebee; color: #c62828; text-align: center;"><strong>-{round(t['diferencia'], 1)}%</strong></td>
            </tr>
        """

    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
        .container {{ max-width: 650px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
        .header {{ background-color: #d9534f; color: #ffffff; padding: 20px; text-align: center; }}
        .header h2 {{ margin: 0; font-size: 22px; }}
        .content {{ padding: 25px; }}
        .alert-box {{ background-color: #ffebee; border-left: 4px solid #c62828; color: #c62828; padding: 15px; border-radius: 4px; font-size: 15px; margin-bottom: 20px; }}
        .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 15px; margin-bottom: 20px; }}
        .info-item {{ background: #f9fbfb; padding: 12px; border-radius: 6px; border-left: 3px solid #d9534f; }}
        .info-label {{ font-size: 12px; color: #777; display: block; margin-bottom: 4px; }}
        .info-value {{ font-size: 15px; font-weight: bold; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; margin-top: 15px; }}
        th {{ background-color: #f2f2f2; padding: 10px; text-align: center; color: #555; }}
        .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
    </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h2>⚠️ Alerta: Diferencia de Saldo en Inicio de Lote</h2>
            </div>
            <div class="content">
                <div class="alert-box">
                    <strong>Atención:</strong> Se ha detectado una caída de nivel de gas mayor al 10% durante el período de vacío sanitario (flameo) en el inicio de un nuevo lote.
                </div>
                <div class="info-grid">
                    <div class="info-item"><span class="info-label">Empresa</span><span class="info-value">{empresa}</span></div>
                    <div class="info-item"><span class="info-label">Sede</span><span class="info-value">{ubicacion}</span></div>
                    <div class="info-item"><span class="info-label">Lote Iniciado</span><span class="info-value">{lote_id}</span></div>
                    <div class="info-item"><span class="info-label">Usuario Operador</span><span class="info-value">{usuario}</span></div>
                </div>
                <h3 style="color: #444; border-bottom: 1px solid #ddd; padding-bottom: 5px;">Detalle de Tanques Afectados</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Tanque</th>
                            <th>Saldo Anterior (Cierre)</th>
                            <th>Nivel de Arranque</th>
                            <th>Pérdida Detectada</th>
                        </tr>
                    </thead>
                    <tbody>
                        {items_html}
                    </tbody>
                </table>
                <p style="margin-top: 20px; font-size: 12px; color: #777;">
                    * El sistema ha permitido el inicio de la calefacción para no detener la operación, pero se requiere conciliación contable de esta diferencia.
                </p>
            </div>
            <div class="footer">
                <p>Reporte automático generado por <strong>BQA-ONE / Energix360</strong>.</p>
            </div>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"🚨 Alerta Saldo GLP (>10%) Flameo - Sede: {ubicacion}"
        msg["From"] = email_from
        msg["To"] = email_admin
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_from

        with smtplib.SMTP(cfg["email_host"], cfg["email_port"]) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, [email_admin], msg.as_string())
        return True
    except Exception as e:
        app.logger.error(f"⛔ Error al enviar alerta de flameo: {e}")
        return False


def _enviar_alerta_pedido_inicio(empresa, ubicacion, lote_id, proveedor_principal, tanques, codigo_pedido):
    """Envía solicitud automática de tanqueo al iniciar calefacción (<70%)."""
    cfg = _get_glp_settings()
    email_user = cfg["email_user"]
    email_pass = cfg["email_pass"]
    email_from = cfg["email_from"]

    if not email_user or not email_pass:
        return False

    destinatarios = _obtener_emails_proveedor(proveedor_principal)
    if not destinatarios and email_user:
        destinatarios = [email_user]
    if not destinatarios:
        return False

    items_html = ""
    for t in tanques:
        try:
            num = t.get("numero") or "N/A"
            nivel = float(t.get("nivel") or 0)
        except Exception:
            num = "Ref"
            nivel = 0.0

        items_html += f"""
            <tr style="border-bottom: 1px solid #eee;">
                <td style="padding: 10px; text-align: center;"><strong>{num}</strong></td>
                <td style="padding: 10px; text-align: center; color: #d9534f;">{nivel}%</td>
                <td style="padding: 10px; text-align: center; font-weight:bold; color: #015249;">80%</td>
            </tr>
        """

    cuerpo = f"""
    <!DOCTYPE html>
    <html lang="es">
    <body style="background-color: #f4f4f4; padding: 20px; font-family: Arial, sans-serif;">
        <div style="background-color: #ffffff; max-width: 500px; margin: 0 auto; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); overflow: hidden;">
            <div style="background-color: #015249; color: white; padding: 20px; text-align: center;">
                <h2 style="margin: 0; font-size: 22px;">Orden de Suministro GLP</h2>
                <p style="margin: 5px 0 0 0; opacity: 0.9; font-size: 14px;">Protocolo de Inicio de Calefacción ({empresa})</p>
            </div>
            <div style="padding: 25px;">
                <p style="color: #333; margin-bottom: 15px;">Estimado proveedor <strong>{proveedor_principal}</strong>,</p>
                <p style="color: #555; line-height: 1.5;">
                    Se ha iniciado un nuevo ciclo productivo en la sede <strong>{ubicacion}</strong> (Lote: {lote_id}).
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
                <p style="margin: 0; color: #999; font-size: 11px;">Energix360 System</p>
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
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_from

        with smtplib.SMTP(cfg["email_host"], cfg["email_port"]) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())
        return True
    except Exception as e:
        print(f"Error email inicio: {e}")
        return False


# ==========================================
# CEREBRO MATEMÁTICO & COPILOTO MULTI-TENANT
# ==========================================
def _calcular_velocidad_consumo(cur, id_empresa, ubicacion, lote):
    """Calcula la derivada del consumo (% por día) filtrada por id_empresa (Ley 1)."""
    cur.execute("""
        SELECT fecha, `nivel tk-1`, `nivel tk-2`
        FROM cardex_glp
        WHERE id_empresa = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
          AND operacion IN ('inicio_calefaccion', 'consumo')
        ORDER BY fecha DESC, id DESC
        LIMIT 5
    """, (id_empresa, ubicacion, lote))

    rows = cur.fetchall()
    if not rows or len(rows) < 2:
        return 8.0

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

        if isinstance(fecha, str):
            fecha = _extraer_fecha_iso(fecha)
        elif isinstance(fecha, datetime):
            fecha = fecha.date()

        avg_nivel = v1 if v2 == 0 else (v1 + v2) / 2.0
        data_points.append({'fecha': fecha, 'nivel': avg_nivel})

    for i in range(len(data_points) - 1):
        actual = data_points[i]
        anterior = data_points[i + 1]
        if not actual['fecha'] or not anterior['fecha']:
            continue
        diff_dias = (actual['fecha'] - anterior['fecha']).days
        if diff_dias <= 0:
            diff_dias = 1
        diff_nivel = anterior['nivel'] - actual['nivel']
        if diff_nivel > 0:
            deltas.append(diff_nivel / diff_dias)

    if not deltas:
        return 8.0
    velocidad_promedio = sum(deltas) / len(deltas)
    return max(velocidad_promedio, 1.0)


def _calcular_eficiencia_acumulada(cur, id_empresa, lote):
    """Calcula la integral del consumo vs pollitos filtrada por id_empresa (Ley 1)."""
    cur.execute("""
        SELECT pollitos
        FROM cardex_glp
        WHERE id_empresa = %s AND lote = %s AND operacion = 'inicio_calefaccion'
        LIMIT 1
    """, (id_empresa, lote))
    p_row = cur.fetchone()
    pollitos = int((p_row.get('pollitos') if isinstance(p_row, dict) else p_row[0]) or 0) if p_row else 0
    if pollitos <= 0:
        return 0.0

    cur.execute("""
        SELECT SUM(neto_gastado) AS total_kg
        FROM cardex_glp
        WHERE id_empresa = %s AND lote = %s
    """, (id_empresa, lote))
    s_row = cur.fetchone()
    total_kg = float((s_row.get('total_kg') if isinstance(s_row, dict) else s_row[0]) or 0.0) if s_row else 0.0
    return total_kg / float(pollitos)


def _analizar_riesgo_glp(cur, id_empresa, empresa, ubicacion, lote, nivel_actual_ponderado, fecha_actual):
    """Algoritmo de proyección y umbral crítico unificado al 30% con aislamiento multi-tenant."""
    cfg = _get_glp_settings()
    co_holidays = cfg["co_holidays"]
    requiere_gas = False
    razon = ""
    sugerencia_carga = 0
    dias_extra = 0
    dias_operacion = 1

    try:
        cur.execute("""
            SELECT dias_extra
            FROM pedidos_gas_glp
            WHERE TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
              AND lote = %s
              AND estatus_flujo IN ('aprobado_webmaster', 'enviado_auto')
            ORDER BY id DESC
            LIMIT 1
        """, (empresa, lote))
        row_extra = cur.fetchone()
        if row_extra:
            dias_extra = int((row_extra['dias_extra'] if isinstance(row_extra, dict) else row_extra[0]) or 0)

        ciclo_total = 15 + dias_extra

        cur.execute("SELECT MIN(fecha) AS min_f FROM cardex_glp WHERE id_empresa = %s AND lote = %s", (id_empresa, lote))
        row_ini = cur.fetchone()
        fecha_ini = (row_ini.get('min_f') if isinstance(row_ini, dict) else row_ini[0]) if row_ini else fecha_actual
        if isinstance(fecha_ini, str):
            fecha_ini = _extraer_fecha_iso(fecha_ini) or fecha_actual
        elif isinstance(fecha_ini, datetime):
            fecha_ini = fecha_ini.date()
        if isinstance(fecha_actual, datetime):
            fecha_actual = fecha_actual.date()

        dias_operacion = (fecha_actual - fecha_ini).days + 1
        dias_restantes = ciclo_total - dias_operacion

        velocidad = _calcular_velocidad_consumo(cur, id_empresa, ubicacion, lote)

        dias_cobertura_necesaria = 1
        check_date = fecha_actual + timedelta(days=1)
        es_puente = False

        while check_date in co_holidays or check_date.weekday() >= 5:
            dias_cobertura_necesaria += 1
            check_date += timedelta(days=1)
            es_puente = True

        nivel_proyectado_fin_cobertura = nivel_actual_ponderado - (velocidad * dias_cobertura_necesaria)
        piso_seguridad = 30.0

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

        sugerencia_carga = max(0, min(85, sugerencia_carga))

    except Exception:
        if nivel_actual_ponderado < 30:
            return {
                "requiere_gas": True,
                "razon": "Fallback seguridad",
                "sugerencia_carga": 50,
                "dias_actuales": 0,
                "dias_extra_active": 0
            }

    return {
        "requiere_gas": requiere_gas,
        "razon": razon,
        "sugerencia_carga": int(sugerencia_carga),
        "dias_actuales": dias_operacion,
        "dias_extra_active": dias_extra
    }


# =========================================================
# ENDPOINTS OPERATIVOS PRINCIPALES
# =========================================================
@bp_glp.route('/context', methods=['GET'])
@login_required_custom
def glp_context():
    """Endpoint de operatividad PWA protegido por sesión activa (Ley 3)."""
    ctx = _obtener_contexto_sesion()
    return jsonify({
        "success": True,
        "message": "Servidor disponible",
        "empresa_id": ctx["id_empresa"]
    })


@bp_glp.route('/obtener_tanques', methods=['POST'])
@login_required_custom
def obtener_tanques():
    try:
        try:
            data = request.get_json(force=True) or {}
        except Exception:
            return jsonify({"success": False, "tanques": [], "message": "JSON inválido"}), 400

        sede = _normalize_sede(data.get('sede'))
        ctx = _obtener_contexto_sesion(data)
        id_empresa = ctx["id_empresa"]

        if not sede or not id_empresa:
            return jsonify({"success": False, "message": "Sesión expirada o sede faltante. Por favor reingresa."})

        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT id, lote, pollitos, fecha, registro
                FROM cardex_glp
                WHERE id_empresa = %s
                  AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                  AND estatus_lote = 'ACTIVO'
                  AND operacion = 'inicio_calefaccion'
                ORDER BY id DESC
                LIMIT 1
            """, (id_empresa, sede))

            lote_row = cur.fetchone()
            lote_activo = False
            info_lote = ""
            bloqueado_por_arribo = False
            motivo_bloqueo = ""
            hay_pedido_pendiente = False
            codigo_pedido_pendiente = ""
            bloqueado_por_pedido = False
            motivo_bloqueo_pedido = ""

            fecha_lote_dt = datetime.now().date()
            limite_fecha = datetime(2026, 6, 15).date()

            if lote_row:
                lote_activo = True
                if isinstance(lote_row, dict):
                    id_inicio = lote_row.get("id")
                    nom_lote = lote_row.get("lote")
                    polli = lote_row.get("pollitos")
                    fecha_lote = lote_row.get("fecha")
                    registro_lote = lote_row.get("registro") or ""
                else:
                    id_inicio = lote_row[0]
                    nom_lote = lote_row[1]
                    polli = lote_row[2]
                    fecha_lote = lote_row[3]
                    registro_lote = lote_row[4] or ""

                info_lote = f"Lote: {nom_lote} | Aves: {polli if polli is not None else 'Pendiente'}"

                if isinstance(fecha_lote, str):
                    fecha_lote_dt = _extraer_fecha_iso(fecha_lote) or datetime.now().date()
                elif isinstance(fecha_lote, datetime):
                    fecha_lote_dt = fecha_lote.date()
                else:
                    fecha_lote_dt = fecha_lote

                arribo_ok, cod_pend = _autosanar_estado_lote(
                    cur, ctx, sede, nom_lote, id_inicio, registro_lote, fecha_lote_dt
                )

                if not arribo_ok:
                    bloqueado_por_arribo = True
                    motivo_bloqueo = (
                        "No puedes realizar consumos, tanqueos o cierres en esta granja porque se inició "
                        "la calefacción pero aún no se confirma la llegada real del pollito. "
                        "Por favor, ve al menú principal y selecciona 'Confirmar llegada del pollito'."
                    )

                if cod_pend:
                    hay_pedido_pendiente = True
                    codigo_pedido_pendiente = cod_pend
                    if fecha_lote_dt >= limite_fecha:
                        bloqueado_por_pedido = True
                        motivo_bloqueo_pedido = (
                            f"⛔ OPERACIÓN BLOQUEADA\n\n"
                            f"No puedes realizar esta operación porque el sistema registra un pedido de gas "
                            f"en tránsito (Código: {codigo_pedido_pendiente}).\n\n"
                            f"Para liberar el sistema, ve al menú principal y usa la opción 'Registrar tanqueo'."
                        )

            cur.execute("""
                SELECT nombre_tanque AS numero, capacidad_gls AS capacidad
                FROM tanques_sedes
                WHERE TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                  AND empresa_id = %s
            """, (sede, id_empresa))
            tks = cur.fetchall()

            tanques_list = []
            for tk in tks:
                num = tk.get("numero") if isinstance(tk, dict) else tk[0]
                cap = tk.get("capacidad") if isinstance(tk, dict) else tk[1]
                match_num = _re.search(r'\d+', str(num))
                num_clean = match_num.group() if match_num else str(num).lower().replace("tk-", "").strip()

                cur.execute(f"""
                    SELECT `nivel tk-{num_clean}`, `nivelfinal tk-{num_clean}`, operacion
                    FROM cardex_glp
                    WHERE TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                      AND id_empresa = %s
                      AND (`nivel tk-{num_clean}` IS NOT NULL OR `nivelfinal tk-{num_clean}` IS NOT NULL)
                    ORDER BY id DESC
                    LIMIT 1
                """, (sede, id_empresa))

                row = cur.fetchone()
                ultimo_nivel = None
                if row:
                    if isinstance(row, dict):
                        nivel_ini = row.get(f"nivel tk-{num_clean}")
                        nivel_fin = row.get(f"nivelfinal tk-{num_clean}")
                        op = row.get("operacion")
                    else:
                        nivel_ini = row[0]
                        nivel_fin = row[1]
                        op = row[2]

                    if op == "tanqueo" and nivel_fin is not None:
                        ultimo_nivel = float(nivel_fin)
                    else:
                        ultimo_nivel = float(nivel_ini) if nivel_ini is not None else 0.0

                tanques_list.append({
                    "numero": num,
                    "capacidad": float(cap or 0),
                    "ultimo_nivel": ultimo_nivel
                })

        return jsonify({
            "success": True,
            "tanques": tanques_list,
            "lote_activo": lote_activo,
            "info_lote": info_lote,
            "hay_pedido_pendiente": hay_pedido_pendiente,
            "codigo_pedido_pendiente": codigo_pedido_pendiente,
            "bloqueado_por_arribo": bloqueado_por_arribo,
            "motivo_bloqueo": motivo_bloqueo,
            "bloqueado_por_pedido": bloqueado_por_pedido,
            "motivo_bloqueo_pedido": motivo_bloqueo_pedido,
            "message": "OK" if tanques_list else "Sin tanques en BD, depende de QR"
        })

    except Exception as e:
        print(f"Error crítico obtener_tanques: {e}")
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})


@bp_glp.route('/registrar_inicio', methods=['POST'])
@login_required_custom
def registrar_inicio_calefaccion():
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"success": False, "message": "Error leyendo JSON"}), 400

    if not data:
        return jsonify({"success": False, "message": "No se recibió JSON válido"}), 400

    ctx = _obtener_contexto_sesion(data)
    usuario = ctx["usuario"]
    empresa = ctx["empresa"]
    id_empresa = ctx["id_empresa"]
    ubicacion = _normalize_sede(data.get('sede'))
    op_id = data.get('op_id')

    if not id_empresa or not ubicacion:
        return jsonify({"success": False, "message": "Sesión inválida o sede faltante."}), 400

    es_sync = bool(data.get('is_sync', False))
    estado_notif = "sync" if es_sync else "exito"

    check = _verificar_idempotencia(id_empresa, op_id, "inicio_calefaccion", ubicacion)
    if check:
        return check

    raw_pollitos = data.get('pollitos')
    try:
        pollitos = int(raw_pollitos) if raw_pollitos not in (None, "", 0, "0") else None
    except Exception:
        pollitos = None

    fecha_llegada_p = _extraer_fecha_iso(data.get('fecha_llegada_pollitos'))
    tanques = data.get('tanques', []) or []
    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para iniciar calefacción."}), 400

    fecha = datetime.now().date()
    lote_id = f"{fecha.strftime('%Y%m%d')}_{ubicacion.replace(' ', '')}"
    saldo_kg = 0.0
    dias_op = 1
    proveedor = None
    tanques_alerta_flameo = []
    tanques_para_pedido = []
    codigo_generado = None
    pedido_info = None
    requiere_gas_ui = False

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                tanques_alerta_flameo = []
                tanques_para_pedido = []
                codigo_generado = None
                pedido_info = None
                requiere_gas_ui = False
                saldo_kg = 0.0

                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT lote
                        FROM cardex_glp
                        WHERE id_empresa = %s
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND estatus_lote = 'ACTIVO'
                        FOR UPDATE
                    """, (id_empresa, ubicacion))
                    if cur.fetchone():
                        return jsonify({"success": False, "message": "⛔ YA EXISTE UN LOTE ACTIVO EN ESTA SEDE."})

                    cur.execute("""
                        INSERT INTO cardex_glp
                            (fecha, empresa, id_empresa, ubicacion, lote, estatus_lote, operacion, tipo, clase, pollitos, fecha_llegada_pollitos, registro, dias_operacion, op_id)
                        VALUES
                            (%s, %s, %s, %s, %s, 'ACTIVO', 'inicio_calefaccion', 'manual', 'saldo inicial', %s, %s, %s, 1, %s)
                    """, (fecha, empresa, id_empresa, ubicacion, lote_id, pollitos, fecha_llegada_p, usuario, op_id))

                    id_operacion = cur.lastrowid
                    carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)
                    densidad = 2.0

                    for tk in tanques:
                        num_raw = str(tk.get("numero", ""))
                        match = _re.search(r'\d+', num_raw)
                        num = match.group() if match else num_raw.upper().replace("TK-", "").replace("TK", "").strip()
                        if not num:
                            continue

                        try:
                            nivel = float(tk.get("nivel", 0))
                        except Exception:
                            nivel = 0.0
                        try:
                            cap = float(tk.get("capacidad", 250))
                        except Exception:
                            cap = 250.0

                        try:
                            cur.execute(f"""
                                SELECT `nivel tk-{num}`, `nivelfinal tk-{num}`, operacion
                                FROM cardex_glp
                                WHERE TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                                  AND id_empresa = %s
                                  AND id < %s
                                  AND (`nivel tk-{num}` IS NOT NULL OR `nivelfinal tk-{num}` IS NOT NULL)
                                ORDER BY id DESC
                                LIMIT 1
                            """, (ubicacion, id_empresa, id_operacion))
                            row_last = cur.fetchone()
                            if row_last:
                                if isinstance(row_last, dict):
                                    n_ini = row_last.get(f"nivel tk-{num}")
                                    n_fin = row_last.get(f"nivelfinal tk-{num}")
                                    op_last = row_last.get("operacion")
                                else:
                                    n_ini = row_last[0]
                                    n_fin = row_last[1]
                                    op_last = row_last[2]

                                ultimo_nivel = float(n_fin) if op_last == "tanqueo" and n_fin is not None else (float(n_ini) if n_ini is not None else 0.0)
                                diferencia = ultimo_nivel - nivel
                                if diferencia > 10.0:
                                    tanques_alerta_flameo.append({
                                        "numero": f"TK-{num}",
                                        "nivel_anterior": ultimo_nivel,
                                        "nivel_nuevo": nivel,
                                        "diferencia": diferencia
                                    })
                        except Exception as e_caida:
                            print(f"Error evaluando caída de nivel por flameo: {e_caida}")

                        col_n = f"nivel tk-{num}"
                        col_c = f"capacidad tk-{num}"
                        col_t = f"testigo nivel tk-{num}"

                        ruta_foto = None
                        foto_recibida = tk.get("fotoBase64") or tk.get("testigo")
                        if foto_recibida:
                            ruta_foto = _guardar_testigo(foto_recibida, carpeta, f"tk{num}_{id_operacion}")

                        query_tk = f"UPDATE cardex_glp SET `{col_n}` = %s, `{col_c}` = %s"
                        params_tk = [nivel, cap]
                        if ruta_foto:
                            query_tk += f", `{col_t}` = %s"
                            params_tk.append(ruta_foto)
                        query_tk += " WHERE id = %s AND id_empresa = %s"
                        params_tk.extend([id_operacion, id_empresa])
                        cur.execute(query_tk, params_tk)

                        saldo_kg += (nivel / 100.0) * cap * densidad
                        if nivel < 70.0:
                            tanques_para_pedido.append({"numero": f"TK-{num}", "nivel": nivel})

                    proveedor = _buscar_proveedor_principal(cur, id_empresa, ubicacion, tanques)
                    cur.execute(
                        "UPDATE cardex_glp SET saldo_estimado_kg = %s, proveedor = %s WHERE id = %s AND id_empresa = %s",
                        (round(saldo_kg, 2), proveedor, id_operacion, id_empresa)
                    )

                    if tanques_para_pedido and proveedor:
                        codigo_generado = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor, cur)
                        requiere_gas_ui = True
                        cur.execute("""
                            UPDATE pedidos_gas_glp
                            SET nivel_solicitado = 80,
                                dias_extra = 0,
                                estatus_flujo = 'pendiente_envio_auto'
                            WHERE codigo_pedido = %s
                              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                        """, (codigo_generado, empresa))
                        cur.execute(
                            "UPDATE cardex_glp SET codigo_pedido = %s WHERE id = %s AND id_empresa = %s",
                            (codigo_generado, id_operacion, id_empresa)
                        )
                        pedido_info = {
                            "generado": True,
                            "codigo": codigo_generado,
                            "proveedor": proveedor,
                            "enviado": False
                        }

                    dias_op = _calcular_actualizar_dias_operacion(cur, id_empresa, ubicacion, lote_id, fecha)

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Inicio calefacción - Lote: {lote_id} - Sede: {ubicacion} - Pollitos: {pollitos} - Pedido: {codigo_generado or 'N/A'}"
                    )

                    mysql.connection.commit()
                    break

            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        # =========================================================
        # ZONA ESTRICTA POST-COMMIT: AUDITORÍA SECUNDARIA, CORREOS Y TELEGRAM
        # =========================================================
        try:
            registrar_auditoria(
                id_empresa, empresa, "GLP", usuario,
                "Inicio Calefacción",
                f"Sede: {ubicacion}. Lote: {lote_id}. Población: {pollitos}. Fecha Llegada: {fecha_llegada_p or 'Pendiente'}.",
                "INFO"
            )
        except Exception:
            pass

        if tanques_alerta_flameo:
            try:
                _enviar_alerta_diferencia_saldos(empresa, ubicacion, usuario, lote_id, tanques_alerta_flameo)
                registrar_auditoria(
                    id_empresa, empresa, "GLP", "Sistema",
                    "⚠️ Alerta Saldo Flameo",
                    f"Diferencia >10% en inicio de lote {lote_id}.",
                    "ALERTA"
                )
            except Exception as e_alerta:
                print(f"Error enviando alerta flameo post-commit: {e_alerta}")

        if codigo_generado and proveedor:
            try:
                enviado = _enviar_alerta_pedido_inicio(empresa, ubicacion, lote_id, proveedor, tanques_para_pedido, codigo_generado)
                if enviado:
                    with mysql.connection.cursor() as cur_post:
                        cur_post.execute("""
                            UPDATE pedidos_gas_glp
                            SET estatus_flujo = 'enviado_auto'
                            WHERE codigo_pedido = %s
                              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                        """, (codigo_generado, empresa))
                        mysql.connection.commit()
                    if pedido_info:
                        pedido_info["enviado"] = True
            except Exception as e_mail:
                print(f"⚠️ Error envío correo inicio post-commit: {e_mail}")

            try:
                _enviar_alerta_telegram_oficial(id_empresa, ubicacion, usuario, "Bajo (Arranque)", codigo_generado)
            except Exception as e_tel:
                print(f"⚠️ Error Telegram Pedido Inicio: {e_tel}")

        notificar_opglp_telegram(
            id_empresa, usuario, "Inicio Calefacción", ubicacion,
            fecha.strftime("%Y-%m-%d"), estado=estado_notif
        )

        mensaje = "Inicio registrado correctamente."
        if pedido_info and pedido_info.get("generado"):
            mensaje += f" ✅ Se generó pedido automático ({pedido_info['codigo']})."

        resumen = {
            "operacion": "inicio_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "pollitos": pollitos,
            "saldo_estimado_kg": round(saldo_kg, 2),
            "tanques": _resumen_tanques([{"numero": t.get("numero"), "nivel": t.get("nivel"), "capacidad": t.get("capacidad")} for t in tanques]),
            "pedido_automatico": pedido_info,
            "requiere_gas": requiere_gas_ui,
            "razon_alerta": f"Pedido automático generado: {codigo_generado}" if codigo_generado else "",
            "dias_operacion": dias_op,
            "op_id": op_id
        }

        return jsonify({
            "success": True,
            "message": mensaje,
            "resumen": resumen,
            "esperando_pollito": True
        })

    except IntegrityError as e_int:
        return _manejar_error_idempotencia(e_int, id_empresa, op_id, "inicio_calefaccion", ubicacion)
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error GLP inicio: {e}")
        traceback.print_exc()
        notificar_opglp_telegram(
            id_empresa, usuario, "Inicio Calefacción", ubicacion,
            datetime.now().strftime("%Y-%m-%d"), estado="fallo"
        )
        return jsonify({"success": False, "message": f"Error interno al iniciar calefacción: {str(e)}"}), 500


@bp_glp.route('/registrar_tanqueo', methods=['POST'])
@login_required_custom
def registrar_tanqueo():
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido recibido."}), 400

    ctx = _obtener_contexto_sesion(data)
    empresa = ctx["empresa"]
    usuario = ctx["usuario"]
    id_empresa = ctx["id_empresa"]
    op_id = (data or {}).get("op_id")
    ubicacion = _normalize_sede(data.get('sede'))
    tanques = data.get('tanques', []) or []

    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400
    if not id_empresa or not ubicacion:
        return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400
    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para el tanqueo."}), 400

    check = _verificar_idempotencia(id_empresa, op_id, "tanqueo", ubicacion)
    if check:
        return check

    es_sync = bool(data.get('is_sync', False))
    estado_notif = "sync" if es_sync else "exito"

    fecha = datetime.now().date()
    lote_id = None
    codigo_trazabilidad = ""
    codigos_cerrados = []
    saldo_estimado_kg = 0.0
    consumo_kg = 0.0
    kg_pollito = 0.0
    pollitos = 0
    dias_operacion = 1
    dens_prom = 0.0
    masa_esperada_total = 0.0
    masa_facturada_total = 0.0
    desvio_total = 0.0
    proveedor_principal = None
    precio_unitario = 0.0
    precio_total = 0.0

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                codigos_cerrados = []
                codigo_trazabilidad = ""
                saldo_estimado_kg = 0.0

                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT lote
                        FROM cardex_glp
                        WHERE id_empresa = %s
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND estatus_lote = 'ACTIVO'
                        ORDER BY fecha DESC, id DESC
                        LIMIT 1
                        FOR UPDATE
                    """, (id_empresa, ubicacion))

                    row_raw = cur.fetchone()
                    lote_id = row_raw.get("lote") if row_raw and isinstance(row_raw, dict) else (row_raw[0] if row_raw else None)

                    if not lote_id:
                        return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

                    # SOLUCIÓN SAN MIGUEL: Cierre integral de TODOS los pedidos abiertos del lote
                    # incluyendo 'pendiente_envio_auto' e insensible a mayúsculas/espacios en cliente
                    cur.execute("""
                        SELECT id, codigo_pedido
                        FROM pedidos_gas_glp
                        WHERE TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND lote = %s
                          AND estatus_flujo IN ('enviado_auto', 'aprobado_webmaster', 'pendiente_aprobacion', 'pendiente_envio_auto')
                          AND COALESCE(estatus, '') NOT IN ('cancelado', 'anulado')
                        ORDER BY id DESC
                        FOR UPDATE
                    """, (empresa, ubicacion, lote_id))

                    rows_ped = cur.fetchall()
                    usuario_registro = usuario

                    if rows_ped:
                        ids_ped = []
                        for rp in rows_ped:
                            pid = rp.get("id") if isinstance(rp, dict) else rp[0]
                            pcod = rp.get("codigo_pedido") if isinstance(rp, dict) else rp[1]
                            ids_ped.append(pid)
                            if pcod:
                                codigos_cerrados.append(pcod)

                        codigo_trazabilidad = codigos_cerrados[0] if codigos_cerrados else ""
                        usuario_registro = f"{usuario} | Pedido: {', '.join(codigos_cerrados)}"

                        placeholders_ped = ", ".join(["%s"] * len(ids_ped))
                        cur.execute(f"""
                            UPDATE pedidos_gas_glp
                            SET estatus_flujo = 'tanqueo_registrado',
                                comentarios = CONCAT(COALESCE(comentarios, ''), ' | Cumplido por OP: ', %s)
                            WHERE id IN ({placeholders_ped})
                              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                        """, (op_id,) + tuple(ids_ped) + (empresa,))

                    columnas = [
                        "fecha", "empresa", "id_empresa", "ubicacion",
                        "lote", "estatus_lote", "operacion", "tipo", "clase",
                        "registro", "op_id", "codigo_pedido"
                    ]
                    valores = [
                        fecha, empresa, id_empresa, ubicacion,
                        lote_id, 'ACTIVO', "tanqueo", "manual", "ingreso",
                        usuario_registro, op_id, codigo_trazabilidad or None
                    ]
                    cur.execute(
                        f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})",
                        valores
                    )
                    id_operacion = cur.lastrowid

                    carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)
                    set_cols = []
                    set_vals = []
                    densidad_estimada = 2.0
                    densidades_registradas = []
                    masas_esperadas = []
                    masas_facturadas = []
                    errores_tanqueo = []

                    for tk in tanques:
                        num_str = str(tk.get("numero", ""))
                        match = _re.search(r'\d+', num_str)
                        num = match.group() if match else ""
                        if not num:
                            continue

                        cap = float(tk.get("capacidad", 0) or 0.0)
                        nivel_ini = float(tk.get("nivel_inicial", 0) or 0.0)
                        nivel_fin = float(tk.get("nivel_final", 0) or 0.0)

                        if (nivel_fin - nivel_ini) <= 0:
                            errores_tanqueo.append(
                                f"• TK-{num}: Inicio {nivel_ini}% -> Fin {nivel_fin}% (En un tanqueo el nivel final debe superar al inicial)."
                            )

                        foto_ini = tk.get("foto_nivel_inicial")
                        foto_fin = tk.get("foto_nivel_final")
                        foto_bau = tk.get("foto_baucher")

                        set_cols.append(f"`nivel tk-{num}` = %s")
                        set_vals.append(nivel_ini)
                        if foto_ini:
                            ruta_ini = _guardar_testigo(foto_ini, carpeta, f"tk{num}_ini_{id_operacion}")
                            if ruta_ini:
                                set_cols.append(f"`testigo nivel tk-{num}` = %s")
                                set_vals.append(ruta_ini)

                        set_cols.append(f"`nivelfinal tk-{num}` = %s")
                        set_vals.append(nivel_fin)
                        if foto_fin:
                            ruta_fin = _guardar_testigo(foto_fin, carpeta, f"tk{num}_fin_{id_operacion}")
                            if ruta_fin:
                                set_cols.append(f"`testigo nivelfinal tk-{num}` = %s")
                                set_vals.append(ruta_fin)

                        set_cols.append(f"`capacidad tk-{num}` = %s")
                        set_vals.append(cap)

                        if foto_bau:
                            ruta_baucher = _guardar_testigo(foto_bau, carpeta, f"tk{num}_baucher_{id_operacion}")
                            if ruta_baucher:
                                set_cols.append(f"`testigo_baucher_tk_{num}` = %s")
                                set_vals.append(ruta_baucher)

                        saldo_estimado_kg += densidad_estimada * cap * (nivel_fin / 100.0)

                        densidad_sum = float(tk.get("densidad_suministrada") or 0.0)
                        if densidad_sum > 0:
                            densidades_registradas.append(densidad_sum)

                        kg_sumin = float(tk.get("kg_suministrados") or 0.0)
                        masas_facturadas.append(kg_sumin)

                        delta_nivel = nivel_fin - nivel_ini
                        if densidad_sum > 0 and cap > 0 and delta_nivel > 0:
                            masas_esperadas.append(densidad_sum * cap * (delta_nivel / 100.0))

                    if errores_tanqueo:
                        raise ValueError("⛔ ERRORES LÓGICOS:\n" + "\n".join(errores_tanqueo))

                    if set_cols:
                        cur.execute(
                            f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id = %s AND id_empresa = %s",
                            set_vals + [id_operacion, id_empresa]
                        )

                    saldo_estimado_gal = saldo_estimado_kg / densidad_estimada if densidad_estimada else 0.0
                    cur.execute(
                        "UPDATE cardex_glp SET saldo_estimado_kg = %s, saldo_estimado_galones = %s WHERE id = %s AND id_empresa = %s",
                        (round(saldo_estimado_kg, 2), round(saldo_estimado_gal, 2), id_operacion, id_empresa)
                    )

                    # Peaje físico estricto en servidor: NO silenciar ValueError si nivel_inicial > saldo anterior
                    tks_consumo = [
                        {"numero": t.get("numero"), "capacidad": t.get("capacidad"), "nivel": t.get("nivel_inicial")}
                        for t in tanques
                    ]
                    consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                        cur, id_empresa, ubicacion, lote_id, id_operacion, tks_consumo
                    )

                    dias_operacion = _calcular_actualizar_dias_operacion(cur, id_empresa, ubicacion, lote_id, fecha)

                    dens_prom = sum(densidades_registradas) / len(densidades_registradas) if densidades_registradas else 0.0
                    masa_esperada_total = sum(masas_esperadas) if masas_esperadas else 0.0
                    masa_facturada_total = sum(masas_facturadas) if masas_facturadas else 0.0
                    desvio_total = (
                        ((masa_facturada_total - masa_esperada_total) / masa_esperada_total) * 100.0
                        if masa_esperada_total > 0 else 0.0
                    )

                    proveedor_principal = _buscar_proveedor_principal(cur, id_empresa, ubicacion, tanques)
                    precio_unitario = 0.0
                    if proveedor_principal:
                        cur.execute(
                            "SELECT precio FROM proveedores WHERE TRIM(UPPER(proveedor)) = TRIM(UPPER(%s)) LIMIT 1",
                            (proveedor_principal,)
                        )
                        prow_raw = cur.fetchone()
                        if prow_raw:
                            precio_unitario = float((prow_raw.get("precio") if isinstance(prow_raw, dict) else prow_raw[0]) or 0.0)

                    precio_total = precio_unitario * float(masa_facturada_total or 0.0)

                    cur.execute("""
                        UPDATE cardex_glp
                        SET densidad_suministrada = %s,
                            masa_esperada_kg = %s,
                            masa_kg_facturada = %s,
                            porcentaje_diferencia = %s,
                            proveedor = %s,
                            precio_unitario = %s,
                            precio_total = %s
                        WHERE id = %s AND id_empresa = %s
                    """, (
                        round(dens_prom, 3) if dens_prom else 0.0,
                        round(masa_esperada_total, 2),
                        round(masa_facturada_total, 2),
                        round(desvio_total, 2),
                        proveedor_principal,
                        round(precio_unitario, 6),
                        round(precio_total, 2),
                        id_operacion,
                        id_empresa
                    ))

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Tanqueo - Sede: {ubicacion} - Lote: {lote_id} - Carga: {round(masa_facturada_total, 2)} kg - Pedidos cerrados: {', '.join(codigos_cerrados) or 'Sin pedido previo'}"
                    )

                    mysql.connection.commit()
                    break

            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        # =========================================================
        # ZONA ESTRICTA POST-COMMIT: CORREOS, AUDITORÍA Y TELEGRAM
        # =========================================================
        try:
            registrar_auditoria(
                empresa_id=id_empresa,
                empresa_nombre=empresa,
                modulo="GLP",
                usuario=usuario,
                accion="Tanqueo Registrado",
                detalle=f"Sede: {ubicacion}. Carga: {round(masa_facturada_total, 2)} kg. Valor: ${precio_total:,.0f} | {', '.join(codigos_cerrados)}",
                nivel="INFO"
            )
        except Exception:
            pass

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
                        id_empresa, empresa, "GLP", "Sistema",
                        "⚠️ Alerta Desviación",
                        f"Desviación {round(desvio_total, 2)}% en {ubicacion}.",
                        "ALERTA"
                    )
            except Exception as e_mail:
                print(f"⛔ Error envío alerta desviación post-commit: {e_mail}")

        notificar_opglp_telegram(
            id_empresa, usuario, "Registrar Tanqueo", ubicacion,
            fecha.strftime("%Y-%m-%d"), estado=estado_notif
        )

        mensaje = "Tanqueo registrado correctamente."
        if codigos_cerrados:
            mensaje += f" Se cerraron los pedidos: {', '.join(codigos_cerrados)}."
        if masa_esperada_total > 0:
            mensaje += f" Facturado: {round(masa_facturada_total, 2)} kg."
            if desvio_total > 8.0:
                mensaje += " ⚠️ Desviación detectada."
        valor_formato = f"{precio_total:,.0f}".replace(",", ".")
        mensaje += f" Valor: ${valor_formato}."

        resumen = {
            "operacion": "tanqueo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques([{"numero": t.get("numero"), "nivel": t.get("nivel_final"), "capacidad": t.get("capacidad")} for t in tanques]),
            "saldo_estimado_kg": round(saldo_estimado_kg, 2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg, 2),
            "kg_pollito": round(kg_pollito, 6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "masa_esperada_kg": round(masa_esperada_total, 2),
            "masa_kg_facturada": round(masa_facturada_total, 2),
            "porcentaje_diferencia": round(desvio_total, 2),
            "proveedor": proveedor_principal,
            "precio_unitario": round(float(precio_unitario or 0.0), 6),
            "precio_total": round(float(precio_total or 0.0), 2),
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except IntegrityError as e:
        return _manejar_error_idempotencia(e, id_empresa, op_id, "tanqueo", ubicacion)
    except ValueError as e:
        mysql.connection.rollback()
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "⛔ Error Lógico Tanqueo", str(e), "WARNING")
        except Exception:
            pass
        return jsonify({"success": False, "message": str(e)})
    except Exception as e:
        mysql.connection.rollback()
        traceback.print_exc()
        notificar_opglp_telegram(
            id_empresa, usuario, "Registrar Tanqueo", ubicacion,
            datetime.now().strftime("%Y-%m-%d"), estado="fallo"
        )
        try:
            registrar_auditoria(id_empresa, empresa, "GLP", usuario, "💀 Error Sistema Tanqueo", str(e), "CRITICAL")
        except Exception:
            pass
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})
	# ======================
# Registrar consumo (VERSIÓN ATÓMICA Y BLINDADA POST-COMMIT)
# ======================
@bp_glp.route('/registrar_consumo', methods=['POST'])
@login_required_custom
def registrar_consumo():
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido recibido."}), 400

    ctx = _obtener_contexto_sesion(data)
    empresa = ctx["empresa"]
    usuario = ctx["usuario"]
    id_empresa = ctx["id_empresa"]
    op_id = (data or {}).get("op_id")
    ubicacion = _normalize_sede(data.get('sede'))
    tanques = data.get('tanques', []) or []

    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400
    if not id_empresa or not ubicacion:
        return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400
    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para el registro."}), 400

    check = _verificar_idempotencia(id_empresa, op_id, "consumo", ubicacion)
    if check:
        return check

    es_sync = bool(data.get('is_sync', False))
    estado_notif = "sync" if es_sync else "exito"

    lote_id = None
    fecha = datetime.now().date()
    saldo_estimado_kg = 0.0
    saldo_estimado_gal = 0.0
    consumo_kg = 0.0
    kg_pollito = 0.0
    pollitos = 0
    dias_operacion = 0
    proveedor_principal = None
    analisis_riesgo = {"requiere_gas": False, "razon": "", "codigo": None}
    nivel_notif_solicitud = 0.0

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                analisis_riesgo = {"requiere_gas": False, "razon": "", "codigo": None}
                nivel_notif_solicitud = 0.0
                saldo_estimado_kg = 0.0

                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT id, lote, fecha, registro
                        FROM cardex_glp
                        WHERE id_empresa = %s
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND estatus_lote = 'ACTIVO'
                          AND operacion = 'inicio_calefaccion'
                        ORDER BY id DESC
                        LIMIT 1
                        FOR UPDATE
                    """, (id_empresa, ubicacion))

                    row_ini = cur.fetchone()
                    if not row_ini:
                        return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

                    if isinstance(row_ini, dict):
                        id_ini = row_ini.get("id")
                        lote_id = row_ini.get("lote")
                        fecha_lote = row_ini.get("fecha")
                        reg_lote = row_ini.get("registro") or ""
                    else:
                        id_ini = row_ini[0]
                        lote_id = row_ini[1]
                        fecha_lote = row_ini[2]
                        reg_lote = row_ini[3] or ""

                    fecha_lote_dt = _extraer_fecha_iso(fecha_lote) if isinstance(fecha_lote, str) else (
                        fecha_lote.date() if isinstance(fecha_lote, datetime) else fecha_lote
                    )
                    limite_fecha = datetime(2026, 6, 15).date()

                    arribo_ok, cod_pend = _autosanar_estado_lote(
                        cur, ctx, ubicacion, lote_id, id_ini, reg_lote, fecha_lote_dt
                    )

                    if not es_sync:
                        if not arribo_ok:
                            return jsonify({
                                "success": False,
                                "message": (
                                    "⛔ OPERACIÓN BLOQUEADA: Aún no se confirma la llegada real del pollito "
                                    "en esta sede. Usa primero 'Confirmar llegada del pollito'."
                                )
                            })
                        if cod_pend and fecha_lote_dt >= limite_fecha:
                            return jsonify({
                                "success": False,
                                "message": (
                                    f"⛔ OPERACIÓN BLOQUEADA: Existe un pedido de gas en tránsito "
                                    f"(Código: {cod_pend}). Registra primero el tanqueo."
                                )
                            })

                    columnas = [
                        "fecha", "empresa", "id_empresa", "ubicacion", "lote",
                        "estatus_lote", "operacion", "tipo", "clase", "registro", "op_id"
                    ]
                    valores = [
                        fecha, empresa, id_empresa, ubicacion, lote_id,
                        'ACTIVO', "consumo", "manual", "egreso", usuario, op_id
                    ]
                    cur.execute(
                        f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})",
                        valores
                    )
                    id_operacion = cur.lastrowid

                    carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)
                    set_cols = []
                    set_vals = []
                    densidad = 2.0
                    sum_nivel_cap = 0.0
                    sum_cap = 0.0

                    for tk in tanques:
                        num_str = str(tk.get("numero", ""))
                        match = _re.search(r'\d+', num_str)
                        num = match.group() if match else ""
                        if not num:
                            continue

                        try:
                            val = float(tk.get("nivel", 0) or 0)
                        except Exception:
                            val = 0.0
                        try:
                            cap = float(tk.get("capacidad", 0) or 0)
                        except Exception:
                            cap = 0.0

                        # CORRECCIÓN CRÍTICA: Soportar fotoBase64 enviado por preguntarNivelFoto y testigo
                        tst = tk.get("fotoBase64") or tk.get("testigo")

                        set_cols.append(f"`nivel tk-{num}` = %s")
                        set_vals.append(val)
                        set_cols.append(f"`capacidad tk-{num}` = %s")
                        set_vals.append(cap)

                        if tst:
                            ruta = _guardar_testigo(tst, carpeta, f"tk{num}_consumo_{id_operacion}")
                            if ruta:
                                set_cols.append(f"`testigo nivel tk-{num}` = %s")
                                set_vals.append(ruta)

                        saldo_estimado_kg += densidad * cap * (val / 100.0)
                        sum_nivel_cap += (val * cap)
                        sum_cap += cap

                    if set_cols:
                        cur.execute(
                            f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id = %s AND id_empresa = %s",
                            set_vals + [id_operacion, id_empresa]
                        )

                    saldo_estimado_gal = saldo_estimado_kg / densidad if densidad else 0.0
                    cur.execute(
                        "UPDATE cardex_glp SET saldo_estimado_kg = %s, saldo_estimado_galones = %s WHERE id = %s AND id_empresa = %s",
                        (round(saldo_estimado_kg, 2), round(saldo_estimado_gal, 2), id_operacion, id_empresa)
                    )

                    consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                        cur, id_empresa, ubicacion, lote_id, id_operacion, tanques
                    )
                    dias_operacion = _calcular_actualizar_dias_operacion(cur, id_empresa, ubicacion, lote_id, fecha)

                    proveedor_principal = _buscar_proveedor_principal(cur, id_empresa, ubicacion, tanques)
                    cur.execute(
                        "UPDATE cardex_glp SET proveedor = %s WHERE id = %s AND id_empresa = %s",
                        (proveedor_principal, id_operacion, id_empresa)
                    )

                    solicitud_gas = data.get("solicitud_gas")
                    if solicitud_gas:
                        nivel_solicitado = float(solicitud_gas.get("nivel", 80))
                        dias_extra = int(solicitud_gas.get("dias_extra", 0))
                        codigo = _generar_codigo_pedido(empresa, lote_id, ubicacion, proveedor_principal, cur)

                        cur.execute("""
                            UPDATE pedidos_gas_glp
                            SET nivel_solicitado = %s,
                                dias_extra = %s,
                                estatus_flujo = 'pendiente_aprobacion',
                                fecha_solicitud = NOW(),
                                comentarios = 'Solicitud generada automáticamente en consumo'
                            WHERE codigo_pedido = %s
                              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                        """, (nivel_solicitado, dias_extra, codigo, empresa))

                        cur.execute(
                            "UPDATE cardex_glp SET codigo_pedido = %s WHERE id = %s AND id_empresa = %s",
                            (codigo, id_operacion, id_empresa)
                        )
                        nivel_notif_solicitud = round(sum_nivel_cap / sum_cap if sum_cap > 0 else 0.0, 1)
                        analisis_riesgo.update({
                            "requiere_gas": True,
                            "razon": f"Solicitud {codigo} enviada a revisión.",
                            "codigo": codigo
                        })

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Consumo - Sede: {ubicacion} - Lote: {lote_id} - Gasto: {round(consumo_kg, 2)} kg - Solicitud: {analisis_riesgo['codigo'] or 'No'}"
                    )

                    mysql.connection.commit()
                    break

            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    print(f"⚠️ Deadlock en Consumo (Intento {intento + 1})... reintentando.")
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        # =========================================================
        # ZONA ESTRICTA POST-COMMIT: AUDITORÍA, CORREO Y TELEGRAM
        # =========================================================
        try:
            registrar_auditoria(
                id_empresa, empresa, "GLP", usuario,
                "Consumo Registrado",
                f"Sede: {ubicacion}. Consumo: {round(consumo_kg, 2)} kg.",
                "INFO"
            )
        except Exception:
            pass

        if analisis_riesgo["codigo"]:
            try:
                _enviar_alerta_telegram_oficial(
                    id_empresa, ubicacion, usuario, nivel_notif_solicitud, analisis_riesgo["codigo"]
                )
                _enviar_alerta_webmaster_nueva_solicitud(
                    empresa, ubicacion, usuario, nivel_notif_solicitud, analisis_riesgo["codigo"]
                )
            except Exception as e_sol:
                print(f"⚠️ Error notificando solicitud de gas post-commit: {e_sol}")

        notificar_opglp_telegram(
            id_empresa, usuario, "Registro de Consumo", ubicacion,
            fecha.strftime("%Y-%m-%d"), estado=estado_notif
        )

        resumen = {
            "operacion": "consumo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques(tanques),
            "saldo_estimado_kg": round(saldo_estimado_kg, 2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg, 2),
            "kg_pollito": round(kg_pollito, 6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "proveedor": proveedor_principal,
            "op_id": op_id,
            "requiere_gas": analisis_riesgo["requiere_gas"],
            "razon_alerta": analisis_riesgo["razon"],
            "codigo_pedido": analisis_riesgo["codigo"]
        }
        return jsonify({"success": True, "message": "Operación guardada exitosamente.", "resumen": resumen})

    except IntegrityError as e:
        return _manejar_error_idempotencia(e, id_empresa, op_id, "consumo", ubicacion)
    except ValueError as e:
        mysql.connection.rollback()
        return jsonify({"success": False, "message": str(e)})
    except Exception as e:
        mysql.connection.rollback()
        traceback.print_exc()
        notificar_opglp_telegram(
            id_empresa, usuario, "Registro de Consumo", ubicacion,
            datetime.now().strftime("%Y-%m-%d"), estado="fallo"
        )
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})


# ======================
# Finalizar calefacción (VERSION PRODUCCIÓN BLINDADA POST-COMMIT)
# ======================
@bp_glp.route('/finalizar_calefaccion_batch', methods=['POST'])
@login_required_custom
def finalizar_calefaccion_batch():
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido recibido."}), 400

    ctx = _obtener_contexto_sesion(data)
    empresa = ctx["empresa"]
    usuario = ctx["usuario"]
    id_empresa = ctx["id_empresa"]
    op_id = (data or {}).get("op_id")
    ubicacion = _normalize_sede(data.get('sede'))
    tanques = data.get('tanques', []) or []

    es_sync = bool(data.get('is_sync', False))
    estado_notif = "sync" if es_sync else "exito"

    if not op_id:
        return jsonify({"success": False, "message": "Falta op_id en la operación"}), 400
    if not id_empresa or not ubicacion:
        return jsonify({"success": False, "message": "Sesión expirada o sede faltante."}), 400
    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para finalizar calefacción."}), 400

    check = _verificar_idempotencia(id_empresa, op_id, "finalizar_calefaccion", ubicacion)
    if check:
        return check

    fecha = datetime.now().date()
    lote_id = None
    saldo_estimado_kg = 0.0
    consumo_kg = 0.0
    kg_pollito = 0.0
    pollitos = 0
    dias_operacion = 1

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                saldo_estimado_kg = 0.0

                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT id, lote, fecha, registro
                        FROM cardex_glp
                        WHERE id_empresa = %s
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND estatus_lote = 'ACTIVO'
                          AND operacion = 'inicio_calefaccion'
                        ORDER BY id DESC
                        LIMIT 1
                        FOR UPDATE
                    """, (id_empresa, ubicacion))

                    row_ini = cur.fetchone()
                    if not row_ini:
                        return jsonify({"success": False, "message": "No hay lote activo en esta sede."})

                    if isinstance(row_ini, dict):
                        id_ini = row_ini.get("id")
                        lote_id = row_ini.get("lote")
                        fecha_lote = row_ini.get("fecha")
                        reg_lote = row_ini.get("registro") or ""
                    else:
                        id_ini = row_ini[0]
                        lote_id = row_ini[1]
                        fecha_lote = row_ini[2]
                        reg_lote = row_ini[3] or ""

                    fecha_lote_dt = _extraer_fecha_iso(fecha_lote) if isinstance(fecha_lote, str) else (
                        fecha_lote.date() if isinstance(fecha_lote, datetime) else fecha_lote
                    )
                    limite_fecha = datetime(2026, 6, 15).date()

                    arribo_ok, cod_pend = _autosanar_estado_lote(
                        cur, ctx, ubicacion, lote_id, id_ini, reg_lote, fecha_lote_dt
                    )

                    if not es_sync:
                        if not arribo_ok:
                            return jsonify({
                                "success": False,
                                "message": (
                                    "⛔ OPERACIÓN BLOQUEADA: No se ha confirmado la llegada del pollito en este lote. "
                                    "Usa 'Confirmar llegada del pollito' antes de cerrar."
                                )
                            })
                        if cod_pend and fecha_lote_dt >= limite_fecha:
                            return jsonify({
                                "success": False,
                                "message": (
                                    f"⛔ OPERACIÓN BLOQUEADA: Existe un pedido de gas en tránsito "
                                    f"(Código: {cod_pend}). Registra primero el tanqueo para cerrar la calefacción."
                                )
                            })

                    columnas = [
                        "fecha", "empresa", "id_empresa", "ubicacion", "lote",
                        "estatus_lote", "operacion", "tipo", "clase", "registro", "op_id"
                    ]
                    valores = [
                        fecha, empresa, id_empresa, ubicacion, lote_id,
                        'ACTIVO', "finalizar_calefaccion", "manual", "saldo final", usuario, op_id
                    ]
                    cur.execute(
                        f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})",
                        valores
                    )
                    id_operacion = cur.lastrowid

                    carpeta = os.path.join("testigos", empresa.replace(" ", "_"), lote_id)
                    set_cols = []
                    set_vals = []
                    densidad = 2.0

                    for tk in tanques:
                        num_match = _re.search(r'\d+', str(tk.get("numero", "")))
                        num = num_match.group() if num_match else ""
                        if not num:
                            continue

                        try:
                            niv = float(tk.get("nivel", 0) or 0.0)
                            cap = float(tk.get("capacidad", 0) or 0.0)
                        except Exception:
                            niv, cap = 0.0, 0.0

                        set_cols.append(f"`nivel tk-{num}` = %s")
                        set_vals.append(niv)
                        set_cols.append(f"`capacidad tk-{num}` = %s")
                        set_vals.append(cap)

                        foto_cierre = tk.get("fotoBase64") or tk.get("testigo")
                        if foto_cierre:
                            ruta = _guardar_testigo(foto_cierre, carpeta, f"tk{num}_final_{id_operacion}")
                            if ruta:
                                set_cols.append(f"`testigo nivel tk-{num}` = %s")
                                set_vals.append(ruta)

                        saldo_estimado_kg += densidad * cap * (niv / 100.0)

                    if set_cols:
                        cur.execute(
                            f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id = %s AND id_empresa = %s",
                            set_vals + [id_operacion, id_empresa]
                        )

                    cur.execute(
                        "UPDATE cardex_glp SET saldo_estimado_kg = %s, saldo_estimado_galones = %s WHERE id = %s AND id_empresa = %s",
                        (round(saldo_estimado_kg, 2), round(saldo_estimado_kg / densidad, 2), id_operacion, id_empresa)
                    )

                    # Peaje físico estricto en servidor: NO silenciar ValueError si nivel > saldo previo
                    consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                        cur, id_empresa, ubicacion, lote_id, id_operacion, tanques
                    )

                    dias_operacion = _calcular_actualizar_dias_operacion(cur, id_empresa, ubicacion, lote_id, fecha)

                    proveedor_cierre = _buscar_proveedor_principal(cur, id_empresa, ubicacion, tanques)
                    cur.execute(
                        "UPDATE cardex_glp SET proveedor = %s WHERE id = %s AND id_empresa = %s",
                        (proveedor_cierre, id_operacion, id_empresa)
                    )

                    cur.execute(
                        "UPDATE cardex_glp SET estatus_lote = 'INACTIVO' WHERE id_empresa = %s AND lote = %s",
                        (id_empresa, lote_id)
                    )
                    cur.execute("""
                        UPDATE pedidos_gas_glp
                        SET estatus_lote = 'INACTIVO'
                        WHERE TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND lote = %s
                    """, (empresa, ubicacion, lote_id))

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Fin calefacción - Lote: {lote_id} - Sede: {ubicacion} - Saldo final: {round(saldo_estimado_kg, 2)} kg"
                    )

                    mysql.connection.commit()
                    break

            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    print(f"⚠️ Deadlock en Cierre (Intento {intento + 1})...")
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        # =========================================================
        # ZONA ESTRICTA POST-COMMIT: AUDITORÍA Y TELEGRAM
        # =========================================================
        try:
            registrar_auditoria(
                id_empresa, empresa, "GLP", usuario,
                "Fin Calefacción",
                f"Sede: {ubicacion}. Lote cerrado: {lote_id}.",
                "INFO"
            )
        except Exception as e_aud:
            print("Error audit:", e_aud)

        notificar_opglp_telegram(
            id_empresa, usuario, "Finalización de Calefacción", ubicacion,
            fecha.strftime("%Y-%m-%d"), estado=estado_notif
        )

        resumen = {
            "operacion": "finalizar_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques([{"numero": t.get("numero"), "nivel": t.get("nivel"), "capacidad": t.get("capacidad")} for t in tanques]),
            "saldo_estimado_kg": round(saldo_estimado_kg, 2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg, 2),
            "kg_pollito": round(kg_pollito, 6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "op_id": op_id
        }

        return jsonify({
            "success": True,
            "message": "Calefacción cerrada correctamente.",
            "resumen": resumen
        })

    except IntegrityError as e:
        return _manejar_error_idempotencia(e, id_empresa, op_id, "finalizar_calefaccion", ubicacion)
    except ValueError as e:
        mysql.connection.rollback()
        return jsonify({"success": False, "message": str(e)})
    except Exception as e:
        mysql.connection.rollback()
        traceback.print_exc()
        notificar_opglp_telegram(
            id_empresa, usuario, "Finalización de Calefacción", ubicacion,
            datetime.now().strftime("%Y-%m-%d"), estado="fallo"
        )
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"})


# ==========================================
# CONSULTA Y VALIDACIÓN DE PEDIDOS / FACTURAS
# ==========================================
@bp_glp.route('/consultar_pedidos_pendientes', methods=['POST'])
@login_required_custom
def consultar_pedidos_pendientes():
    ctx = _obtener_contexto_sesion()
    if not ctx["id_empresa"] or ctx["empresa"] == 'N/D':
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
            WHERE TRIM(UPPER(p.cliente)) = TRIM(UPPER(%s))
              AND p.estatus = 'generado'
            ORDER BY p.fecha_registro DESC
        """, (ctx["empresa"],))

        pedidos = cur.fetchall()
        cur.close()

        pedidos_listos = []
        for pedido in pedidos:
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

            pedidos_listos.append({
                "id": p_id,
                "codigo": p_cod,
                "fecha": p_fec.strftime('%Y-%m-%d %H:%M') if hasattr(p_fec, 'strftime') else (str(p_fec) if p_fec else 'N/A'),
                "proveedor": p_prov if p_prov else 'N/A',
                "ubicacion": p_ubi,
                "lote": p_lot
            })

        return jsonify({"success": True, "pedidos": pedidos_listos})

    except Exception:
        app.logger.error(f"Error al consultar pedidos pendientes GLP: {traceback.format_exc()}")
        return jsonify({"success": False, "message": "Error interno al consultar la base de datos."}), 500


@bp_glp.route('/validar_pedido', methods=['POST'])
@login_required_custom
def validar_pedido():
    data = request.get_json(force=True, silent=True) or {}
    ctx = _obtener_contexto_sesion(data)
    pedido_id = data.get('pedido_id')
    numero_factura = str(data.get('numero_factura') or '').strip()
    validador_cedula = ctx["cedula"]

    if not pedido_id or not numero_factura or validador_cedula == 'N/D':
        return jsonify({
            "success": False,
            "message": "Faltan datos de Pedido, Número de Factura o Cédula del Validador."
        }), 400

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT codigo_pedido, ubicacion
                        FROM pedidos_gas_glp
                        WHERE id = %s
                          AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                          AND estatus = 'generado'
                        FOR UPDATE
                    """, (pedido_id, ctx["empresa"]))
                    row_ped = cur.fetchone()
                    if not row_ped:
                        mysql.connection.rollback()
                        return jsonify({
                            "success": False,
                            "message": "El pedido no existe en su empresa, ya fue validado o no está en estado 'generado'."
                        }), 404

                    cod_ped = row_ped['codigo_pedido'] if isinstance(row_ped, dict) else row_ped[0]
                    ubi_ped = row_ped['ubicacion'] if isinstance(row_ped, dict) else row_ped[1]

                    cur.execute("""
                        UPDATE pedidos_gas_glp
                        SET estatus = 'validado',
                            fecha_validacion = NOW(),
                            validador = %s,
                            numero_factura = %s
                        WHERE id = %s
                          AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                          AND estatus = 'generado'
                    """, (validador_cedula, numero_factura, pedido_id, ctx["empresa"]))

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Validar Pedido GLP - ID: {pedido_id} - Código: {cod_ped} - Sede: {ubi_ped} - Factura: {numero_factura}",
                        backend_emisor="FACTURACION_GLP"
                    )

                    mysql.connection.commit()
                    break
            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        try:
            registrar_auditoria(
                ctx["id_empresa"], ctx["empresa"], "GLP", ctx["usuario"],
                "Validación Factura Pedido",
                f"Pedido {cod_ped} ({ubi_ped}) validado con Factura N° {numero_factura}.",
                "INFO"
            )
        except Exception:
            pass

        return jsonify({
            "success": True,
            "message": f"Pedido {pedido_id} validado correctamente. Número de factura: {numero_factura}."
        })

    except Exception:
        mysql.connection.rollback()
        app.logger.error(f"Error al validar pedido GLP: {traceback.format_exc()}")
        return jsonify({"success": False, "message": "Error interno al actualizar el pedido."}), 500


@bp_glp.route('/facturas', methods=['GET'])
@login_required_custom
def ver_facturas_glp():
    ctx = _obtener_contexto_sesion()
    empresa = ctx["empresa"]
    nombre = ctx["usuario"]
    cedula = ctx["cedula"]

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
            WHERE TRIM(UPPER(p.cliente)) = TRIM(UPPER(%s))
              AND p.estatus = 'generado'
            ORDER BY p.fecha_registro DESC
        """, (empresa,))
        rows = cur.fetchall()
        cur.close()

        pedidos_listos = []
        for row in rows:
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

            pedidos_listos.append({
                "id": p_id,
                "codigo_pedido": p_cod,
                "fecha_generacion": p_fec.strftime('%Y-%m-%d %H:%M') if hasattr(p_fec, 'strftime') else (str(p_fec) if p_fec else 'N/A'),
                "proveedor": p_prov,
                "ubicacion": p_ubi
            })

        return render_template(
            'C_facturas_glp.html',
            pedidos=pedidos_listos,
            empresa=empresa,
            nombre=nombre,
            cedula=cedula
        )

    except Exception:
        app.logger.error(f"Error cargando vista facturas: {traceback.format_exc()}")
        return "Error cargando la página de facturas.", 500


# ==========================================
# ENDPOINTS DE NEGOCIACIÓN Y SANEAMIENTO ADMINISTRATIVO
# ==========================================
@bp_glp.route('/solicitar_pedido_manual', methods=['POST'])
@login_required_custom
def solicitar_pedido_manual():
    data = request.get_json(force=True, silent=True) or {}
    ctx = _obtener_contexto_sesion(data)

    op_id = data.get('op_id')
    nivel_solicitado = data.get('nivel_solicitado')
    dias_extra = data.get('dias_extra', 0)
    notas = data.get('comentarios', '')

    if not op_id or not ctx["id_empresa"]:
        return jsonify({"success": False, "message": "Falta el ID de operación (op_id) o sesión inválida."}), 400

    codigo = None
    empresa = ctx["empresa"]
    ubicacion = ""
    nivel_reportado = 0

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT empresa, ubicacion, lote, proveedor, `nivel tk-1`
                        FROM cardex_glp
                        WHERE op_id = %s AND id_empresa = %s
                        FOR UPDATE
                    """, (op_id, ctx["id_empresa"]))
                    row = cur.fetchone()

                    if not row:
                        return jsonify({"success": False, "message": "Operación no encontrada en Cardex para esta empresa."}), 404

                    if isinstance(row, dict):
                        empresa_row = row.get('empresa')
                        ubicacion = row.get('ubicacion')
                        lote = row.get('lote')
                        proveedor = row.get('proveedor')
                        nivel_reportado = row.get('nivel tk-1') or 0
                    else:
                        empresa_row = row[0]
                        ubicacion = row[1]
                        lote = row[2]
                        proveedor = row[3]
                        nivel_reportado = row[4] or 0

                    empresa = _re.sub(r"\s+", " ", str(empresa_row or ctx["empresa"])).strip()
                    codigo = _generar_codigo_pedido(empresa, lote, ubicacion, proveedor, cur)

                    cur.execute("""
                        UPDATE pedidos_gas_glp
                        SET nivel_solicitado = %s,
                            dias_extra = %s,
                            estatus_flujo = 'pendiente_aprobacion',
                            comentarios = %s,
                            fecha_solicitud = NOW()
                        WHERE codigo_pedido = %s
                          AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                    """, (nivel_solicitado, dias_extra, notas, codigo, empresa))

                    cur.execute(
                        "UPDATE cardex_glp SET codigo_pedido = %s WHERE op_id = %s AND id_empresa = %s",
                        (codigo, op_id, ctx["id_empresa"])
                    )

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Solicitud Pedido Manual - Código: {codigo} - Sede: {ubicacion}"
                    )

                    mysql.connection.commit()
                    break
            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        try:
            _enviar_alerta_telegram_oficial(ctx["id_empresa"], ubicacion, ctx["usuario"], nivel_reportado, codigo)
            _enviar_alerta_webmaster_nueva_solicitud(
                empresa=empresa,
                ubicacion=ubicacion,
                usuario=ctx["usuario"],
                nivel_actual=nivel_reportado,
                codigo_pedido=codigo
            )
        except Exception as e_notify:
            print(f"Advertencia: Alguna notificación falló post-commit: {e_notify}")

        return jsonify({"success": True, "message": "Solicitud enviada a aprobación correctamente.", "codigo": codigo})

    except Exception as e:
        mysql.connection.rollback()
        print(f"Error Crítico en solicitar_pedido_manual: {str(e)}")
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"}), 500


@bp_glp.route('/anular_pedido_sin_evidencia', methods=['POST'])
@login_required_custom
def anular_pedido_sin_evidencia():
    data = request.get_json(force=True, silent=True) or {}
    ctx = _obtener_contexto_sesion(data)
    pedido_id = data.get('id')
    justificacion = str(data.get('justificacion') or '').strip()

    usuario = ctx["usuario"]
    perfil = ctx["perfil"]
    empresa_actor = ctx["empresa"]
    es_webmaster = "webmaster" in perfil.lower()

    if not pedido_id or len(justificacion) < 10:
        return jsonify({"success": False, "message": "La justificación es obligatoria y debe ser detallada."}), 400

    cliente = ""
    ubicacion = ""
    codigo = ""
    factura = "N/D"
    estatus_base = ""

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    if es_webmaster:
                        cur.execute("""
                            SELECT cliente, ubicacion, codigo_pedido, numero_factura, estatus, estatus_flujo, lote
                            FROM pedidos_gas_glp
                            WHERE id = %s
                            FOR UPDATE
                        """, (pedido_id,))
                    else:
                        cur.execute("""
                            SELECT cliente, ubicacion, codigo_pedido, numero_factura, estatus, estatus_flujo, lote
                            FROM pedidos_gas_glp
                            WHERE id = %s
                              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                            FOR UPDATE
                        """, (pedido_id, empresa_actor))

                    row = cur.fetchone()
                    if not row:
                        return jsonify({"success": False, "message": "Pedido no encontrado para esta empresa."}), 404

                    rd = dict(zip([d[0] for d in cur.description], row)) if not isinstance(row, dict) else row
                    cliente = rd['cliente']
                    ubicacion = rd['ubicacion']
                    codigo = rd['codigo_pedido']
                    factura = rd['numero_factura'] or "N/D"
                    estatus_base = rd['estatus']

                    firma = f"Anulado por {perfil} ({usuario}). Razón: {justificacion}"
                    cur.execute("""
                        UPDATE pedidos_gas_glp
                        SET estatus = 'anulado',
                            estatus_flujo = 'anulado_sin_evidencia',
                            comentarios = CONCAT(COALESCE(comentarios, ''), ' | ', %s)
                        WHERE id = %s
                          AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                    """, (firma, pedido_id, cliente))

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Anular pedido sin evidencia - Código: {codigo} - Cliente: {cliente} - Sede: {ubicacion}"
                    )

                    mysql.connection.commit()
                    break
            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        if estatus_base == 'validado':
            try:
                with mysql.connection.cursor() as cur_mail:
                    _enviar_correo_investigacion_anulacion(
                        cur_mail, cliente, ubicacion, codigo, factura, justificacion, usuario, perfil
                    )
            except Exception as e_inv:
                print(f"Error correo investigación post-commit: {e_inv}")

        try:
            nivel_audit = "CRITICAL" if estatus_base == 'validado' else "WARNING"
            registrar_auditoria(
                ctx["id_empresa"], empresa_actor, "GLP", usuario,
                "Anulación Sin Evidencia",
                f"Pedido {codigo} de {cliente} ({ubicacion}). Razón: {justificacion}",
                nivel_audit
            )
        except Exception:
            pass

        return jsonify({"success": True, "message": "Pedido anulado y reportado exitosamente."})

    except Exception as e:
        mysql.connection.rollback()
        print(f"Error en anulación investigativa: {str(e)}")
        return jsonify({"success": False, "message": f"Error interno: {str(e)}"}), 500


def _enviar_correo_investigacion_anulacion(cur, cliente, ubicacion, codigo, factura, justificacion, usuario, perfil):
    """Envía el correo de alerta a Gerencia con copia a la aplicación."""
    cfg = _get_glp_settings()
    email_user = cfg["email_user"]
    email_pass = cfg["email_pass"]
    email_app = cfg["email_admin"]

    if not email_user or not email_pass:
        return

    cur.execute("SELECT email FROM contactos WHERE TRIM(UPPER(empresa)) = TRIM(UPPER(%s))", (cliente,))
    contactos = cur.fetchall()

    destinatarios = []
    for c in contactos:
        em = c['email'] if isinstance(c, dict) else c[0]
        if em:
            destinatarios.append(em)

    todos_los_correos = destinatarios + [email_app]
    rol_texto = "Administrador del Sistema (Webmaster)" if "webmaster" in perfil.lower() else "Supervisor de Sede"

    cuerpo = f"""
    <!DOCTYPE html>
    <html>
    <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
        <div style="background-color: #ffffff; max-width: 600px; margin: 0 auto; border-top: 5px solid #b71c1c; border-radius: 8px; padding: 30px; box-shadow: 0 4px 8px rgba(0,0,0,0.1);">
            <h2 style="color: #b71c1c; margin-top: 0;">🚨 INVESTIGACIÓN REQUERIDA</h2>
            <h3 style="color: #333; margin-bottom: 5px;">Anulación de Suministro GLP Sin Evidencia</h3>
            <p style="color: #666; font-size: 14px; margin-top: 0;">Se ha detectado la baja manual de un pedido previamente contabilizado.</p>
            <div style="background-color: #ffebee; border-left: 4px solid #b71c1c; padding: 15px; margin: 20px 0;">
                <p style="margin: 0; color: #c62828; font-weight: bold;">⚠️ Alerta Financiera:</p>
                <p style="margin: 5px 0 0 0; color: #b71c1c; font-size: 14px;">
                    Se requiere verificación contable urgente de la <b>Factura N° {factura}</b>, ya que no hay registro de ingreso al inventario físico en la granja.
                </p>
            </div>
            <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 14px;">
                <tr><td style="padding: 8px; border-bottom: 1px solid #eee;"><b>Cliente:</b></td><td style="padding: 8px; border-bottom: 1px solid #eee;">{cliente}</td></tr>
                <tr><td style="padding: 8px; border-bottom: 1px solid #eee;"><b>Sede (Granja):</b></td><td style="padding: 8px; border-bottom: 1px solid #eee;">{ubicacion}</td></tr>
                <tr><td style="padding: 8px; border-bottom: 1px solid #eee;"><b>Código Pedido:</b></td><td style="padding: 8px; border-bottom: 1px solid #eee;"><b>{codigo}</b></td></tr>
                <tr><td style="padding: 8px; border-bottom: 1px solid #eee;"><b>Acción ejecutada por:</b></td><td style="padding: 8px; border-bottom: 1px solid #eee;">{rol_texto}: {usuario}</td></tr>
            </table>
            <p style="font-weight: bold; color: #333;">Justificación registrada en el sistema:</p>
            <div style="background-color: #f5f5f5; border: 1px solid #ddd; padding: 15px; font-style: italic; color: #555; border-radius: 4px;">
                "{justificacion}"
            </div>
            <p style="margin-top: 30px; font-size: 12px; color: #999; text-align: center;">
                Notificación generada automáticamente por BQA-ONE. Una copia de este mensaje se almacena en los servidores de la aplicación con fines de auditoría.
            </p>
        </div>
    </body>
    </html>
    """

    try:
        msg = MIMEMultipart()
        msg["Subject"] = f"🚨 INVESTIGACIÓN REQUERIDA: Factura sin respaldo físico - {ubicacion}"
        msg["From"] = email_user
        msg["To"] = ", ".join(destinatarios) if destinatarios else email_app
        if destinatarios:
            msg["Cc"] = email_app
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_user
        msg.attach(MIMEText(cuerpo, "html", "utf-8"))

        with smtplib.SMTP(cfg["email_host"], cfg["email_port"]) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, todos_los_correos, msg.as_string())
    except Exception as e:
        print(f"⛔ Error al enviar correo de investigación: {str(e)}")


@bp_glp.route('/legalizar_tanqueo_extemporaneo', methods=['POST'])
@login_required_custom
def legalizar_tanqueo_extemporaneo():
    data = request.get_json(force=True, silent=True) or {}
    ctx = _obtener_contexto_sesion(data)
    pedido_id = data.get('pedido_id')
    fecha_real = _extraer_fecha_iso(data.get('fecha_real'))
    tanques = data.get('tanques', [])
    op_id = data.get('op_id')

    usuario = ctx["usuario"]
    es_webmaster = "webmaster" in ctx["perfil"].lower()

    if not pedido_id or not fecha_real or not tanques:
        return jsonify({"success": False, "message": "Faltan datos obligatorios para legalizar."}), 400

    check = _verificar_idempotencia(ctx["id_empresa"], op_id, "legalizar_tanqueo_extemporaneo", "Extemporáneo")
    if check:
        return check

    codigo_pedido = ""
    ubicacion = ""
    id_empresa_lote = ctx["id_empresa"]

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    if es_webmaster:
                        cur.execute("""
                            SELECT cliente, ubicacion, lote, proveedor, codigo_pedido
                            FROM pedidos_gas_glp
                            WHERE id = %s
                            FOR UPDATE
                        """, (pedido_id,))
                    else:
                        cur.execute("""
                            SELECT cliente, ubicacion, lote, proveedor, codigo_pedido
                            FROM pedidos_gas_glp
                            WHERE id = %s
                              AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                            FOR UPDATE
                        """, (pedido_id, ctx["empresa"]))

                    row_ped = cur.fetchone()
                    if not row_ped:
                        return jsonify({"success": False, "message": "Pedido no encontrado para esta empresa."}), 404

                    rd_ped = dict(zip([d[0] for d in cur.description], row_ped)) if not isinstance(row_ped, dict) else row_ped
                    cliente = _re.sub(r"\s+", " ", str(rd_ped['cliente'] or ctx["empresa"])).strip()
                    ubicacion = _normalize_sede(rd_ped['ubicacion'])
                    lote_id = rd_ped['lote']
                    proveedor = rd_ped['proveedor']
                    codigo_pedido = rd_ped['codigo_pedido']

                    if es_webmaster:
                        cur.execute("""
                            SELECT id_empresa, estatus_lote, pollitos
                            FROM cardex_glp
                            WHERE lote = %s
                            ORDER BY id DESC
                            LIMIT 1
                            FOR UPDATE
                        """, (lote_id,))
                    else:
                        cur.execute("""
                            SELECT id_empresa, estatus_lote, pollitos
                            FROM cardex_glp
                            WHERE id_empresa = %s AND lote = %s
                            ORDER BY id DESC
                            LIMIT 1
                            FOR UPDATE
                        """, (ctx["id_empresa"], lote_id))

                    row_lote = cur.fetchone()
                    estatus_lote_heredado = "INACTIVO"
                    pollitos_lote = 0
                    if row_lote:
                        rd_lote = dict(zip([d[0] for d in cur.description], row_lote)) if not isinstance(row_lote, dict) else row_lote
                        id_empresa_lote = int(rd_lote.get('id_empresa') or ctx["id_empresa"])
                        estatus_lote_heredado = rd_lote.get('estatus_lote') or "INACTIVO"
                        pollitos_lote = rd_lote.get('pollitos') or 0

                    columnas = [
                        "fecha", "empresa", "id_empresa", "ubicacion", "lote",
                        "estatus_lote", "operacion", "tipo", "clase", "registro",
                        "op_id", "codigo_pedido", "pollitos"
                    ]
                    valores = [
                        fecha_real, cliente, id_empresa_lote, ubicacion, lote_id,
                        estatus_lote_heredado, "tanqueo", "manual", "ingreso", f"{usuario} (Extemp.)",
                        op_id, codigo_pedido, pollitos_lote
                    ]
                    cur.execute(
                        f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s'] * len(columnas))})",
                        valores
                    )
                    id_operacion = cur.lastrowid

                    carpeta = os.path.join("testigos", cliente.replace(" ", "_"), lote_id)
                    set_cols = []
                    set_vals = []
                    densidades = []
                    masas_facturadas = []

                    for tk in tanques:
                        match_n = _re.search(r'\d+', str(tk.get("numero", "")))
                        num = match_n.group() if match_n else str(tk.get("numero", "")).upper().replace("TK-", "").replace("TK", "").strip()
                        if not num:
                            continue

                        cap = float(tk.get("capacidad", 0) or 0.0)
                        nivel_ini = float(tk.get("nivel_inicial", 0) or 0.0)
                        nivel_fin = float(tk.get("nivel_final", 0) or 0.0)

                        set_cols.extend([f"`nivel tk-{num}` = %s", f"`nivelfinal tk-{num}` = %s", f"`capacidad tk-{num}` = %s"])
                        set_vals.extend([nivel_ini, nivel_fin, cap])

                        if tk.get("foto_nivel_inicial"):
                            ruta = _guardar_testigo(tk.get("foto_nivel_inicial"), carpeta, f"tk{num}_ini_ext_{id_operacion}")
                            if ruta:
                                set_cols.append(f"`testigo nivel tk-{num}` = %s")
                                set_vals.append(ruta)

                        if tk.get("foto_nivel_final"):
                            ruta = _guardar_testigo(tk.get("foto_nivel_final"), carpeta, f"tk{num}_fin_ext_{id_operacion}")
                            if ruta:
                                set_cols.append(f"`testigo nivelfinal tk-{num}` = %s")
                                set_vals.append(ruta)

                        if tk.get("foto_baucher"):
                            ruta = _guardar_testigo(tk.get("foto_baucher"), carpeta, f"tk{num}_baucher_ext_{id_operacion}")
                            if ruta:
                                set_cols.append(f"`testigo_baucher_tk_{num}` = %s")
                                set_vals.append(ruta)

                        dens = float(tk.get("densidad_suministrada") or 0.0)
                        if dens > 0:
                            densidades.append(dens)
                        masas_facturadas.append(float(tk.get("kg_suministrados") or 0.0))

                    if set_cols:
                        cur.execute(
                            f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id = %s AND id_empresa = %s",
                            set_vals + [id_operacion, id_empresa_lote]
                        )

                    dens_prom = sum(densidades) / len(densidades) if densidades else 2.0
                    masa_facturada_total = sum(masas_facturadas)

                    cur.execute("""
                        UPDATE cardex_glp
                        SET densidad_suministrada = %s,
                            masa_kg_facturada = %s,
                            proveedor = %s
                        WHERE id = %s AND id_empresa = %s
                    """, (round(dens_prom, 3), round(masa_facturada_total, 2), proveedor, id_operacion, id_empresa_lote))

                    cur.execute("""
                        UPDATE pedidos_gas_glp
                        SET estatus_flujo = 'legalizado_extemporaneo'
                        WHERE id = %s
                          AND TRIM(UPPER(cliente)) = TRIM(UPPER(%s))
                    """, (pedido_id, cliente))

                    _recalcular_historia_lote(cur, id_empresa_lote, ubicacion, lote_id)

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Legalizar Tanqueo Extemp - Código: {codigo_pedido} - Sede: {ubicacion} - Fecha: {fecha_real}"
                    )

                    mysql.connection.commit()
                    break
            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        try:
            registrar_auditoria(
                id_empresa_lote, ctx["empresa"], "GLP", usuario,
                "Legalización Extemporánea",
                f"Se insertó tanqueo ({codigo_pedido}) en {fecha_real} para {ubicacion}. Historia recalculada.",
                "INFO"
            )
        except Exception:
            pass

        return jsonify({
            "success": True,
            "message": "Tanqueo legalizado exitosamente. La historia de la granja ha sido recalculada y saneada."
        })

    except IntegrityError as e_int:
        return _manejar_error_idempotencia(e_int, id_empresa_lote, op_id, "legalizar_tanqueo_extemporaneo", ubicacion)
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error en legalización extemporánea: {str(e)}")
        traceback.print_exc()
        return jsonify({"success": False, "message": f"Error del sistema: {str(e)}"}), 500


def _recalcular_historia_lote(cur, id_empresa, ubicacion, lote_id):
    """
    Recorre TODOS los registros del lote en orden cronológico (fecha ASC, id ASC)
    filtrando por id_empresa (Ley 1) y soportando los 11 tanques del esquema cardex_glp.
    """
    cols_tk = []
    for i in range(1, 12):
        cols_tk.extend([f"`nivel tk-{i}`", f"`nivelfinal tk-{i}`", f"`capacidad tk-{i}`"])

    cur.execute(f"""
        SELECT id, operacion, pollitos, {', '.join(cols_tk)}, densidad_suministrada
        FROM cardex_glp
        WHERE id_empresa = %s
          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
          AND lote = %s
        ORDER BY fecha ASC, id ASC
    """, (id_empresa, ubicacion, lote_id))

    registros = cur.fetchall()
    if not registros:
        return

    col_names = [d[0] for d in cur.description]
    pollitos_base = 0
    acumulado_kg = 0.0
    niveles_anteriores = {}

    for row in registros:
        r = dict(zip(col_names, row)) if not isinstance(row, dict) else row
        row_id = r['id']
        operacion = r['operacion']

        if operacion == 'inicio_calefaccion' and r.get('pollitos'):
            pollitos_base = int(r['pollitos'])
            acumulado_kg = 0.0

        consumo_paso = 0.0
        saldo_paso = 0.0
        densidad_usada = float(r.get('densidad_suministrada') or 2.0)
        if densidad_usada <= 0:
            densidad_usada = 2.0

        for i in range(1, 12):
            cap = float(r.get(f'capacidad tk-{i}') or 0.0)
            if cap <= 0:
                continue

            niv_ini = r.get(f'nivel tk-{i}')
            niv_fin = r.get(f'nivelfinal tk-{i}')

            if operacion in ['consumo', 'tanqueo', 'finalizar_calefaccion']:
                if niv_ini is not None and str(i) in niveles_anteriores:
                    delta = niveles_anteriores[str(i)] - float(niv_ini)
                    if delta > 0:
                        consumo_paso += (delta / 100.0) * cap * 2.0

            nivel_cierre_tk = None
            if operacion == 'tanqueo' and niv_fin is not None:
                nivel_cierre_tk = float(niv_fin)
            elif niv_ini is not None:
                nivel_cierre_tk = float(niv_ini)

            if nivel_cierre_tk is not None:
                niveles_anteriores[str(i)] = nivel_cierre_tk
                saldo_paso += (nivel_cierre_tk / 100.0) * cap * densidad_usada

        acumulado_kg += consumo_paso
        eficiencia = (acumulado_kg / float(pollitos_base)) if pollitos_base > 0 else 0.0

        cur.execute("""
            UPDATE cardex_glp
            SET neto_gastado = %s,
                kg_pollito = %s,
                saldo_estimado_kg = %s,
                saldo_estimado_galones = %s
            WHERE id = %s AND id_empresa = %s
        """, (
            round(consumo_paso, 2),
            round(eficiencia, 6),
            round(saldo_paso, 2),
            round(saldo_paso / densidad_usada if densidad_usada else 0.0, 2),
            row_id,
            id_empresa
        ))


# ==========================================
# CONFIRMACIÓN DE ARRIBO DE POLLITOS (SOLUCIÓN CASO CHUCUNÍ)
# ==========================================
@bp_glp.route('/confirmar_arribo_pollito', methods=['POST'])
@login_required_custom
def confirmar_arribo_pollito():
    try:
        data = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido."}), 400

    ctx = _obtener_contexto_sesion(data)
    sede = _normalize_sede(data.get('sede'))
    id_empresa = ctx["id_empresa"]
    usuario = ctx["usuario"]
    op_id_req = data.get('op_id')

    try:
        poblacion_real = int(float(data.get('poblacion') or 0))
    except Exception:
        poblacion_real = 0

    fecha_arribo_raw = data.get('fecha_arribo')
    fecha_arribo_date = _extraer_fecha_iso(fecha_arribo_raw)

    if not sede or not id_empresa or poblacion_real <= 0 or not fecha_arribo_date:
        return jsonify({
            "success": False,
            "message": "Datos incompletos: verifica la sede, la población (>0) y la fecha de arribo."
        }), 400

    es_sync = bool(data.get('is_sync', False))
    ya_confirmado_previo = False
    pollitos_est = 0
    lote_nom = ""
    dias_op = 1

    max_intentos = 3
    try:
        for intento in range(max_intentos):
            try:
                with mysql.connection.cursor() as cur:
                    cur.execute("""
                        SELECT id, pollitos, lote, registro
                        FROM cardex_glp
                        WHERE id_empresa = %s
                          AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                          AND operacion = 'inicio_calefaccion'
                          AND estatus_lote = 'ACTIVO'
                        ORDER BY id DESC
                        LIMIT 1
                        FOR UPDATE
                    """, (id_empresa, sede))
                    registro_row = cur.fetchone()

                    if not registro_row:
                        return jsonify({"success": False, "message": "No se encontró un lote activo en espera en esta sede."})

                    if isinstance(registro_row, dict):
                        id_inicio = registro_row.get("id")
                        pollitos_est = int(registro_row.get("pollitos") or 0)
                        lote_nom = registro_row.get("lote") or ""
                        reg_texto = registro_row.get("registro") or ""
                    else:
                        id_inicio = registro_row[0]
                        pollitos_est = int(registro_row[1] or 0)
                        lote_nom = registro_row[2] or ""
                        reg_texto = registro_row[3] or ""

                    # Idempotencia: si ya tiene '| Conf. por ', actualizar población/fecha limpia sin duplicar etiqueta
                    if "| Conf. por " in reg_texto:
                        ya_confirmado_previo = True
                        cur.execute("""
                            UPDATE cardex_glp
                            SET pollitos = %s,
                                fecha_llegada_pollitos = %s
                            WHERE id = %s AND id_empresa = %s
                        """, (poblacion_real, fecha_arribo_date, id_inicio, id_empresa))
                    else:
                        ya_confirmado_previo = False
                        cur.execute("""
                            UPDATE cardex_glp
                            SET pollitos = %s,
                                fecha_llegada_pollitos = %s,
                                registro = CONCAT(COALESCE(registro, ''), ' | Conf. por ', %s)
                            WHERE id = %s AND id_empresa = %s
                        """, (poblacion_real, fecha_arribo_date, usuario, id_inicio))

                    dias_op = _calcular_actualizar_dias_operacion(cur, id_empresa, sede, lote_nom, datetime.now().date())

                    _registrar_operacion_bqa_one(
                        cur,
                        ctx,
                        f"Confirmar arribo pollito - Sede: {sede} - Lote: {lote_nom} - Pob Real: {poblacion_real} - Fecha: {fecha_arribo_date} - OP: {op_id_req or 'N/A'}"
                    )

                    mysql.connection.commit()
                    break
            except OperationalError as e_op:
                mysql.connection.rollback()
                if e_op.args and e_op.args[0] == 1213 and intento < max_intentos - 1:
                    time.sleep(0.25 * (intento + 1))
                    continue
                raise e_op

        # =========================================================
        # ZONA ESTRICTA POST-COMMIT: TELEGRAM SOLO SI NO ERA DUPLICADO
        # =========================================================
        if not ya_confirmado_previo:
            cfg = _get_glp_settings()
            prefijo_sync = "🔄 *(SYNC)* " if es_sync else ""
            texto_est = f"{pollitos_est:,}" if pollitos_est > 0 else "No reportada"
            msg = (
                f"{prefijo_sync}🐣 *ALOJAMIENTO CONFIRMADO*\n"
                f"📍 *Sede:* {sede}\n"
                f"👤 *Operador:* {usuario}\n"
                f"🔢 *Población Real:* {poblacion_real:,} (Est: {texto_est})\n"
                f"📅 *Arribo:* {fecha_arribo_date.strftime('%Y-%m-%d')}\n"
                f"✅ El reloj del lote *{lote_nom}* ha iniciado."
            )
            try:
                chat_ids = _obtener_chat_ids_empresa(id_empresa)
                _enviar_mensajes_telegram_hilo(chat_ids, msg, cfg["telegram_token"])
            except Exception as e_tel:
                print(f"Error enviando alerta alojamiento Telegram: {e_tel}")

            try:
                registrar_auditoria(
                    id_empresa, ctx["empresa"], "GLP", usuario,
                    "Confirmación Arribo Pollitos",
                    f"Sede: {sede}. Lote: {lote_nom}. Población real: {poblacion_real}. Arribo: {fecha_arribo_date}.",
                    "INFO"
                )
            except Exception:
                pass

        return jsonify({
            "success": True,
            "idempotent": ya_confirmado_previo,
            "message": "Arribo confirmado correctamente y lote activado.",
            "resumen": {
                "operacion": "confirmar_arribo",
                "sede": sede,
                "lote": lote_nom,
                "pollitos": poblacion_real,
                "dias_operacion": dias_op,
                "op_id": op_id_req
            }
        })

    except Exception as e:
        mysql.connection.rollback()
        print(traceback.format_exc())
        return jsonify({"success": False, "message": f"Error confirmando arribo: {str(e)}"}), 500


# ==========================================
# RUTA PARA ANÁLISIS DE LOTE (DASHBOARD)
# ==========================================
@bp_glp.route('/consultar_analisis_lote', methods=['POST'])
@login_required_custom
def consultar_analisis_lote():
    try:
        data = request.get_json(force=True, silent=True) or {}
        ctx = _obtener_contexto_sesion(data)
        sede = _normalize_sede(data.get('sede'))
        id_empresa = ctx["id_empresa"]

        if not sede or not id_empresa:
            return jsonify({"success": False, "message": "Faltan datos o sesión expirada."}), 400

        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT lote
                FROM cardex_glp
                WHERE id_empresa = %s
                  AND TRIM(UPPER(ubicacion)) = TRIM(UPPER(%s))
                  AND estatus_lote = 'ACTIVO'
                ORDER BY id DESC
                LIMIT 1
            """, (id_empresa, sede))
            lote_row = cur.fetchone()

            if not lote_row:
                return jsonify({"success": False, "message": "No hay un lote ACTIVO en esta sede para analizar."})

            lote_id = lote_row['lote'] if isinstance(lote_row, dict) else lote_row[0]

            # Validación Multi-Tenant estricta (Ley 1) en población de arranque
            cur.execute("""
                SELECT pollitos
                FROM cardex_glp
                WHERE id_empresa = %s
                  AND lote = %s
                  AND operacion = 'inicio_calefaccion'
                LIMIT 1
            """, (id_empresa, lote_id))
            pop_row = cur.fetchone()
            pollitos_arranque = 0
            if pop_row:
                val_pop = pop_row['pollitos'] if isinstance(pop_row, dict) else pop_row[0]
                pollitos_arranque = int(val_pop or 0)

            cur.execute("""
                SELECT fecha, operacion, neto_gastado, kg_pollito, saldo_estimado_kg, dias_operacion, precio_total
                FROM cardex_glp
                WHERE lote = %s AND id_empresa = %s
                ORDER BY fecha ASC, id ASC
            """, (lote_id, id_empresa))

            historial = cur.fetchall()
            labels = []
            data_eficiencia = []

            total_consumo = 0.0
            total_facturado = 0.0
            ultimo_saldo = 0.0
            dias_lote = 0
            eficiencia_actual = 0.0
            consumo_por_fecha = {}

            for row in historial:
                r = dict(zip([d[0] for d in cur.description], row)) if not isinstance(row, dict) else row
                fecha_str = r['fecha'].strftime('%Y-%m-%d') if hasattr(r['fecha'], 'strftime') else str(r['fecha'])

                if fecha_str not in consumo_por_fecha:
                    consumo_por_fecha[fecha_str] = {'eficiencia': 0.0}

                total_consumo += float(r.get('neto_gastado') or 0.0)
                total_facturado += float(r.get('precio_total') or 0.0)

                if float(r.get('saldo_estimado_kg') or 0.0) >= 0 and r.get('saldo_estimado_kg') is not None:
                    ultimo_saldo = float(r.get('saldo_estimado_kg') or 0.0)
                if r.get('dias_operacion') is not None:
                    dias_lote = int(r.get('dias_operacion') or 0)

                ef_tk = float(r.get('kg_pollito') or 0.0)
                if ef_tk > 0:
                    eficiencia_actual = ef_tk
                    consumo_por_fecha[fecha_str]['eficiencia'] = ef_tk

            for f, vals in consumo_por_fecha.items():
                labels.append(f[-5:])
                if vals['eficiencia'] > 0:
                    data_eficiencia.append(round(vals['eficiencia'], 4))
                else:
                    data_eficiencia.append(data_eficiencia[-1] if data_eficiencia else 0.0)

            costo_por_ave = 0.0
            if pollitos_arranque and pollitos_arranque > 0:
                costo_por_ave = total_facturado / float(pollitos_arranque)

            return jsonify({
                "success": True,
                "kpis": {
                    "sede": sede,
                    "lote": lote_id,
                    "dias_operacion": dias_lote,
                    "poblacion": pollitos_arranque or 0,
                    "total_consumo_kg": round(total_consumo, 2),
                    "eficiencia_actual": round(eficiencia_actual, 4),
                    "saldo_actual_kg": round(ultimo_saldo, 2),
                    "total_facturado": round(total_facturado, 0),
                    "costo_por_ave": round(costo_por_ave, 2)
                },
                "grafica": {
                    "labels": labels,
                    "eficiencia": data_eficiencia
                }
            })

    except Exception as e:
        print(f"Error en analisis de lote: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


# ==========================================
# NOTIFICACIONES DE SINCRONIZACIÓN Y COLA OFFLINE (CSRF ACTIVO - LEY 4)
# ==========================================
@bp_glp.route('/notificar_intento_sync', methods=['POST'])
@login_required_custom
def notificar_intento_sync():
    """Informa a Telegram de la intención de sincronización manual del operador."""
    try:
        data = request.get_json(force=True, silent=True) or {}
        ctx = _obtener_contexto_sesion(data)
        sede = _normalize_sede(data.get('sede')) or 'Sede Desconocida'
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M")

        notificar_opglp_telegram(
            ctx["id_empresa"], ctx["usuario"],
            "Intento de Sincronización Manual", sede, fecha, estado="pendientes"
        )
        return jsonify({"success": True})
    except Exception as e:
        print(f"Error en notificar_intento_sync: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@bp_glp.route('/notificar_operaciones_pendientes_background', methods=['POST'])
@login_required_custom
def notificar_operaciones_pendientes_background():
    """
    Ruta disparada silenciosamente por la PWA con X-CSRFToken activo (Ley 4)
    cuando los 3 reintentos de red se agotan y el registro pasa a IndexedDB.
    """
    try:
        data = request.get_json(force=True, silent=True) or {}
        ctx = _obtener_contexto_sesion(data)
        sede = _normalize_sede(data.get('sede')) or 'Sede en campo'
        fecha = datetime.now().strftime("%Y-%m-%d %H:%M")

        notificar_opglp_telegram(
            ctx["id_empresa"], ctx["usuario"],
            "Operación Retenida Offline", sede, fecha, estado="pendientes"
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500