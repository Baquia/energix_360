# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, session
from flask import current_app as app
from datetime import datetime, timedelta
from app.utils import login_required_custom
import os, base64, smtplib, traceback, random
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


def _generar_codigo_pedido(cur, empresa, proveedor, fecha):
    """Genera un código único tipo MM-XXX-9999 y lo registra en pedidos_gas_glp.

    - MM: mes del pedido (01-12)
    - XXX: iniciales de las tres primeras palabras del nombre del cliente
    - 9999: número aleatorio de 4 dígitos, único en la tabla
    """
    # Mes con dos dígitos
    try:
        mes = int(getattr(fecha, "month", 0) or 0)
    except Exception:
        mes = 0
    if mes <= 0 or mes > 12:
        mes = datetime.now().month
    pref_mes = f"{mes:02d}"

    # Iniciales del cliente (3 primeras palabras)
    palabras = (empresa or "").strip().split()
    iniciales = "".join([p[0].upper() for p in palabras[:3]])
    if len(iniciales) < 3:
        iniciales = (iniciales + "XXX")[:3]
    base = f"{pref_mes}-{iniciales}-"

    # Intentar generar un código único hasta 20 veces
    for _ in range(20):
        suf = f"{random.randint(0, 9999):04d}"
        codigo = base + suf

        cur.execute("SELECT 1 FROM pedidos_gas_glp WHERE codigo=%s LIMIT 1", (codigo,))
        if not cur.fetchone():
            # Insertar registro en la tabla de control de pedidos
            cur.execute(
                """
                INSERT INTO pedidos_gas_glp (cliente, proveedor, codigo, estatus, fecha_registro)
                VALUES (%s, %s, %s, %s, %s)
                """.strip(),
                (empresa, proveedor, codigo, "generado", fecha)
            )
            return codigo

    raise Exception("No se pudo generar un código de pedido único tras varios intentos")


def _guardar_testigo(base64_data, carpeta, nombre_archivo):
    """
    Guarda una imagen base64 en disco y devuelve la ruta relativa (para almacenar en DB).
    """
    if not base64_data:
        return None
    try:
        if "," in base64_data:
            base64_data = base64_data.split(",", 1)[1]
        img_bytes = base64.b64decode(base64_data)
    except Exception:
        return None

    os.makedirs(carpeta, exist_ok=True)
    ruta_fisica = os.path.join(carpeta, nombre_archivo)
    with open(ruta_fisica, "wb") as f:
        f.write(img_bytes)

    # Devolver ruta relativa (pensando en static/)
    if ruta_fisica.startswith("static/"):
        return "/" + ruta_fisica.replace("\\", "/")
    return ruta_fisica.replace("\\", "/")


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


def _cargar_tanques_desde_servidor(cur, empresa, sede, lote):
    """
    Carga tanques desde cardex_glp (último inicio_calefaccion para ese lote).
    """
    cur.execute("""
        SELECT *
        FROM cardex_glp
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
          AND lote = %s
          AND operacion = 'inicio_calefaccion'
        ORDER BY fecha DESC, id DESC
        LIMIT 1
    """, (empresa, sede, lote))
    row = cur.fetchone()
    if not row:
        return []

    tanques = []
    for i in range(1, 6):
        nivel = row.get(f"nivel tk-{i}")
        capac = row.get(f"capacidad tk-{i}")
        if nivel is None and capac is None:
            continue
        tanques.append({
            "numero": f"tk-{i}",
            "nivel": float(nivel or 0),
            "capacidad": float(capac or 0),
        })
    return tanques


def _cargar_tanques_desde_qr(qr_data):
    """
    Extrae tanques desde el JSON del QR.
    Espera un campo 'tanques' con lista de: {numero, capacidad}.
    """
    tanques_qr = []
    for tk in (qr_data.get("tanques") or []):
        numero = tk.get("numero")
        try:
            cap = float(tk.get("capacidad", 0))
        except Exception:
            cap = 0.0
        if numero and cap > 0:
            tanques_qr.append({
                "numero": numero,
                "nivel": None,
                "capacidad": cap
            })
    return tanques_qr


def _cargar_tanques_desde_cache(empresa, sede, lote):
    """
    Placeholder: si luego usas una tabla local/cache en servidor.
    De momento, retornamos lista vacía.
    """
    return []


def _actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha_actual=None):
    """
    Calcula y actualiza los días de operación de un lote en cardex_glp.
    - Cuenta desde MIN(fecha) del lote
    - Hasta fecha_actual (default: hoy)
    - Día de inicio = 1
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
    row = cur.fetchone()
    if not row or not row.get("fecha_ini"):
        dias_operacion = 1
    else:
        fecha_ini = row["fecha_ini"]
        if isinstance(fecha_ini, datetime):
            fecha_ini = fecha_ini.date()
        if fecha_ini > fecha_actual:
            dias_operacion = 1
        else:
            dias_operacion = (fecha_actual - fecha_ini).days + 1

    cur.execute("""
        UPDATE cardex_glp
           SET dias_operacion = %s
         WHERE empresa = %s
           AND TRIM(ubicacion) = TRIM(%s)
           AND lote = %s
           AND operacion IN ('inicio_calefaccion','registrar_consumo','finalizar_calefaccion')
    """, (dias_operacion, empresa, ubicacion, lote_id))

    return dias_operacion


def _buscar_proveedor_principal(cur, empresa, sede, tanques):
    """
    Determina el proveedor principal de GLP para esa sede, usando:
    - Si cardex_glp ya tiene proveedor reciente.
    - Si no, se intenta por tabla proveedores_sede (si la tienes).
    - Si no, devuelve None.
    """
    cur.execute("""
        SELECT proveedor
        FROM cardex_glp
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
          AND proveedor IS NOT NULL
          AND proveedor <> ''
        ORDER BY fecha DESC, id DESC
        LIMIT 1
    """, (empresa, sede))
    row = cur.fetchone()
    if row and row.get("proveedor"):
        return row["proveedor"]

    cur.execute("""
        SELECT proveedor
        FROM proveedores_sede
        WHERE empresa = %s
          AND TRIM(ubicacion) = TRIM(%s)
        ORDER BY id DESC
        LIMIT 1
    """, (empresa, sede))
    row = cur.fetchone()
    if row and row.get("proveedor"):
        return row["proveedor"]

    return None


# =============================
# ENDPOINT: obtener tanques GLP
# =============================
@bp_glp.route('/obtener_tanques', methods=['POST'])
@login_required_custom
def obtener_tanques():
    """
    Recibe:
      - empresa (por sesión)
      - sede (en JSON)
      - lote_id
      - qr_data (opcional), json con tanques/capacidad
    Prioridad:
      1) Servidor (cardex_glp)
      2) Caché servidor (si implementamos)
      3) Datos embebidos en QR
    """
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

    empresa = session.get('empresa') or ''
    sede = _normalize_sede(data.get("sede"))
    lote = data.get("lote_id") or data.get("lote") or ''
    qr_data = data.get("qr_data") or {}

    if not empresa or not sede:
        return jsonify({"success": False, "message": "Faltan empresa o sede."}), 400

    try:
        with mysql.connection.cursor() as cur:
            tanques = _cargar_tanques_desde_servidor(cur, empresa, sede, lote)
            source = "servidor"

            if not tanques:
                tanques = _cargar_tanques_desde_cache(empresa, sede, lote)
                source = "cache" if tanques else source

            if not tanques:
                tanques = _cargar_tanques_desde_qr(qr_data)
                source = "qr" if tanques else source

        if not tanques:
            return jsonify({
                "success": False,
                "message": "No se encontraron tanques para la sede/lote.",
                "tanques": [],
                "source": None
            })

        return jsonify({
            "success": True,
            "message": f"Tanques obtenidos desde {source}.",
            "tanques": tanques,
            "source": source
        })

    except Exception:
        print("⛔ Error en /obtener_tanques")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Error interno al obtener tanques."}), 500


# ======================
# Iniciar calefacción
# ======================
@csrf.exempt
@bp_glp.route('/registrar_inicio', methods=['POST'])
@login_required_custom
def registrar_inicio_calefaccion():
    try:
        data = request.get_json(force=True)
    except Exception as e:
        return jsonify({"success": False, "message": "Error leyendo JSON"}), 400

    if not data:
        return jsonify({"success": False, "message": "No se recibió JSON válido"}), 400

    empresa       = session.get('empresa') or ''
    empresa_id    = session.get('empresa_id') or session.get('nit')
    usuario_id    = session.get('usuario_id')
    operador_id   = str(session.get('cedula') or session.get('usuario_id') or '').strip()
    operador_nombre = (
        session.get('usuario_nombre') or
        session.get('nombre') or
        session.get('usuario') or
        ''
    )

    ubicacion  = _normalize_sede(data.get('sede'))
    lote_id    = data.get('lote_id') or data.get('lote') or ''
    pollitos   = data.get('pollitos')
    criadoras  = data.get('criadoras')
    tanques    = data.get('tanques') or []
    densidad   = float(data.get('densidad_estimada') or 0.54)

    fecha_str = data.get('fecha') or ''
    try:
        if fecha_str:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
        else:
            fecha = datetime.now().date()
    except Exception:
        fecha = datetime.now().date()

    if not ubicacion or not lote_id:
        return jsonify({"success": False, "message": "Faltan sede o lote"}), 400

    op_id = data.get('op_id') or ''

    try:
        with mysql.connection.cursor() as cur:
            # Idempotencia por op_id
            cur.execute("""
                SELECT id
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND lote=%s
                  AND operacion='inicio_calefaccion'
                  AND op_id = %s
                LIMIT 1
            """, (empresa, ubicacion, lote_id, op_id))
            dup = cur.fetchone()
            if dup:
                return jsonify({
                    "success": True,
                    "message": "Operación ya registrada (idempotente).",
                    "resumen": None
                })

            columnas = [
                "empresa", "ubicacion", "lote", "fecha",
                "operacion", "pollitos", "criadoras",
                "operador_id", "operador_nombre",
                "saldo_estimado_kg", "saldo_estimado_galones",
                "dias_operacion", "proveedor", "op_id"
            ]
            valores = [
                empresa, ubicacion, lote_id, fecha,
                "inicio_calefaccion", pollitos, criadoras,
                operador_id, operador_nombre,
                0.0, 0.0,
                0, None, op_id
            ]

            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

            set_cols, set_vals = [], []
            densidad = 2.0
            saldo_estimado_kg = 0.0

            for tk in tanques:
                numero = tk.get("numero")
                try:
                    nivel  = float(tk.get("nivel",0))
                except Exception:
                    nivel = 0.0
                try:
                    capacidad = float(tk.get("capacidad",0))
                except Exception:
                    capacidad = 0.0
                testigo = tk.get("testigo")

                if numero and nivel is not None:
                    set_cols.append(f"`nivel {numero}`=%s"); set_vals.append(nivel)
                if numero and capacidad is not None:
                    set_cols.append(f"`capacidad {numero}`=%s"); set_vals.append(capacidad)
                if numero and testigo:
                    ruta_web = _guardar_testigo(testigo, carpeta, f"{numero}.jpg")
                    set_cols.append(f"`testigo nivel {numero}`=%s"); set_vals.append(ruta_web)

                saldo_estimado_kg += densidad * capacidad * (nivel/100.0)

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

            dias_operacion = _actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
            cur.execute("""
                UPDATE cardex_glp
                   SET proveedor=%s
                 WHERE lote=%s AND operacion='inicio_calefaccion'
            """, (proveedor_principal, lote_id))

            # Tu fórmula de solicitud de tanqueo (17-n, cap 80)
            tanques_bajos = []
            for tk in tanques:
                numero = tk.get("numero")
                try:
                    nivel_inicial = float(tk.get("nivel",0))
                except Exception:
                    nivel_inicial = 0.0

                if nivel_inicial <= 30.0:
                    n = dias_operacion
                    pct_req = 8 * (17 - n)
                    pct_sol_bruto = nivel_inicial + pct_req
                    pct_sol = min(80.0, pct_sol_bruto)

                    tanques_bajos.append({
                        "numero": numero,
                        "nivel_inicial": nivel_inicial,
                        "dias_transcurridos": n,
                        "porcentaje_requerido": round(pct_req,2),
                        "porcentaje_solicitado": round(pct_sol,2)
                    })

            if tanques_bajos and proveedor_principal:
                cur.execute("SELECT email1,email2 FROM proveedores WHERE proveedor=%s",
                            (proveedor_principal,))
                c = cur.fetchone() or {}
                destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    # Generar código de pedido de gas y registrarlo en pedidos_gas_glp
                    try:
                        codigo_pedido = _generar_codigo_pedido(cur, empresa, proveedor_principal, fecha)
                    except Exception as e:
                        print("[GLP] No se pudo generar código de pedido:", e)
                        codigo_pedido = None

                    # Cuerpo del correo en HTML, resaltando el código de pedido
                    cuerpo = (
                        "<p>📩 <b>Solicitud de Tanqueo GLP</b></p>"
                        f"<p><b>Empresa:</b> {empresa}<br>"
                        f"<b>Sede:</b> {ubicacion}<br>"
                        f"<b>Lote:</b> {lote_id}<br>"
                        f"<b>Fecha:</b> {fecha.strftime('%Y-%m-%d')}<br>"
                        f"<b>Día de calefacción:</b> {dias_operacion}</p>"
                    )

                    if codigo_pedido:
                        cuerpo += (
                            "<p><b>Código de validación del pedido:</b> "
                            f"<b><u>{codigo_pedido}</u></b></p>"
                            "<p><i>Nota:</i> Para que la factura sea válida, "
                            "el proveedor debe incluir este código en la factura.</p>"
                        )

                    cuerpo += "<p><b>Recomendaciones de llenado:</b><br>"
                    for t in tanques_bajos:
                        cuerpo += (
                            f"- {t['numero']}: nivel {t['nivel_inicial']}% → "
                            f"llenar hasta {t['porcentaje_solicitado']}%<br>"
                        )
                    cuerpo += "</p>"

                    try:
                        msg = MIMEText(cuerpo, "html", "utf-8")
                        msg["Subject"] = "Solicitud de Tanqueo GLP"
                        msg["From"] = EMAIL_FROM
                        msg["To"] = ", ".join(destinatarios)
                        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                            server.starttls()
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
                        print("✅ Correo GLP enviado a:", destinatarios)
                    except Exception:
                        print("⛔ Error al enviar correo GLP:")
                        traceback.print_exc()

            mysql.connection.commit()

        mensaje = f"Lote {lote_id} registrado correctamente."
        if tanques_bajos:
            mensaje += " ⚠️ Se solicitó tanqueo vía email."

        resumen = {
            "operacion": "inicio_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "pollitos": pollitos if empresa=="Pollos GAR SAS" else None,
            "criadoras": criadoras if empresa=="Pollos GAR SAS" else None,
            "tanques": _resumen_tanques(tanques),
            "dias_operacion": dias_operacion,
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "saldo_estimado_galones": round(saldo_estimado_gal,2),
            "proveedor": proveedor_principal,
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except Exception:
        print("⛔ Error en registrar_inicio_calefaccion:")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Error al registrar los datos."})


# ======================
# Registrar tanqueo
# ======================
@csrf.exempt
@bp_glp.route('/registrar_tanqueo', methods=['POST'])
@login_required_custom
def registrar_tanqueo():
    """
    Registra un tanqueo por tanque, incluyendo:
    - nivel antes del tanqueo
    - nivel después del tanqueo
    - densidad, kg suministrados
    - fotos (testigos)
    """
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

    empresa = session.get('empresa') or ''
    usuario = session.get('nombre') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion = _normalize_sede(data.get('sede'))
    lote_id   = data.get('lote_id') or data.get('lote') or ''
    tanques   = data.get('tanques', []) or []
    fecha_str = data.get('fecha') or ''
    op_id     = data.get('op_id') or ''
    densidad  = float(data.get('densidad') or 0.54)

    if not empresa or not ubicacion or not lote_id:
        return jsonify({"success": False, "message": "Faltan datos de empresa/sede/lote."}), 400

    try:
        if fecha_str:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
        else:
            fecha = datetime.now().date()
    except Exception:
        fecha = datetime.now().date()

    # Idempotencia (si ya existe op_id con registrar_tanqueo)
    cur_check = mysql.connection.cursor()
    cur_check.execute("""
        SELECT id
        FROM cardex_glp
        WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s)
          AND lote=%s AND operacion='registrar_tanqueo'
          AND op_id=%s
        LIMIT 1
    """, (empresa, ubicacion, lote_id, op_id))
    dup = cur_check.fetchone()
    if dup:
        return jsonify({
            "success": True,
            "message": "Operación ya recibida (idempotente).",
            "resumen": {"operacion": "tanqueo", "sede": data.get("sede","")}
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

    try:
        with mysql.connection.cursor() as cur:
            columnas = [
                "empresa", "ubicacion", "lote", "fecha",
                "operacion", "usuario", "id_empresa",
                "densidad", "op_id"
            ]
            valores = [
                empresa, ubicacion, lote_id, fecha,
                "registrar_tanqueo", usuario, id_empresa,
                densidad, op_id
            ]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

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
                foto_ini = tk.get("testigo_inicio")
                foto_fin = tk.get("testigo_final")
                testigo_baucher = tk.get("testigo_boucher")

                if num and niv is not None:
                    set_cols.append(f"`nivel {num}`=%s"); set_vals.append(niv)
                if num and cap is not None:
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)

                if foto_ini:
                    ruta_ini = _guardar_testigo(foto_ini, carpeta, f"tanqueo_ini_{num}.jpg")
                    set_cols.append(f"`testigo tanqueo ini {num}`=%s"); set_vals.append(ruta_ini)
                if foto_fin:
                    ruta_fin = _guardar_testigo(foto_fin, carpeta, f"tanqueo_fin_{num}.jpg")
                    set_cols.append(f"`testigo tanqueo fin {num}`=%s"); set_vals.append(ruta_fin)
                if testigo_baucher:
                    ruta_b = _guardar_testigo(testigo_baucher, carpeta, f"baucher_{num}.jpg")
                    set_cols.append(f"`testigo baucher {num}`=%s"); set_vals.append(ruta_b)

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

            # proveedor en consumo también
            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)
            cur.execute("UPDATE cardex_glp SET proveedor=%s WHERE id=%s", (proveedor_principal, id_operacion))

            # Solicitud tanqueo por tu fórmula (17-n)
            tanques_bajos = []
            n = dias_operacion if dias_operacion else 0
            for tk in tanques:
                numero = tk.get("numero")
                try:
                    nivel_inicial = float(tk.get("nivel", 0))
                except Exception:
                    nivel_inicial = 0.0

                if nivel_inicial <= 30.0:
                    pct_req = 8 * (17 - n)
                    pct_sol = min(80.0, nivel_inicial + pct_req)

                    tanques_bajos.append({
                        "numero": numero,
                        "nivel_inicial": nivel_inicial,
                        "dias_transcurridos": n,
                        "porcentaje_requerido": round(pct_req, 2),
                        "porcentaje_solicitado": round(pct_sol, 2)
                    })

            if tanques_bajos and proveedor_principal:
                cur.execute(
                    "SELECT email1,email2 FROM proveedores WHERE proveedor=%s",
                    (proveedor_principal,)
                )
                c = cur.fetchone() or {}
                destinatarios = [
                    e for e in [c.get("email1"), c.get("email2")] if e
                ]
                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    # Generar código de pedido de gas y registrarlo en pedidos_gas_glp
                    try:
                        codigo_pedido = _generar_codigo_pedido(
                            cur, empresa, proveedor_principal, fecha
                        )
                    except Exception as e:
                        print("[GLP] No se pudo generar código de pedido (consumo):", e)
                        codigo_pedido = None

                    # Cuerpo del correo en HTML, resaltando el código de pedido
                    cuerpo = (
                        "<p>📩 <b>Solicitud de Tanqueo GLP</b></p>"
                        f"<p><b>Empresa:</b> {empresa}<br>"
                        f"<b>Sede:</b> {ubicacion}<br>"
                        f"<b>Lote:</b> {lote_id}<br>"
                        f"<b>Fecha:</b> {fecha.strftime('%Y-%m-%d')}<br>"
                        f"<b>Día de calefacción:</b> {dias_operacion}</p>"
                    )

                    if codigo_pedido:
                        cuerpo += (
                            "<p><b>Código de validación del pedido:</b> "
                            f"<b><u>{codigo_pedido}</u></b></p>"
                            "<p><i>Nota:</i> Para que la factura sea válida, "
                            "el proveedor debe incluir este código en la factura.</p>"
                        )

                    cuerpo += "<p><b>Recomendaciones de llenado:</b><br>"
                    for t in tanques_bajos:
                        cuerpo += (
                            f"- {t['numero']}: nivel {t['nivel_inicial']}% → "
                            f"llenar hasta {t['porcentaje_solicitado']}%<br>"
                        )
                    cuerpo += "</p>"

                    try:
                        msg = MIMEText(cuerpo, "html", "utf-8")
                        msg["Subject"] = "Solicitud de Tanqueo GLP"
                        msg["From"] = EMAIL_FROM
                        msg["To"] = ", ".join(destinatarios)
                        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                            server.starttls()
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
                        print("✅ Correo GLP (consumo) enviado a:", destinatarios)
                    except Exception:
                        print("⛔ Error al enviar correo GLP (consumo):")
                        traceback.print_exc()

            mysql.connection.commit()

        mensaje = "Consumo registrado correctamente."
        if consumo_kg > 0:
            mensaje += f" Consumo desde la última operación: {round(consumo_kg,2)} kg"
            if kg_pollito > 0:
                mensaje += f" ({round(kg_pollito,6)} kg/pollito)."

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
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except Exception:
        print("⛔ Error en registrar_tanqueo / consumo:")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Error al registrar tanqueo/consumo."})


# ======================
# Registrar consumo (versión simplificada para otras rutas)
# ======================
@csrf.exempt
@bp_glp.route('/registrar_consumo', methods=['POST'])
@login_required_custom
def registrar_consumo():
    """
    Esta ruta es la versión compatible de registrar_consumo usada por el frontend actual.
    Internamente delega en la lógica de registrar_tanqueo/consumo unificado.
    """
    # Para mantener compatibilidad, redirigimos la lógica:
    return registrar_tanqueo()


# ======================
# Finalizar calefacción
# ======================
@csrf.exempt
@bp_glp.route('/finalizar_calefaccion', methods=['POST'])
@login_required_custom
def finalizar_calefaccion():
    """
    Registra el cierre de calefacción:
    - niveles finales por tanque
    - fotos de cierre
    - marca fin de operación (estatus en cardex_glp)
    """
    try:
        data = request.get_json(force=True)
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

    empresa    = session.get('empresa') or ''
    empresa_id = session.get('empresa_id') or session.get('nit')
    operador_id = str(session.get('cedula') or session.get('usuario_id') or '').strip()
    operador_nombre = (
        session.get('usuario_nombre') or
        session.get('nombre') or
        session.get('usuario') or
        ''
    )

    ubicacion  = _normalize_sede(data.get('sede'))
    lote_id    = data.get('lote_id') or data.get('lote') or ''
    tanques    = data.get('tanques') or []
    densidad   = float(data.get('densidad_estimada') or 0.54)
    fecha_str  = data.get('fecha') or ''
    op_id      = data.get('op_id') or ''

    if not empresa or not ubicacion or not lote_id:
        return jsonify({"success": False, "message": "Faltan datos de empresa/sede/lote."}), 400

    try:
        if fecha_str:
            fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
        else:
            fecha = datetime.now().date()
    except Exception:
        fecha = datetime.now().date()

    try:
        with mysql.connection.cursor() as cur:
            # Idempotencia
            cur.execute("""
                SELECT id
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s)
                  AND lote=%s AND operacion='finalizar_calefaccion'
                  AND op_id=%s
                LIMIT 1
            """, (empresa, ubicacion, lote_id, op_id))
            dup = cur.fetchone()
            if dup:
                return jsonify({
                    "success": True,
                    "message": "Operación ya registrada (idempotente).",
                    "resumen": None
                })

            columnas = [
                "empresa", "ubicacion", "lote", "fecha",
                "operacion", "operador_id", "operador_nombre",
                "saldo_estimado_kg", "saldo_estimado_galones",
                "op_id"
            ]
            valores = [
                empresa, ubicacion, lote_id, fecha,
                "finalizar_calefaccion", operador_id, operador_nombre,
                0.0, 0.0,
                op_id
            ]

            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

            set_cols, set_vals = [], []
            saldo_estimado_kg = 0.0

            for tk in tanques:
                numero = tk.get("numero")
                try:
                    nivel  = float(tk.get("nivel",0))
                except Exception:
                    nivel = 0.0
                try:
                    capacidad = float(tk.get("capacidad",0))
                except Exception:
                    capacidad = 0.0
                testigo = tk.get("testigo")

                if numero and nivel is not None:
                    set_cols.append(f"`nivel {numero}`=%s"); set_vals.append(nivel)
                if numero and capacidad is not None:
                    set_cols.append(f"`capacidad {numero}`=%s"); set_vals.append(capacidad)
                if numero and testigo:
                    ruta_web = _guardar_testigo(testigo, carpeta, f"final_{numero}.jpg")
                    set_cols.append(f"`testigo final {numero}`=%s"); set_vals.append(ruta_web)

                saldo_estimado_kg += densidad * capacidad * (nivel/100.0)

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

            dias_operacion = _actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            mysql.connection.commit()

        mensaje = "Calefacción finalizada correctamente."
        resumen = {
            "operacion": "finalizar_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "tanques": _resumen_tanques(tanques),
            "saldo_estimado_kg": round(saldo_estimado_kg,2),
            "saldo_estimado_galones": round(saldo_estimado_gal,2),
            "dias_operacion": dias_operacion,
            "op_id": op_id
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})

    except Exception:
        print("⛔ Error en finalizar_calefaccion:")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Error al finalizar calefacción."})

