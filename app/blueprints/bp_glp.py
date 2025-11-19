# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, session
from flask import current_app as app
from datetime import datetime
from app.utils import login_required_custom
import os, base64, smtplib, traceback
from email.mime.text import MIMEText
from app import mysql
from app import csrf

bp_glp = Blueprint('bp_glp', __name__, url_prefix='/glp')

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "587"))
EMAIL_USER = os.environ.get("EMAIL_USER", "noreply@example.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS", "password")

# ==========
# Utilidades
# ==========
import re as _re

def _normalize_sede(s):
    """Normaliza 'sede' aceptando formatos 'SEDE | EMPRESA' o espacios raros."""
    s = (s or "").strip()
    if "|" in s:
        s = s.split("|", 1)[0]
    s = _re.sub(r"\s+", " ", s.replace("\u00A0", " ")).strip()
    return s

def _guardar_testigo(base64_data, carpeta, nombre_archivo):
    if "," in base64_data:
        base64_data = base64_data.split(",", 1)[1]
    ruta_archivo = os.path.join(carpeta, nombre_archivo)
    ruta_web = os.path.relpath(ruta_archivo, "static").replace(os.path.sep, "/")
    with open(ruta_archivo, "wb") as f:
        f.write(base64.b64decode(base64_data))
    return ruta_web

def _resumen_tanques(tanques):
    """
    Construye un resumen simple de tanques para devolver al frontend.
    tanques: lista de diccionarios con keys: numero, nivel, capacidad.
    """
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
        salida.append({
            "numero": num,
            "nivel": niv,
            "capacidad": cap
        })
    return salida

def _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha_actual=None):
    """
    Calcula y actualiza los días de operación de un lote en cardex_glp.
    - Cuenta desde la primera fecha registrada del lote (MIN(fecha))
    - Hasta fecha_actual (default: hoy)
    - Se cuenta el día de inicio como día 1 (por eso se suma +1)
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
            dias = (fecha_actual - fecha_ini).days + 1  # Día de inicio = 1
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

def _evaluar_tanques_y_enviar_solicitud_tanqueo(cur, empresa, ubicacion, lote_id, fecha, dias_operacion, tanques):
    """
    Revisa tanques con nivel <= 30% y envía correo al proveedor
    con recomendación de % de tanqueo en función de los días restantes.

    - dias_operacion: día actual de calefacción (1..15)
    - tanques: lista de dicts con al menos {"numero", "nivel"} (nivel en % actual)
    """
    try:
        if dias_operacion is None:
            return

        # Días restantes del ciclo de 15 días
        dias_restantes = max(0, 15 - int(dias_operacion))
        if dias_restantes <= 0:
            # Ya no quedan días de calefacción según el ciclo
            return

        CONSUMO_DIARIO_PORC = 8.0  # 8% del tanque por día (supuesto de diseño)

        tanques_bajos = []
        for tk in tanques:
            num = (tk.get("numero") or "").strip()
            if not num:
                continue
            try:
                nivel = float(tk.get("nivel") or 0.0)
            except Exception:
                nivel = 0.0

            # Solo analizamos tanques por debajo o igual al 30%
            if nivel <= 30.0:
                # % que se estima que se consumirá en los días restantes
                requerido_pct = dias_restantes * CONSUMO_DIARIO_PORC
                # % de tanqueo recomendado = lo que se va a consumir - lo que tiene hoy
                recomendado_pct = requerido_pct - nivel

                if recomendado_pct < 0:
                    recomendado_pct = 0.0
                # Nivel objetivo aproximado después de tanquear
                nivel_objetivo = nivel + recomendado_pct
                if nivel_objetivo > 100.0:
                    nivel_objetivo = 100.0

                tanques_bajos.append({
                    "numero": num,
                    "nivel_actual": round(nivel, 2),
                    "dias_restantes": dias_restantes,
                    "recomendado_tanqueo": round(recomendado_pct, 2),
                    "nivel_objetivo": round(nivel_objetivo, 2),
                    "consumo_estimado_pct": round(requerido_pct, 2),
                })

        if not tanques_bajos:
            return

        # Tomar proveedor principal desde el primer tanque bajo
        proveedor_principal = None
        primer_tk = tanques_bajos[0]
        cur.execute("""
            SELECT proveedor
              FROM tanques_sedes
             WHERE empresa = %s
               AND TRIM(ubicacion) = TRIM(%s)
               AND nombre_tanque = %s
        """, (empresa, ubicacion, primer_tk["numero"]))
        row = cur.fetchone()
        if row and row.get("proveedor"):
            proveedor_principal = row["proveedor"]

        destinatarios = []
        if proveedor_principal:
            cur.execute("SELECT email1, email2 FROM proveedores WHERE proveedor=%s", (proveedor_principal,))
            c = cur.fetchone() or {}
            destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]

        if not destinatarios and EMAIL_USER:
            destinatarios = [EMAIL_USER]

        if not destinatarios:
            return

        # Cuerpo del correo
        cuerpo  = "📩 Solicitud de tanqueo GLP según días restantes de calefacción\n\n"
        cuerpo += f"Empresa: {empresa}\n"
        cuerpo += f"Sede: {ubicacion}\n"
        cuerpo += f"Lote: {lote_id}\n"
        cuerpo += f"Fecha: {fecha.strftime('%Y-%m-%d')}\n"
        cuerpo += f"Día actual de calefacción: {dias_operacion}\n"
        cuerpo += f"Días restantes considerados: {dias_restantes}\n"
        cuerpo += f"Consumo estimado: {CONSUMO_DIARIO_PORC}% por día.\n\n"
        cuerpo += "Recomendación por tanque:\n"

        for t in tanques_bajos:
            cuerpo += (
                f"- {t['numero']}: nivel actual {t['nivel_actual']} %, "
                f"consumo estimado en {t['dias_restantes']} días ≈ {t['consumo_estimado_pct']} %, "
                f"recomendado tanquear {t['recomendado_tanqueo']} %, "
                f"nivel objetivo aproximado {t['nivel_objetivo']} %.\n"
            )

        msg = MIMEText(cuerpo)
        msg["Subject"] = "Solicitud de tanqueo GLP"
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(destinatarios)

        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, destinatarios, msg.as_string())

    except Exception:
        # No dejamos caer toda la operación si falla el correo
        traceback.print_exc()
        return




# =======================================
# Helper UNIFICADO para consumo y kg/pollito
# =======================================
def _calcular_consumo_lote(cur, empresa, ubicacion, lote_id, id_operacion_actual, tanques):
    """
    Calcula el consumo de GLP (en kg) entre la operación anterior
    y la operación actual, para todos los tanques del lote, y
    actualiza la columna `kg/pollito` de la fila actual.

    - Se toma la fila inmediatamente anterior (id < id_operacion_actual).
    - Se leen los niveles anteriores de cada tanque (`nivel tk-n`).
    - Se comparan con los niveles actuales recibidos en 'tanques'.
    - Para cada tanque:
        consumo_tk_kg = densidad * capacidad_gal * max(nivel_prev - nivel_act, 0) / 100
    - La densidad usada es la última registrada:
        densidad_suministrada (si > 0) o densidad_estimada o 2.0.
    - Se obtiene pollitos de la operación inicio_calefaccion.
    - Se guarda `kg/pollito` en la fila de id_operacion_actual.
    """
    if not tanques:
        return 0.0, 0.0, None

    # Fila anterior del lote
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
        # No hay operación previa → no hay consumo para comparar
        return 0.0, 0.0, None

    # Densidad: se usa primero densidad_suministrada si está, si no densidad_estimada, si no 2.0
    densidad = prev.get("densidad_suministrada") or prev.get("densidad_estimada") or 2.0
    try:
        densidad = float(densidad)
        if densidad <= 0:
            densidad = 2.0
    except Exception:
        densidad = 2.0

    consumo_total_kg = 0.0

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
            continue
        try:
            nivel_prev = float(nivel_prev)
        except Exception:
            nivel_prev = 0.0

        # Solo consideramos consumo cuando baja el nivel
        delta = nivel_prev - nivel_act
        if delta <= 0:
            continue

        consumo_tk_kg = densidad * cap * (delta / 100.0)
        consumo_total_kg += consumo_tk_kg

    # Buscar pollitos del inicio de calefacción
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

    # Actualizar la fila actual con kg/pollito y, opcionalmente, neto_gastado
    cur.execute("""
        UPDATE cardex_glp
           SET `kg_pollito` = %s,
               neto_gastado = COALESCE(neto_gastado, 0) + %s
         WHERE id = %s
    """, (kg_pollito, consumo_total_kg, id_operacion_actual))

    return consumo_total_kg, kg_pollito, pollitos

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
    sede_in = data.get('sede') or ''
    sede = _normalize_sede(sede_in)

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
        tanques = [{"numero": r.get("numero") or "", "capacidad": float(r.get("capacidad") or 0), "etiqueta": r.get("numero") or ""} for r in rows]
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
        print(f"JSON procesado con {len(data.get('tanques', []))} tanques.")
    except Exception as e:
        print("Error al procesar JSON:", e)
        return jsonify({"success": False, "message": "Error leyendo JSON"}), 400

    if not data:
        return jsonify({"success": False, "message": "No se recibió JSON válido"}), 400

    usuario    = session.get('nombre') or ''
    empresa    = session.get('empresa') or ''
    id_empresa = session.get('empresa_id') or 0

    ubicacion  = _normalize_sede(data.get('sede'))
    pollitos   = data.get('pollitos')
    criadoras  = data.get('criadoras')
    tanques    = data.get('tanques', [])

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
            row = cur.fetchone()
            if (row or {}).get('activo', 0) > 0:
                cur.execute("""
                    SELECT lote, fecha
                    FROM cardex_glp
                    WHERE empresa = %s
                      AND TRIM(ubicacion) = TRIM(%s)
                      AND estatus_lote = 'ACTIVO'
                    ORDER BY fecha DESC LIMIT 1
                """, (empresa, ubicacion))
                conf = cur.fetchone() or {}
                msg = f"Ya existe un lote activo (lote {conf.get('lote')} del {conf.get('fecha')})."
                return jsonify({"success": False, "message": msg})

            fecha = datetime.now().date()
            fecha_str = fecha.strftime("%Y%m%d")
            lote_id = f"{fecha_str}_{ubicacion.replace(' ', '')}"

            columnas_insert = [
                "fecha","empresa","id_empresa","ubicacion","lote","estatus_lote",
                "operacion","tipo","clase","criadoras","pollitos","registro","dias_operacion"
            ]
            valores_insert = [
                fecha,empresa,id_empresa,ubicacion,lote_id,'ACTIVO',
                'inicio_calefaccion','manual','saldo inicial',
                criadoras if empresa=="Pollos GAR SAS" else None,
                pollitos if empresa=="Pollos GAR SAS" else None,
                usuario,0
            ]
            placeholders = ", ".join(["%s"]*len(columnas_insert))
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas_insert)}) "
                f"VALUES ({placeholders})",
                valores_insert
            )

            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

            columnas_extra=[]; valores_extra=[]
            densidad=2.0; saldo_estimado_kg=0.0

            for tk in tanques:
                numero = tk.get("numero")
                nivel  = float(tk.get("nivel",0))
                capacidad = float(tk.get("capacidad",0))
                testigo = tk.get("testigo")

                if numero and nivel is not None:
                    columnas_extra.append(f"`nivel {numero}`"); valores_extra.append(nivel)
                if numero and capacidad is not None:
                    columnas_extra.append(f"`capacidad {numero}`"); valores_extra.append(capacidad)
                if numero and testigo:
                    ruta_web = _guardar_testigo(testigo, carpeta, f"{numero}.jpg")
                    columnas_extra.append(f"`testigo nivel {numero}`"); valores_extra.append(ruta_web)

                saldo_estimado_kg += densidad * capacidad * (nivel/100.0)

            if columnas_extra:
                set_clause = ", ".join([f"{c}=%s" for c in columnas_extra])
                cur.execute(
                    f"UPDATE cardex_glp SET {set_clause} WHERE lote=%s",
                    valores_extra+[lote_id]
                )

            # Registrar proveedor del primer tanque
            proveedor_principal = None
            if tanques:
                cur.execute("""
                    SELECT proveedor 
                    FROM tanques_sedes
                    WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND nombre_tanque=%s
                """, (empresa, ubicacion, tanques[0].get("numero")))
                p = cur.fetchone()
                proveedor_principal = p.get("proveedor") if p else None

            saldo_estimado_galones = saldo_estimado_kg/densidad if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s,
                       proveedor=%s
                 WHERE lote=%s
            """, (round(saldo_estimado_kg,2),round(saldo_estimado_galones,2),proveedor_principal,lote_id))

            # ---- DÍA DE OPERACIÓN (día 1) ----
            dias_operacion = 1  # inicio = día 1
            cur.execute("""
                UPDATE cardex_glp
                   SET dias_operacion=%s
                 WHERE lote=%s AND operacion='inicio_calefaccion'
            """, (dias_operacion, lote_id))

            # ==========================================================
            # 🔥 CALCULAR % SOLICITADO CON TU NUEVA FÓRMULA
            # ==========================================================
            tanques_bajos = []
            for tk in tanques:
                numero = tk.get("numero")
                nivel_inicial = float(tk.get("nivel",0))

                if nivel_inicial <= 30:   # Solo tanques críticos

                    # ----------------------------------------------
                    # Fórmulas oficiales:
                    # %requerido = 8*(17-n)
                    # %solicitado_bruto = nivel_inicial + %requerido
                    # %solicitado = min(80, %solicitado_bruto)
                    # ----------------------------------------------
                    n = dias_operacion
                    porcentaje_requerido = 8 * (17 - n)
                    porcentaje_solicitado_bruto = nivel_inicial + porcentaje_requerido
                    porcentaje_solicitado = min(80, porcentaje_solicitado_bruto)

                    tanques_bajos.append({
                        "numero": numero,
                        "nivel_inicial": nivel_inicial,
                        "dias_transcurridos": n,
                        "porcentaje_requerido": round(porcentaje_requerido,2),
                        "porcentaje_solicitado": round(porcentaje_solicitado,2)
                    })

            # ==========================================================
            # 🔥 ENVIAR EMAIL SI HAY TANQUES BAJOS
            # ==========================================================
            if tanques_bajos and proveedor_principal:
                cur.execute("SELECT email1,email2 FROM proveedores WHERE proveedor=%s",
                            (proveedor_principal,))
                c = cur.fetchone() or {}
                destinatarios = [e for e in [c.get("email1"),c.get("email2")] if e]

                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    cuerpo  = "📩 Solicitud de Tanqueo GLP\n\n"
                    cuerpo += f"Empresa: {empresa}\n"
                    cuerpo += f"Sede: {ubicacion}\n"
                    cuerpo += f"Lote: {lote_id}\n"
                    cuerpo += f"Fecha: {fecha.strftime('%Y-%m-%d')}\n"
                    cuerpo += f"Día de calefacción: {dias_operacion}\n\n"
                    cuerpo += "Recomendaciones:\n"

                    for t in tanques_bajos:
                        cuerpo += (
                            f"- {t['numero']}: nivel {t['nivel_inicial']}% → "
                            f"Estimado proveedor: por favor llenar hasta {t['porcentaje_solicitado']}% "
                        )

                    msg = MIMEText(cuerpo)
                    msg["Subject"] = "Solicitud de Tanqueo GLP"
                    msg["From"] = EMAIL_USER
                    msg["To"] = ", ".join(destinatarios)

                    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                        server.starttls()
                        server.login(EMAIL_USER, EMAIL_PASS)
                        server.sendmail(EMAIL_USER, destinatarios, msg.as_string())

            mysql.connection.commit()

        mensaje = f"Lote {lote_id} registrado correctamente."
        if tanques_bajos:
            mensaje += " ⚠️ Se solicitó tanqueo vía email."

        resumen = {
            "operacion": "inicio_calefaccion",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            "pollitos": pollitos if empresa == "Pollos GAR SAS" else None,
            "criadoras": criadoras if empresa == "Pollos GAR SAS" else None,
            "tanques": _resumen_tanques(tanques),
            "dias_operacion": dias_operacion
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
    Registrar tanqueo de GLP.

    El frontend debe enviar un JSON de la forma:
    {
        "sede": "Granja_X",
        "tanques": [
            {
                "numero": "tk-1",
                "capacidad": 500,
                "nivel_inicial": 20,
                "foto_nivel_inicial": "data:image/jpeg;base64,...",
                "nivel_final": 80,
                "foto_nivel_final": "data:image/jpeg;base64,...",
                "densidad_suministrada": 1.95,
                "kg_suministrados": 250.0,
                "foto_baucher": "data:image/jpeg;base64,..."
            },
            ...
        ]
    }
    """
    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"success": False, "message": "JSON inválido"}), 400

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
            # Obtener lote ACTIVO
            cur.execute("""
                SELECT lote, MIN(fecha) AS fecha_ini
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC, id DESC
                LIMIT 1
            """, (empresa, ubicacion))
            row = cur.fetchone()
            if not row or not row.get("lote"):
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})
            lote_id = row["lote"]

            # Registrar fila de tanqueo (ingreso)
            fecha = datetime.now().date()
            columnas = [
                "fecha","empresa","id_empresa","ubicacion",
                "lote","estatus_lote","operacion","tipo","clase","registro"
            ]
            valores  = [
                fecha,empresa,id_empresa,ubicacion,
                lote_id,'ACTIVO',"tanqueo","manual","ingreso",usuario
            ]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid  # ID de esta fila

            # Carpeta para guardar evidencias
            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

            # Actualización de columnas por tanque (niveles, capacidades, testigo nivel, baucher)
            set_cols = []
            set_vals = []
            densidad_estimada = 2.0
            saldo_estimado_kg = 0.0

            # Variables para cálculo de masa esperada / facturada
            masas_esperadas = []
            masas_facturadas = []
            desviaciones_porcentuales = []
            densidades_registradas = []

            tanques_resumen = []

            for tk in tanques:
                num = (tk.get("numero") or "").strip()
                if not num:
                    continue

                # Capacidad
                try:
                    cap = float(tk.get("capacidad") or 0.0)
                except Exception:
                    cap = 0.0

                # Niveles antes y después
                try:
                    nivel_ini = float(tk.get("nivel_inicial") or 0.0)
                except Exception:
                    nivel_ini = 0.0
                try:
                    nivel_fin = float(tk.get("nivel_final") or 0.0)
                except Exception:
                    nivel_fin = 0.0

                # Evidencias fotográficas
                foto_ini  = tk.get("foto_nivel_inicial")
                foto_fin  = tk.get("foto_nivel_final")
                foto_bau  = tk.get("foto_baucher")

                # Capacidad
                if cap:
                    set_cols.append(f"`capacidad {num}`=%s")
                    set_vals.append(cap)

                # Guardamos el nivel FINAL como nivel actual del tanque
                set_cols.append(f"`nivel {num}`=%s")
                set_vals.append(nivel_fin)

                # Guardamos como testigo la foto del nivel FINAL
                if foto_fin:
                    ruta_fin = _guardar_testigo(
                        foto_fin,
                        carpeta,
                        f"{num}_nivel_final_tanqueo.jpg"
                    )
                    set_cols.append(f"`testigo nivel {num}`=%s")
                    set_vals.append(ruta_fin)

                # 👉 Guardar baucher específico del tanque en su columna
                if foto_bau:
                    ruta_baucher = _guardar_testigo(
                        foto_bau,
                        carpeta,
                        f"{num}_baucher_tanqueo.jpg"
                    )
                    # ejemplo: testigo_baucher_tk_1
                    col_baucher = f"testigo_baucher_{num.replace('-', '_')}"
                    set_cols.append(f"`{col_baucher}`=%s")
                    set_vals.append(ruta_baucher)

                # Cálculo de saldo estimado (usamos densidad_estimada y nivel FINAL)
                saldo_estimado_kg += densidad_estimada * cap * (nivel_fin / 100.0)

                # Cálculos de masa esperada vs facturada para este tanque
                try:
                    densidad_sum = float(tk.get("densidad_suministrada") or 0.0)
                except Exception:
                    densidad_sum = 0.0
                if densidad_sum > 0:
                    densidades_registradas.append(densidad_sum)

                try:
                    kg_sumin = float(tk.get("kg_suministrados") or 0.0)
                except Exception:
                    kg_sumin = 0.0

                # masa esperada por diferencia de nivel (después - antes)
                delta_nivel = nivel_fin - nivel_ini
                masa_esp_tk = 0.0
                if densidad_sum > 0 and cap > 0 and delta_nivel > 0:
                    masa_esp_tk = densidad_sum * cap * (delta_nivel / 100.0)

                if masa_esp_tk > 0:
                    masas_esperadas.append(masa_esp_tk)
                    masas_facturadas.append(kg_sumin)
                    # desviación porcentual relativa a la esperada
                    if kg_sumin > 0:
                        desvio = ((kg_sumin - masa_esp_tk) / masa_esp_tk) * 100.0
                        desviaciones_porcentuales.append(desvio)

                tanques_resumen.append({
                    "numero": num,
                    "nivel_inicial": nivel_ini,
                    "nivel_final": nivel_fin,
                    "capacidad": cap,
                    "densidad_suministrada": densidad_sum,
                    "kg_suministrados": kg_sumin,
                    "masa_esperada_kg": masa_esp_tk
                })

            # Actualizar columnas de tanques si corresponde
            if set_cols:
                cur.execute(
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s",
                    set_vals + [id_operacion]
                )

            # saldo estimado en galones (con densidad_estimada)
            saldo_estimado_gal = saldo_estimado_kg / densidad_estimada if densidad_estimada else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            # ---- Cálculo unificado de consumo y kg/pollito ----
            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion, 
                # Para consumo usamos el nivel FINAL de cada tanque
                [{"numero": t.get("numero"), 
                  "capacidad": t.get("capacidad"), 
                  "nivel": t.get("nivel_inicial")} for t in tanques]
            )
            # ---- Guardar kg/pollito para esta operación de tanqueo ---- 
            cur.execute("""
               UPDATE cardex_glp
                  SET kg_pollito = %s
                WHERE id = %s
            """, (
                round(kg_pollito, 6) if kg_pollito and kg_pollito > 0 else 0.0,
                id_operacion
            ))

            # ---- DÍAS DE OPERACIÓN UNIFICADOS ----
            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            # ---- Cálculo de masa esperada / facturada y % diferencia promedio ----
            masa_esperada_total = sum(masas_esperadas) if masas_esperadas else 0.0
            masa_facturada_total = sum(masas_facturadas) if masas_facturadas else 0.0
            if desviaciones_porcentuales:
                porcentaje_dif_prom = sum(desviaciones_porcentuales) / len(desviaciones_porcentuales)
            else:
                porcentaje_dif_prom = 0.0

            # Densidad suministrada promedio (si hubo)
            densidad_suministrada_prom = 0.0
            if densidades_registradas:
                densidad_suministrada_prom = sum(densidades_registradas) / len(densidades_registradas)

            # Actualizar fila con datos de control de tanqueo
            cur.execute("""
                UPDATE cardex_glp
                   SET densidad_suministrada=%s,
                       masa_esperada_kg=%s,
                       masa_kg_facturada=%s,
                       porcentaje_diferencia=%s
                 WHERE id=%s
            """, (
                round(densidad_suministrada_prom,3) if densidad_suministrada_prom else 0.0,
                round(masa_esperada_total,2),
                round(masa_facturada_total,2),
                round(porcentaje_dif_prom,2),
                id_operacion
            ))

            # ---- Envío de email si la desviación excede el 8% ----
            alerta_enviada = False
            if masa_esperada_total > 0 and abs(porcentaje_dif_prom) > 8.0:
                # Tomar proveedor principal desde el primer tanque
                proveedor_principal = None
                primer_tanque = tanques[0] if tanques else None
                if primer_tanque and primer_tanque.get("numero"):
                    cur.execute("""
                        SELECT proveedor FROM tanques_sedes
                        WHERE empresa = %s AND TRIM(ubicacion) = TRIM(%s) AND nombre_tanque = %s
                    """, (empresa, ubicacion, primer_tanque.get("numero")))
                    p = cur.fetchone()
                    proveedor_principal = p["proveedor"] if p and p.get("proveedor") else None

                destinatarios = []
                if proveedor_principal:
                    cur.execute("SELECT email1,email2 FROM proveedores WHERE proveedor=%s", (proveedor_principal,))
                    c = cur.fetchone() or {}
                    destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]

                # Si no se encuentra proveedor o emails, se puede usar el correo por defecto
                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    cuerpo  = "⚠️ Desviación en tanqueo de GLP\n\n"
                    cuerpo += f"Empresa: {empresa}\n"
                    cuerpo += f"Sede: {ubicacion}\n"
                    cuerpo += f"Lote: {lote_id}\n"
                    cuerpo += f"Fecha: {fecha.strftime('%Y-%m-%d')}\n\n"
                    cuerpo += f"Masa esperada total: {round(masa_esperada_total,2)} kg\n"
                    cuerpo += f"Masa facturada total: {round(masa_facturada_total,2)} kg\n"
                    cuerpo += f"Desviación promedio: {round(porcentaje_dif_prom,2)} %\n\n"
                    cuerpo += "Detalle por tanque:\n"
                    for t in tanques_resumen:
                        cuerpo += (
                            f"- {t['numero']}: "
                            f"niv.ini {t['nivel_inicial']}% → niv.fin {t['nivel_final']}%, "
                            f"cap {t['capacidad']} gal, "
                            f"dens {t['densidad_suministrada']} kg/gal, "
                            f"esp {round(t['masa_esperada_kg'],2)} kg, "
                            f"fact {t['kg_suministrados']} kg\n"
                        )

                    msg = MIMEText(cuerpo)
                    msg["Subject"] = "⚠️ Desviación en tanqueo GLP"
                    msg["From"] = EMAIL_USER
                    msg["To"] = ", ".join(destinatarios)
                    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                        server.starttls()
                        server.login(EMAIL_USER, EMAIL_PASS)
                        server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
                    alerta_enviada = True

            mysql.connection.commit()

        # Mensaje para frontend
        mensaje = "Tanqueo registrado correctamente."
        if consumo_kg > 0:
            mensaje += f" Consumo desde la última operación: {round(consumo_kg,2)} kg"
            if kg_pollito > 0:
                mensaje += f" ({round(kg_pollito,6)} kg_pollito)."

        # Añadimos info de desviación
        if masa_esperada_total > 0:
            mensaje += (
                f" Control de tanqueo: esperado {round(masa_esperada_total,2)} kg, "
                f"facturado {round(masa_facturada_total,2)} kg "
                f"(desviación promedio {round(porcentaje_dif_prom,2)}%)."
            )
            if masa_esperada_total > 0 and abs(porcentaje_dif_prom) > 8.0:
                mensaje += " ⚠️ Desviación superior al 8%."
                if alerta_enviada:
                    mensaje += " Se envió alerta por email."

        resumen = {
            "operacion": "tanqueo",
            "sede": ubicacion,
            "lote": lote_id,
            "fecha": fecha.strftime("%Y-%m-%d"),
            # Para el resumen usamos una versión compacta de los tanques
            "tanques": _resumen_tanques([
                {"numero": t.get("numero"), "nivel": t.get("nivel_final"), "capacidad": t.get("capacidad")}
                for t in tanques
            ]),
            "saldo_estimado_kg": round(saldo_estimado_kg, 2),
            "saldo_estimado_galones": round(saldo_estimado_gal, 2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg, 2),
            "kg_pollito": round(kg_pollito, 6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos,
            "masa_esperada_kg": round(masa_esperada_total, 2),
            "masa_kg_facturada": round(masa_facturada_total, 2),
            "porcentaje_diferencia": round(porcentaje_dif_prom, 2)
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})
    except Exception:
        print("⛔ Error en registrar_tanqueo:")
        traceback.print_exc()
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

    empresa    = session.get('empresa') or ''
    usuario    = session.get('nombre') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))
    tanques    = data.get('tanques', [])

    if not empresa or not ubicacion:
        return jsonify({"success": False, "message": "Faltan datos de empresa o sede."}), 400

    if not tanques:
        return jsonify({"success": False, "message": "No se recibieron tanques para registrar consumo."}), 400

    try:
        with mysql.connection.cursor() as cur:
            # Obtener lote ACTIVO
            cur.execute("""
                SELECT lote
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC LIMIT 1
            """, (empresa, ubicacion))
            row = cur.fetchone()
            if not row or not row.get("lote"):
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})
            lote_id = row["lote"]

            # Registrar fila de consumo (egreso)
            fecha = datetime.now().date()
            columnas = [
                "fecha","empresa","id_empresa","ubicacion",
                "lote","estatus_lote","operacion","tipo","clase","registro"
            ]
            valores  = [
                fecha,empresa,id_empresa,ubicacion,
                lote_id,'ACTIVO',"consumo","manual","egreso",usuario
            ]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) "
                f"VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            # Carpeta para testigos
            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

            set_cols = []
            set_vals = []
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
                    set_cols.append(f"`nivel {num}`=%s")
                    set_vals.append(niv)
                if num and cap is not None:
                    set_cols.append(f"`capacidad {num}`=%s")
                    set_vals.append(cap)
                if num and tst:
                    ruta = _guardar_testigo(tst, carpeta, f"{num}_consumo.jpg")
                    set_cols.append(f"`testigo nivel {num}`=%s")
                    set_vals.append(ruta)

                saldo_estimado_kg += densidad * cap * (niv / 100.0)

            if set_cols:
                cur.execute(
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s",
                    set_vals + [id_operacion]
                )

            saldo_estimado_gal = saldo_estimado_kg / densidad if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            # ---- Cálculo unificado de consumo y kg/pollito ----
            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion, tanques
            )

            # ---- Guardar kg/pollito para esta operación de consumo ----
            cur.execute("""
                UPDATE cardex_glp
                   SET kg_pollito = %s
                 WHERE id = %s
            """, (
                round(kg_pollito, 6) if kg_pollito and kg_pollito > 0 else 0.0,
                id_operacion
            ))

            # ---- DÍAS DE OPERACIÓN UNIFICADOS ----
            dias_operacion = _calcular_actualizar_dias_operacion(
                cur, empresa, ubicacion, lote_id, fecha
            )

            # ==========================================================
            # 🔥 CÁLCULO DEL % SOLICITADO PARA TANQUEO (SEGÚN TU FÓRMULA)
            # ==========================================================
            tanques_bajos = []
            n = dias_operacion if dias_operacion else 0  # n = días transcurridos

            for tk in tanques:
                numero = tk.get("numero")
                try:
                    nivel_inicial = float(tk.get("nivel", 0))
                except Exception:
                    nivel_inicial = 0.0

                # Solo consideramos tanques en nivel crítico
                if nivel_inicial <= 30.0:
                    # %requerido = 8 * (17 - n)
                    porcentaje_requerido = 8 * (17 - n)
                    # %solicitado_bruto = %inicial + %requerido
                    porcentaje_solicitado_bruto = nivel_inicial + porcentaje_requerido
                    # %solicitado = min(80, %solicitado_bruto)
                    porcentaje_solicitado = min(80.0, porcentaje_solicitado_bruto)

                    tanques_bajos.append({
                        "numero": numero,
                        "nivel_inicial": nivel_inicial,
                        "dias_transcurridos": n,
                        "porcentaje_requerido": round(porcentaje_requerido, 2),
                        "porcentaje_solicitado": round(porcentaje_solicitado, 2)
                    })

            # Determinar proveedor principal a partir del primer tanque
            proveedor_principal = None
            if tanques:
                cur.execute("""
                    SELECT proveedor 
                    FROM tanques_sedes
                    WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND nombre_tanque=%s
                """, (empresa, ubicacion, tanques[0].get("numero")))
                p = cur.fetchone()
                proveedor_principal = p.get("proveedor") if p else None

            # Enviar correo si hay tanques con nivel crítico
            if tanques_bajos and proveedor_principal:
                cur.execute("SELECT email1,email2 FROM proveedores WHERE proveedor=%s",
                            (proveedor_principal,))
                c = cur.fetchone() or {}
                destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]

                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    cuerpo  = "📩 Solicitud de Tanqueo GLP\n\n"
                    cuerpo += f"Empresa: {empresa}\n"
                    cuerpo += f"Sede: {ubicacion}\n"
                    cuerpo += f"Lote: {lote_id}\n"
                    cuerpo += f"Fecha: {fecha.strftime('%Y-%m-%d')}\n"
                    cuerpo += f"Día de calefacción: {dias_operacion}\n\n"
                    cuerpo += "Recomendaciones:\n"

                    for t in tanques_bajos:
                        cuerpo += (
                            f"- {t['numero']}: nivel {t['nivel_inicial']}% → "
                            f"Estimado proveedor: por favor llenar hasta {t['porcentaje_solicitado']}% "
                        )

                    msg = MIMEText(cuerpo)
                    msg["Subject"] = "Solicitud de Tanqueo GLP"
                    msg["From"] = EMAIL_USER
                    msg["To"] = ", ".join(destinatarios)

                    with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                        server.starttls()
                        server.login(EMAIL_USER, EMAIL_PASS)
                        server.sendmail(EMAIL_USER, destinatarios, msg.as_string())

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
            "saldo_estimado_kg": round(saldo_estimado_kg, 2),
            "saldo_estimado_galones": round(saldo_estimado_gal, 2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg, 2),
            "kg_pollito": round(kg_pollito, 6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})
    except Exception:
        print("⛔ Error en registrar_consumo:")
        traceback.print_exc()
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

    empresa    = session.get('empresa') or ''
    usuario    = session.get('nombre') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))
    tanques    = data.get('tanques', [])

    try:
        with mysql.connection.cursor() as cur:
            cur.execute("""
                SELECT lote, MIN(fecha) AS fecha_ini
                FROM cardex_glp
                WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND estatus_lote='ACTIVO'
                ORDER BY fecha DESC LIMIT 1
            """, (empresa, ubicacion))
            row = cur.fetchone()
            if not row or not row.get("lote"):
                return jsonify({"success": False, "message": "No hay lote activo en esta sede."})
            lote_id=row["lote"]
            fecha_ini = row.get("fecha_ini")

            # Registrar línea de cierre (saldo final)
            fecha = datetime.now().date()
            columnas = ["fecha","empresa","id_empresa","ubicacion","lote","estatus_lote","operacion","tipo","clase","registro"]
            valores  = [fecha,empresa,id_empresa,ubicacion,lote_id,'ACTIVO',"finalizar_calefaccion","manual","saldo final",usuario]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas)}) VALUES ({', '.join(['%s']*len(columnas))})",
                valores
            )
            id_operacion = cur.lastrowid

            carpeta=os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

            set_cols=[]; set_vals=[]
            densidad=2.0; saldo_estimado_kg=0.0

            for tk in tanques:
                num=tk.get("numero"); niv=float(tk.get("nivel",0)); cap=float(tk.get("capacidad",0)); tst=tk.get("testigo")
                if num and niv is not None:
                    set_cols.append(f"`nivel {num}`=%s"); set_vals.append(niv)
                if num and cap is not None:
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)
                if num and tst:
                    ruta=_guardar_testigo(tst, carpeta, f"{num}_final.jpg")
                    set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta)
                saldo_estimado_kg += densidad*cap*(niv/100.0)

            if set_cols:
                cur.execute(f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE id=%s", set_vals+[id_operacion])

            saldo_estimado_gal = saldo_estimado_kg/densidad if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s
                 WHERE id=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), id_operacion))

            # Cálculo unificado de consumo y kg/pollito
            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion, tanques
            )
                        # ---- Guardar kg/pollito para esta operación de cierre ----
            cur.execute("""
                UPDATE cardex_glp
                   SET kg_pollito = %s
                 WHERE id = %s
            """, (
                round(kg_pollito, 6) if kg_pollito and kg_pollito > 0 else 0.0,
                id_operacion
            ))
            # Cerrar lote y actualizar días (unificada, contando desde inicio)
            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            cur.execute("""
                UPDATE cardex_glp
                   SET estatus_lote='INACTIVO'
                 WHERE lote=%s
            """, (lote_id,))

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
            "saldo_estimado_kg": round(saldo_estimado_kg, 2),
            "saldo_estimado_galones": round(saldo_estimado_gal, 2),
            "dias_operacion": dias_operacion,
            "kg_consumidos": round(consumo_kg, 2),
            "kg_pollito": round(kg_pollito, 6) if kg_pollito > 0 else 0.0,
            "pollitos": pollitos
        }

        return jsonify({"success": True, "message": mensaje, "resumen": resumen})
    except Exception:
        print("⛔ Error en finalizar_calefaccion_batch:")
        traceback.print_exc()
        return jsonify({"success": False, "message": "Error al finalizar calefacción."})
