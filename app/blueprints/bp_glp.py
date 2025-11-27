# -*- coding: utf-8 -*-
from flask import Blueprint, jsonify, request, session
from flask import current_app as app
from datetime import datetime
from app.utils import login_required_custom
import os, base64, smtplib, traceback
from email.mime.text import MIMEText
from app import mysql, csrf
import re as _re

bp_glp = Blueprint('bp_glp', __name__, url_prefix='/glp')

import os

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
    if not base64_data:
        return None
    if "," in base64_data:
        base64_data = base64_data.split(",", 1)[1]
    ruta_archivo = os.path.join(carpeta, nombre_archivo)
    ruta_web = os.path.relpath(ruta_archivo, "static").replace(os.path.sep, "/")
    with open(ruta_archivo, "wb") as f:
        f.write(base64.b64decode(base64_data))
    return ruta_web


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
    row = cur.fetchone() or {}
    fecha_ini = row.get("fecha_ini")

    dias = 1
    if fecha_ini:
        try:
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

        delta = nivel_prev - nivel_act
        if delta <= 0:
            continue

        consumo_total_kg += densidad * cap * (delta / 100.0)

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
    Esto garantiza proveedor en operaciones offline también.
    """
    if not tanques:
        return None
    primer_num = tanques[0].get("numero")
    if not primer_num:
        return None

    cur.execute("""
        SELECT proveedor
        FROM tanques_sedes
        WHERE empresa=%s AND TRIM(ubicacion)=TRIM(%s) AND nombre_tanque=%s
        LIMIT 1
    """, (empresa, ubicacion, primer_num))
    p = cur.fetchone()
    return p.get("proveedor") if p else None


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
    except Exception as e:
        return jsonify({"success": False, "message": "Error leyendo JSON"}), 400

    if not data:
        return jsonify({"success": False, "message": "No se recibió JSON válido"}), 400

    usuario    = session.get('nombre') or ''
    empresa    = session.get('empresa') or ''
    id_empresa = session.get('empresa_id') or 0
    ubicacion  = _normalize_sede(data.get('sede'))

    # Idempotencia por op_id
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
            "resumen": {"operacion": "inicio_calefaccion", "sede": ubicacion}
        }), 200
    cur_check.close()

    pollitos   = data.get('pollitos')
    criadoras  = data.get('criadoras')
    tanques    = data.get('tanques', []) or []

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
            if row.get("activo", 0) > 0:
                cur.execute("""
                    SELECT lote, fecha
                    FROM cardex_glp
                    WHERE empresa = %s
                      AND TRIM(ubicacion) = TRIM(%s)
                      AND estatus_lote = 'ACTIVO'
                    ORDER BY fecha DESC, id DESC LIMIT 1
                """, (empresa, ubicacion))
                conf = cur.fetchone() or {}
                msg = f"Ya existe un lote activo (lote {conf.get('lote')} del {conf.get('fecha')})."
                return jsonify({"success": False, "message": msg})

            fecha = datetime.now().date()
            lote_id = f"{fecha.strftime('%Y%m%d')}_{ubicacion.replace(' ', '')}"

            columnas_insert = [
                "fecha","empresa","id_empresa","ubicacion","lote","estatus_lote",
                "operacion","tipo","clase","criadoras","pollitos",
                "registro","dias_operacion","op_id"
            ]
            valores_insert = [
                fecha, empresa, id_empresa, ubicacion, lote_id, 'ACTIVO',
                'inicio_calefaccion', 'manual', 'saldo inicial',
                criadoras if empresa=="Pollos GAR SAS" else None,
                pollitos  if empresa=="Pollos GAR SAS" else None,
                usuario, 0, op_id
            ]
            cur.execute(
                f"INSERT INTO cardex_glp ({', '.join(columnas_insert)}) "
                f"VALUES ({', '.join(['%s']*len(columnas_insert))})",
                valores_insert
            )

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
                    f"UPDATE cardex_glp SET {', '.join(set_cols)} WHERE lote=%s",
                    set_vals + [lote_id]
                )

            proveedor_principal = _buscar_proveedor_principal(cur, empresa, ubicacion, tanques)

            saldo_estimado_gal = saldo_estimado_kg/densidad if densidad else 0.0
            cur.execute("""
                UPDATE cardex_glp
                   SET saldo_estimado_kg=%s,
                       saldo_estimado_galones=%s,
                       proveedor=%s
                 WHERE lote=%s
            """, (round(saldo_estimado_kg,2), round(saldo_estimado_gal,2), proveedor_principal, lote_id))

            dias_operacion = 1
            cur.execute("""
                UPDATE cardex_glp
                   SET dias_operacion=%s
                 WHERE lote=%s AND operacion='inicio_calefaccion'
            """, (dias_operacion, lote_id))

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
                    cuerpo  = "📩 Solicitud de Tanqueo GLP\n\n"
                    cuerpo += f"Empresa: {empresa}\nSede: {ubicacion}\nLote: {lote_id}\n"
                    cuerpo += f"Fecha: {fecha.strftime('%Y-%m-%d')}\nDía de calefacción: {dias_operacion}\n\n"
                    cuerpo += "Recomendaciones:\n"
                    for t in tanques_bajos:
                        cuerpo += (
                            f"- {t['numero']}: nivel {t['nivel_inicial']}% → "
                            f"llenar hasta {t['porcentaje_solicitado']}%\n"
                        )

                    try:
                        msg = MIMEText(cuerpo)
                        msg["Subject"] = "Solicitud de Tanqueo GLP"
                        msg["From"] = EMAIL_USER
                        msg["To"] = ", ".join(destinatarios)
                        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                            server.starttls()
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
                    except Exception:
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

            carpeta = os.path.join("static","testigos",empresa.replace(" ","_"),lote_id)
            os.makedirs(carpeta, exist_ok=True)

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
                    set_cols.append(f"`nivel {num}`=%s");     set_vals.append(nivel_fin)
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)

                    if foto_ini:
                        ruta_ini = _guardar_testigo(foto_ini, carpeta, f"{num}_nivel_inicial_tanqueo.jpg")
                        set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta_ini)
                    if foto_fin:
                        ruta_fin = _guardar_testigo(foto_fin, carpeta, f"{num}_nivel_final_tanqueo.jpg")
                        set_cols.append(f"`testigo nivel {num}`=%s"); set_vals.append(ruta_fin)

                    if foto_bau:
                        ruta_baucher = _guardar_testigo(foto_bau, carpeta, f"{num}_baucher_tanqueo.jpg")
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

            # consumo con niveles FINALES
            consumo_kg, kg_pollito, pollitos = _calcular_consumo_lote(
                cur, empresa, ubicacion, lote_id, id_operacion,
                [{"numero": t.get("numero"),
                  "capacidad": t.get("capacidad"),
                  "nivel": t.get("nivel_final")} for t in tanques]
            )

            dias_operacion = _calcular_actualizar_dias_operacion(cur, empresa, ubicacion, lote_id, fecha)

            dens_prom = sum(densidades_registradas)/len(densidades_registradas) if densidades_registradas else 0.0
            masa_esperada_total = sum(masas_esperadas) if masas_esperadas else 0.0
            masa_facturada_total = sum(masas_facturadas) if masas_facturadas else 0.0
            porcentaje_dif_prom = sum(desviaciones_porcentuales)/len(desviaciones_porcentuales) if desviaciones_porcentuales else 0.0

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
                round(porcentaje_dif_prom,2),
                proveedor_principal,
                id_operacion
            ))

            alerta_enviada = False
            if masa_esperada_total > 0 and abs(porcentaje_dif_prom) > 8.0 and proveedor_principal:
                cur.execute("SELECT email1,email2 FROM proveedores WHERE proveedor=%s",
                            (proveedor_principal,))
                c = cur.fetchone() or {}
                destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    cuerpo  = "⚠️ Desviación en tanqueo de GLP\n\n"
                    cuerpo += f"Empresa: {empresa}\nSede: {ubicacion}\nLote: {lote_id}\nFecha: {fecha.strftime('%Y-%m-%d')}\n\n"
                    cuerpo += f"Masa esperada total: {round(masa_esperada_total,2)} kg\n"
                    cuerpo += f"Masa facturada total: {round(masa_facturada_total,2)} kg\n"
                    cuerpo += f"Desviación promedio: {round(porcentaje_dif_prom,2)} %\n"

                    try:
                        msg = MIMEText(cuerpo)
                        msg["Subject"] = "Alerta desviación tanqueo GLP"
                        msg["From"] = EMAIL_USER
                        msg["To"] = ", ".join(destinatarios)
                        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                            server.starttls()
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
                        alerta_enviada = True
                    except Exception:
                        traceback.print_exc()

            mysql.connection.commit()

        mensaje = "Tanqueo registrado correctamente."
        if consumo_kg > 0:
            mensaje += f" Consumo desde la última operación: {round(consumo_kg,2)} kg"
            if kg_pollito > 0:
                mensaje += f" ({round(kg_pollito,6)} kg_pollito)."

        if masa_esperada_total > 0:
            mensaje += (
                f" Control de tanqueo: esperado {round(masa_esperada_total,2)} kg, "
                f"facturado {round(masa_facturada_total,2)} kg "
                f"(desviación promedio {round(porcentaje_dif_prom,2)}%)."
            )
            if abs(porcentaje_dif_prom) > 8.0:
                mensaje += " ⚠️ Desviación superior al 8%."
                if alerta_enviada:
                    mensaje += " Se envió alerta por email."

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
            "porcentaje_diferencia": round(porcentaje_dif_prom,2),
            "proveedor": proveedor_principal,
            "op_id": op_id
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
            "resumen": {"operacion": "consumo", "sede": data.get("sede","")}
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
                lote_id, 'ACTIVO', "consumo", "manual", "egreso",
                usuario, op_id
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
                tst = tk.get("testigo")

                if num and niv is not None:
                    set_cols.append(f"`nivel {num}`=%s"); set_vals.append(niv)
                if num and cap is not None:
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)
                if num and tst:
                    ruta = _guardar_testigo(tst, carpeta, f"{num}_consumo.jpg")
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
                cur.execute("SELECT email1,email2 FROM proveedores WHERE proveedor=%s",
                            (proveedor_principal,))
                c = cur.fetchone() or {}
                destinatarios = [e for e in [c.get("email1"), c.get("email2")] if e]
                if not destinatarios and EMAIL_USER:
                    destinatarios = [EMAIL_USER]

                if destinatarios:
                    cuerpo  = "📩 Solicitud de Tanqueo GLP\n\n"
                    cuerpo += f"Empresa: {empresa}\nSede: {ubicacion}\nLote: {lote_id}\n"
                    cuerpo += f"Fecha: {fecha.strftime('%Y-%m-%d')}\nDía de calefacción: {dias_operacion}\n\n"
                    cuerpo += "Recomendaciones:\n"
                    for t in tanques_bajos:
                        cuerpo += (
                            f"- {t['numero']}: nivel {t['nivel_inicial']}% → "
                            f"llenar hasta {t['porcentaje_solicitado']}%\n"
                        )

                    try:
                        msg = MIMEText(cuerpo)
                        msg["Subject"] = "Solicitud de Tanqueo GLP"
                        msg["From"] = EMAIL_USER
                        msg["To"] = ", ".join(destinatarios)
                        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                            server.starttls()
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.sendmail(EMAIL_USER, destinatarios, msg.as_string())
                    except Exception:
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
            "pollitos": pollitos,
            "proveedor": proveedor_principal,
            "op_id": op_id
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
                tst = tk.get("testigo")

                if num and niv is not None:
                    set_cols.append(f"`nivel {num}`=%s"); set_vals.append(niv)
                if num and cap is not None:
                    set_cols.append(f"`capacidad {num}`=%s"); set_vals.append(cap)
                if num and tst:
                    ruta = _guardar_testigo(tst, carpeta, f"{num}_final.jpg")
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
        return jsonify({"success": False, "message": "Error al finalizar calefacción."})
