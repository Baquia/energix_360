# bp_gestion_mermas.py
from flask import Blueprint, request, jsonify, current_app, session, send_file
from datetime import datetime
import os, base64, uuid
from io import BytesIO

# PDF / Reportes
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.units import cm
from reportlab.lib.utils import ImageReader

# Importa la conexión y el CSRF desde la app principal
from app import mysql, csrf

# === Definición del blueprint ===
bp_gestion_mermas = Blueprint('bp_gestion_mermas', __name__)

# =========================
# Utilidades internas
# =========================

def _get_umbral_pct(empresa_id=None):
    """
    Retorna el umbral de merma (%) para auto-aprobación.
    Si tienes una tabla de parámetros por empresa, léela aquí.
    """
    try:
        # cur = mysql.connection.cursor()
        # cur.execute("SELECT umbral_merma_pct FROM parametros_mermas WHERE empresa_id=%s", (empresa_id,))
        # row = cur.fetchone()
        # cur.close()
        # if row and row.get('umbral_merma_pct') is not None:
        #     return float(row['umbral_merma_pct'])
        pass
    except Exception as e:
        current_app.logger.exception(e)
    # Valor por defecto
    return 1.0


def _save_base64_image(data_url, empresa_slug='generico'):
    """
    Guarda una imagen recibida como data URL base64 en /static/mermas/<empresa>/<yyyymm>/
    Retorna la ruta relativa a /static (ej: 'mermas/pollos_gar/202511/archivo.jpg').
    """
    if not data_url or not isinstance(data_url, str) or not data_url.startswith('data:image'):
        return None
    try:
        header, b64data = data_url.split(',', 1)
        ext = 'jpg'
        if 'png' in header: ext = 'png'
        if 'jpeg' in header: ext = 'jpg'
        yyyymm = datetime.now().strftime('%Y%m')
        folder = os.path.join(current_app.static_folder, 'mermas', empresa_slug, yyyymm)
        os.makedirs(folder, exist_ok=True)
        filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.{ext}"
        path = os.path.join(folder, filename)
        with open(path, 'wb') as f:
            f.write(base64.b64decode(b64data))
        return f"mermas/{empresa_slug}/{yyyymm}/{filename}"
    except Exception as e:
        current_app.logger.exception(e)
        return None


def _insert_merma_registro(data, estatus='aprobada', decision='aprobada'):
    """
    Inserta un registro en mermas_pollosgar o actualiza estatus/decision si viene 'id' en data.
    Retorna el id del registro afectado.
    """
    cur = mysql.connection.cursor()

    if data.get('id'):  # actualización de estado
        cur.execute("""
            UPDATE mermas_pollosgar
               SET estatus=%s,
                   decision=%s,
                   fecha_decision=NOW()
             WHERE id=%s
        """, (estatus, decision, data['id']))
        mysql.connection.commit()
        cur.close()
        return int(data['id'])

    cur.execute("""
        INSERT INTO mermas_pollosgar
            (fecha, empresa, empresa_id, operador_id, operador_nombre,
             cliente, vehiculo, factura, kg_factura, kg_entregados,
             merma_kg, merma_pct, evidencia_url, estatus, decision, fecha_decision)
        VALUES
            (NOW(), %s, %s, %s, %s,
             %s, %s, %s, %s, %s,
             %s, %s, %s, %s, %s, %s)
    """, (
        data.get('empresa'),
        data.get('empresa_id'),
        data.get('operador_id'),
        data.get('operador_nombre'),
        data.get('cliente'),
        data.get('vehiculo'),
        data.get('factura'),
        data.get('kg_factura'),
        data.get('kg_entregados'),
        data.get('merma_kg'),
        data.get('merma_pct'),
        data.get('evidencia_url'),
        estatus,
        decision,
        None if estatus == 'pendiente' else datetime.now()
    ))
    mysql.connection.commit()
    new_id = cur.lastrowid
    cur.close()
    return int(new_id)

# =========================
# Endpoints existentes
# =========================

@bp_gestion_mermas.route('/mermas/umbral', methods=['GET'])
def mermas_umbral():
    empresa_id = session.get('empresa_id') or session.get('nit')
    umbral = _get_umbral_pct(empresa_id)
    return jsonify(success=True, umbral_pct=umbral)


@csrf.exempt
@bp_gestion_mermas.route('/mermas/registrar', methods=['POST'])
def mermas_registrar():
    """
    Reglas:
    - Un operador NO puede tener más de una merma con estatus='pendiente'.
    - Si existe una 'rechazada' del MISMO operador y MISMA factura -> se sobrescribe y vuelve a 'pendiente'.
    - Uso de GET_LOCK por operador para evitar carreras.
    """
    try:
        j = request.get_json(force=True, silent=True) or {}
    except Exception:
        return jsonify(success=False, message="JSON inválido"), 400

    # Sesión
    empresa = (session.get('empresa') or '').strip()
    empresa_id = str(session.get('empresa_id') or session.get('nit') or '').strip()
    operador_id = str(session.get('usuario_id') or '').strip()
    operador_nombre = (session.get('usuario_nombre') or '').strip()

    # Fallback tolerante (solo si sesión viene vacía)
    if not operador_nombre: operador_nombre = (j.get('operador_nombre') or '').strip()
    if not operador_id:     operador_id     = str(j.get('operador_id') or '').strip()
    if not empresa:         empresa         = (j.get('empresa') or '').strip()

    # Datos del formulario
    cliente  = (j.get('cliente')  or '').strip()
    vehiculo = (j.get('vehiculo') or '').strip()
    factura  = (j.get('factura')  or '').strip()

    if not (cliente and vehiculo and factura):
        return jsonify(success=False, message="Faltan campos obligatorios (cliente/vehiculo/factura)"), 400

    try:
        kgf = float(j.get('kg_factura'))
        kge = float(j.get('kg_entregados'))
    except (TypeError, ValueError):
        return jsonify(success=False, message="kg_factura y kg_entregados deben ser numéricos"), 400
    if kgf <= 0: return jsonify(success=False, message="kg_factura debe ser > 0"), 400
    if kge < 0:  return jsonify(success=False, message="kg_entregados debe ser >= 0"), 400

    # Cálculos
    merma_kg  = max(0.0, kgf - kge)
    merma_pct = (merma_kg / kgf * 100.0) if kgf > 0 else 0.0

    # Evidencia
    empresa_slug = (empresa or 'generico').lower().replace(' ', '_')
    evidencia_url = _save_base64_image(j.get('evidencia_foto'), empresa_slug=empresa_slug)

    # Candado por operador
    lock_name = f"mermas_op_{operador_id}"
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT GET_LOCK(%s, 10)", (lock_name,))
        got = cur.fetchone()
        if not got or list(got.values())[0] != 1:
            cur.close()
            return jsonify(success=False, message="No fue posible obtener bloqueo. Intenta de nuevo."), 409

        # ¿Existe pendiente?
        cur.execute("""
            SELECT id, factura
              FROM mermas_pollosgar
             WHERE operador_id=%s AND estatus='pendiente'
             LIMIT 1
        """, (operador_id,))
        row_pend = cur.fetchone()
        if row_pend:
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
            cur.close()
            return jsonify(
                success=False,
                code='PENDING_EXISTS',
                message=f"Tienes una merma pendiente de aprobación (ID {row_pend['id']}, factura {row_pend['factura']})."
            ), 409

        # Regla de reintento sobre rechazada misma factura
        cur.execute("""
            SELECT id
              FROM mermas_pollosgar
             WHERE operador_id=%s AND factura=%s AND estatus='rechazada'
             ORDER BY fecha_decision DESC, fecha DESC
             LIMIT 1
        """, (operador_id, factura))
        row_rech = cur.fetchone()

        if row_rech:
            cur.execute("""
                UPDATE mermas_pollosgar
                   SET fecha=NOW(),
                       empresa=%s, empresa_id=%s,
                       operador_nombre=%s,
                       cliente=%s, vehiculo=%s,
                       kg_factura=%s, kg_entregados=%s,
                       merma_kg=%s, merma_pct=%s,
                       evidencia_url=%s,
                       estatus='pendiente',
                       decision='por_aprobar',
                       comentario_control=NULL,
                       fecha_decision=NULL
                 WHERE id=%s
            """, (
                empresa, empresa_id, operador_nombre,
                cliente, vehiculo,
                round(kgf, 3), round(kge, 3),
                round(merma_kg, 3), round(merma_pct, 3),
                evidencia_url,
                row_rech['id']
            ))
            mysql.connection.commit()
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
            cur.close()
            current_app.logger.info(f"[MERMA REGISTRAR] reintento sobre rechazada -> id={row_rech['id']}")
            return jsonify(success=True, aprobado=False, id=row_rech['id'], umbral_pct=_get_umbral_pct(empresa_id))

        # Alta normal
        umbral = _get_umbral_pct(empresa_id)
        auto_aprueba = merma_pct <= umbral

        data = {
            'empresa': empresa,
            'empresa_id': empresa_id,
            'operador_id': operador_id,
            'operador_nombre': operador_nombre,
            'cliente': cliente,
            'vehiculo': vehiculo,
            'factura': factura,
            'kg_factura': round(kgf, 3),
            'kg_entregados': round(kge, 3),
            'merma_kg': round(merma_kg, 3),
            'merma_pct': round(merma_pct, 3),
            'evidencia_url': evidencia_url
        }

        if auto_aprueba:
            reg_id = _insert_merma_registro(data, estatus='aprobada', decision='aprobada')
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
            cur.close()
            return jsonify(success=True, aprobado=True, id=reg_id, umbral_pct=umbral)
        else:
            reg_id = _insert_merma_registro(data, estatus='pendiente', decision='por_aprobar')
            cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
            cur.close()
            return jsonify(success=True, aprobado=False, id=reg_id, umbral_pct=umbral)

    except Exception as e:
        current_app.logger.exception(e)
        try: cur.execute("SELECT RELEASE_LOCK(%s)", (lock_name,))
        except Exception: pass
        cur.close()
        return jsonify(success=False, message="Error interno al registrar."), 500


@bp_gestion_mermas.route('/mermas/pending', methods=['GET'])
def mermas_pending():
    empresa_id = request.args.get('empresa_id') or session.get('empresa_id') or session.get('nit')
    cur = mysql.connection.cursor()
    if empresa_id:
        cur.execute("""
            SELECT id, fecha, empresa, empresa_id, operador_nombre,
                   cliente, vehiculo, factura, kg_factura, kg_entregados,
                   merma_kg, merma_pct, evidencia_url, estatus, decision
              FROM mermas_pollosgar
             WHERE estatus='pendiente' AND empresa_id=%s
             ORDER BY fecha ASC
        """, (empresa_id,))
    else:
        cur.execute("""
            SELECT id, fecha, empresa, empresa_id, operador_nombre,
                   cliente, vehiculo, factura, kg_factura, kg_entregados,
                   merma_kg, merma_pct, evidencia_url, estatus, decision
              FROM mermas_pollosgar
             WHERE estatus='pendiente'
             ORDER BY fecha ASC
        """)
    rows = cur.fetchall()
    cur.close()
    return jsonify(success=True, items=rows)


@csrf.exempt
@bp_gestion_mermas.route('/mermas/accion', methods=['POST'])
def mermas_accion():
    """
    Recibe: { id, accion, comentario? }
    accion ∈ {'aprobar','no_aprobar','aprobar_nc'}
    """
    j = request.get_json(force=True, silent=True) or {}
    reg_id = j.get('id')
    accion = (j.get('accion') or '').strip().lower()

    if not reg_id or accion not in ('aprobar', 'no_aprobar', 'aprobar_nc'):
        return jsonify(success=False, message='Parámetros inválidos'), 400

    if accion == 'aprobar':
        _insert_merma_registro({'id': reg_id}, estatus='aprobada', decision='aprobada')
        estado = 'aprobada'
    elif accion == 'no_aprobar':
        _insert_merma_registro({'id': reg_id}, estatus='rechazada', decision='no_aprobrada')
        estado = 'rechazada'
    else:
        _insert_merma_registro({'id': reg_id}, estatus='aprobada_no_conforme', decision='aprobada_no_conforme')
        estado = 'aprobada_no_conforme'

    try:
        if j.get('comentario'):
            cur = mysql.connection.cursor()
            cur.execute("""
                UPDATE mermas_pollosgar
                   SET comentario_control=%s
                 WHERE id=%s
            """, (j['comentario'], reg_id))
            mysql.connection.commit()
            cur.close()
    except Exception as e:
        current_app.logger.exception(e)

    return jsonify(success=True, id=reg_id, estado=estado)


@bp_gestion_mermas.route('/mermas/estado', methods=['GET'])
def mermas_estado():
    reg_id = request.args.get('id')
    if not reg_id:
        return jsonify(success=False, message='Falta id'), 400

    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT estatus, decision
          FROM mermas_pollosgar
         WHERE id=%s
    """, (reg_id,))
    row = cur.fetchone()
    cur.close()

    if not row:
        return jsonify(success=False, message='No encontrado'), 404

    return jsonify(success=True, estado=row['estatus'], decision=row['decision'])

# =========================
# NUEVO: Consultas e informes
# =========================

def _parse_date_range(desde, hasta):
    """
    Retorna (fecha_desde, fecha_hasta) en 'YYYY-MM-DD' o None si no vienen.
    Si solo envían 'desde', 'hasta' será hoy. Si solo envían 'hasta', 'desde' será el inicio del mes de 'hasta'.
    """
    try:
        d = datetime.strptime(desde, "%Y-%m-%d").date() if desde else None
    except Exception:
        d = None
    try:
        h = datetime.strptime(hasta, "%Y-%m-%d").date() if hasta else None
    except Exception:
        h = None

    today = datetime.now().date()
    if d and not h:
        h = today
    if h and not d:
        d = h.replace(day=1)
    return (d.isoformat() if d else None, h.isoformat() if h else None)


@bp_gestion_mermas.route('/mermas/opciones', methods=['GET'])
def mermas_opciones():
    """
    /mermas/opciones?tipo=zona|vendedor|cliente|vehiculo
    Devuelve opciones únicas (filtradas por empresa) para poblar selects.
    """
    tipo = (request.args.get('tipo') or '').strip().lower()
    empresa_id = session.get('empresa_id') or session.get('nit')

    colmap = {
        'zona': 'zona',
        'vendedor': 'operador_nombre',
        'cliente': 'cliente',
        'vehiculo': 'vehiculo',
    }
    col = colmap.get(tipo)
    if not col:
        return jsonify(success=False, message='tipo inválido'), 400

    cur = mysql.connection.cursor()
    try:
        if empresa_id:
            cur.execute(f"""
                SELECT DISTINCT {col} AS val
                  FROM mermas_pollosgar
                 WHERE {col} IS NOT NULL AND {col} <> '' AND empresa_id=%s
                 ORDER BY {col} ASC
            """, (empresa_id,))
        else:
            cur.execute(f"""
                SELECT DISTINCT {col} AS val
                  FROM mermas_pollosgar
                 WHERE {col} IS NOT NULL AND {col} <> ''
                 ORDER BY {col} ASC
            """)
        rows = cur.fetchall()
    finally:
        cur.close()

    vals = [r['val'] for r in rows if r.get('val')]
    return jsonify(success=True, items=vals)


@csrf.exempt
@bp_gestion_mermas.route('/mermas/consulta', methods=['POST'])
def mermas_consulta():
    """
    Recibe JSON:
      {
        "tipo": "zona|vendedor|cliente|vehiculo|general",
        "zona"|"vendedor"|"cliente"|"vehiculo"|"estatus": "...",
        "desde": "YYYY-MM-DD",
        "hasta": "YYYY-MM-DD"
      }
    Responde:
      {
        success: true,
        filtros: {...},
        resumen: {kg_totales, merma_total, merma_pct_total, registros},
        detalle_clientes: [...],
        serie_pct: [...],             # % diario (se conserva)
        operaciones: [...],
        chart: {type, labels, values, series_label, x_title, y_title}  # <-- NUEVO
      }
    """
    j = request.get_json(force=True, silent=True) or {}
    tipo = (j.get('tipo') or '').strip().lower()
    desde, hasta = _parse_date_range(j.get('desde'), j.get('hasta'))
    empresa_id = session.get('empresa_id') or session.get('nit')

    where = []
    params = []

    if empresa_id:
        where.append("empresa_id=%s")
        params.append(empresa_id)

    if tipo == 'zona':
        where.append("zona=%s")
        params.append((j.get('zona') or '').strip())
    elif tipo == 'vendedor':
        where.append("operador_nombre=%s")
        params.append((j.get('vendedor') or '').strip())
    elif tipo == 'cliente':
        where.append("cliente=%s")
        params.append((j.get('cliente') or '').strip())
    elif tipo == 'vehiculo':
        where.append("vehiculo=%s")
        params.append((j.get('vehiculo') or '').strip())
    elif tipo == 'general':
        est = (j.get('estatus') or '').strip().lower()
        if est:
            where.append("estatus=%s")
            params.append(est)
    else:
        return jsonify(success=False, message="tipo inválido"), 400

    if desde:
        where.append("DATE(fecha) >= %s")
        params.append(desde)
    if hasta:
        where.append("DATE(fecha) <= %s")
        params.append(hasta)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    cur = mysql.connection.cursor()
    try:
        # Resumen
        cur.execute(f"""
            SELECT COALESCE(SUM(kg_factura),0) AS kg_totales,
                   COALESCE(SUM(merma_kg),0) AS merma_total,
                   COUNT(*) AS registros
              FROM mermas_pollosgar
            {where_sql}
        """, tuple(params))
        resumen = cur.fetchone() or {}

        # Detalle por cliente
        cur.execute(f"""
            SELECT cliente,
                   COALESCE(SUM(kg_factura),0) AS kg_cliente,
                   COALESCE(SUM(merma_kg),0) AS merma_cliente
              FROM mermas_pollosgar
            {where_sql}
             GROUP BY cliente
             ORDER BY kg_cliente DESC
        """, tuple(params))
        detalle_rows = cur.fetchall() or []

        # Serie diaria para % (se conserva por si se usa)
        cur.execute(f"""
            SELECT DATE(fecha) AS fecha,
                   COALESCE(SUM(merma_kg),0)   AS merma_kg_dia,
                   COALESCE(SUM(kg_factura),0) AS kg_factura_dia
              FROM mermas_pollosgar
            {where_sql}
             GROUP BY DATE(fecha)
             ORDER BY DATE(fecha) ASC
        """, tuple(params))
        serie_rows = cur.fetchall() or []

        # Listado completo de operaciones (IMPORTANTE: fecha completa, no DATE(fecha))
        cur.execute(f"""
            SELECT fecha,
                   cliente, vehiculo, factura,
                   kg_factura, kg_entregados,
                   merma_kg, merma_pct,
                   operador_nombre
              FROM mermas_pollosgar
            {where_sql}
             ORDER BY fecha ASC, cliente ASC, factura ASC
        """, tuple(params))
        ops_rows = cur.fetchall() or []
    finally:
        cur.close()

    total_kg = float(resumen.get('kg_totales') or 0.0) or 0.0
    merma_total = float(resumen.get('merma_total') or 0.0) or 0.0
    merma_pct_total = (merma_total / total_kg * 100.0) if total_kg > 0 else 0.0

    # Detalle por cliente (kilos, mermas y %)
    detalle = []
    for r in detalle_rows:
        kgc = float(r.get('kg_cliente') or 0.0)
        mc  = float(r.get('merma_cliente') or 0.0)
        pct = (mc / kgc * 100.0) if kgc > 0 else 0.0
        detalle.append({
            'cliente': r.get('cliente') or '(sin cliente)',
            'kg_cliente': round(kgc, 3),
            'merma_cliente': round(mc, 3),
            'pct_cliente': round(pct, 3),
        })

    # Serie % diario (no es el gráfico principal ahora, pero se deja para usos futuros)
    serie_pct = []
    for r in serie_rows:
        kg_d = float(r.get('kg_factura_dia') or 0.0)
        m_d  = float(r.get('merma_kg_dia') or 0.0)
        pctd = (m_d / kg_d * 100.0) if kg_d > 0 else 0.0
        serie_pct.append({'fecha': str(r.get('fecha')), 'merma_pct': round(pctd, 3)})

    # Operaciones completas
    operaciones = []
    for r in ops_rows:
        operaciones.append({
            'fecha': str(r.get('fecha')),
            'cliente': r.get('cliente'),
            'vehiculo': r.get('vehiculo'),
            'factura': r.get('factura'),
            'kg_factura': round(float(r.get('kg_factura') or 0.0), 3),
            'kg_entregados': round(float(r.get('kg_entregados') or 0.0), 3),
            'merma_kg': round(float(r.get('merma_kg') or 0.0), 3),
            'merma_pct': round(float(r.get('merma_pct') or 0.0), 3),
            'operador_nombre': r.get('operador_nombre')
        })

    # --------- Construcción del CHART según 'tipo' ---------
    chart = {
        'type': 'bar',
        'labels': [],
        'values': [],
        'series_label': 'Merma (%)',
        'x_title': '',
        'y_title': 'Merma (%)'
    }

    if tipo in ('zona', 'vendedor'):
        # Barras: merma % por cliente
        chart['type'] = 'bar'
        chart['labels'] = [d['cliente'] for d in detalle]
        chart['values'] = [d['pct_cliente'] for d in detalle]
        chart['x_title'] = 'Cliente'

    elif tipo == 'cliente':
        # Barras: merma % por factura (agregada por factura)
        fact_kg = {}
        fact_merma = {}
        for op in operaciones:
            fac = (op['factura'] or '').strip()
            if not fac:
                continue
            fact_kg[fac] = fact_kg.get(fac, 0.0) + float(op['kg_factura'] or 0.0)
            fact_merma[fac] = fact_merma.get(fac, 0.0) + float(op['merma_kg'] or 0.0)
        fact_labels = sorted(fact_kg.keys())
        fact_values = []
        for fac in fact_labels:
            kg = fact_kg.get(fac, 0.0)
            mk = fact_merma.get(fac, 0.0)
            pct = (mk / kg * 100.0) if kg > 0 else 0.0
            fact_values.append(round(pct, 3))
        chart['type'] = 'bar'
        chart['labels'] = fact_labels
        chart['values'] = fact_values
        chart['x_title'] = 'Factura'

    elif tipo == 'vehiculo':
        # Puntos: merma % por fecha y hora de entrega
        chart['type'] = 'points'
        # Usamos la fecha completa
        chart['labels'] = [op['fecha'] for op in operaciones]
        chart['values'] = [op['merma_pct'] for op in operaciones]
        chart['x_title'] = 'Fecha y hora de entrega'

    out = {
        'success': True,
        'filtros': {
            'tipo': tipo,
            'desde': desde,
            'hasta': hasta,
            'zona': j.get('zona'),
            'vendedor': j.get('vendedor'),
            'cliente': j.get('cliente'),
            'vehiculo': j.get('vehiculo'),
            'estatus': j.get('estatus')
        },
        'resumen': {
            'kg_totales': round(total_kg, 3),
            'merma_total': round(merma_total, 3),
            'merma_pct_total': round(merma_pct_total, 3),
            'registros': int(resumen.get('registros') or 0),
        },
        'detalle_clientes': detalle,
        'serie_pct': serie_pct,     # se conserva
        'operaciones': operaciones,
        'chart': chart              # <-- NUEVO
    }
    return jsonify(out)


@csrf.exempt
@bp_gestion_mermas.route('/mermas/consulta/pdf', methods=['POST'])
def mermas_consulta_pdf():
    """
    Recibe JSON con:
      titulo, filtros, resumen, detalle_clientes, operaciones, chart_png
    Devuelve un PDF descargable.
    """
    j = request.get_json(force=True, silent=True) or {}
    titulo = (j.get('titulo') or 'Informe de mermas').strip()
    resumen = j.get('resumen') or {}
    detalle = j.get('detalle_clientes') or []
    chart = j.get('chart_png')
    operaciones = j.get('operaciones') or []

    buf = BytesIO()
    p = canvas.Canvas(buf, pagesize=landscape(A4))
    W, H = landscape(A4)

    # Encabezado
    p.setFont("Helvetica-Bold", 16)
    p.drawString(2*cm, H - 2*cm, titulo)

    filtros = j.get('filtros') or {}
    subt = []
    if filtros.get('desde'): subt.append(f"Desde: {filtros['desde']}")
    if filtros.get('hasta'): subt.append(f"Hasta: {filtros['hasta']}")
    if filtros.get('tipo') == 'zona' and filtros.get('zona'): subt.append(f"Zona: {filtros['zona']}")
    if filtros.get('tipo') == 'vendedor' and filtros.get('vendedor'): subt.append(f"Vendedor: {filtros['vendedor']}")
    if filtros.get('tipo') == 'cliente' and filtros.get('cliente'): subt.append(f"Cliente: {filtros['cliente']}")
    if filtros.get('tipo') == 'vehiculo' and filtros.get('vehiculo'): subt.append(f"Vehículo: {filtros['vehiculo']}")
    if filtros.get('tipo') == 'general' and filtros.get('estatus'): subt.append(f"Estatus: {filtros['estatus']}")

    p.setFont("Helvetica", 11)
    p.drawString(2*cm, H - 2.7*cm, " • ".join(subt))

    # Resumen
    y = H - 3.8*cm
    p.setFont("Helvetica-Bold", 12)
    p.drawString(2*cm, y, "Resumen")
    p.setFont("Helvetica", 11)
    y -= 0.6*cm
    p.drawString(2*cm, y, f"KG totales: {resumen.get('kg_totales', 0)}")
    y -= 0.5*cm
    p.drawString(2*cm, y, f"Merma total (kg): {resumen.get('merma_total', 0)}")
    y -= 0.5*cm
    p.drawString(2*cm, y, f"Merma total (%): {resumen.get('merma_pct_total', 0)} %")
    y -= 0.5*cm
    p.drawString(2*cm, y, f"Registros: {resumen.get('registros', 0)}")

    # Tabla por cliente
    y -= 1.0*cm
    p.setFont("Helvetica-Bold", 11)
    p.drawString(2*cm, y, "Detalle por cliente")
    y -= 0.4*cm
    p.setFont("Helvetica-Bold", 10)
    p.drawString(2*cm, y, "Cliente")
    p.drawString(10*cm, y, "KG")
    p.drawString(14*cm, y, "Merma (kg)")
    p.drawString(18*cm, y, "Merma (%)")
    y -= 0.4*cm
    p.setFont("Helvetica", 10)
    for row in detalle[:16]:
        p.drawString(2*cm, y, str(row.get('cliente', '')))
        p.drawRightString(13*cm, y, f"{row.get('kg_cliente', 0)}")
        p.drawRightString(17*cm, y, f"{row.get('merma_cliente', 0)}")
        p.drawRightString(21*cm, y, f"{row.get('pct_cliente', 0)} %")
        y -= 0.42*cm
        if y < 6.5*cm:
            break

    # Gráfico (imagen del canvas del front)
    if chart and isinstance(chart, str) and chart.startswith('data:image'):
        try:
            b64 = chart.split(',', 1)[1]
            img = ImageReader(BytesIO(base64.b64decode(b64)))
            chart_w = W - 4*cm
            chart_h = 8*cm
            chart_x = 2*cm
            chart_y = 4*cm
            p.drawImage(img, chart_x, chart_y, width=chart_w, height=chart_h,
                        preserveAspectRatio=True, anchor='sw', mask='auto')
        except Exception:
            pass

    # Nueva página: listado completo de operaciones
    p.showPage()
    p.setFont("Helvetica-Bold", 14)
    p.drawString(2*cm, H - 2*cm, "Detalle de operaciones")
    p.setFont("Helvetica", 10)

    y = H - 3*cm
    def draw_ops_header(ypos):
        p.setFont("Helvetica-Bold", 10)
        p.drawString(2*cm,  ypos, "Fecha")
        p.drawString(5*cm,  ypos, "Cliente")
        p.drawString(12*cm, ypos, "Factura")
        p.drawRightString(17*cm, ypos, "KG fac.")
        p.drawRightString(20*cm, ypos, "KG ent.")
        p.drawRightString(23*cm, ypos, "Merma (kg)")
        p.drawRightString(26*cm, ypos, "Merma (%)")
        p.setFont("Helvetica", 10)

    draw_ops_header(y)
    y -= 0.5*cm
    for op in operaciones:
        p.drawString(2*cm,  y, str(op.get('fecha') or ''))
        p.drawString(5*cm,  y, str(op.get('cliente') or '')[:35])
        p.drawString(12*cm, y, str(op.get('factura') or ''))
        p.drawRightString(17*cm, y, f"{op.get('kg_factura', 0)}")
        p.drawRightString(20*cm, y, f"{op.get('kg_entregados', 0)}")
        p.drawRightString(23*cm, y, f"{op.get('merma_kg', 0)}")
        p.drawRightString(26*cm, y, f"{op.get('merma_pct', 0)} %")
        y -= 0.42*cm
        if y < 2.5*cm:
            p.showPage()
            p.setFont("Helvetica-Bold", 14)
            p.drawString(2*cm, H - 2*cm, "Detalle de operaciones (cont.)")
            y = H - 3*cm
            draw_ops_header(y)
            y -= 0.5*cm

    p.showPage()
    p.save()
    buf.seek(0)

    fname = f"informe_mermas.pdf"
    return send_file(buf, mimetype='application/pdf',
                     as_attachment=True,
                     download_name=fname)
