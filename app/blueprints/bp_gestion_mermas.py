from flask import Blueprint, request, jsonify, current_app, session
from app import mysql, csrf
import os, base64, uuid
from datetime import datetime

bp_gestion_mermas = Blueprint('bp_gestion_mermas', __name__)

UMBRAL_MERMA = 2.0

def _save_file(data_url, subfolder):
    if not data_url or ',' not in data_url: return None
    try:
        header, b64 = data_url.split(',', 1)
        ext = 'jpg'
        if 'video' in header or 'mp4' in header: ext = 'mp4'
        elif 'png' in header: ext = 'png'
        
        yyyymm = datetime.now().strftime('%Y%m')
        folder = os.path.join(current_app.static_folder, 'mermas', subfolder, yyyymm)
        os.makedirs(folder, exist_ok=True)
        
        filename = f"{uuid.uuid4().hex[:10]}.{ext}"
        path = os.path.join(folder, filename)
        with open(path, 'wb') as f:
            f.write(base64.b64decode(b64))
        return f"mermas/{subfolder}/{yyyymm}/{filename}"
    except Exception as e:
        print(f"Error save file: {e}")
        return None

@csrf.exempt
@bp_gestion_mermas.route('/mermas/registrar', methods=['POST'])
def mermas_registrar():
    try:
        j = request.get_json(force=True)
    except:
        return jsonify(success=False, message="JSON Erroneo"), 400

    empresa_id = str(session.get('empresa_id') or session.get('nit') or '').strip()
    empresa = (session.get('empresa') or '').strip()
    operador_id = str(session.get('cedula') or session.get('usuario_id') or '0').strip()
    operador_nombre = (session.get('nombre') or session.get('usuario_nombre') or 'Operador').strip()

    cliente = j.get('cliente')
    vehiculo = j.get('vehiculo')
    factura = j.get('factura')
    kg_total_global = float(j.get('kg_total', 0))
    items = j.get('items', [])

    if not items: return jsonify(success=False, message="Sin items"), 400

    total_facturado = 0
    total_entregado = 0
    items_procesados = []

    for idx, it in enumerate(items):
        kf = float(it['kg_facturados'])
        ke = float(it['kg_entregados'])
        merma_it = kf - ke
        total_facturado += kf
        total_entregado += ke
        
        vid = _save_file(it.get('evidencia_url'), 'videos')
        f1  = _save_file(it.get('evidencia_url2'), 'fotos')
        f2  = _save_file(it.get('evidencia_url3'), 'fotos')
        
        items_procesados.append({
            'item': it['item'], 'kg_f': kf, 'kg_e': ke, 'merma': merma_it,
            'urls': [vid, f1, f2]
        })

    merma_total = max(0, total_facturado - total_entregado)
    base = kg_total_global if kg_total_global > 0 else total_facturado
    pct = (merma_total / base * 100) if base > 0 else 0
    
    if pct <= UMBRAL_MERMA:
        estatus = 'aprobada'
        decision = 'aprobada'
    else:
        estatus = 'pendiente'
        decision = 'por_aprobar'

    cur = mysql.connection.cursor()
    try:
        now = datetime.now()
        fecha_dec = now if estatus == 'aprobada' else None
        
        for p in items_procesados:
            pct_it = (p['merma'] / p['kg_f'] * 100) if p['kg_f'] > 0 else 0
            
            cur.execute("""
                INSERT INTO mermas_pollosgar
                (fecha, empresa, empresa_id, operador_id, operador_nombre,
                 cliente, vehiculo, factura, item,
                 kg_factura, kg_entregados, merma_kg, merma_pct,
                 evidencia_url, evidencia_url1, evidencia_url2,
                 estatus, decision, fecha_decision, nota_descuento, zona)
                VALUES (%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, '', 0)
            """, (
                now, empresa, empresa_id, operador_id, operador_nombre,
                cliente, vehiculo, factura, p['item'],
                p['kg_f'], p['kg_e'], p['merma'], pct_it,
                p['urls'][0], p['urls'][1], p['urls'][2], 
                estatus, decision, fecha_dec
            ))
        
        mysql.connection.commit()
        return jsonify(success=True, status=estatus.upper(), factura=factura)

    except Exception as e:
        print("ERROR SQL:", e)
        return jsonify(success=False, message=str(e)), 500
    finally:
        cur.close()

# --- ACTUALIZACIÓN DE NOTA (CORREGIDO) ---
@csrf.exempt
@bp_gestion_mermas.route('/mermas/finalizar_con_nota', methods=['POST'])
def finalizar_con_nota():
    try:
        j = request.get_json(force=True)
        print("DEBUG RECV NOTA:", j) # Verás esto en la consola si llega bien

        factura = j.get('factura')
        nota = j.get('nota')

        if not factura or not nota:
            return jsonify(success=False, message="Datos incompletos"), 400

        cur = mysql.connection.cursor()
        # Actualiza TODAS las filas de la factura
        cur.execute("UPDATE mermas_pollosgar SET nota_descuento=%s WHERE factura=%s", (nota, factura))
        mysql.connection.commit()
        
        filas = cur.rowcount
        print(f"DEBUG: Nota actualizada en {filas} items.")
        
        cur.close()
        return jsonify(success=True)
    except Exception as e:
        print("ERROR NOTA:", e)
        return jsonify(success=False, message=str(e)), 500

@bp_gestion_mermas.route('/mermas/pending_grouped', methods=['GET'])
def pending_grouped():
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT * FROM mermas_pollosgar 
        WHERE estatus = 'pendiente'
        ORDER BY fecha ASC, factura ASC
    """)
    rows = cur.fetchall()
    cur.close()
    
    grouped = {}
    for r in rows:
        fac = r['factura']
        if fac not in grouped:
            grouped[fac] = {
                'factura': fac, 'cliente': r['cliente'], 'vehiculo': r['vehiculo'],
                'operador': r['operador_nombre'], 'fecha': r['fecha'],
                'items': [], 's_fact': 0, 's_merma': 0
            }
        
        grouped[fac]['s_fact'] += float(r['kg_factura'])
        grouped[fac]['s_merma'] += float(r['merma_kg'])
        
        grouped[fac]['items'].append({
            'item': r['item'], 'kg_f': float(r['kg_factura']), 'kg_e': float(r['kg_entregados']),
            'merma': float(r['merma_kg']),
            'vid': r['evidencia_url'], 'f1': r['evidencia_url1'], 'f2': r['evidencia_url2']
        })

    data = []
    for k, v in grouped.items():
        v['pct_global'] = (v['s_merma']/v['s_fact']*100) if v['s_fact']>0 else 0
        data.append(v)
        
    return jsonify(items=data)

@csrf.exempt
@bp_gestion_mermas.route('/mermas/decidir_supervisor', methods=['POST'])
def decidir_supervisor():
    j = request.get_json(force=True)
    factura = j['factura']
    decision_front = j['decision']
    
    nuevo_est = 'aprobada'
    nueva_dec = 'aprobada'
    
    if decision_front == 'ENTREGAR_SIN':
        nuevo_est = 'aprobada_no_conforme' 
        nueva_dec = 'aprobada_no_conforme'
    
    cur = mysql.connection.cursor()
    cur.execute("UPDATE mermas_pollosgar SET estatus=%s, decision=%s, fecha_decision=NOW() WHERE factura=%s", 
                (nuevo_est, nueva_dec, factura))
    mysql.connection.commit()
    cur.close()
    return jsonify(success=True)

@bp_gestion_mermas.route('/mermas/check_status_live', methods=['GET'])
def check_status_live():
    factura = request.args.get('factura')
    cur = mysql.connection.cursor()
    cur.execute("SELECT estatus FROM mermas_pollosgar WHERE factura=%s LIMIT 1", (factura,))
    row = cur.fetchone()
    cur.close()
    
    if row:
        st = str(row['estatus']).lower()
        if st in ['aprobada', 'aprobada_no_conforme', 'rechazada']:
            status_front = 'APROBADA'
            if st == 'aprobada_no_conforme': status_front = 'ENTREGADO_SIN_APROBACION'
            return jsonify(completed=True, status=status_front)
            
    return jsonify(completed=False)