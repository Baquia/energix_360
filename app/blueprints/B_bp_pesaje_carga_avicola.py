from flask import Blueprint, render_template, request, jsonify, flash, redirect, session, url_for, send_file
from datetime import datetime, timedelta
from app import mysql
from app.utils import login_required_custom
import MySQLdb.cursors
import json
from io import BytesIO

# Importaciones de ReportLab para la construcción del documento físico
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from app import csrf

bp_gestion_carga = Blueprint('bp_gestion_carga', __name__, url_prefix='/bqa_bascula')

# ========================================================
# RUTA 1: PANEL DE REGISTRO DE BÁSCULA (GET)
# ========================================================
@bp_gestion_carga.route('/panel', methods=['GET'])
@login_required_custom
def panel_bascula():
    id_empresa = session.get('empresa_id')
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()

    if 'cria_beneficio_aves_corral' not in tipo_empresa and str(id_empresa) != '890707006':
        flash('Acceso denegado. Módulo de pesaje exclusivo para operación avícola.', 'danger')
        return redirect('/')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (id_empresa,))
    vehiculos = cur.fetchall()

    cur.execute("""
        SELECT nombre FROM usuarios 
        WHERE empresa_id = %s AND perfil = 'auxiliar_transportecarga' 
        ORDER BY nombre ASC
    """, (id_empresa,))
    auxiliares = cur.fetchall()

    cur.execute("SELECT id, nombre_ruta, tipo_ruta FROM rutas WHERE id_empresa = %s ORDER BY nombre_ruta ASC", (id_empresa,))
    rutas = cur.fetchall()

    cur.execute("""
        SELECT nombre FROM usuarios 
        WHERE empresa_id = %s AND perfil = 'operador_transportecarga' 
        ORDER BY nombre ASC
    """, (id_empresa,))
    conductores = cur.fetchall()

    cur.close()

    return render_template(
        'B_pesaje_carga_avicola.html',
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        nit=session.get('nit'),
        active_module='bascula',
        vehiculos=vehiculos,
        auxiliares=auxiliares,
        rutas=rutas,
        conductores=conductores
    )

# ========================================================
# RUTA 2: API AJAX - PREOPERACIONAL
# ========================================================
@bp_gestion_carga.route('/api/validar_preoperacional', methods=['POST'])
@login_required_custom
def api_validar_preoperacional():
    data = request.get_json()
    placa = data.get('placa')
    conductor = data.get('conductor')
    id_empresa = session.get('empresa_id')

    if not placa or not conductor:
        return jsonify({'valido': False, 'mensaje': 'Seleccione placa y conductor para validar.'})

    hace_24h = datetime.now() - timedelta(days=1)
    hace_24h_str = hace_24h.strftime('%Y-%m-%d %H:%M:%S')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT 1 FROM inspeccion_preoperacional_carga 
        WHERE placa_vehiculo = %s AND nombre_conductor = %s AND id_empresa = %s 
          AND CONCAT(fecha_inspeccion, ' ', hora_inspeccion) >= %s 
        ORDER BY id_inspeccion DESC LIMIT 1
    """, (placa, conductor, id_empresa, hace_24h_str))
    
    preoperacional = cur.fetchone()
    cur.close()

    if not preoperacional:
        return jsonify({'valido': False, 'mensaje': f'El conductor {conductor} no ha registrado ninguna inspección diaria para el vehículo {placa} en las últimas 24 horas.'})
    
    return jsonify({'valido': True, 'mensaje': f'✅ Control de Cumplimiento Exitoso: Inspección preoperacional registrada por {conductor}.'})

# ========================================================
# RUTA 3: PROCESAR PESAJE (POST)
# ========================================================
@bp_gestion_carga.route('/registrar_pesaje', methods=['POST'])
@login_required_custom
def procesar_pesaje_avicola():
    id_empresa = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    usuario_registro = session.get('nombre')

    tipo_vehiculo = request.form.get('tipo_vehiculo')
    tipo_carga = request.form.get('tipo_carga')
    auxiliar = request.form.get('auxiliar', 'N/A')
    peso_bascula = float(request.form.get('peso_bascula', 0))

    # Lógica de Canastas vs Alimento
    if tipo_carga == 'alimento':
        total_canastas = 0
        peso_unitario = 0.0
    else:
        total_canastas = int(request.form.get('total_canastas', 0))
        peso_unitario = float(request.form.get('peso_unitario_canasta', 2.0))

    rutas_seleccionadas = request.form.getlist('rutas_array[]')
    rutas_json = json.dumps(rutas_seleccionadas, ensure_ascii=False)

    remisiones_numeros = request.form.getlist('remision_numero[]')
    remisiones_pesos = request.form.getlist('remision_peso[]')

    tara_camion = 0.0
    capacidad_camion = 0.0
    conductor = ""
    placa = ""
    fecha_actual = datetime.now()

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    try:
        if tipo_vehiculo == 'propio':
            placa = str(request.form.get('placa_propio', '')).upper().strip()
            conductor = request.form.get('conductor_propio', '')

            cur.execute("SELECT peso_vacio, `capacidad (kg)` FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, id_empresa))
            veh_data = cur.fetchone()
            if not veh_data:
                flash(f"Error: Placa {placa} no encontrada.", "danger")
                return redirect(url_for('bp_gestion_carga.panel_bascula'))
            
            tara_camion = float(veh_data['peso_vacio'])
            capacidad_camion = float(veh_data['capacidad (kg)'])
        else: 
            placa = str(request.form.get('placa_tercero', '')).upper().strip()
            tara_camion = float(request.form.get('tara_tercero', 0))
            capacidad_camion = float(request.form.get('capacidad_tercero', 0))
            conductor = request.form.get('conductor_tercero', '')

        # Generar Consecutivo
        fecha_str = fecha_actual.strftime("%Y%m%d")
        cur.execute("SELECT COUNT(*) AS total FROM pesajes_producto_avicola WHERE placa = %s AND id_empresa = %s AND tipo_registro = 'cierre_pesaje' AND DATE(fecha_hora) = CURDATE()", (placa, id_empresa))
        num_viaje = int(cur.fetchone()['total']) + 1
        consecutivo = f"Viaje-{placa}-{fecha_str}-{num_viaje:02d}"

        # Grabar Remisiones
        kg_remisiones_total = 0.0
        for i in range(len(remisiones_numeros)):
            p_rem = float(remisiones_pesos[i])
            kg_remisiones_total += p_rem
            cur.execute("""
                INSERT INTO pesajes_producto_avicola 
                (id_empresa, empresa, fecha_hora, consecutivo_viaje, tipo_registro, tipo_vehiculo, tipo_carga, placa, numero_remision, kg_remision, registro)
                VALUES (%s, %s, %s, %s, 'remision', %s, %s, %s, %s, %s, %s)
            """, (id_empresa, empresa_nombre, fecha_actual, consecutivo, tipo_vehiculo, tipo_carga, placa, remisiones_numeros[i], p_rem, usuario_registro))

        # Ecuaciones
        peso_canastas_total = total_canastas * peso_unitario
        kg_pesados_neto = peso_bascula - tara_camion - peso_canastas_total
        
        if kg_pesados_neto < 0:
            mysql.connection.rollback()
            flash("Error Operativo: El cálculo de peso neto arrojó un valor negativo. Revise la tara y báscula.", "danger")
            return redirect(url_for('bp_gestion_carga.panel_bascula'))

        diferencia_abs = abs(kg_pesados_neto - kg_remisiones_total)
        porcentaje_diferencia = (diferencia_abs / kg_remisiones_total * 100) if kg_remisiones_total > 0 else 0
        capacidad_usada = (kg_pesados_neto / capacidad_camion * 100) if capacidad_camion > 0 else 0

        aprobado = 'si' if porcentaje_diferencia <= 1.5 else 'rev-despachos'

        # Grabar Cierre
        cur.execute("""
            INSERT INTO pesajes_producto_avicola 
            (id_empresa, empresa, fecha_hora, consecutivo_viaje, tipo_registro, tipo_vehiculo, tipo_carga, placa, rutas_y_kilos, 
             conductor, auxiliar, total_canastas, peso_unitario_canasta, peso_canastas, kg_pesados, porcentaje_diferencia, 
             capacidad_usada, aprobado, registro)
            VALUES (%s, %s, %s, %s, 'cierre_pesaje', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_empresa, empresa_nombre, fecha_actual, consecutivo, tipo_vehiculo, tipo_carga, placa, rutas_json, conductor, auxiliar,
              total_canastas, peso_unitario, peso_canastas_total, kg_pesados_neto, round(porcentaje_diferencia, 2), round(capacidad_usada, 2), aprobado, usuario_registro))

        mysql.connection.commit()

        if aprobado == 'si':
            flash(f"✅ Pesaje {consecutivo} registrado. Diferencia {round(porcentaje_diferencia, 2)}%.", "success")
            return redirect(url_for('bp_gestion_carga.panel_bascula', descargar_id=consecutivo))
        else:
            flash(f"⚠️ Alerta en Viaje {consecutivo}: Desfase del {round(porcentaje_diferencia, 2)}%. Bloqueado para despacho.", "warning")
            return redirect(url_for('bp_gestion_carga.panel_bascula'))

    except Exception as e:
        mysql.connection.rollback()
        flash(f"Error interno: {str(e)}", "danger")
        return redirect(url_for('bp_gestion_carga.panel_bascula'))
    finally:
        cur.close()

# ========================================================
# RUTA 4: REPORTES Y MÉTRICAS (ACTUALIZADO CON CAJA_DE_CARGA)
# ========================================================
@bp_gestion_carga.route('/reportes', methods=['GET'])
@login_required_custom
def reportes_bascula():
    id_empresa = session.get('empresa_id')

    # Parámetros de Filtro
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    tipo_flota = request.args.get('tipo_flota', 'todas')
    tipo_carga = request.args.get('tipo_carga', 'todas')
    placa_filtro = request.args.get('placa', 'todas')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    # 1. Obtener placas históricas para el filtro
    cur.execute("SELECT DISTINCT placa FROM pesajes_producto_avicola WHERE id_empresa = %s ORDER BY placa", (id_empresa,))
    placas_historicas = cur.fetchall()

    # 2. Construcción de la Query Dinámica para Cierres
    query = """
        SELECT c.*, 
               (SELECT SUM(kg_remision) FROM pesajes_producto_avicola r WHERE r.consecutivo_viaje = c.consecutivo_viaje AND r.tipo_registro = 'remision') as total_teorico
        FROM pesajes_producto_avicola c
        WHERE c.id_empresa = %s AND c.tipo_registro = 'cierre_pesaje' 
          AND DATE(c.fecha_hora) BETWEEN %s AND %s
    """
    params = [id_empresa, fecha_inicio, fecha_fin]

    if tipo_flota != 'todas':
        query += " AND c.tipo_vehiculo = %s"
        params.append(tipo_flota)
    if tipo_carga != 'todas':
        query += " AND c.tipo_carga = %s"
        params.append(tipo_carga)
    if placa_filtro != 'todas':
        query += " AND c.placa = %s"
        params.append(placa_filtro)

    query += " ORDER BY c.fecha_hora DESC"
    
    cur.execute(query, tuple(params))
    viajes = cur.fetchall()

    # 3. Consulta de Eficiencia de Carga (SOLO FLOTA PROPIA, CORREGIDO A caja_de_carga)
    query_ef = """
        SELECT p.placa, v.caja_de_carga, AVG(p.capacidad_usada) as prom_cap_usada
        FROM pesajes_producto_avicola p
        JOIN vehiculos v ON p.placa = v.placa AND p.id_empresa = v.id_empresa
        WHERE p.id_empresa = %s AND p.tipo_registro = 'cierre_pesaje' AND p.tipo_vehiculo = 'propio'
          AND DATE(p.fecha_hora) BETWEEN %s AND %s
    """
    params_ef = [id_empresa, fecha_inicio, fecha_fin]
    if tipo_carga != 'todas':
        query_ef += " AND p.tipo_carga = %s"
        params_ef.append(tipo_carga)
    if placa_filtro != 'todas':
        query_ef += " AND p.placa = %s"
        params_ef.append(placa_filtro)
        
    query_ef += " GROUP BY p.placa, v.caja_de_carga ORDER BY prom_cap_usada DESC"
    cur.execute(query_ef, tuple(params_ef))
    data_eficiencia = cur.fetchall()
    cur.close()

    # 4. Calcular KPIs Consolidados
    kpis = {
        'total_viajes': len(viajes),
        'kg_pollo_pie': 0,
        'kg_pollo_canal': 0,
        'kg_alimento': 0,
        'viajes_con_desviacion': 0
    }

    for v in viajes:
        peso_neto = float(v['kg_pesados'] or 0)
        if v['tipo_carga'] == 'pollo_pie': kpis['kg_pollo_pie'] += peso_neto
        elif v['tipo_carga'] == 'pollo_canal': kpis['kg_pollo_canal'] += peso_neto
        elif v['tipo_carga'] == 'alimento': kpis['kg_alimento'] += peso_neto
        
        if v['aprobado'] == 'rev-despachos':
            kpis['viajes_con_desviacion'] += 1

    # 5. Procesar Datos de Eficiencia Propia
    eficiencia = {
        'global': 0.0,
        'por_tipo': {},
        'por_placa': []
    }
    
    if data_eficiencia:
        suma_total = 0
        conteo_tipos = {}
        
        for e in data_eficiencia:
            promedio_placa = float(e['prom_cap_usada'] or 0)
            tipo_veh = str(e['caja_de_carga']).capitalize()
            
            # Ranking por Placa
            eficiencia['por_placa'].append({'placa': e['placa'], 'promedio': promedio_placa, 'tipo': tipo_veh})
            suma_total += promedio_placa
            
            # Agrupación por Tipo
            if tipo_veh not in eficiencia['por_tipo']:
                eficiencia['por_tipo'][tipo_veh] = {'suma': 0, 'conteo': 0}
            eficiencia['por_tipo'][tipo_veh]['suma'] += promedio_placa
            eficiencia['por_tipo'][tipo_veh]['conteo'] += 1
            
        eficiencia['global'] = round(suma_total / len(data_eficiencia), 1)
        
        # Calcular promedio final por tipo
        for tipo, valores in eficiencia['por_tipo'].items():
            eficiencia['por_tipo'][tipo] = round(valores['suma'] / valores['conteo'], 1)

    return render_template(
        'B_pesaje_carga_avicola.html',
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        nit=session.get('nit'),
        active_module='reportes',
        placas_historicas=placas_historicas,
        viajes=viajes,
        kpis=kpis,
        eficiencia=eficiencia,
        filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'tipo_flota': tipo_flota, 'tipo_carga': tipo_carga, 'placa': placa_filtro}
    )
# ========================================================
# RUTA 5: DESCARGAR MANIFIESTO EN PDF
# ========================================================
@bp_gestion_carga.route('/descargar_manifiesto/<consecutivo>', methods=['GET'])
@login_required_custom
def generar_manifiesto_pdf(consecutivo):
    id_empresa = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM pesajes_producto_avicola WHERE consecutivo_viaje = %s AND id_empresa = %s", (consecutivo, id_empresa))
    filas = cur.fetchall()
    cur.close()

    if not filas: return "Error: No se encontraron datos.", 404

    cierre = None
    remisiones = []
    kg_remisiones_total = 0.0

    for fila in filas:
        if fila['tipo_registro'] == 'cierre_pesaje': cierre = fila
        elif fila['tipo_registro'] == 'remision':
            remisiones.append(fila)
            kg_remisiones_total += float(fila['kg_remision'])

    if not cierre: return "Error: Datos incompletos.", 500

    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=18, leading=22, textColor=colors.HexColor('#015249'), alignment=1)
    subtitle_style = ParagraphStyle('DocSub', parent=styles['Normal'], fontSize=10, leading=14, textColor=colors.HexColor('#6b7280'), alignment=1)
    cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=9, leading=12)
    cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=9, leading=12, fontName='Helvetica-Bold')
    
    story.append(Paragraph("<b>BQA ONE - MANIFIESTO DE CARGA</b>", title_style))
    story.append(Paragraph(f"Empresa: {empresa_nombre} | NIT: {session.get('nit', 'N/A')}", subtitle_style))
    story.append(Spacer(1, 15))
    
    rutas_lista = json.loads(cierre['rutas_y_kilos']) if cierre['rutas_y_kilos'] else []
    
    # Formatear Tipo de Carga
    tc_format = cierre['tipo_carga'].replace('_', ' ').capitalize()

    meta_data = [
        [Paragraph("<b>Viaje:</b>", cell_style), Paragraph(consecutivo, cell_bold), Paragraph("<b>Fecha:</b>", cell_style), Paragraph(cierre['fecha_hora'].strftime('%Y-%m-%d %H:%M'), cell_style)],
        [Paragraph("<b>Placa:</b>", cell_style), Paragraph(cierre['placa'], cell_bold), Paragraph("<b>Carga:</b>", cell_style), Paragraph(tc_format, cell_bold)],
        [Paragraph("<b>Conductor:</b>", cell_style), Paragraph(cierre['conductor'], cell_style), Paragraph("<b>Auxiliar:</b>", cell_style), Paragraph(cierre['auxiliar'], cell_style)],
        [Paragraph("<b>Rutas:</b>", cell_style), Paragraph(", ".join(rutas_lista), cell_style), Paragraph("<b>Operador:</b>", cell_style), Paragraph(cierre['registro'], cell_style)]
    ]
    t_meta = Table(meta_data, colWidths=[100, 160, 100, 170])
    t_meta.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f9fafb')),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('TOPPADDING', (0,0), (-1,-1), 6), ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(t_meta)
    story.append(Spacer(1, 15))
    
    balanza_data = [
        [Paragraph("<b>Peso Neto Mercancía:</b>", cell_style), Paragraph(f"<b>{float(cierre['kg_pesados']):,.1f} Kg</b>", cell_bold)],
        [Paragraph("<b>Declarado Remisiones:</b>", cell_style), Paragraph(f"{kg_remisiones_total:,.1f} Kg", cell_style)],
        [Paragraph("<b>Diferencia:</b>", cell_style), Paragraph(f"{float(cierre['porcentaje_diferencia'])} %", cell_bold)]
    ]
    if cierre['tipo_carga'] != 'alimento':
        balanza_data.append([Paragraph("<b>Total Empaques:</b>", cell_style), Paragraph(str(cierre['total_canastas']), cell_style)])

    t_bal = Table(balanza_data, colWidths=[150, 150])
    story.append(t_bal)

    doc.build(story)
    buffer.seek(0)
    
    return send_file(buffer, as_attachment=True, download_name=f"Manifiesto_{consecutivo}.pdf", mimetype='application/pdf')

# ========================================================
# RUTA 6: GENERAR REPORTE GLOBAL EN PDF (ACTUALIZADO CON CAJA_DE_CARGA)
# ========================================================
@bp_gestion_carga.route('/descargar_reporte_pdf', methods=['GET'])
@login_required_custom
def descargar_reporte_pdf():
    id_empresa = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    # Capturar mismos filtros
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    tipo_flota = request.args.get('tipo_flota', 'todas')
    tipo_carga = request.args.get('tipo_carga', 'todas')
    placa_filtro = request.args.get('placa', 'todas')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    # 1. Consultar Viajes
    query = """
        SELECT c.*, 
               (SELECT SUM(kg_remision) FROM pesajes_producto_avicola r WHERE r.consecutivo_viaje = c.consecutivo_viaje AND r.tipo_registro = 'remision') as total_teorico
        FROM pesajes_producto_avicola c
        WHERE c.id_empresa = %s AND c.tipo_registro = 'cierre_pesaje' 
          AND DATE(c.fecha_hora) BETWEEN %s AND %s
    """
    params = [id_empresa, fecha_inicio, fecha_fin]
    if tipo_flota != 'todas':
        query += " AND c.tipo_vehiculo = %s"
        params.append(tipo_flota)
    if tipo_carga != 'todas':
        query += " AND c.tipo_carga = %s"
        params.append(tipo_carga)
    if placa_filtro != 'todas':
        query += " AND c.placa = %s"
        params.append(placa_filtro)
    query += " ORDER BY c.fecha_hora DESC"
    
    cur.execute(query, tuple(params))
    viajes = cur.fetchall()

    # 2. Consultar Eficiencia (Solo si aplica)
    data_eficiencia = []
    if tipo_flota in ['todas', 'propio']:
        query_ef = """
            SELECT p.placa, v.caja_de_carga, AVG(p.capacidad_usada) as prom_cap_usada
            FROM pesajes_producto_avicola p
            JOIN vehiculos v ON p.placa = v.placa AND p.id_empresa = v.id_empresa
            WHERE p.id_empresa = %s AND p.tipo_registro = 'cierre_pesaje' AND p.tipo_vehiculo = 'propio'
              AND DATE(p.fecha_hora) BETWEEN %s AND %s
        """
        params_ef = [id_empresa, fecha_inicio, fecha_fin]
        if tipo_carga != 'todas':
            query_ef += " AND p.tipo_carga = %s"
            params_ef.append(tipo_carga)
        if placa_filtro != 'todas':
            query_ef += " AND p.placa = %s"
            params_ef.append(placa_filtro)
            
        query_ef += " GROUP BY p.placa, v.caja_de_carga ORDER BY prom_cap_usada DESC"
        cur.execute(query_ef, tuple(params_ef))
        data_eficiencia = cur.fetchall()

    cur.close()

    # Construcción del PDF
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=16, textColor=colors.HexColor('#015249'), alignment=1)
    sub_style = ParagraphStyle('DocSub', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#6b7280'), alignment=1)
    cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=8)
    
    story.append(Paragraph("<b>REPORTE AUDITORÍA BÁSCULA - BQA ONE</b>", title_style))
    story.append(Paragraph(f"Empresa: {empresa_nombre} | Periodo: {fecha_inicio} al {fecha_fin}", sub_style))
    story.append(Spacer(1, 15))

    # Tabla de Eficiencia (Solo Propia)
    if data_eficiencia:
        story.append(Paragraph("<b>Eficiencia de Ocupación (Flota Propia)</b>", ParagraphStyle('H2', fontSize=10, textColor=colors.HexColor('#015249'))))
        story.append(Spacer(1, 5))
        eff_data = [["Placa", "Tipo Vehículo", "Promedio Capacidad Usada (%)"]]
        for e in data_eficiencia:
            eff_data.append([e['placa'], str(e['caja_de_carga']).capitalize(), f"{float(e['prom_cap_usada'] or 0):.1f}%"])
        
        t_eff = Table(eff_data, colWidths=[100, 150, 150])
        t_eff.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0,0), (-1,0), 8),
            ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ]))
        story.append(t_eff)
        story.append(Spacer(1, 15))

    # Tabla de Viajes
    story.append(Paragraph("<b>Detalle de Viajes</b>", ParagraphStyle('H2', fontSize=10, textColor=colors.HexColor('#015249'))))
    story.append(Spacer(1, 5))
    viajes_data = [["Fecha", "Viaje", "Placa", "Carga", "Teórico (Kg)", "Báscula (Kg)", "Desv %"]]
    
    for v in viajes:
        viajes_data.append([
            v['fecha_hora'].strftime('%d/%m/%Y'),
            Paragraph(v['consecutivo_viaje'], cell_style),
            v['placa'],
            v['tipo_carga'].replace('_', ' ').capitalize(),
            f"{float(v['total_teorico'] or 0):,.1f}",
            f"{float(v['kg_pesados'] or 0):,.1f}",
            f"{float(v['porcentaje_diferencia'] or 0):.1f}%"
        ])
    
    t_viajes = Table(viajes_data, colWidths=[60, 160, 50, 80, 60, 70, 50])
    t_viajes.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')),
        ('TEXTCOLOR', (0,0), (-1,0), colors.white),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,-1), 8),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('ALIGN', (4,1), (6,-1), 'RIGHT'), # Alinear números a la derecha
    ]))
    story.append(t_viajes)

    doc.build(story)
    buffer.seek(0)
    
    return send_file(buffer, as_attachment=True, download_name=f"Reporte_Bascula_{fecha_inicio}_al_{fecha_fin}.pdf", mimetype='application/pdf')

# ========================================================
# RUTA 7: CRON - REPORTE DIARIO 8:00 AM (ACTUALIZADO CON CAJA_DE_CARGA)
# ========================================================
@bp_gestion_carga.route('/cron/reporte_diario_logistica', methods=['GET'])
@csrf.exempt
def cron_reporte_diario_logistica():
    # Solo permite acceso mediante un token de seguridad para evitar que lo corran externos
    if request.args.get('token') != 'BQA_CRON_2026':
        return jsonify({"success": False, "message": "No autorizado"}), 403

    ayer = datetime.now() - timedelta(days=1)
    fecha_ayer_str = ayer.strftime('%Y-%m-%d')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # 1. Buscar qué empresas tuvieron movimiento de flota propia ayer
    cur.execute("""
        SELECT DISTINCT id_empresa, empresa 
        FROM pesajes_producto_avicola 
        WHERE DATE(fecha_hora) = %s AND tipo_vehiculo = 'propio' AND tipo_registro = 'cierre_pesaje'
    """, (fecha_ayer_str,))
    empresas_activas = cur.fetchall()

    for emp in empresas_activas:
        id_empresa = emp['id_empresa']
        nombre_empresa = emp['empresa']

        # 2. Calcular eficiencia por placa de ESE día para ESA empresa
        cur.execute("""
            SELECT p.placa, v.caja_de_carga, AVG(p.capacidad_usada) as prom_cap_usada, COUNT(*) as total_viajes
            FROM pesajes_producto_avicola p
            JOIN vehiculos v ON p.placa = v.placa AND p.id_empresa = v.id_empresa
            WHERE p.id_empresa = %s AND DATE(p.fecha_hora) = %s AND p.tipo_vehiculo = 'propio' AND p.tipo_registro = 'cierre_pesaje'
            GROUP BY p.placa, v.caja_de_carga
            ORDER BY prom_cap_usada DESC
        """, (id_empresa, fecha_ayer_str))
        resultados = cur.fetchall()

        if not resultados: continue

        # 3. Buscar destinatarios de Logística
        cur.execute("""
            SELECT email FROM contactos 
            WHERE empresa = %s AND LOWER(area_contacto) = 'logistica' AND email IS NOT NULL
        """, (nombre_empresa,))
        contactos = cur.fetchall()
        destinatarios = [c['email'] for c in contactos if c['email']]

        if not destinatarios: continue

        # 4. Construir HTML del correo
        filas_html = ""
        for r in resultados:
            eff = float(r['prom_cap_usada'] or 0)
            color = "#16a34a" if eff >= 80 else ("#f59e0b" if eff >= 60 else "#dc2626")
            filas_html += f"""
                <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 10px;"><strong>{r['placa']}</strong> <span style="color:#666; font-size:11px;">({str(r['caja_de_carga']).capitalize()})</span></td>
                    <td style="padding: 10px; text-align:center;">{r['total_viajes']}</td>
                    <td style="padding: 10px; text-align:right; font-weight:bold; color:{color};">{eff:.1f}%</td>
                </tr>
            """

        cuerpo_correo = f"""
        <!DOCTYPE html>
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.1);">
                <div style="background-color: #015249; color: white; padding: 20px; text-align: center;">
                    <h2 style="margin: 0;">Reporte Diario de Ocupación Logística</h2>
                    <p style="margin: 5px 0 0 0; font-size: 14px; opacity: 0.9;">Operación del {ayer.strftime('%d/%m/%Y')}</p>
                </div>
                <div style="padding: 25px;">
                    <p style="color: #333;">Estimado equipo de Logística de <strong>{nombre_empresa}</strong>,</p>
                    <p style="color: #555; line-height: 1.5;">A continuación, se presenta el resumen de eficiencia de ocupación de la flota propia despachada en la jornada anterior:</p>
                    
                    <table style="width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 14px;">
                        <thead>
                            <tr style="background-color: #f8f9fa;">
                                <th style="padding: 12px; text-align: left; color: #666; border-bottom: 2px solid #ddd;">Vehículo</th>
                                <th style="padding: 12px; text-align: center; color: #666; border-bottom: 2px solid #ddd;">Viajes</th>
                                <th style="padding: 12px; text-align: right; color: #666; border-bottom: 2px solid #ddd;">% Ocupación</th>
                            </tr>
                        </thead>
                        <tbody>
                            {filas_html}
                        </tbody>
                    </table>
                    
                    <div style="margin-top: 25px; padding: 12px; background-color: #f0fdf4; border-left: 4px solid #16a34a; font-size: 12px; color: #14532d;">
                        <strong>Nota HSEQ:</strong> Optimizar la capacidad de carga reduce costos operativos y huella de carbono. Valores por debajo del 80% pueden requerir revisión de rutas.
                    </div>
                </div>
                <div style="background-color: #f8f9fa; padding: 15px; text-align: center; border-top: 1px solid #eee;">
                    <p style="margin: 0; color: #999; font-size: 11px;">Enviado automáticamente por <strong>BQA-ONE System</strong></p>
                </div>
            </div>
        </body>
        </html>
        """

        # 5. Enviar el Correo
        email_user = os.environ.get("EMAIL_USER")
        email_pass = os.environ.get("EMAIL_PASS")
        email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
        email_port = int(os.environ.get("EMAIL_PORT", "587"))

        if email_user and email_pass:
            try:
                msg = MIMEMultipart()
                msg["Subject"] = f"📊 Reporte Logística: {nombre_empresa} - {ayer.strftime('%d/%m/%Y')}"
                msg["From"] = email_user
                msg["To"] = ", ".join(destinatarios)
                msg.attach(MIMEText(cuerpo_correo, "html", "utf-8"))

                with smtplib.SMTP(email_host, email_port) as server:
                    server.starttls()
                    server.login(email_user, email_pass)
                    server.sendmail(email_user, destinatarios, msg.as_string())
            except Exception as e:
                print(f"Error enviando correo a {nombre_empresa}: {str(e)}")

    cur.close()
    return jsonify({"success": True, "message": "Cron ejecutado. Correos enviados."})