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

bp_gestion_carga = Blueprint('bp_gestion_carga', __name__, url_prefix='/bqa_bascula')

# ========================================================
# RUTA 1: CARGA DE LA INTERFAZ Y DATOS DINÁMICOS (GET)
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

    # 1. Consultar Vehículos Propios
    cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (id_empresa,))
    vehiculos = cur.fetchall()

    # 2. Consultar Auxiliares
    cur.execute("""
        SELECT nombre FROM usuarios 
        WHERE empresa_id = %s AND perfil = 'auxiliar_transportecarga' 
        ORDER BY nombre ASC
    """, (id_empresa,))
    auxiliares = cur.fetchall()

    # 3. Consultar Rutas
    cur.execute("SELECT id, nombre_ruta, tipo_ruta FROM rutas WHERE id_empresa = %s ORDER BY nombre_ruta ASC", (id_empresa,))
    rutas = cur.fetchall()

    # 4. Consultar Conductores Propios
    cur.execute("""
        SELECT nombre FROM usuarios 
        WHERE empresa_id = %s AND perfil = 'operador_transportecarga' 
        ORDER BY nombre ASC
    """, (id_empresa,))
    conductores = cur.fetchall()

    cur.close()

    return render_template(
        'B_gestion_carga.html',
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        nit=session.get('nit'),
        vehiculos=vehiculos,
        auxiliares=auxiliares,
        rutas=rutas,
        conductores=conductores
    )

# ========================================================
# RUTA 2: API AJAX - FILTRO DE CUMPLIMIENTO PREOPERACIONAL
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
        return jsonify({
            'valido': False, 
            'mensaje': f'El conductor {conductor} no ha registrado ninguna inspección diaria obligatoria para el vehículo {placa} en las últimas 24 horas.'
        })
    
    return jsonify({
        'valido': True, 
        'mensaje': f'✅ Control de Cumplimiento Exitoso: Inspección preoperacional registrada por {conductor} hoy de forma correcta. Formulario liberado.'
    })

# ========================================================
# RUTA 3: PROCESAMIENTO MATEMÁTICO Y GUARDADO FINAL (POST)
# ========================================================
@bp_gestion_carga.route('/registrar_pesaje', methods=['POST'])
@login_required_custom
def procesar_pesaje_avicola():
    id_empresa = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    usuario_registro = session.get('nombre')

    tipo_vehiculo = request.form.get('tipo_vehiculo')
    auxiliar = request.form.get('auxiliar', 'N/A')
    total_canastas = int(request.form.get('total_canastas', 0))
    peso_unitario = float(request.form.get('peso_unitario_canasta', 2.0))
    peso_bascula = float(request.form.get('peso_bascula', 0))

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
                flash(f"Error: Placa {placa} no encontrada en la base de datos.", "danger")
                return redirect(url_for('bp_gestion_carga.panel_bascula'))
            
            tara_camion = float(veh_data['peso_vacio'])
            capacidad_camion = float(veh_data['capacidad (kg)'])

            hace_24h = fecha_actual - timedelta(days=1)
            hace_24h_str = hace_24h.strftime('%Y-%m-%d %H:%M:%S')
            cur.execute("""
                SELECT 1 FROM inspeccion_preoperacional_carga 
                WHERE placa_vehiculo = %s AND nombre_conductor = %s AND id_empresa = %s 
                  AND CONCAT(fecha_inspeccion, ' ', hora_inspeccion) >= %s 
                LIMIT 1
            """, (placa, conductor, id_empresa, hace_24h_str))
            
            if not cur.fetchone():
                flash(f"⛔ Bloqueo Backend: El conductor {conductor} no registra el cumplimiento del preoperacional obligatorio.", "danger")
                return redirect(url_for('bp_gestion_carga.panel_bascula'))

        else: # Terceros
            placa = str(request.form.get('placa_tercero', '')).upper().strip()
            tara_camion = float(request.form.get('tara_tercero', 0))
            capacidad_camion = float(request.form.get('capacidad_tercero', 0))
            conductor = request.form.get('conductor_tercero', '')

        # Generar Consecutivo de Viaje
        fecha_str = fecha_actual.strftime("%Y%m%d")
        cur.execute("""
            SELECT COUNT(*) AS total FROM pesajes_producto_avicola 
            WHERE placa = %s AND id_empresa = %s AND tipo_registro = 'cierre_pesaje' AND DATE(fecha_hora) = CURDATE()
        """, (placa, id_empresa))
        conteo = cur.fetchone()
        num_viaje = int(conteo['total']) + 1
        consecutivo = f"Viaje-{placa}-{fecha_str}-{num_viaje:02d}"

        # 1. Grabar Remisiones una a una
        kg_remisiones_total = 0.0
        for i in range(len(remisiones_numeros)):
            n_rem = remisiones_numeros[i]
            p_rem = float(remisiones_pesos[i])
            kg_remisiones_total += p_rem
            
            cur.execute("""
                INSERT INTO pesajes_producto_avicola 
                (id_empresa, empresa, fecha_hora, consecutivo_viaje, tipo_registro, tipo_vehiculo, placa, numero_remision, kg_remision, registro)
                VALUES (%s, %s, %s, %s, 'remision', %s, %s, %s, %s, %s)
            """, (id_empresa, empresa_nombre, fecha_actual, consecutivo, tipo_vehiculo, placa, n_rem, p_rem, usuario_registro))

        # 2. Ecuaciones de Pesaje
        peso_canastas_total = total_canastas * peso_unitario
        kg_pesados_neto = peso_bascula - tara_camion - peso_canastas_total
        
        if kg_pesados_neto < 0:
            mysql.connection.rollback()
            flash("Error Operativo: El cálculo de peso neto arrojó un valor negativo. Revise los kilogramos de la báscula.", "danger")
            return redirect(url_for('bp_gestion_carga.panel_bascula'))

        diferencia_abs = abs(kg_pesados_neto - kg_remisiones_total)
        porcentaje_diferencia = (diferencia_abs / kg_remisiones_total * 100) if kg_remisiones_total > 0 else 0
        capacidad_usada = (kg_pesados_neto / capacidad_camion * 100) if capacidad_camion > 0 else 0

        # Tolerancia del 1.5%
        aprobado = 'si' if porcentaje_diferencia <= 1.5 else 'rev-despachos'

        # 3. Grabar Cierre de Operación - CORREGIDO: 'approved_status' cambiado por 'aprobado'
        cur.execute("""
            INSERT INTO pesajes_producto_avicola 
            (id_empresa, empresa, fecha_hora, consecutivo_viaje, tipo_registro, tipo_vehiculo, placa, rutas_y_kilos, 
             conductor, auxiliar, total_canastas, peso_unitario_canasta, peso_canastas, kg_pesados, porcentaje_diferencia, 
             capacidad_usada, aprobado, registro)
            VALUES (%s, %s, %s, %s, 'cierre_pesaje', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_empresa, empresa_nombre, fecha_actual, consecutivo, tipo_vehiculo, placa, rutas_json, conductor, auxiliar,
            total_canastas, peso_unitario, peso_canastas_total, kg_pesados_neto, round(porcentaje_diferencia, 2),
            round(capacidad_usada, 2), aprobado, usuario_registro
        ))

        mysql.connection.commit()

        if aprobado == 'si':
            flash(f"✅ Pesaje {consecutivo} registrado con éxito. Diferencia aceptada del {round(porcentaje_diferencia, 2)}%. El manifiesto se descargará automáticamente.", "success")
            return redirect(url_for('bp_gestion_carga.panel_bascula', descargar_id=consecutivo))
        else:
            flash(f"⚠️ Alerta Crítica en Viaje {consecutivo}: Desfase de báscula detectado del {round(porcentaje_diferencia, 2)}% (Supera el límite permitido del 1.5%). El vehículo queda bloqueado para despacho inmediato.", "warning")
            return redirect(url_for('bp_gestion_carga.panel_bascula'))

    except Exception as e:
        mysql.connection.rollback()
        flash(f"Error interno en base de datos: {str(e)}", "danger")
        return redirect(url_for('bp_gestion_carga.panel_bascula'))
    finally:
        cur.close()

# ========================================================
# RUTA 4: NUEVA RUTA EXCLUSIVA PARA DESCARGA DE PDF
# ========================================================
@bp_gestion_carga.route('/descargar_manifiesto/<consecutivo>', methods=['GET'])
@login_required_custom
def generar_manifiesto_pdf(consecutivo):
    id_empresa = session.get('empresa_id')
    empresa_nombre = session.get('empresa')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("""
        SELECT * FROM pesajes_producto_avicola 
        WHERE consecutivo_viaje = %s AND id_empresa = %s
    """, (consecutivo, id_empresa))
    
    filas = cur.fetchall()
    cur.close()

    if not filas:
        return "Error: No se encontraron datos para compilar el manifiesto solicitado.", 404

    cierre = None
    remisiones = []
    kg_remisiones_total = 0.0

    for fila in filas:
        if fila['tipo_registro'] == 'cierre_pesaje':
            cierre = fila
        elif fila['tipo_registro'] == 'remision':
            remisiones.append(fila)
            kg_remisiones_total += float(fila['kg_remision'])

    if not cierre:
        return "Error: Datos de báscula incompletos en el servidor.", 500

    # Inicialización del documento ReportLab
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=40, leftMargin=40, topMargin=40, bottomMargin=40)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=18, leading=22, textColor=colors.HexColor('#015249'), alignment=1)
    subtitle_style = ParagraphStyle('DocSub', parent=styles['Normal'], fontSize=10, leading=14, textColor=colors.HexColor('#6b7280'), alignment=1)
    header_style = ParagraphStyle('BlockHeader', parent=styles['Heading3'], fontSize=11, leading=15, textColor=colors.HexColor('#015249'), spaceBefore=12, spaceAfter=6)
    cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=9, leading=12)
    cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=9, leading=12, fontName='Helvetica-Bold')
    cell_header = ParagraphStyle('CellHeader', parent=styles['Normal'], fontSize=9, leading=12, fontName='Helvetica-Bold', textColor=colors.white)
    
    story.append(Paragraph("<b>BQA ONE - MANIFIESTO DE CARGA Y PESAJE</b>", title_style))
    story.append(Paragraph(f"Empresa: {empresa_nombre} | NIT: {session.get('nit', 'N/A')}", subtitle_style))
    story.append(Spacer(1, 15))
    
    rutas_lista = json.loads(cierre['rutas_y_kilos']) if cierre['rutas_y_kilos'] else []

    meta_data = [
        [Paragraph("<b>Consecutivo Viaje:</b>", cell_style), Paragraph(consecutivo, cell_bold), Paragraph("<b>Fecha / Hora:</b>", cell_style), Paragraph(cierre['fecha_hora'].strftime('%Y-%m-%d %H:%M:%S'), cell_style)],
        [Paragraph("<b>Vehículo (Placa):</b>", cell_style), Paragraph(cierre['placa'], cell_bold), Paragraph("<b>Tipo Flota:</b>", cell_style), Paragraph(cierre['tipo_vehiculo'].capitalize(), cell_style)],
        [Paragraph("<b>Conductor:</b>", cell_style), Paragraph(cierre['conductor'], cell_style), Paragraph("<b>Auxiliar Asignado:</b>", cell_style), Paragraph(cierre['auxiliar'], cell_style)],
        [Paragraph("<b>Rutas:</b>", cell_style), Paragraph(", ".join(rutas_lista), cell_style), Paragraph("<b>Operador Báscula:</b>", cell_style), Paragraph(cierre['registro'], cell_style)]
    ]
    t_meta = Table(meta_data, colWidths=[110, 150, 110, 160])
    t_meta.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f9fafb')),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(t_meta)
    story.append(Spacer(1, 15))
    
    story.append(Paragraph("<b>DETALLE DE REMISIONES PROCESADAS</b>", header_style))
    rem_data = [[Paragraph("Índice", cell_header), Paragraph("Número de Remisión", cell_header), Paragraph("Peso Declaro (Kg)", cell_header)]]
    for idx, rem in enumerate(remisiones, start=1):
        rem_data.append([Paragraph(str(idx), cell_style), Paragraph(str(rem['numero_remision']), cell_style), Paragraph(f"{float(rem['kg_remision']):,.1f} Kg", cell_style)])
    
    rem_data.append([Paragraph("", cell_style), Paragraph("<b>TOTAL DECLARADO EN REMISIONES:</b>", cell_bold), Paragraph(f"<b>{kg_remisiones_total:,.1f} Kg</b>", cell_bold)])
    
    t_rem = Table(rem_data, colWidths=[50, 330, 150])
    t_rem.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')),
        ('BOTTOMPADDING', (0,0), (-1,0), 6),
        ('TOPPADDING', (0,0), (-1,0), 6),
        ('GRID', (0,0), (-1,-2), 0.5, colors.HexColor('#e5e7eb')),
        ('LINEBELOW', (0,-1), (-1,-1), 1.5, colors.HexColor('#015249')),
        ('TOPPADDING', (0,-1), (-1,-1), 6),
        ('BOTTOMPADDING', (0,-1), (-1,-1), 6),
    ]))
    story.append(t_rem)
    story.append(Spacer(1, 15))
    
    story.append(Paragraph("<b>AUDITORÍA DE PESOS EN BÁSCULA</b>", header_style))
    balanza_data = [
        [Paragraph("<b>Peso Bruto Registrado:</b>", cell_style), Paragraph(f"{float(cierre['kg_pesados']) + float(cierre['peso_canastas']):,.1f} Kg", cell_style), Paragraph("<b>Total Canastas:</b>", cell_style), Paragraph(str(cierre['total_canastas']), cell_style)],
        [Paragraph("<b>Peso Neto Mercancía:</b>", cell_style), Paragraph(f"<b>{float(cierre['kg_pesados']):,.1f} Kg</b>", cell_bold), Paragraph("<b>Peso Total Canastas:</b>", cell_style), Paragraph(f"{float(cierre['peso_canastas']):,.1f} Kg", cell_style)],
        [Paragraph("<b>Diferencia vs Remisiones:</b>", cell_style), Paragraph(f"{float(cierre['porcentaje_diferencia'])} %", cell_bold), Paragraph("<b>Capacidad de Carga Usada:</b>", cell_style), Paragraph(f"{float(cierre['capacidad_usada'])} %", cell_style)]
    ]
    t_bal = Table(balanza_data, colWidths=[140, 120, 140, 130])
    t_bal.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f9fafb')),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
    ]))
    story.append(t_bal)
    story.append(Spacer(1, 45))
    
    sig_data = [
        [Paragraph("___________________________________<br/><b>Firma del Conductor</b>", cell_style), 
         Paragraph("___________________________________<br/><b>Firma Operador Báscula</b>", cell_style)]
    ]
    t_sig = Table(sig_data, colWidths=[265, 265])
    t_sig.setStyle(TableStyle([
        ('ALIGN', (0,0), (-1,-1), 'CENTER'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(KeepTogether(t_sig))
    
    doc.build(story)
    buffer.seek(0)
    
    return send_file(buffer, as_attachment=True, download_name=f"Manifiesto_{consecutivo}.pdf", mimetype='application/pdf')