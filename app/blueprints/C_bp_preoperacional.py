# MÓDULO: TRANSPORTE | SUB-SUBMÓDULO: PREOPERACIONALES | CONDICIÓN: OBLIGATORIO
# app/blueprints/C_bp_preoperacional.py
import os
import io
import json
import time
import base64
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps

from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, send_file
from app import mysql
from app.utils import login_required_custom
from datetime import datetime, timedelta
import MySQLdb.cursors

# Librerías PDF (Reportes Platypus)
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch

bp_preoperacional = Blueprint('preoperacional', __name__)

# Configuraciones de Email para Alertas Críticas
EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.hostinger.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "465"))
EMAIL_USER = os.environ.get("EMAIL_USER", "bqa-one@baquia-esm.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

# ==============================================================================
# PERMISOS ESPECÍFICOS DE AUDITORÍA
# ==============================================================================
def auditor_preoperacional_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['gestor_flotacarga', 'controlador_transportecarga', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Flota para auditar.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# 1. RUTAS DEL OPERADOR (DILIGENCIAMIENTO MÓVIL)
# ==============================================================================

@bp_preoperacional.route('/preoperacional')
@login_required_custom
def preoperacional_tc():
    placa_carga = session.get("placa_prelogueada")
    placa_especial = session.get("placa_prelogueada_especial")
    placa = placa_carga or placa_especial

    if placa:
        empresa_id = session.get("empresa_id")
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        # CANDADO MULTIEMPRESA Y PREVENCIÓN DE DUPLICIDAD
        cur.execute("""
            SELECT 1 FROM inspeccion_preoperacional 
            WHERE id_empresa = %s AND placa_vehiculo = %s 
            AND fecha_inspeccion = CURDATE() AND vehiculo_aprobado = 1
            LIMIT 1
        """, (empresa_id, placa))
        
        if cur.fetchone():
            cur.close()
            flash("El vehículo ya cuenta con una inspección aprobada el día de hoy.", "info")
            if placa_carga:
                try: return redirect(url_for('router_universal', modulo='flota'))
                except: return redirect(url_for('flotacarga.dashboard_operador'))
            else:
                return redirect(url_for('operador_flotaespecial.dashboard_operador_especial'))

        # IDENTIFICAR SI ES CARGA O ESPECIAL MANTENIENDO EL LÍMITE MULTITENANT
        if placa_carga:
            cur.execute("SELECT * FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
            vehiculo = cur.fetchone()
        else:
            cur.execute("SELECT placa, clase as tipo, marca as referencia, estatus FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
            vehiculo = cur.fetchone()
            if vehiculo:
                vehiculo['tipo_vehiculo'] = vehiculo.get('tipo', 'Especial')
        
        if not vehiculo:
            session.pop("placa_prelogueada", None)
            session.pop("placa_prelogueada_especial", None)
            flash("Vehículo no válido o no pertenece a su empresa.", "danger")
            cur.close()
            return redirect(url_for('preoperacional.preoperacional_tc'))
            
        cur.execute("SELECT id, nombre_ruta FROM rutas WHERE id_empresa = %s ORDER BY nombre_ruta ASC", (empresa_id,))
        rutas_disponibles = cur.fetchall()
        cur.close()

        return render_template(
            'C_preoperacional_tc.html',
            vehiculo=vehiculo,
            rutas=rutas_disponibles,
            nombre_conductor=session.get('nombre'),
            nit=session.get('nit'),
            empresa=session.get('empresa'),
            vista_auditoria=False
        )
        
    return render_template(
        'C_preoperacional_tc.html', 
        scanner_mode=True,
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        vista_auditoria=False
    )


@bp_preoperacional.route('/preoperacional/validar_qr', methods=['POST'])
@login_required_custom
def validar_qr():
    data = request.get_json(silent=True) or {}
    placa = (data.get("placa") or "").upper().strip()
    qr_nit = str(data.get("nit") or "").strip()
    session_nit = str(session.get("empresa_id") or "").strip()

    if not placa or not qr_nit: 
        return jsonify(success=False, message="Datos de QR incompletos."), 400
    if qr_nit != session_nit: 
        return jsonify(success=False, message="Este vehículo pertenece a otra empresa."), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # Aislamiento Multi-Tenant
    cur.execute("SELECT placa, empresa, id_empresa, 'carga' as flota_tipo FROM vehiculos WHERE placa = %s AND id_empresa = %s LIMIT 1", (placa, session_nit))
    v = cur.fetchone()
    
    if not v:
        cur.execute("SELECT placa, id_empresa, 'especial' as flota_tipo FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s LIMIT 1", (placa, session_nit))
        v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="Vehículo no registrado en su empresa."), 404
        
    # Verificar si ya tiene inspección aprobada hoy
    cur.execute("""
        SELECT 1 FROM inspeccion_preoperacional 
        WHERE id_empresa = %s AND placa_vehiculo = %s 
        AND fecha_inspeccion = CURDATE() AND vehiculo_aprobado = 1
        LIMIT 1
    """, (session_nit, placa))
    inspeccion_hoy = cur.fetchone()

    nuevo_estatus = 'Logueado' if inspeccion_hoy else 'Prelogueado'

    if v["flota_tipo"] == 'carga':
        cur.execute("UPDATE vehiculos SET estatus=%s WHERE placa=%s AND id_empresa=%s", (nuevo_estatus, placa, session_nit))
        session["placa_prelogueada"] = placa
        
        if inspeccion_hoy:
            cur.execute("""
                INSERT INTO historial_sesiones_flota (id_empresa, id_usuario, placa_vehiculo, fecha_login, estado_sesion)
                VALUES (%s, %s, %s, NOW(), 'ACTIVA')
            """, (session_nit, session.get("usuario_id"), placa))
    else:
        cur.execute("UPDATE vehiculos_especial SET estatus=%s WHERE placa=%s AND id_empresa=%s", (nuevo_estatus, placa, session_nit))
        session["placa_prelogueada_especial"] = placa
        
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS historial_sesiones_flotaespecial (
                    id INT AUTO_INCREMENT PRIMARY KEY, id_empresa INT NOT NULL, id_usuario INT NOT NULL, placa_vehiculo VARCHAR(20), fecha_login DATETIME, fecha_logout_manual DATETIME, latitud DECIMAL(10, 8), longitud DECIMAL(11, 8), estado_sesion VARCHAR(20) DEFAULT 'ACTIVA', INDEX(id_empresa, id_usuario)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            cur.execute("""
                INSERT INTO historial_sesiones_flotaespecial (id_empresa, id_usuario, placa_vehiculo, fecha_login, estado_sesion)
                VALUES (%s, %s, %s, NOW(), 'ACTIVA')
            """, (session_nit, session.get("usuario_id"), placa))
        except: pass
        
    mysql.connection.commit()
    cur.close()

    return jsonify(success=True, message="Vehículo verificado.")


@bp_preoperacional.route('/preoperacional/guardar', methods=['POST'])
@login_required_custom
def guardar_inspeccion():
    placa_carga = session.get("placa_prelogueada")
    placa_especial = session.get("placa_prelogueada_especial")
    placa = placa_carga or placa_especial
    tipo_flota = 'carga' if placa_carga else 'especial'

    if not placa:
        flash("Acceso denegado. No hay vehículo enlazado.", "danger")
        return redirect(url_for('preoperacional.preoperacional_tc'))

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        empresa_id = session.get("empresa_id")
        empresa_nombre = session.get("empresa")
        usuario_id = session.get("usuario_id")
        
        detalles_novedades_json = request.form.get('detalles_novedades_json', '{}')
        try: json.loads(detalles_novedades_json) 
        except: detalles_novedades_json = '{}'

        estado_llantas_json = request.form.get('estado_llantas_json', '{}')
        try: llantas_dict = json.loads(estado_llantas_json)
        except: llantas_dict = {}; estado_llantas_json = '{}'
        
        foto_conductor_base64 = request.form.get('foto_conductor_base64')
        firma_grafica_base64 = request.form.get('firma_grafica_base64')
        clasificacion_runt = request.form.get('clasificacion_runt_vehiculo', '')
        
        latitud_raw = request.form.get('latitud', '')
        longitud_raw = request.form.get('longitud', '')
        latitud = float(latitud_raw) if latitud_raw.strip() else None
        longitud = float(longitud_raw) if longitud_raw.strip() else None
        
        now = datetime.now()
        anio_actual = now.year
        fecha_inspeccion = now.date()
        hora_inspeccion = now.time().strftime("%H:%M:%S")

        # BLOQUEO PESIMISTA (FOR UPDATE) PARA ASEGURAR ATOMICIDAD DEL CONSECUTIVO
        if session.get('consecutivo_viaje'):
            consecutivo = session.get('consecutivo_viaje')
        else:
            cur.execute("""
                SELECT COUNT(*) as total FROM inspeccion_preoperacional 
                WHERE id_empresa = %s AND YEAR(fecha_inspeccion) = %s 
                FOR UPDATE
            """, (empresa_id, anio_actual))
            contador = cur.fetchone()['total'] + 1
            siglas = "".join([word[0] for word in empresa_nombre.split() if word.isalpha()])[:4].upper()
            consecutivo = f"{siglas}-{anio_actual}-{str(contador).zfill(5)}"

        def get_int(field_name, default=1):
            try: return int(request.form.get(field_name, default))
            except: return default

        novedades_rojas = []
        novedades_amarillas = []

        doc_values = {
            'doc_licencia_conduccion': 1, 'doc_soat_vigente': 1,
            'doc_tecnomecanica_vigente': 1, 'doc_tarjeta_operacion': 1
        }
        val_cedula = 1
        val_licencia_transito = 1

        fields_3_state = [
            'mec_nivel_aceite_motor', 'mec_liquido_frenos', 'mec_liquido_embrague', 'mec_nivel_refrigerante', 
            'mec_estado_correas', 'mec_ausencia_fugas', 'luc_altas_bajas', 'luc_frenos_stop', 'luc_direccionales', 
            'luc_parqueo_estacionarias', 'luc_reversa_alarma', 'luc_delimitadoras_cocuyos', 'lla_tuercas_pernos', 
            'lla_repuesto_operativa', 'lla_suspension_muelles', 'fre_pedal_firme', 'fre_parqueo_mano', 
            'fre_presion_aire_manometro', 'fre_juego_direccion', 'fre_pito_corneta', 'fre_limpiaparabrisas_plumillas', 
            'car_estado_estructura', 'car_compuertas_carpas_amarres', 'car_cinturones_seguridad', 'car_espejos_retrovisores', 
            'car_vidrio_parabrisas', 'equ_extintor_10lbs', 'equ_tacos_bloqueo', 'equ_senales_reflectivas', 'equ_gato_hidraulico', 
            'equ_cruceta_herramientas', 'equ_botiquin_completo'
        ]

        for f_name in fields_3_state:
            val = get_int(f_name)
            clean_name = (
                f_name.replace('mec_', '')
                .replace('luc_', '')
                .replace('lla_', '')
                .replace('fre_', '')
                .replace('car_', '')
                .replace('equ_', '')
                .replace('_', ' ')
                .title()
            )
            
            if val == 2: 
                novedades_amarillas.append(f"{clean_name}")
            elif val == 3: 
                novedades_rojas.append(f"{clean_name}")

        for pos_llanta, datos_llanta in llantas_dict.items():
            labrado = datos_llanta.get('labrado', 'operativa')
            nombre_legible = datos_llanta.get('nombre_legible', pos_llanta)
            if labrado == 'lisa': novedades_rojas.append(f"Falla Crítica: {nombre_legible} LISA.")
            elif labrado == 'baja': novedades_amarillas.append(f"Desgaste: {nombre_legible} BAJO.")

        observaciones = request.form.get('observaciones_hallazgos', '').strip()
        vehiculo_aprobado = 0 if len(novedades_rojas) > 0 else 1

        alerta_enviada = 0
        alerta_resumen = None
        alerta_dest = None

        if novedades_rojas or novedades_amarillas:
            cur.execute("SELECT email FROM contactos WHERE id_empresa = %s AND area_contacto IN ('logistica', 'talentohumano')", (empresa_id,))
            correos_destino = [c['email'] for c in cur.fetchall() if c.get('email')]
            if correos_destino and EMAIL_USER and EMAIL_PASS:
                try:
                    destinatarios_str = ", ".join(correos_destino)
                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = f"Alerta Preoperacional | {empresa_nombre} | Placa {placa}"
                    msg["From"] = EMAIL_FROM
                    msg["To"] = destinatarios_str 
                    
                    html_body = f"""
                    <!DOCTYPE html><html lang="es"><body>
                        <h2 style="color: #b91c1c;">⚠️ ALERTA DE SEGURIDAD VIAL</h2>
                        <p>Vehículo: {placa} | Conductor: {session.get('nombre')}</p>
                    </body></html>
                    """
                    msg.attach(MIMEText(html_body, "html"))
                    with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
                        server.login(EMAIL_USER, EMAIL_PASS)
                        server.send_message(msg)
                    alerta_enviada = 1; alerta_dest = destinatarios_str
                except: pass

        kilometraje = get_int('kilometraje', 0)
        ruta = 'No aplica'

        query = """
            INSERT INTO inspeccion_preoperacional (
                id_usuario_conductor, id_empresa, consecutivo_anual, fecha_inspeccion, hora_inspeccion, 
                nombre_conductor, placa_vehiculo, tipo_vehiculo, clasificacion_runt_vehiculo, kilometraje_inicial, ruta_destino, 
                doc_licencia_conduccion, fecha_vence_licencia, doc_soat_vigente, fecha_vence_soat,
                doc_tecnomecanica_vigente, fecha_vence_tecnomecanica, doc_tarjeta_operacion, fecha_vence_tarjeta_operacion,
                doc_cedula, doc_licencia_transito, 
                mec_nivel_aceite_motor, mec_liquido_frenos, mec_liquido_embrague, mec_nivel_refrigerante, 
                mec_estado_correas, mec_ausencia_fugas, luc_altas_bajas, luc_frenos_stop, luc_direccionales, 
                luc_parqueo_estacionarias, luc_reversa_alarma, luc_delimitadoras_cocuyos, 
                lla_tuercas_pernos, lla_repuesto_operativa, lla_suspension_muelles, fre_pedal_firme, fre_parqueo_mano, fre_presion_aire_manometro, 
                fre_juego_direccion, fre_pito_corneta, fre_limpiaparabrisas_plumillas, car_estado_estructura, 
                car_compuertas_carpas_amarres, car_cinturones_seguridad, car_espejos_retrovisores, car_vidrio_parabrisas, 
                equ_extintor_10lbs, equ_tacos_bloqueo, equ_senales_reflectivas, equ_gato_hidraulico, 
                equ_cruceta_herramientas, equ_botiquin_completo, observaciones_hallazgos, 
                detalles_novedades_json, estado_llantas_json, vehiculo_aprobado, 
                alerta_email_enviada, alerta_destinatario, alerta_resumen_novedades, firma_digital_conductor,
                foto_conductor_base64, firma_grafica_base64
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
        """
        params = (
            usuario_id, empresa_id, consecutivo, fecha_inspeccion, hora_inspeccion,
            session.get('nombre'), placa, request.form.get('tipo_vehiculo', 'NPR / Turbo'), clasificacion_runt, kilometraje, ruta,
            doc_values['doc_licencia_conduccion'], None, doc_values['doc_soat_vigente'], None, doc_values['doc_tecnomecanica_vigente'], None, doc_values['doc_tarjeta_operacion'], None, val_cedula, val_licencia_transito,
            get_int('mec_aceite_motor'), get_int('mec_liquido_frenos'), get_int('mec_liquido_embrague'), get_int('mec_refrigerante'), get_int('mec_correas'), get_int('mec_fugas'), get_int('luc_altas'), get_int('luc_frenos'), get_int('luc_direccionales'), get_int('luc_parqueo_estacionarias'), get_int('luc_reversa'), get_int('luc_cocuyos'),
            get_int('llan_tuercas'), get_int('llan_repuesto'), get_int('llan_muelles'), get_int('fren_pedal'), get_int('fren_mano'), get_int('fren_manometro'), get_int('fren_juego_direccion'), get_int('fre_pito_corneta'), get_int('fren_plumillas'),
            get_int('est_compuertas'), get_int('est_carpas'), get_int('est_cinturones'), get_int('est_espejos'), get_int('est_parabrisas'),
            get_int('eq_extintor'), get_int('eq_tacos'), get_int('eq_senales'), get_int('equ_gato_hidraulico'), get_int('eq_herramientas'), get_int('eq_botiquin'),
            observaciones, detalles_novedades_json, estado_llantas_json, vehiculo_aprobado, alerta_enviada, alerta_dest, alerta_resumen, f"HASH-AUDIT-{session.get('cedula', usuario_id)}",
            foto_conductor_base64, firma_grafica_base64
        )
        cur.execute(query, params)
        
        if tipo_flota == 'carga':
            cur.execute("UPDATE vehiculos SET estatus = 'Logueado' WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
            cur.execute("""
                INSERT INTO historial_sesiones_flota (id_empresa, id_usuario, placa_vehiculo, fecha_login, estado_sesion, latitud, longitud)
                VALUES (%s, %s, %s, NOW(), 'ACTIVA', %s, %s)
            """, (empresa_id, usuario_id, placa, latitud, longitud))
            mysql.connection.commit()
            flash(f"La inspección se ha registrado y auditado (Selfie + Firma). Consecutivo: {consecutivo}", "success")
            try: return redirect(url_for('router_universal', modulo='flota'))
            except: return redirect(url_for('flotacarga.dashboard_operador'))
        else:
            cur.execute("UPDATE vehiculos_especial SET estatus = 'Logueado' WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
            mysql.connection.commit()
            flash(f"La inspección se ha registrado y auditado exitosamente. Consecutivo: {consecutivo}", "success")
            return redirect(url_for('operador_flotaespecial.dashboard_operador_especial'))

    except Exception as e:
        mysql.connection.rollback()
        flash(f"Error interno: {str(e)}", "danger")
        return redirect(url_for('preoperacional.preoperacional_tc'))
    finally:
        cur.close()


# ==============================================================================
# 2. RUTAS DEL CONTROLADOR (AUDITORÍA MIGRADA)
# ==============================================================================

@bp_preoperacional.route('/preoperacionales/auditoria')
@login_required_custom
@auditor_preoperacional_required
def historial_preoperacionales():
    empresa_id = session.get('empresa_id')
    
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    placa_filtro = request.args.get('placa', 'todas')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

    cur.execute("SELECT DISTINCT placa FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_historicos = cur.fetchall()

    query = """
        SELECT id_inspeccion, consecutivo_anual, fecha_inspeccion, hora_inspeccion, 
               placa_vehiculo, nombre_conductor, vehiculo_aprobado 
        FROM inspeccion_preoperacional 
        WHERE id_empresa = %s AND fecha_inspeccion BETWEEN %s AND %s
    """
    params = [empresa_id, fecha_inicio, fecha_fin]
    
    if placa_filtro != 'todas':
        query += " AND placa_vehiculo = %s"
        params.append(placa_filtro)
        
    query += " ORDER BY fecha_inspeccion DESC, hora_inspeccion DESC"
    
    cur.execute(query, tuple(params))
    inspecciones = cur.fetchall()
    cur.close()

    return render_template(
        'C_preoperacional_tc.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        vista_auditoria=True,
        inspecciones=inspecciones,
        vehiculos_historicos=vehiculos_historicos,
        filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'placa': placa_filtro}
    )


@bp_preoperacional.route('/preoperacionales/pdf/<consecutivo>', methods=['GET'])
@login_required_custom
@auditor_preoperacional_required
def descargar_preoperacional_pdf(consecutivo):
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    nit_empresa = session.get('nit')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM inspeccion_preoperacional WHERE consecutivo_anual = %s AND id_empresa = %s LIMIT 1", (consecutivo, empresa_id))
    insp = cur.fetchone()
    cur.close()

    if not insp:
        flash("Error: Inspección no encontrada o no pertenece a tu empresa.", "danger")
        return redirect(url_for('preoperacional.historial_preoperacionales'))

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=14, textColor=colors.HexColor('#015249'), alignment=1, spaceAfter=10)
    sub_title_style = ParagraphStyle('SubTitle', parent=styles['Heading2'], fontSize=11, textColor=colors.HexColor('#015249'), spaceAfter=5, spaceBefore=10)
    cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=8, leading=10)
    cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=8, leading=10, fontName='Helvetica-Bold')

    base_dir = os.path.abspath(os.path.dirname(__file__))
    static_dir = os.path.join(base_dir, '..', 'static')
    logo_cliente_path = os.path.join(static_dir, f'logo_{nit_empresa}.PNG')
    logo_app_path = os.path.join(static_dir, 'logo_energix360.png')
    
    img_cliente = RLImage(logo_cliente_path, width=1.5*inch, height=0.5*inch, kind='proportional') if os.path.exists(logo_cliente_path) else Paragraph(empresa_nombre, cell_bold)
    img_app = RLImage(logo_app_path, width=1.5*inch, height=0.5*inch, kind='proportional') if os.path.exists(logo_app_path) else Paragraph("BQA-ONE", cell_bold)
    
    t_logos = Table([[img_cliente, img_app]], colWidths=[270, 270])
    t_logos.setStyle(TableStyle([('ALIGN', (0,0), (0,0), 'LEFT'), ('ALIGN', (1,0), (1,0), 'RIGHT'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
    story.append(t_logos)
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>INSPECCIÓN PREOPERACIONAL DETALLADA - SEGURIDAD VIAL</b>", title_style))

    dictamen_texto = "APROBADO (OPERATIVO)" if insp['vehiculo_aprobado'] == 1 else "ALERTA (CRÍTICA)"
    color_dictamen = colors.HexColor('#d1fae5') if insp['vehiculo_aprobado'] == 1 else colors.HexColor('#fee2e2')

    meta_data = [
        [Paragraph("<b>Consecutivo:</b>", cell_style), Paragraph(consecutivo, cell_bold), Paragraph("<b>Fecha / Hora:</b>", cell_style), Paragraph(f"{insp['fecha_inspeccion']} {insp['hora_inspeccion']}", cell_style)],
        [Paragraph("<b>Placa Vehículo:</b>", cell_style), Paragraph(str(insp['placa_vehiculo']).upper(), cell_bold), Paragraph("<b>Conductor:</b>", cell_style), Paragraph(insp['nombre_conductor'], cell_style)],
        [Paragraph("<b>Kilometraje:</b>", cell_style), Paragraph(str(insp['kilometraje_inicial']), cell_style), Paragraph("<b>Ruta:</b>", cell_style), Paragraph(insp['ruta_destino'], cell_style)],
        [Paragraph("<b>DICTAMEN:</b>", cell_style), Paragraph(f"<b>{dictamen_texto}</b>", cell_bold), "", ""]
    ]
    t_meta = Table(meta_data, colWidths=[100, 170, 100, 170])
    t_meta.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f9fafb')), ('BACKGROUND', (1,3), (1,3), color_dictamen),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('TOPPADDING', (0,0), (-1,-1), 4), ('BOTTOMPADDING', (0,0), (-1,-1), 4), ('SPAN', (1,3), (3,3))
    ]))
    story.append(t_meta)

    def get_estado_html(valor, es_doc=False):
        if es_doc:
            return "<font color='#16a34a'><b>AL DÍA / PORTA</b></font>" if valor == 1 else "<font color='#dc2626'><b>FALTANTE / VENCIDO</b></font>"
        if valor == 1: return "<font color='#16a34a'>Operativo</font>"
        elif valor == 2: return "<font color='#d97706'><b>Ajuste</b></font>"
        elif valor == 3: return "<font color='#dc2626'><b>Crítico</b></font>"
        return "N/A"

    checklist_config = [
        ("DOCUMENTACIÓN LEGAL", True, [
            ('doc_cedula', 'Cédula de Ciudadanía'),
            ('doc_licencia_conduccion', 'Licencia de Conducción'), 
            ('doc_licencia_transito', 'Licencia de Tránsito (Propiedad)'),
            ('doc_soat_vigente', 'SOAT Vigente'),
            ('doc_tecnomecanica_vigente', 'Revisión Tecnomecánica'), 
            ('doc_tarjeta_operacion', 'Tarjeta de Operación')
        ]),
        ("ESTADO MECÁNICO Y MOTOR", False, [
            ('mec_nivel_aceite_motor', 'Nivel Aceite Motor'), ('mec_liquido_frenos', 'Líquido de Frenos/Embrague'),
            ('mec_nivel_refrigerante', 'Nivel de Refrigerante'), ('mec_estado_correas', 'Estado de Correas'),
            ('mec_ausencia_fugas', 'Ausencia Fugas (Aceite/Agua/Aire)')
        ]),
        ("SISTEMA DE LUCES", False, [
            ('luc_altas_bajas', 'Luces Altas y Bajas'), ('luc_frenos_stop', 'Luces de Freno (Stop)'),
            ('luc_direccionales', 'Luces Direccionales'), ('luc_parqueo_estacionarias', 'Luces de Parqueo/Estacionarias'),
            ('luc_reversa_alarma', 'Luz y Alarma de Reversa'), ('luc_delimitadoras_cocuyos', 'Luces Delimitadoras (Cocuyos)')
        ]),
        ("SUSPENSIÓN Y REPUESTO", False, [
            ('lla_tuercas_pernos', 'Tuercas y Pernos Completos'), ('lla_repuesto_operativa', 'Llanta Repuesto Operativa'),
            ('lla_suspension_muelles', 'Suspensión y Muelles')
        ]),
        ("FRENOS Y MANDOS DE CABINA", False, [
            ('fre_pedal_firme', 'Firmeza Pedal de Freno'), ('fre_parqueo_mano', 'Freno de Parqueo/Mano'),
            ('fre_presion_aire_manometro', 'Manómetro Presión Aire'), ('fre_juego_direccion', 'Juego de Dirección'),
            ('fre_pito_corneta', 'Pito y Corneta'), ('fre_limpiaparabrisas_plumillas', 'Limpiaparabrisas y Plumillas')
        ]),
        ("CARROCERÍA Y ESTRUCTURA", False, [
            ('car_estado_estructura', 'Estado de Estructura General'), ('car_compuertas_carpas_amarres', 'Compuertas, Carpas y Amarres'),
            ('car_cinturones_seguridad', 'Cinturones de Seguridad'), ('car_espejos_retrovisores', 'Espejos Retrovisores'),
            ('car_vidrio_parabrisas', 'Vidrio Parabrisas')
        ]),
        ("EQUIPO DE PREVENCIÓN", False, [
            ('equ_extintor_10lbs', 'Extintor Cargado'), ('equ_tacos_bloqueo', 'Tacos de Bloqueo'),
            ('equ_senales_reflectivas', 'Señales Reflectivas'), ('equ_gato_hidraulico', 'Gato Hidráulico'),
            ('equ_cruceta_herramientas', 'Cruceta y Herramientas'), ('equ_botiquin_completo', 'Botiquín Completo')
        ])
    ]

    try:
        novedades_dict = json.loads(insp.get('detalles_novedades_json') or '{}')
    except: novedades_dict = {}

    story.append(Spacer(1, 10))

    for titulo, es_doc, campos in checklist_config:
        story.append(Paragraph(f"<b>{titulo}</b>", sub_title_style))
        tabla_datos = [["Ítem Inspeccionado", "Estado", "Observación / Novedad"]]
        
        for campo_db, label in campos:
            valor = insp.get(campo_db)
            estado_lbl = get_estado_html(valor, es_doc)
            obs = novedades_dict.get(campo_db, {}).get('detalle', 'Sin novedad') if valor in [2, 3] else ''
            
            tabla_datos.append([Paragraph(label, cell_style), Paragraph(estado_lbl, cell_style), Paragraph(obs, cell_style)])
        
        t_grupo = Table(tabla_datos, colWidths=[200, 80, 260])
        t_grupo.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')), ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOTTOMPADDING', (0,0), (-1,-1), 2), ('TOPPADDING', (0,0), (-1,-1), 2)
        ]))
        story.append(t_grupo)
        story.append(Spacer(1, 5))

    story.append(Paragraph("<b>ESQUEMA POSICIONAL DE LLANTAS</b>", sub_title_style))
    try:
        llantas_dict = json.loads(insp.get('estado_llantas_json') or '{}')
    except: llantas_dict = {}

    if llantas_dict:
        llantas_data = [["Posición de la Llanta", "Estado Labrado", "Novedad Reportada"]]
        for pos, l_data in llantas_dict.items():
            lab_txt = str(l_data.get('labrado')).upper()
            color_l = "#16a34a" if lab_txt == 'OPERATIVA' else ("#dc2626" if lab_txt == 'LISA' else "#d97706")
            llantas_data.append([
                Paragraph(l_data.get('nombre_legible', pos), cell_style),
                Paragraph(f"<font color='{color_l}'><b>{lab_txt}</b></font>", cell_style),
                Paragraph(l_data.get('novedad', ''), cell_style)
            ])
        t_llan = Table(llantas_data, colWidths=[200, 80, 260])
        t_llan.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#015249')), ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOTTOMPADDING', (0,0), (-1,-1), 2)
        ]))
        story.append(t_llan)
    else:
        story.append(Paragraph("No se registró esquema posicional de llantas en esta inspección.", cell_style))

    def convertir_base64_rlimage(b64_string, w, h):
        try:
            if b64_string and ',' in b64_string:
                img_data = base64.b64decode(b64_string.split(',')[1])
                img_buffer = io.BytesIO(img_data)
                return RLImage(img_buffer, width=w, height=h, kind='proportional')
        except Exception as e:
            pass
        return Paragraph("<i>No disponible</i>", cell_style)

    if insp.get('observaciones_hallazgos'):
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>OBSERVACIONES GENERALES DEL CONDUCTOR</b>", sub_title_style))
        story.append(Paragraph(f"<i>{insp['observaciones_hallazgos']}</i>", cell_style))

    story.append(Spacer(1, 15))
    story.append(Paragraph("<b>AUTENTICACIÓN Y FIRMA</b>", sub_title_style))
    
    declaracion_texto = "<b>Declaración de Veracidad y Cumplimiento Normativo:</b> Declaro bajo la gravedad de juramento que la información aquí registrada es veraz, exacta y ha sido recolectada mediante inspección física directa del vehículo. Este registro preoperacional da cumplimiento estricto al <b>Paso 16 de la Metodología del Plan Estratégico de Seguridad Vial (PESV)</b>, adoptada mediante la <b>Resolución 40595 de 2022 del Ministerio de Transporte de Colombia.</b>"
    story.append(Paragraph(declaracion_texto, cell_style))
    story.append(Spacer(1, 10))
    
    img_firma = convertir_base64_rlimage(insp.get('firma_grafica_base64'), 2*inch, 1*inch)
    img_foto = convertir_base64_rlimage(insp.get('foto_conductor_base64'), 1.2*inch, 1.2*inch)

    firma_data = [
        [Paragraph("<b>Foto Auditoría:</b>", cell_style), Paragraph("<b>Firma Gráfica:</b>", cell_style)], 
        [img_foto, img_firma]
    ]
    t_firma = Table(firma_data, colWidths=[150, 200])
    t_firma.setStyle(TableStyle([
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#16a34a')), 
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), 
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0fdf4')), 
        ('ALIGN', (0,0), (-1,-1), 'CENTER'), 
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE')
    ]))
    story.append(t_firma)

    doc.build(story)
    buffer.seek(0)
    
    return send_file(
        buffer, 
        as_attachment=True, 
        download_name=f"Preoperacional_{consecutivo}.pdf", 
        mimetype='application/pdf'
    )

# ==============================================================================
# 3. MANTENIMIENTO BD (CRON)
# ==============================================================================
@bp_preoperacional.route('/cron/mantenimiento_bd', methods=['GET'])
def cron_limpieza_datos():
    """
    CRON JOB: Ejecutar el día 1 de cada mes en la madrugada.
    Elimina los registros preoperacionales antiguos para no saturar el servidor.
    """
    if request.args.get('token') != 'BQA_CRON_2026':
        return jsonify({"success": False, "message": "No autorizado"}), 403

    cur = mysql.connection.cursor()
    try:
        # Ejecuta el borrado masivo de registros con más de 1 año (12 meses) en tabla unificada
        cur.execute("""
            DELETE FROM inspeccion_preoperacional 
            WHERE fecha_inspeccion < DATE_SUB(CURDATE(), INTERVAL 12 MONTH)
        """)
        
        filas_eliminadas = cur.rowcount
        mysql.connection.commit()
        
        return jsonify({
            "success": True, 
            "message": "Mantenimiento BD Flota completado exitosamente.",
            "registros_eliminados": filas_eliminadas
        }), 200

    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"success": False, "message": f"Error en mantenimiento: {str(e)}"}), 500
    finally:
        cur.close()