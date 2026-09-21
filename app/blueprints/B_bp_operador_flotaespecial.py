# app/blueprints/B_bp_operador_flotaespecial.py
import math
import os
import base64
import uuid
import hashlib
import pytz
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash, current_app
from app import mysql
from app.utils import login_required_custom
from datetime import datetime
import MySQLdb.cursors

bp_operador_flotaespecial = Blueprint('operador_flotaespecial', __name__)

# Zona horaria oficial de Colombia
BOGOTA_TZ = pytz.timezone('America/Bogota')

# ==============================================================================
# HELPER: GUARDADO DE EVIDENCIAS EN BASE64 CON DETECCIÓN MIME
# ==============================================================================
def _guardar_testigo_base64(base64_data, subcarpeta, prefix):
    if not base64_data: 
        return None
    try:
        ext = "jpg"
        if "," in base64_data:
            header, base64_data = base64_data.split(",", 1)
            if "image/png" in header.lower():
                ext = "png"
                
        binary_data = base64.b64decode(base64_data)
        
        static_dir = os.path.join(current_app.static_folder, 'uploads', 'flotaespecial', subcarpeta)
        os.makedirs(static_dir, exist_ok=True)
        
        filename = f"{prefix}_{uuid.uuid4().hex[:8]}.{ext}"
        file_path = os.path.join(static_dir, filename)
        
        with open(file_path, 'wb') as f:
            f.write(binary_data)
            
        ruta_relativa = os.path.relpath(file_path, current_app.static_folder).replace(os.path.sep, "/")
        return f"static/{ruta_relativa}"
    except Exception as e:
        print(f"Error procesando evidencia: {e}")
        return None

# ==============================================================================
# 1. RUTA: DASHBOARD PRINCIPAL DEL OPERADOR DE TRANSPORTE ESPECIAL
# ==============================================================================
@bp_operador_flotaespecial.route('/dashboard_operador_especial')
@login_required_custom
def dashboard_operador_especial():
    estatus_vehiculo = 'No logueado'
    placa = session.get('placa_prelogueada_especial')
    empresa_id = session.get('empresa_id')

    if placa and empresa_id:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT estatus FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        v = cur.fetchone()
        if v:
            estatus_vehiculo = v['estatus']
        cur.close()

    return render_template('B_modulo_operador_flotaespecial.html',
                           nit=session.get('nit'),
                           empresa=session.get('empresa'),
                           nombre=session.get('nombre'),
                           estatus_vehiculo=estatus_vehiculo)

# ==============================================================================
# 1.1 MOTOR DE DATOS DINÁMICOS PARA LA LISTA DE VIAJES ASIGNADOS
# ==============================================================================
@bp_operador_flotaespecial.route('/api/viaje_especial/dashboard_data', methods=['GET'])
@login_required_custom
def dashboard_data_especial():
    empresa_id = session.get('empresa_id')
    usuario_nombre = session.get('nombre')
    placa = session.get('placa_prelogueada_especial')
    
    if not placa:
        return jsonify({"status": "error", "message": "No hay vehículo prelogueado"}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT id_viaje, id_viaje_padre, numero_autorizacion, numero_prescripcion, nombre_usuario, direccion_origen, direccion_destino, 
                   hora_inicio, trayecto, estatus_servicio
            FROM control_viajes_flota_especial 
            WHERE id_empresa = %s 
              AND (vehiculo_asignado = %s OR conductor_asignado = %s)
              AND estatus_servicio = 'ASIGNADO'
            ORDER BY fecha_servicio ASC, hora_inicio ASC
        """, (empresa_id, placa, usuario_nombre))
        viajes_asignados = cur.fetchall()

        for v in viajes_asignados:
            v['hora_inicio'] = str(v['hora_inicio']) if v.get('hora_inicio') else "Pendiente"
            v['trayecto'] = (v.get('trayecto') or 'IDA').upper()

        return jsonify({"status": "success", "viajes_asignados": viajes_asignados}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# ==============================================================================
# 1.2 SOLICITAR ASIGNACIÓN AUTOMÁTICA DEL VIAJE DE VUELTA
# ==============================================================================
@bp_operador_flotaespecial.route('/api/viaje_especial/autoasignar_vuelta', methods=['POST'])
@login_required_custom
def autoasignar_vuelta_especial():
    empresa_id = session.get('empresa_id')
    usuario_nombre = session.get('nombre')
    placa = session.get('placa_prelogueada_especial')
    
    if not placa:
        return jsonify({"status": "error", "message": "Vehículo no prelogueado."}), 400
        
    datos = request.get_json(silent=True) or {}
    id_viaje_ida = str(datos.get('id_viaje', '')).strip().upper() 
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT id_viaje_padre FROM control_viajes_flota_especial WHERE id_viaje = %s AND id_empresa = %s", (id_viaje_ida, empresa_id))
        ida = cur.fetchone()
        if not ida or not ida.get('id_viaje_padre'):
            return jsonify({"status": "error", "message": "Referencia de viaje padre no encontrada."}), 404
            
        cur.execute("""
            SELECT id, id_viaje FROM control_viajes_flota_especial 
            WHERE id_empresa = %s AND id_viaje_padre = %s AND trayecto = 'VUELTA' AND estatus_servicio = 'PDTE. ASIGNAR VUELTA'
            LIMIT 1
        """, (empresa_id, ida['id_viaje_padre']))
        vuelta = cur.fetchone()
        
        if not vuelta:
             return jsonify({"status": "error", "message": "No se encontró un viaje de VUELTA pendiente. El controlador debe aprobarlo en cabina."}), 404
             
        cur.execute("""
            UPDATE control_viajes_flota_especial 
            SET vehiculo_asignado = %s, conductor_asignado = %s, estatus_servicio = 'ASIGNADO'
            WHERE id = %s AND id_empresa = %s
        """, (placa, usuario_nombre, vuelta['id'], empresa_id))
        mysql.connection.commit()
        
        return jsonify({"status": "success", "id_viaje_vuelta": vuelta['id_viaje']}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# ==============================================================================
# 2. LÓGICA DE PRELOGIN Y VIAJES (Enlace con el Vehículo Especial)
# ==============================================================================
@bp_operador_flotaespecial.route('/dashboard/flotaespecial/prelogin', methods=['POST'])
@login_required_custom
def prelogin_flotaespecial():
    modulos_activos = session.get('modulos_activos', [])
    if 'flotaespecial' not in modulos_activos:
        return jsonify(success=False, message="Acceso denegado: Tu empresa no tiene contratado el módulo de Transporte Especial."), 403

    data = request.get_json(silent=True) or {}
    placa = (data.get("placa") or "").upper().strip()
    
    if not placa:
        return jsonify(success=False, message="Placa no detectada."), 400

    empresa = session.get("empresa")
    empresa_id = session.get("empresa_id")
    usuario_id = session.get("usuario_id")
    if not empresa or not usuario_id:
        return jsonify(success=False, message="Sesión inválida."), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    try:
        try:
            cur.execute("ALTER TABLE vehiculos_especial ADD COLUMN estatus VARCHAR(50) DEFAULT 'No logueado', ADD COLUMN ultima_latitud DECIMAL(10, 8) NULL, ADD COLUMN ultima_longitud DECIMAL(11, 8) NULL;")
        except:
            pass

        cur.execute("SELECT id_empresa FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s LIMIT 1", (placa, empresa_id))
        v = cur.fetchone()

        if not v:
            return jsonify(success=False, message="Vehículo no encontrado en el sistema."), 404
        
        if str(v["id_empresa"]) != str(empresa_id):
            return jsonify(success=False, message="Este vehículo no pertenece a su empresa."), 403

        cur.execute("""
            SELECT 1 FROM inspeccion_preoperacional 
            WHERE id_empresa = %s AND placa_vehiculo = %s 
            AND fecha_inspeccion = CURDATE() AND vehiculo_aprobado = 1
            LIMIT 1
        """, (empresa_id, placa))
        inspeccion_hoy = cur.fetchone()

        nuevo_estatus = 'Logueado' if inspeccion_hoy else 'Prelogueado'

        cur.execute("UPDATE vehiculos_especial SET estatus=%s WHERE placa=%s AND id_empresa=%s", (nuevo_estatus, placa, empresa_id))
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historial_sesiones_flotaespecial (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_empresa INT NOT NULL,
                id_usuario INT NOT NULL,
                placa_vehiculo VARCHAR(20),
                fecha_login DATETIME,
                fecha_logout_manual DATETIME,
                latitud DECIMAL(10, 8),
                longitud DECIMAL(11, 8),
                estado_sesion VARCHAR(20) DEFAULT 'ACTIVA',
                INDEX(id_empresa, id_usuario)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        cur.execute("""
            INSERT INTO historial_sesiones_flotaespecial (id_empresa, id_usuario, placa_vehiculo, fecha_login)
            VALUES (%s, %s, %s, %s)
        """, (empresa_id, usuario_id, placa, datetime.now(BOGOTA_TZ)))
        
        mysql.connection.commit()
        
        session["placa_prelogueada_especial"] = placa
        
        return jsonify(
            success=True, 
            message="Vehículo prelogueado correctamente.", 
            redirect_url="/preoperacional"
        )
    except Exception as e:
        mysql.connection.rollback()
        return jsonify(success=False, message=f"Error interno del servidor: {str(e)}"), 500
    finally:
        cur.close()

@bp_operador_flotaespecial.route('/api/viaje_especial/iniciar', methods=['POST'])
@login_required_custom
def iniciar_viaje_especial():
    empresa_id = session.get('empresa_id')
    usuario_nombre = session.get('nombre')
    placa = session.get('placa_prelogueada_especial')

    if not placa:
        return jsonify({"status": "error", "message": "No hay vehículo prelogueado."}), 400

    datos = request.get_json(silent=True) or {}
    id_viaje_alfanumerico = str(datos.get('id_traslado_eps', '')).strip().upper()

    if not id_viaje_alfanumerico:
         return jsonify({"status": "error", "message": "Falta el Código de Acceso del Traslado."}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT id, estatus_servicio, id_viaje FROM control_viajes_flota_especial WHERE id_viaje = %s AND id_empresa = %s", (id_viaje_alfanumerico, empresa_id))
        viaje_data = cur.fetchone()
        
        if not viaje_data:
             return jsonify({"status": "error", "message": "El Código de Acceso no existe o no pertenece a su empresa."}), 404

        if viaje_data['estatus_servicio'] != 'ASIGNADO':
             return jsonify({"status": "error", "message": f"El servicio no está disponible. Estado actual: {viaje_data['estatus_servicio']}."}), 400

        cur.execute("""
            SELECT COUNT(*) as total 
            FROM control_viajes_flota_especial 
            WHERE id_empresa = %s AND vehiculo_asignado = %s AND YEAR(fecha_servicio) = YEAR(NOW()) AND consecutivo_viaje IS NOT NULL
        """, (empresa_id, placa))
        resultado = cur.fetchone()
        contador = (resultado['total'] if resultado else 0) + 1
        
        consecutivo = f"VIAJE-ESP-{placa}-{str(contador).zfill(3)}"
        
        id_viaje_fisico = viaje_data['id']
        
        cur.execute("""
            UPDATE control_viajes_flota_especial 
            SET estatus_servicio = 'EN EJECUCION', 
                operador_ejecucion = %s, 
                fecha_ejecucion_real = %s,
                consecutivo_viaje = %s 
            WHERE id_viaje = %s AND id_empresa = %s
        """, (usuario_nombre, datetime.now(BOGOTA_TZ), consecutivo, id_viaje_alfanumerico, empresa_id))

        mysql.connection.commit()

        session['viaje_activo_especial'] = True
        session['consecutivo_viaje_especial'] = consecutivo
        session['id_viaje_especial'] = id_viaje_fisico
        session['codigo_viaje_alfanumerico'] = id_viaje_alfanumerico

        return jsonify({"status": "success", "consecutivo": consecutivo, "id_viaje_fisico": id_viaje_fisico}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# ==============================================================================
# FASE 1: NUEVO ENDPOINT PARA REPORTE DE NOVEDAD DESDE LA APP MÓVIL
# ==============================================================================
@bp_operador_flotaespecial.route('/api/viaje_especial/reportar_novedad', methods=['POST'])
@login_required_custom
def reportar_novedad_especial():
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    
    id_viaje_alfanumerico = datos.get('id_traslado_eps') or session.get('codigo_viaje_alfanumerico')
    tipo_novedad = datos.get('tipo_novedad')
    descripcion = datos.get('descripcion')
    
    if not id_viaje_alfanumerico:
        return jsonify({"status": "error", "message": "ID de viaje no proporcionado."}), 400
    if tipo_novedad not in ['NOVEDAD_PRE_VIAJE', 'NOVEDAD_RECORRIDO']:
        return jsonify({"status": "error", "message": "Tipo de novedad inválido."}), 400
    if not descripcion:
        return jsonify({"status": "error", "message": "Debe incluir una descripción de la novedad."}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        try:
            cur.execute("""
                ALTER TABLE control_viajes_flota_especial 
                ADD COLUMN estado_novedad VARCHAR(50) NULL, 
                ADD COLUMN descripcion_novedad TEXT NULL, 
                ADD COLUMN fecha_novedad DATETIME NULL, 
                ADD COLUMN monto_liquidacion_manual DECIMAL(10,2) NULL, 
                ADD COLUMN es_trasbordo BOOLEAN DEFAULT FALSE, 
                ADD COLUMN fuec_anulado VARCHAR(50) NULL;
            """)
        except:
            pass

        cur.execute("SELECT id, estatus_servicio FROM control_viajes_flota_especial WHERE id_viaje = %s AND id_empresa = %s", (id_viaje_alfanumerico, empresa_id))
        viaje_info = cur.fetchone()
        
        if not viaje_info:
            return jsonify({"status": "error", "message": "Viaje no encontrado."}), 404
            
        if viaje_info['estatus_servicio'] in ['TERMINADO-PDTE AUDITAR', 'AUDITADO']:
            return jsonify({"status": "error", "message": "No puede reportar contingencias en viajes ya finalizados."}), 400

        lat_falla = datos.get('lat')
        lng_falla = datos.get('lng')
        update_coords_str = ""
        coords_args = []
        
        if tipo_novedad == 'NOVEDAD_RECORRIDO' and lat_falla and lng_falla:
            update_coords_str = ", lat_destino = %s, lng_destino = %s"
            coords_args = [lat_falla, lng_falla]

        query = f"""
            UPDATE control_viajes_flota_especial 
            SET estatus_servicio = %s,
                estado_novedad = 'ACTIVA',
                descripcion_novedad = %s,
                fecha_novedad = %s
                {update_coords_str}
            WHERE id_viaje = %s AND id_empresa = %s
        """
        
        args = [tipo_novedad, descripcion, datetime.now(BOGOTA_TZ)] + coords_args + [id_viaje_alfanumerico, empresa_id]
        
        cur.execute(query, tuple(args))
        mysql.connection.commit()

        if str(id_viaje_alfanumerico) == str(session.get('codigo_viaje_alfanumerico')):
            session.pop('viaje_activo_especial', None)
            session.pop('consecutivo_viaje_especial', None)
            session.pop('id_viaje_especial', None)
            session.pop('codigo_viaje_alfanumerico', None)

        return jsonify({"status": "success", "message": "Novedad reportada. El controlador ha sido alertado."}), 200

    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# ==============================================================================
# 5. FINALIZAR VIAJE NORMAL (ONLINE Y OFFLINE)
# ==============================================================================
@bp_operador_flotaespecial.route('/api/viaje_especial/finalizar', methods=['POST'])
@login_required_custom
def finalizar_viaje_especial():
    datos = request.get_json(silent=True) or {}
    
    id_viaje_fisico = datos.get('id_viaje_fisico') or session.get('id_viaje_especial')
    id_viaje_alfanumerico = datos.get('id_viaje_alfanumerico') or session.get('codigo_viaje_alfanumerico')
    empresa_id = session.get('empresa_id')

    if not id_viaje_fisico or not id_viaje_alfanumerico:
        return jsonify({"status": "error", "message": "No hay un viaje activo para finalizar."}), 400

    foto_origen = datos.get('foto_origen')
    foto_destino = datos.get('foto_destino')
    foto_paciente = datos.get('foto_paciente')
    firma = datos.get('firma')
    telemetria = datos.get('telemetria', {})

    tiempo_efectivo_minutos = 0
    try:
        fmt = '%Y-%m-%d %H:%M:%S'
        h_ori = datetime.strptime(telemetria.get('hora_origen'), fmt) if telemetria.get('hora_origen') else None
        h_des = datetime.strptime(telemetria.get('hora_destino'), fmt) if telemetria.get('hora_destino') else None
        
        t_total = 0
        if h_ori and h_des: 
            t_total += max(0, (h_des - h_ori).total_seconds())
            
        tiempo_efectivo_minutos = int(t_total / 60)
    except Exception as calc_err:
        print(f"Error en cálculo de tiempo efectivo: {calc_err}")

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        ruta_origen = _guardar_testigo_base64(foto_origen, 'viajes', f"origen_{id_viaje_fisico}")
        ruta_destino = _guardar_testigo_base64(foto_destino, 'viajes', f"destino_{id_viaje_fisico}")
        ruta_paciente = _guardar_testigo_base64(foto_paciente, 'viajes', f"paciente_{id_viaje_fisico}")
        ruta_firma = _guardar_testigo_base64(firma, 'viajes', f"firma_{id_viaje_fisico}")

        cur.execute("""
            UPDATE control_viajes_flota_especial 
            SET foto_origen = %s, foto_destino = %s, foto_paciente = %s, firma = %s,
                lat_origen = %s, lng_origen = %s, hora_origen = %s,
                lat_destino = %s, lng_destino = %s, hora_destino = %s,
                tiempo_efectivo_minutos = %s
            WHERE id = %s AND id_empresa = %s
        """, (ruta_origen, ruta_destino, ruta_paciente, ruta_firma, 
              telemetria.get('lat_origen'), telemetria.get('lng_origen'), telemetria.get('hora_origen'),
              telemetria.get('lat_destino'), telemetria.get('lng_destino'), telemetria.get('hora_destino'),
              tiempo_efectivo_minutos,
              id_viaje_fisico, empresa_id))
        
        cur.execute("""
            SELECT c.id, c.consecutivo_viaje, c.fecha_ejecucion_real as fecha_hora_inicio, %s as fecha_hora_fin, c.vehiculo_asignado as placa_vehiculo, 
                   c.foto_origen, c.foto_destino, c.foto_paciente, c.firma, c.id_viaje as id_traslado_eps,
                   c.id_eps_cliente AS eps_cliente, c.numero_autorizacion, c.numero_prescripcion, 
                   c.tipo_servicio AS codigo_servicio, c.nombre_usuario, c.id_usuario, 
                   c.direccion_origen, c.direccion_destino, c.id_viaje, c.trayecto, c.id_viaje_padre,
                   u.nombre AS conductor_nombre, u.cedula AS conductor_cedula,
                   e.nombre_comercial AS empresa_transporte
            FROM control_viajes_flota_especial c
            LEFT JOIN usuarios u ON c.conductor_asignado COLLATE utf8mb4_unicode_ci = u.nombre COLLATE utf8mb4_unicode_ci AND u.empresa_id = c.id_empresa
            LEFT JOIN empresas e ON c.id_empresa = e.id
            WHERE c.id = %s AND c.id_empresa = %s
        """, (datetime.now(BOGOTA_TZ), id_viaje_fisico, empresa_id))
        viaje_data = cur.fetchone()

        if viaje_data:
            def safe_str(val):
                return str(val) if val else 'N/A'

            cadena_seguridad = f"{viaje_data['consecutivo_viaje']}|{viaje_data['placa_vehiculo']}|{viaje_data['id_usuario']}|{viaje_data['numero_autorizacion']}"
            hash_seg = hashlib.sha256(cadena_seguridad.encode('utf-8')).hexdigest()
            
            pdf_filename = f"Reporte_Viaje_{viaje_data['id_viaje']}_{viaje_data['consecutivo_viaje']}.pdf"
            pdf_path_rel = f"uploads/flotaespecial/reportes/{pdf_filename}"
            pdf_path_abs = os.path.join(current_app.static_folder, pdf_path_rel)
            os.makedirs(os.path.dirname(pdf_path_abs), exist_ok=True)
            
            try:
                from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
                from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                from reportlab.lib.pagesizes import letter
                from reportlab.lib.units import inch
                from reportlab.lib import colors

                doc = SimpleDocTemplate(pdf_path_abs, pagesize=letter, rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
                story = []
                styles = getSampleStyleSheet()

                title_style = ParagraphStyle('DocTitle', parent=styles['Heading1'], fontSize=16, textColor=colors.HexColor('#015249'), alignment=1, spaceAfter=5)
                sub_title_style = ParagraphStyle('SubTitle', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#666666'), alignment=1, spaceAfter=20)
                section_style = ParagraphStyle('Section', parent=styles['Normal'], fontSize=10, textColor=colors.white, backColor=colors.HexColor('#015249'), padding=5, spaceBefore=10, spaceAfter=10)
                cell_style = ParagraphStyle('CellText', parent=styles['Normal'], fontSize=9, leading=11)
                cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=9, leading=11, fontName='Helvetica-Bold')
                hash_style = ParagraphStyle('HashText', parent=styles['Normal'], fontSize=9, leading=11, fontName='Courier', textColor=colors.HexColor('#374151'))

                story.append(Paragraph("<b>REPORTE DE RECORRIDO SEGURO</b>", title_style))
                story.append(Paragraph(f"<b>{safe_str(viaje_data.get('empresa_transporte'))}</b>", sub_title_style))

                story.append(Paragraph(f"CÓDIGO DE IDENTIFICACIÓN DEL VIAJE: <b>{safe_str(viaje_data.get('id_viaje'))}</b>", ParagraphStyle('ID', parent=styles['Normal'], alignment=1, fontSize=12, textColor=colors.HexColor('#16a34a'), spaceAfter=20)))

                story.append(Paragraph("<b>1. DATOS DE LA AUTORIZACIÓN Y PACIENTE</b>", section_style))
                data_sec1 = [
                    [Paragraph("<b>EPS / Cliente</b>", cell_style), Paragraph(safe_str(viaje_data.get('eps_cliente')), cell_style), Paragraph("<b>No. Autorización</b>", cell_style), Paragraph(f"<b>{safe_str(viaje_data.get('numero_autorizacion'))}</b>", cell_bold)],
                    [Paragraph("<b>Paciente Transportado</b>", cell_style), Paragraph(f"<b>{safe_str(viaje_data.get('nombre_usuario'))}</b>", cell_bold), Paragraph("<b>Documento / ID</b>", cell_style), Paragraph(safe_str(viaje_data.get('id_usuario')), cell_style)],
                    [Paragraph("<b>Dirección de Origen</b>", cell_style), Paragraph(safe_str(viaje_data.get('direccion_origen')), cell_style), Paragraph("<b>Dirección de Destino</b>", cell_style), Paragraph(safe_str(viaje_data.get('direccion_destino')), cell_style)],
                    [Paragraph("<b>Servicio / Código</b>", cell_style), Paragraph(safe_str(viaje_data.get('codigo_servicio')), cell_style), "", ""]
                ]
                t1 = Table(data_sec1, colWidths=[110, 160, 110, 160])
                t1.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), ('BACKGROUND', (0,0), (0,-1), colors.HexColor('#f9fafb')), ('BACKGROUND', (2,0), (2,2), colors.HexColor('#f9fafb')), ('SPAN', (1,3), (3,3)), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                story.append(t1)

                story.append(Spacer(1, 10))
                story.append(Paragraph("<b>2. DETALLES DE EJECUCIÓN DEL SERVICIO</b>", section_style))
                data_sec2 = [
                    [Paragraph("<b>Operador / Conductor Real</b>", cell_style), Paragraph(f"<b>{safe_str(viaje_data.get('conductor_nombre'))}</b> (CC: {safe_str(viaje_data.get('conductor_cedula'))})", cell_style), Paragraph("<b>Vehículo (Placa)</b>", cell_style), Paragraph(f"<b>{safe_str(viaje_data.get('placa_vehiculo'))}</b>", cell_bold)],
                    [Paragraph("<b>Fecha/Hora Inicio Real</b>", cell_style), Paragraph(str(safe_str(viaje_data.get('fecha_hora_inicio'))), cell_style), Paragraph("<b>Fecha/Hora Fin Real</b>", cell_style), Paragraph(str(safe_str(viaje_data.get('fecha_hora_fin'))), cell_style)],
                    [Paragraph("<b>Consecutivo Físico</b>", cell_style), Paragraph(safe_str(viaje_data.get('consecutivo_viaje')), cell_style), "", ""]
                ]
                t2 = Table(data_sec2, colWidths=[110, 160, 110, 160])
                t2.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), ('BACKGROUND', (0,0), (0,-1), colors.HexColor('#f9fafb')), ('BACKGROUND', (2,0), (2,1), colors.HexColor('#f9fafb')), ('SPAN', (1,2), (3,2)), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                story.append(t2)

                story.append(Spacer(1, 10))
                story.append(Paragraph("<b>3. EVIDENCIAS FOTOGRÁFICAS DE TRAZABILIDAD</b>", section_style))

                def _get_image(path_str):
                    if path_str and path_str != 'N/A':
                        rel_path = path_str.replace("static/", "", 1)
                        abs_p = os.path.join(current_app.static_folder, rel_path)
                        if os.path.exists(abs_p):
                            try:
                                return RLImage(abs_p, width=1.6*inch, height=1.3*inch, kind='proportional')
                            except:
                                pass
                    return Paragraph("<br/><br/><i>SIN EVIDENCIA</i>", ParagraphStyle('NoEv', parent=styles['Normal'], alignment=1, textColor=colors.red, fontSize=8))

                img_ori = _get_image(safe_str(viaje_data.get('foto_origen')))
                img_des = _get_image(safe_str(viaje_data.get('foto_destino')))
                img_pac = _get_image(safe_str(viaje_data.get('foto_paciente'))) 

                data_sec3 = [
                    [img_ori, img_des, img_pac],
                    [Paragraph("<b>PUNTO 1: RECOGIDA</b>", ParagraphStyle('C', parent=styles['Normal'], alignment=1, fontSize=8)), 
                     Paragraph("<b>PUNTO 2: LLEGADA</b>", ParagraphStyle('C', parent=styles['Normal'], alignment=1, fontSize=8)), 
                     Paragraph("<b>FOTO PACIENTE / ACUDIENTE</b>", ParagraphStyle('C', parent=styles['Normal'], alignment=1, fontSize=8))]
                ]
                t3 = Table(data_sec3, colWidths=[175, 175, 175])
                t3.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE'), ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#e5e7eb')), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb'))]))
                story.append(t3)

                story.append(Spacer(1, 15))
                decl_style = ParagraphStyle('Decl', parent=styles['Normal'], fontSize=8, textColor=colors.HexColor('#4b5563'), alignment=4, spaceAfter=15)
                story.append(Paragraph("<b>Declaración Juramentada:</b> Declaro que los datos aquí registrados corresponden a la realidad y que este tramo del viaje ha sido ejecutado de manera real según la orden de servicio establecida, cumpliendo con los protocolos normativos vigentes.", decl_style))

                img_firma = _get_image(safe_str(viaje_data.get('firma')))
                data_firma = [
                    [img_firma],
                    [Paragraph(f"<b>{safe_str(viaje_data.get('nombre_usuario'))}</b>", ParagraphStyle('C', parent=styles['Normal'], alignment=1))],
                    [Paragraph("Firma de Conformidad (Paciente / Acudiente Responsable)", ParagraphStyle('C', parent=styles['Normal'], alignment=1, fontSize=8, textColor=colors.HexColor('#6b7280')))]
                ]
                t_firma = Table(data_firma, colWidths=[250])
                t_firma.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                story.append(t_firma)

                story.append(Spacer(1, 20))
                story.append(Paragraph("<b>CERTIFICACIÓN CRIPTOGRÁFICA BQA-ONE</b>", cell_bold))
                story.append(Paragraph("Este documento certifica la correcta ejecución del servicio mediante GPS y evidencias fotográficas. Ningún dato reportado en este documento puede ser modificado post-ejecución.", cell_style))
                story.append(Paragraph(f"<b>Sello SHA-256 (Hash de Integridad):</b><br/>{hash_seg}", hash_style))

                doc.build(story)
                ruta_pdf_final = f"static/{pdf_path_rel}"
            except Exception as e:
                print(f"Error generando PDF ReportLab: {str(e)}")
                ruta_pdf_final = None

            cur.execute("""
                UPDATE control_viajes_flota_especial 
                SET estatus_servicio = 'TERMINADO-PDTE AUDITAR', 
                    ruta_documento = %s, 
                    hash_seguridad = %s,
                    fecha_fin_real = %s 
                WHERE id_viaje = %s AND id_empresa = %s
            """, (ruta_pdf_final, hash_seg, datetime.now(BOGOTA_TZ), id_viaje_alfanumerico, empresa_id))
            
            if viaje_data.get('trayecto') == 'IDA' and viaje_data.get('id_viaje_padre'):
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET estatus_servicio = 'PDTE. ASIGNAR VUELTA' 
                    WHERE id_viaje_padre = %s AND trayecto = 'VUELTA' AND id_empresa = %s 
                    AND estatus_servicio IN ('CAPTURADO', 'VERIFICADO', 'PROGRAMADO')
                """, (viaje_data.get('id_viaje_padre'), empresa_id))

        mysql.connection.commit()

        if str(id_viaje_fisico) == str(session.get('id_viaje_especial')):
            session.pop('viaje_activo_especial', None)
            session.pop('consecutivo_viaje_especial', None)
            session.pop('id_viaje_especial', None)
            session.pop('codigo_viaje_alfanumerico', None)

        return jsonify({"status": "success"}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

@bp_operador_flotaespecial.route('/api/viaje_especial/recuperar', methods=['POST'])
@login_required_custom
def recuperar_viaje_especial():
    datos = request.get_json(silent=True) or {}
    placa = datos.get('placa')
    empresa_id = session.get('empresa_id')
    usuario_id = session.get('usuario_id')
    
    if not placa:
        return jsonify({"status": "error", "message": "Placa no proporcionada"}), 400

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT id, consecutivo_viaje, id_viaje as id_traslado_eps 
            FROM control_viajes_flota_especial 
            WHERE vehiculo_asignado = %s AND id_empresa = %s AND conductor_asignado = %s AND estatus_servicio = 'EN EJECUCION'
            ORDER BY id DESC LIMIT 1
        """, (placa, empresa_id, session.get('nombre')))
        viaje = cur.fetchone()

        if viaje:
            session['placa_prelogueada_especial'] = placa
            session['viaje_activo_especial'] = True
            session['consecutivo_viaje_especial'] = viaje['consecutivo_viaje']
            session['id_viaje_especial'] = viaje['id']
            session['codigo_viaje_alfanumerico'] = viaje['id_traslado_eps']

            cur.execute("UPDATE vehiculos_especial SET estatus = 'Logueado' WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
            
            cur.execute("""
                INSERT INTO historial_sesiones_flotaespecial (id_empresa, id_usuario, placa_vehiculo, fecha_login, estado_sesion)
                VALUES (%s, %s, %s, %s, 'ACTIVA')
            """, (empresa_id, usuario_id, placa, datetime.now(BOGOTA_TZ)))

            mysql.connection.commit()
            return jsonify({"status": "success", "consecutivo": viaje['consecutivo_viaje']}), 200
        else:
            return jsonify({"status": "error", "message": "No se encontró un viaje activo válido para recuperar."}), 404
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# ==============================================================================
# 3. RUTAS DE SESIÓN EN VIVO (HEARTBEAT Y LOGOUT)
# ==============================================================================
@bp_operador_flotaespecial.route('/api/flotaespecial/heartbeat', methods=['POST'])
@login_required_custom 
def heartbeat_flotaespecial():
    if 'usuario_id' not in session:
        return jsonify({"status": "error", "message": "No autenticado"}), 401
        
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            INSERT INTO monitoreo_actividad (id_usuario, ultima_actividad)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE ultima_actividad = %s
        """, (session.get('usuario_id'), datetime.now(BOGOTA_TZ), datetime.now(BOGOTA_TZ)))
        mysql.connection.commit()
        cur.close()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@bp_operador_flotaespecial.route('/api/flotaespecial/logout_manual', methods=['POST'])
@login_required_custom 
def logout_manual_flotaespecial():
    if 'usuario_id' not in session: 
        return jsonify({"status": "error"}), 401
    
    if session.get('viaje_activo_especial'):
        return jsonify({"status": "error", "message": "Finaliza el viaje primero."}), 403
        
    empresa_id = session.get('empresa_id')
    usuario_id = session.get('usuario_id')
    placa = session.get('placa_prelogueada_especial')
    
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            UPDATE historial_sesiones_flotaespecial 
            SET fecha_logout_manual = %s, estado_sesion = 'FINALIZADA'
            WHERE id_usuario = %s AND id_empresa = %s AND estado_sesion = 'ACTIVA'
            ORDER BY id DESC LIMIT 1
        """, (datetime.now(BOGOTA_TZ), usuario_id, empresa_id))
        
        if placa:
            cur.execute("UPDATE vehiculos_especial SET estatus='No logueado' WHERE placa=%s AND id_empresa=%s", (placa, empresa_id))
            
        mysql.connection.commit()
        cur.close()
        
        session.pop('placa_prelogueada_especial', None)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ==============================================================================
# 4. MOTOR DE RASTREO, GEOCERCA Y RUTAS HISTÓRICAS
# ==============================================================================
@bp_operador_flotaespecial.route('/api/flotaespecial/actualizar_ubicacion', methods=['POST'])
@login_required_custom 
def actualizar_ubicacion_especial():
    if 'usuario_id' not in session or 'placa_prelogueada_especial' not in session:
        return jsonify({"status": "error", "message": "Sesión inválida"}), 401

    placa = session.get('placa_prelogueada_especial')
    empresa_id = session.get('empresa_id')
    datos = request.get_json(silent=True) or {}
    
    ubicaciones = datos.get('ubicaciones', [])
    if not ubicaciones:
        latitud = datos.get('lat')
        longitud = datos.get('lng')
        if latitud and longitud:
            ubicaciones = [{'lat': latitud, 'lng': longitud, 'fecha_hora': datetime.now(BOGOTA_TZ).strftime('%Y-%m-%d %H:%M:%S')}]

    if not ubicaciones:
        return jsonify({"status": "error", "message": "Faltan coordenadas"}), 400

    cur = mysql.connection.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vehiculos_historial_rutas_especial (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_empresa INT NOT NULL,
                placa VARCHAR(20) NOT NULL,
                latitud DECIMAL(10, 8),
                longitud DECIMAL(11, 8),
                fecha_hora DATETIME,
                tipo_registro VARCHAR(50),
                nombre_punto VARCHAR(255),
                INDEX(id_empresa, placa)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        ultima_lat = ubicaciones[-1]['lat']
        ultima_lng = ubicaciones[-1]['lng']
        cur.execute("UPDATE vehiculos_especial SET ultima_latitud = %s, ultima_longitud = %s WHERE placa = %s AND id_empresa = %s", (ultima_lat, ultima_lng, placa, empresa_id))
        
        for ubi in ubicaciones:
            fecha_hora = ubi.get('fecha_hora', datetime.now(BOGOTA_TZ).strftime('%Y-%m-%d %H:%M:%S'))
            cur.execute("""
                INSERT INTO vehiculos_historial_rutas_especial (id_empresa, placa, latitud, longitud, fecha_hora, tipo_registro)
                VALUES (%s, %s, %s, %s, %s, 'Automático')
            """, (empresa_id, placa, ubi['lat'], ubi['lng'], fecha_hora))

        mysql.connection.commit()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()


@bp_operador_flotaespecial.route('/api/flotaespecial/registrar_parada', methods=['POST'])
@login_required_custom 
def registrar_parada_especial():
    if 'usuario_id' not in session or 'placa_prelogueada_especial' not in session:
        return jsonify({"status": "error", "message": "Sesión inválida"}), 401
        
    datos = request.get_json(silent=True) or {}
    empresa_id = session.get('empresa_id')
    placa = session.get('placa_prelogueada_especial')
    usuario_id = session.get('usuario_id')
    
    paradas = datos.get('paradas', [])
    if not paradas and 'lat' in datos:
        paradas = [datos]
        
    if not paradas:
        return jsonify({"status": "error", "message": "Datos vacíos"}), 400
        
    cur = mysql.connection.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS historial_paradas_flotaespecial (
                id INT AUTO_INCREMENT PRIMARY KEY,
                id_empresa INT NOT NULL,
                placa VARCHAR(20) NOT NULL,
                usuario_id INT NOT NULL,
                fecha DATE,
                hora_inicio TIME,
                hora_fin TIME,
                latitud DECIMAL(10, 8),
                longitud DECIMAL(11, 8),
                tipo_actividad VARCHAR(100),
                nombre_punto VARCHAR(255),
                origen_registro VARCHAR(50),
                INDEX(id_empresa, placa)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        ids_insertados = []
        for p in paradas:
            cur.execute("""
                INSERT INTO historial_paradas_flotaespecial 
                (id_empresa, placa, usuario_id, fecha, hora_inicio, hora_fin, latitud, longitud, tipo_actividad, nombre_punto, origen_registro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, placa, usuario_id, 
                p.get('fecha', datetime.now(BOGOTA_TZ).strftime('%Y-%m-%d')),
                p.get('hora_inicio', datetime.now(BOGOTA_TZ).strftime('%H:%M:%S')),
                p.get('hora_fin'),
                p.get('lat'), p.get('lng'),
                p.get('tipo_actividad', 'Otra'),
                p.get('nombre_punto', 'Punto Desconocido'),
                p.get('origen_registro', 'Manual')
            ))
            nuevo_id = cur.lastrowid
            ids_insertados.append({"temp_id": p.get('temp_id'), "db_id": nuevo_id})
            
            fecha_hora = f"{p.get('fecha', datetime.now(BOGOTA_TZ).strftime('%Y-%m-%d'))} {p.get('hora_inicio', datetime.now(BOGOTA_TZ).strftime('%H:%M:%S'))}"
            cur.execute("""
                INSERT INTO vehiculos_historial_rutas_especial (id_empresa, placa, latitud, longitud, fecha_hora, tipo_registro, nombre_punto)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, placa, p.get('lat'), p.get('lng'), fecha_hora,
                'Parada ' + p.get('origen_registro', 'Manual'),
                p.get('nombre_punto', 'Punto Desconocido')
            ))

        mysql.connection.commit()
        return jsonify({"status": "success", "ids_mapeados": ids_insertados}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()


@bp_operador_flotaespecial.route('/api/flotaespecial/actualizar_fin_parada', methods=['POST'])
@login_required_custom 
def actualizar_fin_parada_especial():
    if 'usuario_id' not in session: 
        return jsonify({"status": "error"}), 401
    
    datos = request.get_json(silent=True) or {}
    paradas = datos.get('paradas', [])
    if not paradas and 'id' in datos:
        paradas = [datos]
        
    cur = mysql.connection.cursor()
    try:
        for p in paradas:
            if p.get('id'):
                cur.execute("""
                    UPDATE historial_paradas_flotaespecial 
                    SET hora_fin = %s 
                    WHERE id = %s AND id_empresa = %s
                """, (p.get('hora_fin', datetime.now(BOGOTA_TZ).strftime('%H:%M:%S')), p.get('id'), session.get('empresa_id')))
        mysql.connection.commit()
        return jsonify({"status":"success"})
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status":"error", "message": str(e)}), 500
    finally:
        cur.close()


def calcular_distancia(lat1, lon1, lat2, lon2):
    R = 6371000 
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    delta_phi = math.radians(float(lat2) - float(lat1))
    delta_lambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(delta_phi/2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2.0)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


@bp_operador_flotaespecial.route('/cron/flotaespecial/deslogueo_geocerca', methods=['GET', 'POST'])
def cron_deslogueo_geocerca_especial():
    token = request.args.get('token')
    if token != 'BQA_CRON_2026': return jsonify({"status": "error"}), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT v.placa, v.ultima_latitud, v.ultima_longitud, 
               e.base_latitud, e.base_longitud, e.radio_permitido_metros, v.id_empresa
        FROM vehiculos_especial v
        JOIN empresas e ON v.id_empresa = e.id
        WHERE v.estatus IN ('Logueado', 'Prelogueado') 
          AND v.ultima_latitud IS NOT NULL AND e.base_latitud IS NOT NULL
    """)
    vehiculos = cur.fetchall()
    vehiculos_a_desloguear = []
    
    for v in vehiculos:
        distancia = calcular_distancia(v['ultima_latitud'], v['ultima_longitud'], v['base_latitud'], v['base_longitud'])
        if distancia <= (v['radio_permitido_metros'] or 200):
            vehiculos_a_desloguear.append((v['placa'], v['id_empresa']))
            
    if vehiculos_a_desloguear:
        for placa, id_empresa in vehiculos_a_desloguear:
             cur.execute("UPDATE vehiculos_especial SET estatus = 'No logueado' WHERE placa = %s AND id_empresa = %s", (placa, id_empresa))
        mysql.connection.commit()
        
    cur.close()
    return jsonify({"status": "success", "total_deslogueados": len(vehiculos_a_desloguear)})