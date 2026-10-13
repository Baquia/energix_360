# MÓDULO: TRANSPORTE CARGA Y ESPECIAL | SUBMÓDULO: PESV AUDITORIAS | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/C_bp_auditorias_PESV.py
import io
from flask import Blueprint, render_template, request, session, redirect, url_for, flash, send_file, jsonify
from app import mysql
from app.utils import login_required_custom, submodulo_required
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta

# Librerías estándar para generación de PDF en Python
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors

bp_auditorias_pesv = Blueprint('auditorias_pesv', __name__, url_prefix='/gestor_flota/pesv_bp/auditorias')

# ==============================================================================
# MIDDLEWARE DE JERARQUÍA Y PERMISOS MULTI-MÓDULO
# ==============================================================================
def pesv_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        perfiles_permitidos = [
            'gestor_flotacarga', 
            'controlador_transportecarga', 
            'controlador_flotacarga', 
            'controlador_flotaespecial',
            'controlador_logistica',
            'controlador_pesv',
            'webmaster'
        ]
        
        if perfil not in perfiles_permitidos and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador para ingresar a Auditorías PESV.', 'danger')
            return redirect(url_for('index'))
            
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# RUTAS DEL SUB-SUBMÓDULO DE AUDITORÍAS PESV
# ==============================================================================
@bp_auditorias_pesv.route('/', methods=['GET'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def index_auditorias():
    """
    Lista el historial de auditorías de flota realizadas por el controlador
    y prepara los datos para el formulario de nueva inspección (Multi-Flota).
    """
    nit_empresa = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # 1. Obtener historial de auditorías
    auditorias = []
    try:
        cur.execute("""
            SELECT id, fecha_auditoria, placa_vehiculo, nombre_conductor, 
                   resultado_general, auditor, observaciones 
            FROM auditorias_pesv 
            WHERE id_empresa = %s 
            ORDER BY fecha_auditoria DESC
        """, (nit_empresa,))
        auditorias = cur.fetchall()
    except Exception as e:
        print(f"Aviso - No se pudieron cargar auditorias: {e}")

    # 2. Obtener lista de vehículos (Unificando Flota Carga y Especial mediante Python)
    vehiculos_list = []
    try:
        cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s", (nit_empresa,))
        vehiculos_list.extend(cur.fetchall())
    except Exception: pass

    try:
        cur.execute("SELECT placa FROM vehiculos_especial WHERE id_empresa = %s", (nit_empresa,))
        vehiculos_list.extend(cur.fetchall())
    except Exception: pass

    seen_placas = set()
    vehiculos = []
    for v in vehiculos_list:
        if v['placa'] not in seen_placas:
            vehiculos.append(v)
            seen_placas.add(v['placa'])
    vehiculos.sort(key=lambda x: x['placa'])

    # 3. Obtener lista de conductores (Unificando Flota Carga y Especial mediante Python)
    conductores_list = []
    try:
        cur.execute("SELECT nombre, cedula FROM conductores_carga WHERE id_empresa = %s", (nit_empresa,))
        conductores_list.extend(cur.fetchall())
    except Exception: pass

    try:
        cur.execute("SELECT nombre, cedula FROM conductores_flotaespecial WHERE id_empresa = %s", (nit_empresa,))
        conductores_list.extend(cur.fetchall())
    except Exception: pass

    seen_cedulas = set()
    conductores = []
    for c in conductores_list:
        if c['cedula'] not in seen_cedulas:
            conductores.append(c)
            seen_cedulas.add(c['cedula'])
    conductores.sort(key=lambda x: x['nombre'])
    
    cur.close()

    return render_template(
        'C_auditorias_PESV.html',
        nit=nit_empresa,
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        auditorias=auditorias,
        vehiculos=vehiculos,
        conductores=conductores,
        active_module='auditorias'
    )

@bp_auditorias_pesv.route('/guardar', methods=['POST'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def guardar_auditoria():
    """
    Procesa el formulario de inspección y guarda la auditoría en la base de datos.
    """
    nit_empresa = session.get('empresa_id')
    auditor = session.get('nombre')
    
    placa = str(request.form.get('placa_vehiculo', '')).strip().upper()
    conductor = str(request.form.get('nombre_conductor', '')).strip()
    doc_vigentes = str(request.form.get('documentos_vigentes', 'NO')).strip()
    mecanica_ok = str(request.form.get('estado_mecanico', 'NO')).strip()
    aptitud_ok = str(request.form.get('aptitud_operativa', 'NO')).strip()
    observaciones = str(request.form.get('observaciones', '')).strip()
    
    # Lógica de aprobación automática (Cumplimiento estricto)
    if doc_vigentes == 'SI' and mecanica_ok == 'SI' and aptitud_ok == 'SI':
        resultado = 'APROBADO'
    else:
        resultado = 'RECHAZADO'
        
    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            INSERT INTO auditorias_pesv 
            (id_empresa, placa_vehiculo, nombre_conductor, auditor, 
             documentos_vigentes, estado_mecanico, aptitud_operativa, 
             resultado_general, observaciones, fecha_auditoria) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (nit_empresa, placa, conductor, auditor, doc_vigentes, mecanica_ok, aptitud_ok, resultado, observaciones))
        mysql.connection.commit()
        cur.close()
        
        flash('Auditoría PESV registrada y guardada exitosamente.', 'success')
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error guardando auditoría de flota: {e}")
        flash('Ocurrió un error interno al guardar la auditoría. Intente nuevamente.', 'danger')
        
    return redirect(url_for('auditorias_pesv.index_auditorias'))

@bp_auditorias_pesv.route('/descargar_pdf/<int:id_auditoria>', methods=['GET'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def descargar_pdf(id_auditoria):
    """
    Genera y retorna un documento PDF automatizado con el reporte de la auditoría.
    """
    nit_empresa = session.get('empresa_id')
    
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("""
            SELECT * FROM auditorias_pesv 
            WHERE id = %s AND id_empresa = %s
        """, (id_auditoria, nit_empresa))
        auditoria = cur.fetchone()
        cur.close()
        
        if not auditoria:
            flash('La auditoría solicitada no existe o no tiene permisos para acceder a ella.', 'danger')
            return redirect(url_for('auditorias_pesv.index_auditorias'))
        
        buffer = io.BytesIO()
        p = canvas.Canvas(buffer, pagesize=letter)
        width, height = letter
        
        p.setFont("Helvetica-Bold", 18)
        p.setFillColor(colors.HexColor("#015249"))
        p.drawString(50, height - 60, "INFORME DE AUDITORÍA PESV")
        
        p.setFont("Helvetica", 11)
        p.setFillColor(colors.black)
        p.drawString(50, height - 90, f"Empresa Operadora: {session.get('empresa')}")
        p.drawString(50, height - 110, f"NIT: {nit_empresa}")
        p.drawString(50, height - 130, f"Fecha de Auditoría: {auditoria['fecha_auditoria']}")
        p.drawString(50, height - 150, f"Auditor Responsable: {auditoria['auditor']}")
        
        p.setLineWidth(1)
        p.setStrokeColor(colors.HexColor("#e5e7eb"))
        p.line(50, height - 165, width - 50, height - 165)
        
        p.setFont("Helvetica-Bold", 14)
        p.setFillColor(colors.HexColor("#015249"))
        p.drawString(50, height - 195, "Identificación Operativa")
        
        p.setFont("Helvetica", 11)
        p.setFillColor(colors.black)
        p.drawString(50, height - 220, f"Vehículo Inspeccionado (Placa): {auditoria['placa_vehiculo']}")
        p.drawString(50, height - 240, f"Conductor Evaluado: {auditoria['nombre_conductor']}")
        
        p.setFont("Helvetica-Bold", 14)
        p.setFillColor(colors.HexColor("#015249"))
        p.drawString(50, height - 280, "Puntos de Control Normativo")
        
        p.setFont("Helvetica", 11)
        p.setFillColor(colors.black)
        p.drawString(50, height - 305, f"1. Documentación Obligatoria Vigente (SOAT, RTM): {auditoria['documentos_vigentes']}")
        p.drawString(50, height - 325, f"2. Estado Mecánico y Preoperacional Diario: {auditoria['estado_mecanico']}")
        p.drawString(50, height - 345, f"3. Aptitud Operativa y Comportamiento Humano: {auditoria['aptitud_operativa']}")
        
        p.setFont("Helvetica-Bold", 16)
        color_resultado = colors.HexColor("#059669") if auditoria['resultado_general'] == 'APROBADO' else colors.HexColor("#dc2626")
        p.setFillColor(color_resultado)
        p.drawString(50, height - 395, f"DICTAMEN FINAL: {auditoria['resultado_general']}")
        
        p.setFillColor(colors.black)
        p.setFont("Helvetica-Bold", 12)
        p.drawString(50, height - 440, "Observaciones y Hallazgos Registrados:")
        
        p.setFont("Helvetica", 10)
        observaciones_texto = auditoria.get('observaciones')
        if not observaciones_texto or str(observaciones_texto).strip() == '':
            observaciones_texto = "Ninguna observación registrada."
            
        texto_dinamico = p.beginText(50, height - 465)
        texto_dinamico.setFont("Helvetica", 10)
        
        lineas = observaciones_texto.split('\n')
        for linea in lineas:
            fragmentos = [linea[i:i+90] for i in range(0, len(linea), 90)]
            for frag in fragmentos:
                texto_dinamico.textLine(frag)
        
        p.drawText(texto_dinamico)
        
        p.setStrokeColor(colors.black)
        p.line(50, 150, 250, 150)
        p.setFont("Helvetica-Bold", 10)
        p.drawString(50, 135, f"Firma: {auditoria['auditor']}")
        p.setFont("Helvetica", 9)
        p.drawString(50, 120, "Controlador PESV / BQA-ONE")
        
        p.setFillColor(colors.HexColor("#6b7280"))
        p.drawCentredString(width / 2, 30, "Documento generado automáticamente a través del Motor Operativo BQA-ONE (Energix360)")
        
        p.showPage()
        p.save()
        buffer.seek(0)
        
        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"Reporte_Auditoria_PESV_{auditoria['placa_vehiculo']}_{auditoria['fecha_auditoria'].strftime('%Y%m%d')}.pdf",
            mimetype='application/pdf'
        )
        
    except Exception as e:
        print(f"Error crítico construyendo PDF de auditoría PESV: {e}")
        flash('El generador de informes no pudo construir el documento. Por favor intente más tarde.', 'danger')
        return redirect(url_for('auditorias_pesv.index_auditorias'))

# ==============================================================================
# NUEVO: VERIFICACIÓN DE DOCUMENTOS MULTI-FLOTA (CARGA Y ESPECIAL)
# ==============================================================================
@bp_auditorias_pesv.route('/verificacion_documentos', methods=['GET'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def verificacion_documentos():
    nit_empresa = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    documentos_pendientes = []
    
    # 1. Vehículos de Carga
    try:
        cur.execute("SELECT id, placa, estado_soat, estado_rtm, estado_rcc_rce as estado_poliza_rce, ruta_soat, ruta_tecnomecanica, ruta_poliza_rce FROM vehiculos WHERE id_empresa = %s", (nit_empresa,))
        vehiculos_carga = cur.fetchall()
        for v in vehiculos_carga:
            if v.get('estado_soat') == 'Actualizado' and v.get('ruta_soat'):
                documentos_pendientes.append({"tipo": "vehiculo_carga", "id": v['id'], "identificador": v['placa'], "doc_nombre": "SOAT", "clave": "estado_soat", "ruta": v['ruta_soat']})
            if v.get('estado_rtm') == 'Actualizado' and v.get('ruta_tecnomecanica'):
                documentos_pendientes.append({"tipo": "vehiculo_carga", "id": v['id'], "identificador": v['placa'], "doc_nombre": "Tecnomecánica", "clave": "estado_rtm", "ruta": v['ruta_tecnomecanica']})
            if v.get('estado_poliza_rce') == 'Actualizado' and v.get('ruta_poliza_rce'):
                documentos_pendientes.append({"tipo": "vehiculo_carga", "id": v['id'], "identificador": v['placa'], "doc_nombre": "Póliza RCE", "clave": "estado_poliza_rce", "ruta": v['ruta_poliza_rce']})
    except Exception as e:
        print(f"Aviso - Error consultando vehiculos carga: {e}")

    # 2. Vehículos Especiales
    try:
        cur.execute("SELECT id, placa, estado_soat, estado_rtm, estado_rcc_rce, ruta_pdf_soat, ruta_pdf_tecnomecanica, ruta_pdf_rcc_rce FROM vehiculos_especial WHERE id_empresa = %s", (nit_empresa,))
        vehiculos_especial = cur.fetchall()
        for v in vehiculos_especial:
            if v.get('estado_soat') == 'Actualizado' and v.get('ruta_pdf_soat'):
                documentos_pendientes.append({"tipo": "vehiculo_especial", "id": v['id'], "identificador": v['placa'], "doc_nombre": "SOAT", "clave": "estado_soat", "ruta": v['ruta_pdf_soat']})
            if v.get('estado_rtm') == 'Actualizado' and v.get('ruta_pdf_tecnomecanica'):
                documentos_pendientes.append({"tipo": "vehiculo_especial", "id": v['id'], "identificador": v['placa'], "doc_nombre": "Tecnomecánica", "clave": "estado_rtm", "ruta": v['ruta_pdf_tecnomecanica']})
            if v.get('estado_rcc_rce') == 'Actualizado' and v.get('ruta_pdf_rcc_rce'):
                documentos_pendientes.append({"tipo": "vehiculo_especial", "id": v['id'], "identificador": v['placa'], "doc_nombre": "Póliza RCC/RCE", "clave": "estado_rcc_rce", "ruta": v['ruta_pdf_rcc_rce']})
    except Exception as e:
        print(f"Aviso - Error consultando vehiculos especial: {e}")

    # 3. Conductores Carga
    try:
        cur.execute("SELECT id, cedula, nombre, estado_licencia, estado_seguridad_social, ruta_pdf_licencia, ruta_pdf_seguridad_social FROM conductores_carga WHERE id_empresa = %s", (nit_empresa,))
        conductores_carga = cur.fetchall()
        for c in conductores_carga:
            if c.get('estado_licencia') == 'Actualizado' and c.get('ruta_pdf_licencia'):
                documentos_pendientes.append({"tipo": "conductor_carga", "id": c['id'], "identificador": f"{c['nombre']} ({c['cedula']})", "doc_nombre": "Licencia de Conducción", "clave": "estado_licencia", "ruta": c['ruta_pdf_licencia']})
            if c.get('estado_seguridad_social') == 'Actualizado' and c.get('ruta_pdf_seguridad_social'):
                documentos_pendientes.append({"tipo": "conductor_carga", "id": c['id'], "identificador": f"{c['nombre']} ({c['cedula']})", "doc_nombre": "Seguridad Social", "clave": "estado_seguridad_social", "ruta": c['ruta_pdf_seguridad_social']})
    except Exception as e:
        print(f"Aviso - Error consultando conductores carga: {e}")

    # 4. Conductores Especiales
    try:
        cur.execute("SELECT id, cedula, nombre, estado_licencia, estado_seguridad_social, ruta_pdf_licencia, ruta_pdf_seguridad_social FROM conductores_flotaespecial WHERE id_empresa = %s", (nit_empresa,))
        conductores_especial = cur.fetchall()
        for c in conductores_especial:
            if c.get('estado_licencia') == 'Actualizado' and c.get('ruta_pdf_licencia'):
                documentos_pendientes.append({"tipo": "conductor_especial", "id": c['id'], "identificador": f"{c['nombre']} ({c['cedula']})", "doc_nombre": "Licencia de Conducción", "clave": "estado_licencia", "ruta": c['ruta_pdf_licencia']})
            if c.get('estado_seguridad_social') == 'Actualizado' and c.get('ruta_pdf_seguridad_social'):
                documentos_pendientes.append({"tipo": "conductor_especial", "id": c['id'], "identificador": f"{c['nombre']} ({c['cedula']})", "doc_nombre": "Seguridad Social", "clave": "estado_seguridad_social", "ruta": c['ruta_pdf_seguridad_social']})
    except Exception as e:
        print(f"Aviso - Error consultando conductores especial: {e}")

    cur.close()

    return render_template(
        'C_auditorias_PESV.html',
        nit=nit_empresa,
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        documentos_pendientes=documentos_pendientes,
        active_module='verificacion'
    )

@bp_auditorias_pesv.route('/dictaminar_documento', methods=['POST'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def dictaminar_documento():
    nit_empresa = session.get('empresa_id')
    tipo_entidad = request.form.get('tipo_entidad')
    entidad_id = request.form.get('entidad_id')
    clave_estado = request.form.get('clave_estado')
    dictamen = request.form.get('dictamen')
    
    if not all([tipo_entidad, entidad_id, clave_estado, dictamen]):
        flash("Datos incompletos para el dictamen.", "danger")
        return redirect(url_for('auditorias_pesv.verificacion_documentos'))
        
    if dictamen not in ['Verificado', 'No Conforme']:
        flash("Dictamen no válido.", "danger")
        return redirect(url_for('auditorias_pesv.verificacion_documentos'))

    try:
        cur = mysql.connection.cursor()
        
        tabla = ""
        if tipo_entidad == 'vehiculo_carga': tabla = "vehiculos"
        elif tipo_entidad == 'vehiculo_especial': tabla = "vehiculos_especial"
        elif tipo_entidad == 'conductor_carga': tabla = "conductores_carga"
        elif tipo_entidad == 'conductor_especial': tabla = "conductores_flotaespecial"
        
        if tabla == "vehiculos" and clave_estado == "estado_poliza_rce":
            clave_estado = "estado_rcc_rce"
            
        query = f"UPDATE {tabla} SET {clave_estado} = %s WHERE id = %s AND id_empresa = %s"
        cur.execute(query, (dictamen, entidad_id, nit_empresa))
        mysql.connection.commit()
        cur.close()
        
        flash(f"Documento auditado correctamente. Estado: {dictamen}", "success" if dictamen == 'Verificado' else "warning")
    except Exception as e:
        mysql.connection.rollback()
        print(f"Error al dictaminar documento: {e}")
        flash("Error en base de datos al guardar el dictamen.", "danger")

    return redirect(url_for('auditorias_pesv.verificacion_documentos'))


# ==============================================================================
# NUEVO: CONSULTA DE PREOPERACIONALES CONSOLIDADA
# ==============================================================================
@bp_auditorias_pesv.route('/preoperacionales', methods=['GET'])
@login_required_custom
@pesv_required
@submodulo_required('pesv')
def consulta_preoperacionales():
    nit_empresa = session.get('empresa_id')
    fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
    fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
    placa_filtro = request.args.get('placa', 'todas')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    vehiculos_list = []

    # 1. Consolidar placas de ambas flotas (Tolerante a fallos)
    try:
        cur.execute("SELECT placa FROM vehiculos WHERE id_empresa = %s", (nit_empresa,))
        vehiculos_list.extend(cur.fetchall())
    except Exception as e:
        print(f"Aviso - No se pudo consultar vehículos de carga: {e}")

    try:
        cur.execute("SELECT placa FROM vehiculos_especial WHERE id_empresa = %s", (nit_empresa,))
        vehiculos_list.extend(cur.fetchall())
    except Exception as e:
        print(f"Aviso - No se pudo consultar vehículos especiales: {e}")

    # Eliminar duplicados y ordenar
    seen_placas = set()
    vehiculos_historicos = []
    for v in vehiculos_list:
        if v['placa'] not in seen_placas:
            vehiculos_historicos.append(v)
            seen_placas.add(v['placa'])
    vehiculos_historicos.sort(key=lambda x: x['placa'])

    # 2. Consultar Preoperacionales (Estrictamente filtrados por id_empresa)
    inspecciones = []
    try:
        query = """
            SELECT id_inspeccion, consecutivo_anual, fecha_inspeccion, hora_inspeccion, 
                   placa_vehiculo, nombre_conductor, vehiculo_aprobado 
            FROM inspeccion_preoperacional 
            WHERE id_empresa = %s AND fecha_inspeccion BETWEEN %s AND %s
        """
        params = [nit_empresa, fecha_inicio, fecha_fin]
        
        if placa_filtro != 'todas':
            query += " AND placa_vehiculo = %s"
            params.append(placa_filtro)
            
        query += " ORDER BY fecha_inspeccion DESC, hora_inspeccion DESC"
        
        cur.execute(query, tuple(params))
        inspecciones = cur.fetchall()
    except Exception as e:
        print(f"Error consultando preoperacionales PESV: {e}")
    finally:
        cur.close()

    return render_template(
        'C_auditorias_PESV.html',
        nit=nit_empresa,
        empresa=session.get('empresa'),
        nombre=session.get('nombre'),
        active_module='preoperacionales',
        inspecciones=inspecciones,
        vehiculos_historicos=vehiculos_historicos,
        filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'placa': placa_filtro}
    )