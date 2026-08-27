# app/blueprints/B_bp_flotaespecial_vehiculos.py
import os
import io
import json
import re
import uuid
import pdfplumber
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file, current_app
from werkzeug.utils import secure_filename
from app import mysql
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta
import base64
from PIL import Image as PILImage

# Librerías PDF (Reportes Platypus)
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

bp_flotaespecial_vehiculos = Blueprint('flotaespecial_vehiculos', __name__, url_prefix='/gestor_flotaespecial/vehiculos_bp')

def controlador_flotaespecial_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['controlador_flotaespecial', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Transporte Especial para ingresar a este módulo.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# HELPER: MIGRACIÓN DE COLUMNAS NUEVAS DE VEHÍCULOS
# =========================================================
def asegurar_columnas_vehiculos(cur):
    # Capa de seguridad por si no se ejecutan los scripts SQL manualmente
    columnas = [
        ("vin", "VARCHAR(100)"), ("numero_serie", "VARCHAR(100)"), ("restriccion_movilidad", "VARCHAR(100)"),
        ("blindaje", "VARCHAR(50)"), ("potencia_hp", "VARCHAR(50)"), ("declaracion_importacion", "VARCHAR(100)"),
        ("fecha_importacion", "DATE"), ("puertas", "INT"), ("limitacion_propiedad", "VARCHAR(255)"),
        ("fecha_expedicion_licencia", "DATE"), ("servicio", "VARCHAR(50)"), ("modalidad_servicio", "VARCHAR(100)"),
        ("nivel_servicio", "VARCHAR(100)"), ("radio_accion", "VARCHAR(100)"), ("fecha_expedicion_tarjeta_operacion", "DATE"),
        ("fecha_inicio_tarjeta_operacion", "DATE"), ("ruta_pdf_tarjeta_operacion", "VARCHAR(255)"),
        ("fecha_inicio_rcc_rce", "DATE"), ("ruta_pdf_rcc_rce", "VARCHAR(255)"), ("empresa_transporte", "VARCHAR(150)")
    ]
    for col, tipo in columnas:
        try: cur.execute(f"ALTER TABLE vehiculos_especial ADD COLUMN {col} {tipo}")
        except: pass
        
    cur.execute("""
        CREATE TABLE IF NOT EXISTS empresas_transporte_especial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            nombre VARCHAR(150) NOT NULL,
            nit VARCHAR(50) DEFAULT NULL,
            departamento_sede VARCHAR(100) DEFAULT NULL,
            municipio_sede VARCHAR(100) DEFAULT NULL,
            telefono_contacto VARCHAR(50) DEFAULT NULL,
            email_contacto VARCHAR(100) DEFAULT NULL,
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """)

# =========================================================
# HELPER: CONVERSIÓN DE FECHA HTML
# =========================================================
def _formatear_fecha(cadena_fecha):
    if not cadena_fecha: return None
    m = re.search(r'([0-9]{1,2})[\-\/]([0-9]{1,2})[\-\/]([0-9]{4})', cadena_fecha)
    if m:
        dia, mes, anio = m.group(1).zfill(2), m.group(2).zfill(2), m.group(3)
        return f"{anio}-{mes}-{dia}"
    return None

# =========================================================
# GESTIÓN DE VEHÍCULOS, TERCEROS Y PREOPERACIONALES
# =========================================================
@bp_flotaespecial_vehiculos.route('/', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_vehiculos():
    empresa_id = session.get('empresa_id')
    active_module = request.args.get('active_module', 'vehiculos')
    datos_extraidos = None

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    asegurar_columnas_vehiculos(cur)
    mysql.connection.commit()

    if request.method == 'POST':
        accion = request.form.get('accion')
        
        # ----------------------------------------------------
        # GESTIÓN DE EMPRESAS TERCERAS (CRUD)
        # ----------------------------------------------------
        if accion == 'crear_tercero':
            nombre = request.form.get('nombre', '').strip()
            nit = request.form.get('nit', '').strip()
            dep = request.form.get('departamento_sede', '').strip()
            mun = request.form.get('municipio_sede', '').strip()
            tel = request.form.get('telefono_contacto', '').strip()
            email = request.form.get('email_contacto', '').strip()
            
            try:
                cur.execute("""
                    INSERT INTO empresas_transporte_especial 
                    (id_empresa, nombre, nit, departamento_sede, municipio_sede, telefono_contacto, email_contacto)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (empresa_id, nombre, nit, dep, mun, tel, email))
                mysql.connection.commit()
                flash("Empresa tercera registrada exitosamente.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al registrar tercero: {str(e)}", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='terceros'))

        elif accion == 'editar_tercero':
            t_id = request.form.get('tercero_id')
            nombre = request.form.get('nombre', '').strip()
            nit = request.form.get('nit', '').strip()
            dep = request.form.get('departamento_sede', '').strip()
            mun = request.form.get('municipio_sede', '').strip()
            tel = request.form.get('telefono_contacto', '').strip()
            email = request.form.get('email_contacto', '').strip()
            
            try:
                cur.execute("""
                    UPDATE empresas_transporte_especial 
                    SET nombre=%s, nit=%s, departamento_sede=%s, municipio_sede=%s, telefono_contacto=%s, email_contacto=%s
                    WHERE id=%s AND id_empresa=%s
                """, (nombre, nit, dep, mun, tel, email, t_id, empresa_id))
                mysql.connection.commit()
                flash("Información de empresa tercera actualizada.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al actualizar tercero: {str(e)}", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='terceros'))
            
        elif accion == 'eliminar_tercero':
            t_id = request.form.get('tercero_id')
            try:
                cur.execute("DELETE FROM empresas_transporte_especial WHERE id=%s AND id_empresa=%s", (t_id, empresa_id))
                mysql.connection.commit()
                flash("Empresa tercera eliminada.", "success")
            except Exception as e:
                mysql.connection.rollback()
                flash(f"Error al eliminar tercero.", "danger")
            return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='terceros'))

        # ----------------------------------------------------
        # EXTRACCIÓN DE EXPEDIENTES (OCR)
        # ----------------------------------------------------
        elif accion == 'extraer_pdfs':
            active_module = 'crear_vehiculo'
            datos_extraidos = {}
            
            tipo_vinculacion_ext = request.form.get('tipo_vinculacion_ext', 'Propio')
            empresa_tercera_id = request.form.get('empresa_tercera_ext', '')
            
            datos_extraidos['tipo_vinculacion'] = tipo_vinculacion_ext
            if tipo_vinculacion_ext == 'Tercero' and empresa_tercera_id:
                cur.execute("SELECT nombre, nit FROM empresas_transporte_especial WHERE id=%s AND id_empresa=%s", (empresa_tercera_id, empresa_id))
                emp_terc = cur.fetchone()
                if emp_terc:
                    datos_extraidos['empresa_vinculadora'] = emp_terc['nombre']
                    datos_extraidos['nit_empresa_vinculadora'] = emp_terc['nit']

            ruta_base = os.path.join(current_app.static_folder, 'uploads', 'flotaespecial', 'vehiculos')
            os.makedirs(ruta_base, exist_ok=True)

            # 1. Extracción Licencia de Tránsito
            pdf_prop = request.files.get('pdf_propiedad')
            if pdf_prop and pdf_prop.filename.endswith('.pdf'):
                filename = secure_filename(f"prop_{uuid.uuid4().hex[:8]}.pdf")
                ruta_guardado = os.path.join(ruta_base, filename)
                pdf_prop.save(ruta_guardado)
                datos_extraidos['ruta_pdf_tarjeta_propiedad'] = f"uploads/flotaespecial/vehiculos/{filename}"
                
                try:
                    with pdfplumber.open(ruta_guardado) as pdf:
                        texto_prop = "\n".join([p.extract_text() or "" for p in pdf.pages])
                    
                    m_placa = re.search(r'PLACA[\s\n]*([A-Z0-9]{6})', texto_prop, flags=re.IGNORECASE)
                    if m_placa: datos_extraidos['placa'] = m_placa.group(1)
                    
                    m_marca = re.search(r'MARCA[\s\n]+([A-Za-z0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_marca: datos_extraidos['marca'] = m_marca.group(1)
                    
                    m_linea = re.search(r'LINEA[\s\n]+([A-Za-z0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_linea: datos_extraidos['linea'] = m_linea.group(1)
                    
                    m_modelo = re.search(r'MODELO[\s\n]*([0-9]{4})', texto_prop, flags=re.IGNORECASE)
                    if m_modelo: datos_extraidos['modelo'] = m_modelo.group(1)
                    
                    m_motor = re.search(r'N[UÚ]MERO DE MOTOR[\s\n]+([A-Z0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_motor: datos_extraidos['motor'] = m_motor.group(1)
                    
                    m_chasis = re.search(r'N[UÚ]MERO DE CHASIS[\s\n]+([A-Z0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_chasis: datos_extraidos['chasis'] = m_chasis.group(1)
                    
                    m_vin = re.search(r'VIN[\s\n]+([A-Z0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_vin: datos_extraidos['vin'] = m_vin.group(1)
                    
                    m_serie = re.search(r'N[UÚ]MERO DE SERIE[\s\n]+([A-Z0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_serie: datos_extraidos['numero_serie'] = m_serie.group(1)
                    
                    m_clase = re.search(r'CLASE DE VEH[ÍI]CULO[\s\n]+([A-Za-z]+)', texto_prop, flags=re.IGNORECASE)
                    if m_clase: datos_extraidos['clase'] = m_clase.group(1)
                    
                    m_carroceria = re.search(r'TIPO CARROCER[ÍI]A[\s\n]+([A-Za-z]+)', texto_prop, flags=re.IGNORECASE)
                    if m_carroceria: datos_extraidos['carroceria'] = m_carroceria.group(1)
                    
                    m_color = re.search(r'COLOR[\s\n]+([A-Za-z\s]+)(?:N[UÚ]MERO|TIPO)', texto_prop, flags=re.IGNORECASE)
                    if m_color: datos_extraidos['color'] = m_color.group(1).strip()
                    
                    m_cilind = re.search(r'CILINDRADA CC[\s\n]+([0-9\.]+)', texto_prop, flags=re.IGNORECASE)
                    if m_cilind: datos_extraidos['cilindraje'] = m_cilind.group(1).replace('.', '')
                    
                    m_cap = re.search(r'CAPACIDAD Kg/PSJ[\s\n]+([0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_cap: datos_extraidos['capacidad'] = m_cap.group(1)
                    
                    m_comb = re.search(r'COMBUSTIBLE[\s\n]+([A-Za-z]+)', texto_prop, flags=re.IGNORECASE)
                    if m_comb: datos_extraidos['combustible'] = m_comb.group(1)
                    
                    m_serv = re.search(r'SERVICIO[\s\n]+([A-Za-zÚú]+)', texto_prop, flags=re.IGNORECASE)
                    if m_serv: datos_extraidos['servicio'] = m_serv.group(1)
                    
                    m_puertas = re.search(r'PUERTAS[\s\n]+([0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_puertas: datos_extraidos['puertas'] = m_puertas.group(1)

                    m_hp = re.search(r'POTENCIA HP[\s\n]+([0-9]+)', texto_prop, flags=re.IGNORECASE)
                    if m_hp: datos_extraidos['potencia_hp'] = m_hp.group(1)
                    
                    m_org = re.search(r'ORGANISMO DE TRANSITO[\s\n]+([A-Za-z\s]+)', texto_prop, flags=re.IGNORECASE)
                    if m_org: datos_extraidos['organismo_transito'] = m_org.group(1).strip()

                    m_f_mat = re.search(r'FECHA MATRICULA[\s\n]+([0-9]{2}/[0-9]{2}/[0-9]{4})', texto_prop, flags=re.IGNORECASE)
                    if m_f_mat: datos_extraidos['fecha_matricula'] = _formatear_fecha(m_f_mat.group(1))

                    m_f_exp = re.search(r'FECHA EXP. LIC. TTO.[\s\n]+([0-9]{2}/[0-9]{2}/[0-9]{4})', texto_prop, flags=re.IGNORECASE)
                    if m_f_exp: datos_extraidos['fecha_expedicion_licencia'] = _formatear_fecha(m_f_exp.group(1))

                    m_lim = re.search(r'LIMITACI[OÓ]N A LA PROPIEDAD[\s\n]+([A-Za-z0-9\-\.\s]+?)(?:FECHA|$)', texto_prop, flags=re.IGNORECASE)
                    if m_lim: datos_extraidos['limitacion_propiedad'] = m_lim.group(1).strip()
                    
                except Exception as e:
                    flash(f"Error procesando Licencia de Tránsito: {e}", "warning")

            # 2. Extracción Tarjeta de Operación
            pdf_ope = request.files.get('pdf_operacion')
            if pdf_ope and pdf_ope.filename.endswith('.pdf'):
                filename = secure_filename(f"ope_{uuid.uuid4().hex[:8]}.pdf")
                ruta_guardado = os.path.join(ruta_base, filename)
                pdf_ope.save(ruta_guardado)
                datos_extraidos['ruta_pdf_tarjeta_operacion'] = f"uploads/flotaespecial/vehiculos/{filename}"
                
                try:
                    with pdfplumber.open(ruta_guardado) as pdf:
                        texto_ope = "\n".join([p.extract_text() or "" for p in pdf.pages])
                    
                    m_placa = re.search(r'PLACA:[\s\n]*([A-Z0-9]{6})', texto_ope, flags=re.IGNORECASE)
                    if m_placa and not datos_extraidos.get('placa'): datos_extraidos['placa'] = m_placa.group(1)
                    
                    m_no_to = re.search(r'TARJETA DE OPERACI[OÓ]N\s*No\.?\s*([A-Z0-9]+)', texto_ope, flags=re.IGNORECASE)
                    if m_no_to: datos_extraidos['numero_tarjeta_operacion'] = m_no_to.group(1)

                    m_mod = re.search(r'MODALIDAD DE SERVICIO:[\s\n]*([A-Za-z]+)', texto_ope, flags=re.IGNORECASE)
                    if m_mod: datos_extraidos['modalidad_servicio'] = m_mod.group(1)

                    m_radio = re.search(r'RADIO DE ACCI[OÓ]N:[\s\n]*([A-Za-z]+)', texto_ope, flags=re.IGNORECASE)
                    if m_radio: datos_extraidos['radio_accion'] = m_radio.group(1)

                    # Si NO es tercero explícito por select, intentamos sacar la empresa vinculadora del PDF
                    if tipo_vinculacion_ext != 'Tercero':
                        m_emp = re.search(r'RAZ[OÓ]N SOCIAL EMPRESA:[\s\n]*([A-Za-z\.\s]+)NIT:', texto_ope, flags=re.IGNORECASE)
                        if m_emp: datos_extraidos['empresa_vinculadora'] = m_emp.group(1).strip()

                        m_nit = re.search(r'NIT:[\s\n]*([0-9\-]+)', texto_ope, flags=re.IGNORECASE)
                        if m_nit: datos_extraidos['nit_empresa_vinculadora'] = m_nit.group(1)

                    m_f_exp = re.search(r'FECHA DE EXPEDICI[OÓ]N:[\s\n]*([0-9]{2}[\-\/][0-9]{2}[\-\/][0-9]{4})', texto_ope, flags=re.IGNORECASE)
                    if m_f_exp: datos_extraidos['fecha_expedicion_tarjeta_operacion'] = _formatear_fecha(m_f_exp.group(1))

                    m_f_ini = re.search(r'DESDE:[\s\n]*([0-9]{2}[\-\/][0-9]{2}[\-\/][0-9]{4})', texto_ope, flags=re.IGNORECASE)
                    if m_f_ini: datos_extraidos['fecha_inicio_tarjeta_operacion'] = _formatear_fecha(m_f_ini.group(1))

                    m_f_fin = re.search(r'HASTA:[\s\n]*([0-9]{2}[\-\/][0-9]{2}[\-\/][0-9]{4})', texto_ope, flags=re.IGNORECASE)
                    if m_f_fin: datos_extraidos['vencimiento_tarjeta_operacion'] = _formatear_fecha(m_f_fin.group(1))

                except Exception as e:
                    flash(f"Error procesando Tarjeta de Operación: {e}", "warning")

            # 3. Extracción Póliza RCC-RCE
            pdf_pol = request.files.get('pdf_poliza')
            if pdf_pol and pdf_pol.filename.endswith('.pdf'):
                filename = secure_filename(f"rcc_{uuid.uuid4().hex[:8]}.pdf")
                ruta_guardado = os.path.join(ruta_base, filename)
                pdf_pol.save(ruta_guardado)
                datos_extraidos['ruta_pdf_rcc_rce'] = f"uploads/flotaespecial/vehiculos/{filename}"
                
                try:
                    with pdfplumber.open(ruta_guardado) as pdf:
                        texto_pol = "\n".join([p.extract_text() or "" for p in pdf.pages])
                    
                    m_placa = re.search(r'PLACA:[\s\n]*([A-Z0-9]{6})', texto_pol, flags=re.IGNORECASE)
                    if m_placa and not datos_extraidos.get('placa'): datos_extraidos['placa'] = m_placa.group(1)

                    m_no_pol = re.search(r'No\.\s*P[oó]liza[\s\n]+([0-9\-]+)', texto_pol, flags=re.IGNORECASE)
                    if m_no_pol: datos_extraidos['numero_poliza_rcc_rce'] = m_no_pol.group(1)

                    # Captura Aseguradora genérica por cabecera
                    datos_extraidos['aseguradora_rcc_rce'] = "Seguros del Estado S.A." if "SEGUROS DEL ESTADO" in texto_pol.upper() else "Otra"

                    fechas_poliza = re.findall(r'([0-9]{2}[\s\-\/]*[0-9]{2}[\s\-\/]*[0-9]{4})', texto_pol)
                    if len(fechas_poliza) >= 2:
                        datos_extraidos['fecha_inicio_rcc_rce'] = _formatear_fecha(fechas_poliza[0].replace(' ', '-'))
                        datos_extraidos['vencimiento_rcc_rce'] = _formatear_fecha(fechas_poliza[1].replace(' ', '-'))

                except Exception as e:
                    flash(f"Error procesando Pólizas: {e}", "warning")

            if datos_extraidos:
                flash("Documentos escaneados con éxito. Revise y complete la información en el formulario.", "success")
            else:
                flash("No se subieron PDFs o no se pudo extraer información legible.", "info")

        # ----------------------------------------------------
        # CRUD DE VEHÍCULOS (Guardar Manual / Edit)
        # ----------------------------------------------------
        elif accion in ['crear', 'editar']:
            v_id = request.form.get('vehiculo_id')
            placa = str(request.form.get('placa', '')).upper().strip()
            
            clase = request.form.get('clase', '').strip()
            carroceria = request.form.get('carroceria', '').strip()
            marca = request.form.get('marca', '').strip()
            linea = request.form.get('linea', '').strip()
            modelo = request.form.get('modelo', '').strip()
            color = request.form.get('color', '').strip()
            combustible = request.form.get('combustible', '').strip()
            cilindraje = request.form.get('cilindraje', '').strip()
            capacidad = request.form.get('capacidad') or 0
            potencia_hp = request.form.get('potencia_hp', '').strip()
            puertas = request.form.get('puertas') or 0
            
            vin = request.form.get('vin', '').strip()
            chasis = request.form.get('chasis', '').strip()
            motor = request.form.get('motor', '').strip()
            numero_serie = request.form.get('numero_serie', '').strip()
            
            servicio = request.form.get('servicio', '').strip()
            organismo_transito = request.form.get('organismo_transito', '').strip()
            fecha_matricula = request.form.get('fecha_matricula') or None
            fecha_expedicion_licencia = request.form.get('fecha_expedicion_licencia') or None
            limitacion_propiedad = request.form.get('limitacion_propiedad', '').strip()
            declaracion_importacion = request.form.get('declaracion_importacion', '').strip()
            fecha_importacion = request.form.get('fecha_importacion') or None
            restriccion_movilidad = request.form.get('restriccion_movilidad', '').strip()
            blindaje = request.form.get('blindaje', '').strip()
            
            tipo_vinculacion = request.form.get('tipo_vinculacion', 'Propio').strip()
            empresa_vinculadora = request.form.get('empresa_vinculadora', '').strip()
            
            # LÓGICA DE ASIGNACIÓN EMPRESA TRANSPORTE
            if tipo_vinculacion == 'Propio':
                empresa_transporte = session.get('empresa')
                empresa_vinculadora = session.get('empresa')
                nit_empresa_vinculadora = session.get('nit')
            else:
                empresa_transporte = empresa_vinculadora
                nit_empresa_vinculadora = request.form.get('nit_empresa_vinculadora', '').strip()
                # Extraer NIT automáticamente si se seleccionó del desplegable
                if empresa_vinculadora:
                    cur.execute("SELECT nit FROM empresas_transporte_especial WHERE nombre=%s AND id_empresa=%s", (empresa_vinculadora, empresa_id))
                    t_data = cur.fetchone()
                    if t_data and t_data['nit']:
                        nit_empresa_vinculadora = t_data['nit']
            
            numero_tarjeta_operacion = request.form.get('numero_tarjeta_operacion', '').strip()
            modalidad_servicio = request.form.get('modalidad_servicio', '').strip()
            nivel_servicio = request.form.get('nivel_servicio', '').strip()
            radio_accion = request.form.get('radio_accion', '').strip()
            fecha_expedicion_tarjeta_operacion = request.form.get('fecha_expedicion_tarjeta_operacion') or None
            fecha_inicio_tarjeta_operacion = request.form.get('fecha_inicio_tarjeta_operacion') or None
            vencimiento_tarjeta_operacion = request.form.get('vencimiento_tarjeta_operacion') or None
            
            numero_poliza_rcc_rce = request.form.get('numero_poliza_rcc_rce', '').strip()
            aseguradora_rcc_rce = request.form.get('aseguradora_rcc_rce', '').strip()
            fecha_inicio_rcc_rce = request.form.get('fecha_inicio_rcc_rce') or None
            vencimiento_rcc_rce = request.form.get('vencimiento_rcc_rce') or None
            
            numero_poliza_soat = request.form.get('numero_poliza_soat', '').strip()
            vencimiento_soat = request.form.get('vencimiento_soat') or None
            
            r_prop = request.form.get('ruta_pdf_tarjeta_propiedad', '')
            r_ope = request.form.get('ruta_pdf_tarjeta_operacion', '')
            r_rcc = request.form.get('ruta_pdf_rcc_rce', '')
            
            if placa and clase:
                try:
                    if accion == 'crear':
                        cur.execute("""
                            INSERT INTO vehiculos_especial 
                            (id_empresa, placa, clase, carroceria, marca, linea, modelo, color, combustible, 
                             cilindraje, capacidad, potencia_hp, puertas, vin, chasis, motor, numero_serie,
                             servicio, organismo_transito, fecha_matricula, fecha_expedicion_licencia,
                             limitacion_propiedad, declaracion_importacion, fecha_importacion,
                             restriccion_movilidad, blindaje, tipo_vinculacion, numero_tarjeta_operacion,
                             modalidad_servicio, nivel_servicio, radio_accion, empresa_vinculadora,
                             nit_empresa_vinculadora, empresa_transporte, fecha_expedicion_tarjeta_operacion,
                             fecha_inicio_tarjeta_operacion, vencimiento_tarjeta_operacion,
                             numero_poliza_rcc_rce, aseguradora_rcc_rce, fecha_inicio_rcc_rce, vencimiento_rcc_rce,
                             numero_poliza_soat, vencimiento_soat,
                             ruta_pdf_tarjeta_propiedad, ruta_pdf_tarjeta_operacion, ruta_pdf_rcc_rce) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            empresa_id, placa, clase, carroceria, marca, linea, modelo, color, combustible,
                            cilindraje, capacidad, potencia_hp, puertas, vin, chasis, motor, numero_serie,
                            servicio, organismo_transito, fecha_matricula, fecha_expedicion_licencia,
                            limitacion_propiedad, declaracion_importacion, fecha_importacion,
                            restriccion_movilidad, blindaje, tipo_vinculacion, numero_tarjeta_operacion,
                            modalidad_servicio, nivel_servicio, radio_accion, empresa_vinculadora,
                            nit_empresa_vinculadora, empresa_transporte, fecha_expedicion_tarjeta_operacion,
                            fecha_inicio_tarjeta_operacion, vencimiento_tarjeta_operacion,
                            numero_poliza_rcc_rce, aseguradora_rcc_rce, fecha_inicio_rcc_rce, vencimiento_rcc_rce,
                            numero_poliza_soat, vencimiento_soat, r_prop, r_ope, r_rcc
                        ))
                        mysql.connection.commit()
                        flash(f"Vehículo especial {placa} registrado con todo el expediente.", "success")
                        
                    elif accion == 'editar' and v_id:
                        cur.execute("""
                            UPDATE vehiculos_especial 
                            SET placa=%s, clase=%s, carroceria=%s, marca=%s, linea=%s, modelo=%s, color=%s, 
                                combustible=%s, cilindraje=%s, capacidad=%s, potencia_hp=%s, puertas=%s, 
                                vin=%s, chasis=%s, motor=%s, numero_serie=%s,
                                servicio=%s, organismo_transito=%s, fecha_matricula=%s, fecha_expedicion_licencia=%s,
                                limitacion_propiedad=%s, declaracion_importacion=%s, fecha_importacion=%s,
                                restriccion_movilidad=%s, blindaje=%s, tipo_vinculacion=%s, numero_tarjeta_operacion=%s,
                                modalidad_servicio=%s, nivel_servicio=%s, radio_accion=%s, empresa_vinculadora=%s,
                                nit_empresa_vinculadora=%s, empresa_transporte=%s, fecha_expedicion_tarjeta_operacion=%s,
                                fecha_inicio_tarjeta_operacion=%s, vencimiento_tarjeta_operacion=%s,
                                numero_poliza_rcc_rce=%s, aseguradora_rcc_rce=%s, fecha_inicio_rcc_rce=%s, vencimiento_rcc_rce=%s,
                                numero_poliza_soat=%s, vencimiento_soat=%s
                            WHERE id=%s AND id_empresa=%s
                        """, (
                            placa, clase, carroceria, marca, linea, modelo, color, combustible,
                            cilindraje, capacidad, potencia_hp, puertas, vin, chasis, motor, numero_serie,
                            servicio, organismo_transito, fecha_matricula, fecha_expedicion_licencia,
                            limitacion_propiedad, declaracion_importacion, fecha_importacion,
                            restriccion_movilidad, blindaje, tipo_vinculacion, numero_tarjeta_operacion,
                            modalidad_servicio, nivel_servicio, radio_accion, empresa_vinculadora,
                            nit_empresa_vinculadora, empresa_transporte, fecha_expedicion_tarjeta_operacion,
                            fecha_inicio_tarjeta_operacion, vencimiento_tarjeta_operacion,
                            numero_poliza_rcc_rce, aseguradora_rcc_rce, fecha_inicio_rcc_rce, vencimiento_rcc_rce,
                            numero_poliza_soat, vencimiento_soat,
                            v_id, empresa_id
                        ))
                        if r_prop: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_tarjeta_propiedad=%s WHERE id=%s", (r_prop, v_id))
                        if r_ope: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_tarjeta_operacion=%s WHERE id=%s", (r_ope, v_id))
                        if r_rcc: cur.execute("UPDATE vehiculos_especial SET ruta_pdf_rcc_rce=%s WHERE id=%s", (r_rcc, v_id))
                        
                        mysql.connection.commit()
                        flash(f"Expediente del vehículo {placa} actualizado correctamente.", "success")
                        
                except Exception as e:
                    mysql.connection.rollback()
                    flash(f"Error en base de datos: {str(e)}", "danger")
            else:
                flash("Faltan datos obligatorios (Placa y Clase).", "warning")

        elif accion == 'eliminar':
            vehiculo_id = request.form.get('vehiculo_id')
            try:
                cur.execute("DELETE FROM vehiculos_especial WHERE id = %s AND id_empresa = %s", (vehiculo_id, empresa_id))
                mysql.connection.commit()
                flash("Vehículo especial eliminado de la base de datos.", "success")
            except Exception as e:
                flash("Error al eliminar vehículo.", "danger")

        return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='vehiculos'))

    # ================= MODO LECTURA / VISTA =================
    if active_module == 'preoperacionales':
        fecha_inicio = request.args.get('fecha_inicio', (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'))
        fecha_fin = request.args.get('fecha_fin', datetime.now().strftime('%Y-%m-%d'))
        placa_filtro = request.args.get('placa', 'todas')

        cur.execute("SELECT DISTINCT placa FROM vehiculos_especial WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
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
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='preoperacionales', inspecciones=inspecciones,
            vehiculos_historicos=vehiculos_historicos,
            filtros={'fecha_inicio': fecha_inicio, 'fecha_fin': fecha_fin, 'placa': placa_filtro}
        )
        
    elif active_module == 'terceros':
        cur.execute("SELECT * FROM empresas_transporte_especial WHERE id_empresa = %s ORDER BY nombre ASC", (empresa_id,))
        terceros_db = cur.fetchall()
        cur.close()
        
        return render_template(
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='terceros', terceros=terceros_db
        )
        
    else:
        # Cargar todos los vehículos y terceros para el dashboard principal
        cur.execute("SELECT * FROM vehiculos_especial WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
        vehiculos_db = cur.fetchall()
        
        cur.execute("SELECT id, nombre, nit FROM empresas_transporte_especial WHERE id_empresa = %s ORDER BY nombre ASC", (empresa_id,))
        terceros_db = cur.fetchall()
        
        cur.close()

        return render_template(
            'B_modulo_flotaespecial_vehiculos.html',
            nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
            active_module='vehiculos', vehiculos=vehiculos_db, terceros=terceros_db, datos_extraidos=datos_extraidos
        )

@bp_flotaespecial_vehiculos.route('/preoperacionales/pdf/<consecutivo>', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
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
        return redirect(url_for('flotaespecial_vehiculos.gestion_vehiculos', active_module='preoperacionales'))

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
            ('doc_cedula', 'Cédula de Ciudadanía'), ('doc_licencia_conduccion', 'Licencia de Conducción'), 
            ('doc_licencia_transito', 'Licencia de Tránsito (Propiedad)'), ('doc_soat_vigente', 'SOAT Vigente'),
            ('doc_tecnomecanica_vigente', 'Revisión Tecnomecánica'), ('doc_tarjeta_operacion', 'Tarjeta de Operación')
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

    try: novedades_dict = json.loads(insp.get('detalles_novedades_json') or '{}')
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
    try: llantas_dict = json.loads(insp.get('estado_llantas_json') or '{}')
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
        except: pass
        return Paragraph("<i>No disponible</i>", cell_style)

    if insp.get('observaciones_hallazgos'):
        story.append(Spacer(1, 10))
        story.append(Paragraph("<b>OBSERVACIONES GENERALES DEL CONDUCTOR</b>", sub_title_style))
        story.append(Paragraph(f"<i>{insp['observaciones_hallazgos']}</i>", cell_style))

    story.append(Spacer(1, 15))
    story.append(Paragraph("<b>AUTENTICACIÓN Y FIRMA</b>", sub_title_style))
    declaracion_texto = "<b>Declaración de Veracidad y Cumplimiento Normativo:</b> Declaro bajo la gravedad de juramento que la información aquí registrada es veraz, exacta y ha sido recolectada mediante inspección física directa del vehículo. Este registro preoperacional da cumplimiento estricto al <b>Paso 16 de la Metodología del Plan Estratégico de Seguridad Vial (PESV)</b>."
    story.append(Paragraph(declaracion_texto, cell_style))
    story.append(Spacer(1, 10))
    
    img_firma = convertir_base64_rlimage(insp.get('firma_grafica_base64'), 2*inch, 1*inch)
    img_foto = convertir_base64_rlimage(insp.get('foto_conductor_base64'), 1.2*inch, 1.2*inch)

    firma_data = [[Paragraph("<b>Foto Auditoría:</b>", cell_style), Paragraph("<b>Firma Gráfica:</b>", cell_style)], [img_foto, img_firma]]
    t_firma = Table(firma_data, colWidths=[150, 200])
    t_firma.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.HexColor('#16a34a')), ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#e5e7eb')), ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0fdf4')), ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
    story.append(t_firma)

    doc.build(story)
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name=f"Preoperacional_Especial_{consecutivo}.pdf", mimetype='application/pdf')