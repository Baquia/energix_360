# app/blueprints/B_bp_flotaespecial_eps.py
import os
import re
import uuid
import random
import string
import requests
import threading
import hashlib
import pdfplumber
import pytz
import holidays
from datetime import datetime, timedelta

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image as RLImage, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

from flask import Blueprint, render_template, session, redirect, url_for, request, flash, current_app, jsonify
from werkzeug.utils import secure_filename
from app import mysql
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.barcode import qr

bp_flotaespecial_eps = Blueprint('flotaespecial_eps', __name__, url_prefix='/gestor_flotaespecial/eps_bp')

BOGOTA_TZ = pytz.timezone('America/Bogota')

# =========================================================
# HELPER: CONVERSIÓN DE FECHA HTML (YYYY-MM-DD)
# =========================================================
def _convertir_fecha_html(cadena_fecha):
    if not cadena_fecha:
        return ""
    m = re.search(r'([0-9]{1,2})[\-\/]([0-9]{1,2})[\-\/]([0-9]{4})', cadena_fecha)
    if m:
        dia = m.group(1).zfill(2)
        mes = m.group(2).zfill(2)
        anio = m.group(3)
        return f"{anio}-{mes}-{dia}"
    return cadena_fecha

# =========================================================
# HELPER: MAPEO DIVIPOLA DANE (Básico)
# =========================================================
def _obtener_codigo_dane(departamento, municipio):
    dane_map = {
        "TOLIMA": {"IBAGUE": "73001", "GARZON": "73001"}, 
        "HUILA": {"NEIVA": "41001", "GARZON": "41298"},
        "BOGOTA": {"BOGOTA": "11001"},
        "ANTIOQUIA": {"MEDELLIN": "05001"}
    }
    dep = departamento.upper() if departamento else ""
    mun = municipio.upper() if municipio else ""
    return dane_map.get(dep, {}).get(mun, "")

# =========================================================
# HELPER: CÁLCULO DE TURNOS Y FRECUENCIAS (CON FESTIVOS CO)
# =========================================================
def _get_next_date_turno(current_date, turno):
    """Calcula la siguiente fecha válida según el turno de diálisis saltando festivos"""
    co_holidays = holidays.CO(years=[current_date.year, current_date.year + 1])
    next_d = current_date + timedelta(days=1)
    valid_days = [0, 2, 4] if turno == '1' else [1, 3, 5]
    while next_d.weekday() not in valid_days or next_d in co_holidays:
        next_d += timedelta(days=1)
    return next_d

def _get_next_date_frecuencia(current_date, frecuencia):
    """Calcula la siguiente fecha válida según la frecuencia seleccionada saltando domingos y festivos"""
    if frecuencia == 'diaria': next_d = current_date + timedelta(days=1)
    elif frecuencia == 'cada_2_dias': next_d = current_date + timedelta(days=2)
    elif frecuencia == 'cada_3_dias': next_d = current_date + timedelta(days=3)
    elif frecuencia == 'cada_4_dias': next_d = current_date + timedelta(days=4)
    elif frecuencia == 'semanal': next_d = current_date + timedelta(weeks=1)
    elif frecuencia == 'quincenal': next_d = current_date + timedelta(days=15)
    elif frecuencia == 'mensual': next_d = current_date + timedelta(days=30)
    elif frecuencia == 'bimensual': next_d = current_date + timedelta(days=60)
    elif frecuencia == 'trimestral': next_d = current_date + timedelta(days=90)
    else: next_d = current_date
    
    co_holidays = holidays.CO(years=[next_d.year, next_d.year + 1])
    while next_d.weekday() == 6 or next_d in co_holidays:
        next_d += timedelta(days=1)
        
    return next_d

# =========================================================
# HELPER: TELEGRAM Y EMAIL (Motor Dinámico)
# =========================================================
def _enviar_mensajes_telegram_hilo(chat_ids, mensaje):
    TOKEN = "8841682239:AAFOj8TpeOW4ulhIkNoIyGaTZ2MLlI9ydVo"
    def tarea_envio():
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        for chat_id in chat_ids:
            data = {
                "chat_id": chat_id,
                "text": mensaje,
                "parse_mode": "Markdown"
            }
            for intento in range(3):
                try:
                    resp = requests.post(url, data=data, timeout=10)
                    if resp.status_code == 200:
                        break
                except Exception as e:
                    pass
    hilo = threading.Thread(target=tarea_envio)
    hilo.daemon = True
    hilo.start()

def _enviar_documento_telegram_hilo(chat_ids, mensaje, filepath):
    TOKEN = "8841682239:AAFOj8TpeOW4ulhIkNoIyGaTZ2MLlI9ydVo"
    def tarea_envio():
        url = f"https://api.telegram.org/bot{TOKEN}/sendDocument"
        for chat_id in chat_ids:
            try:
                with open(filepath, 'rb') as f:
                    data = {
                        "chat_id": chat_id,
                        "caption": mensaje,
                        "parse_mode": "HTML"
                    }
                    files = {"document": f}
                    for intento in range(3):
                        try:
                            resp = requests.post(url, data=data, files=files, timeout=15)
                            if resp.status_code == 200:
                                break
                        except Exception:
                            pass
            except Exception:
                pass
    hilo = threading.Thread(target=tarea_envio)
    hilo.daemon = True
    hilo.start()

def notificar_programacion_viaje(empresa_id, empresa_nombre, viaje_data, tipo_evento='ASIGNACION'):
    f_serv = viaje_data.get('fecha_servicio') or 'Pendiente'
    h_ini = viaje_data.get('hora_inicio') or 'Pendiente'
    
    telefono_paciente = viaje_data.get('telefono_usuario') or 'N/D'
    info_acompanante = ""
    if viaje_data.get('lleva_acompanante'):
        info_acompanante = f"👥 *Acompañante:* {viaje_data.get('nombre_acompanante', 'Sí')}\n"

    if tipo_evento == 'ASIGNACION':
        titulo_tg = "🟢 *NUEVA ASIGNACIÓN DE VIAJE*"
        cuerpo_info_tg = (
            f"📦 *Cantidad de Viajes Asignados:* 1\n\n"
            f"📋 *PROGRAMACIÓN ESTIMADA:*\n"
            f"🆔 *ID Viaje:* `{viaje_data['id_viaje']}`\n"
            f"👤 *Paciente:* {viaje_data['nombre_usuario']}\n"
            f"📞 *Teléfono:* {telefono_paciente}\n"
            f"{info_acompanante}"
            f"📅 *Fecha Estimada:* {f_serv} | ⏰ *Hora Inicio:* {h_ini}\n"
            f"📍 *Origen Estimado:* {viaje_data['direccion_origen']}\n\n"
            f"ℹ️ _Nota: Los datos detallados de cada servicio (incluyendo el destino exacto) serán notificados X horas antes del inicio del viaje._"
        )
    else:
        if tipo_evento == 'REPROGRAMACION':
            titulo_tg = "🟡 *VIAJE REPROGRAMADO/ACTUALIZADO*"
        elif tipo_evento == 'CANCELACION':
            titulo_tg = "🔴 *VIAJE CANCELADO*"
        elif tipo_evento == 'RECORDATORIO':
            titulo_tg = "⏰ *RECORDATORIO DE VIAJE PRÓXIMO*"
        else:
            titulo_tg = "🚐 *NOTIFICACIÓN DE SERVICIO*"
            
        cuerpo_info_tg = (
            f"🆔 *ID Viaje:* `{viaje_data['id_viaje']}`\n"
            f"👤 *Paciente:* {viaje_data['nombre_usuario']}\n"
            f"📞 *Teléfono:* {telefono_paciente}\n"
            f"{info_acompanante}"
            f"📅 *Fecha:* {f_serv} | ⏰ *Hora:* {h_ini}\n"
            f"📍 *Origen:* {viaje_data['direccion_origen']}\n"
            f"🏁 *Destino:* {viaje_data['direccion_destino']}"
        )

    mensaje_tg = (
        f"{titulo_tg}\n\n"
        f"🏢 *Empresa:* {empresa_nombre}\n"
        f"🚙 *Vehículo:* {viaje_data['vehiculo_asignado']} | 👨‍✈️ *Conductor:* {viaje_data['conductor_asignado']}\n"
        f"📄 *Auth:* {viaje_data['numero_autorizacion']}\n\n"
        f"{cuerpo_info_tg}"
    )
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT telegram_id FROM usuarios 
        WHERE empresa_id = %s AND (nombre = %s OR perfil = 'controlador_flotaespecial')
    """, (empresa_id, viaje_data['conductor_asignado']))
    usuarios = cur.fetchall()
    cur.close()

    chat_ids = [u['telegram_id'] for u in usuarios if u.get('telegram_id')]
    if chat_ids:
        _enviar_mensajes_telegram_hilo(chat_ids, mensaje_tg)

def generar_id_viaje_unico(cur, empresa_id=None):
    for _ in range(10):
        codigo = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if empresa_id:
            cur.execute("SELECT id FROM control_viajes_flota_especial WHERE id_viaje = %s AND id_empresa = %s", (codigo, empresa_id))
        else:
            cur.execute("SELECT id FROM control_viajes_flota_especial WHERE id_viaje = %s", (codigo,))
        if not cur.fetchone():
            return codigo
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

# =========================================================
# HELPER: CÁLCULO CONSECUTIVO FUEC OFICIAL (21 DÍGITOS)
# =========================================================
def generar_consecutivo_fuec(dir_terr, res_hab, anio_hab, anio_exp, num_contrato, cons_extracto):
    dt = str(dir_terr).zfill(3)[:3]
    rh = str(res_hab).zfill(4)[:4]
    ah = str(anio_hab).zfill(2)[:2]
    ae = str(anio_exp).zfill(4)[:4]
    
    num_c_clean = ''.join(filter(str.isdigit, str(num_contrato)))
    if not num_c_clean: num_c_clean = '0000'
    nc = num_c_clean.zfill(4)[-4:]
    ce = str(cons_extracto).zfill(4)[-4:]
    return f"{dt}{rh}{ah}{ae}{nc}{ce}"

# =========================================================
# HELPER: ASEGURAR TABLAS (ORDEN MAESTRA + VIAJES + CONTRATOS)
# =========================================================
def asegurar_tablas_transporte_especial(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS maestra_traslados_eps_tespecial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            eps_cliente VARCHAR(255),
            id_eps_cliente VARCHAR(50),
            fecha_captura DATETIME DEFAULT CURRENT_TIMESTAMP,
            fecha_entrega_servicio DATE,
            numero_autorizacion VARCHAR(100),
            numero_prescripcion VARCHAR(100),
            tipo_servicio VARCHAR(50),
            codigo_servicio VARCHAR(50),
            estatus_servicio VARCHAR(50) DEFAULT 'capturado',
            nombre_usuario VARCHAR(255),
            tipo_documento VARCHAR(20),
            id_usuario VARCHAR(50),
            fecha_nacimiento DATE,
            edad VARCHAR(20),
            sexo VARCHAR(20),
            numero_carne VARCHAR(50),
            tipo_usuario VARCHAR(50),
            nivel_sisben VARCHAR(50),
            telefono_usuario VARCHAR(50),
            email_usuario VARCHAR(100),
            departamento VARCHAR(100),
            municipio VARCHAR(100),
            direccion_origen VARCHAR(255),
            direccion_destino VARCHAR(255),
            numero_traslados_aporbados INT DEFAULT 0,
            numero_traslados_ejecutados INT DEFAULT 0,
            diferencia INT DEFAULT 0,
            observaciones TEXT,
            ruta_documento VARCHAR(255),
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS control_viajes_flota_especial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            id_eps_cliente VARCHAR(50),
            numero_autorizacion VARCHAR(100),
            numero_prescripcion VARCHAR(100),
            tipo_servicio VARCHAR(50),
            estatus_servicio VARCHAR(50) DEFAULT 'CAPTURADO',
            nombre_usuario VARCHAR(255),
            id_usuario VARCHAR(50),
            telefono_usuario VARCHAR(50),
            departamento VARCHAR(100),
            municipio VARCHAR(100),
            direccion_origen VARCHAR(255),
            direccion_destino VARCHAR(255),
            fecha_servicio DATE,
            hora_inicio TIME,
            hora_fin TIME,
            coordenadas_inicio VARCHAR(100),
            coordenadas_fin VARCHAR(100),
            vehiculo_asignado VARCHAR(20),
            conductor_asignado VARCHAR(100),
            id_viaje VARCHAR(10) UNIQUE,
            ruta_documento VARCHAR(255),
            fecha_notificacion_info DATETIME NULL,
            fecha_notificacion_confirmacion DATETIME NULL,
            operador_ejecucion VARCHAR(100) NULL,
            fecha_ejecucion_real DATETIME NULL,
            fecha_fin_real DATETIME NULL,
            recordatorio_enviado BOOLEAN DEFAULT FALSE,
            auditor_nombre VARCHAR(100) NULL,
            fecha_auditoria DATETIME NULL,
            hash_auditoria VARCHAR(255) NULL,
            ruta_pdf_unificado VARCHAR(255) NULL,
            INDEX(id_empresa),
            INDEX(id_viaje)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS contratos_transporte_especial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            numero_contrato VARCHAR(50) NOT NULL,
            contratante_nombre VARCHAR(255) NOT NULL,
            contratante_nit_cedula VARCHAR(50) NOT NULL,
            categoria_contrato VARCHAR(50) NOT NULL,
            objeto_contrato TEXT,
            convenio_colaboracion VARCHAR(255),
            fecha_inicio DATE,
            fecha_fin DATE,
            responsable_nombre VARCHAR(255),
            responsable_cedula VARCHAR(50),
            responsable_direccion VARCHAR(255),
            responsable_telefono VARCHAR(50),
            estado VARCHAR(20) DEFAULT 'ACTIVO',
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN tipo_documento VARCHAR(20) AFTER nombre_usuario")
    except: pass
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN departamento_destino VARCHAR(100) AFTER direccion_origen")
    except: pass
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN municipio_destino VARCHAR(100) AFTER departamento_destino")
    except: pass
    
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN consecutivo_viaje VARCHAR(50) NULL, ADD COLUMN foto_origen VARCHAR(255) NULL, ADD COLUMN foto_destino VARCHAR(255) NULL, ADD COLUMN foto_paciente VARCHAR(255) NULL, ADD COLUMN firma VARCHAR(255) NULL, ADD COLUMN hash_seguridad VARCHAR(255) NULL, ADD COLUMN lat_origen DECIMAL(10,8) NULL, ADD COLUMN lng_origen DECIMAL(11,8) NULL, ADD COLUMN hora_origen DATETIME NULL, ADD COLUMN lat_destino DECIMAL(10,8) NULL, ADD COLUMN lng_destino DECIMAL(11,8) NULL, ADD COLUMN hora_destino DATETIME NULL, ADD COLUMN tiempo_efectivo_minutos INT DEFAULT 0;")
    except: pass

    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN lat_origen_esperado DECIMAL(10,8) NULL, ADD COLUMN lng_origen_esperado DECIMAL(11,8) NULL, ADD COLUMN lat_destino_esperado DECIMAL(10,8) NULL, ADD COLUMN lng_destino_esperado DECIMAL(11,8) NULL;")
    except: pass
    
    try: cur.execute("ALTER TABLE maestra_traslados_eps_tespecial ADD COLUMN departamento_destino VARCHAR(100) AFTER direccion_origen")
    except: pass
    try: cur.execute("ALTER TABLE maestra_traslados_eps_tespecial ADD COLUMN municipio_destino VARCHAR(100) AFTER departamento_destino")
    except: pass

    try: cur.execute("ALTER TABLE empresas ADD COLUMN codigo_direccion_territorial VARCHAR(10) NULL, ADD COLUMN numero_resolucion_habilitacion VARCHAR(50) NULL, ADD COLUMN anio_habilitacion VARCHAR(4) NULL, ADD COLUMN firma_representante_legal VARCHAR(255) NULL;")
    except: pass
    try: cur.execute("ALTER TABLE fuec ADD COLUMN consecutivo_oficial VARCHAR(50) NULL, ADD COLUMN id_contrato INT NULL, ADD COLUMN categoria_contrato VARCHAR(50) NULL;")
    except: pass
    
    try: cur.execute("ALTER TABLE vehiculos_especial ADD COLUMN capacidad_pasajeros INT DEFAULT 4;")
    except: pass

def controlador_flotaespecial_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['controlador_flotaespecial', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Transporte Especial.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# REDIRECCIÓN RAÍZ
# =========================================================
@bp_flotaespecial_eps.route('/', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_eps():
    return redirect(url_for('flotaespecial_eps.gestion_traslados_captura'))

# =========================================================
# ETAPA 1: CAPTURA (MAESTRA) 
# =========================================================
@bp_flotaespecial_eps.route('/captura', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_captura():
    empresa_id = session.get('empresa_id')
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    asegurar_tablas_transporte_especial(cur)
    mysql.connection.commit()
    
    datos_extraidos = None
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        
        if accion == 'subir_pdf':
            archivo = request.files.get('archivo_pdf')
            if archivo and archivo.filename.endswith('.pdf'):
                try:
                    texto_completo = ""
                    with pdfplumber.open(archivo) as pdf:
                        for pagina in pdf.pages:
                            texto_extraido = pagina.extract_text()
                            if texto_extraido:
                                texto_completo += texto_extraido + "\n"
                    
                    datos_extraidos = {
                        "eps_nombre": "", "eps_nit": "", "numero_autorizacion": "",
                        "numero_prescripcion": "", "numero_orden": "", "id_usuario": "",
                        "nombre_usuario": "", "tipo_documento": "", "fecha_nacimiento": "",
                        "edad": "", "sexo": "", "numero_carne": "", "tipo_usuario": "",
                        "nivel_sisben": "", "telefono_usuario": "", "email_usuario": "",
                        "departamento": "", "municipio": "", "direccion_origen": "",
                        "codigo_servicio": "", "cantidad": "1", "fecha_entrega_servicio": "",
                        "ruta_documento": "",
                        "cod_mun_ent": "", "TipoTec": "S", "ConTec": 1,
                        "NoEntrega": 1, "NoSubEntrega": 0, "TipoIDProv": "NI"
                    }
                    
                    filename = secure_filename(f"auth_{uuid.uuid4().hex[:8]}.pdf")
                    ruta_directorio = os.path.join(current_app.static_folder, 'uploads', 'flotaespecial', 'autorizaciones')
                    os.makedirs(ruta_directorio, exist_ok=True)
                    ruta_guardado = os.path.join(ruta_directorio, filename)
                    archivo.seek(0)
                    archivo.save(ruta_guardado)
                    datos_extraidos["ruta_documento"] = f"uploads/flotaespecial/autorizaciones/{filename}"

                    partes_paciente = re.split(r'DATOS DEL PACIENTE', texto_completo, maxsplit=1, flags=re.IGNORECASE)
                    texto_cabecera = partes_paciente[0] if partes_paciente else texto_completo
                    resto = partes_paciente[1] if len(partes_paciente) > 1 else ""

                    partes_servicios = re.split(r'SERVICIOS AUTORIZADOS', resto, maxsplit=1, flags=re.IGNORECASE)
                    texto_paciente = partes_servicios[0] if partes_servicios else resto
                    texto_servicios = partes_servicios[1] if len(partes_servicios) > 1 else ""

                    m_eps = re.search(r'([A-Za-z\s]+EPS)', texto_cabecera, flags=re.IGNORECASE)
                    if m_eps: datos_extraidos["eps_nombre"] = m_eps.group(1).strip()
                    
                    m_nit = re.search(r'NIT[\s\:\.\-]*([0-9\-]+)', texto_cabecera, flags=re.IGNORECASE)
                    if m_nit: datos_extraidos["eps_nit"] = re.sub(r'[^0-9]', '', m_nit.group(1).strip())

                    m_presc = re.search(r'Prescripci[óo]n[\s\:\#]*([A-Z0-9]{20})', texto_cabecera, flags=re.IGNORECASE)
                    if not m_presc:
                        m_presc = re.search(r'Prescripci[óo]n[\s\:\#]*([A-Z0-9\-]+)', texto_cabecera, flags=re.IGNORECASE)
                    if m_presc: datos_extraidos["numero_prescripcion"] = m_presc.group(1).strip()

                    m_tel = re.search(r'(Tel|Tel[ée]fono|Cel|Celular)[\s\:\.]*([0-9\s]+)', texto_paciente, flags=re.IGNORECASE)
                    if m_tel: datos_extraidos["telefono_usuario"] = m_tel.group(2).strip()

                    m_f_ent = re.search(r'FECHA(?:\s+DE\s+)?ENTREGA[\s\:\.\n]*([0-9]{2,4}[\-\/][0-9]{1,2}[\-\/][0-9]{1,4})', texto_cabecera, flags=re.IGNORECASE)
                    if m_f_ent: datos_extraidos["fecha_entrega_servicio"] = _convertir_fecha_html(m_f_ent.group(1).strip())

                    m_auth_tag = re.search(r'(?:NUMERO DE SOLICITUD ORIGEN|Autorizaci[oó]n|No)[\s\:\.\#\n\|]*([0-9]{5,})', texto_cabecera, flags=re.IGNORECASE)
                    if m_auth_tag:
                        cand = m_auth_tag.group(1).strip()
                        if cand != datos_extraidos.get("numero_prescripcion") and cand not in datos_extraidos.get("eps_nit", ""):
                            datos_extraidos["numero_autorizacion"] = cand

                    if not datos_extraidos["numero_autorizacion"]:
                        numeros_cabecera = re.findall(r'\b([0-9]{5,})\b', texto_cabecera)
                        for num in numeros_cabecera:
                            if num != datos_extraidos.get("numero_prescripcion") and num not in datos_extraidos.get("eps_nit", ""):
                                datos_extraidos["numero_autorizacion"] = num
                                break

                    stop_pattern = r'(?=\s{2,}|\||\n|TIPO|EDAD|SEXO|NUMERO|No CARN|NIVEL|DEPARTAMENTO|CORREO|TELEFONO|MUNICIPIO|FECHA|PRIMER|SEGUNDO|$)'

                    regex_tipo = r'\b(CC|C[ée]dula(?: de ciudadan[íi]a)?|TI|Tarjeta(?: de identidad)?|RC|Registro civil|N[°º]\s*registro civil|RG|N[°º]\s*RG|CE|C\s*ext|Pasaporte|PA|PAS|Permiso de Protecci[óo]n Temporal|PPT)\b'
                    m_tdoc = re.search(r'(?:TIPO\s+DOC|TIPO\s+IDENTIFICACI[OÓ]N|IDENTIFICACI[OÓ]N)[\s\:\.\n\|]*' + regex_tipo, texto_paciente, flags=re.IGNORECASE)
                    if not m_tdoc:
                        m_tdoc = re.search(regex_tipo, texto_paciente, flags=re.IGNORECASE)
                    if m_tdoc:
                        tdoc_raw = m_tdoc.group(1).strip().upper()
                        if "CC" in tdoc_raw or "CÉDULA" in tdoc_raw or "CEDULA" in tdoc_raw: datos_extraidos["tipo_documento"] = "CC"
                        elif "TI" in tdoc_raw or "TARJETA" in tdoc_raw: datos_extraidos["tipo_documento"] = "TI"
                        elif "RC" in tdoc_raw or "REGISTRO" in tdoc_raw: datos_extraidos["tipo_documento"] = "RC"
                        elif "CE" in tdoc_raw or "EXT" in tdoc_raw: datos_extraidos["tipo_documento"] = "CE"
                        elif "PA" in tdoc_raw or "PASAPORTE" in tdoc_raw: datos_extraidos["tipo_documento"] = "PA"
                        elif "PPT" in tdoc_raw or "PERMISO" in tdoc_raw: datos_extraidos["tipo_documento"] = "PPT"
                        else: datos_extraidos["tipo_documento"] = tdoc_raw

                    m_ndoc = re.search(r'NUMERO[\s\:\.\#\n\|]*([0-9]{5,})', texto_paciente, flags=re.IGNORECASE)
                    if m_ndoc:
                        cand_ndoc = re.sub(r'[^0-9]', '', m_ndoc.group(1).strip())
                        if cand_ndoc != datos_extraidos.get("eps_nit") and cand_ndoc != datos_extraidos.get("numero_prescripcion") and cand_ndoc != datos_extraidos.get("numero_autorizacion"):
                            datos_extraidos["id_usuario"] = cand_ndoc

                    m_fnac = re.search(r'FECHA\s+NACIMIENTO[\s\:\.\n\|]*([0-9]{2,4}[\-\/][0-9]{1,2}[\-\/][0-9]{1,4})', texto_paciente, flags=re.IGNORECASE)
                    if m_fnac: datos_extraidos["fecha_nacimiento"] = _convertir_fecha_html(m_fnac.group(1).strip())

                    m_edad = re.search(r'EDAD[\s\:\.\n\|]*([0-9]{1,2})\b', texto_paciente, flags=re.IGNORECASE)
                    if m_edad: datos_extraidos["edad"] = m_edad.group(1).strip()

                    m_sexo = re.search(r'SEXO[\s\n\|]*([MF]|Masculino|Femenino)\b', texto_paciente, flags=re.IGNORECASE)
                    if m_sexo:
                        val_sexo = m_sexo.group(1).strip().upper()
                        datos_extraidos["sexo"] = val_sexo[0]

                    m_carne = re.search(r'(?:No CARN[EÉ]|N[UÚ]MERO\s+CARN[EÉ]|CARN[EÉ])[\s\:\.\#\n\|]*([A-Za-z0-9\-]+)' + stop_pattern, texto_paciente, flags=re.IGNORECASE)
                    if m_carne:
                        cand_carne = m_carne.group(1).strip()
                        if cand_carne != datos_extraidos.get("id_usuario") and cand_carne != datos_extraidos.get("numero_prescripcion"):
                            datos_extraidos["numero_carne"] = cand_carne

                    m_tusu = re.search(r'TIPO\s+USUARIO[\s\:\.\n\|]*([A-Za-z\s]+?)' + stop_pattern, texto_paciente, flags=re.IGNORECASE)
                    if m_tusu: datos_extraidos["tipo_usuario"] = m_tusu.group(1).strip()

                    m_nsis = re.search(r'(?:NIVEL\s+SISB[EÉ]N|SISB[EÉ]N)[\s\:\.\n\|]*([A-Za-z0-9\s]+?)' + stop_pattern, texto_paciente, flags=re.IGNORECASE)
                    if m_nsis: datos_extraidos["nivel_sisben"] = m_nsis.group(1).strip()

                    m_email = re.search(r'(?:CORREO ELECTRONICO|EMAIL|E-MAIL|CORREO)[\s\:\.\n\|]*([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', texto_paciente, flags=re.IGNORECASE)
                    if m_email: datos_extraidos["email_usuario"] = m_email.group(1).strip()

                    deps_col = ["AMAZONAS", "ANTIOQUIA", "ARAUCA", "ATLANTICO", "BOGOTA", "BOLIVAR", "BOYACA", "CALDAS", "CAQUETA", "CASANARE", "CAUCA", "CESAR", "CHOCO", "CORDOBA", "CUNDINAMARCA", "GUAVIARE", "HUILA", "LA GUAJIRA", "MAGDALENA", "META", "NARIÑO", "NORTE DE SANTANDER", "PUTUMAYO", "QUINDIO", "RISARALDA", "SAN ANDRES", "SANTANDER", "SUCRE", "TOLIMA", "VALLE DEL CAUCA", "VAUPES", "VICHADA"]
                    m_dep = re.search(r'DEPARTAMENTO[\s\:\.\n\|]+([A-Za-z\s]+?)' + stop_pattern, texto_paciente, flags=re.IGNORECASE)
                    if m_dep:
                         cand_dep = m_dep.group(1).strip().upper()
                         for d in deps_col:
                              if d in cand_dep:
                                   datos_extraidos["departamento"] = d
                                   break

                    m_mun = re.search(r'MUNICIPIO[\s\:\.\n\|]+([A-Za-z\s]+?)' + stop_pattern, texto_paciente, flags=re.IGNORECASE)
                    if m_mun:
                         cand_mun = m_mun.group(1).strip()
                         cand_mun = re.sub(r'[0-9]+.*', '', cand_mun).strip()
                         if cand_mun:
                              datos_extraidos["municipio"] = cand_mun.upper()

                    datos_extraidos["cod_mun_ent"] = _obtener_codigo_dane(datos_extraidos.get("departamento"), datos_extraidos.get("municipio"))

                    m_dir = re.search(r'DIRECCI[OÓ]N[\s\:\.\n\|]*([^\n\|]+)', texto_paciente, flags=re.IGNORECASE)
                    if m_dir:
                         cand_dir = m_dir.group(1).strip().upper()
                         cand_dir = re.sub(r'(CORREO|TELEFONO|DEPARTAMENTO|MUNICIPIO).*', '', cand_dir).strip()
                         datos_extraidos["direccion_origen"] = re.sub(r'[\s\,]+$', '', cand_dir).strip()[:100] 

                    m_nombres = re.search(r'PRIMER APELLIDO(?:.*?)SEGUNDO NOMBRE[\s\n\|]+([A-Za-zñÑáéíóúÁÉÍÓÚ\s]+?)(?=\n|TIPO|EDAD|SEXO|NUMERO)', texto_paciente, flags=re.IGNORECASE)
                    if m_nombres:
                        datos_extraidos["nombre_usuario"] = re.sub(r'\s+', ' ', m_nombres.group(1)).strip()
                    else:
                        p_apellido = re.search(r'PRIMER APELLIDO[\s\n\|]+([A-Za-zñÑáéíóúÁÉÍÓÚ]+)', texto_paciente, flags=re.IGNORECASE)
                        s_apellido = re.search(r'SEGUNDO APELLIDO[\s\n\|]+([A-Za-zñÑáéíóúÁÉÍÓÚ]+)', texto_paciente, flags=re.IGNORECASE)
                        p_nombre = re.search(r'PRIMER NOMBRE[\s\n\|]+([A-Za-zñÑáéíóúÁÉÍÓÚ]+)', texto_paciente, flags=re.IGNORECASE)
                        s_nombre = re.search(r'SEGUNDO NOMBRE[\s\n\|]+([A-Za-zñÑáéíóúÁÉÍÓÚ]+)', texto_paciente, flags=re.IGNORECASE)

                        nombre_parts = []
                        if p_apellido: nombre_parts.append(p_apellido.group(1).strip())
                        if s_apellido: nombre_parts.append(s_apellido.group(1).strip())
                        if p_nombre: nombre_parts.append(p_nombre.group(1).strip())
                        if s_nombre: nombre_parts.append(s_nombre.group(1).strip())
                        
                        if len(nombre_parts) > 0:
                            datos_extraidos["nombre_usuario"] = " ".join(nombre_parts)

                    codigos_candidatos = re.findall(r'\b([A-Z]{1,4}[0-9]{4,15})\b', texto_servicios, flags=re.IGNORECASE)
                    for cand_cod in codigos_candidatos:
                        cand_cod = cand_cod.strip().upper()
                        if cand_cod != datos_extraidos.get("numero_prescripcion") and cand_cod != datos_extraidos.get("numero_autorizacion") and cand_cod != datos_extraidos.get("id_usuario"):
                            datos_extraidos["codigo_servicio"] = cand_cod
                            break

                    m_cant = re.search(r'(?:CANTIDAD|Total)[\s\:\#\n\|]*([0-9]+)', texto_servicios, flags=re.IGNORECASE)
                    if not m_cant:
                        m_cant = re.search(r'(?:CANTIDAD|Total)[\s\:\#]*([0-9]+)', texto_completo, flags=re.IGNORECASE)
                    if m_cant: datos_extraidos["cantidad"] = m_cant.group(1).strip()

                    flash("PDF procesado. Verifique la información extraída y complete los datos faltantes.", "success")
                except Exception as e:
                    flash(f"Error al leer el PDF: {str(e)}", "danger")
            else:
                flash("Por favor, suba un archivo en formato PDF.", "warning")
                
        elif accion == 'guardar_captura':
            eps_cliente = request.form.get('eps_cliente', '').strip()
            id_eps_cliente = request.form.get('IDProv', '').strip() 
            numero_prescripcion = request.form.get('NoPrescripcion', '').strip()
            fecha_entrega_servicio = request.form.get('fecha_entrega_servicio') or None
            numero_autorizacion = request.form.get('numero_autorizacion', '').strip()
            tipo_servicio = 'POR DEFINIR'
            codigo_servicio = request.form.get('CodSerTecAEntregar')
            
            nombre_usuario = request.form.get('nombre_usuario', '').strip()
            tipo_documento = request.form.get('tipo_documento')
            id_usuario = request.form.get('NoIDPaciente', '').strip()
            fecha_nacimiento = request.form.get('fecha_nacimiento') or None
            edad = request.form.get('edad')
            sexo = request.form.get('sexo')
            numero_carne = request.form.get('numero_carne')
            tipo_usuario = request.form.get('tipo_usuario')
            nivel_sisben = request.form.get('nivel_sisben')
            telefono_usuario = request.form.get('telefono_usuario')
            email_usuario = request.form.get('email_usuario')
            
            departamento = request.form.get('departamento')
            municipio = request.form.get('municipio')
            direccion_origen = request.form.get('DirPaciente', '').strip()
            
            departamento_destino = request.form.get('departamento_destino')
            municipio_destino = request.form.get('municipio_destino')
            direccion_destino = request.form.get('direccion_destino')
            
            cantidad = int(request.form.get('CantTotAEntregar', 0))
            observaciones = request.form.get('observaciones')
            ruta_documento = request.form.get('ruta_documento')
            
            try:
                cur.execute("""
                    INSERT INTO maestra_traslados_eps_tespecial 
                    (id_empresa, eps_cliente, id_eps_cliente, fecha_captura, fecha_entrega_servicio, 
                    numero_autorizacion, numero_prescripcion, tipo_servicio, codigo_servicio, estatus_servicio, 
                    nombre_usuario, tipo_documento, id_usuario, fecha_nacimiento, edad, sexo, numero_carne, 
                    tipo_usuario, nivel_sisben, telefono_usuario, email_usuario, 
                    departamento, municipio, direccion_origen, 
                    departamento_destino, municipio_destino, direccion_destino, 
                    numero_traslados_aporbados, numero_traslados_ejecutados, diferencia, observaciones, ruta_documento)
                    VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s, %s, 'CAPTURADO', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s)
                """, (empresa_id, eps_cliente, id_eps_cliente, fecha_entrega_servicio, numero_autorizacion, numero_prescripcion,
                      tipo_servicio, codigo_servicio, nombre_usuario, tipo_documento, id_usuario, fecha_nacimiento, edad, sexo,
                      numero_carne, tipo_usuario, nivel_sisben, telefono_usuario, email_usuario, 
                      departamento, municipio, direccion_origen, 
                      departamento_destino, municipio_destino, direccion_destino, 
                      cantidad, cantidad, observaciones, ruta_documento))
                mysql.connection.commit()
                flash('Orden Maestra guardada exitosamente.', 'success')
                return redirect(url_for('flotaespecial_eps.gestion_traslados_captura'))
            except Exception as e:
                flash(f'Error al guardar captura: {str(e)}', 'danger')

    cur.execute("SELECT * FROM maestra_traslados_eps_tespecial WHERE id_empresa = %s AND diferencia > 0 ORDER BY id DESC", (empresa_id,))
    traslados_capturados = cur.fetchall()
    cur.close()
    
    return render_template(
        'B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_captura', datos_extraidos=datos_extraidos, traslados_capturados=traslados_capturados
    )

# =========================================================
# ETAPA 2: DESGLOSE STRICTO (_IDA / _VUELTA)
# =========================================================
@bp_flotaespecial_eps.route('/asignacion', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_asignacion():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        maestra_id = request.form.get('maestra_id')
        
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        try:
            cur.execute("SELECT * FROM maestra_traslados_eps_tespecial WHERE id = %s AND id_empresa = %s", (maestra_id, empresa_id))
            maestra = cur.fetchone()
            
            if maestra and maestra['diferencia'] > 0:
                diferencia = maestra['diferencia']
                viajes_generados = 0
                
                id_viaje_padre = generar_id_viaje_unico(cur, empresa_id)

                for _ in range(diferencia):
                    base_viaje = generar_id_viaje_unico(cur, empresa_id) 
                    
                    id_ida = f"{base_viaje}_IDA"
                    id_vuelta = f"{base_viaje}_VUELTA"

                    cur.execute("""
                        INSERT INTO control_viajes_flota_especial 
                        (id_empresa, id_eps_cliente, numero_autorizacion, numero_prescripcion, tipo_servicio, trayecto,
                        nombre_usuario, tipo_documento, id_usuario, telefono_usuario, 
                        departamento, municipio, direccion_origen, 
                        departamento_destino, municipio_destino, direccion_destino, 
                        id_viaje, id_viaje_padre, estatus_servicio, ruta_documento)
                        VALUES (%s, %s, %s, %s, %s, 'IDA', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'CAPTURADO', %s)
                    """, (empresa_id, maestra['id_eps_cliente'], maestra['numero_autorizacion'], maestra['numero_prescripcion'], 
                          maestra['tipo_servicio'], maestra['nombre_usuario'], maestra.get('tipo_documento'), maestra['id_usuario'], maestra['telefono_usuario'], 
                          maestra['departamento'], maestra['municipio'], maestra['direccion_origen'], 
                          maestra.get('departamento_destino'), maestra.get('municipio_destino'), maestra['direccion_destino'], 
                          id_ida, id_viaje_padre, maestra.get('ruta_documento')))
                    
                    cur.execute("""
                        INSERT INTO control_viajes_flota_especial 
                        (id_empresa, id_eps_cliente, numero_autorizacion, numero_prescripcion, tipo_servicio, trayecto,
                        nombre_usuario, tipo_documento, id_usuario, telefono_usuario, 
                        departamento, municipio, direccion_origen, 
                        departamento_destino, municipio_destino, direccion_destino, 
                        id_viaje, id_viaje_padre, estatus_servicio, ruta_documento)
                        VALUES (%s, %s, %s, %s, %s, 'VUELTA', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'CAPTURADO', %s)
                    """, (empresa_id, maestra['id_eps_cliente'], maestra['numero_autorizacion'], maestra['numero_prescripcion'], 
                          maestra['tipo_servicio'], maestra['nombre_usuario'], maestra.get('tipo_documento'), maestra['id_usuario'], maestra['telefono_usuario'], 
                          maestra.get('departamento_destino'), maestra.get('municipio_destino'), maestra['direccion_destino'], 
                          maestra['departamento'], maestra['municipio'], maestra['direccion_origen'], 
                          id_vuelta, id_viaje_padre, maestra.get('ruta_documento')))
                    
                    viajes_generados += 2
                
                cur.execute("UPDATE maestra_traslados_eps_tespecial SET diferencia = 0 WHERE id = %s AND id_empresa = %s", (maestra_id, empresa_id))
                mysql.connection.commit()
                flash(f'Éxito: Se han desglosado {viajes_generados} viajes individuales (IDA/VUELTA) estrictamente divididos.', 'success')
            else:
                flash('La orden maestra ya no tiene traslados disponibles.', 'warning')
        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en desglose: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM maestra_traslados_eps_tespecial WHERE id_empresa = %s AND diferencia > 0 ORDER BY id DESC", (empresa_id,))
    ordenes_maestras = cur.fetchall()
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_asignacion', ordenes_maestras=ordenes_maestras)

# =========================================================
# ETAPA 3: VERIFICACIÓN CON ACOMPAÑANTE Y GEOCODING (TELEFÓNICA AGRUPADA IDA)
# =========================================================
@bp_flotaespecial_eps.route('/verificacion', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_verificacion():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        try:
            if accion in ['editar_y_verificar', 'editar_excepcion_individual']:
                viaje_id = request.form.get('viaje_id')
                id_viaje_padre = request.form.get('id_viaje_padre')
                nombre_usuario = request.form.get('nombre_usuario', '').strip()
                id_usuario = request.form.get('id_usuario', '').strip()
                telefono_usuario = request.form.get('telefono_usuario')
                
                departamento_origen = request.form.get('departamento_origen')
                municipio_origen = request.form.get('municipio_origen')
                direccion_origen = request.form.get('direccion_origen')
                departamento_destino = request.form.get('departamento_destino')
                municipio_destino = request.form.get('municipio_destino')
                direccion_destino = request.form.get('direccion_destino')
                
                lleva_acompanante = 1 if request.form.get('lleva_acompanante') else 0
                nombre_acompanante = request.form.get('nombre_acompanante', '').strip()
                cedula_acompanante = request.form.get('cedula_acompanante', '').strip()
                
                lat_origen_esperado = request.form.get('lat_origen_esperado') or None
                lng_origen_esperado = request.form.get('lng_origen_esperado') or None
                lat_destino_esperado = request.form.get('lat_destino_esperado') or None
                lng_destino_esperado = request.form.get('lng_destino_esperado') or None

                coordenadas_inicio = f"{lat_origen_esperado},{lng_origen_esperado}" if lat_origen_esperado and lng_origen_esperado else None
                coordenadas_fin = f"{lat_destino_esperado},{lng_destino_esperado}" if lat_destino_esperado and lng_destino_esperado else None

                if lleva_acompanante and (not nombre_acompanante or not cedula_acompanante):
                    flash('Al indicar acompañante debe ingresar nombre y cédula.', 'danger')
                    if accion == 'editar_excepcion_individual':
                        return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))
                    return redirect(url_for('flotaespecial_eps.gestion_traslados_verificacion'))
                
                if accion == 'editar_y_verificar':
                    cur.execute("SELECT numero_prescripcion FROM control_viajes_flota_especial WHERE id_viaje_padre = %s AND id_empresa = %s LIMIT 1", (id_viaje_padre, empresa_id))
                    viaje_ref = cur.fetchone()
                    if viaje_ref and viaje_ref['numero_prescripcion']:
                        num_prescripcion = viaje_ref['numero_prescripcion']
                        
                        cur.execute("""
                            UPDATE control_viajes_flota_especial 
                            SET nombre_usuario=%s, id_usuario=%s, telefono_usuario=%s, 
                                departamento=%s, municipio=%s, direccion_origen=%s,
                                departamento_destino=%s, municipio_destino=%s, direccion_destino=%s,
                                lleva_acompanante=%s, nombre_acompanante=%s, cedula_acompanante=%s,
                                lat_origen_esperado=%s, lng_origen_esperado=%s,
                                lat_destino_esperado=%s, lng_destino_esperado=%s,
                                coordenadas_inicio=%s, coordenadas_fin=%s,
                                estatus_servicio = 'VERIFICADO'
                            WHERE numero_prescripcion=%s AND id_empresa=%s AND trayecto = 'IDA' AND estatus_servicio = 'CAPTURADO'
                        """, (nombre_usuario, id_usuario, telefono_usuario, 
                              departamento_origen, municipio_origen, direccion_origen,
                              departamento_destino, municipio_destino, direccion_destino,
                              lleva_acompanante, nombre_acompanante, cedula_acompanante, 
                              lat_origen_esperado, lng_origen_esperado,
                              lat_destino_esperado, lng_destino_esperado,
                              coordenadas_inicio, coordenadas_fin,
                              num_prescripcion, empresa_id))
                        
                        cur.execute("""
                            UPDATE control_viajes_flota_especial 
                            SET nombre_usuario=%s, id_usuario=%s, telefono_usuario=%s, 
                                departamento=%s, municipio=%s, direccion_origen=%s,
                                departamento_destino=%s, municipio_destino=%s, direccion_destino=%s,
                                lleva_acompanante=%s, nombre_acompanante=%s, cedula_acompanante=%s,
                                lat_origen_esperado=%s, lng_origen_esperado=%s,
                                lat_destino_esperado=%s, lng_destino_esperado=%s,
                                coordenadas_inicio=%s, coordenadas_fin=%s,
                                estatus_servicio = 'VERIFICADO'
                            WHERE numero_prescripcion=%s AND id_empresa=%s AND trayecto = 'VUELTA' AND estatus_servicio = 'CAPTURADO'
                        """, (nombre_usuario, id_usuario, telefono_usuario, 
                              departamento_destino, municipio_destino, direccion_destino,
                              departamento_origen, municipio_origen, direccion_origen,
                              lleva_acompanante, nombre_acompanante, cedula_acompanante, 
                              lat_destino_esperado, lng_destino_esperado,
                              lat_origen_esperado, lng_origen_esperado,
                              coordenadas_fin, coordenadas_inicio,
                              num_prescripcion, empresa_id))
                        
                        mysql.connection.commit()
                        if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            flash('Datos auditados telefónicamente y todo el paquete de viajes ha sido verificado.', 'success')
                        else:
                            return jsonify({"status": "success"})
                    else:
                        mysql.connection.rollback()
                        if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                            flash('No se encontró la prescripción asociada para verificar.', 'danger')
                        else:
                            return jsonify({"status": "error", "message": "Prescripción no encontrada."}), 404
                        
                elif accion == 'editar_excepcion_individual':
                    cur.execute("""
                        UPDATE control_viajes_flota_especial 
                        SET nombre_usuario=%s, id_usuario=%s, telefono_usuario=%s, 
                            departamento=%s, municipio=%s, direccion_origen=%s,
                            departamento_destino=%s, municipio_destino=%s, direccion_destino=%s,
                            lleva_acompanante=%s, nombre_acompanante=%s, cedula_acompanante=%s,
                            lat_origen_esperado=%s, lng_origen_esperado=%s,
                            lat_destino_esperado=%s, lng_destino_esperado=%s,
                            coordenadas_inicio=%s, coordenadas_fin=%s
                        WHERE id=%s AND id_empresa=%s
                    """, (nombre_usuario, id_usuario, telefono_usuario, 
                          departamento_origen, municipio_origen, direccion_origen,
                          departamento_destino, municipio_destino, direccion_destino,
                          lleva_acompanante, nombre_acompanante, cedula_acompanante, 
                          lat_origen_esperado, lng_origen_esperado,
                          lat_destino_esperado, lng_destino_esperado,
                          coordenadas_inicio, coordenadas_fin,
                          viaje_id, empresa_id))
                    
                    mysql.connection.commit()
                    if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        flash('Ruta actualizada como excepción solo para este viaje.', 'success')
                        return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))
                    else:
                        return jsonify({"status": "success", "redirect": url_for('flotaespecial_eps.gestion_traslados_asignacion_flota')})
                
        except Exception as e:
            mysql.connection.rollback()
            if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                flash(f'Error en verificación/excepción: {str(e)}', 'danger')
            else:
                return jsonify({"status": "error", "message": str(e)}), 500
        finally:
            cur.close()
        
        if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return redirect(url_for('flotaespecial_eps.gestion_traslados_verificacion'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT MIN(c.id) as id, MAX(c.id_viaje_padre) as id_viaje_padre, c.numero_prescripcion, MAX(c.nombre_usuario) as nombre_usuario, 
               MAX(c.telefono_usuario) as telefono_usuario, MAX(c.tipo_servicio) as tipo_servicio, 
               MIN(c.id_viaje) as id_viaje, MAX(c.tipo_documento) as tipo_documento, MAX(c.id_usuario) as id_usuario, 
               MAX(c.departamento) as departamento, MAX(c.municipio) as municipio, MAX(c.direccion_origen) as direccion_origen,
               MAX(c.departamento_destino) as departamento_destino, MAX(c.municipio_destino) as municipio_destino, MAX(c.direccion_destino) as direccion_destino,
               MAX(m.ruta_documento) AS ruta_documento,
               COUNT(c.id) as total_idas
        FROM control_viajes_flota_especial c
        LEFT JOIN maestra_traslados_eps_tespecial m ON c.numero_prescripcion = m.numero_prescripcion AND c.id_empresa = m.id_empresa
        WHERE c.id_empresa = %s AND c.estatus_servicio = 'CAPTURADO' AND c.trayecto = 'IDA'
        GROUP BY c.numero_prescripcion
        ORDER BY MIN(c.id) ASC
    """, (empresa_id,))
    viajes_capturados = cur.fetchall()
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_verificacion', viajes=viajes_capturados)

# =========================================================
# ETAPA 4: PROGRAMACIÓN (TURNOS Y CASCADAS)
# =========================================================
@bp_flotaespecial_eps.route('/programacion', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_programacion():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        try:
            if accion == 'programar':
                viaje_id = request.form.get('viaje_id')
                fecha_servicio = request.form.get('fecha_servicio')
                hora_inicio = request.form.get('hora_inicio')
                
                tipo_servicio = request.form.get('tipo_servicio')
                turno = request.form.get('turno') 
                frecuencia = request.form.get('frecuencia')
                
                if tipo_servicio:
                    cur.execute("""
                        UPDATE control_viajes_flota_especial 
                        SET tipo_servicio = %s 
                        WHERE numero_prescripcion = (SELECT numero_prescripcion FROM (SELECT numero_prescripcion FROM control_viajes_flota_especial WHERE id=%s) AS tmp) 
                        AND id_empresa = %s
                    """, (tipo_servicio, viaje_id, empresa_id))
                    
                    cur.execute("""
                        UPDATE maestra_traslados_eps_tespecial 
                        SET tipo_servicio = %s 
                        WHERE numero_prescripcion = (SELECT numero_prescripcion FROM (SELECT numero_prescripcion FROM control_viajes_flota_especial WHERE id=%s) AS tmp2) 
                        AND id_empresa = %s
                    """, (tipo_servicio, viaje_id, empresa_id))

                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET fecha_servicio = %s, hora_inicio = %s, estatus_servicio = 'PROGRAMADO'
                    WHERE id = %s AND id_empresa = %s AND trayecto = 'IDA'
                """, (fecha_servicio, hora_inicio, viaje_id, empresa_id))
                
                if (turno in ['1', '2'] or frecuencia) and fecha_servicio:
                    cur.execute("SELECT numero_prescripcion, tipo_servicio FROM control_viajes_flota_especial WHERE id=%s AND id_empresa=%s", (viaje_id, empresa_id))
                    viaje_ref = cur.fetchone()
                    if viaje_ref:
                        cur.execute("""
                            SELECT id, id_viaje_padre FROM control_viajes_flota_especial 
                            WHERE numero_prescripcion=%s AND id_empresa=%s 
                            AND estatus_servicio = 'VERIFICADO' AND trayecto = 'IDA'
                            AND id != %s
                            ORDER BY id ASC
                        """, (viaje_ref['numero_prescripcion'], empresa_id, viaje_id))
                        pendientes = cur.fetchall()
                        
                        curr_date = datetime.strptime(fecha_servicio, '%Y-%m-%d').date()
                        for p in pendientes:
                            if viaje_ref['tipo_servicio'].upper() == 'DIALISIS' and turno in ['1', '2']:
                                curr_date = _get_next_date_turno(curr_date, turno)
                            elif frecuencia and frecuencia != 'unico':
                                curr_date = _get_next_date_frecuencia(curr_date, frecuencia)
                            
                            cur.execute("UPDATE control_viajes_flota_especial SET fecha_servicio=%s, hora_inicio=%s, estatus_servicio='PROGRAMADO' WHERE id=%s AND id_empresa=%s", (curr_date.strftime('%Y-%m-%d'), hora_inicio, p['id'], empresa_id))
                
                mysql.connection.commit()
                flash('Viaje(s) programado(s) correctamente.', 'success')

        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en programación: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_programacion'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT c.*, 
               (SELECT COUNT(*) FROM control_viajes_flota_especial 
                WHERE numero_prescripcion = c.numero_prescripcion 
                AND trayecto = 'IDA' AND id_empresa = %s) as total_idas
        FROM control_viajes_flota_especial c
        WHERE c.id_empresa = %s AND c.estatus_servicio = 'VERIFICADO' AND c.trayecto = 'IDA' 
        ORDER BY c.id ASC
    """, (empresa_id, empresa_id))
    viajes_verificados = cur.fetchall()
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_programacion', viajes=viajes_verificados)

# =========================================================
# GESTIÓN CRUD DE CONTRATOS DE TRANSPORTE
# =========================================================
@bp_flotaespecial_eps.route('/contratos', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_contratos():
    empresa_id = session.get('empresa_id')
    empresa_nit = session.get('nit')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        try:
            if accion == 'guardar_contrato':
                cur.execute("""
                    INSERT INTO contratos_transporte_especial 
                    (id_empresa, numero_contrato, contratante_nombre, contratante_nit_cedula, categoria_contrato,
                     objeto_contrato, convenio_colaboracion, fecha_inicio, fecha_fin,
                     responsable_nombre, responsable_cedula, responsable_direccion, responsable_telefono)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    empresa_id, request.form.get('numero_contrato'), request.form.get('contratante_nombre'),
                    request.form.get('contratante_nit_cedula'), request.form.get('categoria_contrato'),
                    request.form.get('objeto_contrato'), request.form.get('convenio_colaboracion'), 
                    request.form.get('fecha_inicio'), request.form.get('fecha_fin'), 
                    request.form.get('responsable_nombre'), request.form.get('responsable_cedula'), 
                    request.form.get('responsable_direccion'), request.form.get('responsable_telefono')
                ))
                mysql.connection.commit()
                flash('Contrato registrado con éxito.', 'success')
            elif accion == 'eliminar_contrato':
                cur.execute("UPDATE contratos_transporte_especial SET estado = 'INACTIVO' WHERE id = %s AND (id_empresa = %s OR id_empresa = %s)", (request.form.get('contrato_id'), empresa_id, empresa_nit))
                mysql.connection.commit()
                flash('Contrato inactivado con éxito.', 'success')
        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error procesando contrato: {str(e)}', 'danger')
        finally:
            return redirect(url_for('flotaespecial_eps.gestion_contratos'))

    cur.execute("SELECT * FROM contratos_transporte_especial WHERE (id_empresa = %s OR id_empresa = %s) AND estado = 'ACTIVO' ORDER BY id DESC", (empresa_id, empresa_nit))
    contratos = cur.fetchall()
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='gestion_contratos', contratos=contratos)

# =========================================================
# ETAPA 5: ASIGNACIÓN DE FLOTA Y GENERACIÓN DEL FUEC OFICIAL
# =========================================================
@bp_flotaespecial_eps.route('/asignacion_flota', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_asignacion_flota():
    empresa_id = session.get('empresa_id')
    empresa_nit = session.get('nit')
    empresa_nombre = session.get('empresa')
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        try:
            if accion in ['asignar_flota', 'resolver_novedad']:
                viaje_id = request.form.get('viaje_id')
                vehiculo_placa = request.form.get('vehiculo')
                conductor = request.form.get('conductor')
                contrato_id = request.form.get('contrato_id')
                tipo_novedad = request.form.get('tipo_novedad')
                monto_liquidacion_manual = request.form.get('monto_liquidacion_manual')
                
                if not contrato_id:
                    flash('Debe seleccionar un contrato vigente para generar el FUEC.', 'danger')
                    return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))
                
                cur.execute("""
                    SELECT v.*, e.nombre_comercial, e.nit, 
                           e.codigo_direccion_territorial, e.numero_resolucion_habilitacion, e.anio_habilitacion, e.firma_representante_legal
                    FROM vehiculos_especial v 
                    LEFT JOIN empresas e ON (v.id_empresa = e.id OR v.id_empresa = e.nit) 
                    WHERE v.placa = %s AND (v.id_empresa = %s OR v.id_empresa = %s)
                """, (vehiculo_placa, empresa_id, empresa_nit))
                veh_docs = cur.fetchone()

                cur.execute("SELECT id, nombre, cedula, telegram_id FROM usuarios WHERE (empresa_id = %s OR empresa_id = %s) AND perfil IN ('operador_flotaespecial', 'auxiliar_transporte_especial')", (empresa_id, empresa_nit))
                cond_usrs = list(cur.fetchall())
                
                cur.execute("SELECT id, nombre, cedula, vencimiento_licencia_conduccion FROM conductores_flotaespecial WHERE id_empresa = %s OR id_empresa = %s", (empresa_id, empresa_nit))
                cond_flota = list(cur.fetchall())
                
                cond_docs = None
                for c in cond_usrs:
                    if c['nombre'] == conductor:
                        cond_docs = c
                        break
                if not cond_docs:
                    for c in cond_flota:
                        if c['nombre'] == conductor:
                            cond_docs = c
                            break
                
                cur.execute("SELECT * FROM contratos_transporte_especial WHERE id = %s AND (id_empresa = %s OR id_empresa = %s)", (contrato_id, empresa_id, empresa_nit))
                contrato = cur.fetchone()

                hoy_date = datetime.now(BOGOTA_TZ).date()
                errores_docs = []

                if veh_docs:
                    if veh_docs.get('vencimiento_soat') and veh_docs['vencimiento_soat'] < hoy_date: 
                        errores_docs.append("SOAT Vencido")
                    
                    exige_rtm = True
                    if veh_docs.get('fecha_matricula'):
                        edad_dias = (hoy_date - veh_docs['fecha_matricula']).days
                        if edad_dias < (5 * 365.25):
                            exige_rtm = False
                            
                    if exige_rtm:
                        if not veh_docs.get('vencimiento_rtm'):
                            errores_docs.append("RTM No Registrada (Vehículo con 5 años o más)")
                        elif veh_docs['vencimiento_rtm'] < hoy_date:
                            errores_docs.append("RTM Vencida")

                    if veh_docs.get('vencimiento_tarjeta_operacion') and veh_docs['vencimiento_tarjeta_operacion'] < hoy_date: 
                        errores_docs.append("Tarjeta Op. Vencida")
                else: 
                    errores_docs.append("Datos Vehículo No Encontrados")

                if cond_docs:
                    if cond_docs.get('vencimiento_licencia_conduccion') and cond_docs['vencimiento_licencia_conduccion'] < hoy_date: 
                        errores_docs.append("Licencia Vencida")
                
                if errores_docs:
                    flash(f"⛔ Prevención Activa (Decreto 1079): Bloqueo por {', '.join(errores_docs)}.", "danger")
                    return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))
                
                cur.execute("SELECT * FROM control_viajes_flota_especial WHERE id = %s AND id_empresa = %s", (viaje_id, empresa_id))
                viaje_data = cur.fetchone()

                if accion == 'resolver_novedad':
                    try:
                        cur.execute("UPDATE fuec SET categoria_contrato = CONCAT(categoria_contrato, '-ANULADO') WHERE id_traslado_eps = %s AND id_empresa = %s", (viaje_data['id_viaje'], empresa_id))
                    except:
                        pass
                    es_trasbordo = 1 if tipo_novedad == 'NOVEDAD_RECORRIDO' else 0
                    cur.execute("""
                        UPDATE control_viajes_flota_especial 
                        SET estado_novedad = 'RESUELTA', 
                            monto_liquidacion_manual = %s, 
                            es_trasbordo = %s 
                        WHERE id = %s AND id_empresa = %s
                    """, (monto_liquidacion_manual if monto_liquidacion_manual else None, es_trasbordo, viaje_id, empresa_id))

                anio_actual = str(datetime.now(BOGOTA_TZ).year)
                cur.execute("SELECT COUNT(*) as c FROM fuec WHERE id_empresa = %s AND YEAR(fecha_inicio_vigencia) = %s", (empresa_id, anio_actual))
                cons_extracto = cur.fetchone()['c'] + 1
                
                consecutivo_oficial = generar_consecutivo_fuec(
                    veh_docs.get('codigo_direccion_territorial', '000') or '000',
                    veh_docs.get('numero_resolucion_habilitacion', '0000') or '0000',
                    veh_docs.get('anio_habilitacion', '00') or '00',
                    anio_actual,
                    contrato['numero_contrato'],
                    cons_extracto
                )
                
                pdf_filename = f"FUEC_{consecutivo_oficial}.pdf"
                ruta_relativa = f"uploads/flotaespecial/fuec/{pdf_filename}"
                ruta_pdf_abs = os.path.join(current_app.static_folder, ruta_relativa)
                os.makedirs(os.path.dirname(ruta_pdf_abs), exist_ok=True)
                
                doc = SimpleDocTemplate(ruta_pdf_abs, pagesize=letter, rightMargin=20, leftMargin=20, topMargin=20, bottomMargin=20)
                story = []
                styles = getSampleStyleSheet()
                
                style_header = ParagraphStyle('Header', parent=styles['Normal'], alignment=1, fontSize=11, fontName='Helvetica-Bold')
                style_cell = ParagraphStyle('Cell', parent=styles['Normal'], fontSize=8)
                style_cell_bold = ParagraphStyle('CellBold', parent=styles['Normal'], fontSize=8, fontName='Helvetica-Bold')
                
                logo_min = os.path.join(current_app.static_folder, 'logo_mintransporte.png')
                logo_empresa = os.path.join(current_app.static_folder, f"logo_{veh_docs.get('nit')}.PNG")
                
                img_min = RLImage(logo_min, width=2*inch, height=0.6*inch, kind='proportional') if os.path.exists(logo_min) else ""
                img_emp = RLImage(logo_empresa, width=2*inch, height=0.6*inch, kind='proportional') if os.path.exists(logo_empresa) else ""
                
                t_logos = Table([[img_min, img_emp]], colWidths=[3.5*inch, 3.5*inch])
                t_logos.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                story.append(t_logos)
                story.append(Spacer(1, 10))
                
                story.append(Paragraph(f"FORMATO ÚNICO DE EXTRACTO DEL CONTRATO DEL SERVICIO PÚBLICO DE TRANSPORTE TERRESTRE AUTOMOTOR ESPECIAL No. {consecutivo_oficial}", style_header))
                story.append(Spacer(1, 10))
                
                origen_destino_str = f"{viaje_data.get('direccion_origen', '')} - {viaje_data.get('direccion_destino', '')}"
                
                data_cto = [
                    [Paragraph("RAZÓN SOCIAL DE LA EMPRESA:", style_cell_bold), Paragraph(str(veh_docs.get('nombre_comercial') or 'N/A'), style_cell), Paragraph("NIT:", style_cell_bold), Paragraph(str(veh_docs.get('nit') or 'N/A'), style_cell)],
                    [Paragraph("CONTRATO No:", style_cell_bold), Paragraph(str(contrato.get('numero_contrato') or 'N/A'), style_cell), "", ""],
                    [Paragraph("CONTRATANTE:", style_cell_bold), Paragraph(str(contrato.get('contratante_nombre') or 'N/A'), style_cell), Paragraph("NIT/CC:", style_cell_bold), Paragraph(str(contrato.get('contratante_nit_cedula') or 'N/A'), style_cell)],
                    [Paragraph("OBJETO CONTRATO:", style_cell_bold), Paragraph(str(contrato.get('objeto_contrato') or 'N/A'), style_cell), "", ""],
                    [Paragraph("ORIGEN-DESTINO:", style_cell_bold), Paragraph(str(origen_destino_str), style_cell), "", ""],
                    [Paragraph("CONVENIO COLABORACIÓN:", style_cell_bold), Paragraph(str(contrato.get('convenio_colaboracion') or 'N/A'), style_cell), "", ""]
                ]
                t_cto = Table(data_cto, colWidths=[1.5*inch, 2.5*inch, 0.8*inch, 2.2*inch])
                t_cto.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.black), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.black), ('SPAN', (1,1), (3,1)), ('SPAN', (1,3), (3,3)), ('SPAN', (1,4), (3,4)), ('SPAN', (1,5), (3,5))]))
                story.append(t_cto)
                story.append(Spacer(1, 5))
                
                f_ini = contrato['fecha_inicio']
                f_fin = contrato['fecha_fin']
                data_vig = [
                    [Paragraph("VIGENCIA DEL CONTRATO", style_cell_bold), "DIA", "MES", "AÑO"],
                    [Paragraph("FECHA INICIAL", style_cell), f_ini.strftime('%d'), f_ini.strftime('%m'), f_ini.strftime('%Y')],
                    [Paragraph("FECHA VENCIMIENTO", style_cell), f_fin.strftime('%d'), f_fin.strftime('%m'), f_fin.strftime('%Y')]
                ]
                t_vig = Table(data_vig, colWidths=[4*inch, 1*inch, 1*inch, 1*inch])
                t_vig.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.black), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.black), ('ALIGN', (1,0), (-1,-1), 'CENTER')]))
                story.append(t_vig)
                story.append(Spacer(1, 5))
                
                data_veh = [
                    [Paragraph("CARACTERÍSTICAS DEL VEHÍCULO", style_header), "", "", ""],
                    ["PLACA", "MODELO", "MARCA", "CLASE"],
                    [str(veh_docs.get('placa') or 'N/A'), str(veh_docs.get('modelo') or 'N/A'), str(veh_docs.get('marca') or 'N/A'), str(veh_docs.get('clase') or 'N/A')],
                    ["NÚMERO INTERNO", "NÚMERO TARJETA DE OPERACIÓN", "", ""],
                    [str(veh_docs.get('numero_interno') or 'N/A'), str(veh_docs.get('numero_tarjeta_operacion') or 'N/A'), "", ""]
                ]
                t_veh = Table(data_veh, colWidths=[1.75*inch, 1.75*inch, 1.75*inch, 1.75*inch])
                t_veh.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.black), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.black), ('SPAN', (0,0), (3,0)), ('SPAN', (1,3), (3,3)), ('SPAN', (1,4), (3,4)), ('ALIGN', (0,0), (-1,-1), 'CENTER')]))
                story.append(t_veh)
                story.append(Spacer(1, 5))
                
                data_cond = [
                    ["DATOS DE CONDUCTORES", "NOMBRES Y APELLIDOS", "NÚMERO CÉDULA", "NÚMERO LICENCIA", "VIGENCIA"],
                    ["CONDUCTOR 1", str(cond_docs.get('nombre') or 'N/A'), str(cond_docs.get('cedula') or 'N/A'), str(cond_docs.get('cedula') or 'N/A'), str(cond_docs.get('vencimiento_licencia_conduccion') or 'N/A')],
                    ["CONDUCTOR 2", "N/A", "N/A", "N/A", "N/A"],
                    ["CONDUCTOR 3", "N/A", "N/A", "N/A", "N/A"]
                ]
                t_cond = Table(data_cond, colWidths=[1.2*inch, 2.3*inch, 1*inch, 1.2*inch, 1.3*inch])
                t_cond.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.black), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.black), ('ALIGN', (0,0), (-1,-1), 'CENTER'), ('FONTSIZE', (0,0), (-1,-1), 7)]))
                story.append(t_cond)
                story.append(Spacer(1, 5))
                
                data_resp = [
                    [Paragraph("RESPONSABLE DEL CONTRATANTE", style_cell_bold), Paragraph("NOMBRES Y APELLIDOS", style_cell_bold), Paragraph("NÚMERO CÉDULA", style_cell_bold), Paragraph("TELÉFONO", style_cell_bold), Paragraph("DIRECCIÓN", style_cell_bold)],
                    ["", Paragraph(str(contrato.get('responsable_nombre') or 'N/A'), style_cell), Paragraph(str(contrato.get('responsable_cedula') or 'N/A'), style_cell), Paragraph(str(contrato.get('responsable_telefono') or 'N/A'), style_cell), Paragraph(str(contrato.get('responsable_direccion') or 'N/A'), style_cell)]
                ]
                t_resp = Table(data_resp, colWidths=[1.5*inch, 1.5*inch, 1*inch, 1*inch, 2*inch])
                t_resp.setStyle(TableStyle([('BOX', (0,0), (-1,-1), 1, colors.black), ('INNERGRID', (0,0), (-1,-1), 0.5, colors.black), ('ALIGN', (0,0), (-1,-1), 'CENTER')]))
                story.append(t_resp)
                story.append(Spacer(1, 10))
                
                url_verificacion = request.host_url + 'gestor_flotaespecial/eps_bp/verificar_fuec/' + consecutivo_oficial
                qr_code = qr.QrCodeWidget(url_verificacion)
                d = Drawing(100, 100)
                d.add(qr_code)
                
                firma_empresa = RLImage(os.path.join(current_app.static_folder, veh_docs.get('firma_representante_legal', '')), width=1.5*inch, height=0.5*inch) if veh_docs.get('firma_representante_legal') else Spacer(1, 0.5*inch)
                
                data_footer = [
                    [Paragraph(f"<b>{str(veh_docs.get('nombre_comercial') or 'Empresa de Transporte')}</b><br/>NIT: {str(veh_docs.get('nit') or 'N/A')}", style_cell), 
                     d, 
                     [firma_empresa, Paragraph("FIRMA REPRESENTANTE LEGAL", style_cell_bold)]]
                ]
                t_footer = Table(data_footer, colWidths=[2.5*inch, 2*inch, 2.5*inch])
                t_footer.setStyle(TableStyle([('ALIGN', (0,0), (-1,-1), 'CENTER'), ('VALIGN', (0,0), (-1,-1), 'MIDDLE')]))
                story.append(t_footer)
                
                story.append(PageBreak())
                story.append(Paragraph("INSTRUCTIVO PARA LA DETERMINACIÓN DEL NÚMERO CONSECUTIVO DEL FUEC", style_header))
                story.append(Spacer(1, 10))
                story.append(Paragraph("El formato único de Extracto de Contrato FUEC estará constituido por los siguientes números según Resolución 0006652 de 2019:", style_cell))
                story.append(Spacer(1, 5))
                story.append(Paragraph("a) Los tres primeros dígitos corresponden al código de la Dirección Territorial que otorgó la habilitación. b) Los cuatro dígitos siguientes señalarán el número de resolución. c) Los dos siguientes dígitos, el año de habilitación. d) Los cuatro dígitos, el año de expedición. e) Cuatro dígitos del contrato. f) Cuatro dígitos del extracto.", style_cell))
                
                doc.build(story)
                
                cur.execute("""
                    INSERT INTO fuec (id_empresa, id_fuec_unico, consecutivo_oficial, consecutivo_extracto, placa, cedula_conductor, id_traslado_eps, id_contrato, categoria_contrato, numero_contrato, contratante_nombre, contratante_nit_cedula, origen, destino, fecha_inicio_vigencia, fecha_fin_vigencia, ruta_pdf_fuec, codigo_qr_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (empresa_id, consecutivo_oficial, consecutivo_oficial, cons_extracto, vehiculo_placa, cond_docs['cedula'], viaje_data['id_viaje'], contrato_id, contrato['categoria_contrato'], contrato['numero_contrato'], contrato['contratante_nombre'], contrato['contratante_nit_cedula'], viaje_data.get('direccion_origen'), viaje_data.get('direccion_destino'), f_ini, f_fin, ruta_relativa, url_verificacion))

                # Actualizar el viaje con el nuevo vehículo y conductor
                cur.execute("UPDATE control_viajes_flota_especial SET vehiculo_asignado = %s, conductor_asignado = %s, estatus_servicio = 'ASIGNADO' WHERE id = %s AND id_empresa = %s", (vehiculo_placa, conductor, viaje_id, empresa_id))
                
                # REVISIÓN CRÍTICA DE NOTIFICACIÓN TELEGRAM (Fallback y Escapado de HTML)
                cur.execute("SELECT telegram_id FROM usuarios WHERE nombre = %s AND (empresa_id = %s OR empresa_id = %s) LIMIT 1", (conductor, empresa_id, empresa_nit))
                usr_op = cur.fetchone()
                
                if usr_op and usr_op.get('telegram_id'):
                    import html
                    f_serv = html.escape(str(viaje_data.get('fecha_servicio') or 'Pendiente'))
                    h_ini = html.escape(str(viaje_data.get('hora_inicio') or 'Pendiente'))
                    telefono_pac = html.escape(str(viaje_data.get('telefono_usuario') or 'N/D'))
                    nombre_pac = html.escape(str(viaje_data.get('nombre_usuario') or 'N/D'))
                    dir_ori = html.escape(str(viaje_data.get('direccion_origen') or 'N/D'))
                    dir_des = html.escape(str(viaje_data.get('direccion_destino') or 'N/D'))
                    id_v = html.escape(str(viaje_data.get('id_viaje') or 'N/D'))
                    trayecto = html.escape(str(viaje_data.get('trayecto') or 'IDA'))
                    prescripcion = html.escape(str(viaje_data.get('numero_prescripcion') or 'N/A'))
                    
                    info_acompanante = ""
                    if viaje_data.get('lleva_acompanante'):
                        info_acompanante = f"👥 <b>Acompañante:</b> {html.escape(str(viaje_data.get('nombre_acompanante', 'Sí')))}\n"

                    mensaje_tg = (
                        f"🟢 <b>NUEVA ASIGNACIÓN DE VIAJE (FUEC)</b>\n\n"
                        f"🏢 <b>Empresa:</b> {html.escape(str(empresa_nombre))}\n"
                        f"🆔 <b>Prescripción:</b> {prescripcion}\n"
                        f"🚙 <b>Vehículo:</b> {html.escape(str(vehiculo_placa))}\n\n"
                        f"📋 <b>PROGRAMACIÓN:</b>\n"
                        f"ID Viaje: <code>{id_v}</code>\n"
                        f"Trayecto: {trayecto}\n"
                        f"Paciente: {nombre_pac}\n"
                        f"📞 <b>Teléfono:</b> {telefono_pac}\n"
                        f"{info_acompanante}"
                        f"Fecha: {f_serv} | Hora: {h_ini}\n"
                        f"Origen: {dir_ori}\n"
                        f"Destino: {dir_des}"
                    )
                    
                    if os.path.exists(ruta_pdf_abs):
                        _enviar_documento_telegram_hilo([usr_op['telegram_id']], mensaje_tg, ruta_pdf_abs)
                    else:
                        _enviar_mensajes_telegram_hilo([usr_op['telegram_id']], mensaje_tg.replace("<b>", "*").replace("</b>", "*").replace("<code>", "`").replace("</code>", "`"))
                
                mysql.connection.commit()
                if accion == 'resolver_novedad':
                    if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        flash('Novedad resuelta: Flota reasignada y nuevo FUEC oficial generado exitosamente.', 'success')
                    else:
                        return jsonify({"status": "success"})
                else:
                    if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                        flash('Flota asignada y FUEC oficial generado exitosamente.', 'success')
                    else:
                        return jsonify({"status": "success"})

        except Exception as e:
            mysql.connection.rollback()
            if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
                flash(f"Error en asignación/FUEC: {str(e)}", 'danger')
            else:
                return jsonify({"status": "error", "message": str(e)}), 500
        finally:
            cur.close()
            
        if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT * FROM control_viajes_flota_especial 
        WHERE id_empresa = %s 
          AND (estatus_servicio IN ('PROGRAMADO', 'PDTE. ASIGNAR VUELTA') OR estado_novedad = 'ACTIVA')
        ORDER BY fecha_servicio ASC
    """, (empresa_id,))
    viajes_programados = cur.fetchall()
    
    cur.execute("""
        SELECT v.placa, v.clase AS tipo, COALESCE(v.capacidad_pasajeros, 4) as capacidad_pasajeros,
               (SELECT COUNT(*) FROM control_viajes_flota_especial c 
                WHERE c.vehiculo_asignado = v.placa AND c.estatus_servicio IN ('ASIGNADO', 'EN EJECUCION') 
                AND c.fecha_servicio = CURDATE() AND c.id_empresa = %s) as ocupacion_actual
        FROM vehiculos_especial v 
        WHERE v.id_empresa = %s OR v.id_empresa = %s
    """, (empresa_id, empresa_id, empresa_nit))
    vehiculos = cur.fetchall()
    
    cur.execute("SELECT id, nombre, cedula, telegram_id FROM usuarios WHERE (empresa_id = %s OR empresa_id = %s) AND perfil IN ('operador_flotaespecial', 'auxiliar_transporte_especial')", (empresa_id, empresa_nit))
    cond_usrs = list(cur.fetchall())
    
    cur.execute("SELECT id, nombre, cedula, vencimiento_licencia_conduccion FROM conductores_flotaespecial WHERE id_empresa = %s OR id_empresa = %s", (empresa_id, empresa_nit))
    cond_flota = list(cur.fetchall())
    
    cond_dict = {c['cedula']: c for c in cond_usrs}
    for c in cond_flota:
        if c['cedula'] not in cond_dict:
            cond_dict[c['cedula']] = c
    conductores = list(cond_dict.values())

    cur.execute("SELECT id, contratante_nombre, numero_contrato, categoria_contrato FROM contratos_transporte_especial WHERE (id_empresa = %s OR id_empresa = %s) AND estado = 'ACTIVO'", (empresa_id, empresa_nit))
    contratos = cur.fetchall()

    cur.close()
    
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_asignacion_flota', viajes=viajes_programados, vehiculos=vehiculos, conductores=conductores, contratos=contratos)
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_asignacion_flota', viajes=viajes_programados, vehiculos=vehiculos, conductores=conductores, contratos=contratos)

# =========================================================
# ETAPAS 6 Y 7: AUDITORÍA Y RESULTADOS
# =========================================================
@bp_flotaespecial_eps.route('/auditoria', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_auditoria():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        try:
            if accion == 'aprobar_paquete':
                raiz_viaje = request.form.get('id_viaje_padre')
                
                cur.execute("""
                    SELECT id, estatus_servicio, numero_autorizacion, numero_prescripcion, 
                           id_viaje, ruta_documento, hash_seguridad
                    FROM control_viajes_flota_especial
                    WHERE SUBSTRING_INDEX(id_viaje, '_', 1) = %s AND id_empresa = %s AND estatus_servicio = 'TERMINADO-PDTE AUDITAR'
                """, (raiz_viaje, empresa_id))
                tramos = cur.fetchall()
                
                auditados_count = 0
                for viaje in tramos:
                    hash_seg = viaje.get('hash_seguridad')

                    if not hash_seg:
                        try:
                            cur.execute("SELECT hash_seguridad FROM viajes_flotaespecial WHERE id_traslado_eps = %s AND id_empresa = %s", (viaje['id_viaje'], empresa_id))
                            old_rec = cur.fetchone()
                            if old_rec and old_rec.get('hash_seguridad'):
                                hash_seg = old_rec['hash_seguridad']
                        except Exception:
                            pass

                    if hash_seg:
                        auditor = session.get('nombre')
                        fecha_auditoria = datetime.now(BOGOTA_TZ)
                        
                        cadena_auditoria = f"{hash_seg}|{viaje['id_viaje']}|{auditor}|{fecha_auditoria.strftime('%Y-%m-%d %H:%M:%S')}"
                        hash_auditoria = hashlib.sha256(cadena_auditoria.encode('utf-8')).hexdigest()
                        
                        cur.execute("""
                            UPDATE control_viajes_flota_especial 
                            SET estatus_servicio = 'AUDITADO',
                                auditor_nombre = %s,
                                fecha_auditoria = %s,
                                hash_auditoria = %s,
                                ruta_pdf_unificado = %s
                            WHERE id = %s AND id_empresa = %s
                        """, (auditor, fecha_auditoria, hash_auditoria, viaje['ruta_documento'], viaje['id'], empresa_id))
                        
                        cur.execute("""
                            UPDATE maestra_traslados_eps_tespecial 
                            SET numero_traslados_ejecutados = numero_traslados_ejecutados + 1
                            WHERE id_empresa = %s AND (numero_autorizacion = %s OR numero_prescripcion = %s)
                        """, (empresa_id, viaje['numero_autorizacion'], viaje['numero_prescripcion']))
                        auditados_count += 1
                        
                if auditados_count > 0:
                    flash(f'Sello SHA-256 verificado y estatus actualizado. Paquete aprobado ({auditados_count} tramos validados).', 'success')
                else:
                    flash('Error: No se pudieron validar los hashes de seguridad en este paquete.', 'danger')
                    
            elif accion == 'rechazar_tramo':
                viaje_id = request.form.get('viaje_id')
                cur.execute("UPDATE control_viajes_flota_especial SET estatus_servicio = 'ASIGNADO' WHERE id = %s AND id_empresa = %s", (viaje_id, empresa_id))
                flash('Tramo rechazado y devuelto al conductor (Estado ASIGNADO). El paquete completo queda pausado.', 'danger')

            mysql.connection.commit()
        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en auditoría: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_auditoria'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("""
        SELECT SUBSTRING_INDEX(id_viaje, '_', 1) as raiz_viaje
        FROM control_viajes_flota_especial
        WHERE id_empresa = %s AND estatus_servicio IN ('TERMINADO-PDTE AUDITAR', 'AUDITADO')
        GROUP BY raiz_viaje
        HAVING COUNT(id) = 2
           AND SUM(CASE WHEN estatus_servicio = 'TERMINADO-PDTE AUDITAR' THEN 1 ELSE 0 END) > 0
    """, (empresa_id,))
    raices_validas = [r['raiz_viaje'] for r in cur.fetchall()]
    
    viajes_agrupados = {}
    
    if raices_validas:
        format_strings = ','.join(['%s'] * len(raices_validas))
        cur.execute(f"""
            SELECT id, id_viaje_padre, id_viaje, numero_prescripcion, nombre_usuario, trayecto, 
                   conductor_asignado, vehiculo_asignado, fecha_fin_real, ruta_documento, estatus_servicio,
                   direccion_origen, municipio, departamento, lat_origen, lng_origen, lat_origen_esperado, lng_origen_esperado,
                   direccion_destino, municipio_destino, departamento_destino, lat_destino, lng_destino, lat_destino_esperado, lng_destino_esperado, firma,
                   hora_origen, hora_destino, tiempo_efectivo_minutos,
                   SUBSTRING_INDEX(id_viaje, '_', 1) as raiz_viaje
            FROM control_viajes_flota_especial 
            WHERE id_empresa = %s AND SUBSTRING_INDEX(id_viaje, '_', 1) IN ({format_strings})
            ORDER BY raiz_viaje, trayecto
        """, [empresa_id] + raices_validas)
        tramos = cur.fetchall()
        
        for t in tramos:
            p = t['raiz_viaje']
            if p not in viajes_agrupados:
                viajes_agrupados[p] = []
            viajes_agrupados[p].append(t)
            
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_auditoria', viajes_agrupados=viajes_agrupados)

@bp_flotaespecial_eps.route('/auditados', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_auditados():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT id_viaje, numero_autorizacion, nombre_usuario, id_eps_cliente AS eps_cliente, operador_ejecucion, 
               vehiculo_asignado, fecha_fin_real, auditor_nombre, fecha_auditoria, hash_auditoria,
               COALESCE(ruta_pdf_unificado, ruta_documento) as ruta_documento
        FROM control_viajes_flota_especial 
        WHERE id_empresa = %s AND estatus_servicio = 'AUDITADO' 
        ORDER BY id DESC LIMIT 100
    """, (empresa_id,))
    viajes = cur.fetchall()
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_auditados', viajes=viajes)

# =========================================================
# VISTA PÚBLICA DE VERIFICACIÓN (QR)
# =========================================================
@bp_flotaespecial_eps.route('/verificar_fuec/<consecutivo>', methods=['GET'])
def verificar_fuec_publico(consecutivo):
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("SELECT * FROM fuec WHERE consecutivo_oficial = %s LIMIT 1", (consecutivo,))
        fuec_data = cur.fetchone()
        
        if not fuec_data:
            return render_template('B_verificar_fuec.html', estado='INEXISTENTE')
            
        empresa_context_id = fuec_data['id_empresa']

        cur.execute("SELECT * FROM empresas WHERE id = %s AND estatus = 'ACTIVO'", (empresa_context_id,))
        empresa_data = cur.fetchone()
        
        cur.execute("SELECT placa, marca, modelo, clase, numero_tarjeta_operacion AS tarjeta_operacion FROM vehiculos_especial WHERE placa = %s AND id_empresa = %s", (fuec_data['placa'], empresa_context_id))
        vehiculo_data = cur.fetchone() or {'placa': fuec_data['placa'], 'marca': 'N/D', 'modelo': 'N/D', 'clase': 'N/D', 'tarjeta_operacion': 'N/D'}
        
        cur.execute("SELECT nombre, cedula, cedula AS licencia, 'N/D' AS vigencia_licencia FROM usuarios WHERE cedula = %s AND empresa_id = %s", (fuec_data['cedula_conductor'], empresa_context_id))
        conductor_data = cur.fetchall()
        
        pasajeros = []
        if fuec_data.get('categoria_contrato') != 'SALUD_EPS':
            cur.execute("SELECT nombre_usuario AS nombre, id_usuario AS identificacion FROM control_viajes_flota_especial WHERE id_viaje = %s AND id_empresa = %s", (fuec_data['id_traslado_eps'], empresa_context_id))
            pasajeros = cur.fetchall()
            
        estado_fuec = 'VALIDO' if fuec_data['fecha_fin_vigencia'].date() >= datetime.now(BOGOTA_TZ).date() else 'VENCIDO'
        
        return render_template('B_verificar_fuec.html', estado=estado_fuec, fuec=fuec_data, empresa=empresa_data, vehiculo=vehiculo_data, conductores=conductor_data, pasajeros=pasajeros)
    except Exception as e:
        return f"Error interno: {str(e)}"
    finally:
        cur.close()

# =========================================================
# CRON AUTÓNOMO DE RECORDATORIOS
# =========================================================
@bp_flotaespecial_eps.route('/cron/recordatorios', methods=['GET', 'POST'])
def cron_recordatorios_especial():
    token = request.args.get('token')
    if token != 'BQA_CRON_2026': 
        return jsonify({"status": "error", "message": "No autorizado"}), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT v.*, e.nombre_comercial AS empresa_nombre
            FROM control_viajes_flota_especial v
            JOIN empresas e ON v.id_empresa = e.id
            WHERE v.estatus_servicio = 'ASIGNADO'
              AND v.fecha_servicio = CURDATE()
              AND v.hora_inicio BETWEEN CURTIME() AND ADDTIME(CURTIME(), '02:00:00')
              AND v.recordatorio_enviado = FALSE
        """)
        viajes_pendientes = cur.fetchall()
        
        for viaje in viajes_pendientes:
            notificar_programacion_viaje(viaje['id_empresa'], viaje['empresa_nombre'], viaje, 'RECORDATORIO')
            cur.execute("UPDATE control_viajes_flota_especial SET recordatorio_enviado = TRUE WHERE id = %s AND id_empresa = %s", (viaje['id'], viaje['id_empresa']))
            
        mysql.connection.commit()
        return jsonify({"status": "success", "notificados": len(viajes_pendientes)}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()