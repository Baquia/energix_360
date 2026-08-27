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
from datetime import datetime, timedelta
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import letter
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, current_app, jsonify
from werkzeug.utils import secure_filename
from app import mysql
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

bp_flotaespecial_eps = Blueprint('flotaespecial_eps', __name__, url_prefix='/gestor_flotaespecial/eps_bp')

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
# HELPER: CÁLCULO DE TURNOS (DIÁLISIS)
# =========================================================
def _get_next_date(current_date, turno):
    """Calcula la siguiente fecha válida según el turno de diálisis"""
    next_d = current_date + timedelta(days=1)
    valid_days = [0, 2, 4] if turno == '1' else [1, 3, 5]
    while next_d.weekday() not in valid_days:
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

def notificar_programacion_viaje(empresa_id, empresa_nombre, viaje_data, tipo_evento='ASIGNACION'):
    f_serv = viaje_data.get('fecha_servicio') or 'Pendiente'
    h_ini = viaje_data.get('hora_inicio') or 'Pendiente'
    
    if tipo_evento == 'ASIGNACION':
        titulo_tg = "🟢 *NUEVA ASIGNACIÓN DE VIAJE*"
        cuerpo_info_tg = (
            f"📦 *Cantidad de Viajes Asignados:* 1\n\n"
            f"📋 *PROGRAMACIÓN ESTIMADA:*\n"
            f"🆔 *ID Viaje:* `{viaje_data['id_viaje']}`\n"
            f"👤 *Paciente:* {viaje_data['nombre_usuario']}\n"
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

def generar_id_viaje_unico(cur):
    for _ in range(10):
        codigo = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        cur.execute("SELECT id FROM control_viajes_flota_especial WHERE id_viaje = %s", (codigo,))
        if not cur.fetchone():
            return codigo
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

# =========================================================
# HELPER: ASEGURAR TABLAS (ORDEN MAESTRA + VIAJES INDIVIDUALES)
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
            id_viaje VARCHAR(6) UNIQUE,
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

    # Migración de columnas dinámicas
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN tipo_documento VARCHAR(20) AFTER nombre_usuario")
    except: pass
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN departamento_destino VARCHAR(100) AFTER direccion_origen")
    except: pass
    try: cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN municipio_destino VARCHAR(100) AFTER departamento_destino")
    except: pass
    
    try: cur.execute("ALTER TABLE maestra_traslados_eps_tespecial ADD COLUMN departamento_destino VARCHAR(100) AFTER direccion_origen")
    except: pass
    try: cur.execute("ALTER TABLE maestra_traslados_eps_tespecial ADD COLUMN municipio_destino VARCHAR(100) AFTER departamento_destino")
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

                    stop_pattern = r'(?=\s{2,}|\||\n|TIPO|EDAD|SEXO|NUMERO|No CARN|NIVEL|DIRECCION|DEPARTAMENTO|CORREO|TELEFONO|MUNICIPIO|FECHA|PRIMER|SEGUNDO|$)'

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

                    m_dir = re.search(r'DIRECCI[OÓ]N[\s\:\.\n\|]+([A-Za-z0-9\#\-\s\.\/]+?)' + stop_pattern, texto_paciente, flags=re.IGNORECASE)
                    if m_dir:
                         cand_dir = m_dir.group(1).strip().upper()
                         valid_kws = ["CALLE", "CLL", "CARRERA", "CRA", "N°", "NÚMERO", "NUMERO", "AVENIDA", "AV", "AVDA", "SUR", "NORTE", "ESTE", "OESTE", "KILOMETRO", "KM", "VIA"]
                         if any(kw in cand_dir for kw in valid_kws):
                              if datos_extraidos.get("municipio") and datos_extraidos["municipio"] in cand_dir:
                                  cand_dir = cand_dir.replace(datos_extraidos["municipio"], "").strip()
                              if datos_extraidos.get("departamento") and datos_extraidos["departamento"] in cand_dir:
                                  cand_dir = cand_dir.replace(datos_extraidos["departamento"], "").strip()
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
            tipo_servicio = request.form.get('tipo_servicio', 'AMBULATORIA')
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
            
            errores_validacion = []
            if not eps_cliente.upper().endswith("EPS") and not eps_cliente.upper().endswith("S.A.S"):
                errores_validacion.append("Asegúrese de que el nombre de la EPS sea válido (Regla 1).")
            if numero_autorizacion == id_eps_cliente or numero_autorizacion == id_usuario:
                errores_validacion.append("El Número de Orden/Autorización no puede ser igual al NIT ni al documento del paciente (Reglas 2, 3 y 6).")
            if not nombre_usuario:
                errores_validacion.append("No se pudo construir o extraer el nombre del paciente a partir de la sección de Datos (Reglas 4 y 5).")
            if not id_usuario:
                errores_validacion.append("Falta el documento de identidad del paciente (Regla 6).")
            if not direccion_origen:
                errores_validacion.append("Faltan los datos de Origen del viaje (Regla 7).")

            if errores_validacion:
                for err in errores_validacion:
                    flash(err, 'danger')
                return redirect(url_for('flotaespecial_eps.gestion_traslados_captura'))
            
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
                flash('Orden Maestra guardada exitosamente cumpliendo todas las reglas de negocio.', 'success')
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
# ETAPA 2: ASIGNACIÓN Y DESGLOSE (AUTOMÁTICO)
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
                
                for _ in range(diferencia):
                    id_viaje_nuevo = generar_id_viaje_unico(cur)
                    
                    cur.execute("""
                        INSERT INTO control_viajes_flota_especial 
                        (id_empresa, id_eps_cliente, numero_autorizacion, numero_prescripcion, tipo_servicio, 
                        nombre_usuario, tipo_documento, id_usuario, telefono_usuario, 
                        departamento, municipio, direccion_origen, 
                        departamento_destino, municipio_destino, direccion_destino, 
                        vehiculo_asignado, conductor_asignado, 
                        fecha_servicio, hora_inicio, hora_fin, id_viaje, estatus_servicio, ruta_documento)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL, NULL, 
                        NULL, '07:00:00', NULL, %s, 'CAPTURADO', %s)
                    """, (empresa_id, maestra['id_eps_cliente'], maestra['numero_autorizacion'], maestra['numero_prescripcion'], 
                          maestra['tipo_servicio'], maestra['nombre_usuario'], maestra.get('tipo_documento'), maestra['id_usuario'], maestra['telefono_usuario'], 
                          maestra['departamento'], maestra['municipio'], maestra['direccion_origen'], 
                          maestra.get('departamento_destino'), maestra.get('municipio_destino'), maestra['direccion_destino'], 
                          id_viaje_nuevo, maestra.get('ruta_documento')))
                    viajes_generados += 1
                
                # Descontar el saldo al desglosar
                cur.execute("UPDATE maestra_traslados_eps_tespecial SET diferencia = 0 WHERE id = %s", (maestra_id,))
                mysql.connection.commit()
                flash(f'Éxito: Se han desglosado {viajes_generados} viajes individuales listos para verificación y programación.', 'success')
            else:
                flash('La orden maestra ya no tiene traslados disponibles.', 'warning')
        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en asignación: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM maestra_traslados_eps_tespecial WHERE id_empresa = %s AND diferencia > 0 ORDER BY id DESC", (empresa_id,))
    ordenes_maestras = cur.fetchall()
    cur.close()
    
    return render_template(
        'B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_asignacion', ordenes_maestras=ordenes_maestras
    )

# =========================================================
# ETAPA 3: PROGRAMACIÓN DE FECHAS Y TURNOS
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
                turno = request.form.get('turno') # '1' (L-M-V), '2' (M-J-S) o 'manual'
                
                # Actualiza el viaje actual
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET fecha_servicio = %s, hora_inicio = %s, estatus_servicio = 'PROGRAMADO'
                    WHERE id = %s AND id_empresa = %s
                """, (fecha_servicio, hora_inicio, viaje_id, empresa_id))
                
                # Auto-programación para turnos de Diálisis
                if turno in ['1', '2'] and fecha_servicio:
                    cur.execute("SELECT numero_autorizacion FROM control_viajes_flota_especial WHERE id=%s", (viaje_id,))
                    auth = cur.fetchone()
                    if auth:
                        cur.execute("""
                            SELECT id FROM control_viajes_flota_especial 
                            WHERE numero_autorizacion=%s AND id_empresa=%s 
                            AND estatus_servicio = 'CAPTURADO' AND id != %s 
                            ORDER BY id ASC
                        """, (auth['numero_autorizacion'], empresa_id, viaje_id))
                        pendientes = cur.fetchall()
                        
                        curr_date = datetime.strptime(fecha_servicio, '%Y-%m-%d').date()
                        for p in pendientes:
                            curr_date = _get_next_date(curr_date, turno)
                            cur.execute("""
                                UPDATE control_viajes_flota_especial 
                                SET fecha_servicio=%s, hora_inicio=%s, estatus_servicio='PROGRAMADO' 
                                WHERE id=%s
                            """, (curr_date.strftime('%Y-%m-%d'), hora_inicio, p['id']))
                
                mysql.connection.commit()
                flash('Servicio(s) programado(s) exitosamente.', 'success')

        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en el proceso de programación: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_programacion'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT c.*, COALESCE(c.ruta_documento, m.ruta_documento) AS ruta_documento
        FROM control_viajes_flota_especial c
        LEFT JOIN (
            SELECT numero_autorizacion, numero_prescripcion, id_empresa, MAX(ruta_documento) as ruta_documento 
            FROM maestra_traslados_eps_tespecial 
            GROUP BY numero_autorizacion, numero_prescripcion, id_empresa
        ) m 
          ON c.numero_autorizacion = m.numero_autorizacion 
          AND (c.numero_prescripcion = m.numero_prescripcion OR c.numero_prescripcion IS NULL OR m.numero_prescripcion IS NULL) 
          AND c.id_empresa = m.id_empresa
        WHERE c.id_empresa = %s AND c.estatus_servicio = 'CAPTURADO'
        ORDER BY c.id ASC
    """, (empresa_id,))
    viajes_capturados = cur.fetchall()
    cur.close()
    
    return render_template(
        'B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_programacion', viajes=viajes_capturados
    )

# =========================================================
# ETAPA 4: VERIFICACIÓN Y AUDITORÍA DE DATOS
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
            if accion == 'verificar':
                viaje_id = request.form.get('viaje_id')
                cur.execute("UPDATE control_viajes_flota_especial SET estatus_servicio = 'VERIFICADO', fecha_notificacion_info = NOW() WHERE id = %s AND id_empresa = %s", (viaje_id, empresa_id))
                mysql.connection.commit()
                flash('Viaje verificado. Listo para asignar flota.', 'success')

            elif accion == 'editar_y_verificar':
                viaje_id = request.form.get('viaje_id')
                nombre_usuario = request.form.get('nombre_usuario', '').strip()
                tipo_documento = request.form.get('tipo_documento')
                id_usuario = request.form.get('id_usuario', '').strip()
                telefono_usuario = request.form.get('telefono_usuario')
                
                departamento = request.form.get('departamento')
                municipio = request.form.get('municipio')
                direccion_origen = request.form.get('direccion_origen')
                
                departamento_destino = request.form.get('departamento_destino')
                municipio_destino = request.form.get('municipio_destino')
                direccion_destino = request.form.get('direccion_destino')
                
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET nombre_usuario=%s, tipo_documento=%s, id_usuario=%s, telefono_usuario=%s, 
                        departamento=%s, municipio=%s, direccion_origen=%s, 
                        departamento_destino=%s, municipio_destino=%s, direccion_destino=%s,
                        estatus_servicio = 'VERIFICADO', fecha_notificacion_info = NOW()
                    WHERE id=%s AND id_empresa=%s
                """, (nombre_usuario, tipo_documento, id_usuario, telefono_usuario, 
                      departamento, municipio, direccion_origen, 
                      departamento_destino, municipio_destino, direccion_destino, 
                      viaje_id, empresa_id))
                mysql.connection.commit()
                flash('Datos auditados, corregidos y viaje verificado exitosamente.', 'success')
                
        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en el proceso de verificación: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_verificacion'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT c.*, COALESCE(c.ruta_documento, m.ruta_documento) AS ruta_documento
        FROM control_viajes_flota_especial c
        LEFT JOIN (
            SELECT numero_autorizacion, numero_prescripcion, id_empresa, MAX(ruta_documento) as ruta_documento 
            FROM maestra_traslados_eps_tespecial 
            GROUP BY numero_autorizacion, numero_prescripcion, id_empresa
        ) m 
          ON c.numero_autorizacion = m.numero_autorizacion 
          AND (c.numero_prescripcion = m.numero_prescripcion OR c.numero_prescripcion IS NULL OR m.numero_prescripcion IS NULL) 
          AND c.id_empresa = m.id_empresa
        WHERE c.id_empresa = %s AND c.estatus_servicio = 'PROGRAMADO'
        ORDER BY c.fecha_servicio ASC, c.hora_inicio ASC
    """, (empresa_id,))
    viajes_programados = cur.fetchall()
    cur.close()
    
    return render_template(
        'B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_verificacion', viajes=viajes_programados
    )

# =========================================================
# ETAPA 5: ASIGNACIÓN DE FLOTA Y NOTIFICACIÓN (TELEGRAM)
# =========================================================
@bp_flotaespecial_eps.route('/asignacion_flota', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_asignacion_flota():
    empresa_id = session.get('empresa_id')
    empresa_nombre = session.get('empresa')
    
    if request.method == 'POST':
        accion = request.form.get('accion')
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        
        try:
            if accion == 'asignar_flota':
                viaje_id = request.form.get('viaje_id')
                vehiculo = request.form.get('vehiculo')
                conductor = request.form.get('conductor')
                
                cur.execute("""
                    UPDATE control_viajes_flota_especial 
                    SET vehiculo_asignado = %s, conductor_asignado = %s, estatus_servicio = 'ASIGNADO' 
                    WHERE id = %s AND id_empresa = %s
                """, (vehiculo, conductor, viaje_id, empresa_id))
                
                cur.execute("SELECT * FROM control_viajes_flota_especial WHERE id = %s", (viaje_id,))
                viaje_data = cur.fetchone()
                mysql.connection.commit()
                
                if viaje_data:
                    # Invocar notificación oficial a Telegram
                    notificar_programacion_viaje(empresa_id, empresa_nombre, viaje_data, 'ASIGNACION')
                
                flash('Flota asignada y notificación enviada al Telegram del conductor.', 'success')
                
            elif accion == 'novedad':
                viaje_id = request.form.get('viaje_id')
                observacion = request.form.get('observacion')
                cur.execute("UPDATE control_viajes_flota_especial SET fecha_notificacion_info=NOW() WHERE id=%s AND id_empresa=%s", (viaje_id, empresa_id))
                mysql.connection.commit()
                flash('Novedad reportada sobre el viaje.', 'success')

        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en asignación de flota: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_asignacion_flota'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT c.*, COALESCE(c.ruta_documento, m.ruta_documento) AS ruta_documento
        FROM control_viajes_flota_especial c
        LEFT JOIN (
            SELECT numero_autorizacion, numero_prescripcion, id_empresa, MAX(ruta_documento) as ruta_documento 
            FROM maestra_traslados_eps_tespecial 
            GROUP BY numero_autorizacion, numero_prescripcion, id_empresa
        ) m 
          ON c.numero_autorizacion = m.numero_autorizacion 
          AND (c.numero_prescripcion = m.numero_prescripcion OR c.numero_prescripcion IS NULL OR m.numero_prescripcion IS NULL) 
          AND c.id_empresa = m.id_empresa
        WHERE c.id_empresa = %s AND c.estatus_servicio = 'VERIFICADO'
        ORDER BY c.fecha_servicio ASC, c.hora_inicio ASC
    """, (empresa_id,))
    viajes_verificados = cur.fetchall()
    
    cur.execute("SELECT placa, clase AS tipo FROM vehiculos_especial WHERE id_empresa = %s", (empresa_id,))
    vehiculos = cur.fetchall()
    
    cur.execute("SELECT id, nombre, cedula FROM usuarios WHERE empresa_id = %s AND perfil = 'operador_flotaespecial'", (empresa_id,))
    conductores = cur.fetchall()
    cur.close()
    
    return render_template(
        'B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'),
        active_module='traslados_asignacion_flota', viajes=viajes_verificados, vehiculos=vehiculos, conductores=conductores
    )

# =========================================================
# AUDITORÍA DE VIAJES INDIVIDUALES (AUTOMÁTICO Y SEGURO)
# =========================================================
@bp_flotaespecial_eps.route('/auditoria', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_auditoria():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        viaje_id = request.form.get('viaje_id')
        accion = request.form.get('accion')
        
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        try:
            if accion == 'aprobar_auditoria':
                cur.execute("""
                    SELECT c.id, c.estatus_servicio, c.numero_autorizacion, c.numero_prescripcion, 
                           c.id_viaje, c.ruta_documento,
                           v.hash_seguridad 
                    FROM control_viajes_flota_especial c
                    LEFT JOIN viajes_flotaespecial v ON c.id_viaje = v.id_traslado_eps COLLATE utf8mb4_unicode_ci
                    WHERE c.id = %s AND c.id_empresa = %s
                """, (viaje_id, empresa_id))
                viaje = cur.fetchone()
                
                if viaje and viaje['estatus_servicio'] == 'TERMINADO-PDTE AUDITAR':
                    if viaje.get('hash_seguridad'):
                        auditor = session.get('nombre')
                        fecha_auditoria = datetime.now()
                        
                        cadena_auditoria = f"{viaje['hash_seguridad']}|{viaje['id_viaje']}|{auditor}|{fecha_auditoria.strftime('%Y-%m-%d %H:%M:%S')}"
                        hash_auditoria = hashlib.sha256(cadena_auditoria.encode('utf-8')).hexdigest()
                        
                        cur.execute("""
                            UPDATE control_viajes_flota_especial 
                            SET estatus_servicio = 'AUDITADO',
                                auditor_nombre = %s,
                                fecha_auditoria = %s,
                                hash_auditoria = %s,
                                ruta_pdf_unificado = %s
                            WHERE id = %s
                        """, (auditor, fecha_auditoria, hash_auditoria, viaje['ruta_documento'], viaje_id))
                        
                        cur.execute("""
                            UPDATE maestra_traslados_eps_tespecial 
                            SET numero_traslados_ejecutados = numero_traslados_ejecutados + 1
                            WHERE id_empresa = %s AND (numero_autorizacion = %s OR numero_prescripcion = %s)
                        """, (empresa_id, viaje['numero_autorizacion'], viaje['numero_prescripcion']))
                        
                        flash('Sello SHA-256 verificado automáticamente. Auditoría certificada y viaje aprobado.', 'success')
                    else:
                        flash('Error Crítico: El viaje no posee un Sello SHA-256 registrado. No se puede certificar la auditoría.', 'danger')
                else:
                    flash('El viaje ya fue auditado o su estado es inválido para esta operación.', 'warning')
                    
            elif accion == 'rechazar_auditoria':
                obs = request.form.get('observacion', '')
                cur.execute("UPDATE control_viajes_flota_especial SET estatus_servicio = 'ASIGNADO' WHERE id = %s AND id_empresa = %s", (viaje_id, empresa_id))
                flash('La auditoría ha sido rechazada y el viaje se ha devuelto a estado ASIGNADO.', 'danger')

            mysql.connection.commit()
        except Exception as e:
            mysql.connection.rollback()
            flash(f'Error en auditoría: {str(e)}', 'danger')
        finally:
            cur.close()
        return redirect(url_for('flotaespecial_eps.gestion_traslados_auditoria'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT * FROM control_viajes_flota_especial WHERE id_empresa = %s AND estatus_servicio = 'TERMINADO-PDTE AUDITAR' ORDER BY id ASC", (empresa_id,))
    viajes = cur.fetchall()
    cur.close()
    
    return render_template('B_modulo_flotaespecial_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_auditoria', viajes=viajes)

# =========================================================
# HISTORIAL AUDITADOS
# =========================================================
@bp_flotaespecial_eps.route('/auditados', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def gestion_traslados_auditados():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT id_viaje, numero_autorizacion, nombre_usuario, eps_cliente, operador_ejecucion, 
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
            cur.execute("UPDATE control_viajes_flota_especial SET recordatorio_enviado = TRUE WHERE id = %s", (viaje['id'],))
            
        mysql.connection.commit()
        return jsonify({"status": "success", "notificados": len(viajes_pendientes)}), 200
    except Exception as e:
        mysql.connection.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()