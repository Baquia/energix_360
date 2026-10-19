# MÓDULO: UNIVERSAL | ARCHIVO: utils.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import session, flash, redirect, url_for
from functools import wraps
from app import mysql
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
import os
import json
import smtplib
from email.mime.text import MIMEText
from email.utils import make_msgid, formatdate

def login_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('index'))
            
        # --- LÓGICA DE CADUCIDAD DIARIA (4:00 A.M. HORA COLOMBIA) ---
        ahora_utc = datetime.now(timezone.utc)
        ahora_colombia = ahora_utc - timedelta(hours=5)
        corte_hoy = ahora_colombia.replace(hour=4, minute=0, second=0, microsecond=0)
        
        if ahora_colombia >= corte_hoy:
            limite_corte_timestamp = corte_hoy.timestamp()
        else:
            limite_corte_timestamp = (corte_hoy - timedelta(days=1)).timestamp()
            
        login_time = session.get('login_time')
        
        if not login_time or login_time < limite_corte_timestamp:
            session.clear()
            flash('Tu sesión ha expirado por cambio de turno (4:00 a.m.). Por favor, ingresa nuevamente.', 'warning')
            return redirect(url_for('index'))
        # -----------------------------------------------------------
        
        # --- LÓGICA DE CONTROL DE SESIONES ÚNICAS ---
        perfil = str(session.get('nombre_perfil', session.get('perfil', ''))).strip().lower()
        perfiles_multi_sesion = [
            'operador_logistica', 
            'webmaster_admin', 
            'controlador_gas', 
            'gestor_flotacarga',
            'verificador_bodegas',
            'operador_ventas',
            'operador_recursohumano',
            'operador_contable',
            'operador_facturacion'
        ]
        
        if perfil not in perfiles_multi_sesion:
            token_sesion_actual = session.get('token_sesion')
            usuario_id = session.get('usuario_id')
            empresa_id = session.get('empresa_id')
            
            if token_sesion_actual and usuario_id and empresa_id:
                try:
                    cur = mysql.connection.cursor()
                    cur.execute("SELECT token_sesion FROM usuarios WHERE id = %s AND empresa_id = %s", (usuario_id, empresa_id))
                    row = cur.fetchone()
                    cur.close()
                    
                    if row and row[0] != token_sesion_actual:
                        session.clear()
                        flash('Tu sesión fue cerrada porque ingresaste desde otro dispositivo.', 'danger')
                        return redirect(url_for('index'))
                except Exception as e:
                    print(f"Error validando token de sesión única: {e}")
        # ---------------------------------------------
        
        return f(*args, **kwargs)
    return decorated_function


def submodulo_required(submodulo_nombre):
    """
    Decorador de autorización Plug & Play para verificar si la empresa
    posee activo el submódulo en su sesión (session['submodulos_activos']).
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            submodulos_activos = session.get('submodulos_activos', [])
            
            if isinstance(submodulos_activos, str):
                try:
                    submodulos_activos = json.loads(submodulos_activos)
                except Exception:
                    submodulos_activos = []
                    
            if submodulo_nombre not in submodulos_activos:
                flash(f'Acceso denegado: Su empresa no tiene habilitado el submódulo de {submodulo_nombre}.', 'danger')
                return redirect(url_for('index'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def registrar_auditoria(empresa_id, empresa_nombre, modulo, usuario, accion, detalle, nivel='INFO'):
    """
    Mecanismo modular y centralizado.
    Registra un evento transaccional en la tabla audit_log para trazabilidad estricta.
    """
    try:
        # Validar que haya conexión
        if not mysql or not mysql.connection:
            print("⚠️ Auditoría: No hay conexión a BD disponible.")
            return

        cur = mysql.connection.cursor()
        
        # Inserción segura con prevención de inyección SQL
        cur.execute("""
            INSERT INTO audit_log (empresa_id, empresa_nombre, modulo, usuario, accion, detalle, nivel)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (empresa_id, empresa_nombre, modulo, usuario, accion, detalle, nivel))
        
        mysql.connection.commit()
        cur.close()
        
    except Exception as e:
        # Control de daños: Si falla la auditoría, no se tumba el hilo transaccional principal
        print(f"⛔ Error crítico guardando auditoría: {e}")


def validar_aptitud_operativa(conductor, vehiculo):
    """
    Motor de cálculo para validar la aptitud operativa documental y PESV.
    Retorna un diccionario con 'apto' (boolean), 'bloqueos' (list de dicts) y 'alertas' (list).
    """
    resultado = {
        "apto": True,
        "bloqueos": [],
        "alertas": []
    }

    hoy = datetime.now().date()

    def parse_date(date_val):
        if not date_val:
            return None
        if isinstance(date_val, datetime):
            return date_val.date()
        if isinstance(date_val, str):
            try:
                return datetime.strptime(date_val, "%Y-%m-%d").date()
            except Exception:
                return None
        return date_val

    # Paso 1: Edades y Fechas
    fecha_nac = parse_date(conductor.get('fecha_nacimiento'))
    fecha_exp_lic = parse_date(conductor.get('fecha_expedicion_licencia'))

    if fecha_nac and fecha_exp_lic:
        edad_al_expedir = relativedelta(fecha_exp_lic, fecha_nac).years
    else:
        edad_al_expedir = 30  # Default safe assumption si no hay fechas
        resultado['alertas'].append("Datos de nacimiento o expedición de licencia incompletos.")

    # Paso 2: Validación Tarjeta Propiedad
    id_tarjeta = vehiculo.get('id_tarjeta_propiedad')
    if not id_tarjeta or str(id_tarjeta).strip() == "":
        resultado['bloqueos'].append({
            "clave": "tarjeta_propiedad",
            "tipo": "faltante",
            "mensaje": "Tarjeta de Propiedad no registrada o inválida."
        })

    # Paso 3: SOAT y Otros Seguros
    documentos = [
        ("SOAT", "vencimiento_soat", parse_date(vehiculo.get('vencimiento_soat'))),
        ("Tecnomecánica", "vencimiento_tecnomecanica", parse_date(vehiculo.get('vencimiento_tecnomecanica'))),
        ("Póliza RCE", "vencimiento_poliza_rce", parse_date(vehiculo.get('vencimiento_poliza_rce')))
    ]

    for nombre_doc, clave_tec, fecha_vence in documentos:
        if not fecha_vence:
            resultado['bloqueos'].append({
                "clave": clave_tec,
                "tipo": "faltante",
                "mensaje": f"{nombre_doc} no registrado en sistema."
            })
            continue
        dias_restantes = (fecha_vence - hoy).days
        if dias_restantes <= 0:
            resultado['bloqueos'].append({
                "clave": clave_tec,
                "tipo": "vencido",
                "mensaje": f"{nombre_doc} Vencido."
            })
        elif 0 < dias_restantes <= 8:
            resultado['alertas'].append(f"{nombre_doc} próximo a vencer en {dias_restantes} días.")

    # Paso 4: Licencia de Conducción
    categoria_licencia = str(conductor.get('categoria_licencia', '')).strip().capitalize()
    if fecha_exp_lic:
        if categoria_licencia == 'Particular':
            if edad_al_expedir < 60:
                fecha_teorica = fecha_exp_lic + relativedelta(years=10)
            elif 60 <= edad_al_expedir <= 79:
                fecha_teorica = fecha_exp_lic + relativedelta(years=5)
            else:
                fecha_teorica = fecha_exp_lic + relativedelta(years=1)
        elif categoria_licencia == 'Publico' or categoria_licencia == 'Público':
            if edad_al_expedir < 60:
                fecha_teorica = fecha_exp_lic + relativedelta(years=3)
            else:
                fecha_teorica = fecha_exp_lic + relativedelta(years=1)
        else:
            fecha_teorica = fecha_exp_lic + relativedelta(years=3)  # Fallback

        dias_restantes_lic = (fecha_teorica - hoy).days
        if dias_restantes_lic <= 0:
            resultado['bloqueos'].append({
                "clave": "licencia_conduccion",
                "tipo": "vencido",
                "mensaje": "Licencia de Conducción Vencida."
            })
        elif 0 < dias_restantes_lic <= 30:
            resultado['alertas'].append(f"Licencia de Conducción próxima a vencer en {dias_restantes_lic} días.")
    else:
        resultado['bloqueos'].append({
            "clave": "licencia_conduccion",
            "tipo": "faltante",
            "mensaje": "Fecha de expedición de licencia no registrada."
        })

    # Paso 5: Seguridad Social (ÚNICA Y EXCLUSIVAMENTE para Conductores Terceros / Aliados)
    # Para conductores Propios, la empresa cliente realiza el pago unificado parafiscal por nómina corporativa (PILA).
    tipo_conductor = str(conductor.get('tipo_conductor', '')).strip().capitalize()
    if tipo_conductor == 'Tercero':
        venc_ss = parse_date(conductor.get('vencimiento_seguridad_social'))
        ultimo_pago_ss = parse_date(conductor.get('ultimo_pago_seguridad_social'))

        if venc_ss:
            dias_restantes_ss = (venc_ss - hoy).days
            if dias_restantes_ss <= 0:
                resultado['bloqueos'].append({
                    "clave": "seguridad_social",
                    "tipo": "vencido",
                    "mensaje": "Cobertura de Seguridad Social (EPS/ARL) suspendida."
                })
            elif 0 < dias_restantes_ss <= 5:
                resultado['alertas'].append(f"Seguridad Social requiere refrendación en {dias_restantes_ss} días.")
        elif ultimo_pago_ss:
            fecha_fin_cobertura = ultimo_pago_ss + timedelta(days=30)
            dias_restantes_ss = (fecha_fin_cobertura - hoy).days
            if dias_restantes_ss <= 0:
                resultado['bloqueos'].append({
                    "clave": "seguridad_social",
                    "tipo": "vencido",
                    "mensaje": "Cobertura de Seguridad Social (EPS/ARL) suspendida."
                })
            elif 0 < dias_restantes_ss <= 5:
                resultado['alertas'].append(f"Seguridad Social requiere refrendación en {dias_restantes_ss} días.")
        else:
            resultado['bloqueos'].append({
                "clave": "seguridad_social",
                "tipo": "faltante",
                "mensaje": "Último pago de Seguridad Social no registrado."
            })

    # Paso 6: Consolidación
    if len(resultado['bloqueos']) > 0:
        resultado['apto'] = False

    return resultado


def enviar_alerta_novedad_tercero(empresa_id, empresa_nombre, placa, conductor_nombre, novedades_lista):
    """
    Envía notificación corporativa a los controladores ante bloqueos de terceros.
    """
    try:
        if not mysql or not mysql.connection:
            return False

        cur = mysql.connection.cursor()
        cur.execute("""
            SELECT email FROM usuarios
            WHERE empresa_id = %s
            AND perfil IN ('controlador_flotacarga', 'controlador_bodegas', 'controlador_pesv')
            AND email IS NOT NULL AND email != ''
        """, (empresa_id,))

        destinatarios_db = cur.fetchall()
        cur.close()

        destinatarios = []
        for d in destinatarios_db:
            email = d.get('email') if isinstance(d, dict) else d[0]
            if email and email not in destinatarios:
                destinatarios.append(email)

        if not destinatarios:
            return False

        email_user = os.environ.get("EMAIL_USER")
        email_pass = os.environ.get("EMAIL_PASS")
        email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
        try:
            email_port = int(os.environ.get("EMAIL_PORT", "587"))
        except:
            email_port = 587
        email_from = os.environ.get("EMAIL_FROM", email_user)

        if not email_user or not email_pass:
            return False

        items_html = ""
        for nov in novedades_lista:
            mensaje_novedad = nov.get('mensaje', str(nov)) if isinstance(nov, dict) else str(nov)
            items_html += f"<li>{mensaje_novedad}</li>"

        cuerpo = f"""
        <!DOCTYPE html>
        <html>
        <head>
        <style>
            body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
            .container {{ max-width: 600px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
            .header {{ background-color: #015249; color: #ffffff; padding: 20px; text-align: center; }}
            .header h2 {{ margin: 0; font-size: 24px; }}
            .content {{ padding: 25px; }}
            .info-box {{ background-color: #f9fbfb; border-left: 4px solid #015249; padding: 15px; margin-bottom: 20px; }}
            .info-box p {{ margin: 5px 0; }}
            .novedades-box {{ background-color: #ffebee; border: 2px dashed #d9534f; padding: 15px; margin: 20px 0; border-radius: 6px; }}
            .novedades-title {{ font-size: 14px; color: #c62828; font-weight: bold; margin-bottom: 5px; }}
            .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
        </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>Novedad de Ingreso: Tercero Bloqueado</h2>
                </div>
                <div class="content">
                    <p>Estimado equipo de control de <strong>{empresa_nombre}</strong>,</p>
                    <p>El Motor de Visión Operativa ha bloqueado el ingreso de un vehículo aliado debido a novedades críticas en sus documentos o aptitud operativa:</p>

                    <div class="info-box">
                        <p><strong>Vehículo (Placa):</strong> {placa}</p>
                        <p><strong>Conductor Asignado:</strong> {conductor_nombre}</p>
                        <p><strong>Fecha de Detección:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
                    </div>

                    <div class="novedades-box">
                        <div class="novedades-title">CAUSALES DE DETENCIÓN INMEDIATA</div>
                        <ul>
                            {items_html}
                        </ul>
                    </div>

                    <p style="margin-top: 20px; font-size: 14px; color: #555;">
                        <em>El conductor no podrá continuar con el preoperacional hasta que los soportes físicos o digitales sean refrendados en la plataforma.</em>
                    </p>
                </div>
                <div class="footer">
                    <p>Este es un mensaje automático generado por el sistema <strong>BQA-ONE / Energix360</strong>.</p>
                    <p>No responda a este correo.</p>
                </div>
            </div>
        </body>
        </html>
        """

        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"🚨 Alerta Operativa (Tercero) - Placa: {placa} - {empresa_nombre}"
        msg["From"] = email_from
        msg["To"] = ", ".join(destinatarios)
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_from

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())

        return True

    except Exception as e:
        print(f"Error enviando alerta de tercero: {e}")
        return False


def notificar_pesv_soportes_pendientes(empresa_id, empresa_nombre, placa):
    """
    Despacha una notificación dedicada a la cola de trabajo del Controlador PESV
    cuando un conductor aliado sube refrendaciones documentales.
    """
    try:
        if not mysql or not mysql.connection:
            return False

        cur = mysql.connection.cursor()
        cur.execute("""
            SELECT email FROM usuarios
            WHERE empresa_id = %s
            AND perfil = 'controlador_pesv'
            AND email IS NOT NULL AND email != ''
        """, (empresa_id,))

        destinatarios_db = cur.fetchall()
        cur.close()

        destinatarios = []
        for d in destinatarios_db:
            email = d.get('email') if isinstance(d, dict) else d[0]
            if email and email not in destinatarios:
                destinatarios.append(email)

        if not destinatarios:
            return False

        email_user = os.environ.get("EMAIL_USER")
        email_pass = os.environ.get("EMAIL_PASS")
        email_host = os.environ.get("EMAIL_HOST", "smtp.gmail.com")
        try:
            email_port = int(os.environ.get("EMAIL_PORT", "587"))
        except:
            email_port = 587
        email_from = os.environ.get("EMAIL_FROM", email_user)

        if not email_user or not email_pass:
            return False

        cuerpo = f"""
        <!DOCTYPE html>
        <html>
        <head>
        <style>
            body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
            .container {{ max-width: 600px; margin: 20px auto; border: 1px solid #ddd; border-radius: 8px; overflow: hidden; }}
            .header {{ background-color: #015249; color: #ffffff; padding: 20px; text-align: center; }}
            .header h2 {{ margin: 0; font-size: 24px; }}
            .content {{ padding: 25px; }}
            .info-box {{ background-color: #f9fbfb; border-left: 4px solid #015249; padding: 15px; margin-bottom: 20px; }}
            .info-box p {{ margin: 5px 0; }}
            .footer {{ background-color: #f4f4f4; color: #777; padding: 15px; text-align: center; font-size: 12px; }}
            .btn-action {{ background-color: #015249; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; font-weight: bold; display: inline-block; margin-top: 15px; }}
        </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2>Nuevos Soportes PESV en Cola</h2>
                </div>
                <div class="content">
                    <p>Estimado Controlador PESV de <strong>{empresa_nombre}</strong>,</p>
                    <p>Un conductor aliado ha cargado soportes documentales para subsanar novedades de aptitud operativa y solicita validación para continuar su ruta:</p>

                    <div class="info-box">
                        <p><strong>Vehículo (Placa):</strong> {placa}</p>
                        <p><strong>Fecha de Carga:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
                    </div>

                    <p style="margin-top: 20px; font-size: 14px; color: #555;">
                        Ingrese a la plataforma para revisar los documentos, actualizar las fechas de vencimiento y habilitar al conductor.
                    </p>
                </div>
                <div class="footer">
                    <p>Este es un mensaje automático generado por el sistema <strong>BQA-ONE / Energix360</strong>.</p>
                </div>
            </div>
        </body>
        </html>
        """

        msg = MIMEText(cuerpo, "html", "utf-8")
        msg["Subject"] = f"🔔 Acción Requerida: Validar Soportes PESV - Placa: {placa}"
        msg["From"] = email_from
        msg["To"] = ", ".join(destinatarios)
        msg["Message-ID"] = make_msgid()
        msg["Date"] = formatdate(localtime=True)
        msg["Reply-To"] = email_from

        with smtplib.SMTP(email_host, email_port) as server:
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, destinatarios, msg.as_string())

        return True

    except Exception as e:
        print(f"Error enviando alerta PESV: {e}")
        return False