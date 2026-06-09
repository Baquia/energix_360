import os
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash
from app import mysql
from app.utils import login_required_custom
from datetime import datetime
import MySQLdb.cursors
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

bp_flotacarga = Blueprint('flotacarga', __name__)

# =========================================================
# CONFIGURACIÓN DE CORREO (Heredada del entorno del servidor)
# =========================================================
EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.hostinger.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "465"))
EMAIL_USER = os.environ.get("EMAIL_USER", "bqa-one@baquia-esm.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

@bp_flotacarga.route('/preoperacional')
@login_required_custom
def preoperacional_tc():
    """Renderiza el lector QR o el formulario si ya está prelogueado."""
    if "placa_prelogueada" in session:
        placa = session["placa_prelogueada"]
        empresa_id = session.get("empresa_id")
        
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        vehiculo = cur.fetchone()
        cur.close()
        
        if not vehiculo:
            session.pop("placa_prelogueada", None)
            flash("Vehículo no válido o no pertenece a su empresa.", "danger")
            return redirect(url_for('flotacarga.preoperacional_tc'))
            
        ref = str(vehiculo.get('referencia', '')).lower()
        if 'dobletroque' in ref:
            tipo_enum = 'Dobletroque'
        elif 'sencillo' in ref:
            tipo_enum = 'Camión Sencillo'
        else:
            tipo_enum = 'NPR / Turbo'

        return render_template(
            'C_preoperacional_tc.html',
            vehiculo=vehiculo,
            tipo_enum=tipo_enum,
            nombre_conductor=session.get('nombre'),
            nit=session.get('nit'),
            empresa=session.get('empresa')
        )
        
    return render_template(
        'C_preoperacional_tc.html', 
        scanner_mode=True,
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre')
    )

@bp_flotacarga.route('/preoperacional/validar_qr', methods=['POST'])
@login_required_custom
def validar_qr():
    """Valida de forma cruzada el QR escaneado por el operario."""
    data = request.get_json(silent=True) or {}
    placa = (data.get("placa") or "").upper().strip()
    qr_nit = str(data.get("nit") or "").strip()
    
    session_nit = str(session.get("empresa_id") or "").strip()
    session_empresa = session.get("empresa")

    if not placa or not qr_nit:
        return jsonify(success=False, message="Datos de QR incompletos."), 400

    if qr_nit != session_nit:
        return jsonify(success=False, message="Este vehículo pertenece a otra empresa corporativa."), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, empresa, id_empresa FROM vehiculos WHERE placa = %s LIMIT 1", (placa,))
    v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="El vehículo no está registrado en el sistema."), 404
    
    if str(v["id_empresa"]) != session_nit:
        cur.close()
        return jsonify(success=False, message="Consistencia rota: El vehículo no pertenece a su empresa asignada."), 403

    cur.execute("UPDATE vehiculos SET estatus='Prelogueado' WHERE id=%s", (v["id"],))
    mysql.connection.commit()
    cur.close()

    session["placa_prelogueada"] = placa
    return jsonify(success=True, message="Vehículo verificado con éxito.")

@bp_flotacarga.route('/preoperacional/guardar', methods=['POST'])
@login_required_custom
def guardar_inspeccion():
    """Procesa el formulario, aplica motor de reglas y envía correo con diseño corporativo."""
    if "placa_prelogueada" not in session:
        return jsonify(success=False, message="Acceso denegado. No hay vehículo prelogueado."), 403

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        empresa_id = session.get("empresa_id")
        empresa_nombre = session.get("empresa")
        usuario_id = session.get("usuario_id")
        placa = session["placa_prelogueada"]
        
        # 1. GENERACIÓN DEL CONSECUTIVO
        now = datetime.now()
        anio_actual = now.year
        fecha_inspeccion = now.date()
        hora_inspeccion = now.time().strftime("%H:%M:%S")

        cur.execute("""
            SELECT COUNT(*) as total FROM inspeccion_preoperacional_carga 
            WHERE id_empresa = %s AND YEAR(fecha_inspeccion) = %s
        """, (empresa_id, anio_actual))
        contador = cur.fetchone()['total'] + 1
        siglas = "".join([word[0] for word in empresa_nombre.split() if word.isalpha()])[:4].upper()
        consecutivo = f"{siglas}-{anio_actual}-{str(contador).zfill(5)}"

        def get_int(field_name, default=1):
            try:
                return int(request.form.get(field_name, default))
            except ValueError:
                return default

        def get_date(field_name):
            val = request.form.get(field_name)
            return val if val else None

        # 3. MOTOR DE REGLAS Y ALERTAS
        novedades_rojas = []
        novedades_amarillas = []

        docs_config = {
            'doc_licencia_conduccion': ('Licencia de Conducción', 'fecha_vence_licencia'),
            'doc_soat_vigente': ('SOAT', 'fecha_vence_soat'),
            'doc_tecnomecanica_vigente': ('Revisión Tecnomecánica', 'fecha_vence_tecnomecanica'),
            'doc_tarjeta_operacion': ('Tarjeta de Operación', 'fecha_vence_tarjeta_operacion'),
            'doc_manifiesto_carga': ('Manifiesto de Carga', 'fecha_vence_manifiesto')
        }

        for doc_key, (doc_name, date_field) in docs_config.items():
            if get_int(doc_key, 0) == 1:
                fecha_str = get_date(date_field)
                if fecha_str:
                    try:
                        vence = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                        dias_restantes = (vence - fecha_inspeccion).days
                        if dias_restantes < 0:
                            novedades_rojas.append(f"VENCIDO: {doc_name} venció hace {abs(dias_restantes)} días.")
                        elif dias_restantes <= 30:
                            novedades_amarillas.append(f"POR VENCER: {doc_name} vence en {dias_restantes} días ({fecha_str}).")
                    except ValueError:
                        pass
            else:
                novedades_rojas.append(f"FALTANTE: No porta {doc_name}.")

        fields_3_state = [
            'mec_nivel_aceite_motor', 'mec_liquido_frenos', 'mec_liquido_embrague', 'mec_nivel_refrigerante',
            'mec_estado_correas', 'mec_ausencia_fugas', 'luc_altas_bajas', 'luc_frenos_stop',
            'luc_direccionales', 'luc_parqueo_estacionarias', 'luc_reversa_alarma', 'luc_delimitadoras_cocuyos',
            'lla_presion_delanteras', 'lla_presion_traseras_pachas', 'lla_profundidad_labrado',
            'lla_tuercas_pernos', 'lla_repuesto_operativa', 'lla_suspension_muelles', 'fre_pedal_firme',
            'fre_parqueo_mano', 'fre_presion_aire_manometro', 'fre_juego_direccion', 'fre_pito_corneta',
            'fre_limpiaparabrisas_plumillas', 'car_estado_estructura', 'car_compuertas_carpas_amarres',
            'car_cinturones_seguridad', 'car_espejos_retrovisores', 'car_vidrio_parabrisas',
            'equ_extintor_10lbs', 'equ_tacos_bloqueo', 'equ_senales_reflectivas', 'equ_gato_hidraulico',
            'equ_cruceta_herramientas', 'equ_botiquin_completo'
        ]

        for f_name in fields_3_state:
            val = get_int(f_name)
            clean_name = f_name.replace('mec_', '').replace('luc_', '').replace('lla_', '').replace('fre_', '').replace('car_', '').replace('equ_', '').replace('_', ' ').title()
            if val == 2:
                novedades_amarillas.append(f"{clean_name}")
            elif val == 3:
                novedades_rojas.append(f"{clean_name}")

        observaciones = request.form.get('observaciones_hallazgos', '').strip()
        
        # --- CÁLCULO AUTOMÁTICO DE APROBACIÓN ---
        vehiculo_aprobado = 0 if len(novedades_rojas) > 0 else 1

        # 4. ENVÍO DE CORREO DINÁMICO (DISEÑO CORPORATIVO HTML)
        alerta_enviada = 0
        alerta_resumen = None
        alerta_dest = None

        if novedades_rojas or novedades_amarillas:
            cur.execute("""
                SELECT email FROM contactos 
                WHERE id_empresa = %s AND area_contacto IN ('logistica', 'talentohumano')
            """, (empresa_id,))
            contactos_db = cur.fetchall()
            correos_destino = [c['email'] for c in contactos_db if c.get('email')]

            if correos_destino and EMAIL_USER and EMAIL_PASS:
                try:
                    destinatarios_str = ", ".join(correos_destino)
                    
                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = f"Alerta Preoperacional | {empresa_nombre} | Placa {placa}"
                    msg["From"] = EMAIL_FROM
                    msg["To"] = destinatarios_str 

                    dictamen_text = "<span style='color:#10b981; font-weight:bold;'>OPERATIVO (Sin Novedades Críticas)</span>" if vehiculo_aprobado == 1 else "<span style='color:#ef4444; font-weight:bold;'>INMOVILIZADO (Novedades Críticas Reportadas)</span>"
                    
                    alerta_critica_html = ""
                    if vehiculo_aprobado == 0:
                        alerta_critica_html = """
                        <div style="background-color: #fee2e2; border: 2px solid #ef4444; padding: 18px; margin-bottom: 25px; border-radius: 8px; text-align: center;">
                            <h2 style="color: #b91c1c; margin: 0 0 10px 0; font-size: 18px;">⚠️ ¡ALERTA CRÍTICA DE SEGURIDAD! ⚠️</h2>
                            <p style="color: #991b1b; margin: 0; font-size: 15px; font-weight: bold; line-height: 1.4;">
                                ESTE VEHÍCULO NO ES APTO PARA OPERAR. PRESENTA NOVEDADES QUE NO GARANTIZAN LA SEGURIDAD VIAL. SE REQUIERE INMOVILIZACIÓN E INTERVENCIÓN INMEDIATA.
                            </p>
                        </div>
                        """
                    
                    html_body = f"""
                    <!DOCTYPE html>
                    <html lang="es">
                    <head>
                        <meta charset="UTF-8">
                        <title>Alerta Preoperacional</title>
                    </head>
                    <body style="margin: 0; padding: 0; font-family: 'Segoe UI', Arial, sans-serif; background-color: #f4f6f8;">
                        <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #f4f6f8; padding: 20px;">
                            <tr>
                                <td align="center">
                                    <table width="600" border="0" cellspacing="0" cellpadding="0" style="background-color: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 12px rgba(0,0,0,0.05);">
                                        <tr>
                                            <td style="background-color: #015249; padding: 25px; text-align: center;">
                                                <h1 style="color: #ffffff; margin: 0; font-size: 24px; letter-spacing: 1px;">BQA-ONE</h1>
                                                <p style="color: #eefcf9; margin: 5px 0 0 0; font-size: 14px; opacity: 0.9;">Alerta de Seguridad Vial y Flota</p>
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="padding: 30px;">
                                                {alerta_critica_html}
                                                <p style="color: #374151; font-size: 15px; line-height: 1.5; margin-top: 0;">
                                                    Se ha registrado una nueva inspección preoperacional con hallazgos que requieren su atención para la empresa <strong>{empresa_nombre}</strong>.
                                                </p>
                                                <table width="100%" border="0" cellspacing="0" cellpadding="12" style="background-color: #f9fafb; border-left: 4px solid #015249; margin-bottom: 25px; border-radius: 0 6px 6px 0;">
                                                    <tr>
                                                        <td style="color: #4b5563; font-size: 14px; line-height: 1.6;">
                                                            <strong>Consecutivo:</strong> {consecutivo}<br>
                                                            <strong>Placa del Vehículo:</strong> <span style="font-size: 16px; font-weight: bold; color: #015249;">{placa}</span><br>
                                                            <strong>Conductor:</strong> {session.get('nombre')}<br>
                                                            <strong>Fecha de Reporte:</strong> {fecha_inspeccion} {hora_inspeccion}<br>
                                                            <strong>Dictamen General:</strong> {dictamen_text}
                                                        </td>
                                                    </tr>
                                                </table>
                    """

                    if novedades_rojas:
                        html_body += """
                                                <h3 style="color: #dc2626; margin-bottom: 10px; font-size: 16px; border-bottom: 1px solid #fee2e2; padding-bottom: 5px;">🛑 Novedades Críticas (Rojas)</h3>
                                                <ul style="color: #991b1b; font-size: 14px; line-height: 1.5; margin-top: 0; margin-bottom: 25px; padding-left: 20px;">
                        """
                        for n in novedades_rojas: html_body += f"<li style='margin-bottom: 4px;'>{n}</li>"
                        html_body += "</ul>"

                    if novedades_amarillas:
                        html_body += """
                                                <h3 style="color: #d97706; margin-bottom: 10px; font-size: 16px; border-bottom: 1px solid #fef3c7; padding-bottom: 5px;">⚠️ Novedades de Ajuste (Amarillas)</h3>
                                                <ul style="color: #b45309; font-size: 14px; line-height: 1.5; margin-top: 0; margin-bottom: 25px; padding-left: 20px;">
                        """
                        for n in novedades_amarillas: html_body += f"<li style='margin-bottom: 4px;'>{n}</li>"
                        html_body += "</ul>"
                    
                    if observaciones:
                        html_body += f"""
                                                <h3 style="color: #015249; margin-bottom: 10px; font-size: 16px; border-bottom: 1px solid #e5e7eb; padding-bottom: 5px;">📝 Observaciones del Conductor</h3>
                                                <p style="color: #4b5563; font-size: 14px; line-height: 1.5; background-color: #f3f4f6; padding: 12px; border-radius: 6px; font-style: italic;">"{observaciones}"</p>
                        """
                    
                    html_body += f"""
                                            </td>
                                        </tr>
                                        <tr>
                                            <td style="background-color: #f1f5f9; padding: 20px; text-align: center; color: #64748b; font-size: 12px; border-top: 1px solid #e2e8f0;">
                                                <p style="margin: 0; line-height: 1.5;">Este es un mensaje automático generado por el módulo de logística y flota de <strong>BQA-ONE Technology</strong>.</p>
                                                <p style="margin: 5px 0 0 0;">Por favor no responda directamente a este correo.</p>
                                                <p style="margin: 15px 0 0 0;">&copy; {anio_actual} Baquía ESM. Todos los derechos reservados.</p>
                                            </td>
                                        </tr>
                                    </table>
                                </td>
                            </tr>
                        </table>
                    </body>
                    </html>
                    """
                    
                    msg.attach(MIMEText(html_body, "html"))

                    if EMAIL_PORT == 465:
                        with smtplib.SMTP_SSL(EMAIL_HOST, EMAIL_PORT) as server:
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.send_message(msg)
                    else: 
                        with smtplib.SMTP(EMAIL_HOST, EMAIL_PORT) as server:
                            server.starttls()
                            server.login(EMAIL_USER, EMAIL_PASS)
                            server.send_message(msg)

                    alerta_enviada = 1
                    alerta_dest = destinatarios_str
                    alerta_resumen = f"Rojas: {len(novedades_rojas)} | Amarillas: {len(novedades_amarillas)}"

                except Exception as err:
                    print(f"Error interno enviando correo: {err}")
                    alerta_enviada = 0
                    alerta_dest = "Fallo envío"
                    alerta_resumen = f"Intentó enviar a: {destinatarios_str}. Detectadas {len(novedades_rojas)} rojas."
            else:
                alerta_enviada = 0
                alerta_dest = "Sin contactos o Credenciales SMTP faltantes"
                alerta_resumen = f"Configuración SMTP no disponible. Detectadas {len(novedades_rojas)} rojas."

        # 5. INSERCIÓN A BASE DE DATOS (Exactamente 61 Parámetros)
        kilometraje = get_int('kilometraje_inicial', 0)
        ruta = request.form.get('ruta_destino', '')

        query = """
            INSERT INTO inspeccion_preoperacional_carga (
                id_usuario_conductor, id_empresa, consecutivo_anual, fecha_inspeccion, hora_inspeccion, 
                nombre_conductor, placa_vehiculo, tipo_vehiculo, kilometraje_inicial, ruta_destino, 
                
                doc_licencia_conduccion, fecha_vence_licencia, doc_soat_vigente, fecha_vence_soat,
                doc_tecnomecanica_vigente, fecha_vence_tecnomecanica, doc_tarjeta_operacion, fecha_vence_tarjeta_operacion,
                doc_manifiesto_carga, fecha_vence_manifiesto, 
                
                mec_nivel_aceite_motor, mec_liquido_frenos, mec_liquido_embrague, mec_nivel_refrigerante, 
                mec_estado_correas, mec_ausencia_fugas, luc_altas_bajas, luc_frenos_stop, luc_direccionales, 
                luc_parqueo_estacionarias, luc_reversa_alarma, luc_delimitadoras_cocuyos, lla_presion_delanteras, 
                lla_presion_traseras_pachas, lla_profundidad_labrado, lla_tuercas_pernos, lla_repuesto_operativa, 
                lla_suspension_muelles, fre_pedal_firme, fre_parqueo_mano, fre_presion_aire_manometro, 
                fre_juego_direccion, fre_pito_corneta, fre_limpiaparabrisas_plumillas, car_estado_estructura, 
                car_compuertas_carpas_amarres, car_cinturones_seguridad, car_espejos_retrovisores, car_vidrio_parabrisas, 
                
                equ_extintor_10lbs, equ_tacos_bloqueo, equ_senales_reflectivas, equ_gato_hidraulico, 
                equ_cruceta_herramientas, equ_botiquin_completo, observaciones_hallazgos, vehiculo_aprobado, 
                
                alerta_email_enviada, alerta_destinatario, alerta_resumen_novedades,
                firma_digital_conductor
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s
            )
        """

        params = (
            usuario_id, empresa_id, consecutivo, fecha_inspeccion, hora_inspeccion,
            session.get('nombre'), placa, request.form.get('tipo_vehiculo'), kilometraje, ruta,
            
            get_int('doc_licencia_conduccion', 0), get_date('fecha_vence_licencia'),
            get_int('doc_soat_vigente', 0), get_date('fecha_vence_soat'),
            get_int('doc_tecnomecanica_vigente', 0), get_date('fecha_vence_tecnomecanica'),
            get_int('doc_tarjeta_operacion', 0), get_date('fecha_vence_tarjeta_operacion'),
            get_int('doc_manifiesto_carga', 0), get_date('fecha_vence_manifiesto'),
            
            get_int('mec_nivel_aceite_motor'), get_int('mec_liquido_frenos'), get_int('mec_liquido_embrague'),
            get_int('mec_nivel_refrigerante'), get_int('mec_estado_correas'), get_int('mec_ausencia_fugas'),
            get_int('luc_altas_bajas'), get_int('luc_frenos_stop'), get_int('luc_direccionales'),
            get_int('luc_parqueo_estacionarias'), get_int('luc_reversa_alarma'), get_int('luc_delimitadoras_cocuyos'),
            get_int('lla_presion_delanteras'), get_int('lla_presion_traseras_pachas'), get_int('lla_profundidad_labrado'),
            get_int('lla_tuercas_pernos'), get_int('lla_repuesto_operativa'), get_int('lla_suspension_muelles'),
            get_int('fre_pedal_firme'), get_int('fre_parqueo_mano'), get_int('fre_presion_aire_manometro'),
            get_int('fre_juego_direccion'), get_int('fre_pito_corneta'), get_int('fre_limpiaparabrisas_plumillas'),
            get_int('car_estado_estructura'), get_int('car_compuertas_carpas_amarres'), get_int('car_cinturones_seguridad'),
            get_int('car_espejos_retrovisores'), get_int('car_vidrio_parabrisas'),
            
            get_int('equ_extintor_10lbs'), get_int('equ_tacos_bloqueo'), get_int('equ_senales_reflectivas'),
            get_int('equ_gato_hidraulico'), get_int('equ_cruceta_herramientas'), get_int('equ_botiquin_completo'),
            observaciones, vehiculo_aprobado,
            
            alerta_enviada, alerta_dest, alerta_resumen,
            f"FIRMA-DIGITAL-{session.get('cedula')}"
        )

        cur.execute(query, params)

        cur.execute("UPDATE vehiculos SET estatus = 'Logueado' WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        
        mysql.connection.commit()
        cur.close()

        session.pop("placa_prelogueada", None)
        
        # --- AQUÍ RADICA EL CAMBIO PRINCIPAL PARA EL FRONTEND AJAX ---
        return jsonify(
            success=True, 
            message=f"La inspección del vehículo <b>{placa}</b> se ha registrado y firmado digitalmente de manera exitosa.<br><br><b>Consecutivo:</b> {consecutivo}",
            redirect_url=url_for('gestionavicola_bp.router_universal', modulo='flota')
        )

    except Exception as e:
        print(f"Error crítico en guardar inspección: {e}")
        return jsonify(success=False, message=f"Ocurrió un error interno al guardar la inspección: {e}"), 500

@bp_flotacarga.route('/combustible')
@login_required_custom
def combustible_tc():
    return render_template('C_combustible_tc.html')