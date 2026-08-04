# app/blueprints/C_bp_preoperacional.py
import os
import json
from flask import Blueprint, render_template, session, redirect, url_for, request, jsonify, flash
from app import mysql
from app.utils import login_required_custom
from datetime import datetime
import MySQLdb.cursors
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

bp_preoperacional = Blueprint('preoperacional', __name__)

EMAIL_HOST = os.environ.get("EMAIL_HOST", "smtp.hostinger.com")
EMAIL_PORT = int(os.environ.get("EMAIL_PORT", "465"))
EMAIL_USER = os.environ.get("EMAIL_USER", "bqa-one@baquia-esm.com")
EMAIL_PASS = os.environ.get("EMAIL_PASS")
EMAIL_FROM = os.environ.get("EMAIL_FROM", EMAIL_USER)

@bp_preoperacional.route('/preoperacional')
@login_required_custom
def preoperacional_tc():
    if "placa_prelogueada" in session:
        placa = session["placa_prelogueada"]
        empresa_id = session.get("empresa_id")
        
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT * FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        vehiculo = cur.fetchone()
        
        if not vehiculo:
            session.pop("placa_prelogueada", None)
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
            empresa=session.get('empresa')
        )
        
    return render_template(
        'C_preoperacional_tc.html', 
        scanner_mode=True,
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        nombre=session.get('nombre')
    )

@bp_preoperacional.route('/preoperacional/validar_qr', methods=['POST'])
@login_required_custom
def validar_qr():
    data = request.get_json(silent=True) or {}
    placa = (data.get("placa") or "").upper().strip()
    qr_nit = str(data.get("nit") or "").strip()
    session_nit = str(session.get("empresa_id") or "").strip()

    if not placa or not qr_nit: return jsonify(success=False, message="Datos de QR incompletos."), 400
    if qr_nit != session_nit: return jsonify(success=False, message="Este vehículo pertenece a otra empresa."), 403

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, empresa, id_empresa FROM vehiculos WHERE placa = %s LIMIT 1", (placa,))
    v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="Vehículo no registrado."), 404
    if str(v["id_empresa"]) != session_nit:
        cur.close()
        return jsonify(success=False, message="Consistencia rota: El vehículo no es suyo."), 403

    cur.execute("UPDATE vehiculos SET estatus='Prelogueado' WHERE id=%s", (v["id"],))
    mysql.connection.commit()
    cur.close()

    session["placa_prelogueada"] = placa
    return jsonify(success=True, message="Vehículo verificado.")

@bp_preoperacional.route('/preoperacional/guardar', methods=['POST'])
@login_required_custom
def guardar_inspeccion():
    if "placa_prelogueada" not in session:
        flash("Acceso denegado.", "danger")
        return redirect(url_for('preoperacional.preoperacional_tc'))

    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        empresa_id = session.get("empresa_id")
        empresa_nombre = session.get("empresa")
        usuario_id = session.get("usuario_id")
        placa = session["placa_prelogueada"]
        
        detalles_novedades_json = request.form.get('detalles_novedades_json', '{}')
        try: json.loads(detalles_novedades_json) 
        except: detalles_novedades_json = '{}'

        estado_llantas_json = request.form.get('estado_llantas_json', '{}')
        try: llantas_dict = json.loads(estado_llantas_json)
        except: llantas_dict = {}; estado_llantas_json = '{}'
        
        # Base64 (Foto y Firma)
        foto_conductor_base64 = request.form.get('foto_conductor_base64')
        firma_grafica_base64 = request.form.get('firma_grafica_base64')
        
        # Geolocalización capturada en el formulario
        latitud_raw = request.form.get('latitud', '')
        longitud_raw = request.form.get('longitud', '')
        latitud = float(latitud_raw) if latitud_raw.strip() else None
        longitud = float(longitud_raw) if longitud_raw.strip() else None
        
        now = datetime.now()
        anio_actual = now.year
        fecha_inspeccion = now.date()
        hora_inspeccion = now.time().strftime("%H:%M:%S")

        # =========================================================
        # INTEGRACIÓN CON VIAJE ACTIVO
        # Si el operador inició viaje antes, usamos ese consecutivo
        # =========================================================
        if session.get('consecutivo_viaje'):
            consecutivo = session.get('consecutivo_viaje')
        else:
            cur.execute("SELECT COUNT(*) as total FROM inspeccion_preoperacional_carga WHERE id_empresa = %s AND YEAR(fecha_inspeccion) = %s", (empresa_id, anio_actual))
            contador = cur.fetchone()['total'] + 1
            siglas = "".join([word[0] for word in empresa_nombre.split() if word.isalpha()])[:4].upper()
            consecutivo = f"{siglas}-{anio_actual}-{str(contador).zfill(5)}"

        def get_int(field_name, default=1):
            try: return int(request.form.get(field_name, default))
            except: return default

        def get_date(field_name):
            val = request.form.get(field_name)
            return val if val else None

        novedades_rojas = []
        novedades_amarillas = []

        docs_config = {
            'doc_licencia_conduccion': ('Licencia Conducción', 'fecha_vence_licencia'), 'doc_soat_vigente': ('SOAT', 'fecha_vence_soat'),
            'doc_tecnomecanica_vigente': ('Tecnomecánica', 'fecha_vence_tecnomecanica'), 'doc_tarjeta_operacion': ('Tarjeta Operación', 'fecha_vence_tarjeta_operacion'),
            'doc_manifiesto_carga': ('Manifiesto Carga', 'fecha_vence_manifiesto')
        }

        for doc_key, (doc_name, date_field) in docs_config.items():
            if get_int(doc_key, 0) == 1:
                fecha_str = get_date(date_field)
                if fecha_str:
                    try:
                        vence = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                        dias_restantes = (vence - fecha_inspeccion).days
                        if dias_restantes < 0: novedades_rojas.append(f"VENCIDO: {doc_name}")
                        elif dias_restantes <= 30: novedades_amarillas.append(f"POR VENCER: {doc_name}")
                    except: pass
            else: novedades_rojas.append(f"FALTANTE: {doc_name}")

        fields_3_state = [
            'mec_nivel_aceite_motor', 'mec_liquido_frenos', 'mec_liquido_embrague', 'mec_nivel_refrigerante', 'mec_estado_correas', 'mec_ausencia_fugas', 'luc_altas_bajas', 'luc_frenos_stop', 'luc_direccionales', 'luc_parqueo_estacionarias', 'luc_reversa_alarma', 'luc_delimitadoras_cocuyos', 'lla_tuercas_pernos', 'lla_repuesto_operativa', 'lla_suspension_muelles', 'fre_pedal_firme', 'fre_parqueo_mano', 'fre_presion_aire_manometro', 'fre_juego_direccion', 'fre_pito_corneta', 'fre_limpiaparabrisas_plumillas', 'car_estado_estructura', 'car_compuertas_carpas_amarres', 'car_cinturones_seguridad', 'car_espejos_retrovisores', 'car_vidrio_parabrisas', 'equ_extintor_10lbs', 'equ_tacos_bloqueo', 'equ_senales_reflectivas', 'equ_gato_hidraulico', 'equ_cruceta_herramientas', 'equ_botiquin_completo'
        ]

        for f_name in fields_3_state:
            val = get_int(f_name)
            clean_name = f_name.replace('mec_', '').replace('luc_', '').replace('lla_', '').replace('fre_', '').replace('car_', '').replace('equ_', '').replace('_', ' ').title()
            if val == 2: novedades_amarillas.append(f"{clean_name}")
            elif val == 3: novedades_rojas.append(f"{clean_name}")

        for pos_llanta, datos_llanta in llantas_dict.items():
            labrado = datos_llanta.get('labrado', 'operativa')
            novedad_texto = datos_llanta.get('novedad', '').strip()
            nombre_legible = datos_llanta.get('nombre_legible', pos_llanta)
            if labrado == 'lisa': novedades_rojas.append(f"Falla Crítica: {nombre_legible} LISA.")
            elif labrado == 'baja': novedades_amarillas.append(f"Desgaste: {nombre_legible} BAJO.")
            elif novedad_texto: novedades_amarillas.append(f"Novedad en {nombre_legible}: {novedad_texto}")

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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
        """
        params = (
            usuario_id, empresa_id, consecutivo, fecha_inspeccion, hora_inspeccion,
            session.get('nombre'), placa, request.form.get('tipo_vehiculo', 'NPR / Turbo'), kilometraje, ruta,
            get_int('doc_licencia_conduccion', 0), get_date('fecha_vence_licencia'), get_int('doc_soat_vigente', 0), get_date('fecha_vence_soat'), get_int('doc_tecnomecanica_vigente', 0), get_date('fecha_vence_tecnomecanica'), get_int('doc_tarjeta_operacion', 0), get_date('fecha_vence_tarjeta_operacion'), get_int('doc_manifiesto_carga', 0), get_date('fecha_vence_manifiesto'),
            get_int('mec_aceite_motor'), get_int('mec_liquido_frenos'), get_int('mec_liquido_embrague'), get_int('mec_refrigerante'), get_int('mec_correas'), get_int('mec_fugas'), get_int('luc_altas'), get_int('luc_frenos'), get_int('luc_direccionales'), get_int('luc_parqueo_estacionarias'), get_int('luc_reversa'), get_int('luc_cocuyos'),
            get_int('llan_tuercas'), get_int('llan_repuesto'), get_int('llan_muelles'), get_int('fren_pedal'), get_int('fren_mano'), get_int('fren_manometro'), get_int('fren_juego_direccion'), get_int('fre_pito_corneta'), get_int('fren_plumillas'),
            get_int('est_compuertas'), get_int('est_carpas'), get_int('est_cinturones'), get_int('est_espejos'), get_int('est_parabrisas'),
            get_int('eq_extintor'), get_int('eq_tacos'), get_int('eq_senales'), get_int('equ_gato_hidraulico'), get_int('eq_herramientas'), get_int('eq_botiquin'),
            observaciones, detalles_novedades_json, estado_llantas_json, vehiculo_aprobado, alerta_enviada, alerta_dest, alerta_resumen, f"HASH-AUDIT-{session.get('cedula', usuario_id)}",
            foto_conductor_base64, firma_grafica_base64
        )
        cur.execute(query, params)
        cur.execute("UPDATE vehiculos SET estatus = 'Logueado' WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        
        # --- REGISTRO OFICIAL DE SESIÓN DE FLOTA CON COORDENADAS GPS ---
        cur.execute("""
            INSERT INTO historial_sesiones_flota (id_empresa, id_usuario, placa_vehiculo, fecha_login, estado_sesion, latitud, longitud)
            VALUES (%s, %s, %s, NOW(), 'ACTIVA', %s, %s)
        """, (empresa_id, usuario_id, placa, latitud, longitud))
        # --------------------------------------------------------------------------------
        
        mysql.connection.commit()
        cur.close()

        flash(f"La inspección se ha registrado y auditado (Selfie + Firma) exitosamente. Consecutivo: {consecutivo}", "success")
        try: return redirect(url_for('router_universal', modulo='flota'))
        except: return redirect(url_for('flotacarga.dashboard_operador'))

    except Exception as e:
        flash(f"Error interno: {str(e)}", "danger")
        return redirect(url_for('preoperacional.preoperacional_tc'))