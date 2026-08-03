# app/blueprints/C_bp_combustible_flota.py
import os
import base64
import uuid
from flask import Blueprint, render_template, session, redirect, url_for, request, flash
from app import mysql
from app.utils import login_required_custom
from datetime import datetime
import MySQLdb.cursors

bp_combustible_flota = Blueprint('combustible_flota', __name__)

@bp_combustible_flota.route('/combustible', methods=['GET', 'POST'])
@login_required_custom
def registrar_combustible():
    empresa_id = session.get('empresa_id')
    
    if request.method == 'POST':
        # SEGURIDAD: Priorizar la placa de la sesión sobre la del formulario
        placa_session = session.get('placa_prelogueada')
        placa = placa_session if placa_session else str(request.form.get('placa', '')).upper().strip()
        
        tipo_combustible = request.form.get('tipo_combustible', '').strip()
        fecha_tanqueo = request.form.get('fecha_tanqueo')
        kilometraje = request.form.get('kilometraje')
        galones = request.form.get('galones')
        valor_total = request.form.get('valor_total')
        
        usuario_id = session.get('usuario_id')
        nombre_operador = session.get('nombre')
        empresa_nombre = session.get('empresa')
        
        # Geolocalización (si viene en el form)
        latitud = request.form.get('latitud') or None
        longitud = request.form.get('longitud') or None

        if not all([placa, tipo_combustible, fecha_tanqueo, kilometraje, galones, valor_total]):
            flash("Por favor, complete todos los campos obligatorios.", "danger")
            return redirect(url_for('combustible_flota.registrar_combustible'))

        # CANDADO MULTIEMPRESA: Verificar que la placa pertenece a la empresa de la sesión
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT id FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, empresa_id))
        vehiculo = cur.fetchone()
        
        if not vehiculo:
            cur.close()
            flash("El vehículo seleccionado no es válido o no pertenece a su empresa.", "danger")
            return redirect(url_for('combustible_flota.registrar_combustible'))

        # Función auxiliar para procesar y guardar imágenes en Base64
        def guardar_imagen_fisica(base64_data, prefijo):
            if base64_data and ',' in base64_data:
                try:
                    header, encoded = base64_data.split(",", 1)
                    data = base64.b64decode(encoded)
                    
                    upload_folder = os.path.join('app', 'static', 'uploads', 'vouchers')
                    os.makedirs(upload_folder, exist_ok=True)
                    
                    # Soporte para PNG (Firma) o JPG (Voucher/Selfie)
                    ext = "png" if "image/png" in header else "jpg"
                    filename = f"{prefijo}_{placa}_{uuid.uuid4().hex[:8]}.{ext}"
                    filepath = os.path.join(upload_folder, filename)
                    
                    with open(filepath, "wb") as f:
                        f.write(data)
                        
                    return f"uploads/vouchers/{filename}"
                except Exception as e:
                    print(f"Error procesando imagen {prefijo}: {e}")
            return None

        # Procesamos las tres evidencias gráficas y guardamos sus rutas
        foto_voucher_path = guardar_imagen_fisica(request.form.get('foto_voucher_base64', ''), 'voucher')
        foto_selfie_path = guardar_imagen_fisica(request.form.get('foto_selfie_base64', ''), 'selfie')
        firma_path = guardar_imagen_fisica(request.form.get('firma_grafica_base64', ''), 'firma')

        try:
            # Modificamos el INSERT para coincidir exactamente con el esquema SQL
            cur.execute("""
                INSERT INTO vehiculos_combustible_flota (
                    id_empresa, empresa, placa, tipo_combustible, fecha_tanqueo, 
                    kilometraje_actual, galones, valor_total, id_operador, nombre_operador, 
                    foto_voucher_path, foto_selfie_path, firma_path, latitud, longitud
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, empresa_nombre, placa, tipo_combustible, fecha_tanqueo,
                kilometraje, galones, valor_total, usuario_id, nombre_operador, 
                foto_voucher_path, foto_selfie_path, firma_path, latitud, longitud
            ))
            mysql.connection.commit()
            flash("Registro de tanqueo guardado exitosamente.", "success")
            
            return redirect(url_for('router_universal', modulo='flota'))
            
        except Exception as e:
            mysql.connection.rollback()
            flash(f"Error al guardar el registro: {str(e)}", "danger")
        finally:
            cur.close()

        return redirect(url_for('combustible_flota.registrar_combustible'))

    # MÉTODO GET: Cargar vehículos de la empresa para el selector (Fallback)
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa, referencia FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    # Inyectamos la variable placa_prelogueada a la vista
    return render_template(
        'C_combustible_flota.html',
        vehiculos=vehiculos_db,
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        nit=session.get('empresa_id'),
        hoy=datetime.now().strftime('%Y-%m-%d'),
        placa_prelogueada=session.get('placa_prelogueada')
    )