# app/blueprints/C_bp_combustible_flota.py
import os
import base64
import time
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
    
    # 1. PARCHE AUTOMÁTICO DE BASE DE DATOS (Agrega las columnas si no existen)
    try:
        cur_patch = mysql.connection.cursor()
        cur_patch.execute("""
            ALTER TABLE vehiculos_combustible_flota 
            ADD COLUMN foto_voucher_path VARCHAR(255) NULL, 
            ADD COLUMN foto_selfie_path VARCHAR(255) NULL, 
            ADD COLUMN firma_path VARCHAR(255) NULL, 
            ADD COLUMN latitud DECIMAL(10, 8) NULL, 
            ADD COLUMN longitud DECIMAL(11, 8) NULL
        """)
        mysql.connection.commit()
        cur_patch.close()
    except Exception:
        # Si las columnas ya existen, el motor lanzará una excepción que podemos ignorar de forma segura
        pass

    if request.method == 'POST':
        placa = str(request.form.get('placa', '')).upper().strip()
        tipo_combustible = request.form.get('tipo_combustible', '').strip()
        fecha_tanqueo = request.form.get('fecha_tanqueo')
        kilometraje = request.form.get('kilometraje')
        galones = request.form.get('galones')
        valor_total = request.form.get('valor_total')
        
        # Nuevos datos de Evidencia y GPS
        foto_voucher_base64 = request.form.get('foto_voucher_base64')
        foto_selfie_base64 = request.form.get('foto_selfie_base64')
        firma_grafica_base64 = request.form.get('firma_grafica_base64')
        
        latitud_raw = request.form.get('latitud', '')
        longitud_raw = request.form.get('longitud', '')
        latitud = float(latitud_raw) if latitud_raw.strip() else None
        longitud = float(longitud_raw) if longitud_raw.strip() else None

        usuario_id = session.get('usuario_id')
        nombre_operador = session.get('nombre')
        empresa_nombre = session.get('empresa')

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

        # 2. PROCESAMIENTO Y ALMACENAMIENTO OPTIMIZADO DE IMÁGENES
        foto_voucher_path, foto_selfie_path, firma_path = None, None, None
        
        try:
            base_dir = os.path.abspath(os.path.dirname(__file__))
            static_dir = os.path.join(base_dir, '..', 'static')
            mes_anio = datetime.now().strftime('%Y_%m')
            
            # Crear ruta estructurada: /static/uploads/combustible/ID_EMPRESA/YYYY_MM/
            upload_rel_dir = os.path.join('uploads', 'combustible', str(empresa_id), mes_anio)
            upload_abs_dir = os.path.join(static_dir, upload_rel_dir)
            os.makedirs(upload_abs_dir, exist_ok=True)
            
            ts = str(int(time.time()))

            def guardar_imagen_b64(b64_str, prefijo, ext):
                if not b64_str: return None
                try:
                    head, data = b64_str.split(',', 1)
                    img_data = base64.b64decode(data)
                    filename = f"{prefijo}_{placa}_{ts}.{ext}"
                    filepath = os.path.join(upload_abs_dir, filename)
                    with open(filepath, 'wb') as f:
                        f.write(img_data)
                    # Guardamos la ruta relativa para facilitar su uso en Jinja (ej. url_for('static', filename=path))
                    return f"{upload_rel_dir}/{filename}".replace('\\', '/')
                except Exception as e:
                    print(f"Error procesando {prefijo}: {e}")
                    return None

            foto_voucher_path = guardar_imagen_b64(foto_voucher_base64, "voucher", "jpg")
            foto_selfie_path = guardar_imagen_b64(foto_selfie_base64, "selfie", "jpg")
            firma_path = guardar_imagen_b64(firma_grafica_base64, "firma", "png")
            
        except Exception as img_err:
            print(f"Alerta guardando imágenes: {img_err}")

        # 3. REGISTRO EN LA BASE DE DATOS
        try:
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
            flash("Registro de tanqueo y evidencias guardado exitosamente.", "success")
            
            # Retorna al enrutador maestro
            return redirect(url_for('router_universal', modulo='flota'))
            
        except Exception as e:
            mysql.connection.rollback()
            flash(f"Error al guardar el registro: {str(e)}", "danger")
        finally:
            cur.close()

        return redirect(url_for('combustible_flota.registrar_combustible'))

    # MÉTODO GET: Cargar vehículos de la empresa para el selector
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT placa, referencia FROM vehiculos WHERE id_empresa = %s ORDER BY placa ASC", (empresa_id,))
    vehiculos_db = cur.fetchall()
    cur.close()

    return render_template(
        'C_combustible_flota.html',
        vehiculos=vehiculos_db,
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        nit=session.get('empresa_id'),
        hoy=datetime.now().strftime('%Y-%m-%d')
    )