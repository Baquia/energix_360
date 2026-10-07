# MÓDULO: TRANSPORTE_ESPECIAL | SUBMÓDULO: EPS_FACTURACION (OPCIONAL P&P)
# app/blueprints/B_bp_procesofacturacion_eps_transporteespecial.py
import os
import json
import zipfile
import io
import csv
import hashlib
from datetime import datetime
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file, Response, current_app, jsonify
from app import mysql
from app.utils import login_required_custom, submodulo_required
from functools import wraps
import MySQLdb.cursors
import pytz

bp_procesofacturacion_eps = Blueprint('procesofacturacion_eps', __name__, url_prefix='/gestor_flotaespecial/facturacion_eps')

BOGOTA_TZ = pytz.timezone('America/Bogota')

def controlador_flotaespecial_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        if perfil not in ['controlador_flotaespecial', 'webmaster', 'facturacion'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil autorizado para pre-facturación.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# HELPER: ASEGURAR TABLAS DE FACTURACIÓN
# =========================================================
def asegurar_tablas_facturacion(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tarifario_eps_contratos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            id_eps_cliente VARCHAR(50) NOT NULL,
            numero_contrato VARCHAR(50) NOT NULL,
            codigo_servicio VARCHAR(50) NOT NULL,
            valor_unidad DECIMAL(12,2) NOT NULL DEFAULT 0.00,
            estado VARCHAR(20) DEFAULT 'ACTIVO',
            INDEX(id_empresa, id_eps_cliente)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS prefacturas_eps_tespecial (
            id INT AUTO_INCREMENT PRIMARY KEY,
            id_empresa INT NOT NULL,
            id_eps_cliente VARCHAR(50) NOT NULL,
            numero_contrato VARCHAR(50) NOT NULL,
            fecha_inicio DATE NOT NULL,
            fecha_fin DATE NOT NULL,
            valor_total DECIMAL(15,2) DEFAULT 0.00,
            cantidad_servicios INT DEFAULT 0,
            ruta_json_rips VARCHAR(255) NULL,
            codigo_cuv VARCHAR(100) NULL,
            estado_rips VARCHAR(50) DEFAULT 'PENDIENTE',
            estado_factura VARCHAR(50) DEFAULT 'PENDIENTE',
            cufe VARCHAR(255) NULL,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX(id_empresa)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    try:
        cur.execute("ALTER TABLE control_viajes_flota_especial ADD COLUMN id_prefactura INT NULL;")
    except:
        pass

# =========================================================
# VISTA PRINCIPAL DEL MÓDULO (Lotes de Facturación)
# =========================================================
@bp_procesofacturacion_eps.route('/', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
def index_facturacion():
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    asegurar_tablas_facturacion(cur)
    mysql.connection.commit()

    # Obtener lotes de prefacturas
    cur.execute("SELECT * FROM prefacturas_eps_tespecial WHERE id_empresa = %s ORDER BY id DESC", (empresa_id,))
    prefacturas = cur.fetchall()
    
    # Obtener servicios auditados pendientes de facturar (agrupados por id_viaje_padre)
    cur.execute("""
        SELECT id_viaje_padre, numero_prescripcion, id_eps_cliente, COUNT(*) as tramos, MAX(fecha_servicio) as ultima_fecha
        FROM control_viajes_flota_especial 
        WHERE id_empresa = %s AND estatus_servicio = 'AUDITADO' AND id_prefactura IS NULL
        GROUP BY id_viaje_padre, numero_prescripcion, id_eps_cliente
    """, (empresa_id,))
    pendientes = cur.fetchall()
    
    cur.close()
    return render_template(
        'B_modulo_procesofacturacion_eps.html', 
        nit=session.get('nit'), 
        empresa=session.get('empresa'), 
        nombre=session.get('nombre'),
        active_module='index_facturacion',
        prefacturas=prefacturas,
        pendientes=pendientes
    )

# =========================================================
# 1. CONSOLIDAR LOTE DE PRE-FACTURACIÓN
# =========================================================
@bp_procesofacturacion_eps.route('/consolidar_lote', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
def consolidar_lote():
    empresa_id = session.get('empresa_id')
    id_eps = request.form.get('id_eps_cliente')
    contrato = request.form.get('numero_contrato')
    fecha_inicio = request.form.get('fecha_inicio')
    fecha_fin = request.form.get('fecha_fin')

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        # Seleccionar viajes auditados en el rango que NO estén facturados
        cur.execute("""
            SELECT id, id_viaje_padre, numero_prescripcion, tipo_servicio 
            FROM control_viajes_flota_especial 
            WHERE id_empresa = %s AND id_eps_cliente = %s AND estatus_servicio = 'AUDITADO' 
              AND id_prefactura IS NULL AND fecha_servicio BETWEEN %s AND %s
        """, (empresa_id, id_eps, fecha_inicio, fecha_fin))
        viajes = cur.fetchall()

        if not viajes:
            flash('No se encontraron servicios auditados pendientes en el rango especificado.', 'warning')
            return redirect(url_for('procesofacturacion_eps.index_facturacion'))

        # Agrupar por servicio integral (IDA/VUELTA consolidado)
        servicios_integrales = set(v['id_viaje_padre'] for v in viajes if v['id_viaje_padre'])
        cantidad_servicios = len(servicios_integrales)
        
        # Calcular valor total basado en el tarifario
        valor_total = 0.00
        cur.execute("SELECT codigo_servicio, valor_unidad FROM tarifario_eps_contratos WHERE id_empresa=%s AND id_eps_cliente=%s AND numero_contrato=%s", (empresa_id, id_eps, contrato))
        tarifas = {t['codigo_servicio']: t['valor_unidad'] for t in cur.fetchall()}
        
        # Simplificación para el cálculo: se toma el código del primer tramo del viaje padre
        for viaje_padre in servicios_integrales:
            tramo = next((v for v in viajes if v['id_viaje_padre'] == viaje_padre), None)
            if tramo and tramo['tipo_servicio'] in tarifas:
                valor_total += float(tarifas[tramo['tipo_servicio']])

        # Crear Lote
        cur.execute("""
            INSERT INTO prefacturas_eps_tespecial 
            (id_empresa, id_eps_cliente, numero_contrato, fecha_inicio, fecha_fin, valor_total, cantidad_servicios, estado_rips)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'GENERADO')
        """, (empresa_id, id_eps, contrato, fecha_inicio, fecha_fin, valor_total, cantidad_servicios))
        lote_id = cur.lastrowid

        # Congelar viajes
        ids_viajes = [v['id'] for v in viajes]
        format_strings = ','.join(['%s'] * len(ids_viajes))
        cur.execute(f"UPDATE control_viajes_flota_especial SET id_prefactura = %s WHERE id IN ({format_strings})", [lote_id] + ids_viajes)
        
        mysql.connection.commit()
        flash(f'Lote #{lote_id} consolidado exitosamente. Servicios: {cantidad_servicios}', 'success')

    except Exception as e:
        mysql.connection.rollback()
        flash(f'Error al consolidar el lote: {str(e)}', 'danger')
    finally:
        cur.close()
    
    return redirect(url_for('procesofacturacion_eps.index_facturacion'))

# =========================================================
# 2. GENERACIÓN RIPS JSON (Res. 0948 de 2026 - Anexo 1)
# =========================================================
@bp_procesofacturacion_eps.route('/descargar_rips/<int:lote_id>', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
def descargar_rips(lote_id):
    empresa_id = session.get('empresa_id')
    num_factura = request.form.get('numero_factura', f"FEV-{lote_id}")
    
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        # 1. Obtener la prefactura
        cur.execute("SELECT * FROM prefacturas_eps_tespecial WHERE id = %s AND id_empresa = %s", (lote_id, empresa_id))
        lote = cur.fetchone()
        if not lote:
            flash('Lote de facturación no encontrado.', 'danger')
            return redirect(url_for('procesofacturacion_eps.index_facturacion'))

        # 2. Obtener los viajes consolidados cruzando con tarifario
        cur.execute("""
            SELECT c.*, t.valor_unidad
            FROM control_viajes_flota_especial c
            LEFT JOIN tarifario_eps_contratos t ON c.tipo_servicio = t.codigo_servicio AND c.id_empresa = t.id_empresa AND t.numero_contrato = %s
            WHERE c.id_prefactura = %s AND c.id_empresa = %s
        """, (lote['numero_contrato'], lote_id, empresa_id))
        viajes = cur.fetchall()

        if not viajes:
            flash('No hay viajes asociados a este lote.', 'danger')
            return redirect(url_for('procesofacturacion_eps.index_facturacion'))

        # 3. Agrupar por usuarios y armar la estructura RIPS JSON según Anexo 1 para Transporte
        usuarios_dict = {}
        consecutivo_servicio = 1

        for v in viajes:
            id_usr = v['id_usuario']
            if id_usr not in usuarios_dict:
                # Capturar datos demográficos inyectados desde el formulario del frontend
                cod_mun = request.form.get(f'mun_{id_usr}', '00000')
                cod_zona = request.form.get(f'zona_{id_usr}', '01')
                
                usuarios_dict[id_usr] = {
                    "tipoDocumentoIdentificacion": v.get('tipo_documento', 'CC'),
                    "numDocumentoIdentificacion": id_usr,
                    "tipoUsuario": "01",
                    "fechaNacimiento": "1990-01-01", # Placeholder demográfico requerido
                    "codSexo": "M",
                    "codPaisResidencia": "170",
                    "codMunicipioResidencia": cod_mun,
                    "codZonaTerritorialResidencia": cod_zona,
                    "incapacidad": "02",
                    "consecutivo": len(usuarios_dict) + 1,
                    "codPaisOrigen": "170",
                    "registroSIRAS": None,
                    "servicios": {
                        "otrosServicios": []
                    }
                }

            # Construir el nodo "otrosServicios" (Transporte - Código 03) según Anexo 1
            servicio_obj = {
                "codPrestador": session.get('nit')[:10],
                "numAutorizacion": v.get('numero_autorizacion') if v.get('numero_autorizacion') else None,
                "idMIPRES": v.get('numero_prescripcion') if v.get('numero_prescripcion') else None,
                "fechaSuministroTecnologia": v['fecha_servicio'].strftime('%Y-%m-%d %H:%M') if v.get('fecha_servicio') else None,
                "tipoOS": "03", # 03 corresponde a Transporte Especial
                "codTecnologiaSalud": v.get('tipo_servicio', '0000'), 
                "nomTecnologiaSalud": None, # Exención norma: null para transporte
                "cantidadOS": 1,
                "tipoDocumentoIdentificacion": None, # Exención norma: null para transporte
                "numDocumentoIdentificacion": None, # Exención norma: null para transporte
                "vrUnitOS": float(v.get('valor_unidad') or 0.0),
                "vrDispensacion": 0, # Exención norma: Siempre 0 para transporte
                "vrServicio": float(v.get('valor_unidad') or 0.0),
                "conceptoRecaudo": "05", # No aplica pago moderador por defecto
                "valorPagoModerador": 0,
                "numFEVPagoModerador": None,
                "codigoVIDA": None,
                "consecutivo": consecutivo_servicio
            }
            usuarios_dict[id_usr]["servicios"]["otrosServicios"].append(servicio_obj)
            consecutivo_servicio += 1

        rips_payload = {
            "numDocumentoIdObligado": session.get('nit'),
            "numFactura": num_factura,
            "tipoNota": None,
            "numNota": None,
            "usuarios": list(usuarios_dict.values())
        }

        # 4. Generar Archivo ZIP en Memoria
        json_str = json.dumps(rips_payload, ensure_ascii=False, indent=4)
        memory_file = io.BytesIO()
        with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(f"RIPS_{num_factura}.json", json_str)
        memory_file.seek(0)

        # 5. Actualizar estado de la prefactura
        cur.execute("UPDATE prefacturas_eps_tespecial SET estado_rips = 'JSON_GENERADO' WHERE id = %s", (lote_id,))
        mysql.connection.commit()

        return send_file(
            memory_file,
            mimetype='application/zip',
            as_attachment=True,
            download_name=f'RIPS_{num_factura}.zip'
        )

    except Exception as e:
        mysql.connection.rollback()
        flash(f'Error al generar JSON: {str(e)}', 'danger')
        return redirect(url_for('procesofacturacion_eps.index_facturacion'))
    finally:
        cur.close()

@bp_procesofacturacion_eps.route('/api/obtener_pacientes_lote/<int:lote_id>', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
def api_obtener_pacientes_lote(lote_id):
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    try:
        cur.execute("""
            SELECT DISTINCT id_usuario, nombre_usuario 
            FROM control_viajes_flota_especial 
            WHERE id_prefactura = %s AND id_empresa = %s
        """, (lote_id, session.get('empresa_id')))
        pacientes = cur.fetchall()
        return jsonify({"status": "success", "pacientes": pacientes}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        cur.close()

# =========================================================
# 3. REGISTRAR CUV (Código Único de Validación)
# =========================================================
@bp_procesofacturacion_eps.route('/registrar_cuv', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
def registrar_cuv():
    empresa_id = session.get('empresa_id')
    lote_id = request.form.get('lote_id')
    codigo_cuv = request.form.get('codigo_cuv').strip()

    cur = mysql.connection.cursor()
    try:
        cur.execute("""
            UPDATE prefacturas_eps_tespecial 
            SET codigo_cuv = %s, estado_rips = 'VALIDADO_MINSALUD'
            WHERE id = %s AND id_empresa = %s
        """, (codigo_cuv, lote_id, empresa_id))
        mysql.connection.commit()
        flash(f'CUV {codigo_cuv} registrado exitosamente para el Lote #{lote_id}.', 'success')
    except Exception as e:
        mysql.connection.rollback()
        flash(f'Error al registrar CUV: {str(e)}', 'danger')
    finally:
        cur.close()

    return redirect(url_for('procesofacturacion_eps.index_facturacion'))

# =========================================================
# 4. EXPORTAR PLANO CONTABLE (CSV/EXCEL)
# =========================================================
@bp_procesofacturacion_eps.route('/exportar_csv/<int:lote_id>', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
def exportar_csv(lote_id):
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("SELECT * FROM prefacturas_eps_tespecial WHERE id = %s AND id_empresa = %s", (lote_id, empresa_id))
    lote = cur.fetchone()
    
    if not lote:
        cur.close()
        flash('Lote no encontrado.', 'danger')
        return redirect(url_for('procesofacturacion_eps.index_facturacion'))

    cur.execute("""
        SELECT numero_prescripcion, numero_autorizacion, id_usuario, nombre_usuario, tipo_servicio, fecha_servicio
        FROM control_viajes_flota_especial 
        WHERE id_prefactura = %s AND trayecto = 'IDA'
    """, (lote_id,))
    viajes = cur.fetchall()
    cur.close()

    output = io.StringIO()
    writer = csv.writer(output, delimiter=';')
    
    writer.writerow(['NIT_CLIENTE', 'NUMERO_CONTRATO', 'CUV_MINSALUD', 'PRESCRIPCION', 'AUTORIZACION', 'ID_PACIENTE', 'NOMBRE_PACIENTE', 'CODIGO_SERVICIO', 'FECHA_SERVICIO', 'VALOR_TOTAL_LOTE'])
    
    for v in viajes:
        writer.writerow([
            lote['id_eps_cliente'],
            lote['numero_contrato'],
            lote.get('codigo_cuv', 'PENDIENTE'),
            v['numero_prescripcion'],
            v['numero_autorizacion'],
            v['id_usuario'],
            v['nombre_usuario'],
            v['tipo_servicio'],
            v['fecha_servicio'].strftime('%Y-%m-%d') if v['fecha_servicio'] else '',
            lote['valor_total']
        ])
    
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment;filename=PreFactura_Lote_{lote_id}.csv"}
    )

# =========================================================
# MIGRACIÓN FASE 1: AUDITORÍA DE VIAJES Y GENERACIÓN SHA-256
# =========================================================
@bp_procesofacturacion_eps.route('/auditoria', methods=['GET', 'POST'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
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
        return redirect(url_for('procesofacturacion_eps.gestion_traslados_auditoria'))
        
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("""
        SELECT SUBSTRING_INDEX(id_viaje, '_', 1) as raiz_viaje
        FROM control_viajes_flota_especial
        WHERE id_empresa = %s AND estatus_servicio IN ('TERMINADO-PDTE AUDITAR', 'AUDITADO') AND id_prefactura IS NULL
        GROUP BY raiz_viaje
        HAVING COUNT(id) = 2
           AND SUM(CASE WHEN estatus_servicio = 'TERMINADO-PDTE AUDITAR' THEN 1 ELSE 0 END) > 0
    """, (empresa_id,))
    raices_validas = [r['raiz_viaje'] for r in cur.fetchall()]
    
    viajes_agrupados = {}
    
    if raices_validas:
        format_strings = ','.join(['%s'] * len(raices_validas))
        cur.execute(f"""
            SELECT c.id, c.id_viaje_padre, c.id_viaje, c.numero_prescripcion, c.nombre_usuario, c.trayecto, 
                   c.conductor_asignado, c.vehiculo_asignado, c.fecha_fin_real, c.ruta_documento, c.estatus_servicio,
                   c.direccion_origen, c.municipio, c.departamento, c.lat_origen, c.lng_origen, c.lat_origen_esperado, c.lng_origen_esperado,
                   c.direccion_destino, c.municipio_destino, c.departamento_destino, c.lat_destino, c.lng_destino, c.lat_destino_esperado, c.lng_destino_esperado, c.firma,
                   c.hora_origen, c.hora_destino, c.tiempo_efectivo_minutos,
                   COALESCE(c.distancia_estimada_km, 0.00) as distancia_estimada_km,
                   COALESCE(c.distancia_real_km, 0.00) as distancia_real_km,
                   COALESCE(c.desviacion_origen_m, 0) as desviacion_origen_m,
                   COALESCE(c.desviacion_destino_m, 0) as desviacion_destino_m,
                   SUBSTRING_INDEX(c.id_viaje, '_', 1) as raiz_viaje,
                   (SELECT SUM(COALESCE(sub.distancia_estimada_km, 0.00)) FROM control_viajes_flota_especial sub WHERE sub.id_viaje_padre = c.id_viaje_padre AND sub.id_empresa = c.id_empresa) as consolidado_estimado_paquete,
                   (SELECT SUM(COALESCE(sub.distancia_real_km, 0.00)) FROM control_viajes_flota_especial sub WHERE sub.id_viaje_padre = c.id_viaje_padre AND sub.id_empresa = c.id_empresa) as consolidado_real_paquete
            FROM control_viajes_flota_especial c
            WHERE c.id_empresa = %s AND SUBSTRING_INDEX(c.id_viaje, '_', 1) IN ({format_strings})
            ORDER BY raiz_viaje, c.trayecto
        """, [empresa_id] + raices_validas)
        tramos = cur.fetchall()
        
        for t in tramos:
            p = t['raiz_viaje']
            if p not in viajes_agrupados:
                viajes_agrupados[p] = []
            viajes_agrupados[p].append(t)
            
    cur.close()
    
    return render_template('B_modulo_procesofacturacion_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_auditoria', viajes_agrupados=viajes_agrupados)

# =========================================================
# MIGRACIÓN FASE 1: HISTORIAL DE AUDITADOS
# =========================================================
@bp_procesofacturacion_eps.route('/auditados', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
@submodulo_required('eps_facturacion')
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
    
    return render_template('B_modulo_procesofacturacion_eps.html', nit=session.get('nit'), empresa=session.get('empresa'), nombre=session.get('nombre'), active_module='traslados_auditados', viajes=viajes)

def transmitir_rips_json_api(lote_id):
    pass

def emitir_factura_dian_api(lote_id, cuv_codigo):
    pass