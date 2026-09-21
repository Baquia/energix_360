# app/blueprints/B_bp_procesofacturacion_eps_transporteespecial.py
import os
import json
import zipfile
import io
import csv
from datetime import datetime
from flask import Blueprint, render_template, session, redirect, url_for, request, flash, send_file, Response
from app import mysql
from app.utils import login_required_custom
from functools import wraps
import MySQLdb.cursors

bp_procesofacturacion_eps = Blueprint('procesofacturacion_eps', __name__, url_prefix='/gestor_flotaespecial/facturacion_eps')

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
# VISTA PRINCIPAL DEL MÓDULO
# =========================================================
@bp_procesofacturacion_eps.route('/', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
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
        prefacturas=prefacturas,
        pendientes=pendientes
    )

# =========================================================
# 1. CONSOLIDAR LOTE DE PRE-FACTURACIÓN
# =========================================================
@bp_procesofacturacion_eps.route('/consolidar_lote', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
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
# 2. GENERACIÓN RIPS JSON (Res. 2275 de 2023)
# =========================================================
@bp_procesofacturacion_eps.route('/descargar_rips/<int:lote_id>', methods=['GET'])
@login_required_custom
@controlador_flotaespecial_required
def descargar_rips(lote_id):
    empresa_id = session.get('empresa_id')
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("SELECT * FROM control_viajes_flota_especial WHERE id_empresa = %s AND id_prefactura = %s", (empresa_id, lote_id))
    viajes = cur.fetchall()
    cur.close()

    if not viajes:
        flash('No hay viajes asociados a este lote.', 'danger')
        return redirect(url_for('procesofacturacion_eps.index_facturacion'))

    # Estructura base RIPS JSON (Aislado completamente del SHA-256 interno)
    # Solo datos normativos de MinSalud
    usuarios_us = []
    servicios_os = []
    
    visitados = set()
    for v in viajes:
        # Agrupar por paciente único para el array 'usuarios' (US)
        if v['id_usuario'] not in visitados:
            usuarios_us.append({
                "tipoDocumentoIdentificacion": v.get('tipo_documento', 'CC'),
                "numDocumentoIdentificacion": v['id_usuario'],
                "tipoUsuario": "01", # Ajustar según tabla real
                "fechaNacimiento": "1990-01-01", # Ejemplo: se debe cruzar con maestra
                "codSexo": "M",
                "codPaisResidencia": "170",
                "codMunicipioResidencia": "001",
                "codZonaTerritorialResidencia": "01"
            })
            visitados.add(v['id_usuario'])
            
        # Generar cobro único por servicio integral (Agrupando IDA/VUELTA)
        # Aquí se asume que si el trayecto es IDA, registra el servicio para no duplicarlo con la VUELTA
        if v['trayecto'] == 'IDA':
            servicios_os.append({
                "numAutorizacion": v.get('numero_autorizacion', ''),
                "numPrescripcion": v.get('numero_prescripcion', ''),
                "tipoDocumentoIdentificacion": v.get('tipo_documento', 'CC'),
                "numDocumentoIdentificacion": v['id_usuario'],
                "codPrestador": "000000000001", # Reemplazar con código de habilitación real
                "fechaRegistro": v['fecha_servicio'].strftime('%Y-%m-%d %H:%M') if v['fecha_servicio'] else "",
                "codProcedimiento": v.get('tipo_servicio', ''),
                "valorProcedimiento": 0.00 # Traer del tarifario
            })

    rips_payload = {
        "usuarios": usuarios_us,
        "servicios": servicios_os
    }

    json_str = json.dumps(rips_payload, indent=4)
    
    memory_file = io.BytesIO()
    with zipfile.ZipFile(memory_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"RIPS_LOTE_{lote_id}.json", json_str)
    memory_file.seek(0)

    return send_file(
        memory_file,
        mimetype='application/zip',
        as_attachment=True,
        download_name=f'RIPS_LOTE_{lote_id}.zip'
    )

# =========================================================
# 3. REGISTRAR CUV (Código Único de Validación)
# =========================================================
@bp_procesofacturacion_eps.route('/registrar_cuv', methods=['POST'])
@login_required_custom
@controlador_flotaespecial_required
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
    
    # Encabezados compatibles con ERPs estándar (Ej. Siigo, Alegra)
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
# 5. STUBS PARA FUTURAS INTEGRACIONES API (FASE 2)
# =========================================================
def transmitir_rips_json_api(lote_id):
    """
    STUB: Función preparada para consumir directamente el Web Service 
    de SISPRO / MinSalud mediante HTTP POST y capturar el CUV automáticamente.
    """
    pass

def emitir_factura_dian_api(lote_id, cuv_codigo):
    """
    STUB: Función preparada para generar el XML UBL 2.1 (Anexo Salud)
    y transmitirlo vía API a un Proveedor Tecnológico para obtener el CUFE.
    """
    pass