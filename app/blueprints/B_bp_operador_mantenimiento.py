# MÓDULO: MANTENIMIENTO | SUBMÓDULO: OPERADOR MANTENIMIENTO | CONDICIÓN: OBLIGATORIO | TIPO: 1

from flask import Blueprint, render_template, session, request, jsonify, current_app
from app import mysql
import MySQLdb.cursors
import time
import json
import os
import base64
import uuid
from app.utils import login_required_custom

bp_operador_mantenimiento = Blueprint('operador_mantenimiento', __name__)

# ==============================================================================
# VISTA PRINCIPAL (OPERADOR EN CAMPO)
# ==============================================================================
@bp_operador_mantenimiento.route('/B_operador_mantenimiento.html')
@login_required_custom
def vista_operador():
    """
    Renderiza la PWA Offline-First para el registro de inspecciones.
    """
    usuario = session.get('nombre', 'Operador')
    empresa = session.get('empresa', 'Empresa')
    nit = str(session.get('empresa_id', ''))

    return render_template('B_operador_mantenimiento.html',
                           usuario=usuario,
                           empresa=empresa,
                           nit=nit)


# ==============================================================================
# HELPER: GUARDADO FÍSICO DE IMÁGENES
# ==============================================================================
def guardar_imagen_fisica(b64_string, subcarpeta):
    """
    Decodifica una cadena Base64 y la guarda como archivo físico en el servidor.
    Retorna la ruta relativa para ser almacenada en la base de datos (VARCHAR).
    """
    if not b64_string or ',' not in b64_string:
        return None
    try:
        header, encoded = b64_string.split(',', 1)
        ext = "png" if "png" in header else "jpg"
        filename = f"{uuid.uuid4().hex}.{ext}"
        
        upload_folder = os.path.join(current_app.static_folder, 'uploads', 'mantenimiento', subcarpeta)
        os.makedirs(upload_folder, exist_ok=True)
        
        filepath = os.path.join(upload_folder, filename)
        with open(filepath, "wb") as fh:
            fh.write(base64.b64decode(encoded))
            
        return f"/static/uploads/mantenimiento/{subcarpeta}/{filename}"
    except Exception as e:
        print(f"Error guardando imagen física: {e}")
        return None

# ==============================================================================
# API: SINCRONIZACIÓN DE INSPECCIONES (OFFLINE-FIRST)
# ==============================================================================
@bp_operador_mantenimiento.route('/api/mantenimiento/sincronizar_inspeccion', methods=['POST'])
@login_required_custom
def sincronizar_inspeccion():
    """
    Recibe el payload JSON desde la PWA (IndexedDB) y lo consolida en la BD.
    Convierte Base64 a rutas físicas y añade georreferenciación.
    """
    uid = str(session.get('usuario_id'))
    empresa_id = str(session.get('empresa_id'))
    
    data = request.json
    if not data:
        return jsonify({'error': 'Payload vacío'}), 400

    id_local = data.get('id_local')
    cliente = data.get('cliente')
    ubicacion = data.get('ubicacion')
    fecha_inspeccion = data.get('fecha')
    encargado_nombre = data.get('encargado_nombre')
    latitud = data.get('latitud')
    longitud = data.get('longitud')
    
    # Procesamiento y liberación de carga (Base64 -> Archivo Físico)
    firma_ruta = guardar_imagen_fisica(data.get('firma_base64'), 'firmas')
    firma_oper_ruta = guardar_imagen_fisica(data.get('firma_operador_base64'), 'firmas')
    foto_encargado_ruta = guardar_imagen_fisica(data.get('foto_encargado_base64'), 'actas')
    
    items = data.get('items', [])

    if not id_local or not cliente:
        return jsonify({'error': 'Faltan datos obligatorios (id_local, cliente)'}), 400

    for intento in range(3):
        try:
            cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)

            # 1. Crear tablas optimizadas (Garantiza portabilidad)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mantenimiento_inspecciones (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    id_empresa VARCHAR(50) NOT NULL,
                    usuario_id VARCHAR(50) NOT NULL,
                    id_local VARCHAR(100) NOT NULL,
                    cliente VARCHAR(255),
                    ubicacion VARCHAR(255),
                    fecha_inspeccion DATETIME,
                    encargado_nombre VARCHAR(255),
                    firma_base64 VARCHAR(255),
                    firma_operador_base64 VARCHAR(255),
                    foto_encargado_base64 VARCHAR(255),
                    latitud DECIMAL(10,8),
                    longitud DECIMAL(11,8),
                    fecha_sincronizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_local_empresa (id_local, id_empresa)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS mantenimiento_inspeccion_items (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    id_inspeccion INT NOT NULL,
                    id_empresa VARCHAR(50) NOT NULL,
                    nombre VARCHAR(255),
                    hallazgo VARCHAR(100),
                    observaciones TEXT,
                    foto_base64 VARCHAR(255),
                    FOREIGN KEY (id_inspeccion) REFERENCES mantenimiento_inspecciones(id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # Auto-migración en caso de que las tablas ya existieran con esquema viejo
            try: cur.execute("ALTER TABLE mantenimiento_inspecciones ADD COLUMN firma_operador_base64 VARCHAR(255)")
            except: pass
            try: cur.execute("ALTER TABLE mantenimiento_inspecciones ADD COLUMN latitud DECIMAL(10,8)")
            except: pass
            try: cur.execute("ALTER TABLE mantenimiento_inspecciones ADD COLUMN longitud DECIMAL(11,8)")
            except: pass
            try: cur.execute("ALTER TABLE mantenimiento_inspeccion_items ADD COLUMN observaciones TEXT")
            except: pass

            # 2. Control de Idempotencia (Bloqueo Pesimista)
            cur.execute("""
                SELECT id FROM mantenimiento_inspecciones 
                WHERE id_local = %s AND id_empresa = %s FOR UPDATE
            """, (id_local, empresa_id))
            
            registro_existente = cur.fetchone()

            if registro_existente:
                cur.close()
                return jsonify({'status': 'ok', 'message': 'Inspección ya estaba sincronizada.'})

            # 3. Inserción de la Cabecera
            cur.execute("""
                INSERT INTO mantenimiento_inspecciones 
                (id_empresa, usuario_id, id_local, cliente, ubicacion, fecha_inspeccion, 
                 encargado_nombre, firma_base64, firma_operador_base64, foto_encargado_base64, latitud, longitud)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                empresa_id, uid, id_local, cliente, ubicacion, 
                fecha_inspeccion.replace('T', ' ').split('.')[0] if fecha_inspeccion else None, 
                encargado_nombre, firma_ruta, firma_oper_ruta, foto_encargado_ruta, latitud, longitud
            ))
            
            inspeccion_id = cur.lastrowid

            # 4. Inserción de los Ítems (Detalle)
            if items:
                for idx, item in enumerate(items):
                    foto_item_ruta = guardar_imagen_fisica(item.get('foto_base64'), 'items')
                    cur.execute("""
                        INSERT INTO mantenimiento_inspeccion_items 
                        (id_inspeccion, id_empresa, nombre, hallazgo, observaciones, foto_base64)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        inspeccion_id, 
                        empresa_id, 
                        item.get('nombre'), 
                        item.get('hallazgo'), 
                        item.get('observaciones'),
                        foto_item_ruta
                    ))

            # 5. REGISTRO DE AUDITORÍA BQA-ONE (Obligatorio)
            audit_payload = {
                'id_local': id_local,
                'cliente': cliente,
                'ubicacion': ubicacion,
                'encargado_nombre': encargado_nombre,
                'total_items': len(items),
                'latitud': latitud,
                'longitud': longitud
            }
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS registro_operaciones_bqa_one (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                    empresa VARCHAR(150),
                    empresa_id VARCHAR(50),
                    backend_emisor VARCHAR(100),
                    usuario VARCHAR(100),
                    cedula VARCHAR(50),
                    accion VARCHAR(100),
                    datos JSON
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            
            cur.execute("""
                INSERT INTO registro_operaciones_bqa_one 
                (empresa, empresa_id, backend_emisor, usuario, cedula, accion, datos) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                session.get('empresa', 'Sistema'), 
                empresa_id, 
                'B_bp_operador_mantenimiento.py', 
                session.get('nombre', 'N/A'), 
                session.get('cedula', 'N/A'), 
                'SINCRONIZAR_INSPECCION_MANTENIMIENTO', 
                json.dumps(audit_payload)
            ))

            mysql.connection.commit()
            cur.close()
            
            return jsonify({'status': 'ok', 'message': 'Sincronización exitosa.'})

        except Exception as e:
            mysql.connection.rollback()
            if "deadlock" in str(e).lower() and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify({'error': str(e)}), 500