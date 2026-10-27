# MÓDULO: WEBMASTER_ADMIN | ARCHIVO: bp_901811727_admin.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/bp_901811727_admin.py
import json
import time
from flask import Blueprint, request, jsonify, session
from app import mysql, csrf
from app.utils import login_required_custom
from flask_bcrypt import Bcrypt

bcrypt = Bcrypt()
bp_admin = Blueprint('bp_901811727_admin', __name__)

def registrar_operacion_bqa(cur, accion, datos):
    try:
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
        if hasattr(datos, 'to_dict'):
            datos_dict = datos.to_dict(flat=False)
            datos_dict = {k: v[0] if len(v) == 1 else v for k, v in datos_dict.items()}
            payload = json.dumps(datos_dict)
        elif isinstance(datos, dict):
            payload = json.dumps(datos)
        else:
            payload = json.dumps({"raw": str(datos)})
        
        emp = session.get('empresa', 'Sistema')
        emp_id = session.get('empresa_id', 'N/A')
        usr = session.get('nombre', 'N/A')
        ced = session.get('cedula', 'N/A')
        
        cur.execute("""
            INSERT INTO registro_operaciones_bqa_one 
            (empresa, empresa_id, backend_emisor, usuario, cedula, accion, datos) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (emp, emp_id, 'bp_901811727_admin.py', usr, ced, accion, payload))
    except Exception as e:
        print(f"Error auditoria bqa_one: {e}")

# ==============================================================================
# MATRIZ MAESTRA DE PRODUCTO (Regla Determinista del SaaS BQA-ONE)
# ==============================================================================
MATRIZ_MAESTRA = {
    'bodegas': {
        'nombre_comercial': 'Gestión de Bodegas y Despachos (WMS)',
        'perfiles': [
            {'perfil': 'controlador_logistica', 'operacion': 'bodegas', 'archivo': 'B_modulo_controlador_bodegas.html'},
            {'perfil': 'operador_logistica', 'operacion': 'bodegas', 'archivo': 'B_modulo_operador_bodegas.html'},
            {'perfil': 'verificador_bodegas', 'operacion': 'bodegas', 'archivo': 'B_verificador_bodegas.html'}
        ]
    },
    'flota': {
        'nombre_comercial': 'Gestión de Flota de Carga',
        'perfiles': [
            {'perfil': 'gestor_flotacarga', 'operacion': 'flota', 'archivo': 'B_modulo_controlador_flotacarga.html'},
            {'perfil': 'operador_flotacarga', 'operacion': 'flota', 'archivo': 'B_modulo_operador_flotacarga.html'}
        ]
    },
    'flotaespecial': {
        'nombre_comercial': 'Transporte Especial',
        'perfiles': [
            {'perfil': 'controlador_flotaespecial', 'operacion': 'flotaespecial', 'archivo': 'B_modulo_controlador_flotaespecial.html'},
            {'perfil': 'operador_flotaespecial', 'operacion': 'flotaespecial', 'archivo': 'B_modulo_operador_flotaespecial.html'}
        ]
    },
    'ventas': {
        'nombre_comercial': 'Ventas y Comercialización (M3)',
        'perfiles': [
            {'perfil': 'gerente_ventasdistribucion', 'operacion': 'ventas', 'archivo': 'B_gerente_ventasdistribucion.html'},
            {'perfil': 'controlador_ventas', 'operacion': 'ventas', 'archivo': 'B_controlador_ventas.html'},
            {'perfil': 'operador_ventas', 'operacion': 'ventas', 'archivo': 'B_modulo_operador_ventas.html'}
        ]
    },
    'facturacion': {
        'nombre_comercial': 'Facturación',
        'perfiles': [
            {'perfil': 'controlador_facturacion', 'operacion': 'facturacion', 'archivo': 'B_controlador_facturacion.html'},
            {'perfil': 'operador_facturacion', 'operacion': 'facturacion', 'archivo': 'B_modulo_operador_facturacion.html'}
        ]
    },
    'contabilidad': {
        'nombre_comercial': 'Contabilidad',
        'perfiles': [
            {'perfil': 'controlador_contabilidad', 'operacion': 'contabilidad', 'archivo': 'B_controlador_contabilidad.html'},
            {'perfil': 'operador_contable', 'operacion': 'contabilidad', 'archivo': 'B_modulo_operador_contable.html'}
        ]
    },
    'recursos_humanos': {
        'nombre_comercial': 'Recursos Humanos',
        'perfiles': [
            {'perfil': 'controlador_recurso_humano', 'operacion': 'recursos_humanos', 'archivo': 'B_controlador_recursohumano.html'},
            {'perfil': 'operador_recursohumano', 'operacion': 'recursos_humanos', 'archivo': 'B_modulo_operador_recursohumano.html'}
        ]
    },
    'gas': {
        'nombre_comercial': 'Gestión de Gas / GLP',
        'perfiles': [
            {'perfil': 'controlador_gas', 'operacion': 'gas', 'archivo': 'B_controladorgas.html'},
            {'perfil': 'operador_gas', 'operacion': 'gas', 'archivo': 'B_operador_glp.html'},
            {'perfil': 'controlfacturas_gas', 'operacion': 'gas', 'archivo': 'C_facturas_glp.html'}
        ]
    },
    'mermas': {
        'nombre_comercial': 'Control de Mermas',
        'perfiles': [
            {'perfil': 'controlador_mermas', 'operacion': 'mermas', 'archivo': 'controlador_mermas.html'},
            {'perfil': 'operador_mermas', 'operacion': 'mermas', 'archivo': 'operador_mermas.html'}
        ]
    },
    'carga': {
        'nombre_comercial': 'Gestión de Carga (Báscula)',
        'perfiles': [
            {'perfil': 'operador_bascula', 'operacion': 'carga', 'archivo': 'B_pesaje_carga_avicola.html'}
        ]
    },
    'comercializador_glp': {
        'nombre_comercial': 'Comercializador GLP',
        'perfiles': [
            {'perfil': 'controlador_comercializadorglp', 'operacion': 'comercializador_glp', 'archivo': 'equipos.html'},
            {'perfil': 'mecanico_glp', 'operacion': 'comercializador_glp', 'archivo': 'visitas_offline.html'}
        ]
    },
    'pesv': {
        'nombre_comercial': 'Auditorías PESV',
        'perfiles': [
            {'perfil': 'controlador_pesv', 'operacion': 'pesv', 'archivo': 'B_controlador_pesv.html'}
        ]
    },
    'preoperacional': {
        'nombre_comercial': 'Preoperacionales de Flota',
        'perfiles': []
    },
    'eps_gestion': {
        'nombre_comercial': 'Gestión Operativa EPS',
        'perfiles': []
    },
    'eps_facturacion': {
        'nombre_comercial': 'Pre-Facturación EPS',
        'perfiles': []
    },
    'avicola': {
        'nombre_comercial': 'Sector Avícola',
        'perfiles': []
    }
}

@bp_admin.route('/api/matriz_maestra', methods=['GET'])
@login_required_custom
def api_matriz_maestra():
    return jsonify(success=True, matriz=MATRIZ_MAESTRA)

# ==============================================================================
# RUTAS DE GESTIÓN (ADMINISTRACIÓN CRUD COMPLETA)
# ==============================================================================

@csrf.exempt
@bp_admin.route('/gestionar_empresa', methods=['POST'])
@login_required_custom
def gestionar_empresa():
    d = request.form
    modulos_seleccionados = request.form.getlist('modulos[]')
    if not modulos_seleccionados:
        modulos_seleccionados = request.form.getlist('modulos')
    
    nit = d.get('nit')
    nombre_comercial = d.get('nombre_comercial')
    tipo_empresa = d.get('tipo_empresa', 'general')
    accion = d.get('accion', 'crear')
    modulos_json = json.dumps(modulos_seleccionados)
    
    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if accion == 'crear':
                cur.execute("INSERT INTO empresas (nit, nombre_comercial, tipo_empresa, submodulos_activos) VALUES (%s, %s, %s, %s)", 
                           (nit, nombre_comercial, tipo_empresa, modulos_json))
            else:
                cur.execute("SELECT id FROM empresas WHERE nit=%s FOR UPDATE", (nit,))
                cur.execute("UPDATE empresas SET nombre_comercial=%s, tipo_empresa=%s, submodulos_activos=%s WHERE nit=%s", 
                           (nombre_comercial, tipo_empresa, modulos_json, nit))
                           
            # Reset y Asignación de Módulos (Tabla de permisos directos)
            cur.execute("DELETE FROM modulos_empresas_autorizadas WHERE id_empresa = %s", (nit,))
            for modulo in modulos_seleccionados:
                cur.execute("INSERT INTO modulos_empresas_autorizadas (id_empresa, modulo, estatus) VALUES (%s, %s, 'activo')", (nit, modulo))
                
                # Auto-Aprovisionamiento / Inyección determinista de Perfiles al Inquilino
                if modulo in MATRIZ_MAESTRA:
                    for p in MATRIZ_MAESTRA[modulo]['perfiles']:
                        # Verificar si el inquilino ya posee este perfil para no duplicarlo
                        cur.execute("SELECT id FROM perfiles WHERE nit=%s AND perfil=%s FOR UPDATE", (nit, p['perfil']))
                        if not cur.fetchone():
                            cur.execute("""
                                INSERT INTO perfiles (empresa, nit, operacion, perfil, archivo_destino) 
                                VALUES (%s, %s, %s, %s, %s)
                            """, (nombre_comercial, nit, p['operacion'], p['perfil'], p['archivo']))
                
            registrar_operacion_bqa(cur, 'GESTION_EMPRESA', d)
            mysql.connection.commit()
            return jsonify(success=True, message="Empresa, Módulos y Perfiles de Matriz procesados correctamente.")
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia. Intente nuevamente.")

# ---------------------------------------------------------------

@csrf.exempt
@bp_admin.route('/registrar_empresa', methods=['POST'])
@login_required_custom
def registrar_empresa():
    nombre_comercial = request.form.get('nombre_comercial', '').strip()
    nit = request.form.get('nit', '').strip()
    tipo_empresa = request.form.get('tipo_empresa', 'general').strip()
    accion = request.form.get('accion', 'crear').strip()
    
    if not nombre_comercial or not nit:
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    
    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if accion == 'crear':
                cur.execute("SELECT id FROM empresas WHERE nit = %s FOR UPDATE", (nit,))
                if cur.fetchone():
                    return jsonify({'success': False, 'message': 'La empresa ya existe.'})
                cur.execute("INSERT INTO empresas (nit, nombre_comercial, tipo_empresa) VALUES (%s, %s, %s)", (nit, nombre_comercial, tipo_empresa))
                msg = 'Empresa creada correctamente.'
            else:
                cur.execute("SELECT id FROM empresas WHERE nit = %s FOR UPDATE", (nit,))
                cur.execute("UPDATE empresas SET nombre_comercial=%s, tipo_empresa=%s WHERE nit=%s", (nombre_comercial, tipo_empresa, nit))
                msg = 'Empresa actualizada correctamente.'
                
            registrar_operacion_bqa(cur, 'REGISTRO_EMPRESA', request.form)
            mysql.connection.commit()
            return jsonify({'success': True, 'message': msg})
        except Exception as e:
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify({'success': False, 'message': f'Error: {str(e)}'})
        finally:
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/gestionar_tipo_empresa', methods=['POST'])
@login_required_custom
def gestionar_tipo_empresa():
    d = request.form
    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if d.get('accion') == 'crear':
                cur.execute("INSERT INTO tipos_empresa (tipo) VALUES (%s)", (d.get('tipo'),))
            else:
                cur.execute("SELECT id FROM tipos_empresa WHERE id=%s FOR UPDATE", (d.get('id'),))
                cur.execute("UPDATE tipos_empresa SET tipo=%s WHERE id=%s", (d.get('tipo'), d.get('id')))
                
            registrar_operacion_bqa(cur, 'GESTION_TIPO_EMPRESA', d)
            mysql.connection.commit()
            return jsonify(success=True, message="Tipo de empresa gestionado exitosamente.")
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/gestionar_usuario', methods=['GET', 'POST'])
@login_required_custom
def gestionar_usuario():
    d = request.form
    empresa_id = d.get('empresa_id')
    if not empresa_id:
        return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if d.get('accion') == 'crear':
                pw = bcrypt.generate_password_hash(d.get('password')).decode('utf-8')
                
                perfil_raw = d.get('perfil', '')
                id_perfil = None
                perfil_nombre = perfil_raw
                if '|' in perfil_raw:
                    parts = perfil_raw.split('|', 1)
                    if parts[0].isdigit():
                        id_perfil = int(parts[0])
                        perfil_nombre = parts[1]
                
                cur.execute("""INSERT INTO usuarios (cedula, nombre, password, perfil, id_perfil, empresa_id, empresa, telegram_id, telefono) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                           (d.get('cedula'), d.get('nombre'), pw, perfil_nombre, id_perfil, empresa_id, d.get('empresa_select'), None, d.get('telefono')))
                msg = "Usuario creado exitosamente con Perfil Inyectado."
                
            elif d.get('accion') == 'eliminar':
                cedula = d.get('cedula')
                cur.execute("SELECT id FROM usuarios WHERE cedula=%s AND empresa_id=%s FOR UPDATE", (cedula, empresa_id))
                cur.execute("DELETE FROM usuarios WHERE cedula=%s AND empresa_id=%s", (cedula, empresa_id))
                msg = "Usuario eliminado correctamente."
                
            else:
                cedula = d.get('cedula')
                nuevo_telefono = d.get('telefono')
                telegram_id_enviado = d.get('telegram_id')
                
                perfil_raw = d.get('perfil', '')
                id_perfil = None
                perfil_nombre = perfil_raw
                if '|' in perfil_raw:
                    parts = perfil_raw.split('|', 1)
                    if parts[0].isdigit():
                        id_perfil = int(parts[0])
                        perfil_nombre = parts[1]

                cur.execute("SELECT telefono FROM usuarios WHERE cedula=%s AND empresa_id=%s FOR UPDATE", (cedula, empresa_id))
                row = cur.fetchone()
                if not row:
                    mysql.connection.rollback()
                    return jsonify(success=False, message="Usuario no encontrado o no pertenece a la empresa actual.")
                    
                telefono_actual = row['telefono'] if isinstance(row, dict) else row[0]

                if str(telefono_actual) != str(nuevo_telefono):
                    telegram_id_enviado = None
                elif not telegram_id_enviado or telegram_id_enviado.strip() == "":
                    telegram_id_enviado = None

                query = "UPDATE usuarios SET nombre=%s, perfil=%s, id_perfil=%s, telegram_id=%s, telefono=%s, empresa=%s, tipo_usuario=%s, clase=%s"
                params = [d.get('nombre'), perfil_nombre, id_perfil, telegram_id_enviado, nuevo_telefono, d.get('empresa_select'), d.get('tipo_usuario'), d.get('clase')]
                
                if d.get('password'):
                    query += ", password=%s"
                    params.append(bcrypt.generate_password_hash(d.get('password')).decode('utf-8'))
                    
                query += " WHERE cedula=%s AND empresa_id=%s"
                params.extend([cedula, empresa_id])
                
                cur.execute(query, tuple(params))
                msg = "Usuario actualizado exitosamente."
                
            registrar_operacion_bqa(cur, 'GESTION_USUARIO', d)
            mysql.connection.commit()
            return jsonify(success=True, message=msg)
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/gestionar_perfil', methods=['POST'])
@login_required_custom
def gestionar_perfil():
    d = request.form
    nit = d.get('nit')
    if not nit:
        return jsonify(success=False, message="Falta NIT de empresa (Validación Multi-Tenant requerida).")

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if d.get('accion') == 'crear':
                cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil, archivo_destino) VALUES (%s, %s, %s, %s, %s)",
                           (d.get('empresa_select'), nit, d.get('operacion'), d.get('perfil'), d.get('archivo_destino')))
            else:
                cur.execute("SELECT id FROM perfiles WHERE id=%s AND nit=%s FOR UPDATE", (d.get('id'), nit))
                cur.execute("UPDATE perfiles SET operacion=%s, perfil=%s, archivo_destino=%s WHERE id=%s AND nit=%s", 
                           (d.get('operacion'), d.get('perfil'), d.get('archivo_destino'), d.get('id'), nit))
                           
            registrar_operacion_bqa(cur, 'GESTION_PERFIL', d)
            mysql.connection.commit()
            return jsonify(success=True, message="Perfil manual y ruteo procesado.")
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/obtener_perfiles')
@login_required_custom
def obtener_perfiles():
    empresa_id = request.args.get('empresa_id', '').strip()
    operacion = request.args.get('operacion', '').strip()
    if not empresa_id:
        return jsonify({'perfiles': []})
    import MySQLdb.cursors
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        if operacion:
            cur.execute("SELECT DISTINCT perfil FROM perfiles WHERE nit = %s AND operacion = %s", (empresa_id, operacion))
            perfiles = [row['perfil'] for row in cur.fetchall()]
        else:
            cur.execute("SELECT id, perfil, operacion FROM perfiles WHERE nit = %s ORDER BY operacion, perfil", (empresa_id,))
            perfiles = cur.fetchall()
        cur.close()
        return jsonify({'perfiles': perfiles})
    except Exception as e:
        return jsonify({'perfiles': []})

@csrf.exempt
@bp_admin.route('/gestionar_proveedor', methods=['POST'])
@login_required_custom
def gestionar_proveedor():
    d = request.form
    empresa_id = d.get('id_empresa') or session.get('empresa_id')
    if not empresa_id:
        return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if d.get('accion') == 'crear':
                cur.execute("""INSERT INTO proveedores (proveedor, id_proveedor, email1, email2, producto_servicio, precio, id_empresa) 
                               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                           (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), 'GLP', d.get('precio'), empresa_id))
            else:
                cur.execute("SELECT id_proveedor FROM proveedores WHERE id_proveedor=%s AND id_empresa=%s FOR UPDATE", (d.get('id_proveedor'), empresa_id))
                cur.execute("""UPDATE proveedores SET proveedor=%s, email1=%s, email2=%s, precio=%s WHERE id_proveedor=%s AND id_empresa=%s""",
                           (d.get('proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor'), empresa_id))
                           
            registrar_operacion_bqa(cur, 'GESTION_PROVEEDOR', d)
            mysql.connection.commit()
            return jsonify(success=True, message="Proveedor actualizado.")
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/registrar_proveedor', methods=['POST'])
@login_required_custom
def registrar_proveedor():
    d = request.form
    accion = d.get('accion', 'crear')
    empresa_id = d.get('id_empresa') or session.get('empresa_id')
    if not empresa_id:
        return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if accion == 'crear':
                cur.execute("""
                    INSERT INTO proveedores (proveedor, id_proveedor, email1, email2, producto_servicio, precio, id_empresa)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), 'GLP', d.get('precio'), empresa_id))
                msg = "Proveedor creado."
            else:
                cur.execute("SELECT id_proveedor FROM proveedores WHERE id_proveedor=%s AND id_empresa=%s FOR UPDATE", (d.get('id_proveedor'), empresa_id))
                cur.execute("""
                    UPDATE proveedores 
                    SET proveedor=%s, email1=%s, email2=%s, precio=%s 
                    WHERE id_proveedor=%s AND id_empresa=%s
                """, (d.get('proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor'), empresa_id))
                msg = "Proveedor actualizado."
                
            registrar_operacion_bqa(cur, 'REGISTRO_PROVEEDOR', d)
            mysql.connection.commit()
            return jsonify(success=True, message=msg)
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/consultar_proveedores', methods=['POST'])
@login_required_custom
def consultar_proveedores():
    empresa_id = request.get_json().get('empresa_id')
    if not empresa_id:
        return jsonify({"success": False, "message": "Empresa no especificada"})
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT proveedor, id_proveedor, email1, email2 FROM proveedores WHERE id_empresa = %s", (empresa_id,))
        data = [{"proveedor": r[0], "id_proveedor": r[1], "email1": r[2], "email2": r[3]} for r in cur.fetchall()]
        return jsonify({"success": True, "proveedores": data})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})
    finally:
        cur.close()

@csrf.exempt
@bp_admin.route('/gestionar_contacto', methods=['POST'])
@login_required_custom
def gestionar_contacto():
    d = request.form
    empresa_id = d.get('id_empresa') or session.get('empresa_id')
    if not empresa_id:
        return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if d.get('accion') == 'crear':
                cur.execute("INSERT INTO contactos (empresa, id_empresa, area_contacto, email) VALUES (%s, %s, %s, %s)",
                           (d.get('empresa_nombre'), empresa_id, d.get('area_contacto'), d.get('email')))
            else:
                cur.execute("SELECT id FROM contactos WHERE id=%s AND id_empresa=%s FOR UPDATE", (d.get('id'), empresa_id))
                cur.execute("UPDATE contactos SET area_contacto=%s, email=%s WHERE id=%s AND id_empresa=%s", 
                           (d.get('area_contacto'), d.get('email'), d.get('id'), empresa_id))
                
            registrar_operacion_bqa(cur, 'GESTION_CONTACTO', d)
            mysql.connection.commit()
            return jsonify(success=True, message="Contacto gestionado.")
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@csrf.exempt
@bp_admin.route('/registrar_contacto', methods=['POST'])
@login_required_custom
def registrar_contacto():
    d = request.form
    accion = d.get('accion', 'crear')
    empresa_id = d.get('id_empresa') or session.get('empresa_id')
    if not empresa_id:
        return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if accion == 'crear':
                cur.execute("""
                    INSERT INTO contactos (empresa, id_empresa, area_contacto, email)
                    VALUES (%s, %s, %s, %s)
                """, (d.get('empresa_nombre'), empresa_id, d.get('area_contacto'), d.get('email')))
                msg = "Contacto creado."
            else:
                cur.execute("SELECT id FROM contactos WHERE id=%s AND id_empresa=%s FOR UPDATE", (d.get('id'), empresa_id))
                cur.execute("""
                    UPDATE contactos SET area_contacto=%s, email=%s 
                    WHERE id=%s AND id_empresa=%s
                """, (d.get('area_contacto'), d.get('email'), d.get('id'), empresa_id))
                msg = "Contacto actualizado."
                
            registrar_operacion_bqa(cur, 'REGISTRO_CONTACTO', d)
            mysql.connection.commit()
            return jsonify(success=True, message=msg)
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

# ==============================================================================
# NUEVO: GESTIÓN MAESTRA DE SEDES (GRANJAS, BODEGAS, OFICINAS, PLANTAS)
# ==============================================================================

@csrf.exempt
@bp_admin.route('/gestionar_sede', methods=['POST'])
@login_required_custom
def gestionar_sede():
    d = request.form
    empresa_id = d.get('id_empresa') or session.get('empresa_id')
    if not empresa_id:
        return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

    # Extraer campos JSON
    fuentes_energia = request.form.getlist('fuentes_energia[]')
    if not fuentes_energia:
        fuentes_energia = request.form.getlist('fuentes_energia')
        
    tecnologia_alternativa = request.form.getlist('tecnologia_alternativa[]')
    if not tecnologia_alternativa:
        tecnologia_alternativa = request.form.getlist('tecnologia_alternativa')
        
    kpi_eficiencia = d.get('kpi_eficiencia', '{}')

    for intento in range(3):
        cur = mysql.connection.cursor()
        try:
            if d.get('accion') == 'crear':
                cur.execute("""
                    INSERT INTO sedes_empresas 
                    (empresa_id, empresa_nombre, nombre_sede, tipo_sede, clima, tecnologia_produccion, unidad_produccion, fuentes_energia, tecnologia_alternativa, kpi_eficiencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    empresa_id, 
                    d.get('empresa_nombre', ''), 
                    d.get('nombre_sede'), 
                    d.get('tipo_sede'), 
                    d.get('clima'), 
                    d.get('tecnologia_produccion'), 
                    d.get('unidad_produccion'), 
                    json.dumps(fuentes_energia), 
                    json.dumps(tecnologia_alternativa), 
                    kpi_eficiencia
                ))
                msg = "Sede creada exitosamente."
            elif d.get('accion') == 'eliminar':
                cur.execute("SELECT id FROM sedes_empresas WHERE id=%s AND empresa_id=%s FOR UPDATE", (d.get('id'), empresa_id))
                cur.execute("DELETE FROM sedes_empresas WHERE id=%s AND empresa_id=%s", (d.get('id'), empresa_id))
                msg = "Sede eliminada correctamente."
            else:
                cur.execute("SELECT id FROM sedes_empresas WHERE id=%s AND empresa_id=%s FOR UPDATE", (d.get('id'), empresa_id))
                cur.execute("""
                    UPDATE sedes_empresas 
                    SET nombre_sede=%s, tipo_sede=%s, clima=%s, tecnologia_produccion=%s, unidad_produccion=%s, fuentes_energia=%s, tecnologia_alternativa=%s, kpi_eficiencia=%s
                    WHERE id=%s AND empresa_id=%s
                """, (
                    d.get('nombre_sede'), 
                    d.get('tipo_sede'), 
                    d.get('clima'), 
                    d.get('tecnologia_produccion'), 
                    d.get('unidad_produccion'), 
                    json.dumps(fuentes_energia), 
                    json.dumps(tecnologia_alternativa), 
                    kpi_eficiencia, 
                    d.get('id'), 
                    empresa_id
                ))
                msg = "Sede actualizada exitosamente."
                
            registrar_operacion_bqa(cur, 'GESTION_SEDE', d)
            mysql.connection.commit()
            return jsonify(success=True, message=msg)
        except Exception as e: 
            mysql.connection.rollback()
            if ('1213' in str(e) or 'Deadlock' in str(e)) and intento < 2:
                time.sleep(0.5)
                continue
            return jsonify(success=False, message=str(e))
        finally: 
            cur.close()
    return jsonify(success=False, message="Error de concurrencia.")

@bp_admin.route('/obtener_todas_sedes')
@login_required_custom
def obtener_todas_sedes():
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT id, empresa_id, empresa_nombre, nombre_sede, tipo_sede, clima, 
               tecnologia_produccion, unidad_produccion, fuentes_energia, 
               tecnologia_alternativa, kpi_eficiencia 
        FROM sedes_empresas
    """)
    rows = cur.fetchall()
    cols = ['id', 'empresa_id', 'empresa_nombre', 'nombre_sede', 'tipo_sede', 'clima', 
            'tecnologia_produccion', 'unidad_produccion', 'fuentes_energia', 
            'tecnologia_alternativa', 'kpi_eficiencia']
    res = [dict(zip(cols, r)) if not isinstance(r, dict) else r for r in rows]
    cur.close()
    return jsonify(success=True, sedes=res)

# ==============================================================================
# RUTAS DE LECTURA (PARA LLENAR LAS TABLAS DEL FRONTEND)
# ==============================================================================

@bp_admin.route('/obtener_todos_tipos_empresa')
@login_required_custom
def obtener_todos_tipos_empresa():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, tipo FROM tipos_empresa")
    rows = cur.fetchall()
    res = [dict(zip(['id','tipo'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, tipos=res)

@bp_admin.route('/obtener_todos_usuarios')
@login_required_custom
def obtener_todos_usuarios():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, cedula, nombre, perfil, id_perfil, empresa, empresa_id, telegram_id, telefono FROM usuarios")
    rows = cur.fetchall()
    cols = ['id','cedula','nombre','perfil','id_perfil','empresa','empresa_id','telegram_id','telefono']
    res = [dict(zip(cols, r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, users=res)

@bp_admin.route('/obtener_todos_proveedores')
@login_required_custom
def obtener_todos_proveedores():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id_proveedor, proveedor, email1, email2, precio FROM proveedores")
    rows = cur.fetchall()
    res = [dict(zip(['id_proveedor','proveedor','email1','email2','precio'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, providers=res)

@bp_admin.route('/obtener_todos_contactos')
@login_required_custom
def obtener_todos_contactos():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, empresa, id_empresa, area_contacto, email FROM contactos")
    rows = cur.fetchall()
    res = [dict(zip(['id','empresa','id_empresa','area_contacto','email'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close(); return jsonify(success=True, contacts=res)

@bp_admin.route('/obtener_todos_perfiles')
@login_required_custom
def obtener_todos_perfiles():
    cur = mysql.connection.cursor()
    cur.execute("SELECT id, empresa, nit, operacion, perfil, archivo_destino FROM perfiles")
    rows = cur.fetchall()
    res = [dict(zip(['id','empresa','nit','operacion','perfil','archivo_destino'], r)) if not isinstance(r, dict) else r for r in rows]
    cur.close()
    return jsonify(success=True, profiles=res)

@csrf.exempt
@bp_admin.route('/obtener_periodo', methods=['POST'])
@login_required_custom
def obtener_periodo():
    p = request.form.get('periodo')
    return jsonify({'success': True, 'periodo': p, 'fecha_inicio': request.form.get('fecha_inicio'), 'fecha_fin': request.form.get('fecha_fin')})

@csrf.exempt
@bp_admin.route('/obtener_modulos_empresa', methods=['POST'])
@login_required_custom
def obtener_modulos_empresa():
    nit = request.form.get('nit')
    if not nit:
        return jsonify(success=False, modulos=[], submodulos_activos=[])
        
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT modulo FROM modulos_empresas_autorizadas WHERE id_empresa = %s AND estatus = 'activo'", (nit,))
        modulos = [row[0] if not isinstance(row, dict) else row['modulo'] for row in cur.fetchall()]
        
        cur.execute("SELECT submodulos_activos FROM empresas WHERE nit = %s", (nit,))
        row_emp = cur.fetchone()
        submodulos_activos = []
        if row_emp:
            raw_sub = row_emp[0] if not isinstance(row_emp, dict) else row_emp.get('submodulos_activos')
            if raw_sub:
                try:
                    submodulos_activos = json.loads(raw_sub) if isinstance(raw_sub, str) else raw_sub
                except:
                    submodulos_activos = []
                    
        return jsonify(success=True, modulos=modulos, submodulos_activos=submodulos_activos)
    except Exception as e:
        return jsonify(success=False, message=str(e))
    finally:
        cur.close()

@bp_admin.route('/api/obtener_rutas_modulo', methods=['GET'])
@login_required_custom
def api_obtener_rutas_modulo():
    modulo = request.args.get('modulo', '').strip().lower()
    rutas = set()
    try:
        from flask import current_app
        import os
        
        tpl_dir = os.path.join(current_app.root_path, 'templates')
        if os.path.exists(tpl_dir):
            for f in os.listdir(tpl_dir):
                if f.endswith('.html'):
                    if modulo and modulo in f.lower():
                        rutas.add(f)
                        continue
                        
                    ruta_completa = os.path.join(tpl_dir, f)
                    try:
                        with open(ruta_completa, 'r', encoding='utf-8') as archivo:
                            for _ in range(5):
                                linea = archivo.readline()
                                if not linea:
                                    break
                                if 'MÓDULO:' in linea.upper() or 'SUBMÓDULO:' in linea.upper():
                                    if modulo and modulo in linea.lower():
                                        rutas.add(f)
                                        break
                    except Exception:
                        pass
                        
        rutas.add('panel_principal.html')
        rutas.add('A_dashboard_universal.html')
        
    except Exception as e:
        print(f"Error escaneando rutas de introspección: {e}")
        
    return jsonify(success=True, rutas=sorted(list(rutas)))