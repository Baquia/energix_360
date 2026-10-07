# MÓDULO: WEBMASTER_ADMIN | ARCHIVO: bp_901811727_admin.py
# app/blueprints/bp_901811727_admin.py
import json
from flask import Blueprint, request, jsonify, session
from app import mysql, csrf
from app.utils import login_required_custom
from flask_bcrypt import Bcrypt

bcrypt = Bcrypt()
bp_admin = Blueprint('bp_901811727_admin', __name__)

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
    
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO empresas (nit, nombre_comercial, tipo_empresa) VALUES (%s, %s, %s)", 
                       (nit, d.get('nombre_comercial'), d.get('tipo_empresa')))
        else:
            cur.execute("UPDATE empresas SET nombre_comercial=%s, tipo_empresa=%s WHERE nit=%s", 
                       (d.get('nombre_comercial'), d.get('tipo_empresa'), nit))
                       
        cur.execute("DELETE FROM modulos_empresas_autorizadas WHERE id_empresa = %s", (nit,))
        for modulo in modulos_seleccionados:
            cur.execute("INSERT INTO modulos_empresas_autorizadas (id_empresa, modulo, estatus) VALUES (%s, %s, 'activo')", (nit, modulo))
            
        mysql.connection.commit()
        return jsonify(success=True, message="Empresa y módulos procesados correctamente.")
    except Exception as e: 
        mysql.connection.rollback()
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/gestionar_empresa_transporte_especial', methods=['POST'])
@login_required_custom
def gestionar_empresa_transporte_especial():
    d = request.form
    submodulos_seleccionados = request.form.getlist('submodulos[]')
    if not submodulos_seleccionados:
        submodulos_seleccionados = request.form.getlist('submodulos')
        
    nit = d.get('nit')
    nombre_comercial = d.get('nombre_comercial')
    accion = d.get('accion', 'crear')
    
    cur = mysql.connection.cursor()
    try:
        submodulos_json = json.dumps(submodulos_seleccionados, ensure_ascii=False)
        
        if accion == 'crear':
            cur.execute("""
                INSERT INTO empresas (nit, nombre_comercial, tipo_empresa, submodulos_activos) 
                VALUES (%s, %s, 'transporte_especial', %s)
            """, (nit, nombre_comercial, submodulos_json))
        else:
            cur.execute("""
                UPDATE empresas 
                SET nombre_comercial=%s, tipo_empresa='transporte_especial', submodulos_activos=%s 
                WHERE nit=%s
            """, (nombre_comercial, submodulos_json, nit))
                       
        # Sincronización en modulos_empresas_autorizadas
        cur.execute("DELETE FROM modulos_empresas_autorizadas WHERE id_empresa = %s", (nit,))
        
        # Módulo base fijo
        cur.execute("INSERT INTO modulos_empresas_autorizadas (id_empresa, modulo, estatus) VALUES (%s, 'flotaespecial', 'activo')", (nit,))
        
        # Submódulos seleccionados
        for submod in submodulos_seleccionados:
            cur.execute("INSERT INTO modulos_empresas_autorizadas (id_empresa, modulo, estatus) VALUES (%s, %s, 'activo')", (nit, submod))
            
        mysql.connection.commit()
        return jsonify(success=True, message="Empresa de Transporte Especial y submódulos procesados correctamente.")
    except Exception as e: 
        mysql.connection.rollback()
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@bp_admin.route('/obtener_empresas_transporte_especial')
@login_required_custom
def obtener_empresas_transporte_especial():
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT id, nit, nombre_comercial, tipo_empresa, submodulos_activos FROM empresas WHERE tipo_empresa = 'transporte_especial'")
        rows = cur.fetchall()
        empresas = []
        cols = ['id', 'nit', 'nombre_comercial', 'tipo_empresa', 'submodulos_activos']
        for r in rows:
            row_dict = dict(zip(cols, r)) if not isinstance(r, dict) else r
            sub_raw = row_dict.get('submodulos_activos')
            if sub_raw:
                try:
                    row_dict['submodulos_activos'] = json.loads(sub_raw) if isinstance(sub_raw, str) else sub_raw
                except:
                    row_dict['submodulos_activos'] = []
            else:
                row_dict['submodulos_activos'] = []
            empresas.append(row_dict)
        return jsonify(success=True, empresas=empresas)
    except Exception as e:
        return jsonify(success=False, message=str(e))
    finally:
        cur.close()

@bp_admin.route('/api/submodulos_transporte_especial', methods=['GET'])
@login_required_custom
def api_submodulos_transporte_especial():
    submodulos_directos = [
        {"clave": "controlador_flota", "nombre": "Submódulo B: Controladores de Flota", "desc": "Acceso al panel administrativo, monitoreo satelital, vehículos y QRs."},
        {"clave": "operador_flota", "nombre": "Submódulo B: Operadores en Ruta", "desc": "Acceso a la PWA móvil para conductores (Pre-logueo, inicio/fin de viaje)."},
        {"clave": "eps_gestion", "nombre": "Submódulo B: Gestión Operativa EPS", "desc": "Flujo de captura, desglose, programación y kanban de pacientes."},
        {"clave": "eps_facturacion", "nombre": "Sub-submódulo C: Pre-Facturación EPS", "desc": "Generación de RIPS JSON y CUV (Depende de Gestión Operativa)."},
        {"clave": "preoperacional", "nombre": "Sub-submódulo C: Preoperacionales", "desc": "Auditoría diaria PESV y registro de novedades de vehículos."}
    ]
    modulos_universales = [
        {"clave": "mantenimiento", "nombre": "Mantenimiento Preventivo (Proyectado)", "desc": "Módulo Universal de Mantenimiento"},
        {"clave": "pesv", "nombre": "PESV - Seguridad Vial Institucional (Proyectado)", "desc": "Plan Estratégico de Seguridad Vial corporativo"},
        {"clave": "contabilidad", "nombre": "Contabilidad (Proyectado)", "desc": "Módulo Contable General"},
        {"clave": "combustible", "nombre": "Control de Combustibles", "desc": "Gestión y Control de Combustibles"}
    ]
    return jsonify(success=True, directos=submodulos_directos, universales=modulos_universales)

# --- NUEVOS CONTROLADORES: TRANSPORTE DE CARGA ---

@csrf.exempt
@bp_admin.route('/gestionar_empresa_transporte_carga', methods=['POST'])
@login_required_custom
def gestionar_empresa_transporte_carga():
    d = request.form
    submodulos_seleccionados = request.form.getlist('submodulos[]')
    if not submodulos_seleccionados:
        submodulos_seleccionados = request.form.getlist('submodulos')
        
    nit = d.get('nit')
    nombre_comercial = d.get('nombre_comercial')
    accion = d.get('accion', 'crear')
    
    cur = mysql.connection.cursor()
    try:
        submodulos_json = json.dumps(submodulos_seleccionados, ensure_ascii=False)
        
        if accion == 'crear':
            cur.execute("""
                INSERT INTO empresas (nit, nombre_comercial, tipo_empresa, submodulos_activos) 
                VALUES (%s, %s, 'transporte_carga', %s)
            """, (nit, nombre_comercial, submodulos_json))
        else:
            cur.execute("""
                UPDATE empresas 
                SET nombre_comercial=%s, tipo_empresa='transporte_carga', submodulos_activos=%s 
                WHERE nit=%s
            """, (nombre_comercial, submodulos_json, nit))
                       
        # Sincronización en modulos_empresas_autorizadas
        cur.execute("DELETE FROM modulos_empresas_autorizadas WHERE id_empresa = %s", (nit,))
        
        # Submódulos seleccionados inyectados directamente
        for submod in submodulos_seleccionados:
            cur.execute("INSERT INTO modulos_empresas_autorizadas (id_empresa, modulo, estatus) VALUES (%s, %s, 'activo')", (nit, submod))
            
        mysql.connection.commit()
        return jsonify(success=True, message="Empresa de Transporte de Carga y submódulos procesados correctamente.")
    except Exception as e: 
        mysql.connection.rollback()
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@bp_admin.route('/obtener_empresas_transporte_carga')
@login_required_custom
def obtener_empresas_transporte_carga():
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT id, nit, nombre_comercial, tipo_empresa, submodulos_activos FROM empresas WHERE tipo_empresa = 'transporte_carga'")
        rows = cur.fetchall()
        empresas = []
        cols = ['id', 'nit', 'nombre_comercial', 'tipo_empresa', 'submodulos_activos']
        for r in rows:
            row_dict = dict(zip(cols, r)) if not isinstance(r, dict) else r
            sub_raw = row_dict.get('submodulos_activos')
            if sub_raw:
                try:
                    row_dict['submodulos_activos'] = json.loads(sub_raw) if isinstance(sub_raw, str) else sub_raw
                except:
                    row_dict['submodulos_activos'] = []
            else:
                row_dict['submodulos_activos'] = []
            empresas.append(row_dict)
        return jsonify(success=True, empresas=empresas)
    except Exception as e:
        return jsonify(success=False, message=str(e))
    finally:
        cur.close()

@bp_admin.route('/api/submodulos_transporte_carga', methods=['GET'])
@login_required_custom
def api_submodulos_transporte_carga():
    submodulos_directos = [
        {"clave": "controlador_flota", "nombre": "Submódulo B: Controladores de Flota", "desc": "Acceso al panel administrativo, monitoreo satelital en tiempo real, gestión de vehículos y creación de códigos QR."},
        {"clave": "operador_flota", "nombre": "Submódulo B: Operadores en Ruta", "desc": "Acceso a la PWA móvil para conductores (Pre-logueo QR, inicio/fin de viaje y reporte de paradas)."},
        {"clave": "carga", "nombre": "Submódulo B: Gestión de Carga (Báscula)", "desc": "Módulo transversal para la operación de báscula, remisiones y pesaje."},
        {"clave": "preoperacional", "nombre": "Sub-submódulo C: Preoperacionales", "desc": "Diligenciamiento y auditoría de seguridad vial diaria (PESV)."}
    ]
    modulos_universales = [
        {"clave": "combustible", "nombre": "Control de Combustibles", "desc": "Módulo Universal de Gestión, monitoreo de consumo y eficiencia de combustible."},
        {"clave": "mantenimiento", "nombre": "Mantenimiento Preventivo (Proyectado)", "desc": "Módulo Universal de Mantenimiento"},
        {"clave": "pesv", "nombre": "PESV - Seguridad Vial Institucional (Proyectado)", "desc": "Plan Estratégico de Seguridad Vial corporativo"},
        {"clave": "contabilidad", "nombre": "Contabilidad (Proyectado)", "desc": "Módulo Contable General"}
    ]
    return jsonify(success=True, directos=submodulos_directos, universales=modulos_universales)

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
    
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("SELECT * FROM empresas WHERE nit = %s", (nit,))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'La empresa ya existe.'})
            cur.execute("INSERT INTO empresas (nit, nombre_comercial, tipo_empresa) VALUES (%s, %s, %s)", (nit, nombre_comercial, tipo_empresa))
            msg = 'Empresa creada correctamente.'
        else:
            cur.execute("UPDATE empresas SET nombre_comercial=%s, tipo_empresa=%s WHERE nit=%s", (nombre_comercial, tipo_empresa, nit))
            msg = 'Empresa actualizada correctamente.'
            
        mysql.connection.commit()
        return jsonify({'success': True, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_admin.route('/gestionar_tipo_empresa', methods=['POST'])
@login_required_custom
def gestionar_tipo_empresa():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO tipos_empresa (tipo) VALUES (%s)", (d.get('tipo'),))
        else:
            cur.execute("UPDATE tipos_empresa SET tipo=%s WHERE id=%s", (d.get('tipo'), d.get('id')))
        mysql.connection.commit()
        return jsonify(success=True, message="Tipo de empresa gestionado exitosamente.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/gestionar_usuario', methods=['GET', 'POST'])
@login_required_custom
def gestionar_usuario():
    d = request.form
    cur = mysql.connection.cursor()
    empresa_id = d.get('empresa_id')
    try:
        if not empresa_id:
            return jsonify(success=False, message="Falta ID de empresa (Validación Multi-Tenant requerida).")

        if d.get('accion') == 'crear':
            pw = bcrypt.generate_password_hash(d.get('password')).decode('utf-8')
            cur.execute("""INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa, telegram_id, telefono) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (d.get('cedula'), d.get('nombre'), pw, d.get('tipo_usuario'), d.get('clase'), d.get('perfil'), empresa_id, d.get('empresa_select'), None, d.get('telefono')))
            msg = "Usuario creado exitosamente."
            
        elif d.get('accion') == 'eliminar':
            cedula = d.get('cedula')
            cur.execute("DELETE FROM usuarios WHERE cedula=%s AND empresa_id=%s", (cedula, empresa_id))
            msg = "Usuario eliminado correctamente."
            
        else:
            cedula = d.get('cedula')
            nuevo_telefono = d.get('telefono')
            telegram_id_enviado = d.get('telegram_id')

            cur.execute("SELECT telefono FROM usuarios WHERE cedula=%s AND empresa_id=%s", (cedula, empresa_id))
            row = cur.fetchone()
            if not row:
                return jsonify(success=False, message="Usuario no encontrado o no pertenece a la empresa actual.")
                
            telefono_actual = row['telefono'] if isinstance(row, dict) else row[0]

            if str(telefono_actual) != str(nuevo_telefono):
                telegram_id_enviado = None
            elif not telegram_id_enviado or telegram_id_enviado.strip() == "":
                telegram_id_enviado = None

            query = "UPDATE usuarios SET nombre=%s, perfil=%s, telegram_id=%s, telefono=%s, empresa_id=%s, empresa=%s, tipo_usuario=%s, clase=%s"
            params = [d.get('nombre'), d.get('perfil'), telegram_id_enviado, nuevo_telefono, empresa_id, d.get('empresa_select'), d.get('tipo_usuario'), d.get('clase')]
            
            if d.get('password'):
                query += ", password=%s"
                params.append(bcrypt.generate_password_hash(d.get('password')).decode('utf-8'))
                
            query += " WHERE cedula=%s AND empresa_id=%s"
            params.extend([cedula, empresa_id])
            
            cur.execute(query, tuple(params))
            msg = "Usuario actualizado exitosamente."
            
        mysql.connection.commit()
        return jsonify(success=True, message=msg)
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/gestionar_perfil', methods=['POST'])
@login_required_custom
def gestionar_perfil():
    d = request.form
    cur = mysql.connection.cursor()
    nit = d.get('nit')
    try:
        if not nit:
            return jsonify(success=False, message="Falta NIT de empresa (Validación Multi-Tenant requerida).")

        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil, archivo_destino) VALUES (%s, %s, %s, %s, %s)",
                       (d.get('empresa_select'), nit, d.get('operacion'), d.get('perfil'), d.get('archivo_destino')))
        else:
            cur.execute("UPDATE perfiles SET operacion=%s, perfil=%s, archivo_destino=%s WHERE id=%s AND nit=%s", 
                       (d.get('operacion'), d.get('perfil'), d.get('archivo_destino'), d.get('id'), nit))
        mysql.connection.commit()
        return jsonify(success=True, message="Perfil y ruteo procesado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/obtener_perfiles')
@login_required_custom
def obtener_perfiles():
    empresa_id = request.args.get('empresa_id', '').strip()
    operacion = request.args.get('operacion', '').strip()
    if not empresa_id or not operacion:
        return jsonify({'perfiles': []})
    try:
        cur = mysql.connection.cursor()
        cur.execute("SELECT DISTINCT perfil FROM perfiles WHERE nit = %s AND operacion = %s", (empresa_id, operacion))
        perfiles = [row['perfil'] if isinstance(row, dict) else row[0] for row in cur.fetchall()]
        cur.close()
        return jsonify({'perfiles': perfiles})
    except Exception as e:
        return jsonify({'perfiles': []})

@csrf.exempt
@bp_admin.route('/gestionar_proveedor', methods=['POST'])
@login_required_custom
def gestionar_proveedor():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("""INSERT INTO proveedores (proveedor, id_proveedor, email1, email2, producto_servicio, precio) 
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                       (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), 'GLP', d.get('precio')))
        else:
            cur.execute("""UPDATE proveedores SET proveedor=%s, email1=%s, email2=%s, precio=%s WHERE id_proveedor=%s""",
                       (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor')))
        mysql.connection.commit()
        return jsonify(success=True, message="Proveedor actualizado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/registrar_proveedor', methods=['POST'])
@login_required_custom
def registrar_proveedor():
    d = request.form
    accion = d.get('accion', 'crear')
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("""
                INSERT INTO proveedores (proveedor, id_proveedor, email1, email2, producto_servicio, precio)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (d.get('proveedor'), d.get('id_proveedor'), d.get('email1'), d.get('email2'), 'GLP', d.get('precio')))
            msg = "Proveedor creado."
        else:
            cur.execute("""
                UPDATE proveedores 
                SET proveedor=%s, email1=%s, email2=%s, precio=%s 
                WHERE id_proveedor=%s
            """, (d.get('proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor')))
            msg = "Proveedor actualizado."
        mysql.connection.commit()
        return jsonify(success=True, message=msg)
    except Exception as e: return jsonify(success=False, message=str(e))
    finally: cur.close()

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
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO contactos (empresa, id_empresa, area_contacto, email) VALUES (%s, %s, %s, %s)",
                       (d.get('empresa_nombre'), d.get('id_empresa'), d.get('area_contacto'), d.get('email')))
        else:
            cur.execute("UPDATE contactos SET area_contacto=%s, email=%s WHERE id=%s", (d.get('area_contacto'), d.get('email'), d.get('id')))
        mysql.connection.commit()
        return jsonify(success=True, message="Contacto gestionado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/registrar_contacto', methods=['POST'])
@login_required_custom
def registrar_contacto():
    d = request.form
    accion = d.get('accion', 'crear')
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("""
                INSERT INTO contactos (empresa, id_empresa, area_contacto, email)
                VALUES (%s, %s, %s, %s)
            """, (d.get('empresa_nombre'), d.get('id_empresa'), d.get('area_contacto'), d.get('email')))
            msg = "Contacto creado."
        else:
            cur.execute("""
                UPDATE contactos SET area_contacto=%s, email=%s 
                WHERE id=%s
            """, (d.get('area_contacto'), d.get('email'), d.get('id')))
            msg = "Contacto actualizado."
        mysql.connection.commit()
        return jsonify(success=True, message=msg)
    except Exception as e: return jsonify(success=False, message=str(e))
    finally: cur.close()

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
    cur.execute("SELECT id, cedula, nombre, perfil, empresa, empresa_id, telegram_id, telefono FROM usuarios")
    rows = cur.fetchall()
    cols = ['id','cedula','nombre','perfil','empresa','empresa_id','telegram_id','telefono']
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
                    # 1. Búsqueda por coincidencia en el nombre físico
                    if modulo and modulo in f.lower():
                        rutas.add(f)
                        continue
                        
                    # 2. Búsqueda profunda por Etiqueta Arquitectónica (MÓDULO / SUBMÓDULO)
                    ruta_completa = os.path.join(tpl_dir, f)
                    try:
                        with open(ruta_completa, 'r', encoding='utf-8') as archivo:
                            # Leer solo las primeras 5 líneas por eficiencia de memoria
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
                        
        # Rutas universales garantizadas de Base / Default
        rutas.add('panel_principal.html')
        rutas.add('A_dashboard_universal.html')
        
    except Exception as e:
        print(f"Error escaneando rutas de introspección: {e}")
        
    return jsonify(success=True, rutas=sorted(list(rutas)))