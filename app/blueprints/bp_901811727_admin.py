# bp_901811727_admin.py
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
    try:
        if d.get('accion') == 'crear':
            pw = bcrypt.generate_password_hash(d.get('password')).decode('utf-8')
            cur.execute("""INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa, telegram_id, telefono) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                       (d.get('cedula'), d.get('nombre'), pw, d.get('tipo_usuario'), d.get('clase'), d.get('perfil'), d.get('empresa_id'), d.get('empresa_select'), None, d.get('telefono')))
            msg = "Usuario creado exitosamente."
            
        elif d.get('accion') == 'eliminar':
            cedula = d.get('cedula')
            cur.execute("DELETE FROM usuarios WHERE cedula=%s", (cedula,))
            msg = "Usuario eliminado correctamente."
            
        else:
            cedula = d.get('cedula')
            nuevo_telefono = d.get('telefono')
            telegram_id_enviado = d.get('telegram_id')

            cur.execute("SELECT telefono FROM usuarios WHERE cedula=%s", (cedula,))
            row = cur.fetchone()
            telefono_actual = row['telefono'] if isinstance(row, dict) else row[0]

            if str(telefono_actual) != str(nuevo_telefono):
                telegram_id_enviado = None
            elif not telegram_id_enviado or telegram_id_enviado.strip() == "":
                telegram_id_enviado = None

            query = "UPDATE usuarios SET nombre=%s, perfil=%s, telegram_id=%s, telefono=%s, empresa_id=%s, empresa=%s"
            params = [d.get('nombre'), d.get('perfil'), telegram_id_enviado, nuevo_telefono, d.get('empresa_id'), d.get('empresa_select')]
            
            if d.get('password'):
                query += ", password=%s"
                params.append(bcrypt.generate_password_hash(d.get('password')).decode('utf-8'))
                
            query += " WHERE cedula=%s"
            params.append(cedula)
            
            cur.execute(query, tuple(params))
            msg = "Usuario actualizado exitosamente."
            
        mysql.connection.commit()
        return jsonify(success=True, message=msg)
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/registrar_usuario', methods=['POST'])
@login_required_custom
def registrar_usuario():
    data = request.form
    cedula = data.get('cedula', '').strip()
    nombre = data.get('nombre', '').strip()
    password = data.get('password', '').strip()
    accion = data.get('accion', 'crear').strip()
    
    if accion == 'crear' and not all([cedula, nombre, password]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
        
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'El usuario ya existe.'})
            password_hashed = bcrypt.generate_password_hash(password).decode('utf-8')
            cur.execute("""
                INSERT INTO usuarios (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa, telegram_id, telefono)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (cedula, nombre, password_hashed, data.get('tipo_usuario'), data.get('clase'), 
                  data.get('perfil'), data.get('empresa_id'), data.get('empresa_select'), data.get('telegram_id'), data.get('telefono')))
            msg = 'Usuario creado.'
        else:
            if password:
                password_hashed = bcrypt.generate_password_hash(password).decode('utf-8')
                cur.execute("""
                    UPDATE usuarios SET nombre=%s, password=%s, perfil=%s, empresa_id=%s, empresa=%s, telegram_id=%s, telefono=%s
                    WHERE cedula=%s
                """, (nombre, password_hashed, data.get('perfil'), data.get('empresa_id'), data.get('empresa_select'), data.get('telegram_id'), data.get('telefono'), cedula))
            else:
                cur.execute("""
                    UPDATE usuarios SET nombre=%s, perfil=%s, empresa_id=%s, empresa=%s, telegram_id=%s, telefono=%s
                    WHERE cedula=%s
                """, (nombre, data.get('perfil'), data.get('empresa_id'), data.get('empresa_select'), data.get('telegram_id'), data.get('telefono'), cedula))
            msg = 'Usuario actualizado.'

        mysql.connection.commit()
        return jsonify({'success': True, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
    finally:
        cur.close()

@csrf.exempt
@bp_admin.route('/gestionar_perfil', methods=['POST'])
@login_required_custom
def gestionar_perfil():
    d = request.form
    cur = mysql.connection.cursor()
    try:
        if d.get('accion') == 'crear':
            cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil, archivo_destino) VALUES (%s, %s, %s, %s, %s)",
                       (d.get('empresa_select'), d.get('nit'), d.get('operacion'), d.get('perfil'), d.get('archivo_destino')))
        else:
            cur.execute("UPDATE perfiles SET operacion=%s, perfil=%s, archivo_destino=%s WHERE id=%s", 
                       (d.get('operacion'), d.get('perfil'), d.get('archivo_destino'), d.get('id')))
        mysql.connection.commit()
        return jsonify(success=True, message="Perfil y ruteo procesado.")
    except Exception as e: 
        return jsonify(success=False, message=str(e))
    finally: 
        cur.close()

@csrf.exempt
@bp_admin.route('/registrar_perfil', methods=['POST'])
@login_required_custom
def registrar_perfil():
    empresa_nombre = request.form.get('empresa_select', '').strip()
    nit = request.form.get('nit', '').strip()
    operacion = request.form.get('operacion', '').strip()
    perfil = request.form.get('perfil', '').strip()
    accion = request.form.get('accion', 'crear').strip()
    pid = request.form.get('id', '')

    if not all([empresa_nombre, nit, operacion, perfil]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})
    
    cur = mysql.connection.cursor()
    try:
        if accion == 'crear':
            cur.execute("SELECT nit FROM empresas WHERE nombre_comercial = %s", (empresa_nombre,))
            empresa = cur.fetchone()
            nit_db = empresa['nit'] if isinstance(empresa, dict) else (empresa[0] if empresa else None)
            if not empresa or str(nit_db) != nit:
                return jsonify({'success': False, 'message': 'Empresa no válida o NIT incorrecto.'})
                
            cur.execute("SELECT * FROM perfiles WHERE nit = %s AND operacion = %s AND perfil = %s", (nit, operacion, perfil))
            if cur.fetchone():
                return jsonify({'success': False, 'message': 'El perfil ya existe.'})
                
            cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil) VALUES (%s, %s, %s, %s)", (empresa_nombre, nit, operacion, perfil))
            msg = 'Perfil creado correctamente.'
        else:
            cur.execute("UPDATE perfiles SET operacion=%s, perfil=%s WHERE id=%s", (operacion, perfil, pid))
            msg = 'Perfil actualizado correctamente.'

        mysql.connection.commit()
        return jsonify({'success': True, 'message': msg})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})
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
                       (d.get('proveedor'), d.get('email1'), d.get('email2'), d.get('precio'), d.get('id_proveedor')))
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
        return jsonify(success=False, modulos=[])
        
    cur = mysql.connection.cursor()
    try:
        cur.execute("SELECT modulo FROM modulos_empresas_autorizadas WHERE id_empresa = %s AND estatus = 'activo'", (nit,))
        modulos = [row[0] if not isinstance(row, dict) else row['modulo'] for row in cur.fetchall()]
        return jsonify(success=True, modulos=modulos)
    except Exception as e:
        return jsonify(success=False, message=str(e))
    finally:
        cur.close()