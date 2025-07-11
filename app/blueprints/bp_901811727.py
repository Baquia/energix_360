from flask import Blueprint, render_template, session, request, jsonify,flash,redirect, url_for
from app import mysql, csrf
from app.forms import RegistroUsuarioForm
from app.utils import login_required_custom

bp_901811727 = Blueprint('bp_901811727', __name__)

@bp_901811727.route('/901811727.html')
@login_required_custom
def panel_webmaster():
    form = RegistroUsuarioForm()
    cur = mysql.connection.cursor()
    cur.execute("SELECT nit, nombre_comercial FROM empresas")
    empresas = cur.fetchall()
    cur.close()
    return render_template('901811727.html', nombre=session.get('nombre'), empresa=session.get('empresa'), form=form, empresas=empresas)

@csrf.exempt
@bp_901811727.route('/registrar_empresa', methods=['POST'])
@login_required_custom
def registrar_empresa():
    nombre_comercial = request.form.get('nombre_comercial', '').strip()
    nit = request.form.get('nit', '').strip()

    if not nombre_comercial or not nit:
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})

    cur = mysql.connection.cursor()
    cur.execute("SELECT * FROM empresas WHERE nit = %s", (nit,))
    existente = cur.fetchone()

    if existente:
        cur.close()
        return jsonify({'success': False, 'message': 'La empresa ya existe.'})

    try:
        cur.execute("INSERT INTO empresas (nit, nombre_comercial) VALUES (%s, %s)", (nit, nombre_comercial))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Empresa creada correctamente.'})
    except Exception as e:
        print("Error al insertar empresa:", e)
        return jsonify({'success': False, 'message': 'Error en el servidor al crear la empresa.'})
    finally:
        cur.close()


@csrf.exempt
@bp_901811727.route('/registrar_perfil', methods=['POST'])
@login_required_custom
def registrar_perfil():
    empresa_nombre = request.form.get('empresa_select', '').strip()
    nit = request.form.get('nit', '').strip()
    operacion = request.form.get('operacion', '').strip()
    perfil = request.form.get('perfil', '').strip()

    if not empresa_nombre or not nit or not operacion or not perfil:
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})

    cur = mysql.connection.cursor()
    cur.execute("SELECT nit FROM empresas WHERE nombre_comercial = %s", (empresa_nombre,))
    empresa = cur.fetchone()

    if not empresa:
        cur.close()
        return jsonify({'success': False, 'message': 'La empresa no existe.'})

    if str(empresa['nit']) != nit:
        cur.close()
        return jsonify({'success': False, 'message': 'El NIT no corresponde a la empresa seleccionada.'})

    cur.execute("SELECT * FROM perfiles WHERE nit = %s AND operacion = %s AND perfil = %s", (nit, operacion, perfil))
    existente = cur.fetchone()

    if existente:
        cur.close()
        return jsonify({'success': False, 'message': 'El perfil ya existe.'})

    try:
        cur.execute("INSERT INTO perfiles (nit, operacion, perfil) VALUES (%s, %s, %s)", (nit, operacion, perfil))
        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Perfil creado correctamente.'})
    except Exception as e:
        print("Error al insertar perfil:", e)
        return jsonify({'success': False, 'message': 'Error en el servidor al crear el perfil.'})
    finally:
        cur.close()

@csrf.exempt
@csrf.exempt
@bp_901811727.route('/obtener_perfiles')
@login_required_custom
def obtener_perfiles():
    empresa_id = request.args.get('empresa_id', '').strip()
    operacion = request.args.get('operacion', '').strip()

    if not empresa_id or not operacion:
        return jsonify({'perfiles': []})

    try:
        cur = mysql.connection.cursor()
        cur.execute("""
            SELECT DISTINCT perfil 
            FROM perfiles 
            WHERE nit = %s AND operacion = %s
        """, (empresa_id, operacion))
        perfiles = [row['perfil'] for row in cur.fetchall()]
        cur.close()

        return jsonify({'perfiles': perfiles})

    except Exception as e:
        print(f"Error en obtener_perfiles: {e}")
        return jsonify({'perfiles': [], 'error': 'Error de servidor'})

from werkzeug.security import generate_password_hash

@csrf.exempt
@bp_901811727.route('/registrar_usuario', methods=['POST'])
@login_required_custom
def registrar_usuario():
    cedula = request.form.get('cedula', '').strip()
    nombre = request.form.get('nombre', '').strip()
    password = request.form.get('password', '').strip()
    tipo_usuario = request.form.get('tipo_usuario', '').strip()
    clase = request.form.get('clase', '').strip()
    empresa_id = request.form.get('empresa_id', '').strip()
    empresa_nombre = request.form.get('empresa_select', '').strip()
    operacion = request.form.get('operacion', '').strip()
    perfil = request.form.get('perfil', '').strip()

    if not all([cedula, nombre, password, tipo_usuario, clase, empresa_id, empresa_nombre, operacion, perfil]):
        return jsonify({'success': False, 'message': 'Faltan datos obligatorios.'})

    cur = mysql.connection.cursor()
    cur.execute("SELECT id FROM usuarios WHERE cedula = %s", (cedula,))
    if cur.fetchone():
        cur.close()
        return jsonify({'success': False, 'message': 'El usuario ya está registrado.'})

    try:
        password_hashed = generate_password_hash(password)
        cur.execute("""
            INSERT INTO usuarios 
            (cedula, nombre, password, tipo_usuario, clase, perfil, empresa_id, empresa)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (cedula, nombre, password_hashed, tipo_usuario, clase, perfil, empresa_id, empresa_nombre))

        mysql.connection.commit()
        return jsonify({'success': True, 'message': 'Usuario creado correctamente.'})
    except Exception as e:
        print("Error al insertar usuario:", e)
        return jsonify({'success': False, 'message': 'Error en el servidor al crear el usuario.'})
    finally:
        cur.close()
