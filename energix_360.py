# energix_360.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm, RegistroUsuarioForm
from functools import wraps
from flask import send_from_directory
from flask import current_app

app = create_app()


@app.route("/login_energix360_offline.html")
def login_energix360_offline():
    return render_template("login_energix360_offline.html")

# Decorador para proteger rutas
def login_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function


@app.route("/sw.js")
def sw():
    return send_from_directory(current_app.static_folder, "sw.js",
                           mimetype="application/javascript")


@app.route('/', methods=['GET'])
def index():
    form = LoginForm()
    cur = mysql.connection.cursor()
    cur.execute("SELECT nit, nombre_comercial FROM empresas")
    empresas = cur.fetchall()
    form.empresa.choices = [(e['nit'], e['nombre_comercial']) for e in empresas]
    cur.close()
    return render_template('login_energix360.html', form=form, empresas=empresas)

@app.route('/login', methods=['POST'])
@csrf.exempt
def login():
    data = request.get_json(force=True)
    print(">>> Datos recibidos:", data)

    cedula = data.get('cedula')
    password = data.get('password')
    nombre_empresa = data.get('empresa')

    if not cedula or not password or not nombre_empresa:
        return jsonify(success=False, message="Datos incompletos")

    cur = mysql.connection.cursor()
    cur.execute("SELECT nit FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
    empresa_resultado = cur.fetchone()

    if not empresa_resultado:
        return jsonify(success=False, message="EMPRESA NO ENCONTRADA")

    nit_empresa = str(empresa_resultado['nit'])

    cur.execute("SELECT * FROM usuarios WHERE cedula = %s", (cedula,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        return jsonify(success=False, message="USUARIO NO EXISTE")

    if not bcrypt.check_password_hash(usuario['password'], password):
        return jsonify(success=False, message="CONTRASEÑA INCORRECTA")

    if str(usuario['empresa_id']) != nit_empresa:
        return jsonify(success=False, message="CONTRASEÑA INCORRECTA")

    # Guardar sesión
    # Guardar sesión
    session['usuario_id'] = usuario['id']          # ID interno (lo dejamos igual)
    session['cedula'] = usuario['cedula']          # NUEVO: cedula del operador
    session['nombre'] = usuario['nombre']
    session['usuario_nombre'] = usuario['nombre']  # NUEVO: para mermas
    session['empresa'] = usuario['empresa']
    session['empresa_id'] = usuario['empresa_id']


    # --- SALT PARA LOGIN OFFLINE (no requiere cambios en BD) ---
    offline_salt = f"{usuario['cedula']}|{usuario['empresa_id']}"

    return jsonify(
        success=True,
        html=f"{nit_empresa}.html",
        offline_enabled=True,
        offline_salt=offline_salt,
        usuario={
            "id": usuario["id"],
            "cedula": usuario["cedula"],
            "nombre": usuario["nombre"],
            "empresa": usuario["empresa"],
            "empresa_id": usuario["empresa_id"],
            "perfil": usuario.get("perfil")
        }
    )


# Ruta protegida: renderiza tablero solo si hay sesión activa
#@app.route('/<nit>.html')
#@login_required_custom
#def empresa_tablero(nit):
    #if nit == '901811727':
        #form = RegistroUsuarioForm()
        #return render_template(f"{nit}.html", form=form, nombre=session.get('nombre'), empresa=session.get('empresa'))
    #else:
        #return render_template(f"{nit}.html", nombre=session.get('nombre'), empresa=session.get('empresa'))

# Logout general
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

# Ejecutar en localhost
if __name__ == '__main__':

    print("\n✅ RUTAS DISPONIBLES EN FLASK:")
    for rule in app.url_map.iter_rules():
        print(f"→ {rule}  →  {rule.endpoint}")
        
    app.run(debug=True, port=5001)

