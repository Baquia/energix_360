# energix_360.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm, RegistroUsuarioForm
from functools import wraps
from flask import send_from_directory
from flask import current_app

app = create_app()

# --- NUEVO: EVITAR CACHÉ DE NAVEGADOR EN HTML ---
@app.after_request
def add_security_headers(response):
    # Si la respuesta es HTML, prohibir que el navegador la guarde en su caché nativa
    # Esto evita ver datos de sesión de otros usuarios al dar "Atrás" o si falla el SW.
    if "text/html" in response.headers.get("Content-Type", ""):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response
# -----------------------------------------------

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
    
    cur.execute("SELECT nit, tipo_empresa FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
    empresa_resultado = cur.fetchone()

    if not empresa_resultado:
        cur.close()
        return jsonify(success=False, message="EMPRESA NO ENCONTRADA")

    nit_empresa = str(empresa_resultado['nit'])
    
    tipo_empresa = empresa_resultado.get('tipo_empresa') or 'general'

    cur.execute("SELECT * FROM usuarios WHERE cedula = %s", (cedula,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        return jsonify(success=False, message="USUARIO NO EXISTE")

    if not bcrypt.check_password_hash(usuario['password'], password):
        return jsonify(success=False, message="CONTRASEÑA INCORRECTA")

    if str(usuario['empresa_id']) != nit_empresa:
        return jsonify(success=False, message="USUARIO NO PERTENECE A ESTA EMPRESA")

    # Guardar sesión
    session['usuario_id'] = usuario['id']          
    session['cedula'] = usuario['cedula']          
    session['nombre'] = usuario['nombre']
    session['usuario_nombre'] = usuario['nombre']  
    session['empresa'] = usuario['empresa']
    session['empresa_id'] = usuario['empresa_id']

    # --- SALT PARA LOGIN OFFLINE ---
    offline_salt = f"{usuario['cedula']}|{usuario['empresa_id']}"

    if tipo_empresa == 'transporte_especial':
        ruta_html = f"te/{nit_empresa}.html"
    else:
        ruta_html = f"{nit_empresa}.html"

    return jsonify(
        success=True,
        html=ruta_html,  
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

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    print("\n✅ RUTAS DISPONIBLES EN FLASK:")
    for rule in app.url_map.iter_rules():
        print(f"→ {rule}  →  {rule.endpoint}")
        
    app.run(debug=True, port=5001)