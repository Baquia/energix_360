# energix_360.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory, current_app
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm, RegistroUsuarioForm
from functools import wraps
import os

# 1. DETERMINAR LA RUTA REAL DE LA CARPETA APP
# Según tu aclaración, el logo está en la carpeta 'static' que está DENTRO de 'app'
base_dir = os.path.abspath(os.path.dirname(__file__))
# Forzamos la ruta a energix_360/app/static
static_path = os.path.join(base_dir, "app", "static")

# 2. INICIALIZAR LA APP
# Usamos create_app() para cargar toda tu configuración
app = create_app()

# 3. ASIGNAR LA CARPETA STATIC CORRECTA (DENTRO DE APP)
# Esto recuperará el logo y las imágenes de la carpeta app/static
app.static_folder = static_path
app.static_url_path = "/static"

# --- CONFIGURACIÓN DE SEGURIDAD Y CACHÉ ---
@app.after_request
def add_security_headers(response):
    if "text/html" in response.headers.get("Content-Type", ""):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

# --- DECORADORES ---
def login_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- RUTAS PRINCIPALES ---

@app.route("/login_energix360_offline.html")
def login_energix360_offline():
    return render_template("login_energix360_offline.html")

@app.route("/sw.js")
def sw():
    return send_from_directory(app.static_folder, "sw.js",
                           mimetype="application/javascript")

# EN energix_360.py

@app.route('/')
def index():
    # 1. Si ya tiene sesión, redirigir a su panel correspondiente
    if 'usuario_id' in session:
        # Recuperamos el tipo guardado en sesión
        tipo_str = str(session.get('tipo_empresa', '')).strip().lower()
        
        # Lógica de redirección (reutilizando tu lógica de login)
        if 'ventas_distribucion' in tipo_str:
             return redirect('/control_logistica.html')
        elif 'webmaster' in tipo_str:
             return redirect('/901811727.html')
        
        # Redirección por defecto
        return redirect(f"/{session.get('empresa_id')}.html")

    form = LoginForm()

    # --- CORRECCIÓN CLAVE AQUÍ ---
    import MySQLdb.cursors 
    
    # 2. Usamos DictCursor para que la BD devuelva nombres de columnas
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT nit, nombre_comercial FROM empresas")
        empresas = cur.fetchall()
        cur.close()

        # 3. Llenamos el SelectField
        # Ahora sí funcionará e['nit'] porque estamos usando DictCursor
        form.empresa.choices = [(e['nit'], e['nombre_comercial']) for e in empresas]
    
    except Exception as err:
        print(f"Error cargando empresas: {err}")
        form.empresa.choices = []

    return render_template('login_energix360.html', form=form) 
    # NOTA: Si tu archivo HTML de login se llama 'login.html', cambia 'index.html' por 'login.html'

from flask import jsonify, request, session
import MySQLdb.cursors # Importante para que funcionen los diccionarios

@app.route('/login', methods=['POST'])
@csrf.exempt
def login():
    data = request.get_json(force=True)
    
    cedula = data.get('cedula')
    password = data.get('password')
    nombre_empresa = data.get('empresa')

    if not cedula or not password or not nombre_empresa:
        return jsonify(success=False, message="Datos incompletos")

    import MySQLdb.cursors
    # Usamos DictCursor para acceder por nombre de columna
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
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

    session['usuario_id'] = usuario['id']          
    session['cedula'] = usuario['cedula']          
    session['nombre'] = usuario['nombre']
    session['usuario_nombre'] = usuario['nombre']  
    session['empresa'] = usuario['empresa']
    session['empresa_id'] = usuario['empresa_id']

    offline_salt = f"{usuario['cedula']}|{usuario['empresa_id']}"

    # --- LÓGICA DE REDIRECCIÓN ACTUALIZADA ---
    tipo_str = tipo_empresa.lower()

    # Ahora buscamos el término exacto que pusiste en la BD
    if 'ventas_distribucion' in tipo_str:
        ruta_html = "control_logistica.html"
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


# --- BLOQUE DE ARRANQUE ---
if __name__ == '__main__':
    print("\n--- REVISIÓN DE CARPETA ESTÁTICA ---")
    print(f"Buscando logo y fotos en: {app.static_folder}")
    
    if os.path.exists(app.static_folder):
        print("✅ LA CARPETA EXISTE FÍSICAMENTE.")
    else:
        print("❌ ERROR: LA CARPETA NO EXISTE EN ESA RUTA.")

    app.run(debug=True, port=5002)