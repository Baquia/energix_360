# energix_360.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory, current_app
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm, RegistroUsuarioForm
from functools import wraps
import os
import MySQLdb.cursors

# 1. DETERMINAR LA RUTA REAL DE LA CARPETA APP
base_dir = os.path.abspath(os.path.dirname(__file__))
static_path = os.path.join(base_dir, "app", "static")

# 2. INICIALIZAR LA APP
app = create_app()

# 3. ASIGNAR LA CARPETA STATIC CORRECTA
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

# --- RUTAS PRINCIPALES Y PWA ---
@app.route("/login_energix360_offline.html")
def login_energix360_offline():
    return render_template("login_energix360_offline.html")

@app.route("/sw.js")
def sw():
    return send_from_directory(app.static_folder, "sw.js", mimetype="application/javascript")

@app.route('/')
def index():
    # 1. Si ya tiene sesión, redirigir mediante Blueprints (CORRIGE EL ERROR 404)
    if 'usuario_id' in session:
        tipo_str = str(session.get('tipo_empresa', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()
        
        # Redirección lógica basada en el nuevo sistema de Blueprints
        if 'ventas_distribucion' in tipo_str:
             return redirect(url_for('logistica_bp.panel_logistica'))
             
        elif 'cria_beneficio_aves_corral' in tipo_str or empresa_id == '890707006':
             # Enviamos a la ruta virtual, no al archivo físico
             return redirect(url_for('gestionavicola_bp.panel_avicola'))
             
        elif 'webmaster' in tipo_str or empresa_id == '901811727':
             return redirect(url_for('bp_901811727.control_usuarios'))
        
        # Fallback para empresas GLP o desconocidas
        return redirect(url_for('bp_glp.ver_facturas_glp'))

    # 2. Si no hay sesión, preparar formulario de login
    form = LoginForm()
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT nit, nombre_comercial FROM empresas ORDER BY nombre_comercial ASC")
        empresas = cur.fetchall()
        cur.close()
        form.empresa.choices = [(e['nit'], e['nombre_comercial']) for e in empresas]
    except Exception as err:
        print(f"Error cargando empresas: {err}")
        form.empresa.choices = []

    return render_template('login_energix360.html', form=form) 

@app.route('/login', methods=['POST'])
@csrf.exempt
def login():
    data = request.get_json(force=True)
    
    cedula = data.get('cedula')
    password = data.get('password')
    nombre_empresa = data.get('empresa')

    if not cedula or not password or not nombre_empresa:
        return jsonify(success=False, message="Datos incompletos")

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("SELECT nit, tipo_empresa FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
    empresa_resultado = cur.fetchone()

    if not empresa_resultado:
        cur.close()
        return jsonify(success=False, message="EMPRESA NO ENCONTRADA")

    nit_empresa = str(empresa_resultado['nit'])
    tipo_empresa = str(empresa_resultado.get('tipo_empresa') or 'general').lower()

    cur.execute("SELECT * FROM usuarios WHERE cedula = %s", (cedula,))
    usuario = cur.fetchone()

    if not usuario:
        cur.close()
        return jsonify(success=False, message="USUARIO NO EXISTE")

    if not bcrypt.check_password_hash(usuario['password'], password):
        cur.close()
        return jsonify(success=False, message="CONTRASEÑA INCORRECTA")

    if str(usuario['empresa_id']) != nit_empresa:
        cur.close()
        return jsonify(success=False, message="USUARIO NO PERTENECE A ESTA EMPRESA")

    # --- LECTURA DE MÓDULOS PERMITIDOS (ESPECÍFICO AVÍCOLAS) ---
    modulos_activos = []
    if 'cria_beneficio_aves_corral' in tipo_empresa or nit_empresa == '890707006':
        cur.execute("SELECT modulo FROM modulos_empresas_avicolas WHERE id_empresa = %s AND estatus = 'activo'", (nit_empresa,))
        modulos_bd = cur.fetchall()
        modulos_activos = [m['modulo'] for m in modulos_bd]

    cur.close()

    # Iniciar Sesión con todos los campos necesarios para los Blueprints
    session['usuario_id'] = usuario['id']          
    session['cedula'] = usuario['cedula']          
    session['nombre'] = usuario['nombre']
    session['usuario_nombre'] = usuario['nombre']  
    session['empresa'] = usuario['empresa']
    session['empresa_id'] = usuario['empresa_id']
    session['nit'] = usuario['empresa_id'] # NIT para compatibilidad
    session['tipo_empresa'] = tipo_empresa
    session['perfil'] = usuario.get('perfil', '').strip()
    session['modulos_activos'] = modulos_activos

    offline_salt = f"{usuario['cedula']}|{usuario['empresa_id']}"

    # --- DETERMINAR RUTA DE RETORNO (Evita el error 404 del NIT.html) ---
    if 'ventas_distribucion' in tipo_empresa:
        ruta_html = "control_logistica.html"
    elif 'cria_beneficio_aves_corral' in tipo_empresa or nit_empresa == '890707006':
        ruta_html = "gestion_avicola.html"
    elif 'webmaster' in tipo_empresa:
        ruta_html = "901811727.html"
    else:
        ruta_html = "panel_general.html"

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
    # Verificación de carpetas en desarrollo local
    if not os.path.exists(app.static_folder):
        print(f"ADVERTENCIA: No se encontró la carpeta estática en {app.static_folder}")
    app.run(debug=True, port=5002)