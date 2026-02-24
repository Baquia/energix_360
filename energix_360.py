# energix_360.py
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm
from functools import wraps
import os
import MySQLdb.cursors

# 1. DETERMINACIÓN DE RUTAS FÍSICAS (Estructura de Carpetas)
base_dir = os.path.abspath(os.path.dirname(__file__))
static_path = os.path.join(base_dir, "app", "static")

# 2. INICIALIZACIÓN DE LA APLICACIÓN (Factory Pattern)
app = create_app()

# 3. ASIGNACIÓN DE CARPETA STATIC (Para Logos y PWA)
app.static_folder = static_path
app.static_url_path = "/static"

# --- CONFIGURACIÓN DE SEGURIDAD Y LIMPIEZA DE CACHÉ ---
@app.after_request
def add_security_headers(response):
    """Garantiza que el navegador no use versiones viejas del HTML tras un despliegue."""
    if "text/html" in response.headers.get("Content-Type", ""):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

# --- DECORADORES DE PROTECCIÓN DE RUTA ---
def login_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- RUTAS DE SISTEMA (PWA Y OFFLINE) ---
@app.route("/login_energix360_offline.html")
def login_energix360_offline():
    return render_template("login_energix360_offline.html")

@app.route("/sw.js")
def sw():
    """Service Worker para soporte Offline."""
    return send_from_directory(app.static_folder, "sw.js", mimetype="application/javascript")

# --- CONTROLADOR PRINCIPAL (INDEX / REDIRECCIONAMIENTO) ---
@app.route('/')
def index():
    """
    Controla el acceso inicial. Si el usuario ya está logueado, 
    lo envía a su módulo real usando Blueprints.
    """
    if 'usuario_id' in session:
        # Recuperamos datos de sesión sanitizados
        tipo_str = str(session.get('tipo_empresa', '')).strip().lower()
        perfil_str = str(session.get('perfil', '')).strip().lower()
        empresa_id = str(session.get('empresa_id', '')).strip()

        # 1. PRIORIDAD: WEBMASTER (Evita el 404 detectado)
        if 'webmaster' in tipo_str or empresa_id == '901811727' or 'webmaster' in perfil_str:
             return redirect(url_for('bp_901811727.control_usuarios'))

        # 2. SECTOR AVÍCOLA (Gestión Genérica)
        elif 'cria_beneficio_aves_corral' in tipo_str or empresa_id == '890707006':
             return redirect(url_for('gestionavicola_bp.panel_avicola'))
        
        # 3. LOGÍSTICA Y DISTRIBUCIÓN
        elif 'ventas_distribucion' in tipo_str:
             return redirect(url_for('logistica_bp.panel_logistica'))

        # 4. DEFAULT: MÓDULO GLP
        return redirect(url_for('bp_glp.ver_facturas_glp'))

    # Si no hay sesión, cargamos el formulario de login con la lista de empresas
    form = LoginForm()
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT nit, nombre_comercial FROM empresas ORDER BY nombre_comercial ASC")
        empresas_db = cur.fetchall()
        cur.close()
        form.empresa.choices = [(e['nit'], e['nombre_comercial']) for e in empresas_db]
    except Exception as err:
        print(f"Error al cargar empresas en el login: {err}")
        form.empresa.choices = []

    return render_template('login_energix360.html', form=form) 

# --- CONTROLADOR DE AUTENTICACIÓN (LOGIN) ---
@app.route('/login', methods=['POST'])
@csrf.exempt
def login():
    """
    Procesa el login via JSON. Valida empresa, usuario, contraseña 
    y determina la ruta virtual de destino.
    """
    data = request.get_json(force=True)
    cedula = data.get('cedula')
    password = data.get('password')
    nombre_empresa = data.get('empresa')

    if not all([cedula, password, nombre_empresa]):
        return jsonify(success=False, message="Datos incompletos.")

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    # A. Validar Existencia de Empresa
    cur.execute("SELECT nit, tipo_empresa FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
    emp_info = cur.fetchone()
    if not emp_info:
        cur.close()
        return jsonify(success=False, message="La empresa seleccionada no existe.")

    nit_empresa = str(emp_info['nit'])
    tipo_empresa = str(emp_info.get('tipo_empresa') or '').lower()

    # B. Validar Usuario y Password
    cur.execute("SELECT * FROM usuarios WHERE cedula = %s", (cedula,))
    usuario = cur.fetchone()

    if not usuario or not bcrypt.check_password_hash(usuario['password'], password):
        cur.close()
        return jsonify(success=False, message="Cédula o contraseña incorrecta.")

    if str(usuario['empresa_id']) != nit_empresa:
        cur.close()
        return jsonify(success=False, message="El usuario no pertenece a la empresa seleccionada.")

    # C. Cargar permisos específicos (Módulos activos para Avícolas)
    modulos_activos = []
    if 'cria_beneficio_aves_corral' in tipo_empresa or nit_empresa == '890707006':
        cur.execute("SELECT modulo FROM modulos_empresas_avicolas WHERE id_empresa = %s AND estatus = 'activo'", (nit_empresa,))
        modulos_activos = [m['modulo'] for m in cur.fetchall()]
    
    cur.close()

    # D. INICIALIZACIÓN DE SESIÓN
    session.update({
        'usuario_id': usuario['id'],
        'cedula': usuario['cedula'],
        'nombre': usuario['nombre'],
        'empresa': usuario['empresa'],
        'empresa_id': usuario['empresa_id'],
        'tipo_empresa': tipo_empresa,
        'perfil': str(usuario.get('perfil') or '').strip().lower(),
        'modulos_activos': modulos_activos
    })

    # E. DETERMINACIÓN DE RUTA VIRTUAL (Sin archivos físicos inexistentes)
    perfil = session['perfil']
    
    # Lógica de enrutamiento para el Frontend
    if 'webmaster' in tipo_empresa or nit_empresa == '901811727' or 'webmaster' in perfil:
        ruta_virtual = "901811727.html" # Ruta manejada por bp_901811727
    elif 'ventas_distribucion' in tipo_empresa:
        ruta_virtual = "control_logistica.html" # Ruta manejada por logistica_bp
    elif 'cria_beneficio_aves_corral' in tipo_empresa or nit_empresa == '890707006':
        ruta_virtual = "gestion_avicola.html" # Ruta manejada por gestionavicola_bp
    else:
        ruta_virtual = "glp.html" # Fallback a GLP para evitar 404

    return jsonify(
        success=True,
        html=ruta_virtual,
        usuario={"id": usuario["id"], "nombre": usuario["nombre"]}
    )

# --- CIERRE DE SESIÓN ---
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

# --- EJECUCIÓN (MODO DESARROLLO) ---
if __name__ == '__main__':
    # Verificamos integridad de carpetas antes de arrancar
    if not os.path.exists(app.static_folder):
        print(f"⚠️  ALERTA: Carpeta static no encontrada en {app.static_folder}")
    
    app.run(debug=True, port=5002)