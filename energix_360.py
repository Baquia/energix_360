# energix_360.py
import os
import uuid
from flask import render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta, timezone

# ==============================================================================
# 1. CONFIGURACIÓN DE RUTAS FÍSICAS E INICIALIZACIÓN (App Factory)
# ==============================================================================
base_dir = os.path.abspath(os.path.dirname(__file__))
static_path = os.path.join(base_dir, "app", "static")

app = create_app()
app.static_folder = static_path
app.static_url_path = "/static"

# ==============================================================================
# 3. MIDDLEWARE Y DECORADORES DE SEGURIDAD
# ==============================================================================
@app.after_request
def add_security_headers(response):
    if "text/html" in response.headers.get("Content-Type", ""):
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response

def login_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('index'))
            
        # --- LÓGICA DE CADUCIDAD DIARIA (4:00 A.M. HORA COLOMBIA) ---
        ahora_utc = datetime.now(timezone.utc)
        ahora_colombia = ahora_utc - timedelta(hours=5)
        corte_hoy = ahora_colombia.replace(hour=4, minute=0, second=0, microsecond=0)
        
        # Determinar cuál fue el último corte de las 4:00 AM que ya pasó
        if ahora_colombia >= corte_hoy:
            limite_corte_timestamp = corte_hoy.timestamp()
        else:
            limite_corte_timestamp = (corte_hoy - timedelta(days=1)).timestamp()
            
        login_time = session.get('login_time')
        
        # Si no tiene registro de tiempo o inició sesión antes del último corte de las 4 AM, lo expulsamos
        if not login_time or login_time < limite_corte_timestamp:
            session.clear()
            flash('Tu sesión ha expirado por cambio de turno (4:00 a.m.). Por favor, ingresa nuevamente.', 'warning')
            return redirect(url_for('index'))
        # -----------------------------------------------------------
        
        # --- LÓGICA DE CONTROL DE SESIONES ÚNICAS ---
        perfil = str(session.get('perfil', '')).strip().lower()
        # NUEVA REGLA: Sólo estos perfiles pueden tener múltiples sesiones abiertas a la vez
        perfiles_multi_sesion = ['operador_logistica', 'webmaster_admin', 'supervisor_gas', 'gestor_flotacarga']
        
        if perfil not in perfiles_multi_sesion:
            token_sesion_actual = session.get('token_sesion')
            usuario_id = session.get('usuario_id')
            
            if token_sesion_actual and usuario_id:
                try:
                    cur = mysql.connection.cursor()
                    cur.execute("SELECT token_sesion FROM usuarios WHERE id = %s", (usuario_id,))
                    row = cur.fetchone()
                    cur.close()
                    
                    # Si el token en BD es distinto, significa que inició sesión en otro dispositivo
                    if row and row[0] != token_sesion_actual:
                        session.clear()
                        flash('Tu sesión fue cerrada porque ingresaste desde otro dispositivo.', 'danger')
                        return redirect(url_for('index'))
                except Exception as e:
                    print(f"Error validando token de sesión única: {e}")
        # ---------------------------------------------
        
        return f(*args, **kwargs)
    return decorated_function

# ==============================================================================
# 4. RUTAS OFFLINE (PWA)
# ==============================================================================
@app.route("/login_energix360_offline.html")
def login_energix360_offline():
    return render_template("login_energix360_offline.html")

@app.route("/sw.js")
def sw():
    return send_from_directory(app.static_folder, "sw.js", mimetype="application/javascript")

# ==============================================================================
# 5. CONTROLADOR DE INICIO (INDEX)
# ==============================================================================
@app.route('/')
def index():
    if 'usuario_id' in session:
        return redirect(url_for('panel_principal'))

    form = LoginForm()
    try:
        cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
        cur.execute("SELECT nit, nombre_comercial FROM empresas ORDER BY nombre_comercial ASC")
        empresas_db = cur.fetchall()
        cur.close()
        form.empresa.choices = [(e['nit'], e['nombre_comercial']) for e in empresas_db]
    except Exception as err:
        print(f"Error al cargar lista de empresas: {err}")
        form.empresa.choices = []

    return render_template('login_energix360.html', form=form)

# ==============================================================================
# 6. PANEL DE CONTROL UNIFICADO (DASHBOARD)
# ==============================================================================
@app.route('/panel_principal.html')
@login_required_custom
def panel_principal():
    if 'webmaster' in str(session.get('tipo_empresa', '')).lower() or session.get('empresa_id') == '901811727':
        return redirect(url_for('bp_901811727.panel_webmaster'))

    nombre_empresa = session.get('empresa', 'Empresa')
    nombre_usuario = session.get('nombre', 'Usuario')
    nit_empresa = session.get('empresa_id')
    modulos_activos = session.get('modulos_activos', [])
    perfil_usuario = session.get('perfil')

    return render_template(
        'A_dashboard_universal.html',
        empresa=nombre_empresa,
        usuario=nombre_usuario,
        nit=nit_empresa,
        modulos_activos=modulos_activos,
        perfil=perfil_usuario
    )

# ==============================================================================
# 7. PROCESADOR DE AUTENTICACIÓN (LOGIN VÍA JSON)
# ==============================================================================
@app.route('/login', methods=['GET', 'POST'])
@csrf.exempt
def login():
    if request.method == 'GET':
        return redirect(url_for('index'))

    data = request.get_json(force=True)
    cedula = data.get('cedula')
    password = data.get('password')
    nombre_empresa = data.get('empresa')

    if not all([cedula, password, nombre_empresa]):
        return jsonify(success=False, message="Por favor, complete todos los campos.")

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    
    cur.execute("SELECT nit, tipo_empresa FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
    emp_info = cur.fetchone()
    if not emp_info:
        cur.close()
        return jsonify(success=False, message="Empresa no encontrada.")

    nit_empresa = str(emp_info['nit'])
    tipo_empresa = str(emp_info.get('tipo_empresa') or '').lower()

    cur.execute("SELECT * FROM usuarios WHERE cedula = %s", (cedula,))
    usuario = cur.fetchone()

    if not usuario or not bcrypt.check_password_hash(usuario['password'], password):
        cur.close()
        return jsonify(success=False, message="Cédula o contraseña incorrecta.")

    if str(usuario['empresa_id']) != nit_empresa:
        cur.close()
        return jsonify(success=False, message="El usuario no pertenece a la empresa seleccionada.")

    # --- GENERACIÓN DE TOKEN DE SESIÓN Y TIMESTAMP (HORA COLOMBIA) ---
    token_sesion = str(uuid.uuid4())
    ahora_utc = datetime.now(timezone.utc)
    ahora_colombia = ahora_utc - timedelta(hours=5)
    login_timestamp = ahora_colombia.timestamp()
    
    try:
        cur.execute("UPDATE usuarios SET token_sesion = %s WHERE id = %s", (token_sesion, usuario['id']))
        mysql.connection.commit()
    except Exception as e:
        print(f"Error al guardar token de sesión: {e}")
    # ------------------------------------------------------------------

    modulos_activos = []
    try:
        cur.execute("""
            SELECT modulo FROM modulos_empresas_autorizadas 
            WHERE id_empresa = %s AND estatus = 'activo'
        """, (nit_empresa,))
        modulos_activos = [m['modulo'] for m in cur.fetchall()]
    except Exception as e:
        print(f"Error cargando módulos autorizados: {e}")
    
    cur.close()

    session.update({
        'usuario_id': usuario['id'],
        'cedula': usuario['cedula'],
        'nombre': usuario['nombre'],
        'empresa': usuario['empresa'],
        'empresa_id': usuario['empresa_id'],
        'nit': usuario['empresa_id'],
        'tipo_empresa': tipo_empresa,
        'perfil': str(usuario.get('perfil') or '').strip().lower(),
        'modulos_activos': modulos_activos,
        'token_sesion': token_sesion,
        'login_time': login_timestamp
    })

    print(f"[{datetime.now()}] Login Exitoso - CC: {session['cedula']} | NIT: {session['empresa_id']} | Perfil: {session['perfil']} | Modulos: {session['modulos_activos']}")

    return jsonify(
        success=True,
        html="panel_principal.html", 
        usuario={"id": usuario["id"], "nombre": usuario["nombre"]}
    )

# ==============================================================================
# 8. ENRUTADOR MAESTRO UNIVERSAL (DINÁMICO POR BD CON CAPA DE TRADUCCIÓN)
# ==============================================================================
import inspect
import re

_MAPA_RUTAS_AUTODESCUBIERTO = {}

def ejecutar_escaneo_introspeccion():
    global _MAPA_RUTAS_AUTODESCUBIERTO
    if _MAPA_RUTAS_AUTODESCUBIERTO:
        return _MAPA_RUTAS_AUTODESCUBIERTO

    nuevo_mapa = {}
    from flask import current_app
    for endpoint, func in current_app.view_functions.items():
        try:
            codigo_fuente = inspect.getsource(func)
            htmls_encontrados = re.findall(r'[\'"]([^\'"]+\.html)[\'"]', codigo_fuente)
            for html in htmls_encontrados:
                if html not in nuevo_mapa:
                    nuevo_mapa[html] = endpoint
        except Exception:
            pass

    _MAPA_RUTAS_AUTODESCUBIERTO = nuevo_mapa
    return _MAPA_RUTAS_AUTODESCUBIERTO

@app.route('/router/<modulo>')
@login_required_custom
def router_universal(modulo):
    perfil_usuario = str(session.get('perfil', '')).strip().lower()
    nit_empresa = str(session.get('empresa_id', '')).strip()
    modulos_comprados = session.get('modulos_activos', [])

    if modulo not in modulos_comprados:
        flash(f"Tu empresa no tiene contratado el módulo de {modulo.upper()}.", "warning")
        return redirect(url_for('panel_principal'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT archivo_destino FROM perfiles 
        WHERE nit = %s AND operacion = %s AND perfil = %s
    """, (nit_empresa, modulo, perfil_usuario))
    regla_perfil = cur.fetchone()
    cur.close()

    if not regla_perfil or not regla_perfil.get('archivo_destino'):
        flash(f"Acceso denegado: Tu perfil no tiene rutas asignadas.", "danger")
        return redirect(url_for('panel_principal'))

    archivo_destino = str(regla_perfil['archivo_destino']).strip()
    mapa_sistema = ejecutar_escaneo_introspeccion()

    # Redirección inteligente al Blueprint
    if archivo_destino in mapa_sistema:
        return redirect(url_for(mapa_sistema[archivo_destino]))
    elif not archivo_destino.endswith(".html"):
        return redirect(url_for(archivo_destino))
    else:
        return render_template(archivo_destino, nombre=session.get('nombre'), empresa=session.get('empresa'), perfil=perfil_usuario, nit=nit_empresa, tipo_empresa=session.get('tipo_empresa'))

# ==============================================================================
# 9. CIERRE DE SESIÓN
# ==============================================================================
@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/api/modulos_sistema_disponibles')
@login_required_custom
def api_modulos_sistema_disponibles():
    from flask import current_app
    excluir = ['static', 'bp_901811727', 'main_router']
    
    # 1. Escaneo en bruto de los nombres de los blueprints en memoria
    modulos_raw = list(set([name.replace('bp_', '').replace('B_bp_', '').replace('B_modulo_', '') 
                        for name in current_app.blueprints.keys() if name not in excluir]))
    
    # 2. Diccionario Traductor para consolidar la V1 en la V2
    mapa_normalizacion = {
        'glp': 'gas', 
        'supervisorgas': 'gas',
        'flotacarga': 'flota', 
        'combustible_flota': 'flota', 
        'gestorflota': 'flota', 
        'preoperacional': 'flota',
        'gestion_carga': 'carga', 
        'gestionavicola_bp': 'carga',
        'gestion_mermas': 'mermas'
    }
    
    # 3. Homologamos hacia los 4 nombres estándar del Dashboard V2
    modulos_v2 = set()
    for mod in modulos_raw:
        nombre_limpio = mapa_normalizacion.get(mod, mod)
        modulos_v2.add(nombre_limpio)
        
    return jsonify(success=True, modulos=sorted(list(modulos_v2)))

# ==============================================================================
# 10. BLOQUE DE EJECUCIÓN LOCAL
# ==============================================================================
if __name__ == '__main__':
    if not os.path.exists(app.static_folder):
        print(f"ADVERTENCIA: Carpeta static no detectada en: {app.static_folder}")
    
    app.run(debug=True, port=5002)