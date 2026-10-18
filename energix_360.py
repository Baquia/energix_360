# MÓDULO: UNIVERSAL | ARCHIVO: energix_360.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
import os
import uuid
import json
import inspect
import re
import time
from functools import wraps
from datetime import datetime, timedelta, timezone

from flask import (
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    jsonify,
    send_from_directory,
    current_app
)
import MySQLdb.cursors

from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm
from app.utils import registrar_auditoria

# ==============================================================================
# 1. CONFIGURACIÓN DE RUTAS FÍSICAS E INICIALIZACIÓN (App Factory)
# ==============================================================================
base_dir = os.path.abspath(os.path.dirname(__file__))
static_path = os.path.join(base_dir, "app", "static")

app = create_app()
app.static_folder = static_path
app.static_url_path = "/static"

# ==============================================================================
# 2. AUDITORÍA TRANSACCIONAL CENTRALIZADA BQA-ONE
# ==============================================================================
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
        """, (emp, emp_id, 'energix_360.py', usr, ced, accion, payload))
    except Exception as e:
        print(f"Error auditoria bqa_one en energix_360.py: {e}")

# ==============================================================================
# MATRIZ MAESTRA DE PRODUCTO (Regla Determinista del SaaS BQA-ONE)
# ==============================================================================
MATRIZ_MAESTRA = {
    'bodegas': {
        'nombre_comercial': 'Gestión de Bodegas y Despachos (WMS)',
        'perfiles': [
            {'perfil': 'controlador_logistica', 'operacion': 'bodegas', 'archivo': 'B_modulo_controlador_bodegas.html'},
            {'perfil': 'operador_logistica', 'operacion': 'bodegas', 'archivo': 'B_modulo_operador_bodegas.html'},
            {'perfil': 'verificador_logistica', 'operacion': 'bodegas', 'archivo': 'B_verificador_bodegas.html'}
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
        
        if ahora_colombia >= corte_hoy:
            limite_corte_timestamp = corte_hoy.timestamp()
        else:
            limite_corte_timestamp = (corte_hoy - timedelta(days=1)).timestamp()
            
        login_time = session.get('login_time')
        
        if not login_time or login_time < limite_corte_timestamp:
            session.clear()
            flash('Tu sesión ha expirado por cambio de turno (4:00 a.m.). Por favor, ingresa nuevamente.', 'warning')
            return redirect(url_for('index'))
        # -----------------------------------------------------------
        
        # --- LÓGICA DE CONTROL DE SESIONES ÚNICAS ---
        perfil = str(session.get('nombre_perfil', session.get('perfil', ''))).strip().lower()
        perfiles_multi_sesion = [
            'operador_logistica', 
            'webmaster_admin', 
            'controlador_gas', 
            'gestor_flotacarga',
            'verificador_bodegas',
            'operador_ventas',
            'operador_recursohumano',
            'operador_contable',
            'operador_facturacion'
        ]
        
        if perfil not in perfiles_multi_sesion:
            token_sesion_actual = session.get('token_sesion')
            usuario_id = session.get('usuario_id')
            empresa_id = session.get('empresa_id')
            
            if token_sesion_actual and usuario_id and empresa_id:
                try:
                    cur = mysql.connection.cursor()
                    cur.execute("SELECT token_sesion FROM usuarios WHERE id = %s AND empresa_id = %s", (usuario_id, empresa_id))
                    row = cur.fetchone()
                    cur.close()
                    
                    if row and row[0] != token_sesion_actual:
                        session.clear()
                        flash('Tu sesión fue cerrada porque ingresaste desde otro dispositivo.', 'danger')
                        return redirect(url_for('index'))
                except Exception as e:
                    print(f"Error validando token de sesión única: {e}")
        # ---------------------------------------------
        
        return f(*args, **kwargs)
    return decorated_function

def submodulo_required(submodulo_nombre):
    """
    Middleware Plug & Play: Verifica si el inquilino (empresa) tiene habilitado 
    el submódulo opcional en su arreglo JSON de base de datos.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            submodulos_activos = session.get('submodulos_activos', [])
            if isinstance(submodulos_activos, str):
                try:
                    submodulos_activos = json.loads(submodulos_activos)
                except Exception:
                    submodulos_activos = []
                    
            if submodulo_nombre not in submodulos_activos:
                flash(f'Acceso denegado: Su empresa no tiene activo el submódulo de {submodulo_nombre}.', 'danger')
                return redirect(url_for('panel_principal'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

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
    nombre_perfil = str(session.get('nombre_perfil', session.get('perfil', ''))).strip().lower()
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
    empresa_id = str(session.get('empresa_id', '')).strip()

    # BIFURCACIÓN ESTRICTA CORPORATIVA BAQUÍA-EMS SAS
    if empresa_id == '901811727' or 'webmaster' in tipo_empresa:
        if nombre_perfil in ['gerente_bqa', 'webmaster', 'webmaster_admin', 'admin_general']:
            return redirect('/hub_gerencia_baquia.html')
        elif nombre_perfil == 'webmaster_baqone':
            return redirect('/hub_webmaster_baqone.html')
        elif nombre_perfil in ['webmaster_gestionenergetica', 'webmaster_energia']:
            return redirect('/hub_gestion_energetica.html')
        elif nombre_perfil == 'webmaster_inspecciones':
            return redirect('/hub_inspecciones_tecnicas_clientes.html')
        elif nombre_perfil == 'webmaster_administrador_baquia':
            return redirect('/hub_administracion_baquia.html')
        else:
            return redirect('/hub_gerencia_baquia.html')

    # Resolución Dinámica por Perfil (Motor Dinámico)
    archivo_destino = session.get('archivo_destino', 'A_dashboard_universal.html')

    # Mantenimiento del tipo_empresa como Clasificador de Negocio para consolidación de Hubs
    if archivo_destino in ['panel_principal.html', 'A_dashboard_universal.html']:
        if tipo_empresa == 'transporte_especial':
            return redirect('/gestor_flotaespecial/dashboard')
        elif tipo_empresa == 'transporte_carga':
            return redirect('/gestor_flota/dashboard')
        elif tipo_empresa == 'ventas_distribucion':
            if nombre_perfil in ['gerente_ventasdistribucion', 'gerente']:
                return redirect('/gerencia_ventas/dashboard')
            elif nombre_perfil in ['controlador_bodegas', 'controlador_logistica']:
                return redirect('/hub_bodegas/dashboard')
            elif nombre_perfil == 'controlador_ventas':
                return redirect('/hub_ventas/dashboard')
            elif nombre_perfil in ['controlador_administracion', 'controlador_recurso_humano', 'controlador_contabilidad', 'controlador_facturacion']:
                return redirect('/hub_administracion/dashboard')
            return redirect('/hub_ventas_distribucion/dashboard')
        elif tipo_empresa == 'cria_beneficio_aves_corral':
            return redirect(url_for('router_universal', modulo='avicola'))

    # Enrutamiento Automático Dinámico
    mapa_sistema = ejecutar_escaneo_introspeccion()

    if archivo_destino in mapa_sistema:
        return redirect(url_for(mapa_sistema[archivo_destino]))
    elif not archivo_destino.endswith(".html"):
        return redirect(url_for(archivo_destino))
    else:
        return render_template(
            archivo_destino,
            empresa=session.get('empresa', 'Empresa'),
            usuario=session.get('nombre', 'Usuario'),
            nit=empresa_id,
            modulos_activos=session.get('modulos_activos', []),
            perfil=nombre_perfil
        )

# ==============================================================================
# 7. PROCESADOR DE AUTENTICACIÓN (LOGIN VÍA JSON)
# ==============================================================================
@app.route('/login', methods=['GET', 'POST'])
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
    
    cur.execute("SELECT nit, tipo_empresa, submodulos_activos FROM empresas WHERE nombre_comercial = %s", (nombre_empresa,))
    emp_info = cur.fetchone()
    if not emp_info:
        cur.close()
        return jsonify(success=False, message="Empresa no encontrada.")

    nit_empresa = str(emp_info['nit'])
    tipo_empresa = str(emp_info.get('tipo_empresa') or '').lower()

    submodulos_raw = emp_info.get('submodulos_activos')
    submodulos_activos_empresa = []
    if submodulos_raw:
        if isinstance(submodulos_raw, str):
            try:
                submodulos_activos_empresa = json.loads(submodulos_raw)
            except Exception:
                submodulos_activos_empresa = []
        else:
            submodulos_activos_empresa = submodulos_raw

    # JOIN con tabla perfiles (Normalización con enlace por id_perfil y fallback por nombre)
    cur.execute("""
        SELECT u.*, 
               p.perfil as nombre_perfil, p.archivo_destino 
        FROM usuarios u 
        LEFT JOIN perfiles p ON (u.id_perfil = p.id OR (u.id_perfil IS NULL AND u.perfil = p.perfil AND u.empresa_id = p.nit))
        WHERE u.cedula = %s AND u.empresa_id = %s
    """, (cedula, nit_empresa))
    usuario = cur.fetchone()

    if not usuario or not bcrypt.check_password_hash(usuario['password'], password):
        cur.close()
        return jsonify(success=False, message="Cédula o contraseña incorrecta.")

    if str(usuario['empresa_id']) != nit_empresa:
        cur.close()
        return jsonify(success=False, message="El usuario no pertenece a la empresa seleccionada.")

    # --- GENERACIÓN DE TOKEN DE SESIÓN Y TIMESTAMP ---
    token_sesion = str(uuid.uuid4())
    ahora_utc = datetime.now(timezone.utc)
    ahora_colombia = ahora_utc - timedelta(hours=5)
    login_timestamp = ahora_colombia.timestamp()
    
    try:
        cur.execute("UPDATE usuarios SET token_sesion = %s WHERE id = %s AND empresa_id = %s", (token_sesion, usuario['id'], nit_empresa))
        mysql.connection.commit()
    except Exception as e:
        print(f"Error al guardar token de sesión: {e}")

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

    perfil_final = str(usuario.get('nombre_perfil') or usuario.get('perfil') or '').strip().lower()
    archivo_destino_final = str(usuario.get('archivo_destino') or 'A_dashboard_universal.html')

    session.clear()
    # Inyección Estricta de Contexto en Sesión
    session.update({
        'usuario_id': usuario['id'],
        'id_perfil': usuario.get('id_perfil'),
        'nombre_perfil': perfil_final,
        'perfil': perfil_final,
        'empresa_id': usuario['empresa_id'],
        'nit': usuario['empresa_id'],
        'tipo_empresa': tipo_empresa,
        'modulos_activos': modulos_activos,
        'submodulos_activos': submodulos_activos_empresa, 
        'archivo_destino': archivo_destino_final,
        'nombre': usuario['nombre'],
        'empresa': nombre_empresa,
        'cedula': usuario['cedula'],
        'token_sesion': token_sesion,
        'login_time': login_timestamp
    })

    print(f"[{datetime.now()}] Login Exitoso - CC: {session['cedula']} | NIT: {session['empresa_id']} | Perfil: {session['nombre_perfil']} | Archivo Destino: {session['archivo_destino']}")

    return jsonify(
        success=True,
        html="panel_principal.html", 
        usuario={"id": usuario["id"], "nombre": usuario["nombre"]}
    )

# ==============================================================================
# 8. ENRUTADOR MAESTRO UNIVERSAL (DINÁMICO POR BD CON CAPA DE TRADUCCIÓN)
# ==============================================================================
def ejecutar_escaneo_introspeccion():
    if 'MAPA_RUTAS_AUTODESCUBIERTO' in current_app.config:
        return current_app.config['MAPA_RUTAS_AUTODESCUBIERTO']

    nuevo_mapa = {}
    for endpoint, func in current_app.view_functions.items():
        if endpoint == 'router_universal':
            continue
            
        try:
            codigo_fuente = inspect.getsource(func)
            htmls_encontrados = re.findall(r'[\'"]([^\'"]+\.html)[\'"]', codigo_fuente)
            for html in htmls_encontrados:
                if html not in nuevo_mapa:
                    nuevo_mapa[html] = endpoint
        except Exception:
            pass

    current_app.config['MAPA_RUTAS_AUTODESCUBIERTO'] = nuevo_mapa
    return current_app.config['MAPA_RUTAS_AUTODESCUBIERTO']

@app.route('/router/<modulo>')
@login_required_custom
def router_universal(modulo):
    id_perfil = session.get('id_perfil')
    nombre_perfil = str(session.get('nombre_perfil', session.get('perfil', ''))).strip().lower()
    nit_empresa = str(session.get('empresa_id', '')).strip()
    modulos_comprados = session.get('modulos_activos', [])
    submodulos_activos = session.get('submodulos_activos', [])
    tipo_empresa = session.get('tipo_empresa')

    # Excepción para permitir paso si el módulo es nativo del tipo de empresa
    es_nativo = False
    if modulo == 'flotaespecial' and tipo_empresa == 'transporte_especial': es_nativo = True
    elif modulo == 'carga' and tipo_empresa == 'transporte_carga': es_nativo = True
    elif modulo == 'ventas' and tipo_empresa == 'ventas_distribucion': es_nativo = True
    elif modulo == 'avicola' and tipo_empresa == 'cria_beneficio_aves_corral': es_nativo = True
    
    # Módulos transversales universales permitidos
    modulos_transversales_permitidos = [
        'preoperacional', 'eps_gestion', 'eps_facturacion', 
        'operador_flotaespecial', 'operador_flotacarga', 'operador_bodegas', 
        'verificador_bodegas', 'vendedor', 'operador_ventas', 
        'operador_recursohumano', 'operador_contable', 'operador_facturacion', 'pesv'
    ]
    
    if modulo not in modulos_comprados and modulo not in submodulos_activos and not es_nativo and modulo not in modulos_transversales_permitidos:
        flash(f"Tu empresa no tiene habilitado el módulo o submódulo de {modulo.upper()}.", "warning")
        return redirect(url_for('panel_principal'))

    # Resolución Dinámica por Perfil y Módulo según BD
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    regla_perfil = None
    if id_perfil:
        cur.execute("""
            SELECT archivo_destino FROM perfiles 
            WHERE nit = %s AND operacion = %s AND id = %s
        """, (nit_empresa, modulo, id_perfil))
        regla_perfil = cur.fetchone()

    if not regla_perfil or not regla_perfil.get('archivo_destino'):
        cur.execute("""
            SELECT archivo_destino FROM perfiles 
            WHERE nit = %s AND operacion = %s AND perfil = %s
        """, (nit_empresa, modulo, nombre_perfil))
        regla_perfil = cur.fetchone()

    archivo_destino = None
    if regla_perfil and regla_perfil.get('archivo_destino'):
        archivo_destino = str(regla_perfil['archivo_destino']).strip()
    else:
        # Fallback a la Matriz Maestra de Producto
        if modulo in MATRIZ_MAESTRA:
            for p_info in MATRIZ_MAESTRA[modulo]['perfiles']:
                if p_info['perfil'] == nombre_perfil:
                    archivo_destino = p_info['archivo']
                    break

    cur.close()

    if not archivo_destino:
        return redirect(url_for('panel_principal'))

    mapa_sistema = ejecutar_escaneo_introspeccion()

    if archivo_destino in mapa_sistema:
        return redirect(url_for(mapa_sistema[archivo_destino]))
    elif not archivo_destino.endswith(".html"):
        return redirect(url_for(archivo_destino))
    else:
        return render_template(
            archivo_destino,
            nombre=session.get('nombre'), 
            empresa=session.get('empresa'), 
            perfil=nombre_perfil, 
            nit=nit_empresa, 
            tipo_empresa=tipo_empresa
        )

# ==============================================================================
# 8.5 ONBOARDING TRANSACCIONAL ATÓMICO (WIZARD 4 PASOS)
# ==============================================================================
@app.route('/api/admin/onboarding_empresa', methods=['POST'])
@login_required_custom
def onboarding_empresa():
    nombre_perfil = session.get('nombre_perfil', session.get('perfil', ''))
    if nombre_perfil not in ['webmaster', 'webmaster_admin', 'admin_general']:
        return jsonify(success=False, message="Acceso denegado."), 403

    data = request.get_json(force=True)
    nombre_comercial = data.get('nombre_comercial')
    nit = data.get('nit')
    tipo_empresa = data.get('tipo_empresa')
    modulos = data.get('modulos', [])
    submodulos = data.get('submodulos', [])
    
    usuario_cedula = data.get('usuario_cedula')
    usuario_nombre = data.get('usuario_nombre')
    usuario_password = data.get('usuario_password')

    if not all([nombre_comercial, nit, tipo_empresa, usuario_cedula, usuario_nombre, usuario_password]):
        return jsonify(success=False, message="Faltan datos obligatorios para el onboarding."), 400

    try:
        cur = mysql.connection.cursor()
        cur.execute("START TRANSACTION")

        # Paso 1: Empresa
        cur.execute("INSERT INTO empresas (nombre_comercial, nit, tipo_empresa, submodulos_activos) VALUES (%s, %s, %s, %s)", 
                    (nombre_comercial, nit, tipo_empresa, json.dumps(submodulos)))

        # Paso 2: Módulos Activos y Aprovisionamiento Determinista de Perfiles de la Matriz
        for mod in modulos:
            cur.execute("INSERT INTO modulos_empresas_autorizadas (id_empresa, modulo, estatus) VALUES (%s, %s, 'activo')", (nit, mod))
            
            # Inyección de roles nativos cableados
            if mod in MATRIZ_MAESTRA:
                for p in MATRIZ_MAESTRA[mod]['perfiles']:
                    cur.execute("""
                        INSERT INTO perfiles (empresa, nit, operacion, perfil, archivo_destino)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (nombre_comercial, nit, p['operacion'], p['perfil'], p['archivo']))

        # Paso 3: Perfiles Base y Administrador Raíz
        archivo_destino_def = 'panel_principal.html'
        if tipo_empresa == 'transporte_especial':
            archivo_destino_def = 'B_modulo_controlador_flotaespecial.html'
        elif tipo_empresa == 'transporte_carga':
            archivo_destino_def = 'B_modulo_controlador_flotacarga.html'
        elif tipo_empresa == 'ventas_distribucion':
            archivo_destino_def = 'B_gerente_ventasdistribucion.html'
        
        # Perfil raíz para el administrador de la empresa
        cur.execute("INSERT INTO perfiles (empresa, nit, operacion, perfil, archivo_destino) VALUES (%s, %s, %s, %s, %s)",
                    (nombre_comercial, nit, 'admin', 'admin_general', archivo_destino_def))
        id_perfil_admin = cur.lastrowid

        # Paso 4: Usuario Administrador Inicial vinculado a su id_perfil
        hashed_pw = bcrypt.generate_password_hash(usuario_password).decode('utf-8')
        cur.execute("""INSERT INTO usuarios (cedula, nombre, empresa_id, id_perfil, perfil, password, tipo_usuario, clase) 
                       VALUES (%s, %s, %s, %s, 'admin_general', %s, 'cliente', 'admin')""",
                    (usuario_cedula, usuario_nombre, nit, id_perfil_admin, hashed_pw))

        # Persistencia de Auditoría Obligatoria en Ecosistema
        payload_str = json.dumps(data)
        registrar_auditoria(
            empresa_id=nit,
            empresa_nombre=nombre_comercial,
            modulo='Onboarding',
            usuario=f"{session.get('cedula')} - {session.get('nombre')}",
            accion='CREACION_EMPRESA_COMPLETA',
            detalle=payload_str,
            nivel='INFO'
        )
        registrar_operacion_bqa(cur, 'ONBOARDING_EMPRESA', data)

        cur.execute("COMMIT")
        cur.close()
        return jsonify(success=True, message="Onboarding transaccional completado exitosamente.")
    except Exception as e:
        mysql.connection.rollback()
        return jsonify(success=False, message=f"Error en Onboarding: {str(e)}"), 500

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
    # Retorna de forma limpia las soluciones reconocidas en la Matriz Maestra
    return jsonify(success=True, modulos=sorted(list(MATRIZ_MAESTRA.keys())))

# ==============================================================================
# 10. BLOQUE DE EJECUCIÓN LOCAL
# ==============================================================================
if __name__ == '__main__':
    if not os.path.exists(app.static_folder):
        print(f"ADVERTENCIA: Carpeta static no detectada en: {app.static_folder}")
    
    app.run(debug=True, port=5002)