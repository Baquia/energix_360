# MÓDULO: UNIVERSAL | ARCHIVO: energix_360.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
import os
import uuid
import json
from flask import render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory, current_app
from app import create_app, mysql, csrf, bcrypt
from app.forms import LoginForm
from functools import wraps
import MySQLdb.cursors
from datetime import datetime, timedelta, timezone
import time

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
        perfil = str(session.get('perfil', '')).strip().lower()
        perfiles_multi_sesion = [
            'operador_logistica', 
            'webmaster_admin', 
            'supervisor_gas', 
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
    if 'webmaster' in str(session.get('tipo_empresa', '')).lower() or session.get('empresa_id') == '901811727':
        return redirect(url_for('bp_901811727_main.panel_webmaster'))

    perfil_usuario = str(session.get('perfil', '')).strip().lower()
    tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()

    # REGLA M1, M2 Y M3 ESTRICTA: El Operador NUNCA pasa por el panel de tarjetas unificado
    if perfil_usuario == 'operador_flotaespecial':
        return redirect(url_for('router_universal', modulo='flotaespecial'))
    elif perfil_usuario == 'operador_flotacarga':
        return redirect(url_for('router_universal', modulo='carga'))
    elif perfil_usuario == 'operador_logistica':
        return redirect(url_for('router_universal', modulo='operador_bodegas'))
    elif perfil_usuario == 'verificador_bodegas':
        return redirect(url_for('router_universal', modulo='verificador_bodegas'))
    elif perfil_usuario in ['vendedor', 'operador_ventas']:
        return redirect(url_for('router_universal', modulo='operador_ventas'))
    elif perfil_usuario in ['operador_recursohumano', 'operador_rrhh']:
        return redirect(url_for('router_universal', modulo='operador_recursohumano'))
    elif perfil_usuario == 'operador_contable':
        return redirect(url_for('router_universal', modulo='operador_contable'))
    elif perfil_usuario == 'operador_facturacion':
        return redirect(url_for('router_universal', modulo='operador_facturacion'))
    elif perfil_usuario == 'controlador_pesv':
        return redirect('/gestor_flota/pesv_bp/')

    # SALTO DIRECTO Y DEFINITIVO: Omite la vista universal (Imágenes 4 y 7 redundantes)
    # Encamina la petición incondicionalmente a la vista nativa consolidada del controlador.
    if tipo_empresa == 'transporte_especial':
        return redirect('/gestor_flotaespecial/dashboard')
    elif tipo_empresa == 'transporte_carga':
        return redirect('/gestor_flota/dashboard')
    elif tipo_empresa == 'ventas_distribucion':
        # Enrutamiento jerárquico según perfil para la suite M3
        if perfil_usuario in ['gerente_ventasdistribucion', 'gerente']:
            return redirect('/gerencia_ventas/dashboard')
        elif perfil_usuario in ['controlador_bodegas', 'controlador_logistica']:
            return redirect('/hub_bodegas/dashboard')
        elif perfil_usuario == 'controlador_ventas':
            return redirect('/hub_ventas/dashboard')
        elif perfil_usuario in ['controlador_administracion', 'controlador_recurso_humano', 'controlador_contabilidad', 'controlador_facturacion']:
            return redirect('/hub_administracion/dashboard')
        return redirect('/hub_ventas_distribucion/dashboard')
    elif tipo_empresa == 'cria_beneficio_aves_corral':
        return redirect(url_for('router_universal', modulo='avicola'))

    # Fallback para empresas generales que requieran vista de tarjetas mezclada
    nombre_empresa = session.get('empresa', 'Empresa')
    nombre_usuario = session.get('nombre', 'Usuario')
    nit_empresa = session.get('empresa_id')
    modulos_activos = session.get('modulos_activos', [])

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

    cur.execute("SELECT * FROM usuarios WHERE cedula = %s AND empresa_id = %s", (cedula, nit_empresa))
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
        'submodulos_activos': submodulos_activos_empresa, 
        'token_sesion': token_sesion,
        'login_time': login_timestamp
    })

    print(f"[{datetime.now()}] Login Exitoso - CC: {session['cedula']} | NIT: {session['empresa_id']} | Perfil: {session['perfil']} | Modulos: {session['modulos_activos']} | Submodulos: {session['submodulos_activos']}")

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

def ejecutar_escaneo_introspeccion():
    if 'MAPA_RUTAS_AUTODESCUBIERTO' in current_app.config:
        return current_app.config['MAPA_RUTAS_AUTODESCUBIERTO']

    nuevo_mapa = {}
    from flask import current_app
    for endpoint, func in current_app.view_functions.items():
        # REGLA ESTRICTA BQA-ONE: Ignorar router_universal para no crear un loop infinito 
        # en la asignación del mapa de introspección que causa el BuildError.
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
    perfil_usuario = str(session.get('perfil', '')).strip().lower()
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
    
    # Módulos transversales universales permitidos (Evita bloqueos falsos)
    modulos_transversales_permitidos = [
        'preoperacional', 'eps_gestion', 'eps_facturacion', 
        'operador_flotaespecial', 'operador_flotacarga', 'operador_bodegas', 
        'verificador_bodegas', 'vendedor', 'operador_ventas', 
        'operador_recursohumano', 'operador_contable', 'operador_facturacion', 'pesv'
    ]
    
    if modulo not in modulos_comprados and modulo not in submodulos_activos and not es_nativo and modulo not in modulos_transversales_permitidos:
        flash(f"Tu empresa no tiene habilitado el módulo o submódulo de {modulo.upper()}.", "warning")
        return redirect(url_for('panel_principal'))

    # FALLBACK ESTRUCTURAL PARA SUB-HUBS NATIVOS
    if es_nativo:
        if modulo == 'ventas':
            if perfil_usuario in ['gerente_ventasdistribucion', 'gerente']:
                return redirect('/gerencia_ventas/dashboard')
            elif perfil_usuario in ['controlador_bodegas', 'controlador_logistica']:
                return redirect('/hub_bodegas/dashboard')
            elif perfil_usuario == 'controlador_ventas':
                return redirect('/hub_ventas/dashboard')
            elif perfil_usuario in ['controlador_administracion', 'controlador_recurso_humano', 'controlador_contabilidad', 'controlador_facturacion']:
                return redirect('/hub_administracion/dashboard')
            return redirect('/hub_ventas_distribucion/dashboard')
        elif modulo == 'flotaespecial':
            # REGLA M1 ESTRICTA: Aislamiento estricto del Operador Flota Especial
            if perfil_usuario == 'operador_flotaespecial':
                try:
                    max_reintentos = 3
                    preop = None
                    for intento in range(max_reintentos):
                        try:
                            cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
                            # Bloqueo pesimista OBLIGATORIO para prevenir race conditions en turno
                            cur.execute("""
                                SELECT id_inspeccion 
                                FROM inspeccion_preoperacional 
                                WHERE id_usuario_conductor = %s 
                                  AND id_empresa = %s 
                                  AND fecha_inspeccion = CURDATE() 
                                FOR UPDATE
                            """, (session.get('usuario_id'), nit_empresa))
                            preop = cur.fetchone()
                            mysql.connection.commit()
                            cur.close()
                            break
                        except Exception as e:
                            mysql.connection.rollback()
                            if intento == max_reintentos - 1:
                                raise e
                            time.sleep(0.5)
                    
                    if preop:
                        return redirect(url_for('router_universal', modulo='operador_flotaespecial'))
                    else:
                        return redirect(url_for('router_universal', modulo='preoperacional'))
                except Exception as e:
                    print(f"Error en validación M1 (Preoperacional): {e}")
                    return redirect(url_for('router_universal', modulo='preoperacional'))
            else:
                return redirect('/gestor_flotaespecial/dashboard')
        elif modulo == 'carga':
            # REGLA M2 ESTRICTA: Aislamiento estricto del Operador Transporte Carga
            if perfil_usuario == 'operador_flotacarga':
                # El flujo dicta que el operador de carga entra directamente a su Hub/Lector QR
                return redirect(url_for('router_universal', modulo='operador_flotacarga'))
            elif perfil_usuario == 'controlador_pesv':
                return redirect('/gestor_flota/pesv_bp/')
            else:
                return redirect('/gestor_flota/dashboard')
        elif modulo == 'avicola':
            return render_template('A_gestion_avicola_offline.html', nombre=session.get('nombre'), empresa=session.get('empresa'), perfil=perfil_usuario, nit=nit_empresa, tipo_empresa=tipo_empresa)

    # FALLBACK ANTI-LOOP DE SUBMÓDULOS CON ENRUTAMIENTO DIRECTO A BLUEPRINTS
    if modulo == 'eps_gestion':
        return redirect('/gestor_flotaespecial/eps_bp/')
    elif modulo == 'eps_facturacion':
        return redirect('/gestor_flotaespecial/facturacion_eps_bp/')
    elif modulo == 'pesv':
        return redirect('/gestor_flota/pesv_bp/')
    elif modulo == 'preoperacional':
        return redirect(url_for('preoperacional.preoperacional_tc'))
    elif modulo == 'operador_flotaespecial':
        return redirect(url_for('operador_flotaespecial.dashboard_operador_especial'))
    elif modulo == 'operador_flotacarga':
        return redirect(url_for('flotacarga.dashboard_operador'))
    elif modulo == 'operador_bodegas':
        return redirect(url_for('oper_bodegas.bodega_operativa'))
    elif modulo == 'verificador_bodegas':
        return redirect(url_for('verificador_bodegas.bodega_verificador'))
    elif modulo == 'vendedor':
        return redirect(url_for('operador_ventas.dashboard_operador'))
    elif modulo == 'operador_ventas':
        return redirect(url_for('operador_ventas.dashboard_operador'))
    elif modulo == 'operador_recursohumano':
        return redirect(url_for('operador_recursohumano.dashboard_operador'))
    elif modulo == 'operador_contable':
        return redirect(url_for('operador_contable.dashboard_operador'))
    elif modulo == 'operador_facturacion':
        return redirect(url_for('operador_facturacion.dashboard_operador'))

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("""
        SELECT archivo_destino FROM perfiles 
        WHERE nit = %s AND operacion = %s AND perfil = %s
    """, (nit_empresa, modulo, perfil_usuario))
    regla_perfil = cur.fetchone()
    cur.close()

    if not regla_perfil or not regla_perfil.get('archivo_destino'):
        flash(f"Acceso denegado: Tu perfil no tiene rutas asignadas para {modulo}.", "danger")
        return redirect(url_for('panel_principal'))

    archivo_destino = str(regla_perfil['archivo_destino']).strip()
    mapa_sistema = ejecutar_escaneo_introspeccion()

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
    excluir = ['static', 'bp_901811727_main', 'bp_901811727_admin', 'bp_901811727_energia_glp', 'bp_901811727_audit', 'main_router']
    
    # Saneamiento unificado reconociendo los prefijos arquitectónicos B_ y C_
    modulos_raw = list(set([name.replace('bp_', '').replace('B_bp_', '').replace('C_bp_', '').replace('B_modulo_', '').replace('C_modulo_', '') 
                        for name in current_app.blueprints.keys() if name not in excluir]))
    
    mapa_normalizacion = {
        'glp': 'gas', 
        'supervisorgas': 'gas',
        'flotacarga': 'flota', 
        'combustible_flota': 'flota', 
        'gestorflota': 'flota', 
        'preoperacional': 'flota',
        'gestion_carga': 'carga', 
        'gestionavicola_bp': 'carga',
        'gestion_mermas': 'mermas',
        'controlador_flotaespecial': 'flotaespecial',
        'operador_flotaespecial': 'flotaespecial',
        'bodegas': 'bodegas',
        'oper_bodegas': 'bodegas',
        'verificador_bodegas': 'bodegas',
        'hub_bodegas': 'bodegas',
        'hub_ventas': 'ventas',
        'hub_administracion': 'administracion',
        'gerencia_ventas': 'gerencia',
        'hub_ventas_distribucion': 'ventas',
        'controlador_pesv': 'pesv',
        'auditorias_pesv': 'pesv'
    }
    
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