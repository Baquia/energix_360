# MÓDULO: UNIVERSAL | ARCHIVO: __init__.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/__init__.py
from flask import Flask
from flask_mysqldb import MySQL
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from dotenv import load_dotenv
from flask_bcrypt import Bcrypt
import os

# Extensiones globales
mysql = MySQL()
bcrypt = Bcrypt()
login_manager = LoginManager()
csrf = CSRFProtect()

def create_app():
    """
    Factory principal de la aplicación Flask.
    """
    load_dotenv()

    app = Flask(__name__)

    # ==========================
    # CONFIGURACIÓN BÁSICA
    # ==========================
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'cambia-esta-clave-en-produccion')

    # ==========================
    # DETECTAR ENTORNO
    # ==========================
    EN_PYTHONANYWHERE = os.path.exists("/home/baquiasoft")

    if EN_PYTHONANYWHERE:
        print("DEBUG ENTORNO = PYTHONANYWHERE (producción)")
        app.config['MYSQL_HOST'] = 'baquiasoft.mysql.pythonanywhere-services.com'
        app.config['MYSQL_USER'] = 'baquiasoft'
        app.config['MYSQL_PASSWORD'] = 'Ataraxia123*/'
        app.config['MYSQL_DB'] = 'baquiasoft$energix_v2'
    else:
        print("DEBUG ENTORNO = LOCAL (desarrollo)")
        app.config['MYSQL_HOST'] = 'localhost'
        app.config['MYSQL_USER'] = 'root'
        app.config['MYSQL_PASSWORD'] = ''
        app.config['MYSQL_DB'] = 'energix_360'

    # INICIALIZAR EXTENSIONES
    mysql.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)
    csrf.init_app(app)

    # Configuración Login
    login_manager.login_view = 'index'
    login_manager.login_message = "Por favor inicie sesión."

    # =========================================================
    #  REGISTRO DE BLUEPRINTS (Estructura Unificada)
    # =========================================================

    # ---------------------------------------------------------
    #  GRUPO A: CONTROLADORES PRINCIPALES / EMPRESAS Y HUBS
    # ---------------------------------------------------------
    from app.blueprints.A_bp_pwa_Avicola import gestionavicola_bp  
    
    # --- SUBMÓDULOS WEBMASTER BQA-ONE (OPERATIVOS) ---
    from app.blueprints.bp_901811727_main import bp_main
    from app.blueprints.bp_901811727_admin import bp_admin
    from app.blueprints.bp_901811727_energia_glp import bp_energia_glp
    from app.blueprints.bp_901811727_audit import bp_audit

    # --- NUEVOS HUBS CORPORATIVOS BAQUÍA-EMS SAS (ENRUTAMIENTO) ---
    from app.blueprints.B_bp_hub_gerencia_baquia import B_bp_hub_gerencia_baquia
    from app.blueprints.B_bp_hub_webmaster_baqone import B_bp_hub_webmaster_baqone
    from app.blueprints.B_bp_hub_gestion_energetica import B_bp_hub_gestion_energetica
    from app.blueprints.B_bp_hub_inspecciones_tecnicas import B_bp_hub_inspecciones_tecnicas
    from app.blueprints.B_bp_hub_administracion_baquia import B_bp_hub_administracion_baquia

    # --- NUEVOS HUBS NATIVOS: VENTAS Y DISTRIBUCIÓN (M3) ---
    from app.blueprints.B_bp_hub_bodegas import bp_hub_bodegas
    from app.blueprints.B_bp_hub_ventas import bp_hub_ventas
    from app.blueprints.B_bp_hub_administracion import bp_hub_administracion

    app.register_blueprint(gestionavicola_bp)
    
    app.register_blueprint(bp_main)
    app.register_blueprint(bp_admin)
    app.register_blueprint(bp_energia_glp)
    app.register_blueprint(bp_audit)

    # Registro de nuevos Hubs
    app.register_blueprint(B_bp_hub_gerencia_baquia)
    app.register_blueprint(B_bp_hub_webmaster_baqone)
    app.register_blueprint(B_bp_hub_gestion_energetica)
    app.register_blueprint(B_bp_hub_inspecciones_tecnicas)
    app.register_blueprint(B_bp_hub_administracion_baquia)
    
    app.register_blueprint(bp_hub_bodegas)
    app.register_blueprint(bp_hub_ventas)
    app.register_blueprint(bp_hub_administracion)

    # ---------------------------------------------------------
    #  GRUPO B: MÓDULOS FUNCIONALES / SERVICIOS / CONTROLADORES
    # ---------------------------------------------------------
    from app.blueprints.B_bp_operador_glp import bp_operador_glp                               
    from app.blueprints.B_bp_gestion_mermas import bp_gestion_mermas         
    from app.blueprints.B_bp_supervisorgas import bp_supervisorgas         
    from app.blueprints.B_bp_controlador_bodegas import bp_bodegas                     
    from app.blueprints.B_bp_controlador_flotacarga import bp_gestorflota
    from app.blueprints.B_bp_pesaje_carga_avicola import bp_gestion_carga               
    from app.blueprints.B_bp_comercializador_glp import bp_comercializador_glp
    from app.blueprints.B_bp_validacion_terceros_carga import bp_validacion_terceros_carga
    
    # --- TRANSPORTE ESPECIAL (Blueprints Oficiales) ---
    from app.blueprints.B_bp_controlador_flotaespecial import bp_controlador_flotaespecial
    from app.blueprints.B_bp_flotaespecial_flota import bp_flotaespecial_flota
    from app.blueprints.B_bp_flotaespecial_eps import bp_flotaespecial_eps
    from app.blueprints.B_bp_procesofacturacion_eps_transporteespecial import bp_procesofacturacion_eps
    from app.blueprints.C_bp_hub_herramientas_flotaespecial import bp_hub_herramientas_flota

    # --- TRANSPORTE CARGA (Blueprints Refactorizados Plug & Play) ---
    from app.blueprints.C_bp_hub_herramientas_flotacarga import bp_hub_herramientas_flota as bp_hub_herramientas_flotacarga
    from app.blueprints.B_bp_flotacarga_flota import bp_flotacarga_flota
    from app.blueprints.B_bp_flotacarga_operativa import bp_flotacarga_operativa
    
    # --- SUBMÓDULOS PESV (Tipo 2) ---
    from app.blueprints.B_bp_controlador_pesv import bp_controlador_pesv
    from app.blueprints.C_bp_auditorias_PESV import bp_auditorias_pesv

    # --- CONTROLADORES Y GERENCIA VENTAS Y DISTRIBUCIÓN (M3) ---
    from app.blueprints.B_bp_gerente_ventasdistribucion import bp_gerente_ventasdistribucion
    from app.blueprints.B_bp_controlador_ventas import bp_controlador_ventas
    from app.blueprints.B_bp_controlador_recurso_humano import bp_controlador_recurso_humano
    from app.blueprints.B_bp_controlador_contabilidad import bp_controlador_contabilidad
    from app.blueprints.B_bp_controlador_facturacion import bp_controlador_facturacion

    app.register_blueprint(bp_operador_glp)
    app.register_blueprint(bp_gestion_mermas)
    app.register_blueprint(bp_supervisorgas)                               
    app.register_blueprint(bp_bodegas)
    app.register_blueprint(bp_gestorflota)
    app.register_blueprint(bp_gestion_carga)
    app.register_blueprint(bp_comercializador_glp)
    app.register_blueprint(bp_validacion_terceros_carga) 
    
    # Registro Módulo Transporte Especial
    app.register_blueprint(bp_controlador_flotaespecial)
    app.register_blueprint(bp_flotaespecial_flota)
    app.register_blueprint(bp_flotaespecial_eps)
    app.register_blueprint(bp_procesofacturacion_eps)
    app.register_blueprint(bp_hub_herramientas_flota)

    # Registro Submódulos Transporte de Carga P&P
    app.register_blueprint(bp_hub_herramientas_flotacarga)
    app.register_blueprint(bp_flotacarga_flota)
    app.register_blueprint(bp_flotacarga_operativa)
    
    # Registro PESV P&P
    app.register_blueprint(bp_controlador_pesv)
    app.register_blueprint(bp_auditorias_pesv)

    # Registro Controladores Ventas y Distribución
    app.register_blueprint(bp_gerente_ventasdistribucion)
    app.register_blueprint(bp_controlador_ventas)
    app.register_blueprint(bp_controlador_recurso_humano)
    app.register_blueprint(bp_controlador_contabilidad)
    app.register_blueprint(bp_controlador_facturacion)

    # ---------------------------------------------------------
    #  GRUPO C: OPERACIONES EN CAMPO / MÓVIL / OPERADORES
    # ---------------------------------------------------------
    from app.blueprints.B_bp_operador_bodegas import bp_oper_bodegas
    from app.blueprints.B_bp_verificador_bodegas import bp_verificador_bodegas
    from app.blueprints.B_bp_operador_flotacarga import bp_flotacarga
    from app.blueprints.C_bp_combustible_flota import bp_combustible_flota
    from app.blueprints.C_bp_preoperacional import bp_preoperacional
    from app.blueprints.C_bp_mecanico_glp import bp_mecanico_glp
    from app.blueprints.B_bp_operador_flotaespecial import bp_operador_flotaespecial

    # --- OPERADORES VENTAS Y DISTRIBUCIÓN (M3) ---
    from app.blueprints.B_bp_operador_ventas import bp_operador_ventas
    from app.blueprints.B_bp_operador_recursohumano import bp_operador_recursohumano
    from app.blueprints.B_bp_operador_contable import bp_operador_contable
    from app.blueprints.B_bp_operador_facturacion import bp_operador_facturacion
    
    app.register_blueprint(bp_oper_bodegas)
    app.register_blueprint(bp_verificador_bodegas)
    app.register_blueprint(bp_flotacarga)
    app.register_blueprint(bp_combustible_flota)
    app.register_blueprint(bp_preoperacional)
    app.register_blueprint(bp_mecanico_glp)
    app.register_blueprint(bp_operador_flotaespecial)
    
    # Registro Operadores Ventas y Distribución
    app.register_blueprint(bp_operador_ventas)
    app.register_blueprint(bp_operador_recursohumano)
    app.register_blueprint(bp_operador_contable)
    app.register_blueprint(bp_operador_facturacion)

    return app

@login_manager.user_loader
def load_user(user_id):
    if not user_id:
        return None

    try:
        cur = mysql.connection.cursor()
        from flask import session
        empresa_id = session.get('empresa_id')
        
        if empresa_id:
             cur.execute(
                 "SELECT id, nombre, cedula, tipo, clase, rol, empresa_id FROM usuarios WHERE id = %s AND empresa_id = %s",
                 (user_id, empresa_id)
             )
        else:
             cur.execute(
                 "SELECT id, nombre, cedula, tipo, clase, rol, empresa_id FROM usuarios WHERE id = %s",
                 (user_id,)
             )
        
        user_data = cur.fetchone()
        cur.close()
    except Exception as e:
        print(f"Error load_user: {e}")
        return None

    if user_data:
        from app.models import Usuario
        user_obj = Usuario(
            user_data[0],  # id
            user_data[1],  # nombre
            user_data[2],  # cedula
            user_data[3],  # tipo
            user_data[4],  # clase
            user_data[5],  # rol
            user_data[6]   # empresa_id
        )
        return user_obj
    return None