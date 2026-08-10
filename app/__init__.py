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
    #  REGISTRO DE BLUEPRINTS (Ajustado a los nombres reales)
    # =========================================================

    # ---------------------------------------------------------
    #  GRUPO A: CONTROLADORES PRINCIPALES / EMPRESAS
    # ---------------------------------------------------------
    from app.blueprints.A_bp_pwa_Avicola import gestionavicola_bp  
    from app.blueprints.bp_901811727 import bp_901811727              

    app.register_blueprint(gestionavicola_bp)
    app.register_blueprint(bp_901811727)

    # ---------------------------------------------------------
    #  GRUPO B: MÓDULOS FUNCIONALES / SERVICIOS
    # ---------------------------------------------------------
    from app.blueprints.bp_glp import bp_glp                               
    from app.blueprints.B_bp_gestion_mermas import bp_gestion_mermas         
    from app.blueprints.B_bp_supervisorgas import bp_supervisorgas         
    from app.blueprints.B_bp_controlador_bodegas import bp_bodegas                     
    from app.blueprints.B_bp_controlador_flotacarga import bp_gestorflota
    from app.blueprints.B_bp_pesaje_carga_avicola import bp_gestion_carga               
    
    # --- NUEVO: Comercializador GLP ---
    from app.blueprints.B_bp_comercializador_glp import bp_comercializador_glp
    
    # --- NUEVO: Transporte Especial ---
    from app.blueprints.B_bp_controlador_flotaespecial import bp_controlador_flotaespecial
   
    app.register_blueprint(bp_glp)
    app.register_blueprint(bp_gestion_mermas)
    app.register_blueprint(bp_supervisorgas)                               
    app.register_blueprint(bp_bodegas)
    app.register_blueprint(bp_gestorflota)
    app.register_blueprint(bp_gestion_carga)
    
    # --- NUEVO: Comercializador GLP ---
    app.register_blueprint(bp_comercializador_glp)
    
    # --- NUEVO: Transporte Especial ---
    app.register_blueprint(bp_controlador_flotaespecial)

    # ---------------------------------------------------------
    #  GRUPO C: OPERACIONES EN CAMPO / MÓVIL
    # ---------------------------------------------------------
    from app.blueprints.B_bp_operador_bodegas import bp_oper_bodegas
    from app.blueprints.B_bp_verificador_bodegas import bp_verificador_bodegas
    from app.blueprints.B_bp_operador_flotacarga import bp_flotacarga
    from app.blueprints.C_bp_combustible_flota import bp_combustible_flota
    from app.blueprints.C_bp_preoperacional import bp_preoperacional
    
    # --- NUEVO: Mecánico GLP (PWA) ---
    from app.blueprints.C_bp_mecanico_glp import bp_mecanico_glp
    
    # --- NUEVO: Transporte Especial ---
    from app.blueprints.B_bp_operador_flotaespecial import bp_operador_flotaespecial
    
    app.register_blueprint(bp_oper_bodegas)
    app.register_blueprint(bp_verificador_bodegas)
    app.register_blueprint(bp_flotacarga)
    app.register_blueprint(bp_combustible_flota)
    app.register_blueprint(bp_preoperacional)
    
    # --- NUEVO: Mecánico GLP (PWA) ---
    app.register_blueprint(bp_mecanico_glp)
    
    # --- NUEVO: Transporte Especial ---
    app.register_blueprint(bp_operador_flotaespecial)

    return app


@login_manager.user_loader
def load_user(user_id):
    if not user_id:
        return None

    try:
        cur = mysql.connection.cursor()
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
        from app.models import User
        user_obj = User(
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