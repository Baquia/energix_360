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
        app.config['MYSQL_PASSWORD'] = 'Metanoia765/*'
        app.config['MYSQL_DB'] = 'baquiasoft$energix_360'
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
    #  REGISTRO DE BLUEPRINTS (Arquitectura A/B)
    # =========================================================

    # ---------------------------------------------------------
    #  GRUPO A: CONTROLADORES PRINCIPALES / EMPRESAS
    #  (Gestionan la lógica de negocio de alto nivel)
    # ---------------------------------------------------------
    from app.blueprints.bp_890707006 import bp_890707006    # Pollos GAR
    from app.blueprints.bp_901811727 import bp_901811727    # Webmaster / Admin
    from app.blueprints.A_bp_logistica import logistica_bp  # Logística y Distribución (NUEVO)

    app.register_blueprint(bp_890707006)
    app.register_blueprint(bp_901811727)
    app.register_blueprint(logistica_bp)

    # ---------------------------------------------------------
    #  GRUPO B: MÓDULOS FUNCIONALES / SERVICIOS
    #  (Proveen herramientas: Gas, Mermas, Transporte...)
    # ---------------------------------------------------------
    
    # --SUBMODULOS PARA EMPRESAS AVICOLAS --#
    from app.blueprints.bp_glp import bp_glp #modulo de control GLP
    from app.blueprints.bp_gestion_mermas import bp_gestion_mermas #modulo de gestion Mermas
    
    # --SUBMODULOS PARA VENTAS Y DISTRIBICION --#
    from app.blueprints.B_bp_bodegas import bp_bodegas#modulo de gestion Mermas
    from app.blueprints.B_bp_flotacarga import bp_flotacarga #modulo de gestion Mermas
   
    app.register_blueprint(bp_glp)
    app.register_blueprint(bp_gestion_mermas)
    app.register_blueprint(bp_bodegas)
    app.register_blueprint(bp_flotacarga)

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
        # Ajustamos para instanciar User correctamente según tu modelo
        # (id, nombre, cedula, tipo, clase, rol, empresa_id)
        # Nota: Asegúrate de que tu clase User acepte estos argumentos en este orden
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