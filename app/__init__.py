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
        
        print("DEBUG MYSQL_HOST CONFIG =", app.config['MYSQL_HOST'])

    app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

    # ==========================
    # INICIALIZAR EXTENSIONES
    # ==========================
    mysql.init_app(app)
    csrf.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)

    login_manager.login_view = 'index'
    login_manager.login_message_category = 'warning'

    # ==========================
    # REGISTRO DE BLUEPRINTS
    # ==========================
    
    # 1. Módulos Existentes
    from app.blueprints.bp_890707006 import bp_890707006
    from app.blueprints.bp_901811727 import bp_901811727
    from app.blueprints.bp_glp import bp_glp
    from app.blueprints.bp_gestion_mermas import bp_gestion_mermas
    
    # 2. Transporte Especial (TE)
    from app.blueprints.bp_transporte_especial import bp_transporte_especial

    # 3. Transporte Carga (TC) - ¡NUEVO!
    from app.blueprints.bp_transporte_carga import bp_transporte_carga

    # Registro en la App
    app.register_blueprint(bp_890707006)
    app.register_blueprint(bp_901811727)
    app.register_blueprint(bp_glp)
    app.register_blueprint(bp_gestion_mermas)
    app.register_blueprint(bp_transporte_especial)
    app.register_blueprint(bp_transporte_carga)  # <--- Registro del nuevo Blueprint

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
    except Exception:
        return None

    if user_data:
        from .models import Usuario
        return Usuario(
            id=user_data['id'],
            nombre=user_data['nombre'],
            cedula=user_data['cedula'],
            tipo=user_data['tipo'],
            clase=user_data['clase'],
            rol=user_data['rol'],
            empresa_id=user_data['empresa_id']
        )

    return None