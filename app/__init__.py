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
    Se encarga de:
    - Cargar variables de entorno (.env)
    - Configurar la app (SECRET_KEY, MySQL, etc.)
    - Inicializar extensiones (MySQL, CSRF, Bcrypt, LoginManager)
    - Registrar blueprints
    """
    load_dotenv()

    app = Flask(__name__)

    # ==========================
    # CONFIGURACIÓN BÁSICA
    # ==========================
    # Clave secreta
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'cambia-esta-clave-en-produccion')

    # Configuración MySQL
    # Ajusta estos valores a tu configuración real en PythonAnywhere
    app.config['MYSQL_HOST'] = os.getenv('MYSQL_HOST', 'localhost')
    app.config['MYSQL_USER'] = os.getenv('MYSQL_USER', 'root')
    app.config['MYSQL_PASSWORD'] = os.getenv('MYSQL_PASSWORD', '')
    app.config['MYSQL_DB'] = os.getenv('MYSQL_DB', 'energix_360')
    # Para que fetchone()/fetchall() devuelvan diccionarios
    app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

    # (Si usas otras opciones de configuración, puedes agregarlas aquí)

    # ==========================
    # INICIALIZAR EXTENSIONES
    # ==========================
    mysql.init_app(app)
    csrf.init_app(app)
    bcrypt.init_app(app)
    login_manager.init_app(app)

    # Vista de login por defecto para @login_required
    # Cambia 'index' si tu función de login se llama distinto
    login_manager.login_view = 'index'
    login_manager.login_message_category = 'warning'

    # ==========================
    # REGISTRO DE BLUEPRINTS
    # ==========================
    # Asegúrate de que estos módulos existan en app/blueprints/
    from app.blueprints.bp_890707006 import bp_890707006
    from app.blueprints.bp_901811727 import bp_901811727
    from app.blueprints.bp_glp import bp_glp
    from app.blueprints.bp_gestion_mermas import bp_gestion_mermas

    app.register_blueprint(bp_890707006)
    app.register_blueprint(bp_901811727)
    app.register_blueprint(bp_glp)
    app.register_blueprint(bp_gestion_mermas)

    return app


@login_manager.user_loader
def load_user(user_id):
    """
    Callback requerido por Flask-Login para cargar el usuario
    desde la base de datos, dado su ID.
    """
    if not user_id:
        return None

    try:
        cur = mysql.connection.cursor()
        cur.execute(
            """
            SELECT id, nombre, cedula, tipo, clase, rol, empresa_id
            FROM usuarios
            WHERE id = %s
            """,
            (user_id,)
        )
        user_data = cur.fetchone()
        cur.close()
    except Exception:
        # Si hay error de conexión o similar, devolvemos None
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
