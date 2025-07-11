# app/__init__.py
from flask import Flask
from flask_mysqldb import MySQL
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from dotenv import load_dotenv
from flask_bcrypt import Bcrypt
import os

mysql = MySQL()
bcrypt = Bcrypt()
login_manager = LoginManager()
csrf = CSRFProtect()

def create_app():
    load_dotenv()
    app = Flask(__name__)

    # Seguridad y configuración
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY')
    app.config['MYSQL_HOST'] = 'localhost'
    app.config['MYSQL_USER'] = 'root'
    app.config['MYSQL_PASSWORD'] = ''
    app.config['MYSQL_DB'] = 'energix_360bd'
    app.config['MYSQL_CURSORCLASS'] = 'DictCursor'

    # Inicializar extensiones
    mysql.init_app(app)
    login_manager.init_app(app)
    csrf.init_app(app)
    csrf.exempt('bp_901811727.registrar_empresa')  # ← excluye esa ruta del CSRF


    # Registrar Blueprints
    from app.blueprints.bp_901811727 import bp_901811727
    from app.blueprints.bp_890707006 import bp_890707006

    app.register_blueprint(bp_901811727)
    app.register_blueprint(bp_890707006)

    return app

@login_manager.user_loader
def load_user(user_id):
    from . import mysql
    cur = mysql.connection.cursor()
    cur.execute("SELECT * FROM usuarios WHERE id = %s", (user_id,))
    user_data = cur.fetchone()
    cur.close()
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
