# app/utils.py
from flask import session, flash, redirect, url_for
from functools import wraps

def login_required_custom(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'usuario_id' not in session:
            flash('Debe iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function


# --- Asegúrate de que esta línea esté al inicio de app/utils.py ---
from app import mysql 
# (Si ya tienes 'from app import mysql, csrf...', no la repitas, solo verifica que esté mysql)


# --- PEGA ESTO AL FINAL DEL ARCHIVO ---

def registrar_auditoria(empresa_id, empresa_nombre, modulo, usuario, accion, detalle, nivel='INFO'):
    """
    Registra un evento en la tabla audit_log para trazabilidad.
    """
    try:
        # Validar que haya conexión
        if not mysql or not mysql.connection:
            print("⚠️ Auditoría: No hay conexión a BD disponible.")
            return

        cur = mysql.connection.cursor()
        
        # Inserción segura
        cur.execute("""
            INSERT INTO audit_log (empresa_id, empresa_nombre, modulo, usuario, accion, detalle, nivel)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (empresa_id, empresa_nombre, modulo, usuario, accion, detalle, nivel))
        
        mysql.connection.commit()
        cur.close()
        
    except Exception as e:
        # Usamos print para no romper el flujo principal si falla la auditoría
        print(f"⛔ Error guardando auditoría: {e}")