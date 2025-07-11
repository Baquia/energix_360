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
