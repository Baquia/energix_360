# MÓDULO: TRANSPORTE_ESPECIAL | SUBMÓDULO: CONTROLADOR_BASE (FIJO)
# app/blueprints/B_bp_controlador_flotaespecial.py
import json
from flask import Blueprint, render_template, session, redirect, url_for, flash
from app import mysql
from app.utils import login_required_custom
from functools import wraps

bp_controlador_flotaespecial = Blueprint('controlador_flotaespecial', __name__, url_prefix='/gestor_flotaespecial')

def controlador_flotaespecial_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        perfil = str(session.get('perfil', '')).strip().lower()
        tipo_empresa = str(session.get('tipo_empresa', '')).strip().lower()
        
        if perfil not in ['controlador_flotaespecial', 'webmaster'] and 'webmaster' not in tipo_empresa:
            flash('Acceso denegado: Se requiere perfil de Controlador de Transporte Especial para ingresar a este módulo.', 'danger')
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# =========================================================
# 1. SUB-MENÚ INTERMEDIO DE SELECCIÓN (HUB TRANSPORTE ESPECIAL)
# =========================================================
@bp_controlador_flotaespecial.route('/dashboard')
@login_required_custom
@controlador_flotaespecial_required
def dashboard_controlador():
    nit = session.get('nit')
    if nit:
        try:
            cur = mysql.connection.cursor()
            cur.execute("SELECT submodulos_activos FROM empresas WHERE nit = %s", (nit,))
            row = cur.fetchone()
            cur.close()
            if row:
                raw_sub = row[0] if isinstance(row, (tuple, list)) else row.get('submodulos_activos')
                if raw_sub:
                    try:
                        session['submodulos_activos'] = json.loads(raw_sub) if isinstance(raw_sub, str) else raw_sub
                    except Exception:
                        session['submodulos_activos'] = []
                else:
                    session['submodulos_activos'] = []
        except Exception as e:
            print(f"Error actualizando submodulos_activos en sesión: {e}")

    return render_template(
        'B_modulo_controlador_flotaespecial.html',
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        usuario=session.get('nombre'),
        modulos_activos=session.get('modulos_activos', []),
        perfil=session.get('perfil')
    )