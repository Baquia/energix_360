# MÓDULO: UNIVERSAL | ARCHIVO: A_bp_pwa_Avicola.py | CONDICIÓN: OBLIGATORIO | TIPO: 1
from flask import Blueprint, render_template, session, flash, redirect, url_for
from app.utils import login_required_custom

gestionavicola_bp = Blueprint('gestionavicola', __name__)

@gestionavicola_bp.route('/A_gestion_avicola.html')
@login_required_custom
def panel_avicola():
    # 1. Validar Perfil por seguridad
    perfil = str(session.get('perfil', '')).strip().lower()
    
    if perfil != 'webmaster':
        flash("Acceso denegado: No tienes rol de Webmaster para Avícola.", "danger")
        return redirect(url_for('panel_principal'))

    return render_template('A_gestion_avicola.html')

@gestionavicola_bp.route("/B_operador_glp_offline.html")
def glp_offline():
    # Renderiza la vista PWA Offline del Operador GLP
    return render_template("B_operador_glp_offline.html")