from flask import Blueprint, render_template, session, redirect, url_for, flash
from app.utils import login_required_custom

bp_supervisorgas = Blueprint('bp_supervisorgas', __name__)

@bp_supervisorgas.route('/B_supervisorgas.html')
@login_required_custom
def panel_supervisor():
    # 1. Validar Perfil por seguridad
    perfil = str(session.get('perfil', '')).strip().lower()
    if perfil != 'supervisor_gas':
        flash("Acceso denegado: No tienes rol de Supervisor de Gas.", "danger")
        return redirect(url_for('gestionavicola_bp.panel_avicola'))

    # 2. Renderizar plantilla inyectando los datos de SU propia empresa
    return render_template(
        'B_supervisorgas.html',
        nombre=session.get('nombre'),
        empresa=session.get('empresa'),
        empresa_id=session.get('empresa_id') # Inyectamos el NIT
    )