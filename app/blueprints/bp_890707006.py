from flask import Blueprint, render_template, session, flash, redirect, url_for
from app.utils import login_required_custom
from app import mysql

bp_890707006 = Blueprint('bp_890707006', __name__)

@bp_890707006.route('/890707006.html')
@login_required_custom
def panel_pollosgar():
    return render_template('890707006.html', nombre=session.get('nombre'), empresa=session.get('empresa'))


@bp_890707006.route('/dashboard/gas')
@login_required_custom
def acceso_modulo_gas():
    usuario_id = session.get('usuario_id')

    if not usuario_id:
        flash("Sesión no válida. Vuelva a iniciar sesión.", "warning")
        return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for('index'))

    if usuario['perfil'] != 'Gar_Operador_gas':
        flash("No tiene acceso al módulo de gas.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

    # Si el perfil es Granjero, redirigir a la vista glp.html
    return render_template('glp.html', nombre=usuario['nombre'], nit=usuario['empresa_id'])

@bp_890707006.route('/dashboard/mermas')
@login_required_custom
def acceso_modulo_mermas():
    usuario_id = session.get('usuario_id')
    if not usuario_id:
        flash("Sesión no válida. Vuelva a iniciar sesión.", "warning")
        return redirect(url_for('index'))

    # Traemos nombre, perfil y empresa_id
    cur = mysql.connection.cursor()
    cur.execute("SELECT nombre, perfil, empresa_id FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for('index'))

    # Dejamos solo minúsculas, pero SIN reemplazar guiones bajos
    perfil = (usuario.get('perfil') or '').strip().lower()

    # 1) Controlador de mermas -> panel de aprobación
    if perfil == 'gar_controlador_mermas':
        return render_template(
            'controlmermas.html',
            nombre=usuario['nombre'],
            nit=usuario['empresa_id']
        )

    # 2) Operador de mermas / admin mermas -> registro de mermas
    if perfil in ('gar_operador_mermas', 'admin_mermas', 'mermas'):
        return render_template(
            'mermas.html',
            nombre=usuario['nombre'],
            nit=usuario['empresa_id']
        )

    # 3) Ningún perfil autorizado
    flash("No tiene acceso al módulo Control de Mermas.", "danger")
    return redirect(url_for('bp_890707006.panel_pollosgar'))
