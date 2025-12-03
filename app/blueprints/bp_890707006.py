from flask import Blueprint, render_template, session, flash, redirect, url_for, request, jsonify
from app.utils import login_required_custom
from app import mysql, csrf
from app import mysql
import re

bp_890707006 = Blueprint('bp_890707006', __name__)

@app.route("/890707006_offline.html")
def panel_pollosgar_offline():
    return render_template("890707006_offline.html")

@app.route("/glp_offline.html")
def glp_offline():
    return render_template("glp_offline.html")

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

# ================================
# 🚗 ACCESO AL MÓDULO DE VEHÍCULOS
# ================================
# ==========================================
# 🚗 PRELOGUEO VEHÍCULO POR QR (placa)
# ==========================================
@csrf.exempt
@bp_890707006.route('/dashboard/flota/prelogin', methods=['POST'])
@login_required_custom
def flota_prelogin_qr():
    try:
        j = request.get_json(force=True, silent=True) or {}
        placa = (j.get("placa") or "").strip().upper()
    except Exception:
        return jsonify(success=False, message="JSON inválido"), 400

    if not placa:
        return jsonify(success=False, message="QR sin placa válida."), 400

    usuario_id = session.get("usuario_id")
    if not usuario_id:
        return jsonify(success=False, message="Sesión no válida."), 401

    # Traer usuario
    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT id, nombre, perfil, empresa_id
        FROM usuarios
        WHERE id=%s
    """, (usuario_id,))
    usuario = cur.fetchone()

    if not usuario:
        cur.close()
        return jsonify(success=False, message="Usuario no encontrado."), 404

    perfil = (usuario.get("perfil") or "").strip().lower()


    empresa_id = str(
    session.get("empresa_id") or
    session.get("nit") or
    usuario.get("empresa_id") or
    ""
    ).strip()

    # Validar perfil operarios_vehiculos
    if perfil != "operarios_vehiculos":
        cur.close()
        return jsonify(success=False, message="No tiene acceso a flota."), 403

    #Normalizacion del QR
    # ...
    placa = (j.get("placa") or "")
    placa = placa.strip().upper()
    placa = re.sub(r'[^A-Z0-9]', '', placa)   # <-- deja SOLO letras/números

    empresa_id = str(
    session.get("empresa_id") or
    session.get("nit") or
    usuario.get("empresa_id") or
    ""
    ).strip()
    empresa_id = re.sub(r'\D', '', empresa_id)  # <-- deja SOLO dígitos
    
    # Verificar vehículo pertenece a la empresa
    cur.execute("""
    SELECT id, estatus
    FROM vehiculos
    WHERE UPPER(TRIM(placa))=%s AND id_empresa=%s
    LIMIT 1
    """, (placa, int(empresa_id) if empresa_id else 0))
    v = cur.fetchone()

    if not v:
        cur.close()
        return jsonify(success=False, message="Vehículo no pertenece a su empresa o no existe."), 404

    # Actualizar estatus → prelogueado
    cur.execute("""
        UPDATE vehiculos
        SET estatus='Prelogueado'
        WHERE id=%s
    """, (v["id"],))
    mysql.connection.commit()
    cur.close()

    # Guardar placa en sesión para usarla en vehiculos.html
    session["placa_prelogueada"] = placa

    return jsonify(
        success=True,
        message="Vehículo prelogueado.",
        redirect_url=url_for("bp_890707006.acceso_modulo_flota")
    )

@bp_890707006.route('/dashboard/flota')
@login_required_custom
def acceso_modulo_flota():
    usuario_id = session.get('usuario_id')
    if not usuario_id:
        flash("Sesión no válida. Vuelva a iniciar sesión.", "warning")
        return redirect(url_for('index'))

    cur = mysql.connection.cursor()
    cur.execute("""
        SELECT nombre, perfil, empresa_id 
        FROM usuarios 
        WHERE id = %s
    """, (usuario_id,))
    usuario = cur.fetchone()
    cur.close()

    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for('index'))

    perfil = (usuario.get('perfil') or '').strip().lower()
    if perfil != 'operarios_vehiculos':
        flash("No tiene acceso al módulo de flota.", "danger")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

    placa = session.get("placa_prelogueada")  # ← viene del QR

    if not placa:
        flash("Debe escanear primero el QR del vehículo.", "warning")
        return redirect(url_for('bp_890707006.panel_pollosgar'))

    return render_template(
        'vehiculos.html',
        nombre=usuario['nombre'],
        nit=usuario['empresa_id'],
        placa=placa
    )




