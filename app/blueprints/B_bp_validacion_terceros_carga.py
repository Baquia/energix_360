# MÓDULO: TRANSPORTE_CARGA | SUBMÓDULO: VALIDACION_TERCEROS | CONDICIÓN: OBLIGATORIO | TIPO: 1
# app/blueprints/B_bp_validacion_terceros_carga.py
import os
from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, session
from app import mysql, csrf
from app.utils import login_required_custom
import MySQLdb.cursors
from datetime import datetime
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadSignature

bp_validacion_terceros_carga = Blueprint('validacion_terceros_carga', __name__, url_prefix='/terceros_carga')

# ==============================================================================
# SEGURIDAD PÚBLICA: GENERACIÓN Y VERIFICACIÓN DE TOKENS TEMPORALES
# ==============================================================================
def generate_token(placa, nit_empresa):
    """Genera un token seguro en la URL con validez temporal para el tercero."""
    s = URLSafeTimedSerializer(current_app.config['SECRET_KEY'])
    return s.dumps({'placa': placa, 'nit_empresa': nit_empresa}, salt='terceros-token')

def verify_token(token, max_age=3600):
    """Verifica la firma del token (expira en 1 hora por defecto)."""
    s = URLSafeTimedSerializer(current_app.config['SECRET_KEY'])
    try:
        data = s.loads(token, salt='terceros-token', max_age=max_age)
        return data
    except (SignatureExpired, BadSignature):
        return None

# ==============================================================================
# PASO 1: LECTOR/INGRESO DE PLACA Y VALIDACIÓN DE APTITUD OPERATIVA
# ==============================================================================
@bp_validacion_terceros_carga.route('/ingreso/<nit_empresa>', methods=['GET', 'POST'])
def ingreso_terceros(nit_empresa):
    """
    Punto de entrada público. Valida la placa contra la base de datos de la empresa,
    evalúa la aptitud operativa PESV y rutea al preoperacional o a la pantalla de bloqueo
    sin requerir captura fotográfica ni validaciones biométricas.
    """
    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, nit, nombre_comercial FROM empresas WHERE nit = %s", (nit_empresa,))
    empresa = cur.fetchone()
    
    if not empresa:
        cur.close()
        return "Empresa no encontrada o NIT inválido en el QR.", 404

    if request.method == 'POST':
        placa = str(request.form.get('placa', '')).upper().strip()
        
        if not placa:
            cur.close()
            flash("Por favor, ingrese la placa del vehículo.", "warning")
            return redirect(url_for('validacion_terceros_carga.ingreso_terceros', nit_empresa=nit_empresa))

        # Verificar vehículo en BD aislando al inquilino
        cur.execute("SELECT * FROM vehiculos WHERE placa = %s AND id_empresa = %s LIMIT 1", (placa, nit_empresa))
        vehiculo = cur.fetchone()

        if not vehiculo:
            cur.close()
            flash(f"El vehículo con placa {placa} no está registrado en la base de datos de {empresa['nombre_comercial']}.", "danger")
            return redirect(url_for('validacion_terceros_carga.ingreso_terceros', nit_empresa=nit_empresa))

        propiedad = str(vehiculo.get('propiedad', '')).strip().lower()

        if propiedad == 'propio':
            cur.close()
            flash("ESTA NO ES SU RUTA, ACCESO DENEGADO. Por favor inicie sesión en la aplicación interna.", "danger")
            return redirect(url_for('validacion_terceros_carga.ingreso_terceros', nit_empresa=nit_empresa))
        else:
            # Extraer conductor asignado al vehículo tercero
            cur.execute("SELECT * FROM conductores_carga WHERE vehiculo_asignado = %s AND id_empresa = %s LIMIT 1", (placa, nit_empresa))
            conductor = cur.fetchone() or {}
            cur.close()
            
            conductor_nombre = conductor.get('nombre', 'NO ASIGNADO')

            # ==============================================================
            # EVALUACIÓN PESV Y ESTADO DOCUMENTAL
            # ==============================================================
            try:
                from app.utils import validar_aptitud_operativa, enviar_alerta_novedad_tercero
                resultado = validar_aptitud_operativa(conductor, vehiculo)
            except ImportError:
                print("⚠️ Motor validar_aptitud_operativa no importado, usando fallback.")
                resultado = {"apto": True, "bloqueos": [], "alertas": []}

            # Si el motor detecta novedades, despachar correo corporativo
            if not resultado['apto'] or len(resultado['alertas']) > 0:
                try:
                    enviar_alerta_novedad_tercero(
                        empresa_id=nit_empresa,
                        empresa_nombre=empresa['nombre_comercial'],
                        placa=placa,
                        conductor_nombre=conductor_nombre,
                        novedades_lista=[b['mensaje'] for b in resultado.get('bloqueos', [])]
                    )
                except Exception as e:
                    print(f"Aviso envío alerta novedad tercero: {e}")
            
            # Generación de token temporal seguro para dar paso al formulario sin sesión de usuario
            token = generate_token(placa, nit_empresa)

            # Inyección del diccionario al frontend
            return render_template(
                'B_validacion_terceros_carga.html', 
                empresa=empresa, 
                placa=placa, 
                mostrar_validacion=True,
                resultado_motor=resultado,
                token_acceso=token
            )

    cur.close()
    return render_template('B_validacion_terceros_carga.html', empresa=empresa, mostrar_validacion=False)

# ==============================================================================
# PASO 2: DILIGENCIAMIENTO PREOPERACIONAL EXTERNO
# ==============================================================================
@bp_validacion_terceros_carga.route('/preoperacional/<token>', methods=['GET'])
def preoperacional_publico(token):
    """
    Redirige al formulario preoperacional inyectando las variables requeridas (Tras validación de aptitud).
    """
    data_token = verify_token(token)
    if not data_token:
        return "El enlace seguro ha expirado o es inválido. Por favor, escanee el código QR e inicie el proceso nuevamente.", 403
        
    placa = data_token['placa']
    nit_empresa = data_token['nit_empresa']

    cur = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cur.execute("SELECT id, nit, nombre_comercial FROM empresas WHERE nit = %s", (nit_empresa,))
    empresa = cur.fetchone()
    
    cur.execute("SELECT * FROM vehiculos WHERE placa = %s AND id_empresa = %s", (placa, nit_empresa))
    vehiculo = cur.fetchone()

    cur.execute("SELECT id, nombre_ruta FROM rutas WHERE id_empresa = %s ORDER BY nombre_ruta ASC", (nit_empresa,))
    rutas_disponibles = cur.fetchall()
    cur.close()

    session['placa_prelogueada'] = placa
    session['empresa_id'] = nit_empresa
    session['nit'] = nit_empresa
    session['empresa'] = empresa['nombre_comercial'] if empresa else ''
    session['nombre'] = f"Conductor Aliado ({placa})"
    session['usuario_id'] = -1  # ID Dummy/Fantasma para identificar al tercero
    session['es_tercero'] = True  # Bandera de aislamiento estricto para terceros

    return render_template(
        'C_preoperacional_tc.html',
        vehiculo=vehiculo,
        rutas=rutas_disponibles,
        nombre_conductor=session.get('nombre'),
        nit=session.get('nit'),
        empresa=session.get('empresa'),
        vista_auditoria=False,
        es_tercero=True
    )

# ==============================================================================
# PASO 3: CONFIRMACIÓN Y PANTALLA DE SALIDA SEGURA (SIN ACCESO AL SISTEMA)
# ==============================================================================
@bp_validacion_terceros_carga.route('/exito_preoperacional/<consecutivo>/<placa>', methods=['GET'])
def exito_preoperacional(consecutivo, placa):
    """
    Pantalla de salida externa y segura para conductores de carga aliados (terceros).
    Confirma el registro y dictamen sin dar acceso a ninguna funcionalidad del sistema interno.
    """
    return render_template(
        'B_exito_preoperacional_tercero.html',
        consecutivo=consecutivo,
        placa=placa
    )